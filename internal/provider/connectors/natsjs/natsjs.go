// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Package natsjs is a NATS JetStream backend for Arke. It implements the
// provider.Provider contract so it can be selected via
// ConnectionConfiguration.provider = "natsjs", side by side with amqp091.
//
// It covers the core publish / subscribe / ack / delayed-retry / dead-letter /
// dedup / header-filter paths an AMQP client exercises through Arke, mapping
// each onto a native JetStream primitive where one exists and onto a small
// amount of proxy-side translation where it does not. It is not a one-to-one
// replacement for every RabbitMQ feature — see doc/design/natsjs-connector.md
// for the per-feature parity matrix and known limitations.
package natsjs

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
)

const providerName = "natsjs"

// retryCountHeaderName mirrors the amqp091 connector. Clients that count retries
// via this header (rather than RabbitMQ's x-death) keep working unchanged because
// the connector synthesizes it from JetStream's delivery count — JetStream has no
// x-death equivalent.
const retryCountHeaderName = "x-retry-count"

const (
	defaultAckWait           = 30 * time.Second
	defaultInactiveThreshold = 5 * time.Minute
	defaultDedupWindow       = 2 * time.Minute
	connectPollInterval      = 50 * time.Millisecond
	// defaultJSAPITimeout bounds JetStream management API calls (stream and
	// consumer creation). Without an explicit deadline the JetStream client
	// applies a 5s default, which can expire while a replicated stream is
	// still forming its raft group on cold or network-attached storage.
	// Override via NATSJS_API_TIMEOUT.
	defaultJSAPITimeout = 30 * time.Second
	// defaultConsumeHeartbeat is the pull-consumer idle heartbeat. If the
	// delivery path stalls, the missed heartbeat is surfaced through the
	// consume error handler and the client re-issues its pull request after
	// roughly twice this interval, instead of ~30s with the library defaults.
	defaultConsumeHeartbeat = 5 * time.Second
	// defaultStreamMaxAge bounds how long a stream retains messages so the
	// JetStream log does not grow without bound (RabbitMQ deletes on ack;
	// JetStream LimitsPolicy does not). 72h is generous enough not to truncate a
	// realistic consumer-outage backlog, short enough to stop indefinite
	// accumulation. Override via NATSJS_STREAM_MAX_AGE.
	defaultStreamMaxAge = 72 * time.Hour
)

// supportedSourceOptionsList intentionally matches the amqp091 connector so that
// existing client Sources validate against the server unchanged.
var supportedSourceOptionsList = []string{
	"MessageTTL", "DeadLetterAddress", "DeadLetterSubject", "Expires", "Offset", "ConsumerGroup",
}

var supportedSourceOptions map[string]bool

// GetClientIdentifier is a var so tests can override it (matches amqp091).
var GetClientIdentifier = util.GetClientIdentifier

type natsBrokerDetails struct {
	sync.Mutex
	nc               *nats.Conn
	js               jetstream.JetStream
	clientIdentifier string
	connectionConfig *pb.ConnectionConfiguration

	// activeMessages maps an Arke message UUID -> the in-flight jetstream.Msg
	// so a later Ack/Nack/Retry/DeadLetter RPC can resolve it.
	activeMessages *util.ConcurrentMap
	// knownStreams memoizes streams we have already ensured.
	knownStreams *util.ConcurrentMap
	// consumeContexts maps source name -> jetstream.ConsumeContext for teardown.
	consumeContexts *util.ConcurrentMap

	state    atomic.Uint32
	consumed int64
	produced int64
}

type natsjsProvider struct {
	connections *util.ConcurrentMap
	// streams collapses concurrent stream-creation calls across connections.
	streams *streamRegistry
}

// streamRegistry collapses concurrent ensureStream calls for the same stream
// into a single JetStream API call. When many clients (re)connect at once —
// e.g. after a broker or proxy restart — every connection would otherwise
// issue its own CreateOrUpdateStream for the same shared streams, piling
// redundant load on the JetStream metadata leader exactly when it is busiest.
// Entries are keyed by broker endpoint + stream name so distinct brokers never
// share results. Only in-flight calls are tracked here; success is memoized
// per connection (knownStreams), preserving the property that a fresh
// connection re-asserts its topology.
type streamRegistry struct {
	mu       sync.Mutex
	inflight map[string]*inflightEnsure
}

type inflightEnsure struct {
	done chan struct{}
	err  error
}

func newStreamRegistry() *streamRegistry {
	return &streamRegistry{inflight: make(map[string]*inflightEnsure)}
}

// ensure runs create once per key at a time; callers that arrive while a call
// for the same key is in flight wait for it and share its result. Failures
// are not cached — the next caller retries.
func (r *streamRegistry) ensure(ctx context.Context, key string, create func() error) error {
	r.mu.Lock()
	if e, ok := r.inflight[key]; ok {
		r.mu.Unlock()
		select {
		case <-e.done:
			return e.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e := &inflightEnsure{done: make(chan struct{})}
	r.inflight[key] = e
	r.mu.Unlock()

	e.err = create()

	r.mu.Lock()
	delete(r.inflight, key)
	r.mu.Unlock()
	close(e.done)
	return e.err
}

func init() {
	provider.Register(providerName, NewNATSJetStreamProvider)
	supportedSourceOptions = make(map[string]bool, len(supportedSourceOptionsList))
	for _, o := range supportedSourceOptionsList {
		supportedSourceOptions[o] = true
	}
}

// NewNATSJetStreamProvider returns a new natsjs provider singleton.
func NewNATSJetStreamProvider() provider.Provider {
	return &natsjsProvider{
		connections: util.NewConcurrentMap(),
		streams:     newStreamRegistry(),
	}
}

// streamReplicas controls the JetStream replication factor (1 = single node,
// 3 = R3/Raft for HA). Configured via NATSJS_STREAM_REPLICAS.
func streamReplicas() int {
	if v := os.Getenv("NATSJS_STREAM_REPLICAS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 1
}

// streamMaxAge is the primary guard against unbounded JetStream log growth.
// Configured via NATSJS_STREAM_MAX_AGE as a Go duration ("72h", "24h", "0" =
// keep forever); defaults to defaultStreamMaxAge.
//
// Note: because the connector uses one stream per address root, this is a
// stream-wide bound, NOT a faithful per-source x-message-ttl. A per-source
// MessageTTL is deliberately NOT folded in here: ensureStream is also called from
// the publish path (which has no Source), so mixing a per-source TTL with the
// global default would make MaxAge flap on the shared stream depending on whether
// a publish or a subscribe touched it last. Per-source TTL fidelity needs a
// per-source stream topology — see the "Known limitations" section of
// doc/design/natsjs-connector.md.
func streamMaxAge() time.Duration {
	if v := os.Getenv("NATSJS_STREAM_MAX_AGE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			return d
		}
	}
	return defaultStreamMaxAge
}

// streamMaxBytes optionally caps a stream's on-disk size as a hard storage
// safety net. With the default discard=old, JetStream evicts the oldest messages
// only when near the cap, so recent backlog is preserved as long as possible.
// Configured via NATSJS_STREAM_MAX_BYTES (bytes); default 0 = unlimited (rely on
// MaxAge). Set it to bound disk per stream when volume is high.
func streamMaxBytes() int64 {
	if v := os.Getenv("NATSJS_STREAM_MAX_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

// jsAPITimeout bounds JetStream management API calls (CreateOrUpdateStream /
// CreateOrUpdateConsumer). First-touch creation of a replicated stream has to
// finish raft-group formation and storage allocation before the call returns,
// so it can legitimately take longer than the JetStream client's built-in 5s
// default, particularly on network-attached storage. Configured via
// NATSJS_API_TIMEOUT as a Go duration ("30s", "1m"); defaults to
// defaultJSAPITimeout.
func jsAPITimeout() time.Duration {
	if v := os.Getenv("NATSJS_API_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return defaultJSAPITimeout
}

func (p *natsjsProvider) getBrokerDetails(ctx context.Context) (*natsBrokerDetails, error) {
	clientID, err := GetClientIdentifier(ctx)
	if err != nil {
		return nil, err
	}
	if bd := p.getBrokerDetailsByIdentifier(clientID); bd != nil {
		return bd, nil
	}
	return nil, fmt.Errorf("could not retrieve broker details for connection: %s", clientID)
}

func (p *natsjsProvider) getBrokerDetailsByIdentifier(id string) *natsBrokerDetails {
	if bd, ok := p.connections.Get(id); ok {
		return bd.(*natsBrokerDetails)
	}
	return nil
}

func (p *natsjsProvider) ClientExists(id string) bool {
	return p.getBrokerDetailsByIdentifier(id) != nil
}

// Connect establishes a NATS connection and JetStream context for the client.
func (p *natsjsProvider) Connect(ctx context.Context, cfg *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error {
	clientID, err := GetClientIdentifier(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error(), IsFatal: true}
	}

	scheme := "nats://"
	if cfg.GetTls() {
		scheme = "tls://"
	}
	url := fmt.Sprintf("%s%s:%d", scheme, cfg.GetHost(), cfg.GetPort())

	bd := &natsBrokerDetails{
		clientIdentifier: clientID,
		connectionConfig: cfg,
		activeMessages:   util.NewConcurrentMap(),
		knownStreams:     util.NewConcurrentMap(),
		consumeContexts:  util.NewConcurrentMap(),
	}
	bd.state.Store(provider.CONNECTING)

	opts := []nats.Option{
		nats.Name(cfg.GetClientName()),
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
		// Keep bd.state honest so WaitForConnect reflects the real link. With
		// RetryOnFailedConnect, nats.Connect returns a non-error connection even
		// when the broker is unreachable; nats.go then reconnects in the
		// background (MaxReconnects(-1)). These callbacks mirror the amqp091
		// connectionWatcher: WaitForConnect waits out a broker outage instead of
		// falsely reporting CONNECTED.
		nats.ConnectHandler(func(*nats.Conn) { bd.state.Store(provider.CONNECTED) }),
		nats.ReconnectHandler(func(*nats.Conn) { bd.state.Store(provider.CONNECTED) }),
		nats.DisconnectErrHandler(func(_ *nats.Conn, _ error) { bd.state.Store(provider.DISCONNECTED) }),
		nats.ClosedHandler(func(*nats.Conn) { bd.state.Store(provider.CLOSED) }),
	}
	if creds := cfg.GetCredentials(); creds != nil && creds.GetUsername() != "" {
		opts = append(opts, nats.UserInfo(creds.GetUsername(), creds.GetPassword()))
	}
	if cfg.GetTls() {
		// The broker certificate is verified against the system trust store;
		// client-provided CA certificates are not supported (matches amqp091).
		tlsCfg := &tls.Config{InsecureSkipVerify: tlsSkipVerify} //nolint:gosec // operator opt-in, mirrors amqp091
		opts = append(opts, nats.Secure(tlsCfg))
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return &pb.Error{Message: fmt.Sprintf("nats connect failed: %s", err.Error()), IsFatal: true}
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return &pb.Error{Message: fmt.Sprintf("jetstream init failed: %s", err.Error()), IsFatal: true}
	}

	bd.nc = nc
	bd.js = js
	// If the broker was reachable, nats.Connect already completed the handshake
	// (the ConnectHandler may not fire for a synchronous connect), so reflect the
	// live status here; otherwise stay CONNECTING until a callback flips it.
	if nc.IsConnected() {
		bd.state.Store(provider.CONNECTED)
	}
	p.connections.Add(clientID, bd)
	util.Logger.Debugf("natsjs: client %s connecting to %s", clientID, url)
	return nil
}

func (p *natsjsProvider) WaitForConnect(ctx context.Context) bool {
	clientID, err := GetClientIdentifier(ctx)
	if err != nil {
		return false
	}
	deadline := time.Now().Add(provider.CONNECTTIMEOUT * time.Second)
	for time.Now().Before(deadline) {
		if bd := p.getBrokerDetailsByIdentifier(clientID); bd != nil && bd.state.Load() == provider.CONNECTED {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(connectPollInterval):
		}
	}
	return false
}

func (p *natsjsProvider) Disconnect(ctx context.Context) {
	clientID, err := GetClientIdentifier(ctx)
	if err != nil {
		return
	}
	bd := p.getBrokerDetailsByIdentifier(clientID)
	if bd == nil {
		return
	}
	for _, name := range bd.consumeContexts.GetList() {
		if cc, ok := bd.consumeContexts.Get(name); ok {
			cc.(jetstream.ConsumeContext).Stop()
		}
	}
	if bd.nc != nil {
		_ = bd.nc.Drain()
	}
	bd.state.Store(provider.CLOSED)
	p.connections.Delete(clientID)
	util.Logger.Debugf("natsjs: client %s disconnected", clientID)
}

// ensureStream lazily creates (or updates) a JetStream stream that captures all
// subjects under the given address root. Concurrent calls for the same stream
// — across all client connections to the same broker — are collapsed into one
// API call (see streamRegistry). That call gets an explicit deadline
// (jsAPITimeout) and is detached from the triggering caller's cancellation:
// the stream is shared topology and other callers may be waiting on the same
// result.
func (p *natsjsProvider) ensureStream(ctx context.Context, bd *natsBrokerDetails, addressName string) (string, error) {
	streamName := streamNameFor(addressName)
	if _, ok := bd.knownStreams.Get(streamName); ok {
		return streamName, nil
	}
	key := fmt.Sprintf("%s:%d/%s", bd.connectionConfig.GetHost(), bd.connectionConfig.GetPort(), streamName)
	err := p.streams.ensure(ctx, key, func() error {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), jsAPITimeout())
		defer cancel()
		_, err := bd.js.CreateOrUpdateStream(cctx, jetstream.StreamConfig{
			Name:       streamName,
			Subjects:   streamSubjectsFor(addressName),
			Storage:    jetstream.FileStorage,
			Retention:  jetstream.LimitsPolicy,
			Duplicates: defaultDedupWindow,
			// Replicas via NATSJS_STREAM_REPLICAS (default 1). Set to 3 against a
			// clustered nats-server for the HA-equivalent of RabbitMQ quorum queues.
			Replicas: streamReplicas(),
			// MaxAge/MaxBytes bound the log so it does not grow without limit the way
			// the default LimitsPolicy otherwise would (RabbitMQ deletes on ack;
			// JetStream retains acked messages). MaxAge is the time guard, MaxBytes an
			// optional hard storage cap. Both are mutable, so this also reins in
			// streams created before the limits existed. See natsjs-connector.md.
			MaxAge:   streamMaxAge(),
			MaxBytes: streamMaxBytes(),
		})
		return err
	})
	if err != nil {
		return "", err
	}
	bd.knownStreams.Add(streamName, true)
	return streamName, nil
}

func firstSubject(addr *pb.Address) string {
	if subs := addr.GetSubjects(); len(subs) > 0 {
		return subs[0]
	}
	return ""
}

// Publish drains the message channel, publishing each message to JetStream.
func (p *natsjsProvider) Publish(ctx context.Context, in <-chan *pb.Message, errChan chan<- *pb.Error) *pb.Error {
	bd, err := p.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error(), IsFatal: true}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-in:
			if !ok || msg == nil {
				return nil
			}
			// The server blocks on <-errChan for every message it hands the
			// provider (server.go: `mc <- msg` then `pubErr := <-ec`), so we
			// must always reply exactly once — nil on success.
			errChan <- p.publishMsg(ctx, bd, msg)
		}
	}
}

func (p *natsjsProvider) PublishOne(ctx context.Context, msg *pb.Message) *pb.Error {
	bd, err := p.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error(), IsFatal: true}
	}
	return p.publishMsg(ctx, bd, msg)
}

func (p *natsjsProvider) publishMsg(ctx context.Context, bd *natsBrokerDetails, msg *pb.Message) *pb.Error {
	addr := msg.GetAddress()
	if _, err := p.ensureStream(ctx, bd, addr.GetName()); err != nil {
		return &pb.Error{Message: fmt.Sprintf("ensure stream: %s", err.Error())}
	}
	nmsg := &nats.Msg{
		Subject: publishSubjectFor(addr.GetName(), firstSubject(addr)),
		Data:    msg.GetBody(),
		Header:  pbToNatsHeader(msg.GetHeaders()),
	}
	var pubOpts []jetstream.PublishOpt
	// publish_id + publisher_name -> Nats-Msg-Id (dedup within the stream window).
	if msg.GetPublisherName() != "" || msg.GetPublishId() != 0 {
		pubOpts = append(pubOpts, jetstream.WithMsgID(fmt.Sprintf("%s-%d", msg.GetPublisherName(), msg.GetPublishId())))
	}
	if _, err := bd.js.PublishMsg(ctx, nmsg, pubOpts...); err != nil {
		return &pb.Error{Message: fmt.Sprintf("publish: %s", err.Error())}
	}
	atomic.AddInt64(&bd.produced, 1)
	return nil
}

// Subscribe ensures topology, creates a consumer, and forwards deliveries to the
// message channel until the context is cancelled. Blocks per the Provider
// contract.
func (p *natsjsProvider) Subscribe(ctx context.Context, source *pb.Source, out chan<- *pb.Message) *pb.Error {
	bd, err := p.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error(), IsFatal: true}
	}
	addr := source.GetAddress().GetName()
	streamName, serr := p.ensureStream(ctx, bd, addr)
	if serr != nil {
		return &pb.Error{Message: fmt.Sprintf("ensure stream: %s", serr.Error()), IsFatal: true}
	}
	// The management API calls below get the same explicit deadline as stream
	// creation (see jsAPITimeout): creating a replicated consumer forms its own
	// raft group, which can outlast the client library's 5s default while the
	// server is cold or busy.
	tctx, tcancel := context.WithTimeout(ctx, jsAPITimeout())
	defer tcancel()
	stream, serr := bd.js.Stream(tctx, streamName)
	if serr != nil {
		return &pb.Error{Message: fmt.Sprintf("open stream: %s", serr.Error()), IsFatal: true}
	}

	prefetch := int(source.GetPrefetchCount())
	if prefetch <= 0 {
		prefetch = 1
	}
	consCfg := jetstream.ConsumerConfig{
		FilterSubjects: filterSubjectsFor(source),
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  deliverPolicyFor(source),
		AckWait:        defaultAckWait,
		MaxAckPending:  prefetch,
	}
	// Durable work-queue consumer for non-transient QUEUE / stream-group sources:
	// it persists across client reconnects so a backlog published during a
	// consumer outage is redelivered (RabbitMQ durable-queue parity). The
	// DeliverPolicy only applies on first creation; reconnects resume from the
	// durable's stored ack position. Transient (auto-delete/exclusive/TEMPORARY)
	// sources stay ephemeral and auto-expire after InactiveThreshold.
	if durable := durableName(source); durable != "" {
		consCfg.Durable = durable
	} else {
		consCfg.InactiveThreshold = defaultInactiveThreshold
	}

	cons, cerr := stream.CreateOrUpdateConsumer(tctx, consCfg)
	if cerr != nil {
		// Include the mapped filter subjects: a NATS "invalid subject" (10052)
		// otherwise gives no hint which source/subject produced it.
		return &pb.Error{
			Message: fmt.Sprintf("create consumer (filters=%v): %s", consCfg.FilterSubjects, cerr.Error()),
			IsFatal: true,
		}
	}

	// DeclareOnly: topology established, do not consume.
	if source.GetDeclareOnly() {
		return nil
	}

	cc, cerr := cons.Consume(func(m jetstream.Msg) {
		p.handleDelivery(ctx, bd, source, m, out)
	},
		// A short idle heartbeat plus an error handler keep a stalled delivery
		// path from failing silently: if the server stops serving this
		// consumer's pulls (broker restart, consumer raft leader not yet
		// serving after creation), the missed heartbeat is logged at warn and
		// the library re-issues its pull request after ~2x the heartbeat,
		// instead of ~30s — and invisibly — with the defaults.
		jetstream.PullHeartbeat(defaultConsumeHeartbeat),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			util.Logger.Warn("natsjs: consume error on source {0}: {1}", source.GetName(), err.Error())
		}),
	)
	if cerr != nil {
		return &pb.Error{Message: fmt.Sprintf("consume: %s", cerr.Error()), IsFatal: true}
	}
	bd.consumeContexts.Add(source.GetName(), cc)
	defer func() {
		cc.Stop()
		bd.consumeContexts.Delete(source.GetName())
	}()

	<-ctx.Done()
	return nil
}

func deliverPolicyFor(source *pb.Source) jetstream.DeliverPolicy {
	// Maps the Offset option. Transient consumers typically want "new" (only
	// messages after subscribe); stream replay consumers want "all".
	switch source.GetOptions()["Offset"] {
	case "first":
		return jetstream.DeliverAllPolicy
	case "next", "":
		return jetstream.DeliverNewPolicy
	default:
		return jetstream.DeliverNewPolicy
	}
}

func (p *natsjsProvider) handleDelivery(ctx context.Context, bd *natsBrokerDetails, source *pb.Source, m jetstream.Msg, out chan<- *pb.Message) {
	defer util.RecoverPanic()

	headers := natsToPbHeader(m.Headers())
	// Synthesize x-retry-count from JetStream's delivery count so a client's retry
	// policy works without RabbitMQ's x-death header.
	if md, err := m.Metadata(); err == nil && md.NumDelivered > 1 {
		headers[retryCountHeaderName] = strconv.FormatUint(md.NumDelivered-1, 10)
	}

	// Proxy-side replacement for RabbitMQ headers-exchange routing.
	if !evaluateFilters(source.GetFilters(), headers) {
		_ = m.Ack()
		return
	}

	uuid := util.GenUUID()
	bd.activeMessages.Add(uuid, m)
	pbmsg := &pb.Message{Uuid: uuid, Headers: headers, Body: m.Data()}

	select {
	case out <- pbmsg:
		atomic.AddInt64(&bd.consumed, 1)
	case <-ctx.Done():
		bd.activeMessages.Delete(uuid)
	}
}

func (p *natsjsProvider) takeMessage(ctx context.Context, uuid string) (jetstream.Msg, *natsBrokerDetails, *pb.Error) {
	bd, err := p.getBrokerDetails(ctx)
	if err != nil {
		return nil, nil, &pb.Error{Message: err.Error()}
	}
	mu, ok := bd.activeMessages.Get(uuid)
	if !ok {
		return nil, bd, &pb.Error{Message: fmt.Sprintf("no message with uuid %s", uuid)}
	}
	bd.activeMessages.Delete(uuid)
	return mu.(jetstream.Msg), bd, nil
}

func (p *natsjsProvider) Ack(ctx context.Context, uuid string) *pb.Error {
	m, _, perr := p.takeMessage(ctx, uuid)
	if perr != nil {
		return perr
	}
	if err := m.Ack(); err != nil {
		return &pb.Error{Message: err.Error()}
	}
	return nil
}

// Nack negatively acknowledges for immediate redelivery (literal AMQP nack).
func (p *natsjsProvider) Nack(ctx context.Context, uuid string) *pb.Error {
	m, _, perr := p.takeMessage(ctx, uuid)
	if perr != nil {
		return perr
	}
	if err := m.Nak(); err != nil {
		return &pb.Error{Message: err.Error()}
	}
	return nil
}

// Retry requeues the message after a delay. NakWithDelay is the native
// JetStream primitive that replaces RabbitMQ's per-message-TTL + dead-letter
// retry-queue idiom; JetStream increments the delivery count for us, which
// handleDelivery surfaces as x-retry-count.
func (p *natsjsProvider) Retry(ctx context.Context, _ *pb.Source, uuid string, delay int32) *pb.Error {
	m, _, perr := p.takeMessage(ctx, uuid)
	if perr != nil {
		return perr
	}
	if err := m.NakWithDelay(time.Duration(delay) * time.Second); err != nil {
		return &pb.Error{Message: err.Error()}
	}
	return nil
}

// DeadLetter publishes the message to the configured dead-letter subject (there
// is no native DLX in JetStream) and terminates redelivery.
func (p *natsjsProvider) DeadLetter(ctx context.Context, source *pb.Source, uuid string) *pb.Error {
	m, bd, perr := p.takeMessage(ctx, uuid)
	if perr != nil {
		return perr
	}
	opts := source.GetOptions()
	if dla := opts["DeadLetterAddress"]; dla != "" {
		if _, err := p.ensureStream(ctx, bd, dla); err == nil {
			dlqMsg := &nats.Msg{
				Subject: publishSubjectFor(dla, opts["DeadLetterSubject"]),
				Data:    m.Data(),
				Header:  m.Headers(),
			}
			if _, err := bd.js.PublishMsg(ctx, dlqMsg); err != nil {
				util.Logger.Debugf("natsjs: dead-letter publish failed for %s: %s", uuid, err.Error())
			}
		}
	}
	if err := m.Term(); err != nil {
		return &pb.Error{Message: err.Error()}
	}
	return nil
}

func (p *natsjsProvider) SupportedSourceOptions() map[string]bool {
	return supportedSourceOptions
}

func (p *natsjsProvider) Stats() *provider.Stats {
	stats := &provider.Stats{}
	for _, id := range p.connections.GetList() {
		bd := p.getBrokerDetailsByIdentifier(id)
		if bd == nil {
			continue
		}
		stats.Clients = append(stats.Clients, &provider.ClientStats{
			ID:             id,
			ActiveMessages: bd.activeMessages.Length(),
			Streams:        bd.consumeContexts.Length(),
			Produced:       int(atomic.LoadInt64(&bd.produced)),
			Consumed:       int(atomic.LoadInt64(&bd.consumed)),
		})
	}
	return stats
}

func (p *natsjsProvider) SourceStats(ctx context.Context, source *pb.Source) *pb.SourceStats {
	stats := &pb.SourceStats{Name: source.GetName()}
	bd, err := p.getBrokerDetails(ctx)
	if err != nil {
		stats.Error = &pb.Error{Message: err.Error()}
		return stats
	}
	streamName := streamNameFor(source.GetAddress().GetName())
	stream, serr := bd.js.Stream(ctx, streamName)
	if serr != nil {
		stats.Error = &pb.Error{Message: serr.Error()}
		return stats
	}
	info, ierr := stream.Info(ctx)
	if ierr != nil {
		stats.Error = &pb.Error{Message: ierr.Error()}
		return stats
	}
	stats.MessageCount = int64(info.State.Msgs)       //nolint:gosec
	stats.ConsumerCount = int32(info.State.Consumers) //nolint:gosec
	stats.LastOffset = int64(info.State.LastSeq)      //nolint:gosec
	// For a source with a durable consumer, report that consumer's actual
	// backlog (undelivered + delivered-but-unacked) instead of the stream
	// depth: the stream retains acked messages under its retention limits, so
	// its message count keeps growing after consumers are caught up. This
	// matches the amqp091 connector, which reports the queue's ready+unacked
	// count — the number consumer-scaling logic wants. Without a durable
	// consumer (or before it exists), fall back to the stream view.
	if durable := durableName(source); durable != "" {
		if cons, cerr := stream.Consumer(ctx, durable); cerr == nil {
			if ci, ierr := cons.Info(ctx); ierr == nil {
				stats.MessageCount = int64(ci.NumPending) + int64(ci.NumAckPending) //nolint:gosec
				stats.CurrentOffset = int64(ci.AckFloor.Stream)                     //nolint:gosec
			}
		}
	}
	// Note: publish_rate / deliver_rate are not exposed directly by
	// JetStream stream info; they would need to be derived from sampling. Left
	// at zero for now.
	return stats
}
