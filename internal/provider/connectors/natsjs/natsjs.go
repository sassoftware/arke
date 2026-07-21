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
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
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

// Error strings a client may match on, so they have to read identically
// whichever connector answers. Both take amqp091's wording: noMessageError is
// what it returns for an ack, nack, or dead-letter naming a message it does
// not hold, and offsetNotFoundError is the RabbitMQ stream client's answer
// when a stream has no offset to report, which its SourceStats passes through.
const (
	noMessageError          = "No message with uuid %s"
	offsetNotFoundError     = "Offset not found"
	unsupportedConfirmError = "Unsupported: Publish does not support publish confirmation"
	queueDedupError         = "Message deduplication is not supported by Queue types"
)

// streamMissingError answers a unary publish to a STREAM address whose stream
// was never declared. amqp091's wording there is stream-client plumbing
// ("failed to create a stream publisher"), so instead of copying it this takes
// the RabbitMQ stream client's canonical phrase; the contract a client relies
// on is that the publish errors rather than creating the stream.
const streamMissingError = "publish: stream %q does not exist"

const (
	// defaultAckWait is how long the server waits for an ack before
	// redelivering a message. See ackWait for the trade-off it sets;
	// override via NATSJS_ACK_WAIT.
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
	// ephemeralDeleteTimeout bounds the best-effort DeleteConsumer issued when
	// an ephemeral subscription ends; teardown must not hang on a broker that
	// is already gone.
	ephemeralDeleteTimeout = 5 * time.Second
	// defaultStreamMaxAge bounds how long a stream retains messages so the
	// JetStream log does not grow without bound (RabbitMQ deletes on ack;
	// JetStream LimitsPolicy does not). 72h is generous enough not to truncate a
	// realistic consumer-outage backlog, short enough to stop indefinite
	// accumulation. Override via NATSJS_STREAM_MAX_AGE.
	defaultStreamMaxAge = 72 * time.Hour
	// sacPriorityGroup is the priority group name used for single-active-
	// consumer sources. One group is enough: RabbitMQ's SAC has no notion of
	// multiple groups, and every subscriber of the durable joins this one.
	sacPriorityGroup = "arke"
	// defaultSACPinnedTTL is how long the server waits for a new pull request
	// from the pinned (active) client before unpinning and failing over to a
	// standby. See sacPinnedTTL; override via NATSJS_SAC_PINNED_TTL.
	defaultSACPinnedTTL = time.Minute
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

	// activeMessages maps an Arke message UUID -> an inflightMsg (the in-flight
	// jetstream.Msg plus the subscription that delivered it) so a later
	// Ack/Nack/Retry/DeadLetter RPC can resolve it and teardown can release
	// only the closing subscription's own deliveries.
	activeMessages *util.ConcurrentMap
	// knownStreams memoizes streams we have already ensured.
	knownStreams *util.ConcurrentMap
	// consumeContexts holds each live subscription's jetstream.ConsumeContext
	// so Disconnect can stop them and Stats can count them. Keyed per
	// subscription (source name plus a serial from subscriptionSeq), NOT per
	// source name: one connection may legitimately subscribe the same source
	// name more than once — single-active groups share a name and differ only
	// by ConsumerGroup — and a name key would make those overwrite each other,
	// undercounting Stats and letting one teardown drop the other's entry.
	consumeContexts *util.ConcurrentMap
	// subscriptionSeq disambiguates consumeContexts keys.
	subscriptionSeq atomic.Uint64
	// rates derives SourceStats publish/deliver rates from JetStream counters.
	rates *rateTracker

	// durableFilters guards competing consumers on one durable against applying
	// different proxy-side header filters. Each consumer evaluates only its own
	// Subscribe's filters (evaluateFilters in handleDelivery), so if two
	// subscribers of one durable disagree, a message JetStream hands to the
	// "wrong" one is filtered out and lost to the subscriber that wanted it.
	// Keyed by durable name; the entry records the header-filter fingerprint the
	// live subscribers share and how many hold it, so a conflicting declaration
	// is rejected instead of silently dropping messages. Subject filters are
	// excluded — they are the server's and update in place.
	durableFilters  map[string]*durableFilterUse
	durableFilterMu sync.Mutex

	state    atomic.Uint32
	consumed int64
	produced int64

	// clientDisconnect distinguishes a client-initiated Disconnect from the
	// NATS library closing the connection terminally (both read state CLOSED):
	// a disconnected client's Subscribe parks until its stream ends instead of
	// ending the stream. Set before the consume contexts are stopped, so any
	// Subscribe woken by that teardown observes it. Mirrors amqp091's flag of
	// the same name.
	clientDisconnect atomic.Bool
}

// durableFilterUse records the shared header-filter fingerprint of a durable's
// live subscribers and a reference count so the entry lives exactly as long as
// at least one subscriber holds it (see natsBrokerDetails.durableFilters).
type durableFilterUse struct {
	fingerprint string
	refs        int
}

// inflightMsg is a delivered-but-unresolved message together with the
// subscription (subKey) that delivered it. Recording the owner lets teardown
// release only the closing subscription's deliveries even when several
// subscriptions share one server-side consumer (competing consumers on a
// durable, single-active standbys) — otherwise closing one subscription would
// nak and drop a sibling's in-flight messages, failing the sibling's later
// ack and duplicating the message.
type inflightMsg struct {
	msg    jetstream.Msg
	subKey string
	// redeliverOnNack inverts Nack's terminate-by-default behaviour for this
	// message. It is set only by a failed DeadLetter, whose caller answers the
	// error with a nack that has to put the message back for another
	// dead-letter attempt rather than destroy it. See Nack.
	redeliverOnNack bool
}

type natsjsProvider struct {
	connections *util.ConcurrentMap
	// streams collapses concurrent stream-creation calls across connections.
	streams *streamRegistry
	// connectMu serializes the check-then-insert in Connect. The gRPC server's
	// own "already connected?" guard (brokerConnect) is not atomic, so two
	// Connect calls for one client identifier can both reach the provider;
	// without this, the second would overwrite the first's entry and orphan its
	// nats.Conn, which reconnects forever (MaxReconnects(-1)).
	connectMu sync.Mutex
	// bindMu serializes the read-modify-write of an address's
	// address-to-address binding set. See ensureStreamFor.
	bindMu sync.Mutex
}

// streamRegistry collapses concurrent ensureStream calls for the same stream
// into a single JetStream API call. When many clients (re)connect at once —
// e.g. after a broker or proxy restart — every connection would otherwise
// issue its own CreateOrUpdateStream for the same shared streams, piling
// redundant load on the JetStream metadata leader exactly when it is busiest.
// Entries are keyed by broker endpoint + credential identity + stream name
// (streamEnsureKey) so distinct brokers — or distinct accounts on one broker
// — never share results. Only in-flight calls are tracked here; success is
// memoized per connection (knownStreams), preserving the property that a
// fresh connection re-asserts its topology.
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

// streamEnsureKey identifies one stream-creation target for the registry:
// broker endpoint, credential identity, stream name. The credential part is
// what keeps two connections that share an endpoint but authenticate as
// different users — on a multi-account server, different accounts with
// disjoint JetStream state and permissions — from coalescing onto one
// CreateOrUpdateStream, which would run under whichever account got there
// first and hand its outcome (success in the wrong account, or that
// account's permission failure) to the other. The stream name goes last and
// can never contain '/' (see streamNameFor), so an embedded '/' in a
// username cannot make two distinct targets read alike.
func streamEnsureKey(cfg *pb.ConnectionConfiguration, streamName string) string {
	return fmt.Sprintf("%s:%d/%s/%s",
		cfg.GetHost(), cfg.GetPort(), cfg.GetCredentials().GetUsername(), streamName)
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

// ackWait is how long the server waits for a delivered message's ack before
// redelivering it. It sets one dial between two failure modes: a crashed
// client's in-flight messages are stuck for the full ack wait before another
// consumer gets them (shorter is better), while a healthy consumer that takes
// longer than the ack wait to process a message gets a duplicate redelivery
// (longer is better). The 30s default favors failover; deployments whose
// consumers legitimately hold messages longer — RabbitMQ's equivalent
// consumer timeout defaults to 30 minutes — should raise it via
// NATSJS_ACK_WAIT (Go duration). Note the pull buffer counts too: with a
// large prefetch a backlogged consumer can hold messages client-side longer
// than the ack wait just waiting their turn.
func ackWait() time.Duration {
	if v := os.Getenv("NATSJS_ACK_WAIT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return defaultAckWait
}

// sacPinnedTTL is the failover deadline for single-active-consumer sources:
// when the pinned (active) client sends no new pull request for this long,
// the server unpins it and the next standby's pull takes over. It must
// comfortably exceed the pull re-issue cadence (~30s with the client
// library's defaults) or the pin flaps between healthy standbys; lower it —
// together with faster pulls — only to shrink failover time. Configured via
// NATSJS_SAC_PINNED_TTL (Go duration).
func sacPinnedTTL() time.Duration {
	if v := os.Getenv("NATSJS_SAC_PINNED_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return defaultSACPinnedTTL
}

func (p *natsjsProvider) getBrokerDetails(ctx context.Context) (*natsBrokerDetails, error) {
	clientID, err := GetClientIdentifier(ctx)
	if err != nil {
		return nil, err
	}
	if bd := p.getBrokerDetailsByIdentifier(clientID); bd != nil {
		return bd, nil
	}
	// Error string matches the amqp091 connector: clients (and the
	// integration suite) observe it through the ack path after a disconnect.
	return nil, fmt.Errorf("could not retrieve broker details for this connection: %s", clientID)
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
		rates:            newRateTracker(),
		durableFilters:   make(map[string]*durableFilterUse),
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
	// Insert under connectMu so a racing Connect for the same client identifier
	// cannot both install a connection: the loser closes its freshly-dialed
	// nats.Conn instead of overwriting the winner's and leaking a background-
	// reconnecting link. A client already has a working connection in that case,
	// so this is a success (mirrors the server's "connected more than once").
	p.connectMu.Lock()
	if _, exists := p.connections.Get(clientID); exists {
		p.connectMu.Unlock()
		nc.Close()
		util.Logger.Debugf("natsjs: client %s already connected; closing the redundant link", clientID)
		return nil
	}
	p.connections.Add(clientID, bd)
	p.connectMu.Unlock()
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
	// Mark the connection closed before stopping its consume contexts:
	// stopping one wakes its Subscribe (Closed() fires), which reads the
	// state to tell deliberate teardown from a consumer lost on the server —
	// and the flag to tell a client's own Disconnect from a terminal close.
	bd.clientDisconnect.Store(true)
	bd.state.Store(provider.CLOSED)
	for _, name := range bd.consumeContexts.GetList() {
		if cc, ok := bd.consumeContexts.Get(name); ok {
			cc.(jetstream.ConsumeContext).Stop()
		}
	}
	if bd.nc != nil {
		_ = bd.nc.Drain()
	}
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
// ensureStream asserts the stream for a bare address name — one with no
// address-to-address binding to declare. See ensureStreamFor.
func (p *natsjsProvider) ensureStream(ctx context.Context, bd *natsBrokerDetails, addressName string) (string, error) {
	return p.ensureStreamFor(ctx, bd, &pb.Address{Name: addressName})
}

// ensureStreamFor asserts the stream backing an address, including the
// address-to-address binding its ParentAddress declares, if any.
//
// A binding from a parent address is a routing rule on a broker that routes;
// here, where the stream *is* the storage, the equivalent is to source the
// bound subjects out of the parent's stream into this one. The sourced copies
// keep the subject they were published under (no subject transform), so a
// message routed in from the parent is indistinguishable to a consumer from
// one published to this address directly, and only the parent's stream listens
// on the parent's subjects — no two streams ever claim the same subject space.
func (p *natsjsProvider) ensureStreamFor(ctx context.Context, bd *natsBrokerDetails, addr *pb.Address) (string, error) {
	streamName := streamNameFor(addr.GetName())
	bindings := parentBindingSubjects(addr)
	// The per-connection memo only remembers that the stream exists, which is
	// all an unbound address needs. A binding set is per-address state that
	// another subscriber may have extended since, so an address with a parent
	// re-asserts it every time; the assertion is idempotent, and addresses
	// with parents are rare enough for the extra round-trip not to matter.
	if len(bindings) == 0 {
		if _, ok := bd.knownStreams.Get(streamName); ok {
			return streamName, nil
		}
	}
	build := func() error {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), jsAPITimeout())
		defer cancel()
		cfg := jetstream.StreamConfig{
			Name:       streamName,
			Subjects:   streamSubjectsFor(addr.GetName()),
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
		}
		if len(bindings) > 0 {
			parentStream, perr := p.ensureStream(cctx, bd, addr.GetParentAddress().GetName())
			if perr != nil {
				return fmt.Errorf("ensure parent address stream: %w", perr)
			}
			sources, serr := p.boundSources(cctx, bd, streamName, parentStream, bindings)
			if serr != nil {
				return serr
			}
			cfg.Sources = sources
		}
		_, err := bd.js.CreateOrUpdateStream(cctx, cfg)
		return err
	}
	var err error
	if len(bindings) > 0 {
		// boundSources reads the current binding set to add to it, so the
		// read-modify-write has to be serialized or a concurrent subscriber
		// declaring a different binding would be overwritten. The streams
		// registry cannot do this job: it *collapses* concurrent callers, which
		// is right when they all want the same stream but would silently drop
		// one of two different binding sets.
		p.bindMu.Lock()
		err = build()
		p.bindMu.Unlock()
	} else {
		err = p.streams.ensure(ctx, streamEnsureKey(bd.connectionConfig, streamName), build)
	}
	if err != nil {
		return "", err
	}
	bd.knownStreams.Add(streamName, true)
	return streamName, nil
}

// lookupStream answers the stream name backing an address only if the stream
// already exists on the server; it never creates one. The unary publish path
// uses it for STREAM addresses, where declaring the stream is the reader's job
// (see PublishOne). A hit is memoized alongside ensureStreamFor's entries:
// either way the connection has established that the stream exists, which is
// all the memo records.
func (p *natsjsProvider) lookupStream(ctx context.Context, bd *natsBrokerDetails, addr *pb.Address) (string, error) {
	streamName := streamNameFor(addr.GetName())
	if _, ok := bd.knownStreams.Get(streamName); ok {
		return streamName, nil
	}
	lctx, cancel := context.WithTimeout(ctx, jsAPITimeout())
	defer cancel()
	if _, err := bd.js.Stream(lctx, streamName); err != nil {
		return "", err
	}
	bd.knownStreams.Add(streamName, true)
	return streamName, nil
}

// boundSources merges bindings into the stream's existing source set, so a
// second address-to-address binding adds to the first rather than replacing
// it. Bindings accumulate the same way on RabbitMQ, where declaring one never
// removes another; nothing here unbinds, so a binding outlives the
// subscription that declared it, exactly as an AMQP exchange-to-exchange
// binding outlives the client that bound it.
func (p *natsjsProvider) boundSources(ctx context.Context, bd *natsBrokerDetails,
	streamName, parentStream string, bindings []string) ([]*jetstream.StreamSource, error) {
	sources := []*jetstream.StreamSource{}
	if st, err := bd.js.Stream(ctx, streamName); err == nil {
		if info, ierr := st.Info(ctx); ierr == nil {
			sources = info.Config.Sources
		}
	} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, fmt.Errorf("read existing bindings: %w", err)
	}
	var from *jetstream.StreamSource
	for _, s := range sources {
		if s.Name == parentStream {
			from = s
			break
		}
	}
	if from == nil {
		from = &jetstream.StreamSource{Name: parentStream}
		sources = append(sources, from)
	}
	for _, subject := range bindings {
		if slices.ContainsFunc(from.SubjectTransforms, func(t jetstream.SubjectTransformConfig) bool {
			return t.Source == subject
		}) {
			continue
		}
		// Destination == Source: the copy keeps the subject it was published
		// under, which is what lets one consumer filter select both the
		// messages routed in from the parent and those published here directly.
		from.SubjectTransforms = append(from.SubjectTransforms,
			jetstream.SubjectTransformConfig{Source: subject, Destination: subject})
	}
	return sources, nil
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
			if msg.GetConfirm() {
				// The streaming publish is the fire-and-forget path; a client
				// that wants a broker confirmation calls PublishOne, which
				// returns one. amqp091 refuses the combination and so does
				// this: a JetStream publish is acknowledged either way, so
				// quietly accepting Confirm here would leave a client believing
				// the option means the same thing on both brokers when only one
				// of them honours it.
				errChan <- &pb.Error{Message: unsupportedConfirmError}
				continue
			}
			errChan <- p.publishMsg(ctx, bd, msg, false)
		}
	}
}

func (p *natsjsProvider) PublishOne(ctx context.Context, msg *pb.Message) *pb.Error {
	bd, err := p.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error(), IsFatal: true}
	}
	// A STREAM address must name a stream that already exists: amqp091 routes
	// unary stream publishes through the RabbitMQ Streams client, which
	// refuses a stream nobody declared (declaring is the reader's job —
	// streamSubscribe's DeclareStream). Auto-creating here instead would turn
	// a typo'd address name into a junk stream that swallows messages no
	// reader will ever see. Only this unary path checks: amqp091's streaming
	// Publish sends every address type, STREAM included, over an
	// auto-declared exchange and reports no error either, so the streaming
	// path keeps the auto-create.
	requireDeclared := msg.GetAddress().GetType() == pb.Address_STREAM
	return p.publishMsg(ctx, bd, msg, requireDeclared)
}

func (p *natsjsProvider) publishMsg(ctx context.Context, bd *natsBrokerDetails, msg *pb.Message, requireDeclared bool) *pb.Error {
	addr := msg.GetAddress()
	// Message-contract refusals come before any topology is touched: amqp091
	// refuses both of these before it reaches for the stream or exchange, so
	// a publish that is wrong twice over reports the contract error, not the
	// missing stream — and a refused publish creates nothing.
	if msg.GetPublishId() > 0 && addr.GetType() == pb.Address_QUEUE {
		// JetStream would happily deduplicate this, since dedup here is a
		// property of the stream rather than of the address type. Refusing it
		// anyway keeps the option meaning one thing across brokers: amqp091
		// cannot offer dedup on a queue address (RabbitMQ dedup comes from
		// streams), so a client allowed to set it here would be writing code
		// that silently loses its deduplication on RabbitMQ.
		return &pb.Error{Message: queueDedupError}
	}
	if msg.GetPublishId() > 0 && msg.GetPublisherName() == "" {
		return &pb.Error{Message: "PublisherName not set on message, PublisherName is required when PublishID is set"}
	}
	assertStream := p.ensureStreamFor
	if requireDeclared {
		assertStream = p.lookupStream
	}
	streamName, err := assertStream(ctx, bd, addr)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return &pb.Error{Message: fmt.Sprintf(streamMissingError, addr.GetName())}
	}
	if err != nil {
		return &pb.Error{Message: fmt.Sprintf("ensure stream: %s", err.Error())}
	}
	nmsg := &nats.Msg{
		Subject: publishSubjectFor(addr.GetName(), firstSubject(addr)),
		Data:    msg.GetBody(),
		Header:  pbToNatsHeader(msg.GetHeaders()),
	}
	var pubOpts []jetstream.PublishOpt
	// publish_id + publisher_name -> Nats-Msg-Id (dedup within the stream window).
	if msg.GetPublishId() > 0 {
		pubOpts = append(pubOpts, jetstream.WithMsgID(fmt.Sprintf("%s-%d", msg.GetPublisherName(), msg.GetPublishId())))
	}
	_, perr := bd.js.PublishMsg(ctx, nmsg, pubOpts...)
	if errors.Is(perr, jetstream.ErrNoStreamResponse) {
		// No stream captured the subject even though this connection ensured
		// one: the stream has been deleted underneath the connection (an
		// operator reset, a storage wipe). The memoized entry would pin that
		// stale answer for the connection's lifetime, failing every subsequent
		// publish — and a NATS client outlives broker state changes that would
		// sever an AMQP connection. Drop the entry, re-assert the stream, and
		// retry once; the first attempt was not stored (no stream answered),
		// so the retry cannot duplicate it. For a STREAM address the
		// re-assertion is the existence check again, so a stream deleted out
		// from under the connection is reported missing, not resurrected.
		bd.knownStreams.Delete(streamName)
		if _, rerr := assertStream(ctx, bd, addr); rerr == nil {
			_, perr = bd.js.PublishMsg(ctx, nmsg, pubOpts...)
		} else if errors.Is(rerr, jetstream.ErrStreamNotFound) {
			return &pb.Error{Message: fmt.Sprintf(streamMissingError, addr.GetName())}
		}
	}
	if perr != nil {
		return &pb.Error{Message: fmt.Sprintf("publish: %s", perr.Error())}
	}
	atomic.AddInt64(&bd.produced, 1)
	return nil
}

// Subscribe ensures topology, creates a consumer, and forwards deliveries to the
// message channel until the context is cancelled. Blocks per the Provider
// contract. It mirrors amqp091's queueSubscribe: one linear setup path with
// many small guards (hence the complexity nolint).
func (p *natsjsProvider) Subscribe(ctx context.Context, source *pb.Source, out chan<- *pb.Message) *pb.Error { //nolint:gocognit,gocyclo
	bd, err := p.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error(), IsFatal: true}
	}
	// MessageTTL is accepted so existing client sources validate against
	// SupportedSourceOptions, but retention here is stream-wide: warn
	// instead of silently ignoring it, so a source owner asking for a short
	// TTL learns their data outlives it without reading the design doc.
	if unapplied := unappliedSourceOptions(source); len(unapplied) > 0 {
		util.Logger.Warn(
			"natsjs: source {0} sets {1}, which natsjs accepts but does not apply; retention is stream-wide (NATSJS_STREAM_MAX_AGE / NATSJS_STREAM_MAX_BYTES)",
			source.GetName(), strings.Join(unapplied, ", "))
	}
	// Expires, by contrast, IS applied: it becomes the consumer's
	// InactiveThreshold (see below), so validate it up front before any
	// topology is created.
	expires, xerr := expiresThreshold(source)
	if xerr != nil {
		return &pb.Error{
			Message: fmt.Sprintf("source %q: %s", source.GetName(), xerr.Error()),
			IsFatal: true,
		}
	}
	// A single-active stream source must name the ConsumerGroup its instances
	// coordinate through (see durableName); amqp091 rejects this combination
	// with the same message. Without a group each subscriber would get its own
	// durable and they would all compete — the opposite of single-active.
	if source.GetSingleActiveConsumer() && source.GetType() == pb.Source_STREAM &&
		source.GetOptions()["ConsumerGroup"] == "" {
		return &pb.Error{
			Message: fmt.Sprintf("source %q: single active consumer requested but no ConsumerGroup option set", source.GetName()),
			IsFatal: true,
		}
	}
	addr := source.GetAddress()
	streamName, serr := p.ensureStreamFor(ctx, bd, addr)
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
	if errors.Is(serr, jetstream.ErrStreamNotFound) {
		// Same stale-memo case as the publish path: the stream was deleted out
		// from under the connection after this connection ensured it. Drop the
		// entry and re-assert the stream instead of failing the subscribe until
		// the client reconnects.
		bd.knownStreams.Delete(streamName)
		if _, rerr := p.ensureStreamFor(ctx, bd, addr); rerr == nil {
			stream, serr = bd.js.Stream(tctx, streamName)
		}
	}
	if serr != nil {
		return &pb.Error{Message: fmt.Sprintf("open stream: %s", serr.Error()), IsFatal: true}
	}

	// AMQP prefetch 0 means unlimited (amqp091 leaves the channel default in
	// that case rather than calling SetPrefetch), so map it to JetStream's
	// explicit unlimited (-1) — not to the server default of 1000, and
	// certainly not to 1, which would silently serialize the consumer. Note
	// that Arke's gRPC server raises a prefetch below 1 to 1 before any
	// provider sees the source (SetSourceDefaults, internal/server), so a
	// subscribe arriving through the server never takes this branch; it keeps
	// the provider contract honest for direct (in-process) users and for any
	// future change to the server-side defaulting.
	prefetch := int(source.GetPrefetchCount())
	if prefetch <= 0 {
		prefetch = -1
	}
	deliverPolicy, startSeq, derr := deliverPolicyFor(source)
	if derr != nil {
		return &pb.Error{Message: fmt.Sprintf("source %q: %s", source.GetName(), derr.Error()), IsFatal: true}
	}
	consCfg := jetstream.ConsumerConfig{
		FilterSubjects: filterSubjectsFor(source),
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  deliverPolicy,
		OptStartSeq:    startSeq, // 0 (ignored) unless DeliverByStartSequence
		AckWait:        ackWait(),
		MaxAckPending:  prefetch,
	}
	// Durable work-queue consumer for non-transient QUEUE / stream-group sources:
	// it persists across client reconnects so a backlog published during a
	// consumer outage is redelivered (RabbitMQ durable-queue parity). The
	// DeliverPolicy only applies on first creation; reconnects resume from the
	// durable's stored ack position. Transient (auto-delete/exclusive/TEMPORARY)
	// sources stay ephemeral and auto-expire after InactiveThreshold.
	//
	// The Expires option (AMQP x-expires: delete the queue after this many ms
	// without consumers) maps onto InactiveThreshold for BOTH kinds — the
	// broker deletes the consumer once nothing has pulled from it for the
	// threshold, exactly the disuse-deletion x-expires asks for. Only the
	// consumer is deleted; the messages stay in the shared per-address stream
	// under its own retention (see Known limitations in the design doc).
	// Unset keeps the amqp091 defaults: transient sources expire after
	// defaultInactiveThreshold (amqp091 defaults auto-delete/exclusive queues
	// to the same 5 minutes), durables never expire.
	consCfg.InactiveThreshold = expires
	if durable := durableName(source); durable != "" {
		consCfg.Durable = durable
		if source.GetSingleActiveConsumer() {
			// RabbitMQ's single-active-consumer delivers to one consumer at a
			// time — the point is ordered processing across competing
			// instances — and fails over when that consumer goes away. The
			// JetStream equivalent is a pinned-client priority group
			// (nats-server 2.11+): the server pins the first client to pull
			// and standbys' pulls wait; when the pinned client stops pulling
			// for PinnedTTL (or its pin is otherwise released) the next
			// standby takes over.
			consCfg.PriorityPolicy = jetstream.PriorityPolicyPinned
			consCfg.PriorityGroups = []string{sacPriorityGroup}
			consCfg.PinnedTTL = sacPinnedTTL()
		}
	} else if expires == 0 {
		consCfg.InactiveThreshold = defaultInactiveThreshold
	}

	// Competing consumers on one durable each apply their own proxy-side header
	// filter, so reject a second live subscriber whose header filter differs
	// rather than silently dropping the messages JetStream routes to the
	// "wrong" one (see natsBrokerDetails.durableFilters). Claimed before the
	// consumer is touched and held for the life of this Subscribe; the defer
	// covers every return path below.
	if consCfg.Durable != "" {
		if err := bd.claimDurableFilter(consCfg.Durable, headerFilterFingerprint(source.GetFilters())); err != nil {
			return &pb.Error{Message: fmt.Sprintf("source %q: %s", source.GetName(), err.Error()), IsFatal: true}
		}
		defer bd.releaseDurableFilter(consCfg.Durable)
	}

	cons, cerr := stream.CreateOrUpdateConsumer(tctx, consCfg)
	if cerr != nil && consCfg.Durable != "" && isStartPositionConflict(cerr) {
		// A durable's start position (DeliverPolicy/OptStartSeq) is fixed at
		// creation — JetStream rejects updates to it (err 10012), so a client
		// re-subscribing with a different Offset would otherwise fail here
		// forever. The documented contract is that Offset applies on first
		// creation only and a reconnecting durable resumes from its stored ack
		// position, so attach to the existing consumer as-is; to reposition,
		// use a new durable (source/ConsumerGroup) name. Only start-position
		// conflicts are absorbed this way: any other configuration error stays
		// fatal, because attaching to a consumer whose effective config
		// silently differs from the requested one (wrong ack policy, stale
		// filter subjects) would consume the wrong way without any signal.
		if existing, aerr := stream.Consumer(tctx, consCfg.Durable); aerr == nil {
			util.Logger.Warn(
				"natsjs: durable consumer {0} exists with a different start position ({1}); resuming it unchanged",
				consCfg.Durable, cerr.Error())
			cons, cerr = existing, nil
		}
	}
	if cerr != nil {
		// Include the mapped filter subjects: a NATS "invalid subject" (10052)
		// otherwise gives no hint which source/subject produced it.
		return &pb.Error{
			Message: fmt.Sprintf("create consumer (filters=%v): %s", consCfg.FilterSubjects, cerr.Error()),
			IsFatal: true,
		}
	}

	// DeclareOnly: topology established, do not consume. An ephemeral consumer
	// created only to validate that topology is garbage the moment we return —
	// its server-generated name is never seen again — so delete it eagerly
	// rather than letting it linger for the full inactivity threshold.
	if source.GetDeclareOnly() {
		if consCfg.Durable == "" {
			deleteEphemeralConsumer(ctx, stream, cons.CachedInfo().Name)
		}
		return nil
	}

	// consumerLost carries the first authoritative sign that this
	// subscription's server-side consumer no longer exists (deleted
	// administratively, or expired after an outage longer than the ephemeral
	// inactivity threshold). The final wait below turns it into a
	// subscription-ending error so the client's re-subscribe recreates the
	// consumer — without it the subscription would sit deaf forever, warning
	// (or, after a terminal consume error, silently) while delivering
	// nothing. RabbitMQ parity: a deleted queue closes its consumers'
	// channel, and the re-subscribe re-declares it.
	consumerName := cons.CachedInfo().Name
	consumerLost := make(chan error, 1)
	noteLost := func(err error) {
		select {
		case consumerLost <- err:
		default:
		}
	}
	// probeConsumer asks the server whether the consumer still exists. A
	// consumer that vanished while no pull was pending (expiry during a long
	// outage) never produces an authoritative error on the consume path —
	// re-issued pulls just find no responder and heartbeats go missing,
	// indefinitely — so those symptoms trigger this bounded, single-flight
	// lookup instead. Only an explicit "not found" answer counts (for the
	// consumer, or for the whole stream — a wiped store takes both): a
	// timeout or network failure proves nothing about the consumer.
	var probing atomic.Bool
	probeConsumer := func() {
		if bd.state.Load() != provider.CONNECTED || !probing.CompareAndSwap(false, true) {
			return
		}
		go func() {
			defer probing.Store(false)
			pctx, pcancel := context.WithTimeout(context.WithoutCancel(ctx), ephemeralDeleteTimeout)
			defer pcancel()
			if _, perr := stream.Consumer(pctx, consumerName); errors.Is(perr, jetstream.ErrConsumerNotFound) ||
				errors.Is(perr, jetstream.ErrStreamNotFound) {
				noteLost(perr)
			}
		}()
	}
	consumeOpts := []jetstream.PullConsumeOpt{
		// A short idle heartbeat plus an error handler keep a stalled delivery
		// path from failing silently: if the server stops serving this
		// consumer's pulls (broker restart, consumer raft leader not yet
		// serving after creation), the missed heartbeat is logged at warn and
		// the library re-issues its pull request after ~2x the heartbeat,
		// instead of ~30s — and invisibly — with the defaults. Standby pulls
		// of a pinned consumer receive idle heartbeats too, so single-active
		// standbys do not trip this.
		jetstream.PullHeartbeat(defaultConsumeHeartbeat),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			util.Logger.Warn("natsjs: consume error on source {0}: {1}", source.GetName(), err.Error())
			// The deleted/not-found errors are authoritative; anything else
			// (missed heartbeats, pulls with no responder) is only a symptom
			// that warrants checking.
			if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) {
				noteLost(err)
				return
			}
			probeConsumer()
		}),
	}
	// Pull requests must carry the priority group exactly when the consumer
	// has one, so key off the effective (server-reported) config, not the
	// requested one: a server too old for priority groups (< 2.11) silently
	// drops the fields, and an existing durable the subscribe merely attached
	// to may predate them. In either case single-active cannot be enforced —
	// consumers compete, today's behavior — and that degradation must be
	// visible, not silent.
	if groups := cons.CachedInfo().Config.PriorityGroups; len(groups) > 0 {
		consumeOpts = append(consumeOpts, jetstream.PullPriorityGroup(groups[0]))
	} else if consCfg.PriorityPolicy == jetstream.PriorityPolicyPinned {
		util.Logger.Warn(
			"natsjs: source {0} requested a single active consumer but the consumer has no priority group "+
				"(broker predates them, or an existing durable could not be updated); consumers will compete",
			source.GetName())
	}
	// subKey tags this subscription: it keys its consume context and stamps the
	// messages it delivers (inflightMsg), so teardown releases only its own
	// deliveries even when it shares a server-side consumer with siblings.
	// Generated before Consume so the delivery callback can capture it.
	subKey := fmt.Sprintf("%s#%d", source.GetName(), bd.subscriptionSeq.Add(1))
	cc, cerr := cons.Consume(func(m jetstream.Msg) {
		p.handleDelivery(ctx, bd, source, subKey, m, out)
	}, consumeOpts...)
	if cerr != nil {
		if consCfg.Durable == "" {
			deleteEphemeralConsumer(ctx, stream, consumerName)
		}
		return &pb.Error{Message: fmt.Sprintf("consume: %s", cerr.Error()), IsFatal: true}
	}
	bd.consumeContexts.Add(subKey, cc)
	defer func() {
		cc.Stop()
		// Stop is asynchronous: wait (bounded) for the consume loop to finish
		// unsubscribing, then flush so the server has processed the
		// unsubscribe before the naks below — otherwise a nak'd message can
		// be redelivered straight into this subscription's still-registered
		// pull request and sit unclaimed until AckWait expires it again.
		select {
		case <-cc.Closed():
		case <-time.After(ephemeralDeleteTimeout):
		}
		_ = bd.nc.FlushTimeout(ephemeralDeleteTimeout)
		bd.consumeContexts.Delete(subKey)
		releaseInFlight(bd, subKey, consCfg.Durable != "")
		if consCfg.Durable == "" {
			deleteEphemeralConsumer(ctx, stream, consumerName)
		}
	}()

	select {
	case <-ctx.Done():
		return nil
	case lerr := <-consumerLost:
		return consumerLostError(source, consumerName, lerr)
	case <-cc.Closed():
		// The consume machinery stopped on its own: the library treats a
		// handful of consume errors as terminal ("consumer deleted" when the
		// server removes the consumer under a pending pull) and silently
		// stops the ConsumeContext. Blocking on ctx here regardless would
		// leave a subscription that never delivers again. Closed() also
		// fires for this connection's own teardown (Disconnect stops every
		// consume context, Drain closes the subscriptions), which is not an
		// error — the context or connection state says which case this is.
		if bd.clientDisconnect.Load() && ctx.Err() == nil {
			// Client-initiated Disconnect with the consume stream still
			// open: do not end the subscription — returning here would end
			// the caller's whole consume stream, but the amqp091 connector
			// leaves it open (its subscribe loop just goes quiet once the
			// AMQP connection closes), so a client that disconnects and
			// then acks its in-flight messages gets each ack answered with
			// a "could not retrieve broker details" failure rather than
			// end-of-stream. Delivery cannot resume — the consume context
			// is stopped and the connection is draining — so just wait for
			// the client to close its stream. Scoped to the client's own
			// Disconnect (not any CLOSED state): a connection the library
			// closed terminally still ends the subscription.
			<-ctx.Done()
			return nil
		}
		if ctx.Err() != nil || bd.state.Load() == provider.CLOSED {
			return nil
		}
		select {
		case lerr := <-consumerLost:
			return consumerLostError(source, consumerName, lerr)
		default:
		}
		return &pb.Error{Message: fmt.Sprintf(
			"source %q: consuming stopped unexpectedly (consumer %s); re-subscribe to recreate the consumer",
			source.GetName(), consumerName)}
	}
}

// consumerLostError is the non-fatal error Subscribe ends with when the
// server-side consumer disappeared: non-fatal so the client's re-subscribe
// path runs Subscribe again, which recreates the consumer (and, after a
// storage wipe, the stream). Matches the amqp091 connector, which ends a
// subscription with a non-fatal error when the broker closes the channel of
// a deleted queue.
func consumerLostError(source *pb.Source, consumerName string, err error) *pb.Error {
	return &pb.Error{Message: fmt.Sprintf(
		"source %q: server-side consumer %s no longer exists (%s); re-subscribe to recreate it",
		source.GetName(), consumerName, err.Error())}
}

// releaseInFlight drops a just-ended subscription's unresolved deliveries from
// activeMessages, identified by the owning subKey. Acks travel on the consume
// stream that closed, so nothing can resolve these uuids anymore; left alone
// they would sit in the map for the life of the connection (a leak that also
// inflates the active-message stat). Durable consumers additionally get a
// best-effort Nak so the messages redeliver promptly — RabbitMQ requeues a
// closed channel's unacked deliveries immediately, whereas an untouched
// JetStream delivery waits out the full AckWait. Ephemeral consumers skip the
// Nak: the consumer is deleted on this same teardown path, which discards its
// delivery state anyway.
//
// Ownership is by subKey, not by the (stream, consumer) pair the delivery
// carries: several subscriptions can share one server-side consumer (competing
// consumers on a durable, single-active standbys), so filtering on the
// consumer would release a sibling's still-in-flight messages — failing the
// sibling's later ack and duplicating the message. A message the owner
// resolves concurrently with teardown is deleted by whichever side gets there
// first; the loser's ack/nak fails and is ignored (at-least-once either way).
func releaseInFlight(bd *natsBrokerDetails, subKey string, nak bool) {
	for _, uuid := range bd.activeMessages.GetList() {
		mu, ok := bd.activeMessages.Get(uuid)
		if !ok {
			continue
		}
		im := mu.(inflightMsg)
		if im.subKey != subKey {
			continue
		}
		bd.activeMessages.Delete(uuid)
		if !nak {
			continue
		}
		if err := im.msg.Nak(); err != nil {
			util.Logger.Debugf("natsjs: nak of in-flight message %s at subscription end: %s", uuid, err.Error())
		}
	}
}

// claimDurableFilter registers this subscription's header-filter fingerprint
// against a durable. The first subscriber sets the fingerprint; later
// subscribers must match it (they share the server-side consumer, so their
// proxy-side header filters must agree or one drops the other's messages —
// see natsBrokerDetails.durableFilters). A mismatch is refused; a match takes
// a reference. Every successful claim must be paired with releaseDurableFilter.
func (bd *natsBrokerDetails) claimDurableFilter(durable, fingerprint string) error {
	bd.durableFilterMu.Lock()
	defer bd.durableFilterMu.Unlock()
	if use, ok := bd.durableFilters[durable]; ok {
		if use.fingerprint != fingerprint {
			return fmt.Errorf(
				"durable consumer %q already has a live subscriber with a different header filter; "+
					"competing consumers on one durable must share their header filters", durable)
		}
		use.refs++
		return nil
	}
	bd.durableFilters[durable] = &durableFilterUse{fingerprint: fingerprint, refs: 1}
	return nil
}

// releaseDurableFilter drops one reference taken by claimDurableFilter,
// forgetting the durable's fingerprint once the last live subscriber leaves so
// a later re-subscribe may legitimately declare a new filter.
func (bd *natsBrokerDetails) releaseDurableFilter(durable string) {
	bd.durableFilterMu.Lock()
	defer bd.durableFilterMu.Unlock()
	if use, ok := bd.durableFilters[durable]; ok {
		use.refs--
		if use.refs <= 0 {
			delete(bd.durableFilters, durable)
		}
	}
}

// headerFilterFingerprint canonically (order-independently) serializes a
// source's proxy-side header filters so two subscriptions sharing a durable can
// be compared. Only the header filters (evaluateFilters) are fingerprinted;
// subject filters are the server's and update the shared consumer in place.
func headerFilterFingerprint(filters []*pb.Filter) string {
	if len(filters) == 0 {
		return ""
	}
	parts := make([]string, 0, len(filters))
	for _, f := range filters {
		matches := make([]string, 0, len(f.GetMatches()))
		for _, m := range f.GetMatches() {
			matches = append(matches, fmt.Sprintf("%q=%q", m.GetName(), m.GetValue()))
		}
		sort.Strings(matches)
		parts = append(parts, fmt.Sprintf("t%d[%s]", f.GetType(), strings.Join(matches, ",")))
	}
	sort.Strings(parts)
	return strings.Join(parts, ";")
}

// deleteEphemeralConsumer removes an ephemeral consumer as soon as its
// subscription ends. Ephemeral consumers do expire on their own after
// InactiveThreshold, but until then each one still counts against the server's
// per-stream consumer limit, so clients that churn transient subscriptions
// faster than the threshold would accumulate dead consumers on the server.
// Deleting eagerly leaves the threshold as a janitor for unclean exits (a
// crashed client never reaches this path) only. Best-effort: the subscription
// context is already cancelled when this runs, so the call gets a detached,
// bounded context, and a failure is only logged — the consumer then simply
// expires via the threshold as before.
func deleteEphemeralConsumer(ctx context.Context, stream jetstream.Stream, name string) {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ephemeralDeleteTimeout)
	defer cancel()
	if err := stream.DeleteConsumer(dctx, name); err != nil {
		util.Logger.Debugf("natsjs: delete ephemeral consumer %s: %s", name, err.Error())
	}
}

// unappliedSourceOptions returns the retention options the source sets that
// natsjs accepts (so sources written for the amqp091 connector still validate)
// but does not apply — retention on natsjs is stream-wide (see streamMaxAge
// and the design doc's Known limitations). Expires is NOT in this list: it
// maps onto the consumer's InactiveThreshold (see expiresThreshold).
func unappliedSourceOptions(source *pb.Source) []string {
	var unapplied []string
	for _, opt := range []string{"MessageTTL"} {
		if source.GetOptions()[opt] != "" {
			unapplied = append(unapplied, opt)
		}
	}
	return unapplied
}

// expiresThreshold parses the Expires source option — AMQP x-expires, the
// number of milliseconds a queue may go unused before the broker deletes it —
// into the consumer InactiveThreshold that implements the same disuse
// deletion here. Zero (with nil error) means the option is unset and the
// caller applies its kind's default. The error strings mirror the amqp091
// connector, which validates the same option the same way; the broker itself
// rejects a non-positive x-expires, so that case is refused here too rather
// than passed through (a zero threshold would mean "never expire" for a
// durable but "server default" for an ephemeral — neither is what the client
// asked for).
func expiresThreshold(source *pb.Source) (time.Duration, error) {
	raw := source.GetOptions()["Expires"]
	if raw == "" {
		return 0, nil
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New("value for Expires option must be a quoted integer")
	}
	if val <= 0 {
		return 0, errors.New("value for Expires option must be a positive integer")
	}
	return time.Duration(val) * time.Millisecond, nil
}

// deliverPolicyFor maps the RabbitMQ-Streams `Offset` option onto a JetStream
// DeliverPolicy, mirroring the amqp091 connector's toStreamOffset so both
// connectors accept the same offset vocabulary. The second return value is the
// start sequence, meaningful only when the policy is DeliverByStartSequence.
//
// Mapping: `first`/`continue` -> deliver all (a durable "continue"s from its
// stored ack floor on reconnect regardless, so all-from-start is the correct
// first-creation fallback); `last` -> the final message; `next`/"" -> only
// messages published after the consumer is created; an absolute number ->
// start at that stream sequence.
//
// NOTE: a numeric offset is an offset in the Arke/RabbitMQ-Streams sense —
// 0 names the first message in the log — not a raw JetStream sequence, so it
// round-trips through SourceStats (see offsetOf/seqOf). It still is not
// portable *across* brokers: offset 7 names the eighth message of whichever
// log is being read, and two brokers' logs hold different messages. An
// unrecognized offset is an error, like toStreamOffset: starting a consumer at
// a silently different position than the one it asked for loses or replays
// data.
func deliverPolicyFor(source *pb.Source) (jetstream.DeliverPolicy, uint64, error) {
	off := source.GetOptions()["Offset"]
	switch strings.ToLower(off) {
	case "first", "continue":
		return jetstream.DeliverAllPolicy, 0, nil
	case "last":
		return jetstream.DeliverLastPolicy, 0, nil
	case "next", "":
		return jetstream.DeliverNewPolicy, 0, nil
	default:
		if offset, err := strconv.ParseUint(strings.TrimSpace(off), 10, 64); err == nil {
			if offset == 0 {
				return jetstream.DeliverAllPolicy, 0, nil
			}
			return jetstream.DeliverByStartSequencePolicy, seqOf(offset), nil
		}
		return jetstream.DeliverNewPolicy, 0, fmt.Errorf(
			"invalid offset: %q (expected first, continue, last, next, or a numeric stream sequence)", off)
	}
}

// startPositionConflictMessages are the server's refusals to change a durable
// consumer's start position, which JetStream fixes at creation. They arrive
// wrapped in the generic consumer-create API error (err 10012) whose
// description carries the specific refusal, so the description is what
// distinguishes an immutable start position from every other create or update
// failure sharing that code.
var startPositionConflictMessages = []string{
	"deliver policy can not be updated",
	"start sequence can not be updated",
	"start time can not be updated",
}

// isStartPositionConflict reports whether err is JetStream refusing to move an
// existing durable consumer's start position (DeliverPolicy / OptStartSeq /
// OptStartTime) — the one configuration conflict Subscribe absorbs by
// attaching to the existing consumer unchanged.
func isStartPositionConflict(err error) bool {
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode != jetstream.JSErrCodeConsumerCreate {
		return false
	}
	for _, m := range startPositionConflictMessages {
		if strings.Contains(apiErr.Description, m) {
			return true
		}
	}
	return false
}

func (p *natsjsProvider) handleDelivery(ctx context.Context, bd *natsBrokerDetails, source *pb.Source, subKey string, m jetstream.Msg, out chan<- *pb.Message) {
	defer util.RecoverPanic()

	headers := natsToPbHeader(m)
	// Synthesize x-retry-count from JetStream's delivery count so a client's retry
	// policy works without RabbitMQ's x-death header.
	if md, err := m.Metadata(); err == nil && md.NumDelivered > 1 {
		headers[retryCountHeaderName] = strconv.FormatUint(md.NumDelivered-1, 10)
	}

	// Proxy-side replacement for RabbitMQ headers-exchange routing. A failed
	// ack here only means the message will be redelivered and re-filtered —
	// harmless, but it inflates NumDelivered (and so the synthesized
	// x-retry-count) of a message the client never saw, so leave a trace.
	if !evaluateFilters(source.GetFilters(), headers) {
		if err := m.Ack(); err != nil {
			util.Logger.Debugf("natsjs: ack of header-filtered message on source %s failed: %s",
				source.GetName(), err.Error())
		}
		return
	}

	uuid := util.GenUUID()
	bd.activeMessages.Add(uuid, inflightMsg{msg: m, subKey: subKey})
	pbmsg := &pb.Message{Uuid: uuid, Headers: headers, Body: m.Data()}

	select {
	case out <- pbmsg:
		atomic.AddInt64(&bd.consumed, 1)
	case <-ctx.Done():
		// The subscription ended before the server took the message: drop the
		// claim and nak (best-effort) so a durable redelivers it promptly
		// instead of after AckWait. Harmless for ephemerals, whose consumer is
		// deleted on teardown.
		bd.activeMessages.Delete(uuid)
		_ = m.Nak()
	}
}

func (p *natsjsProvider) takeMessage(ctx context.Context, uuid string) (jetstream.Msg, *pb.Error) {
	bd, err := p.getBrokerDetails(ctx)
	if err != nil {
		return nil, &pb.Error{Message: err.Error()}
	}
	mu, ok := bd.activeMessages.Get(uuid)
	if !ok {
		return nil, &pb.Error{Message: fmt.Sprintf(noMessageError, uuid)}
	}
	bd.activeMessages.Delete(uuid)
	return mu.(inflightMsg).msg, nil
}

// takeMessageForNack is takeMessage plus the message's redeliverOnNack flag,
// which only Nack needs.
func (p *natsjsProvider) takeMessageForNack(ctx context.Context, uuid string) (jetstream.Msg, bool, *pb.Error) {
	bd, err := p.getBrokerDetails(ctx)
	if err != nil {
		return nil, false, &pb.Error{Message: err.Error()}
	}
	mu, ok := bd.activeMessages.Get(uuid)
	if !ok {
		return nil, false, &pb.Error{Message: fmt.Sprintf(noMessageError, uuid)}
	}
	bd.activeMessages.Delete(uuid)
	im := mu.(inflightMsg)
	return im.msg, im.redeliverOnNack, nil
}

// markForRedelivery records that a nack of this message must put it back
// rather than terminate it. Called on every path where DeadLetter gives up
// with the message still in flight: the error it returns makes the server nack
// the same uuid, and that nack is the retry, not a rejection.
func markForRedelivery(bd *natsBrokerDetails, uuid string) {
	mu, ok := bd.activeMessages.Get(uuid)
	if !ok {
		return
	}
	im := mu.(inflightMsg)
	im.redeliverOnNack = true
	bd.activeMessages.Add(uuid, im)
}

func (p *natsjsProvider) Ack(ctx context.Context, uuid string) *pb.Error {
	m, perr := p.takeMessage(ctx, uuid)
	if perr != nil {
		return perr
	}
	if err := m.Ack(); err != nil {
		return &pb.Error{Message: err.Error()}
	}
	return nil
}

// Nack rejects a message without putting it back, which is what a nack means
// on the Arke contract: amqp091 answers the same call with
// Delivery.Nack(requeue=false), so RabbitMQ drops the message (or moves it to
// the queue's dead-letter exchange, which the server instead asks for
// explicitly through DeadLetter). JetStream's Nak means the opposite —
// redeliver immediately — so naking here turns one nacked message into an
// unbounded redelivery loop between server and client, at thousands of
// deliveries a second, for as long as the subscription lives. Term is the
// primitive that carries the intended meaning: stop redelivering.
//
// The exception is the server's fallback nack after a failed DeadLetter, which
// exists to put the message back so dead-lettering can be retried. DeadLetter
// marks the message for redelivery before returning that error.
func (p *natsjsProvider) Nack(ctx context.Context, uuid string) *pb.Error {
	m, redeliver, perr := p.takeMessageForNack(ctx, uuid)
	if perr != nil {
		return perr
	}
	if redeliver {
		if err := m.Nak(); err != nil {
			return &pb.Error{Message: err.Error()}
		}
		return nil
	}
	if err := m.Term(); err != nil {
		return &pb.Error{Message: err.Error()}
	}
	return nil
}

// Retry requeues the message after a delay. NakWithDelay is the native
// JetStream primitive that replaces RabbitMQ's per-message-TTL + dead-letter
// retry-queue idiom; JetStream increments the delivery count for us, which
// handleDelivery surfaces as x-retry-count.
func (p *natsjsProvider) Retry(ctx context.Context, _ *pb.Source, uuid string, delay int32) *pb.Error {
	m, perr := p.takeMessage(ctx, uuid)
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
//
// RabbitMQ moves a nacked message to its DLX broker-side; here the move is two
// proxy-side steps, so ordering is what protects the data: the original is
// Term'd only after the DLQ publish succeeds. If the DLQ stream cannot be
// ensured or published to, the message is left in flight — still ack-pending on
// the server and still resolvable by uuid — and an error is returned, so the
// caller's fallback (the server nacks on a DeadLetter error) redelivers it and
// dead-lettering is retried, instead of the message being removed from
// redelivery while also absent from the DLQ. The DLQ publish carries a
// Nats-Msg-Id derived from the original's stream sequence, so a retried
// dead-letter of the same message cannot duplicate it in the DLQ within the
// dedup window, and the x-retry-count the consumer saw when it gave the
// message up (the proxy-side analogue of the x-death trail RabbitMQ's
// broker-side move preserves).
func (p *natsjsProvider) DeadLetter(ctx context.Context, source *pb.Source, uuid string) *pb.Error {
	bd, err := p.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	mu, ok := bd.activeMessages.Get(uuid)
	if !ok {
		return &pb.Error{Message: fmt.Sprintf(noMessageError, uuid)}
	}
	m := mu.(inflightMsg).msg
	// Every way out of this function short of a stored DLQ copy leaves the
	// message in flight for the caller's fallback nack to retry, so make that
	// nack mean "put it back" for as long as the attempt is unresolved. The
	// success path removes the message from activeMessages outright, so the
	// flag cannot outlive a dead-letter that worked.
	markForRedelivery(bd, uuid)
	opts := source.GetOptions()
	dla, hasDLA := opts["DeadLetterAddress"]
	if !hasDLA {
		return &pb.Error{Message: "DeadLetterAddress not set"}
	}
	if dla == "" {
		return &pb.Error{Message: "DeadLetterAddress is empty"}
	}
	if _, serr := p.ensureStream(ctx, bd, dla); serr != nil {
		util.Logger.Warn(
			"natsjs: dead-letter of message {0} failed (ensure stream for {1}: {2}); message stays in flight",
			uuid, dla, serr.Error())
		return &pb.Error{Message: fmt.Sprintf("dead-letter ensure stream: %s", serr.Error())}
	}
	// Copied so neither the retry count below nor the publish options
	// mutate the original's header map.
	dlqHeader := copyHeader(m.Headers())
	var pubOpts []jetstream.PublishOpt
	if md, merr := m.Metadata(); merr == nil {
		pubOpts = append(pubOpts,
			jetstream.WithMsgID(fmt.Sprintf("dlq-%s-%d", md.Stream, md.Sequence.Stream)))
		// RabbitMQ stamps a dead-lettered copy broker-side (x-death) with
		// how it died; the JetStream copy is a plain publish that would
		// otherwise lose that trail. Carry the same retry count the
		// consumer saw when it gave the message up, synthesized exactly
		// like handleDelivery does for deliveries.
		if md.NumDelivered > 1 {
			if dlqHeader == nil {
				dlqHeader = nats.Header{}
			}
			dlqHeader.Set(retryCountHeaderName, strconv.FormatUint(md.NumDelivered-1, 10))
		}
	}
	// RabbitMQ dead-letters a message under its original routing key
	// unless the queue overrides it (x-dead-letter-routing-key, which is
	// what DeadLetterSubject maps to on the AMQP side). Preserve the
	// original key the same way when no override is set, so DLQ consumers
	// can bind by routing key and reprocessing tools can see where each
	// message was originally headed.
	dlSubject := opts["DeadLetterSubject"]
	if dlSubject == "" {
		dlSubject = routingKeyFromSubject(source.GetAddress().GetName(), m.Subject())
	}
	dlqMsg := &nats.Msg{
		Subject: publishSubjectFor(dla, dlSubject),
		Data:    m.Data(),
		Header:  dlqHeader,
	}
	if _, perr := bd.js.PublishMsg(ctx, dlqMsg, pubOpts...); perr != nil {
		if errors.Is(perr, jetstream.ErrNoStreamResponse) {
			// The DLQ stream was deleted after this connection ensured it.
			// Drop the memoized entry so the dead-letter retry that follows
			// the returned error (the server nacks, the message redelivers)
			// re-creates the stream instead of failing the same way forever.
			bd.knownStreams.Delete(streamNameFor(dla))
		}
		util.Logger.Warn(
			"natsjs: dead-letter of message {0} failed (publish to {1}: {2}); message stays in flight",
			uuid, dlqMsg.Subject, perr.Error())
		return &pb.Error{Message: fmt.Sprintf("dead-letter publish: %s", perr.Error())}
	}
	// Term only after the DLQ copy is safely stored, and drop the active-message
	// entry only after Term succeeds: if Term fails, DeadLetter returns an error
	// and the server falls back to nacking this uuid (server.go), which can only
	// resolve while the entry is still present. Deleting first would strand the
	// original ack-pending until AckWait instead of redelivering promptly.
	if err := m.Term(); err != nil {
		return &pb.Error{Message: err.Error()}
	}
	bd.activeMessages.Delete(uuid)
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

// rateTracker derives messages-per-second rates from the monotonic counters
// JetStream exposes. RabbitMQ's management API computes publish/deliver rates
// server-side; JetStream reports only absolute counters, so the connector
// differences them between successive SourceStats calls. The first
// observation of a key establishes a baseline and reports zero, as does a
// counter that moved backwards (a recreated stream or consumer).
type rateTracker struct {
	mu      sync.Mutex
	samples map[string]rateSample
}

type rateSample struct {
	when  time.Time
	count uint64
}

func newRateTracker() *rateTracker {
	return &rateTracker{samples: make(map[string]rateSample)}
}

// observe records (now, count) for key and returns the average rate since the
// previous observation of that key.
func (rt *rateTracker) observe(key string, now time.Time, count uint64) float32 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	prev, ok := rt.samples[key]
	rt.samples[key] = rateSample{when: now, count: count}
	if !ok || count < prev.count || !now.After(prev.when) {
		return 0
	}
	return float32(float64(count-prev.count) / now.Sub(prev.when).Seconds())
}

// offsetOf converts a JetStream stream sequence to the Offset vocabulary Arke
// reports and accepts. The two do not share an origin: JetStream numbers the
// first message in a stream 1, while a RabbitMQ Stream — the vocabulary
// amqp091 established and clients read SourceStats through — numbers it 0. A
// sequence of 0 is JetStream for "no message", which has no offset, and stays
// 0 rather than going negative; callers distinguish it by LastSeq or by the
// "Offset not found" error. seqOf inverts this.
func offsetOf(seq uint64) int64 {
	if seq == 0 {
		return 0
	}
	return int64(seq - 1) //nolint:gosec // a stream sequence cannot exceed int64
}

// seqOf converts an Arke Offset back to the JetStream stream sequence to start
// a consumer at. It inverts offsetOf, so an offset read from SourceStats and
// handed back as a source's Offset resumes at exactly the message it named.
func seqOf(offset uint64) uint64 { return offset + 1 }

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
	now := time.Now()
	stats.MessageCount = int64(info.State.Msgs)       //nolint:gosec
	stats.ConsumerCount = int32(info.State.Consumers) //nolint:gosec
	stats.LastOffset = offsetOf(info.State.LastSeq)
	durable := durableName(source)
	// JetStream exposes no rates, only counters; sample them between calls
	// (see rateTracker). The stream's LastSeq counts every publish to the
	// address root, so the publish rate is per address, not per binding.
	// The sample key includes the polling identity — durable and source name,
	// since consumer groups share a source name and differ only by their
	// durable: the counters are shared, but each poller runs on its own
	// cadence, and a shared key would make every observation window the gap
	// since whichever poller came last — with several pollers on one address
	// the later polls of a cycle would report rates over milliseconds instead
	// of the poll interval.
	stats.PublishRate = bd.rates.observe("pub/"+streamName+"/"+durable+"/"+source.GetName(), now, info.State.LastSeq)
	// For a source with a durable consumer, report that consumer's actual
	// backlog (undelivered + delivered-but-unacked) instead of the stream
	// depth: the stream retains acked messages under its retention limits, so
	// its message count keeps growing after consumers are caught up. This
	// matches the amqp091 connector, which reports the queue's ready+unacked
	// count — the number consumer-scaling logic wants. Without a durable
	// consumer (or before it exists), fall back to the stream view.
	if durable != "" {
		if cons, cerr := stream.Consumer(ctx, durable); cerr == nil {
			// Consumer() fetched fresh info to return the handle; reuse it
			// instead of paying a second round-trip on every stats poll.
			ci := cons.CachedInfo()
			stats.MessageCount = int64(ci.NumPending) + int64(ci.NumAckPending) //nolint:gosec
			stats.CurrentOffset = offsetOf(ci.AckFloor.Stream)
			// The stream-wide consumer count set above spans every source on
			// the address (streams are shared per address root), so it says
			// nothing about THIS source — it cannot even distinguish zero
			// attached clients from many, because the durable itself counts.
			// The per-source equivalent of RabbitMQ's queue consumer count is
			// the clients pulling this durable: report its open pull requests.
			// (A client whose pull buffer is full can briefly read as zero.)
			stats.ConsumerCount = int32(ci.NumWaiting) //nolint:gosec
			// Delivered.Consumer counts deliveries (redeliveries included,
			// like RabbitMQ's deliver rate). Ephemeral sources have no
			// consumer identity to sample here, so their rate stays zero.
			// Keyed per polling source like the publish rate above (sources
			// can share a durable through a common ConsumerGroup).
			stats.DeliverRate = bd.rates.observe("del/"+streamName+"/"+durable+"/"+source.GetName(), now, ci.Delivered.Consumer)
		}
	}
	// A stream source reports on a log, not on a queue, and amqp091 says so in
	// two ways this has to match. It fills a message count only for
	// Source_QUEUE (getStreamOrQueueStats): a stream's readers each hold their
	// own position in one retained log, so there is no single backlog that
	// belongs to the source — the position is the reading, and it is already
	// in LastOffset/CurrentOffset. And when the log has no offset to report it
	// answers with RabbitMQ's own "Offset not found" (updateStatsForStream)
	// rather than a silent zero.
	if source.GetType() == pb.Source_STREAM {
		stats.MessageCount = 0
		if info.State.LastSeq == 0 {
			stats.Error = &pb.Error{Message: offsetNotFoundError}
		}
	}
	return stats
}
