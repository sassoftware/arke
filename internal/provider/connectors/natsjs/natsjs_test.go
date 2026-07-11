// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package natsjs

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The connector resolves the per-client broker state from the client identifier
// in the context. For tests we override it with a fixed identifier (the real
// implementation reads it from gRPC metadata).
func init() {
	GetClientIdentifier = func(context.Context) (string, error) {
		return "test-client", nil
	}
}

// runJetStreamServer starts an in-process NATS server with JetStream enabled,
// backed by a temp store dir that is cleaned up with the test.
func runJetStreamServer(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // choose a free port
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats server did not become ready")
	}
	t.Cleanup(s.Shutdown)
	return s
}

// connectClient builds a provider and connects a single client to the server.
func connectClient(t *testing.T, s *natsserver.Server) (*natsjsProvider, context.Context) {
	t.Helper()
	addr := s.Addr().(*net.TCPAddr)
	p := NewNATSJetStreamProvider().(*natsjsProvider)
	cfg := &pb.ConnectionConfiguration{
		Host:       "127.0.0.1",
		Port:       int32(addr.Port), //nolint:gosec // local server port fits int32
		ClientName: "test",
	}
	ctx := context.Background()
	if perr := p.Connect(ctx, cfg, false); perr != nil {
		t.Fatalf("connect: %s", perr.GetMessage())
	}
	if !p.WaitForConnect(ctx) {
		t.Fatal("WaitForConnect returned false against a live server")
	}
	return p, ctx
}

func queueSource(name, addr string, subjects ...string) *pb.Source {
	return &pb.Source{
		Name:    name,
		Type:    pb.Source_QUEUE,
		Address: &pb.Address{Name: addr, Subjects: subjects},
		// Offset:first => deliver from the start so tests can publish before
		// subscribing without racing the consumer setup.
		Options: map[string]string{"Offset": "first"},
	}
}

// recv waits for one message on out or fails after a generous timeout.
func recv(t *testing.T, out <-chan *pb.Message) *pb.Message {
	t.Helper()
	select {
	case m := <-out:
		return m
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a message")
		return nil
	}
}

func TestConnectDisconnect(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	assert.True(t, p.ClientExists("test-client"))

	p.Disconnect(ctx)
	assert.False(t, p.ClientExists("test-client"))
}

func TestSupportedSourceOptions(t *testing.T) {
	p := NewNATSJetStreamProvider().(*natsjsProvider)
	opts := p.SupportedSourceOptions()
	for _, k := range []string{"MessageTTL", "DeadLetterAddress", "DeadLetterSubject", "Expires", "Offset", "ConsumerGroup"} {
		assert.True(t, opts[k], "expected %s to be supported", k)
	}
	assert.False(t, opts["NotARealOption"])
}

func TestPublishOneAndSubscribe(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.orders", Subjects: []string{"created"}}
	const n = 5
	for i := 0; i < n; i++ {
		msg := &pb.Message{Address: addr, Body: []byte(fmt.Sprintf("m%d", i))}
		if perr := p.PublishOne(ctx, msg); perr != nil {
			t.Fatalf("publish %d: %s", i, perr.GetMessage())
		}
	}

	out := make(chan *pb.Message, n)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, queueSource("events.orders.consumer", "events.orders", "created"), out)

	for i := 0; i < n; i++ {
		m := recv(t, out)
		assert.NoError(t, errOf(p.Ack(ctx, m.GetUuid())))
	}

	stats := p.Stats()
	require.Len(t, stats.Clients, 1)
	assert.Equal(t, n, stats.Clients[0].Produced)
	assert.Equal(t, n, stats.Clients[0].Consumed)
}

func TestPublishChannel(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	in := make(chan *pb.Message)
	errChan := make(chan *pb.Error)
	go func() { _ = p.Publish(ctx, in, errChan) }()

	addr := &pb.Address{Name: "events.audit", Subjects: []string{"login"}}
	// The server protocol sends a message then blocks reading exactly one error.
	in <- &pb.Message{Address: addr, Body: []byte("hello")}
	assert.Nil(t, <-errChan, "publish should report nil on success")
	close(in)
}

func TestRetrySynthesizesRetryCount(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.retry", Subjects: []string{"job"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("work")}))

	out := make(chan *pb.Message, 2)
	src := queueSource("events.retry.consumer", "events.retry", "job")
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	first := recv(t, out)
	assert.Empty(t, first.GetHeaders()[retryCountHeaderName], "first delivery has no retry count")
	require.Nil(t, p.Retry(ctx, src, first.GetUuid(), 1))

	second := recv(t, out)
	assert.Equal(t, "1", second.GetHeaders()[retryCountHeaderName], "redelivery carries x-retry-count")
	require.Nil(t, p.Ack(ctx, second.GetUuid()))
}

func TestNackRedelivers(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.nack", Subjects: []string{"job"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("work")}))

	out := make(chan *pb.Message, 2)
	src := queueSource("events.nack.consumer", "events.nack", "job")
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	first := recv(t, out)
	require.Nil(t, p.Nack(ctx, first.GetUuid()))

	second := recv(t, out)
	require.Nil(t, p.Ack(ctx, second.GetUuid()))
}

func TestDeadLetter(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.dl", Subjects: []string{"job"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("poison")}))

	out := make(chan *pb.Message, 1)
	src := queueSource("events.dl.consumer", "events.dl", "job")
	src.Options["DeadLetterAddress"] = "events.dlq"
	src.Options["DeadLetterSubject"] = "failed"
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	m := recv(t, out)
	require.Nil(t, p.DeadLetter(ctx, src, m.GetUuid()))

	// The dead-lettered copy lands on the DLQ stream.
	dlqStats := p.SourceStats(ctx, &pb.Source{
		Name:    "events.dlq.consumer",
		Address: &pb.Address{Name: "events.dlq"},
	})
	assert.Nil(t, dlqStats.GetError())
	assert.Equal(t, int64(1), dlqStats.GetMessageCount())

	// The copy carries a deterministic Nats-Msg-Id, so a retried dead-letter of
	// the same message dedups in the DLQ instead of duplicating.
	bd := p.getBrokerDetailsByIdentifier("test-client")
	dlqStream, serr := bd.js.Stream(ctx, streamNameFor("events.dlq"))
	require.NoError(t, serr)
	raw, gerr := dlqStream.GetMsg(ctx, 1)
	require.NoError(t, gerr)
	assert.NotEmpty(t, raw.Header.Get("Nats-Msg-Id"), "DLQ copy must carry a dedup message id")
}

// TestDeadLetterFailureKeepsMessage: when the dead-letter publish cannot happen
// (here the DLQ address's subject space is already claimed by a foreign
// stream, so ensuring its stream fails), DeadLetter must NOT Term the
// original. It returns an error and leaves the message in flight, so the
// caller's fallback nack still resolves the uuid and the message is
// redelivered — the failure mode is retry, not silent loss.
func TestDeadLetterFailureKeepsMessage(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	// Claim the DLQ subject space so ensureStream for the DLA fails with
	// "subjects overlap with an existing stream".
	bd := p.getBrokerDetailsByIdentifier("test-client")
	_, serr := bd.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "squatter",
		Subjects: []string{"events.dlx.~", "events.dlx.~.>"},
	})
	require.NoError(t, serr)

	addr := &pb.Address{Name: "events.dlfail", Subjects: []string{"job"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("poison")}))

	out := make(chan *pb.Message, 2)
	src := queueSource("events.dlfail.consumer", "events.dlfail", "job")
	src.Options["DeadLetterAddress"] = "events.dlx"
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	m := recv(t, out)
	perr := p.DeadLetter(ctx, src, m.GetUuid())
	require.NotNil(t, perr, "DeadLetter must fail when the DLQ publish cannot happen")
	assert.Contains(t, perr.GetMessage(), "dead-letter")

	// Not Term'd: the same uuid is still nackable, and the nack redelivers.
	require.Nil(t, p.Nack(ctx, m.GetUuid()), "a failed dead-letter leaves the message nackable")
	again := recv(t, out)
	assert.Equal(t, "poison", string(again.GetBody()))
	require.Nil(t, p.Ack(ctx, again.GetUuid()))
}

// TestPrefetchMapsToMaxAckPending: a positive PrefetchCount becomes the
// consumer's MaxAckPending. A prefetch of 0 is the AMQP "unlimited"
// convention — the amqp091 connector honors it by leaving the channel
// default, which is unlimited — so it maps to JetStream's unlimited (-1),
// not to a one-message-at-a-time clamp.
func TestPrefetchMapsToMaxAckPending(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	bd := p.getBrokerDetailsByIdentifier("test-client")
	maxAckPending := func(name string, prefetch int32) int {
		t.Helper()
		src := queueSource(name, "events.prefetch", "e")
		src.PrefetchCount = prefetch
		src.DeclareOnly = true
		require.Nil(t, p.Subscribe(ctx, src, nil))
		stream, serr := bd.js.Stream(ctx, streamNameFor("events.prefetch"))
		require.NoError(t, serr)
		cons, cerr := stream.Consumer(ctx, durableName(src))
		require.NoError(t, cerr)
		return cons.CachedInfo().Config.MaxAckPending
	}

	assert.Equal(t, 7, maxAckPending("events.prefetch.limited", 7))
	assert.Equal(t, -1, maxAckPending("events.prefetch.unlimited", 0))
}

func TestDeduplication(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.dedup", Subjects: []string{"e"}}
	dup := func() *pb.Message {
		return &pb.Message{Address: addr, Body: []byte("x"), PublisherName: "pub", PublishId: 42}
	}
	require.Nil(t, p.PublishOne(ctx, dup()))
	require.Nil(t, p.PublishOne(ctx, dup()))

	stats := p.SourceStats(ctx, &pb.Source{
		Name:    "events.dedup.consumer",
		Address: &pb.Address{Name: "events.dedup"},
	})
	assert.Nil(t, stats.GetError())
	assert.Equal(t, int64(1), stats.GetMessageCount(), "duplicate publish_id within the window is collapsed")
}

func TestHeaderFilterDropsNonMatching(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.filter", Subjects: []string{"e"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("keep"), Headers: map[string]string{"region": "us"}}))
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("drop"), Headers: map[string]string{"region": "eu"}}))

	out := make(chan *pb.Message, 2)
	src := queueSource("events.filter.consumer", "events.filter", "e")
	src.Filters = []*pb.Filter{{
		Type:    pb.Filter_ALL,
		Matches: []*pb.Match{{Name: "region", Value: "us"}},
	}}
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	m := recv(t, out)
	assert.Equal(t, "keep", string(m.GetBody()))
	require.Nil(t, p.Ack(ctx, m.GetUuid()))

	// The non-matching message is acked and dropped proxy-side, so nothing else
	// should arrive.
	select {
	case extra := <-out:
		t.Fatalf("unexpected second delivery: %q", string(extra.GetBody()))
	case <-time.After(500 * time.Millisecond):
	}
}

func TestDurableConsumerResumesBacklog(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.durable", Subjects: []string{"e"}}
	src := queueSource("events.durable.consumer", "events.durable", "e")

	// Establish the durable consumer, then stop consuming (client "outage").
	declCtx, declCancel := context.WithCancel(ctx)
	go p.Subscribe(declCtx, src, make(chan *pb.Message, 1))
	time.Sleep(200 * time.Millisecond)
	declCancel()
	time.Sleep(100 * time.Millisecond)

	// Publish a backlog while no consumer is attached.
	const n = 3
	for i := 0; i < n; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte(fmt.Sprintf("b%d", i))}))
	}

	// Reattach the same durable; the backlog is redelivered.
	out := make(chan *pb.Message, n)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	for i := 0; i < n; i++ {
		m := recv(t, out)
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
	}
}

func TestDeclareOnly(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	src := queueSource("events.declare.consumer", "events.declare", "e")
	src.DeclareOnly = true

	// DeclareOnly establishes topology and returns without blocking.
	done := make(chan *pb.Error, 1)
	go func() { done <- p.Subscribe(ctx, src, make(chan *pb.Message)) }()
	select {
	case perr := <-done:
		assert.Nil(t, perr)
	case <-time.After(10 * time.Second):
		t.Fatal("DeclareOnly Subscribe did not return")
	}

	// The stream exists afterwards.
	stats := p.SourceStats(ctx, &pb.Source{Name: "x", Address: &pb.Address{Name: "events.declare"}})
	assert.Nil(t, stats.GetError())
}

func TestAckUnknownUUID(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	perr := p.Ack(ctx, "does-not-exist")
	require.NotNil(t, perr)
	assert.Contains(t, perr.GetMessage(), "no message with uuid")
}

func TestStreamConfigFromEnv(t *testing.T) {
	t.Setenv("NATSJS_STREAM_REPLICAS", "5")
	assert.Equal(t, 5, streamReplicas())
	t.Setenv("NATSJS_STREAM_REPLICAS", "bad")
	assert.Equal(t, 1, streamReplicas())

	t.Setenv("NATSJS_STREAM_MAX_AGE", "24h")
	assert.Equal(t, 24*time.Hour, streamMaxAge())
	t.Setenv("NATSJS_STREAM_MAX_AGE", "0")
	assert.Equal(t, time.Duration(0), streamMaxAge())

	t.Setenv("NATSJS_STREAM_MAX_BYTES", "1048576")
	assert.Equal(t, int64(1048576), streamMaxBytes())
	t.Setenv("NATSJS_STREAM_MAX_BYTES", "0")
	assert.Equal(t, int64(0), streamMaxBytes())
}

func TestJSAPITimeoutFromEnv(t *testing.T) {
	t.Setenv("NATSJS_API_TIMEOUT", "45s")
	assert.Equal(t, 45*time.Second, jsAPITimeout())
	t.Setenv("NATSJS_API_TIMEOUT", "bad")
	assert.Equal(t, defaultJSAPITimeout, jsAPITimeout())
	t.Setenv("NATSJS_API_TIMEOUT", "-1s")
	assert.Equal(t, defaultJSAPITimeout, jsAPITimeout())
}

func TestStreamRegistryCollapsesConcurrentCalls(t *testing.T) {
	r := newStreamRegistry()
	release := make(chan struct{})
	leaderStarted := make(chan struct{})
	const key = "broker:4222/arke_events"

	// One call gets in flight and blocks...
	leaderErr := make(chan error, 1)
	go func() {
		leaderErr <- r.ensure(context.Background(), key, func() error {
			close(leaderStarted)
			<-release
			return nil
		})
	}()
	<-leaderStarted

	// ...then every caller that arrives while it is in flight must piggyback
	// on it instead of running its own create.
	var extraCalls int32
	const n = 8
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			errs <- r.ensure(context.Background(), key, func() error {
				atomic.AddInt32(&extraCalls, 1)
				return nil
			})
		}()
	}
	time.Sleep(250 * time.Millisecond) // let the followers reach ensure
	close(release)

	assert.NoError(t, <-leaderErr)
	for i := 0; i < n; i++ {
		assert.NoError(t, <-errs)
	}
	assert.Equal(t, int32(0), atomic.LoadInt32(&extraCalls), "followers share the in-flight result")
}

func TestStreamRegistryDoesNotCacheFailures(t *testing.T) {
	r := newStreamRegistry()
	boom := fmt.Errorf("create failed")
	assert.ErrorIs(t, r.ensure(context.Background(), "k", func() error { return boom }), boom)

	calls := 0
	assert.NoError(t, r.ensure(context.Background(), "k", func() error { calls++; return nil }))
	assert.Equal(t, 1, calls, "a failed create is retried by the next caller")
}

func TestStreamRegistryFollowerHonorsContext(t *testing.T) {
	r := newStreamRegistry()
	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_ = r.ensure(context.Background(), "k", func() error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := r.ensure(ctx, "k", func() error {
		t.Error("follower must not run create")
		return nil
	})
	assert.ErrorIs(t, err, context.Canceled)
	close(release)
}

func TestDeliverPolicyFor(t *testing.T) {
	mk := func(off string) *pb.Source { return &pb.Source{Options: map[string]string{"Offset": off}} }
	check := func(off string, wantPol jetstream.DeliverPolicy, wantSeq uint64) {
		t.Helper()
		pol, seq, err := deliverPolicyFor(mk(off))
		assert.Nil(t, err, "error for Offset=%q", off)
		assert.Equal(t, wantPol, pol, "policy for Offset=%q", off)
		assert.Equal(t, wantSeq, seq, "start seq for Offset=%q", off)
	}
	check("first", jetstream.DeliverAllPolicy, 0)
	check("First", jetstream.DeliverAllPolicy, 0) // case-insensitive, like amqp091
	check("continue", jetstream.DeliverAllPolicy, 0)
	check("last", jetstream.DeliverLastPolicy, 0)
	check("next", jetstream.DeliverNewPolicy, 0)
	check("", jetstream.DeliverNewPolicy, 0)
	check("100", jetstream.DeliverByStartSequencePolicy, 100)
	check("0", jetstream.DeliverAllPolicy, 0) // offset 0 == from the beginning

	pol, seq, err := deliverPolicyFor(&pb.Source{})
	assert.Nil(t, err)
	assert.Equal(t, jetstream.DeliverNewPolicy, pol)
	assert.Equal(t, uint64(0), seq)

	// An unrecognized offset is rejected, like amqp091's toStreamOffset —
	// starting at a silently different position would lose or replay data.
	for _, bad := range []string{"garbage", "-1", "1.5"} {
		_, _, err := deliverPolicyFor(mk(bad))
		assert.ErrorContains(t, err, "invalid offset", "Offset=%q", bad)
	}
}

func TestFirstSubject(t *testing.T) {
	assert.Equal(t, "created", firstSubject(&pb.Address{Subjects: []string{"created", "ignored"}}))
	assert.Equal(t, "", firstSubject(&pb.Address{}))
}

func TestWaitForConnectNoClient(t *testing.T) {
	p := NewNATSJetStreamProvider().(*natsjsProvider)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, p.WaitForConnect(ctx))
}

func TestSourceStatsMissingStream(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)
	stats := p.SourceStats(ctx, &pb.Source{Name: "x", Address: &pb.Address{Name: "never.created"}})
	assert.NotNil(t, stats.GetError())
}

// TestClientIdentifierFailurePropagates drives the error branch every method
// shares: when the client identifier cannot be resolved, each returns an error
// rather than touching the (nil) connection.
func TestClientIdentifierFailurePropagates(t *testing.T) {
	saved := GetClientIdentifier
	defer func() { GetClientIdentifier = saved }()
	GetClientIdentifier = func(context.Context) (string, error) {
		return "", fmt.Errorf("no client identifier")
	}

	p := NewNATSJetStreamProvider().(*natsjsProvider)
	ctx := context.Background()
	src := queueSource("c", "events.x", "e")
	msg := &pb.Message{Address: &pb.Address{Name: "events.x"}}

	assert.NotNil(t, p.Connect(ctx, &pb.ConnectionConfiguration{Host: "127.0.0.1", Port: 1}, false))
	assert.NotNil(t, p.PublishOne(ctx, msg))
	assert.NotNil(t, p.Publish(ctx, make(chan *pb.Message), make(chan *pb.Error)))
	assert.NotNil(t, p.Subscribe(ctx, src, make(chan *pb.Message)))
	assert.NotNil(t, p.Ack(ctx, "x"))
	assert.NotNil(t, p.Nack(ctx, "x"))
	assert.NotNil(t, p.Retry(ctx, src, "x", 1))
	assert.NotNil(t, p.DeadLetter(ctx, src, "x"))
	assert.False(t, p.WaitForConnect(ctx))
	assert.NotNil(t, p.SourceStats(ctx, src).GetError())
	p.Disconnect(ctx) // exercises the early-return path; must not panic
}

// errOf adapts a *pb.Error into a Go error for assert.NoError-style checks.
func errOf(e *pb.Error) error {
	if e == nil {
		return nil
	}
	return fmt.Errorf("%s", e.GetMessage())
}

// TestPrefixAddressesCoexist is the end-to-end regression test for addresses
// in a dotted-prefix relationship (e.g. "events.jobs" and
// "events.jobs.filter"). Before the "~" delimiter both addresses mapped to
// overlapping stream subjects, so whichever stream was created first won and
// every ensure of the other failed with "subjects overlap with an existing
// stream" (err 10065). Both creation orders are exercised, and traffic on one
// address must not leak into the other even when a routing key spells out the
// other address's name.
func TestPrefixAddressesCoexist(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	// parent first, then child
	parent := &pb.Address{Name: "events.jobs", Subjects: []string{"created"}}
	child := &pb.Address{Name: "events.jobs.filter", Subjects: []string{"created"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: parent, Body: []byte("to-parent")}))
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: child, Body: []byte("to-child")}),
		"creating the child stream after the parent must not collide")

	// child first, then parent
	child2 := &pb.Address{Name: "events.audit.trail", Subjects: []string{"e"}}
	parent2 := &pb.Address{Name: "events.audit", Subjects: []string{"e"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: child2, Body: []byte("x")}))
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: parent2, Body: []byte("x")}),
		"creating the parent stream after the child must not collide")

	// a routing key on the parent that spells the child's name stays on the
	// parent's stream
	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: &pb.Address{Name: "events.jobs", Subjects: []string{"filter.created"}},
		Body:    []byte("still-to-parent"),
	}))

	out := make(chan *pb.Message, 4)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, queueSource("events.jobs.filter.consumer", "events.jobs.filter", "created"), out)

	m := recv(t, out)
	assert.Equal(t, "to-child", string(m.GetBody()),
		"the child consumer sees only the child's message")
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
	select {
	case extra := <-out:
		t.Fatalf("unexpected cross-address delivery: %q", extra.GetBody())
	case <-time.After(500 * time.Millisecond):
	}

	// the parent consumer sees both parent messages
	pout := make(chan *pb.Message, 4)
	go p.Subscribe(subCtx, queueSource("events.jobs.consumer", "events.jobs", "#"), pout)
	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		m := recv(t, pout)
		got[string(m.GetBody())] = true
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
	}
	assert.True(t, got["to-parent"] && got["still-to-parent"], "got %v", got)
}

// streamSource builds a stream-style source positioned at the given RabbitMQ
// Streams offset ("first"/"next"/...). It is the offset-parameterized sibling
// of queueSource.
func streamSource(name, addr, offset string, subjects ...string) *pb.Source {
	return &pb.Source{
		Name:    name,
		Type:    pb.Source_QUEUE,
		Address: &pb.Address{Name: addr, Subjects: subjects},
		Options: map[string]string{"Offset": offset},
	}
}

// TestStreamOffsetReplay is the RabbitMQ-Streams parity test. A natsjs address
// is a retained JetStream log (LimitsPolicy, acked messages are NOT deleted),
// so it behaves like a RabbitMQ Stream rather than a classic/quorum work
// queue: independent consumers each read from their own offset, and a fully
// acked backlog is still replayable by a fresh consumer.
func TestStreamOffsetReplay(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.stream", Subjects: []string{"e"}}
	const n = 4
	for i := 0; i < n; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte(fmt.Sprintf("m%d", i))}))
	}

	drain := func(t *testing.T, name string) int {
		t.Helper()
		out := make(chan *pb.Message, n)
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go p.Subscribe(subCtx, streamSource(name, "events.stream", "first", "e"), out)
		for i := 0; i < n; i++ {
			m := recv(t, out)
			require.Nil(t, p.Ack(ctx, m.GetUuid()))
		}
		return n
	}

	// Consumer A reads and acks the whole backlog from offset "first".
	t.Run("first_reads_full_backlog", func(t *testing.T) {
		assert.Equal(t, n, drain(t, "events.stream.a"))
	})

	// The defining Streams property: a second, independent consumer with its
	// own durable/offset still replays all n messages even though consumer A
	// already acked them. A RabbitMQ classic/quorum queue would have nothing
	// left to deliver here — a Stream does.
	t.Run("independent_consumer_replays_after_acks", func(t *testing.T) {
		assert.Equal(t, n, drain(t, "events.stream.b"),
			"an independent stream consumer replays the retained log")
	})
}

// TestStreamOffsetNextSkipsBacklog: offset "next" (and the default) positions a
// new consumer at the tail — it sees only messages published after it was
// created, matching RabbitMQ Streams "next".
func TestStreamOffsetNextSkipsBacklog(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.tail", Subjects: []string{"e"}}
	// Backlog published before the consumer exists.
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("old")}))

	out := make(chan *pb.Message, 2)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, streamSource("events.tail.consumer", "events.tail", "next", "e"), out)
	// Let the durable form at the tail before publishing "new", so DeliverNew's
	// start point is unambiguously after "old".
	time.Sleep(500 * time.Millisecond)

	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("new")}))

	m := recv(t, out)
	assert.Equal(t, "new", string(m.GetBody()), "next offset skips the pre-existing backlog")
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
	select {
	case extra := <-out:
		t.Fatalf("unexpected extra delivery (backlog leaked past 'next'): %q", extra.GetBody())
	case <-time.After(500 * time.Millisecond):
	}
}

// TestStreamOffsetLastAndNumeric is the behavioral counterpart to
// TestDeliverPolicyFor: it drives the two offset positions that amqp091's
// toStreamOffset supports and that natsjs now honors — "last" (final message
// only) and an absolute numeric stream sequence.
func TestStreamOffsetLastAndNumeric(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.offset", Subjects: []string{"e"}}
	const n = 4 // seq: m0=1, m1=2, m2=3, m3=4
	for i := 0; i < n; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte(fmt.Sprintf("m%d", i))}))
	}

	t.Run("last_delivers_only_final", func(t *testing.T) {
		out := make(chan *pb.Message, n)
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go p.Subscribe(subCtx, streamSource("events.offset.last", "events.offset", "last", "e"), out)

		m := recv(t, out)
		assert.Equal(t, "m3", string(m.GetBody()), "last offset starts at the final message")
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
		select {
		case extra := <-out:
			t.Fatalf("last should deliver only the final message, got extra %q", extra.GetBody())
		case <-time.After(500 * time.Millisecond):
		}
	})

	t.Run("numeric_offset_starts_at_sequence", func(t *testing.T) {
		out := make(chan *pb.Message, n)
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		// JetStream sequence 3 == m2 (1-based); expect m2 then m3.
		go p.Subscribe(subCtx, streamSource("events.offset.num", "events.offset", "3", "e"), out)

		got := []string{}
		for i := 0; i < 2; i++ {
			m := recv(t, out)
			got = append(got, string(m.GetBody()))
			require.Nil(t, p.Ack(ctx, m.GetUuid()))
		}
		assert.Equal(t, []string{"m2", "m3"}, got, "numeric offset starts at the given stream sequence")
	})
}

// TestSubscribeInvalidOffsetRejected: an offset outside the shared vocabulary
// fails the subscribe (mirroring amqp091's toStreamOffset) instead of silently
// starting the consumer at "next".
func TestSubscribeInvalidOffsetRejected(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	out := make(chan *pb.Message, 1)
	perr := p.Subscribe(ctx, streamSource("events.bad.consumer", "events.bad", "latest", "e"), out)
	require.NotNil(t, perr)
	assert.Contains(t, perr.GetMessage(), "invalid offset")
	assert.True(t, perr.GetIsFatal())
}

// TestDurableOffsetPinnedAtCreation: a durable's start position is fixed when
// the consumer is first created — JetStream rejects DeliverPolicy/OptStartSeq
// updates (err 10012) — so a re-subscribe that asks for a different Offset must
// not fail; it attaches to the existing durable and resumes from its stored ack
// position, per the documented contract.
func TestDurableOffsetPinnedAtCreation(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.pinned", Subjects: []string{"e"}}
	const n = 4
	for i := 0; i < n; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte(fmt.Sprintf("m%d", i))}))
	}

	src := func(offset string) *pb.Source {
		s := streamSource("events.pinned.consumer", "events.pinned", offset, "e")
		// One message in flight at a time, so m1..m3 stay undelivered while
		// the first subscription is up and are immediately deliverable to the
		// re-attach below (unlimited prefetch would leave them ack-pending
		// until AckWait).
		s.PrefetchCount = 1
		return s
	}

	// Create the durable at "first" and ack only m0, leaving m1..m3 pending.
	out := make(chan *pb.Message, n)
	subCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan *pb.Error, 1)
	go func() { errCh <- p.Subscribe(subCtx, src("first"), out) }()
	m := recv(t, out)
	assert.Equal(t, "m0", string(m.GetBody()))
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
	cancel()
	require.Nil(t, <-errCh)

	// Re-subscribe the same durable asking for sequence 4 (m3). The request
	// conflicts with the pinned start position; the consumer resumes from its
	// stored ack floor instead, so the next delivery is m1, not m3.
	out2 := make(chan *pb.Message, n)
	subCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	errCh2 := make(chan *pb.Error, 1)
	go func() { errCh2 <- p.Subscribe(subCtx2, src("4"), out2) }()
	m2 := recv(t, out2)
	assert.Equal(t, "m1", string(m2.GetBody()),
		"existing durable resumes from its stored position; the conflicting offset is ignored")
	require.Nil(t, p.Ack(ctx, m2.GetUuid()))
	cancel2()
	require.Nil(t, <-errCh2, "conflicting offset on an existing durable must not fail the subscribe")
}

// TestIsStartPositionConflict pins down which consumer-create errors the
// durable fallback absorbs: only the server's refusals to move a start
// position. Everything else — other immutable-field refusals sharing err code
// 10012, other API errors, non-API errors — stays fatal.
func TestIsStartPositionConflict(t *testing.T) {
	mk := func(code jetstream.ErrorCode, desc string) error {
		return &jetstream.APIError{ErrorCode: code, Description: desc}
	}
	assert.True(t, isStartPositionConflict(mk(jetstream.JSErrCodeConsumerCreate, "deliver policy can not be updated")))
	assert.True(t, isStartPositionConflict(mk(jetstream.JSErrCodeConsumerCreate, "start sequence can not be updated")))
	assert.True(t, isStartPositionConflict(mk(jetstream.JSErrCodeConsumerCreate, "start time can not be updated")))

	// Same error code, different refusal: not a start-position conflict.
	assert.False(t, isStartPositionConflict(mk(jetstream.JSErrCodeConsumerCreate, "replay policy can not be updated")))
	assert.False(t, isStartPositionConflict(mk(jetstream.JSErrCodeConsumerCreate, "invalid subject")))
	// Different code, or not an API error at all.
	assert.False(t, isStartPositionConflict(mk(jetstream.JSErrCodeStreamNotFound, "stream not found")))
	assert.False(t, isStartPositionConflict(fmt.Errorf("deliver policy can not be updated")))
}

// TestDurableConflictBeyondStartPositionStaysFatal: the attach-unchanged
// fallback exists for start-position conflicts only. If the existing durable
// differs in any other way — here an incompatible ReplayPolicy — resuming it
// unchanged would silently consume with a configuration the source never
// asked for, so the subscribe must fail instead.
func TestDurableConflictBeyondStartPositionStaysFatal(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.replay", Subjects: []string{"e"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("x")}))

	src := queueSource("events.replay.consumer", "events.replay", "e")

	// A consumer with the connector's durable name already exists, matching on
	// start position but differing in another immutable field.
	bd := p.getBrokerDetailsByIdentifier("test-client")
	stream, serr := bd.js.Stream(ctx, streamNameFor("events.replay"))
	require.NoError(t, serr)
	_, cerr := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        durableName(src),
		FilterSubjects: filterSubjectsFor(src),
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy, // matches Offset "first"
		ReplayPolicy:   jetstream.ReplayOriginalPolicy,
	})
	require.NoError(t, cerr)

	// Bound the subscribe so a regression (wrongly attaching and blocking)
	// fails the test instead of hanging it.
	subCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	perr := p.Subscribe(subCtx, src, make(chan *pb.Message, 1))
	require.NotNil(t, perr, "a non-start-position config conflict must fail the subscribe")
	assert.True(t, perr.GetIsFatal())
	assert.Contains(t, perr.GetMessage(), "create consumer")
}

// TestEmptyRoutingKeyDelivers covers fanout-style publishes that carry no
// routing key: they map to the bare "<root>.~" subject, which both the stream
// and a pattern-less consumer capture.
func TestEmptyRoutingKeyDelivers(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.fanout"}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("no-key")}))

	out := make(chan *pb.Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, queueSource("events.fanout.consumer", "events.fanout"), out)

	m := recv(t, out)
	assert.Equal(t, "no-key", string(m.GetBody()))
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
}

// TestSourceStatsReportsConsumerBacklog: for a durable source the message
// count is the consumer's backlog, not the stream depth — the stream retains
// acked messages under its retention limits, so its count never shrinks as
// consumers catch up.
func TestSourceStatsReportsConsumerBacklog(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.backlog", Subjects: []string{"e"}}
	for i := 0; i < 3; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("x")}))
	}

	// Declare the durable consumer without consuming: its backlog is 3.
	src := queueSource("events.backlog.consumer", "events.backlog", "e")
	src.DeclareOnly = true
	require.Nil(t, p.Subscribe(ctx, src, nil))

	stats := p.SourceStats(ctx, src)
	require.Nil(t, stats.GetError())
	assert.Equal(t, int64(3), stats.GetMessageCount(), "durable backlog")

	// Drain and ack everything; backlog returns to 0 even though the stream
	// still retains the acked messages.
	src.DeclareOnly = false
	out := make(chan *pb.Message, 3)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)
	for i := 0; i < 3; i++ {
		m := recv(t, out)
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
	}
	require.Eventually(t, func() bool {
		st := p.SourceStats(ctx, src)
		return st.GetError() == nil && st.GetMessageCount() == 0
	}, 10*time.Second, 200*time.Millisecond, "backlog drains to 0 after acks")
}

// TestEphemeralConsumerDeletedOnTeardown verifies a transient source's
// ephemeral consumer is removed from the server as soon as its subscription
// ends, instead of counting against the stream's consumer limit until the
// inactivity threshold expires it.
func TestEphemeralConsumerDeletedOnTeardown(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	src := &pb.Source{
		Name:       "events.tmp.listener",
		Type:       pb.Source_QUEUE,
		AutoDelete: true, // transient -> ephemeral consumer
		Address:    &pb.Address{Name: "events.tmp", Subjects: []string{"a"}},
	}
	require.Empty(t, durableName(src), "test source must map to an ephemeral consumer")

	out := make(chan *pb.Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		_ = p.Subscribe(subCtx, src, out)
		close(done)
	}()

	bd := p.getBrokerDetailsByIdentifier("test-client")
	var stream jetstream.Stream
	require.Eventually(t, func() bool {
		st, err := bd.js.Stream(ctx, streamNameFor("events.tmp"))
		if err != nil {
			return false
		}
		stream = st
		info, ierr := st.Info(ctx)
		return ierr == nil && info.State.Consumers == 1
	}, 10*time.Second, 20*time.Millisecond, "ephemeral consumer never appeared")

	cancel()
	<-done

	assert.Eventually(t, func() bool {
		info, err := stream.Info(ctx)
		return err == nil && info.State.Consumers == 0
	}, 10*time.Second, 20*time.Millisecond, "ephemeral consumer was not deleted on teardown")
}

func TestRateTracker(t *testing.T) {
	rt := newRateTracker()
	t0 := time.Now()

	// First observation only establishes the baseline.
	assert.Zero(t, rt.observe("k", t0, 100))
	// 10 messages over 2 seconds.
	assert.InDelta(t, 5.0, rt.observe("k", t0.Add(2*time.Second), 110), 1e-6)
	// A counter that moved backwards (stream recreated) re-baselines at zero.
	assert.Zero(t, rt.observe("k", t0.Add(3*time.Second), 4))
	assert.InDelta(t, 6.0, rt.observe("k", t0.Add(4*time.Second), 10), 1e-6)
	// A non-advancing clock cannot divide by zero.
	assert.Zero(t, rt.observe("k", t0.Add(4*time.Second), 20))
	// Keys are independent.
	assert.Zero(t, rt.observe("other", t0, 1))
}

// TestSourceStatsRates verifies publish/deliver rates are sampled between
// SourceStats calls: the first call baselines at zero, and a later call after
// publishing and consuming reports positive rates.
func TestSourceStatsRates(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.rates", Subjects: []string{"e"}}
	src := queueSource("events.rates.consumer", "events.rates", "e")

	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("x")}))

	out := make(chan *pb.Message, 8)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)
	require.Nil(t, p.Ack(ctx, recv(t, out).GetUuid()))

	// Baseline scrape: no previous observation, rates are zero.
	stats := p.SourceStats(ctx, src)
	require.Nil(t, stats.GetError())
	assert.Zero(t, stats.GetPublishRate())
	assert.Zero(t, stats.GetDeliverRate())

	for i := 0; i < 5; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("y")}))
		require.Nil(t, p.Ack(ctx, recv(t, out).GetUuid()))
	}
	time.Sleep(50 * time.Millisecond) // guarantee a measurable interval

	stats = p.SourceStats(ctx, src)
	require.Nil(t, stats.GetError())
	assert.Positive(t, stats.GetPublishRate(), "5 publishes since the last scrape")
	assert.Positive(t, stats.GetDeliverRate(), "5 deliveries since the last scrape")
}

func TestUnappliedSourceOptions(t *testing.T) {
	src := func(opts map[string]string) *pb.Source { return &pb.Source{Options: opts} }

	assert.Empty(t, unappliedSourceOptions(src(nil)))
	assert.Empty(t, unappliedSourceOptions(src(map[string]string{"Offset": "first", "MessageTTL": ""})))
	assert.Equal(t, []string{"MessageTTL"}, unappliedSourceOptions(src(map[string]string{"MessageTTL": "300000"})))
	assert.Equal(t, []string{"MessageTTL", "Expires"},
		unappliedSourceOptions(src(map[string]string{"MessageTTL": "300000", "Expires": "600000"})))
}
