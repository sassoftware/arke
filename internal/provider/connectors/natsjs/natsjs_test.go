// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package natsjs

import (
	"context"
	"fmt"
	"net"
	"sync"
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
	return runJetStreamServerAt(t, -1) // choose a free port
}

// runJetStreamServerAt is runJetStreamServer on a caller-chosen port, so a
// test can restart "the broker" at the address a client is connected to —
// with a fresh store dir, i.e. a broker that lost its JetStream state.
func runJetStreamServerAt(t *testing.T, port int) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      port,
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

// TestConcurrentConnectClosesRedundantLink: the gRPC server's "already
// connected?" check is not atomic, so two Connect calls for one client
// identifier can both reach the provider. Only one connection may survive; the
// other's nats.Conn must be closed rather than orphaned to reconnect forever
// (MaxReconnects(-1)). The proof is the server seeing exactly one live client.
func TestConcurrentConnectClosesRedundantLink(t *testing.T) {
	s := runJetStreamServer(t)
	addr := s.Addr().(*net.TCPAddr)
	p := NewNATSJetStreamProvider().(*natsjsProvider)
	cfg := &pb.ConnectionConfiguration{
		Host:       "127.0.0.1",
		Port:       int32(addr.Port), //nolint:gosec // local server port fits int32
		ClientName: "test",
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if perr := p.Connect(ctx, cfg, false); perr != nil {
				t.Errorf("connect: %s", perr.GetMessage())
			}
		}()
	}
	wg.Wait()
	require.True(t, p.WaitForConnect(ctx))

	// Exactly one live server-side client connection: the redundant dial was
	// closed. Drain can lag the close slightly, so let it settle.
	require.Eventually(t, func() bool {
		return s.NumClients() == 1
	}, 5*time.Second, 50*time.Millisecond,
		"a racing Connect must leave exactly one live NATS connection, not leak the loser")

	p.Disconnect(ctx)
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

	// Fail the first delivery, then dead-letter the redelivery, so the copy
	// has a retry trail to carry.
	first := recv(t, out)
	require.Nil(t, p.Nack(ctx, first.GetUuid()))
	m := recv(t, out)
	require.Equal(t, "1", m.GetHeaders()[retryCountHeaderName], "redelivery precondition")
	require.Nil(t, p.DeadLetter(ctx, src, m.GetUuid()))

	// The dead-lettered copy lands on the DLQ stream.
	dlqStats := p.SourceStats(ctx, &pb.Source{
		Name:    "events.dlq.consumer",
		Address: &pb.Address{Name: "events.dlq"},
	})
	assert.Nil(t, dlqStats.GetError())
	assert.Equal(t, int64(1), dlqStats.GetMessageCount())

	// The copy carries a deterministic Nats-Msg-Id, so a retried dead-letter of
	// the same message dedups in the DLQ instead of duplicating — and the same
	// retry count the consumer saw when it gave the message up (RabbitMQ's
	// broker-side dead-lettering preserves the death trail via x-death).
	bd := p.getBrokerDetailsByIdentifier("test-client")
	dlqStream, serr := bd.js.Stream(ctx, streamNameFor("events.dlq"))
	require.NoError(t, serr)
	raw, gerr := dlqStream.GetMsg(ctx, 1)
	require.NoError(t, gerr)
	assert.NotEmpty(t, raw.Header.Get("Nats-Msg-Id"), "DLQ copy must carry a dedup message id")
	assert.Equal(t, "1", raw.Header.Get(retryCountHeaderName),
		"DLQ copy must carry the retry count at dead-letter time")
}

// TestDeadLetterPreservesRoutingKey: when a source sets no DeadLetterSubject,
// the DLQ copy must keep the original message's routing key — RabbitMQ
// dead-letters under the original key unless x-dead-letter-routing-key
// overrides it, and DLQ consumers may bind by routing key.
func TestDeadLetterPreservesRoutingKey(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.dlrk", Subjects: []string{"region.us.created"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("poison")}))

	out := make(chan *pb.Message, 1)
	src := queueSource("events.dlrk.consumer", "events.dlrk", "region.#")
	src.Options["DeadLetterAddress"] = "events.dlrk.dlq"
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	m := recv(t, out)
	require.Nil(t, p.DeadLetter(ctx, src, m.GetUuid()))

	bd := p.getBrokerDetailsByIdentifier("test-client")
	dlqStream, serr := bd.js.Stream(ctx, streamNameFor("events.dlrk.dlq"))
	require.NoError(t, serr)
	raw, gerr := dlqStream.GetMsg(ctx, 1)
	require.NoError(t, gerr)
	assert.Equal(t, "events.dlrk.dlq.~.region.us.created", raw.Subject,
		"the DLQ copy must carry the original routing key under the DLA root")
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

func TestDeadLetterEmptyAddressKeepsMessage(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.dlempty", Subjects: []string{"job"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("poison")}))

	out := make(chan *pb.Message, 2)
	src := queueSource("events.dlempty.consumer", "events.dlempty", "job")
	src.Options["DeadLetterAddress"] = ""
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	m := recv(t, out)
	perr := p.DeadLetter(ctx, src, m.GetUuid())
	require.NotNil(t, perr, "an empty DLA must fail instead of terming the original")
	assert.Contains(t, perr.GetMessage(), "DeadLetterAddress is empty")

	require.Nil(t, p.Nack(ctx, m.GetUuid()), "a failed dead-letter leaves the message nackable")
	again := recv(t, out)
	assert.Equal(t, "poison", string(again.GetBody()))
	require.Nil(t, p.Ack(ctx, again.GetUuid()))
}

// TestDeadLetterFailedTermKeepsMessageResolvable: if the DLQ copy is stored but
// Term fails, DeadLetter must leave the active-message entry in place. The
// server resolves its fallback nack (server.go, on a DeadLetter error) by
// uuid, so deleting the entry before Term succeeds would make that nack a
// no-op and strand the original ack-pending until AckWait, instead of
// redelivering it promptly.
func TestDeadLetterFailedTermKeepsMessageResolvable(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.dlterm", Subjects: []string{"job"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("poison")}))

	out := make(chan *pb.Message, 1)
	src := queueSource("events.dlterm.consumer", "events.dlterm", "job")
	src.Options["DeadLetterAddress"] = "events.dlterm.dlq"
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	m := recv(t, out)

	// Force Term to fail without breaking the DLQ publish: ack the underlying
	// message directly (marking it already-acked), so DeadLetter's DLQ publish
	// still lands but its subsequent Term returns ErrMsgAlreadyAckd.
	bd := p.getBrokerDetailsByIdentifier("test-client")
	mu, ok := bd.activeMessages.Get(m.GetUuid())
	require.True(t, ok)
	require.NoError(t, mu.(inflightMsg).msg.Ack())

	perr := p.DeadLetter(ctx, src, m.GetUuid())
	require.NotNil(t, perr, "DeadLetter must surface the Term failure")

	// The DLQ copy was stored...
	dlqStats := p.SourceStats(ctx, &pb.Source{Name: "x", Address: &pb.Address{Name: "events.dlterm.dlq"}})
	assert.Equal(t, int64(1), dlqStats.GetMessageCount(), "the DLQ copy must still have been published")

	// ...but the original stays resolvable so the server's fallback nack lands.
	_, stillThere := bd.activeMessages.Get(m.GetUuid())
	assert.True(t, stillThere,
		"a failed Term must not remove the active-message entry the fallback nack needs")
}

// TestPrefetchMapsToMaxAckPending: a positive PrefetchCount becomes the
// consumer's MaxAckPending. A prefetch of 0 is the AMQP "unlimited"
// convention — the amqp091 connector honors it by leaving the channel
// default, which is unlimited — so it maps to JetStream's unlimited (-1),
// not to a one-message-at-a-time clamp.
// A stream can be deleted out from under a live connection — an operator
// reset, a storage wipe, external cleanup — without the NATS connection
// noticing, and knownStreams pins the "already ensured" answer for the
// connection's lifetime. Publish, Subscribe, and the dead-letter path must
// detect the resulting no-stream errors, drop the stale entry, and re-create
// the stream instead of failing every call until the client reconnects.
func TestPublishRecreatesDeletedStream(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.wiped", Subjects: []string{"created"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("before")}))

	bd := p.getBrokerDetailsByIdentifier("test-client")
	require.NoError(t, bd.js.DeleteStream(ctx, streamNameFor("events.wiped")))

	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("after")}),
		"publish must re-ensure the stream instead of failing on the stale memo")

	stream, serr := bd.js.Stream(ctx, streamNameFor("events.wiped"))
	require.NoError(t, serr, "the stream must have been re-created")
	assert.Equal(t, uint64(1), stream.CachedInfo().State.Msgs, "the retried publish must be stored")
}

func TestSubscribeRecreatesDeletedStream(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.wiped2", Subjects: []string{"created"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("before")}))
	bd := p.getBrokerDetailsByIdentifier("test-client")
	require.NoError(t, bd.js.DeleteStream(ctx, streamNameFor("events.wiped2")))

	out := make(chan *pb.Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, queueSource("events.wiped2.consumer", "events.wiped2", "created"), out)

	// The subscribe must have re-created the stream and its consumer; without
	// the heal it returns fatal "stream not found" and nothing ever consumes.
	require.Eventually(t, func() bool {
		stream, serr := bd.js.Stream(ctx, streamNameFor("events.wiped2"))
		return serr == nil && stream.CachedInfo().State.Consumers > 0
	}, 10*time.Second, 50*time.Millisecond, "subscribe must re-create the deleted stream")

	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("after")}))
	m := recv(t, out)
	assert.Equal(t, "after", string(m.GetBody()))
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
}

func TestDeadLetterRecoversDeletedDLQStream(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.dlsrc", Subjects: []string{"job"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("p1")}))
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("p2")}))

	out := make(chan *pb.Message, 2)
	src := queueSource("events.dlsrc.consumer", "events.dlsrc", "job")
	src.Options["DeadLetterAddress"] = "events.dlq.wiped"
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	first := recv(t, out)
	require.Nil(t, p.DeadLetter(ctx, src, first.GetUuid()), "first dead-letter creates the DLQ stream")

	bd := p.getBrokerDetailsByIdentifier("test-client")
	require.NoError(t, bd.js.DeleteStream(ctx, streamNameFor("events.dlq.wiped")))

	second := recv(t, out)
	require.NotNil(t, p.DeadLetter(ctx, src, second.GetUuid()),
		"the publish into the deleted DLQ stream must fail")
	// The failed call dropped the stale memo, so this retry — the message is
	// still in flight and resolvable, per the dead-letter failure contract —
	// re-creates the DLQ stream and succeeds.
	require.Nil(t, p.DeadLetter(ctx, src, second.GetUuid()),
		"the dead-letter retry must re-create the DLQ stream")
}

// TestSubscribeEndsWhenConsumerDeleted: deleting a subscription's server-side
// consumer out from under it (an operator cleanup, a misdirected tool) used
// to leave the subscription permanently deaf — the client library treats the
// resulting consume error as terminal and silently stops delivering, while
// Subscribe kept blocking on its context. Subscribe must instead end with a
// non-fatal error so the client's re-subscribe recreates the consumer.
func TestSubscribeEndsWhenConsumerDeleted(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.zap", Subjects: []string{"k"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m1")}))

	src := queueSource("events.zap.consumer", "events.zap", "k")
	out := make(chan *pb.Message, 2)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan *pb.Error, 1)
	go func() { done <- p.Subscribe(subCtx, src, out) }()

	m := recv(t, out)
	require.Nil(t, p.Ack(ctx, m.GetUuid()))

	bd := p.getBrokerDetailsByIdentifier("test-client")
	stream, serr := bd.js.Stream(ctx, streamNameFor("events.zap"))
	require.NoError(t, serr)
	require.NoError(t, stream.DeleteConsumer(ctx, durableName(src)))

	select {
	case perr := <-done:
		require.NotNil(t, perr, "Subscribe must end with an error, not block forever")
		assert.False(t, perr.GetIsFatal(), "consumer loss must be retriable (non-fatal)")
	case <-time.After(30 * time.Second):
		t.Fatal("Subscribe kept blocking after its consumer was deleted")
	}

	// The re-subscribe — the client's reaction to the ended stream —
	// recreates the durable and delivery resumes. (Offset "first" replays the
	// retained backlog into the fresh durable, so drain until the new
	// message arrives.)
	out2 := make(chan *pb.Message, 4)
	go p.Subscribe(subCtx, src, out2)
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m2")}))
	for {
		m2 := recv(t, out2)
		require.Nil(t, p.Ack(ctx, m2.GetUuid()))
		if string(m2.GetBody()) == "m2" {
			break
		}
	}
}

// TestSubscribeRecoversAfterServerStateWipe: a broker restart that loses its
// JetStream state (storage wipe, factory reset) does not sever a NATS client
// the way it severs an AMQP client — the connection reconnects transparently
// and the subscription keeps pulling a consumer that no longer exists. No
// terminal error ever arrives on the consume path: pulls find no responder
// and heartbeats go missing, indefinitely. The existence probe must notice
// the consumer is gone and end the subscription (non-fatal), so the client's
// re-subscribe rebuilds stream and consumer.
func TestSubscribeRecoversAfterServerStateWipe(t *testing.T) {
	s := runJetStreamServer(t)
	port := s.Addr().(*net.TCPAddr).Port
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.wipe3", Subjects: []string{"k"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m1")}))

	src := queueSource("events.wipe3.consumer", "events.wipe3", "k")
	out := make(chan *pb.Message, 2)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan *pb.Error, 1)
	go func() { done <- p.Subscribe(subCtx, src, out) }()
	m := recv(t, out)
	require.Nil(t, p.Ack(ctx, m.GetUuid()))

	// Restart the broker at the same address with an empty store. The client
	// reconnects on its own; stream and consumer are gone server-side.
	s.Shutdown()
	s.WaitForShutdown()
	runJetStreamServerAt(t, port)

	select {
	case perr := <-done:
		require.NotNil(t, perr, "Subscribe must end with an error, not starve silently")
		assert.False(t, perr.GetIsFatal(), "state loss must be retriable (non-fatal)")
	case <-time.After(60 * time.Second):
		t.Fatal("Subscribe kept blocking after the broker lost its JetStream state")
	}

	// The re-subscribe rebuilds the whole topology on the wiped broker and
	// delivery resumes.
	out2 := make(chan *pb.Message, 2)
	go p.Subscribe(subCtx, src, out2)
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m2")}))
	m2 := recv(t, out2)
	assert.Equal(t, "m2", string(m2.GetBody()))
	require.Nil(t, p.Ack(ctx, m2.GetUuid()))
}

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

func TestPublishIDRequiresPublisherName(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	perr := p.PublishOne(ctx, &pb.Message{
		Address:   &pb.Address{Name: "events.dedup.required", Subjects: []string{"e"}},
		Body:      []byte("x"),
		PublishId: 1,
	})
	require.NotNil(t, perr)
	assert.Contains(t, perr.GetMessage(), "PublisherName not set")
}

func TestPublisherNameWithoutPublishIDDoesNotDedup(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.dedup.nameonly", Subjects: []string{"e"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("a"), PublisherName: "pub"}))
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("b"), PublisherName: "pub"}))

	stats := p.SourceStats(ctx, &pb.Source{
		Name:    "events.dedup.nameonly.consumer",
		Address: &pb.Address{Name: "events.dedup.nameonly"},
	})
	assert.Nil(t, stats.GetError())
	assert.Equal(t, int64(2), stats.GetMessageCount(), "publisher_name alone must not collapse messages as pub-0")
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

// TestConflictingHeaderFiltersOnDurableRejected: header filters are evaluated
// proxy-side per consumer, so two competing subscribers of one durable that
// disagree would each drop the other's messages (JetStream hands a message to
// one of them, and if it does not match that one's filter it is acked and
// lost). RabbitMQ header bindings are queue-wide and cannot diverge like this,
// so reject the conflicting declaration instead of silently losing messages.
// A second subscriber whose header filter matches is still allowed (competing
// consumers with a shared filter are legitimate).
func TestConflictingHeaderFiltersOnDurableRejected(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.hfconflict", Subjects: []string{"e"}}
	mk := func(region string) *pb.Source {
		src := queueSource("events.hfconflict.consumer", "events.hfconflict", "e")
		src.Filters = []*pb.Filter{{
			Type:    pb.Filter_ALL,
			Matches: []*pb.Match{{Name: "region", Value: region}},
		}}
		return src
	}

	// Subscriber A (region=us) claims the durable and stays live; receiving a
	// matching message proves it is past the claim.
	outA := make(chan *pb.Message, 1)
	ctxA, cancelA := context.WithCancel(ctx)
	defer cancelA()
	go p.Subscribe(ctxA, mk("us"), outA)
	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: addr, Body: []byte("k"), Headers: map[string]string{"region": "us"}}))
	require.Nil(t, p.Ack(ctx, recv(t, outA).GetUuid()))

	// B tries to share the same durable with a different header filter: rejected.
	perr := p.Subscribe(ctx, mk("eu"), make(chan *pb.Message, 1))
	require.NotNil(t, perr, "a conflicting header filter on a shared durable must be rejected")
	assert.True(t, perr.GetIsFatal(), "the rejection is a client misconfiguration, not retriable")
	assert.Contains(t, perr.GetMessage(), "header filter")

	// A matching second subscriber is accepted (does not reject).
	outC := make(chan *pb.Message, 1)
	ctxC, cancelC := context.WithCancel(ctx)
	defer cancelC()
	errC := make(chan *pb.Error, 1)
	go func() { errC <- p.Subscribe(ctxC, mk("us"), outC) }()
	select {
	case perr := <-errC:
		t.Fatalf("a matching header filter must not be rejected: %s", perr.GetMessage())
	case <-time.After(500 * time.Millisecond):
	}

	// Once every subscriber of the durable ends, a fresh filter is allowed again
	// (the fingerprint is forgotten when the last reference drops).
	cancelA()
	cancelC()
	require.Eventually(t, func() bool {
		return p.Subscribe(context.Background(), func() *pb.Source {
			s := mk("eu")
			s.DeclareOnly = true
			return s
		}(), nil) == nil
	}, 5*time.Second, 50*time.Millisecond,
		"a new filter must be accepted after the durable's subscribers all leave")
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

func TestAckWaitFromEnv(t *testing.T) {
	t.Setenv("NATSJS_ACK_WAIT", "5m")
	assert.Equal(t, 5*time.Minute, ackWait())
	t.Setenv("NATSJS_ACK_WAIT", "bad")
	assert.Equal(t, defaultAckWait, ackWait())
	t.Setenv("NATSJS_ACK_WAIT", "0")
	assert.Equal(t, defaultAckWait, ackWait())
}

func TestSACPinnedTTLFromEnv(t *testing.T) {
	t.Setenv("NATSJS_SAC_PINNED_TTL", "10s")
	assert.Equal(t, 10*time.Second, sacPinnedTTL())
	t.Setenv("NATSJS_SAC_PINNED_TTL", "bad")
	assert.Equal(t, defaultSACPinnedTTL, sacPinnedTTL())
	t.Setenv("NATSJS_SAC_PINNED_TTL", "0")
	assert.Equal(t, defaultSACPinnedTTL, sacPinnedTTL())
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

// TestStreamEnsureKeyScopedToCredentials: the registry must never coalesce
// two connections that share a broker endpoint but authenticate as different
// users — on a multi-account server those are different accounts with
// disjoint JetStream state and permissions, so sharing one
// CreateOrUpdateStream call would hand one account the other's outcome
// (success in the wrong account, or a permission failure that is not its
// own).
func TestStreamEnsureKeyScopedToCredentials(t *testing.T) {
	cfg := func(user string) *pb.ConnectionConfiguration {
		c := &pb.ConnectionConfiguration{Host: "nats", Port: 4222}
		if user != "" {
			c.Credentials = &pb.Credentials{Username: user, Password: "s"}
		}
		return c
	}
	assert.Equal(t, streamEnsureKey(cfg("a"), "arke_x"), streamEnsureKey(cfg("a"), "arke_x"),
		"same endpoint, account, and stream must coalesce")
	assert.NotEqual(t, streamEnsureKey(cfg("a"), "arke_x"), streamEnsureKey(cfg("b"), "arke_x"),
		"different accounts must not share an ensure result")
	assert.NotEqual(t, streamEnsureKey(cfg(""), "arke_x"), streamEnsureKey(cfg("a"), "arke_x"),
		"anonymous and authenticated connections must not share")
	assert.NotEqual(t, streamEnsureKey(cfg("a"), "arke_x"), streamEnsureKey(cfg("a"), "arke_y"),
		"different streams must not share")
	other := cfg("a")
	other.Port = 4223
	assert.NotEqual(t, streamEnsureKey(cfg("a"), "arke_x"), streamEnsureKey(other, "arke_x"),
		"different endpoints must not share")
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

// TestUnderscoreAddressesCoexist: address names "events.a" and "events_a" are
// distinct AMQP exchanges, but a naive dots-to-underscores stream name maps
// both onto one JetStream stream, whose config each address's re-ensure then
// flips to its own subjects — each breaking the other. The name mapping must
// keep them on separate streams.
func TestUnderscoreAddressesCoexist(t *testing.T) { //nolint:dupl // deliberately parallel to TestEscapedAddressesCoexist
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	dotted := &pb.Address{Name: "events.a", Subjects: []string{"k"}}
	scored := &pb.Address{Name: "events_a", Subjects: []string{"k"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: dotted, Body: []byte("to-dotted")}))
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: scored, Body: []byte("to-scored")}),
		"publishing to events_a after events.a must not hijack its stream")

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for addr, want := range map[string]string{"events.a": "to-dotted", "events_a": "to-scored"} {
		out := make(chan *pb.Message, 2)
		go p.Subscribe(subCtx, queueSource(addr+".consumer", addr, "k"), out)
		m := recv(t, out)
		assert.Equal(t, want, string(m.GetBody()), "consumer on %q sees its own message", addr)
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
		select {
		case extra := <-out:
			t.Fatalf("unexpected cross-address delivery on %q: %q", addr, extra.GetBody())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// TestEscapedAddressesCoexist: address names "evt.~.b" and "evt._.b" are
// distinct AMQP exchanges, but a lossy token sanitizer mapped both onto the
// root "evt._.b" — one shared subject space and one shared stream, so each
// address received the other's messages and each ensure reconfigured the
// other's stream. Escaped tokens must keep them fully apart.
func TestEscapedAddressesCoexist(t *testing.T) { //nolint:dupl // deliberately parallel to TestUnderscoreAddressesCoexist
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	tilde := &pb.Address{Name: "evt.~.b", Subjects: []string{"k"}}
	scored := &pb.Address{Name: "evt._.b", Subjects: []string{"k"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: tilde, Body: []byte("to-tilde")}))
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: scored, Body: []byte("to-scored")}),
		"publishing to evt._.b after evt.~.b must not land on its stream")

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for addr, want := range map[string]string{"evt.~.b": "to-tilde", "evt._.b": "to-scored"} {
		out := make(chan *pb.Message, 2)
		go p.Subscribe(subCtx, queueSource(addr+".consumer", addr, "k"), out)
		m := recv(t, out)
		assert.Equal(t, want, string(m.GetBody()), "consumer on %q sees its own message", addr)
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
		select {
		case extra := <-out:
			t.Fatalf("unexpected cross-address delivery on %q: %q", addr, extra.GetBody())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// TestEscapedRoutingKeysStayDistinct: routing keys "a b" and "a_b" are
// distinct on a direct exchange, but the lossy sanitizer merged both onto
// "a_b", so a queue bound to "a_b" also received every "a b" message.
func TestEscapedRoutingKeysStayDistinct(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	pub := func(rk, body string) {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{
			Address: &pb.Address{Name: "evt.direct", Subjects: []string{rk}},
			Body:    []byte(body),
		}))
	}
	pub("a b", "spaced")
	pub("a_b", "scored")

	src := queueSource("evt.direct.consumer", "evt.direct", "a_b")
	src.Address.Type = pb.Address_QUEUE
	out := make(chan *pb.Message, 2)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	m := recv(t, out)
	assert.Equal(t, "scored", string(m.GetBody()), `binding "a_b" must match only routing key "a_b"`)
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
	select {
	case extra := <-out:
		t.Fatalf("binding %q wrongly matched routing key %q message", "a_b", string(extra.GetBody()))
	case <-time.After(500 * time.Millisecond):
	}
}

// TestUnderscoreSourceNamesStayIndependent: two queues named "grp.a" and
// "grp_a" bound to the same address are independent queues — each receives
// every message — but a naive name mapping collapses them onto one durable,
// turning them into competing consumers that split the traffic.
func TestUnderscoreSourceNamesStayIndependent(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.split", Subjects: []string{"k"}}
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	outDot := make(chan *pb.Message, 2)
	outScore := make(chan *pb.Message, 2)
	go p.Subscribe(subCtx, queueSource("grp.a", "events.split", "k"), outDot)
	go p.Subscribe(subCtx, queueSource("grp_a", "events.split", "k"), outScore)

	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("fan-out")}))

	for name, out := range map[string]<-chan *pb.Message{"grp.a": outDot, "grp_a": outScore} {
		m := recv(t, out)
		assert.Equal(t, "fan-out", string(m.GetBody()), "queue %q gets its own copy", name)
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
	}
}

// TestSameSourceNameSubscriptionsTrackedSeparately: consumer groups share one
// source name and differ only by ConsumerGroup, so a single connection can
// hold several live subscriptions for the same name. Each must keep its own
// bookkeeping entry: keyed by name alone, the second Add overwrites the
// first, Stats undercounts the connection's streams, and the first teardown
// deletes the survivor's entry (so Disconnect would no longer Stop it).
func TestSameSourceNameSubscriptionsTrackedSeparately(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	src := func(cg string) *pb.Source {
		return &pb.Source{
			Name:    "events.shared.consumer",
			Type:    pb.Source_STREAM,
			Address: &pb.Address{Name: "events.shared", Subjects: []string{"e"}},
			Options: map[string]string{"Offset": "first", "ConsumerGroup": cg},
		}
	}

	outA := make(chan *pb.Message, 2)
	outB := make(chan *pb.Message, 2)
	ctxA, cancelA := context.WithCancel(ctx)
	ctxB, cancelB := context.WithCancel(ctx)
	defer cancelB()
	errA := make(chan *pb.Error, 1)
	go func() { errA <- p.Subscribe(ctxA, src("grp.a"), outA) }()
	go p.Subscribe(ctxB, src("grp.b"), outB)

	// Each group's durable receives the message, proving both subscriptions
	// are live and consuming.
	addr := &pb.Address{Name: "events.shared", Subjects: []string{"e"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("x")}))
	require.Nil(t, p.Ack(ctx, recv(t, outA).GetUuid()))
	require.Nil(t, p.Ack(ctx, recv(t, outB).GetUuid()))

	require.Eventually(t, func() bool {
		stats := p.Stats()
		return len(stats.Clients) == 1 && stats.Clients[0].Streams == 2
	}, 5*time.Second, 20*time.Millisecond,
		"two live subscriptions sharing a source name must both be tracked")

	// Ending one subscription removes only its own entry.
	cancelA()
	require.Nil(t, <-errA)
	stats := p.Stats()
	require.Len(t, stats.Clients, 1)
	assert.Equal(t, 1, stats.Clients[0].Streams,
		"the sibling's teardown must not drop the survivor's entry")
}

// streamSource builds a stream-typed source (Source_STREAM) positioned at the
// given RabbitMQ Streams offset ("first"/"next"/...). It carries a
// ConsumerGroup, derived from the source name, because the group is a stream
// source's durable identity — each test consumer keeps its own durable. See
// TestStreamSourceWithoutGroupReadsIndependently for the group-less
// (ephemeral) form.
func streamSource(name, addr, offset string, subjects ...string) *pb.Source {
	return &pb.Source{
		Name:    name,
		Type:    pb.Source_STREAM,
		Address: &pb.Address{Name: addr, Subjects: subjects},
		Options: map[string]string{"Offset": offset, "ConsumerGroup": name + ".grp"},
	}
}

// TestStreamSourceWithoutGroupReadsIndependently: a STREAM source with no
// ConsumerGroup has no durable identity, so every subscriber is an
// independent ephemeral reader positioned by its own Offset and sees every
// message. That is deliberate RabbitMQ parity — stream consumers read a
// shared log and never compete (unlike queue consumers) — distinct from the
// transient-source fan-out documented as a known limitation.
func TestStreamSourceWithoutGroupReadsIndependently(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.reader", Subjects: []string{"e"}}
	const n = 3
	for i := 0; i < n; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte(fmt.Sprintf("m%d", i))}))
	}

	src := func() *pb.Source {
		return &pb.Source{
			Name:    "events.reader.consumer",
			Type:    pb.Source_STREAM,
			Address: &pb.Address{Name: "events.reader", Subjects: []string{"e"}},
			Options: map[string]string{"Offset": "first"},
		}
	}

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	outA := make(chan *pb.Message, n)
	outB := make(chan *pb.Message, n)
	go p.Subscribe(subCtx, src(), outA)
	go p.Subscribe(subCtx, src(), outB)
	for _, out := range []<-chan *pb.Message{outA, outB} {
		for i := 0; i < n; i++ {
			m := recv(t, out)
			assert.Equal(t, fmt.Sprintf("m%d", i), string(m.GetBody()),
				"each reader replays the full log in order")
			require.Nil(t, p.Ack(ctx, m.GetUuid()))
		}
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

func TestDirectAddressBindingIsExact(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	src := &pb.Source{
		Name: "events.direct.consumer",
		Address: &pb.Address{
			Name:     "events.direct",
			Type:     pb.Address_QUEUE,
			Subjects: []string{"#"},
		},
		Options: map[string]string{"Offset": "first"},
	}
	out := make(chan *pb.Message, 2)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: &pb.Address{Name: "events.direct", Type: pb.Address_QUEUE, Subjects: []string{"actual"}},
		Body:    []byte("wildcard-leak"),
	}))
	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: &pb.Address{Name: "events.direct", Type: pb.Address_QUEUE, Subjects: []string{"#"}},
		Body:    []byte("literal-match"),
	}))

	m := recv(t, out)
	assert.Equal(t, "literal-match", string(m.GetBody()), "direct bindings must not treat # as a topic wildcard")
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
	select {
	case extra := <-out:
		t.Fatalf("unexpected direct-address wildcard delivery: %q", string(extra.GetBody()))
	case <-time.After(500 * time.Millisecond):
	}
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
// Streams are shared per address root, so the stream-wide consumer count
// spans every source on the address. A durable source's ConsumerCount must
// instead reflect the clients attached to ITS consumer — RabbitMQ reports the
// queue's own consumer count — or two queues on one exchange each report the
// other's consumers.
func TestSourceStatsConsumerCountIsPerSource(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	srcA := queueSource("events.cc.a", "events.cc", "created")
	srcB := queueSource("events.cc.b", "events.cc", "created")
	outA := make(chan *pb.Message, 1)
	outB := make(chan *pb.Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, srcA, outA)
	go p.Subscribe(subCtx, srcB, outB)

	// Wait for both subscriptions' pulls to register, then check that each
	// source counts only its own attached client. Pre-fix both report the
	// stream-wide count of 2.
	require.Eventually(t, func() bool {
		return p.SourceStats(ctx, srcA).GetConsumerCount() == 1 &&
			p.SourceStats(ctx, srcB).GetConsumerCount() == 1
	}, 10*time.Second, 100*time.Millisecond,
		"each durable source must report exactly its own attached client")
}

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

// TestTeardownReleasesInFlightMessages: when a subscription ends, its
// delivered-but-unresolved messages are removed from the active-message map
// (their acks can only arrive on the consume stream that just closed, so they
// could never be resolved again — a leak for the life of the connection) and
// nak'd, so a durable redelivers them promptly instead of after the full
// AckWait, matching RabbitMQ's immediate requeue of a closed channel's
// unacked deliveries.
func TestTeardownReleasesInFlightMessages(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.inflight", Subjects: []string{"e"}}
	const n = 3
	for i := 0; i < n; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte(fmt.Sprintf("m%d", i))}))
	}

	// First subscription receives the backlog but never resolves it.
	src := queueSource("events.inflight.consumer", "events.inflight", "e")
	out := make(chan *pb.Message, n)
	subCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan *pb.Error, 1)
	go func() { errCh <- p.Subscribe(subCtx, src, out) }()
	for i := 0; i < n; i++ {
		recv(t, out) // deliberately left unacked
	}
	cancel()
	require.Nil(t, <-errCh)

	// The abandoned deliveries are no longer tracked...
	stats := p.Stats()
	require.Len(t, stats.Clients, 1)
	assert.Zero(t, stats.Clients[0].ActiveMessages,
		"teardown must release the subscription's in-flight messages")

	// ...and a re-subscribe gets them back well before AckWait (30s) because
	// teardown nak'd them (recv's 10s timeout is the proof of promptness).
	out2 := make(chan *pb.Message, n)
	subCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	go p.Subscribe(subCtx2, src, out2)
	for i := 0; i < n; i++ {
		m := recv(t, out2)
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
	}
}

// TestTeardownReleasesOnlyItsOwnInFlight: two subscriptions with the same
// (durable) source name attach to one server-side consumer and compete, like
// two service instances on a shared queue. When the idle sibling ends, teardown
// must release only its own deliveries — not nak and drop the message the other
// subscription is still holding, which would fail that subscription's ack and
// redeliver the message as a duplicate.
func TestTeardownReleasesOnlyItsOwnInFlight(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.siblings", Subjects: []string{"e"}}
	src := queueSource("events.siblings.consumer", "events.siblings", "e")

	// Subscription A takes one message and holds it unacked.
	outA := make(chan *pb.Message, 1)
	ctxA, cancelA := context.WithCancel(ctx)
	defer cancelA()
	errA := make(chan *pb.Error, 1)
	go func() { errA <- p.Subscribe(ctxA, src, outA) }()
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("held")}))
	held := recv(t, outA) // delivered to A, deliberately left unacked

	// Subscription B joins the same durable as a competing sibling, then ends
	// without ever receiving anything of its own.
	outB := make(chan *pb.Message, 1)
	ctxB, cancelB := context.WithCancel(ctx)
	errB := make(chan *pb.Error, 1)
	go func() { errB <- p.Subscribe(ctxB, src, outB) }()
	time.Sleep(400 * time.Millisecond) // let B's pull reach the server
	cancelB()
	require.Nil(t, <-errB)

	// A's held message must still resolve — the sibling teardown owned none of it.
	require.Nil(t, p.Ack(ctx, held.GetUuid()),
		"a sibling subscription's teardown must not release this subscription's in-flight message")

	// And it must not have been redelivered (which a wrongful nak would cause).
	select {
	case dup := <-outA:
		t.Fatalf("message redelivered after a sibling teardown: %q", dup.GetBody())
	case <-time.After(300 * time.Millisecond):
	}
}

// TestRedundantBindingsCoexist: an AMQP binding set may be redundant — a
// wildcard binding alongside a specific key it already covers — and RabbitMQ
// routes each message to the queue once no matter how many bindings match.
// JetStream rejects a consumer whose filter subjects overlap, so the
// connector must collapse the redundant filters instead of failing the
// subscribe (and each message still arrives exactly once).
func TestRedundantBindingsCoexist(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	pub := func(key, body string) {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{
			Address: &pb.Address{Name: "events.redundant", Subjects: []string{key}},
			Body:    []byte(body),
		}))
	}
	pub("orders.created", "covered-by-both")
	pub("other.thing", "covered-by-wildcard")

	out := make(chan *pb.Message, 4)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan *pb.Error, 1)
	go func() {
		errCh <- p.Subscribe(subCtx,
			queueSource("events.redundant.consumer", "events.redundant", "#", "orders.created"), out)
	}()

	got := map[string]int{}
	for i := 0; i < 2; i++ {
		m := recv(t, out)
		got[string(m.GetBody())]++
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
	}
	assert.Equal(t, map[string]int{"covered-by-both": 1, "covered-by-wildcard": 1}, got)

	select {
	case extra := <-out:
		t.Fatalf("message delivered twice: %q", extra.GetBody())
	case perr := <-errCh:
		t.Fatalf("subscribe failed on redundant bindings: %s", perr.GetMessage())
	case <-time.After(500 * time.Millisecond):
	}
}

// sacSource is queueSource with SingleActiveConsumer set.
func sacSource(name, addr string, subjects ...string) *pb.Source {
	s := queueSource(name, addr, subjects...)
	s.SingleActiveConsumer = true
	return s
}

// TestSingleActiveConsumerPinsOneClient: a SingleActiveConsumer source maps to
// a pinned-client priority group, so with two live subscribers exactly one
// receives (RabbitMQ SAC semantics — ordered processing across competing
// instances) and the standby takes over after the active one goes away and
// its pin expires.
func TestSingleActiveConsumerPinsOneClient(t *testing.T) {
	t.Setenv("NATSJS_SAC_PINNED_TTL", "1s") // fast failover for the test
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.sac", Subjects: []string{"e"}}
	src := sacSource("events.sac.consumer", "events.sac", "e")

	// The first subscriber pulls alone, so it takes the pin.
	outA := make(chan *pb.Message, 8)
	ctxA, cancelA := context.WithCancel(ctx)
	errA := make(chan *pb.Error, 1)
	go func() { errA <- p.Subscribe(ctxA, src, outA) }()

	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m0")}))
	m := recv(t, outA)
	assert.Equal(t, "m0", string(m.GetBody()))
	require.Nil(t, p.Ack(ctx, m.GetUuid()))

	// A standby joins. Everything published while the pin holder is alive
	// must keep going to the pin holder only.
	outB := make(chan *pb.Message, 8)
	ctxB, cancelB := context.WithCancel(ctx)
	defer cancelB()
	go p.Subscribe(ctxB, src, outB)
	time.Sleep(500 * time.Millisecond) // let the standby's pull reach the server

	for i := 1; i <= 3; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte(fmt.Sprintf("m%d", i))}))
	}
	for i := 1; i <= 3; i++ {
		m := recv(t, outA)
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
	}
	select {
	case got := <-outB:
		t.Fatalf("standby received %q while another consumer held the pin", got.GetBody())
	case <-time.After(500 * time.Millisecond):
	}

	// The active subscriber goes away; once its pin expires (1s here) the
	// standby is pinned and receives.
	cancelA()
	require.Nil(t, <-errA)
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m4")}))
	m = recv(t, outB)
	assert.Equal(t, "m4", string(m.GetBody()), "standby takes over after the active consumer goes away")
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
}

// TestSingleActiveConsumerUpgradesExistingDurable: priority-group config is
// update-mutable, so a durable created before SingleActiveConsumer was set on
// the source is upgraded in place — same name, same ack position — instead of
// failing the subscribe.
func TestSingleActiveConsumerUpgradesExistingDurable(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.sacup", Subjects: []string{"e"}}
	plain := queueSource("events.sacup.consumer", "events.sacup", "e")

	// Create the durable without single-active (an older deployment).
	out := make(chan *pb.Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan *pb.Error, 1)
	go func() { errCh <- p.Subscribe(subCtx, plain, out) }()
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m0")}))
	m := recv(t, out)
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
	cancel()
	require.Nil(t, <-errCh)

	// Re-subscribe with SingleActiveConsumer set: the consumer is updated in
	// place and keeps delivering from its stored position.
	sac := sacSource("events.sacup.consumer", "events.sacup", "e")
	out2 := make(chan *pb.Message, 1)
	subCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	go p.Subscribe(subCtx2, sac, out2)

	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m1")}))
	m = recv(t, out2)
	assert.Equal(t, "m1", string(m.GetBody()))
	require.Nil(t, p.Ack(ctx, m.GetUuid()))

	// The effective config now carries the pinned priority group.
	bd := p.getBrokerDetailsByIdentifier("test-client")
	stream, serr := bd.js.Stream(ctx, streamNameFor("events.sacup"))
	require.NoError(t, serr)
	cons, cerr := stream.Consumer(ctx, durableName(sac))
	require.NoError(t, cerr)
	assert.Equal(t, []string{sacPriorityGroup}, cons.CachedInfo().Config.PriorityGroups)
	assert.Equal(t, jetstream.PriorityPolicyPinned, cons.CachedInfo().Config.PriorityPolicy)
}

// TestSingleActiveConsumerGroupsAreIndependent: the ConsumerGroup option is
// the identity single-active instances coordinate through (amqp091 uses it as
// the consumer reference the same way), so two groups of a source — same
// source name, different ConsumerGroup — must get independent pinned durables
// that each receive the whole stream. Naming the durable after the source
// name instead would collapse every group onto one pinned consumer and starve
// all groups but the pin holder's.
func TestSingleActiveConsumerGroupsAreIndependent(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.sacgroups", Subjects: []string{"e"}}
	mkSrc := func(group string) *pb.Source {
		return &pb.Source{
			Name:                 "events.sacgroups.consumer", // shared across groups
			Type:                 pb.Source_STREAM,
			SingleActiveConsumer: true,
			Address:              &pb.Address{Name: "events.sacgroups", Subjects: []string{"e"}},
			Options:              map[string]string{"Offset": "first", "ConsumerGroup": "grp-" + group},
		}
	}

	const n = 2
	for i := 0; i < n; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte(fmt.Sprintf("m%d", i))}))
	}

	outA := make(chan *pb.Message, n)
	outB := make(chan *pb.Message, n)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, mkSrc("a"), outA)
	go p.Subscribe(subCtx, mkSrc("b"), outB)

	// Each group replays the full backlog independently.
	for i := 0; i < n; i++ {
		require.Nil(t, p.Ack(ctx, recv(t, outA).GetUuid()))
	}
	for i := 0; i < n; i++ {
		require.Nil(t, p.Ack(ctx, recv(t, outB).GetUuid()))
	}
}

// TestSingleActiveStreamSourceRequiresConsumerGroup: a single-active stream
// source names no ConsumerGroup to coordinate through — amqp091 rejects the
// combination, and so does natsjs, instead of inventing a per-subscriber
// durable that would make the instances compete.
func TestSingleActiveStreamSourceRequiresConsumerGroup(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	src := &pb.Source{
		Name:                 "events.sacnogroup.consumer",
		Type:                 pb.Source_STREAM,
		SingleActiveConsumer: true,
		Address:              &pb.Address{Name: "events.sacnogroup", Subjects: []string{"e"}},
	}
	perr := p.Subscribe(ctx, src, make(chan *pb.Message, 1))
	require.NotNil(t, perr)
	assert.Contains(t, perr.GetMessage(), "no ConsumerGroup option set")
	assert.True(t, perr.GetIsFatal())
}

// TestDeclareOnlyEphemeralConsumerCleanedUp: DeclareOnly on a transient source
// validates topology, but the ephemeral consumer it creates can never be
// attached to afterwards (its server-generated name is not surfaced), so it is
// deleted before Subscribe returns instead of lingering until the inactivity
// threshold.
func TestDeclareOnlyEphemeralConsumerCleanedUp(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	src := &pb.Source{
		Name:        "events.tmpdecl.listener",
		Type:        pb.Source_QUEUE,
		AutoDelete:  true, // transient -> ephemeral consumer
		DeclareOnly: true,
		Address:     &pb.Address{Name: "events.tmpdecl", Subjects: []string{"a"}},
	}
	require.Empty(t, durableName(src), "test source must map to an ephemeral consumer")
	require.Nil(t, p.Subscribe(ctx, src, nil))

	bd := p.getBrokerDetailsByIdentifier("test-client")
	stream, serr := bd.js.Stream(ctx, streamNameFor("events.tmpdecl"))
	require.NoError(t, serr)
	info, ierr := stream.Info(ctx)
	require.NoError(t, ierr)
	assert.Zero(t, info.State.Consumers, "declare-only ephemeral consumer must not linger")
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

// TestSourceStatsRatesIndependentPerSource: several sources commonly share
// one address (many queues bound to one exchange), and each polls its stats
// on its own cadence. The rate baselines must therefore be per source: with
// a shared per-stream key, any other source's poll resets the window, and a
// poll right after it reports the rate over that tiny gap — here, zero —
// instead of over the poller's own interval.
func TestSourceStatsRatesIndependentPerSource(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.ratesplit", Subjects: []string{"e"}}
	srcA := queueSource("events.ratesplit.a", "events.ratesplit", "e")
	srcB := queueSource("events.ratesplit.b", "events.ratesplit", "e")

	// Create the stream and baseline source A.
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("x")}))
	require.Nil(t, p.SourceStats(ctx, srcA).GetError())

	for i := 0; i < 5; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("y")}))
	}

	// Source B polls (its own baseline) between A's polls. A's next poll must
	// still see the 5 publishes inside its own window.
	require.Nil(t, p.SourceStats(ctx, srcB).GetError())
	time.Sleep(20 * time.Millisecond)

	stats := p.SourceStats(ctx, srcA)
	require.Nil(t, stats.GetError())
	assert.Positive(t, stats.GetPublishRate(),
		"another source's poll must not reset this source's rate window")
}

// TestSourceStatsRatesIndependentPerGroup: consumer groups share one source
// name and differ only by ConsumerGroup, so a name-keyed publish-rate sample
// makes the groups reset each other's baseline window exactly like the
// per-source case above — the sample key must carry the durable identity too.
func TestSourceStatsRatesIndependentPerGroup(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.grpsplit", Subjects: []string{"e"}}
	src := func(cg string) *pb.Source {
		return &pb.Source{
			Name:    "events.grpsplit.consumer",
			Type:    pb.Source_STREAM,
			Address: &pb.Address{Name: "events.grpsplit", Subjects: []string{"e"}},
			Options: map[string]string{"ConsumerGroup": cg},
		}
	}
	srcA, srcB := src("grp.a"), src("grp.b")

	// Create the stream and baseline group A.
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("x")}))
	require.Nil(t, p.SourceStats(ctx, srcA).GetError())

	for i := 0; i < 5; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("y")}))
	}

	// Group B polls (its own baseline) between A's polls. A's next poll must
	// still see the 5 publishes inside its own window.
	require.Nil(t, p.SourceStats(ctx, srcB).GetError())
	time.Sleep(20 * time.Millisecond)

	stats := p.SourceStats(ctx, srcA)
	require.Nil(t, stats.GetError())
	assert.Positive(t, stats.GetPublishRate(),
		"another group's poll must not reset this group's rate window")
}

func TestUnappliedSourceOptions(t *testing.T) {
	src := func(opts map[string]string) *pb.Source { return &pb.Source{Options: opts} }

	assert.Empty(t, unappliedSourceOptions(src(nil)))
	assert.Empty(t, unappliedSourceOptions(src(map[string]string{"Offset": "first", "MessageTTL": ""})))
	assert.Equal(t, []string{"MessageTTL"}, unappliedSourceOptions(src(map[string]string{"MessageTTL": "300000"})))
	assert.Equal(t, []string{"MessageTTL", "Expires"},
		unappliedSourceOptions(src(map[string]string{"MessageTTL": "300000", "Expires": "600000"})))
}
