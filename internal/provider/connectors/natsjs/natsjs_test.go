// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package natsjs

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/util/tracing"
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

// TestSubscribeOutlivesDisconnect: a client-initiated Disconnect must not end
// a live Subscribe — ending it ends the caller's whole consume stream, but
// the amqp091 connector leaves that stream open (its subscribe loop goes
// quiet when the AMQP connection closes), so a client that disconnects and
// then acks straggler messages gets per-ack failures, not end-of-stream.
// Subscribe may only return once the subscription's own context ends.
func TestSubscribeOutlivesDisconnect(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)
	bd := p.getBrokerDetailsByIdentifier("test-client")

	src := queueSource("events.disc.q", "events.disc", "e")
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan *pb.Error, 1)
	go func() { done <- p.Subscribe(subCtx, src, make(chan *pb.Message, 1)) }()

	require.Eventually(t, func() bool {
		stream, serr := bd.js.Stream(ctx, streamNameFor("events.disc"))
		if serr != nil {
			return false
		}
		info, ierr := stream.Info(ctx)
		return ierr == nil && info.State.Consumers == 1
	}, 10*time.Second, 20*time.Millisecond, "subscription never established")

	p.Disconnect(ctx)

	select {
	case serr := <-done:
		t.Fatalf("Subscribe ended on Disconnect (%v); it must stay open until the stream context ends", serr)
	case <-time.After(2 * time.Second):
	}

	cancel()
	select {
	case serr := <-done:
		assert.Nil(t, serr, "a disconnected subscription ends cleanly with its context")
	case <-time.After(10 * time.Second):
		t.Fatal("Subscribe did not return after its context ended")
	}
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

// TestRetryFailureKeepsMessageResolvable: if NakWithDelay fails, Retry must
// leave the active-message entry in place instead of deleting it up front
// (which a plain takeMessage would). The server falls back to nacking this
// same uuid when Retry returns an error (server.go, mirroring the existing
// DeadLetter fallback) — a fallback that can only resolve anything while the
// uuid stays in activeMessages. Deleting it first strands the message with
// neither a nak nor a term having reached the server, until AckWait expires
// on its own instead of redelivering promptly.
func TestRetryFailureKeepsMessageResolvable(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.retryfail", Subjects: []string{"job"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("work")}))

	out := make(chan *pb.Message, 1)
	src := queueSource("events.retryfail.consumer", "events.retryfail", "job")
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	m := recv(t, out)

	// Force NakWithDelay to fail: ack the underlying message directly,
	// bypassing the connector's own bookkeeping, so the entry stays in
	// activeMessages but the message itself is already resolved server-side.
	bd := p.getBrokerDetailsByIdentifier("test-client")
	mu, ok := bd.activeMessages.Get(m.GetUuid())
	require.True(t, ok)
	require.NoError(t, mu.(inflightMsg).msg.Ack())

	perr := p.Retry(ctx, src, m.GetUuid(), 1)
	require.NotNil(t, perr, "Retry must surface the NakWithDelay failure")

	entry, stillThere := bd.activeMessages.Get(m.GetUuid())
	require.True(t, stillThere,
		"a failed NakWithDelay must not remove the active-message entry the fallback nack needs")
	assert.True(t, entry.(inflightMsg).redeliverOnNack,
		"the fallback nack must put the message back, not term it, after a failed retry")
}

// TestNackDoesNotRedeliver pins Nack to amqp091's Delivery.Nack(requeue=false):
// a nacked message is rejected, not put back. Naking it instead (JetStream's
// Nak, which means redeliver now) turns one nacked message into an unbounded
// delivery loop, because the client nacks each redelivery straight back.
func TestNackDoesNotRedeliver(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.nack", Subjects: []string{"job"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("work")}))

	out := make(chan *pb.Message, 8)
	src := queueSource("events.nack.consumer", "events.nack", "job")
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	first := recv(t, out)
	require.Nil(t, p.Nack(ctx, first.GetUuid()))

	select {
	case m := <-out:
		t.Fatalf("nacked message was redelivered (uuid %s); Nack must not requeue", m.GetUuid())
	case <-time.After(2 * time.Second):
	}

	// Resolved server-side, not merely undelivered: nothing is left ack-pending
	// to come back once AckWait expires.
	stats := p.SourceStats(ctx, src)
	require.Nil(t, stats.GetError())
	require.EqualValues(t, 0, stats.GetMessageCount(), "nacked message still pending")
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
	// has a retry trail to carry. Retry (not Nack) is what asks for a
	// redelivery — Nack rejects the message outright.
	first := recv(t, out)
	require.Nil(t, p.Retry(ctx, src, first.GetUuid(), 0))
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

// TestDeadLetterPreservesRoutingKeyRoutedFromParent is the same contract for a
// message that reached the source through an address-to-address binding.
// Sourcing keeps the subject the message was published under, so it stays
// rooted at the PARENT address while the source names the child — and RabbitMQ
// preserves the original routing key through both the exchange-to-exchange
// route and the DLX move, so the DLQ copy must still carry it.
//
// Regression: the routing key was recovered by stripping the consuming
// address's own prefix, which a parent-rooted subject never matches, so every
// message routed in from a parent dead-lettered under an empty key — landing
// on the bare DLA subject where a DLQ consumer bound by routing key would
// never see it.
func TestDeadLetterPreservesRoutingKeyRoutedFromParent(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	parent := &pb.Address{Name: "events.dlrk.parent", Type: pb.Address_TOPIC}
	child := &pb.Address{
		Name:          "events.dlrk.child",
		Type:          pb.Address_TOPIC,
		Subjects:      []string{"region.us.created"},
		ParentAddress: parent,
	}

	out := make(chan *pb.Message, 1)
	src := &pb.Source{Name: "events.dlrk.child.consumer", Type: pb.Source_QUEUE, Address: child,
		Options: map[string]string{
			"Offset":            "first",
			"DeadLetterAddress": "events.dlrk.routed.dlq",
		}}
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)
	time.Sleep(500 * time.Millisecond)

	// Published to the PARENT: the binding sources it into the child's stream
	// under its original, parent-rooted subject.
	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: &pb.Address{Name: "events.dlrk.parent", Subjects: []string{"region.us.created"}},
		Body:    []byte("poison")}))

	m := recv(t, out)
	require.Nil(t, p.DeadLetter(ctx, src, m.GetUuid()))

	bd := p.getBrokerDetailsByIdentifier("test-client")
	dlqStream, serr := bd.js.Stream(ctx, streamNameFor("events.dlrk.routed.dlq"))
	require.NoError(t, serr)
	raw, gerr := dlqStream.GetMsg(ctx, 1)
	require.NoError(t, gerr)
	assert.Equal(t, "events.dlrk.routed.dlq.~.region.us.created", raw.Subject,
		"a message routed in from a parent must dead-letter under its original routing key")
}

// TestDeadLetterFailureKeepsMessage: when the dead-letter publish cannot happen
// (here the DLQ address's subject space is already claimed by a foreign
// stream, so ensuring its stream fails), DeadLetter must NOT Term the
// original. It returns an error and leaves the message in flight, so the
// caller's fallback nack still resolves the uuid and the message is
// redelivered — the failure mode is retry, not silent loss.
func TestDeadLetterFailureKeepsMessage(t *testing.T) {
	// The fallback nack of a failed dead-letter is paced (see
	// deadLetterRetryDelay); shorten it so the test does not wait it out.
	t.Setenv("NATSJS_DEADLETTER_RETRY_DELAY", "100ms")

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
	// The fallback nack of a failed dead-letter is paced (see
	// deadLetterRetryDelay); shorten it so the test does not wait it out.
	t.Setenv("NATSJS_DEADLETTER_RETRY_DELAY", "100ms")

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

// TestPublishOneStreamAddressRequiresDeclaredStream pins the amqp091 contract
// for unary publishes to a STREAM address: the stream must already have been
// declared by a reader (amqp091 routes these through the RabbitMQ Streams
// client, which refuses a stream nobody declared), so a typo'd address name
// errors instead of silently minting a junk stream that swallows the
// messages.
func TestPublishOneStreamAddressRequiresDeclaredStream(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.undeclared", Type: pb.Address_STREAM, Subjects: []string{"e"}}
	perr := p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m")})
	require.NotNil(t, perr, "publishing to an undeclared stream must fail")
	assert.Equal(t, fmt.Sprintf(streamMissingError, "events.undeclared"), perr.GetMessage())

	bd := p.getBrokerDetailsByIdentifier("test-client")
	_, serr := bd.js.Stream(ctx, streamNameFor("events.undeclared"))
	assert.ErrorIs(t, serr, jetstream.ErrStreamNotFound, "the refused publish must not create the stream")

	// Once a reader has declared the stream, the same publish succeeds.
	_, eerr := p.ensureStream(ctx, bd, "events.undeclared")
	require.NoError(t, eerr)
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m")}))
}

// TestPublishOneStreamAddressDeletedStreamNotRecreated is the STREAM-address
// counterpart of TestPublishRecreatesDeletedStream: the self-heal that
// re-ensures an auto-created stream must not resurrect one the operator
// deleted, because for a STREAM address the stream is the reader's declared
// topology — a RabbitMQ stream publisher errors after deletion too.
func TestPublishOneStreamAddressDeletedStreamNotRecreated(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.dropped", Type: pb.Address_STREAM, Subjects: []string{"e"}}
	bd := p.getBrokerDetailsByIdentifier("test-client")
	_, eerr := p.ensureStream(ctx, bd, "events.dropped")
	require.NoError(t, eerr)
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("before")}))

	require.NoError(t, bd.js.DeleteStream(ctx, streamNameFor("events.dropped")))

	perr := p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("after")})
	require.NotNil(t, perr, "a deleted stream must fail the publish, not be resurrected")
	assert.Equal(t, fmt.Sprintf(streamMissingError, "events.dropped"), perr.GetMessage())
	_, serr := bd.js.Stream(ctx, streamNameFor("events.dropped"))
	assert.ErrorIs(t, serr, jetstream.ErrStreamNotFound)
}

// TestPublishStreamingPathStillAutoCreatesStreamAddress pins the deliberate
// split: only the unary path requires a declared stream. amqp091's streaming
// Publish sends STREAM addresses over an auto-declared exchange and reports
// no error either, so the streaming path keeps auto-creating — the message is
// stored where amqp091 would drop it into an unbound exchange.
func TestPublishStreamingPathStillAutoCreatesStreamAddress(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	in := make(chan *pb.Message, 1)
	errChan := make(chan *pb.Error, 1)
	pubCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Publish(pubCtx, in, errChan)
	in <- &pb.Message{
		Address: &pb.Address{Name: "events.streamed", Type: pb.Address_STREAM, Subjects: []string{"e"}},
		Body:    []byte("m")}
	select {
	case e := <-errChan:
		require.Nil(t, e, "the streaming publish must auto-create the stream: %v", e.GetMessage())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the publish reply")
	}

	bd := p.getBrokerDetailsByIdentifier("test-client")
	stream, serr := bd.js.Stream(ctx, streamNameFor("events.streamed"))
	require.NoError(t, serr, "the stream must have been auto-created")
	assert.Equal(t, uint64(1), stream.CachedInfo().State.Msgs)
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

// TestExpiresSetsInactiveThreshold: the Expires option (AMQP x-expires —
// delete the queue after this many ms without consumers) must become the
// consumer's InactiveThreshold, for durables and ephemerals alike; unset
// keeps the defaults (durables never expire, transients get
// defaultInactiveThreshold). Pre-fix the value was accepted, warned about,
// and silently ignored: durables got no threshold and ephemerals always got
// the 5-minute default, so a client shortening or lengthening its queue's
// disuse lifetime diverged from amqp091 without any signal.
func TestExpiresSetsInactiveThreshold(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)
	bd := p.getBrokerDetailsByIdentifier("test-client")

	durableThreshold := func(name string, opts map[string]string) time.Duration {
		t.Helper()
		src := queueSource(name, "events.expires", "e")
		for k, v := range opts {
			src.Options[k] = v
		}
		src.DeclareOnly = true
		require.Nil(t, p.Subscribe(ctx, src, nil))
		stream, serr := bd.js.Stream(ctx, streamNameFor("events.expires"))
		require.NoError(t, serr)
		cons, cerr := stream.Consumer(ctx, durableName(src))
		require.NoError(t, cerr)
		return cons.CachedInfo().Config.InactiveThreshold
	}

	assert.Equal(t, 2*time.Minute,
		durableThreshold("events.expires.set", map[string]string{"Expires": "120000"}),
		"a durable's Expires must become its InactiveThreshold")
	assert.Zero(t, durableThreshold("events.expires.unset", nil),
		"a durable without Expires never expires")

	// Ephemeral consumers are deleted eagerly when their subscription ends,
	// so inspect the config while the subscription is live.
	ephemeralThreshold := func(name string, opts map[string]string) time.Duration {
		t.Helper()
		src := &pb.Source{
			Name:       name,
			Type:       pb.Source_QUEUE,
			AutoDelete: true, // transient -> ephemeral consumer
			Address:    &pb.Address{Name: "events.expires.tmp." + name, Subjects: []string{"e"}},
			Options:    opts,
		}
		require.Empty(t, durableName(src), "test source must map to an ephemeral consumer")
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go p.Subscribe(subCtx, src, make(chan *pb.Message, 1))
		var threshold time.Duration
		require.Eventually(t, func() bool {
			stream, serr := bd.js.Stream(ctx, streamNameFor(src.GetAddress().GetName()))
			if serr != nil {
				return false
			}
			infos := stream.ListConsumers(ctx)
			for info := range infos.Info() {
				threshold = info.Config.InactiveThreshold
				return true
			}
			return false
		}, 10*time.Second, 20*time.Millisecond, "ephemeral consumer never appeared")
		return threshold
	}

	assert.Equal(t, time.Minute,
		ephemeralThreshold("eph.set", map[string]string{"Expires": "60000"}),
		"a transient source's Expires must become its InactiveThreshold")
	assert.Equal(t, defaultInactiveThreshold, ephemeralThreshold("eph.unset", nil),
		"a transient source without Expires keeps the default threshold")
}

func TestSubscribeInvalidExpiresRejected(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	for _, bad := range []string{"10m", "0", "-5"} {
		src := queueSource("events.expires.bad", "events.expires.bad", "e")
		src.Options["Expires"] = bad
		src.DeclareOnly = true // the reject must come before any topology work
		serr := p.Subscribe(ctx, src, nil)
		require.NotNil(t, serr, "Expires=%q must be rejected", bad)
		assert.True(t, serr.GetIsFatal())
		assert.Contains(t, serr.GetMessage(), "value for Expires option must be")
	}
}

func TestDeduplication(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	// Dedup is a STREAM-address feature on both brokers — RabbitMQ gets it from
	// the Streams client, which is the only thing amqp091's publish paths reach
	// for a STREAM address — and a unary publish to one needs a declared stream.
	bd := p.getBrokerDetailsByIdentifier("test-client")
	_, eerr := p.ensureStream(ctx, bd, "events.dedup")
	require.NoError(t, eerr)

	addr := &pb.Address{Name: "events.dedup", Type: pb.Address_STREAM, Subjects: []string{"e"}}
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

	// A STREAM address: the only kind whose unary publish reaches the
	// PublisherName check at all, since every other type is refused outright
	// for asking for dedup (see TestPublishDedupRefusedOnEveryNonStreamAddress).
	bd := p.getBrokerDetailsByIdentifier("test-client")
	_, eerr := p.ensureStream(ctx, bd, "events.dedup.required")
	require.NoError(t, eerr)

	perr := p.PublishOne(ctx, &pb.Message{
		Address:   &pb.Address{Name: "events.dedup.required", Type: pb.Address_STREAM, Subjects: []string{"e"}},
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
	// Worded exactly as amqp091 words it: a client matching on the text must
	// not have to know which connector answered.
	assert.Equal(t, "No message with uuid does-not-exist", perr.GetMessage())
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
	// A numeric offset counts from 0 like a RabbitMQ Stream's, so it is one
	// less than the JetStream sequence it starts the consumer at.
	check("100", jetstream.DeliverByStartSequencePolicy, 101)
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

// TestStreamOffsetContinueResumes: offset "continue" resumes a group-less
// stream source where its last subscription stopped, instead of replaying the
// log from the start like "first". amqp091 answers "continue" from RabbitMQ
// Streams' server-side offset tracking (QueryOffset by consumer name, +1);
// here the source's durable holds the position, so a re-subscribe under the
// same name picks up after the messages it already acked.
func TestStreamOffsetContinueResumes(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.cont", Subjects: []string{"e"}}
	publish := func(t *testing.T, n int, tag string) {
		t.Helper()
		for i := 0; i < n; i++ {
			require.Nil(t, p.PublishOne(ctx,
				&pb.Message{Address: addr, Body: []byte(fmt.Sprintf("%s%d", tag, i))}))
		}
	}
	src := func() *pb.Source {
		return &pb.Source{
			Name:    "events.cont.consumer",
			Type:    pb.Source_STREAM,
			Address: &pb.Address{Name: "events.cont", Subjects: []string{"e"}},
			Options: map[string]string{"Offset": "continue"},
		}
	}

	// With nothing read yet there is no stored position, so "continue" starts
	// at the beginning — RabbitMQ's QueryOffset finds no offset and falls back
	// to offset 0 the same way.
	publish(t, 2, "first")
	drain := func(t *testing.T, want int) []string {
		t.Helper()
		out := make(chan *pb.Message, want+4)
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go p.Subscribe(subCtx, src(), out)
		got := make([]string, 0, want)
		for i := 0; i < want; i++ {
			m := recv(t, out)
			require.Nil(t, p.Ack(ctx, m.GetUuid()))
			got = append(got, string(m.GetBody()))
		}
		// Nothing beyond what was asked for: a replay would show up here.
		select {
		case m := <-out:
			t.Fatalf("unexpected extra message %q; continue replayed the log", m.GetBody())
		case <-time.After(time.Second):
		}
		return got
	}
	require.Equal(t, []string{"first0", "first1"}, drain(t, 2))

	// The second subscription must see only what was published after the
	// first one stopped.
	publish(t, 3, "second")
	require.Equal(t, []string{"second0", "second1", "second2"}, drain(t, 3),
		"continue replayed already-acked messages instead of resuming")
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

	t.Run("numeric_offset_starts_at_offset", func(t *testing.T) {
		out := make(chan *pb.Message, n)
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		// Offsets count from 0 like a RabbitMQ Stream's, so offset 2 is the
		// third message, m2 — not JetStream's sequence 2, which is m1.
		go p.Subscribe(subCtx, streamSource("events.offset.num", "events.offset", "2", "e"), out)

		got := []string{}
		for i := 0; i < 2; i++ {
			m := recv(t, out)
			got = append(got, string(m.GetBody()))
			require.Nil(t, p.Ack(ctx, m.GetUuid()))
		}
		assert.Equal(t, []string{"m2", "m3"}, got, "numeric offset starts at the named message")
	})

	// The offset vocabulary has to round-trip: an offset read from SourceStats
	// and handed back as a source's Offset must name the same message. A
	// connector reporting JetStream's 1-based sequence but accepting it back
	// as a 0-based offset would quietly skip one message per hop.
	t.Run("offset_from_stats_round_trips", func(t *testing.T) {
		stats := p.SourceStats(ctx, streamSource("events.offset.rt", "events.offset", "first", "e"))
		require.Nil(t, stats.GetError())
		require.EqualValues(t, n-1, stats.GetLastOffset(), "last offset names the final message")

		out := make(chan *pb.Message, n)
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		last := strconv.FormatInt(stats.GetLastOffset(), 10)
		go p.Subscribe(subCtx, streamSource("events.offset.rt2", "events.offset", last, "e"), out)

		m := recv(t, out)
		assert.Equal(t, "m3", string(m.GetBody()), "LastOffset fed back must start at the last message")
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
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

// TestPublishContractRefusals pins the two publish combinations amqp091
// refuses, so a client cannot write code against natsjs that silently means
// something else on RabbitMQ. Both error strings are amqp091's, verbatim.
func TestPublishContractRefusals(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	// Deduplication on a queue address: RabbitMQ gets dedup from streams, so
	// amqp091 cannot offer it on a queue at all.
	perr := p.PublishOne(ctx, &pb.Message{
		Address:       &pb.Address{Name: "events.q", Type: pb.Address_QUEUE, Subjects: []string{"k"}},
		PublisherName: "pub", PublishId: 1, Body: []byte("x")})
	require.NotNil(t, perr)
	assert.Equal(t, queueDedupError, perr.GetMessage())

	// ...and equally on a topic address. amqp091's PublishOne sends every
	// non-STREAM address through publishOneQueue, which is where that refusal
	// lives, so TOPIC and FILTER are refused on exactly the same grounds.
	require.NotNil(t, p.PublishOne(ctx, &pb.Message{
		Address:       &pb.Address{Name: "events.t", Type: pb.Address_TOPIC, Subjects: []string{"k"}},
		PublisherName: "pub", PublishId: 1, Body: []byte("x")}))

	// A message wrong twice over — dedup contract violated AND aimed at an
	// undeclared stream — reports the contract error: amqp091 refuses both
	// of these before it reaches for the stream, so the refusals precede the
	// stream lookup here too.
	perr = p.PublishOne(ctx, &pb.Message{
		Address:   &pb.Address{Name: "events.tcr.undeclared", Type: pb.Address_STREAM, Subjects: []string{"k"}},
		PublishId: 7, Body: []byte("x")})
	require.NotNil(t, perr)
	assert.Contains(t, perr.GetMessage(), "PublisherName not set on message")

	// Confirm on the fire-and-forget streaming publish.
	in := make(chan *pb.Message, 1)
	errChan := make(chan *pb.Error, 2)
	pubCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Publish(pubCtx, in, errChan)
	in <- &pb.Message{Address: &pb.Address{Name: "events.t", Subjects: []string{"k"}},
		Body: []byte("x"), Confirm: true}
	select {
	case e := <-errChan:
		require.NotNil(t, e)
		assert.Equal(t, unsupportedConfirmError, e.GetMessage())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the publish error")
	}
	// Exactly one reply per message: the server does an unconditional receive
	// after each send, so a second value here would answer the *next* message.
	select {
	case e := <-errChan:
		t.Fatalf("second reply for one message: %v", e)
	case <-time.After(time.Second):
	}
}

// TestParentAddressBindingRoutes covers address-to-address binding: a child
// address bound to a parent receives what is published to the parent under the
// bound keys, still receives what is published to it directly, and does not
// receive what the parent routes elsewhere.
func TestParentAddressBindingRoutes(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	parent := &pb.Address{Name: "events.parent", Type: pb.Address_TOPIC}
	child := &pb.Address{
		Name:          "events.child",
		Type:          pb.Address_FILTER,
		Subjects:      []string{"bound.one", "bound.two"},
		ParentAddress: parent,
	}

	out := make(chan *pb.Message, 8)
	src := &pb.Source{Name: "events.child.consumer", Type: pb.Source_QUEUE, Address: child,
		Options: map[string]string{"Offset": "first"}}
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)
	time.Sleep(500 * time.Millisecond)

	pub := func(t *testing.T, addrName, key, body string) {
		t.Helper()
		require.Nil(t, p.PublishOne(ctx, &pb.Message{
			Address: &pb.Address{Name: addrName, Subjects: []string{key}},
			Body:    []byte(body)}))
	}
	// Routed in from the parent under a bound key...
	pub(t, "events.parent", "bound.one", "routed")
	// ...not routed: the parent has no binding to the child for this key.
	pub(t, "events.parent", "unbound.key", "not-routed")
	// ...and published straight to the child.
	pub(t, "events.child", "bound.two", "direct")

	got := map[string]bool{}
	deadline := time.After(6 * time.Second)
	for len(got) < 2 {
		select {
		case m := <-out:
			got[string(m.GetBody())] = true
			require.Nil(t, p.Ack(ctx, m.GetUuid()))
		case <-deadline:
			t.Fatalf("timed out; received %v", got)
		}
	}
	assert.True(t, got["routed"], "a message published to the parent under a bound key must reach the child")
	assert.True(t, got["direct"], "a direct publish to the child must still arrive")

	select {
	case m := <-out:
		t.Fatalf("child received %q, which the parent does not route to it", m.GetBody())
	case <-time.After(2 * time.Second):
	}
}

// TestParentAddressBindingsAccumulate: a second address-to-address binding on
// the same address adds to the first rather than replacing it, the way
// declaring a binding on RabbitMQ never removes another.
func TestParentAddressBindingsAccumulate(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	parent := &pb.Address{Name: "events.acc.parent", Type: pb.Address_TOPIC}
	childWith := func(subjects ...string) *pb.Address {
		return &pb.Address{Name: "events.acc.child", Type: pb.Address_TOPIC,
			Subjects: subjects, ParentAddress: parent}
	}
	sub := func(t *testing.T, name string, addr *pb.Address) chan *pb.Message {
		t.Helper()
		out := make(chan *pb.Message, 8)
		subCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		go p.Subscribe(subCtx, &pb.Source{Name: name, Type: pb.Source_QUEUE, Address: addr,
			Options: map[string]string{"Offset": "first"}}, out)
		time.Sleep(500 * time.Millisecond)
		return out
	}

	first := sub(t, "events.acc.first", childWith("key.one"))
	// The second subscriber declares a different binding on the same address.
	second := sub(t, "events.acc.second", childWith("key.two"))

	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: &pb.Address{Name: "events.acc.parent", Subjects: []string{"key.one"}},
		Body:    []byte("one")}))
	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: &pb.Address{Name: "events.acc.parent", Subjects: []string{"key.two"}},
		Body:    []byte("two")}))

	// The first subscriber's binding must have survived the second's declare.
	m := recv(t, first)
	assert.Equal(t, "one", string(m.GetBody()), "the earlier binding was dropped by a later one")
	m = recv(t, second)
	assert.Equal(t, "two", string(m.GetBody()))
}

// TestEmptyRoutingKeyDelivers covers fanout-style publishes that carry no
// routing key: they map to the bare "<root>.~" subject, which the stream
// captures and a consumer selects by binding the empty routing key. The
// binding has to be declared — a source with no bindings receives nothing,
// whatever the routing key (see TestNoBindingsReceiveNothing).
func TestEmptyRoutingKeyDelivers(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.fanout"}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("no-key")}))

	out := make(chan *pb.Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, queueSource("events.fanout.consumer", "events.fanout", ""), out)

	m := recv(t, out)
	assert.Equal(t, "no-key", string(m.GetBody()))
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
}

// TestNoBindingsReceiveNothing pins the AMQP rule that a source with no
// routing-key bindings receives nothing. amqp091 declares no binding at all
// for such a source (declareBinding iterates an empty subject list), so
// selecting the whole address instead — which a pattern-less consumer used to
// do — hands the source every message published to it.
func TestNoBindingsReceiveNothing(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	out := make(chan *pb.Message, 4)
	src := queueSource("events.unbound.consumer", "events.unbound")
	src.Address.Subjects = nil
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)
	// Let the consumer establish before publishing, so a delivery would race
	// in rather than be missed.
	time.Sleep(500 * time.Millisecond)

	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: &pb.Address{Name: "events.unbound"}, Body: []byte("no-key")}))
	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: &pb.Address{Name: "events.unbound", Subjects: []string{"some.key"}},
		Body:    []byte("keyed")}))

	select {
	case m := <-out:
		t.Fatalf("source with no bindings received %q", m.GetBody())
	case <-time.After(2 * time.Second):
	}
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

// TestSourceStatsStreamSourceReportsOffsetsNotDepth pins the two ways a
// Source_STREAM's stats differ from a queue's on amqp091: no message count
// (the readers of a shared log have no common backlog — the offsets are the
// reading), and RabbitMQ's "Offset not found" when the log holds no offset
// yet, rather than a silent zero.
func TestSourceStatsStreamSourceReportsOffsetsNotDepth(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	src := &pb.Source{
		Name:    "events.log.consumer",
		Type:    pb.Source_STREAM,
		Address: &pb.Address{Name: "events.log", Type: pb.Address_STREAM, Subjects: []string{"e"}},
	}

	// An empty log has no offset to report.
	bd, bderr := p.getBrokerDetails(ctx)
	require.NoError(t, bderr)
	_, serr := p.ensureStream(ctx, bd, "events.log")
	require.Nil(t, serr)
	empty := p.SourceStats(ctx, src)
	require.NotNil(t, empty.GetError())
	assert.Equal(t, offsetNotFoundError, empty.GetError().GetMessage())

	addr := &pb.Address{Name: "events.log", Subjects: []string{"e"}}
	const n = 5
	for i := 0; i < n; i++ {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m")}))
	}

	stats := p.SourceStats(ctx, src)
	require.Nil(t, stats.GetError())
	assert.EqualValues(t, n-1, stats.GetLastOffset(), "offsets count from 0, so the fifth message is offset 4")
	assert.Zero(t, stats.GetMessageCount(), "a stream source reports no backlog, like amqp091")
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
	// Expires is applied (consumer InactiveThreshold), so it must not be
	// reported — and warned about — as unapplied.
	assert.Equal(t, []string{"MessageTTL"},
		unappliedSourceOptions(src(map[string]string{"MessageTTL": "300000", "Expires": "600000"})))
}

func TestExpiresThreshold(t *testing.T) {
	src := func(opts map[string]string) *pb.Source { return &pb.Source{Options: opts} }

	d, err := expiresThreshold(src(nil))
	require.NoError(t, err)
	assert.Zero(t, d, "unset means the kind's default applies")

	d, err = expiresThreshold(src(map[string]string{"Expires": "600000"}))
	require.NoError(t, err)
	assert.Equal(t, 10*time.Minute, d)

	_, err = expiresThreshold(src(map[string]string{"Expires": "10m"}))
	require.EqualError(t, err, "value for Expires option must be a quoted integer",
		"error string mirrors the amqp091 connector")
	_, err = expiresThreshold(src(map[string]string{"Expires": "0"}))
	require.EqualError(t, err, "value for Expires option must be a positive integer")
	_, err = expiresThreshold(src(map[string]string{"Expires": "-5"}))
	require.EqualError(t, err, "value for Expires option must be a positive integer")
}

// connectSecondClient attaches a second, independently-identified client to the
// same server, so a test can exercise what one connection's state (the
// knownStreams memo, say) hides from another.
func connectSecondClient(t *testing.T, s *natsserver.Server, id string) (*natsjsProvider, context.Context) {
	t.Helper()
	saved := GetClientIdentifier
	t.Cleanup(func() { GetClientIdentifier = saved })
	GetClientIdentifier = func(context.Context) (string, error) { return id, nil }

	addr := s.Addr().(*net.TCPAddr)
	p := NewNATSJetStreamProvider().(*natsjsProvider)
	ctx := context.Background()
	if perr := p.Connect(ctx, &pb.ConnectionConfiguration{
		Host: "127.0.0.1", Port: int32(addr.Port), //nolint:gosec // local server port fits int32
		ClientName: id}, false); perr != nil {
		t.Fatalf("connect %s: %s", id, perr.GetMessage())
	}
	if !p.WaitForConnect(ctx) {
		t.Fatalf("WaitForConnect returned false for %s", id)
	}
	return p, ctx
}

// TestBindingSurvivesBindinglessDeclare: asserting a stream that has
// address-to-address bindings must carry them forward even when the asserting
// call declares none of its own. CreateOrUpdateStream replaces the source set
// wholesale, so a config that omits it silently unbinds the address — and the
// call that omits it is the most ordinary one there is: a publisher naming the
// bound address directly, which carries no ParentAddress. The per-connection
// knownStreams memo hides this from the connection that declared the binding,
// so the regression only shows up from a second one.
func TestBindingSurvivesBindinglessDeclare(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	parent := &pb.Address{Name: "events.bsurv.parent", Type: pb.Address_TOPIC}
	child := &pb.Address{
		Name:          "events.bsurv.child",
		Type:          pb.Address_TOPIC,
		Subjects:      []string{"key.one"},
		ParentAddress: parent,
	}

	out := make(chan *pb.Message, 8)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, &pb.Source{Name: "events.bsurv.consumer", Type: pb.Source_QUEUE,
		Address: child, Options: map[string]string{"Offset": "first"}}, out)
	time.Sleep(500 * time.Millisecond)

	pubToParent := func(t *testing.T, body string) {
		t.Helper()
		require.Nil(t, p.PublishOne(ctx, &pb.Message{
			Address: &pb.Address{Name: "events.bsurv.parent", Subjects: []string{"key.one"}},
			Body:    []byte(body)}))
	}

	pubToParent(t, "before")
	m := recv(t, out)
	require.Equal(t, "before", string(m.GetBody()))
	require.Nil(t, p.Ack(ctx, m.GetUuid()))

	// A second client publishes straight to the bound address, naming no
	// parent. Its connection has never asserted this stream, so it takes the
	// create-or-update path with no bindings of its own to declare.
	p2, ctx2 := connectSecondClient(t, s, "other-client")
	require.Nil(t, p2.PublishOne(ctx2, &pb.Message{
		Address: &pb.Address{Name: "events.bsurv.child", Subjects: []string{"direct"}},
		Body:    []byte("direct")}))

	// The binding the first client declared must still route.
	GetClientIdentifier = func(context.Context) (string, error) { return "test-client", nil }
	pubToParent(t, "after")

	deadline := time.After(10 * time.Second)
	for {
		select {
		case m := <-out:
			require.Nil(t, p.Ack(ctx, m.GetUuid()))
			if string(m.GetBody()) == "after" {
				return
			}
		case <-deadline:
			t.Fatal("the address-to-address binding did not survive a bindingless declare")
		}
	}
}

// TestFailedDeadLetterRedeliveryIsPaced: a dead-letter that fails leaves the
// message in flight so the server's fallback nack retries it (see
// TestDeadLetterFailureKeepsMessage). That retry runs the same attempt against
// the same configuration, so when the failure is permanent — here an empty
// DeadLetterAddress — an immediate nak spins the message between server and
// client as fast as the client can nack, and MaxDeliver is deliberately unset
// so nothing bounds it. The redelivery must be paced instead.
func TestFailedDeadLetterRedeliveryIsPaced(t *testing.T) {
	t.Setenv("NATSJS_DEADLETTER_RETRY_DELAY", "500ms")

	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.dlpace", Subjects: []string{"job"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("poison")}))

	out := make(chan *pb.Message, 64)
	src := queueSource("events.dlpace.consumer", "events.dlpace", "job")
	src.Options["DeadLetterAddress"] = ""
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	// Replay the gRPC server's nack handling for one poison message: it calls
	// DeadLetter whenever the DeadLetterAddress key is present (whatever its
	// value) and falls back to Nack when that errors.
	deliveries := 0
	const window = 3 * time.Second
	deadline := time.After(window)
	for {
		select {
		case m := <-out:
			deliveries++
			if deliveries > 40 {
				t.Fatalf("redelivery storm: %d deliveries of one message in under %s", deliveries, window)
			}
			require.NotNil(t, p.DeadLetter(ctx, src, m.GetUuid()))
			require.Nil(t, p.Nack(ctx, m.GetUuid()))
		case <-deadline:
			// Paced, but still retrying: dropping the message instead would
			// lose it while it is also absent from any DLQ.
			assert.GreaterOrEqual(t, deliveries, 2, "a failed dead-letter must still be retried")
			assert.LessOrEqual(t, deliveries, 20,
				"redelivery of a permanently-failing dead-letter must be paced, not immediate")
			return
		}
	}
}

// TestSubscribeInvalidOffsetCreatesNoStream: an unrecognized Offset is a
// contract refusal, and a refused subscribe must not leave topology behind for
// an address the client only ever named by mistake.
func TestSubscribeInvalidOffsetCreatesNoStream(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	out := make(chan *pb.Message, 1)
	perr := p.Subscribe(ctx, streamSource("events.nostream.consumer", "events.nostream", "latest", "e"), out)
	require.NotNil(t, perr)
	assert.Contains(t, perr.GetMessage(), "invalid offset")

	bd := p.getBrokerDetailsByIdentifier("test-client")
	_, serr := bd.js.Stream(ctx, streamNameFor("events.nostream"))
	assert.ErrorIs(t, serr, jetstream.ErrStreamNotFound,
		"a subscribe refused on its options must not have created the address's stream")
}

// TestConcurrentResolveAndMarkLeavesNoOrphans: resolving a message and marking
// one for redelivery are both read-modify-writes over activeMessages. Run
// concurrently without a lock between them, a mark landing between a resolve's
// read and its delete reinserts an entry nothing can ever resolve, leaking it
// for the life of the connection and inflating the active-message stat.
func TestConcurrentResolveAndMarkLeavesNoOrphans(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)
	bd := p.getBrokerDetailsByIdentifier("test-client")

	const n = 300
	uuids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		uuid := fmt.Sprintf("uuid-%d", i)
		uuids = append(uuids, uuid)
		bd.activeMessages.Add(uuid, inflightMsg{subKey: "sub#1"})
	}

	var wg sync.WaitGroup
	for _, uuid := range uuids {
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = p.takeMessage(ctx, uuid) }()
		go func() { defer wg.Done(); markForRedelivery(bd, uuid) }()
	}
	wg.Wait()

	assert.Equal(t, 0, bd.activeMessages.Length(),
		"marking a message for redelivery must not reinsert one that was resolved concurrently")
}

// TestPublishDedupRefusedOnEveryNonStreamAddress pins the address types a
// unary publish may ask for deduplication on. amqp091's PublishOne routes only
// Address_STREAM to publishOneStream (the RabbitMQ Streams client, the one
// thing that can deduplicate); every other type — TOPIC, QUEUE and FILTER —
// goes to publishOneQueue, which refuses PublishId outright (amqp091.go:1501).
// TOPIC is the case that matters most: it is the proto zero value, so an
// address that never sets a type lands here.
//
// Regression: the refusal used to test == Address_QUEUE, so a TOPIC or FILTER
// address silently accepted the id and deduplicated — giving a client a
// guarantee that turns into a hard error the moment it runs against RabbitMQ.
func TestPublishDedupRefusedOnEveryNonStreamAddress(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	for _, tc := range []struct {
		name string
		typ  pb.Address_TargetType
	}{
		{"topic (the proto zero value)", pb.Address_TOPIC},
		{"queue", pb.Address_QUEUE},
		{"filter", pb.Address_FILTER},
	} {
		t.Run(tc.name, func(t *testing.T) {
			perr := p.PublishOne(ctx, &pb.Message{
				Address:       &pb.Address{Name: "events.dedup.scope." + tc.name, Type: tc.typ, Subjects: []string{"k"}},
				PublisherName: "pub", PublishId: 1, Body: []byte("x")})
			require.NotNil(t, perr, "a unary publish asking for dedup on a non-STREAM address must be refused")
			assert.Equal(t, queueDedupError, perr.GetMessage())
		})
	}

	// A STREAM address is the one that may: declare it, then dedup applies.
	bd := p.getBrokerDetailsByIdentifier("test-client")
	_, eerr := p.ensureStream(ctx, bd, "events.dedup.scope.stream")
	require.NoError(t, eerr)
	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address:       &pb.Address{Name: "events.dedup.scope.stream", Type: pb.Address_STREAM, Subjects: []string{"k"}},
		PublisherName: "pub", PublishId: 1, Body: []byte("x")}))
}

// TestStreamingPublishIgnoresPublishID pins the other half of the dedup
// contract: the whole publish-id contract belongs to the unary path only.
// amqp091's streaming Publish calls prepareAndSend directly for EVERY address
// type — the plain channel publish, which reads neither PublishId nor
// PublisherName — so natsjs must not refuse the id there (that would be
// stricter than RabbitMQ, breaking a publisher that works there today) and
// must not honour it there either (that would be a guarantee RabbitMQ does not
// give, and honouring it discards a message RabbitMQ stores).
//
// Regression: the address type alone decided this, so a STREAM address was
// deduplicated on the streaming path too — silently dropping the second
// publish, where amqp091 never reaches the Streams client that deduplicates.
func TestStreamingPublishIgnoresPublishID(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  pb.Address_TargetType
	}{
		{"topic (the proto zero value)", pb.Address_TOPIC},
		{"queue", pb.Address_QUEUE},
		// The one that regressed: STREAM is the type the UNARY path
		// deduplicates on, but the streaming path never routes to the client
		// that does the deduplicating, so the id has to be dropped here too.
		{"stream", pb.Address_STREAM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := runJetStreamServer(t)
			p, ctx := connectClient(t, s)

			in := make(chan *pb.Message, 2)
			errChan := make(chan *pb.Error, 2)
			pubCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			go p.Publish(pubCtx, in, errChan)

			addr := &pb.Address{Name: "events.stream.dedupignored", Type: tc.typ, Subjects: []string{"k"}}
			for i := 0; i < 2; i++ {
				in <- &pb.Message{Address: addr, Body: []byte("x"), PublisherName: "pub", PublishId: 42}
				select {
				case e := <-errChan:
					require.Nil(t, e, "the streaming path must not refuse a publish id: %v", e.GetMessage())
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for the publish reply")
				}
			}

			stats := p.SourceStats(ctx, &pb.Source{
				Name:    "events.stream.dedupignored.consumer",
				Address: &pb.Address{Name: "events.stream.dedupignored"},
			})
			assert.Nil(t, stats.GetError())
			assert.Equal(t, int64(2), stats.GetMessageCount(),
				"the id must be ignored on the streaming path, not deduplicated on — RabbitMQ drops it on the floor")
		})
	}
}

// TestStreamingPublishAcceptsPublishIDWithoutPublisherName pins the companion
// refusal to the unary path as well. amqp091 requires a PublisherName
// alongside a PublishID only in publishOneStream (amqp091.go:1544), where the
// RabbitMQ Streams client needs a named publisher to deduplicate against. The
// streaming Publish never routes there and reads neither field, so this
// publish succeeds on RabbitMQ and must succeed here.
func TestStreamingPublishAcceptsPublishIDWithoutPublisherName(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	in := make(chan *pb.Message, 1)
	errChan := make(chan *pb.Error, 1)
	pubCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Publish(pubCtx, in, errChan)

	in <- &pb.Message{
		Address:   &pb.Address{Name: "events.stream.noPubName", Type: pb.Address_TOPIC, Subjects: []string{"k"}},
		Body:      []byte("x"),
		PublishId: 42,
	}
	select {
	case e := <-errChan:
		require.Nil(t, e, "the streaming path reads neither PublishId nor PublisherName: %v", e.GetMessage())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the publish reply")
	}
}

// TestHeadersAddressIgnoresRoutingKey covers a headers address that also
// carries subjects. RabbitMQ's headers exchange never looks at the routing key
// — rabbit_exchange_type_headers matches every binding of the exchange on its
// header arguments alone — so the subjects amqp091 passes to QueueBind decide
// nothing, and a message published under any key reaches the consumer.
//
// Regression: subjects on a FILTER address used to become NATS consumer filter
// subjects, so a message whose routing key matched no subject was dropped —
// silently delivering strictly less than RabbitMQ would.
func TestHeadersAddressIgnoresRoutingKey(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.headers", Type: pb.Address_FILTER, Subjects: []string{"bound.one"}}
	out := make(chan *pb.Message, 8)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, &pb.Source{Name: "events.headers.consumer", Type: pb.Source_QUEUE,
		Address: addr, Options: map[string]string{"Offset": "first"}}, out)
	time.Sleep(500 * time.Millisecond)

	for _, key := range []string{"bound.one", "some.other.key"} {
		require.Nil(t, p.PublishOne(ctx, &pb.Message{
			Address: &pb.Address{Name: "events.headers", Type: pb.Address_FILTER, Subjects: []string{key}},
			Body:    []byte(key)}))
	}

	got := map[string]bool{}
	for len(got) < 2 {
		m := recv(t, out)
		got[string(m.GetBody())] = true
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
	}
	assert.True(t, got["bound.one"], "the message under the declared key must arrive")
	assert.True(t, got["some.other.key"],
		"a headers exchange ignores routing keys, so a message under any key must arrive")
}

// TestHeadersAddressStillAppliesHeaderFilters is the guard on the test above:
// selecting the whole address must hand the decision to the header filters,
// not deliver everything unconditionally.
func TestHeadersAddressStillAppliesHeaderFilters(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.headers.filtered", Type: pb.Address_FILTER, Subjects: []string{"bound.one"}}
	out := make(chan *pb.Message, 8)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, &pb.Source{
		Name: "events.headers.filtered.consumer", Type: pb.Source_QUEUE, Address: addr,
		Filters: []*pb.Filter{{Type: pb.Filter_ALL, Matches: []*pb.Match{{Name: "tenant", Value: "acme"}}}},
		Options: map[string]string{"Offset": "first"},
	}, out)
	time.Sleep(500 * time.Millisecond)

	// Matching headers under a key no subject names: delivered (headers decide).
	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: &pb.Address{Name: "events.headers.filtered", Type: pb.Address_FILTER, Subjects: []string{"any.key"}},
		Headers: map[string]string{"tenant": "acme"}, Body: []byte("match")}))
	// Non-matching headers under the declared key: filtered out.
	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: &pb.Address{Name: "events.headers.filtered", Type: pb.Address_FILTER, Subjects: []string{"bound.one"}},
		Headers: map[string]string{"tenant": "other"}, Body: []byte("nomatch")}))

	m := recv(t, out)
	assert.Equal(t, "match", string(m.GetBody()))
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
	select {
	case extra := <-out:
		t.Fatalf("header filters must still gate a headers address; got %q", string(extra.GetBody()))
	case <-time.After(2 * time.Second):
	}
}

// TestBareStreamAssertionIssuesNoUpdate pins that asserting a stream that
// already satisfies the requested config writes nothing.
//
// Carrying the existing binding set forward makes ensureStreamFor a
// read-modify-write, and p.bindMu serializes that only inside one process —
// Arke runs more than one replica. Skipping the write when nothing would
// change takes the overwhelmingly common case (every publisher's first touch
// of an existing stream) out of that cross-process race, so a bare assertion
// can no longer be the thing that drops a binding another replica just added.
func TestBareStreamAssertionIssuesNoUpdate(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)
	bd := p.getBrokerDetailsByIdentifier("test-client")

	streamName := streamNameFor("events.noupdate")
	_, eerr := p.ensureStream(ctx, bd, "events.noupdate")
	require.NoError(t, eerr)

	// Watch the JetStream management API for updates to this stream. A plain
	// subscription is an observer: the server's own responder still receives
	// the request, so this changes nothing about the call itself.
	updates := make(chan struct{}, 8)
	sub, serr := bd.nc.Subscribe("$JS.API.STREAM.UPDATE."+streamName, func(*nats.Msg) {
		updates <- struct{}{}
	})
	require.NoError(t, serr)
	defer sub.Unsubscribe() //nolint:errcheck
	require.NoError(t, bd.nc.Flush())

	// Drop the per-connection memo so the assertion really re-runs against the
	// server, exactly as a fresh connection's first touch would.
	bd.knownStreams.Delete(streamName)
	_, eerr = p.ensureStream(ctx, bd, "events.noupdate")
	require.NoError(t, eerr)
	require.NoError(t, bd.nc.Flush())

	select {
	case <-updates:
		t.Fatal("a bare assertion of an already-satisfying stream must not issue a stream update")
	case <-time.After(500 * time.Millisecond):
	}

	// ...but an assertion that genuinely changes something still writes. A
	// binding is the case that matters: it must survive the skip logic.
	child := &pb.Address{
		Name:          "events.noupdate",
		Type:          pb.Address_TOPIC,
		Subjects:      []string{"routed.key"},
		ParentAddress: &pb.Address{Name: "events.noupdate.parent", Type: pb.Address_TOPIC},
	}
	_, eerr = p.ensureStreamFor(ctx, bd, child)
	require.NoError(t, eerr)
	require.NoError(t, bd.nc.Flush())
	select {
	case <-updates:
	case <-time.After(2 * time.Second):
		t.Fatal("declaring a new binding must still update the stream")
	}

	// And the binding really landed — the skip must not have swallowed it.
	st, ferr := bd.js.Stream(ctx, streamName)
	require.NoError(t, ferr)
	sources := st.CachedInfo().Config.Sources
	require.Len(t, sources, 1)
	assert.Equal(t, streamNameFor("events.noupdate.parent"), sources[0].Name)
	require.Len(t, sources[0].SubjectTransforms, 1)
	assert.Equal(t, publishSubjectFor("events.noupdate.parent", "routed.key"),
		sources[0].SubjectTransforms[0].Source)

	// Re-asserting that same binding is now itself a no-op.
	bd.knownStreams.Delete(streamName)
	_, eerr = p.ensureStreamFor(ctx, bd, child)
	require.NoError(t, eerr)
	require.NoError(t, bd.nc.Flush())
	select {
	case <-updates:
		t.Fatal("re-declaring an existing binding must not issue a stream update")
	case <-time.After(500 * time.Millisecond):
	}

	// A SECOND binding onto a parent already in the source set must still
	// write. This is the case the skip gets wrong if the config being built
	// shares its StreamSource objects with the snapshot it is compared
	// against: merging appends to the transforms in place, so both sides move
	// together and a genuinely new binding reads as no change at all. A first
	// binding hides that — it appends a whole element, so the lengths differ
	// regardless.
	sibling := &pb.Address{
		Name:          "events.noupdate",
		Type:          pb.Address_TOPIC,
		Subjects:      []string{"second.key"},
		ParentAddress: &pb.Address{Name: "events.noupdate.parent", Type: pb.Address_TOPIC},
	}
	_, eerr = p.ensureStreamFor(ctx, bd, sibling)
	require.NoError(t, eerr)
	require.NoError(t, bd.nc.Flush())
	select {
	case <-updates:
	case <-time.After(2 * time.Second):
		t.Fatal("a second binding onto an already-bound parent must still update the stream")
	}

	st, ferr = bd.js.Stream(ctx, streamName)
	require.NoError(t, ferr)
	sources = st.CachedInfo().Config.Sources
	require.Len(t, sources, 1, "both bindings share one parent, so one source carries both")
	assert.Len(t, sources[0].SubjectTransforms, 2, "the earlier binding must survive the later one")
}

// TestStreamConfigSatisfied covers the comparison that lets ensureStreamFor
// skip an assertion which would change nothing (see
// TestBareStreamAssertionIssuesNoUpdate for why that matters). It must answer
// "satisfied" only when every field the connector manages already matches —
// a false positive there silently drops a real config change.
func TestStreamConfigSatisfied(t *testing.T) {
	base := func() *jetstream.StreamConfig {
		return &jetstream.StreamConfig{
			Name:       "arke_events_x",
			Subjects:   []string{"events.x.~", "events.x.~.>"},
			Storage:    jetstream.FileStorage,
			Retention:  jetstream.LimitsPolicy,
			Duplicates: defaultDedupWindow,
			Replicas:   1,
			MaxAge:     defaultStreamMaxAge,
		}
	}
	assert.True(t, streamConfigSatisfied(base(), base()))

	// Subject order is the server's to choose, not a difference.
	reordered := base()
	reordered.Subjects = []string{"events.x.~.>", "events.x.~"}
	assert.True(t, streamConfigSatisfied(reordered, base()))

	// Each managed field, differing.
	for name, mutate := range map[string]func(*jetstream.StreamConfig){
		"subjects":   func(c *jetstream.StreamConfig) { c.Subjects = []string{"events.x.~"} },
		"max age":    func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour },
		"max bytes":  func(c *jetstream.StreamConfig) { c.MaxBytes = 1 << 20 },
		"replicas":   func(c *jetstream.StreamConfig) { c.Replicas = 3 },
		"duplicates": func(c *jetstream.StreamConfig) { c.Duplicates = time.Minute },
		"storage":    func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage },
		"retention":  func(c *jetstream.StreamConfig) { c.Retention = jetstream.WorkQueuePolicy },
	} {
		t.Run(name+" differs", func(t *testing.T) {
			live := base()
			mutate(live)
			assert.False(t, streamConfigSatisfied(live, base()),
				"a differing %s must not read as satisfied", name)
		})
	}

	// A stream with no bindings does not satisfy a config that declares one...
	want := base()
	want.Sources = []*jetstream.StreamSource{{Name: "arke_events_p",
		SubjectTransforms: []jetstream.SubjectTransformConfig{{Source: "events.p.~.k", Destination: "events.p.~.k"}}}}
	assert.False(t, streamConfigSatisfied(base(), want))

	// ...and one that already carries exactly that binding does.
	live := base()
	live.Sources = []*jetstream.StreamSource{{Name: "arke_events_p",
		SubjectTransforms: []jetstream.SubjectTransformConfig{{Source: "events.p.~.k", Destination: "events.p.~.k"}}}}
	assert.True(t, streamConfigSatisfied(live, want))

	// An extra binding on the live stream is a difference in the other
	// direction: the merged config would have carried it forward, so a
	// mismatch here means something else changed it concurrently.
	extra := base()
	extra.Sources = append(copySources(live.Sources), &jetstream.StreamSource{Name: "arke_events_q"})
	assert.False(t, streamConfigSatisfied(extra, want))
}

// TestCopySourcesIsDeep guards the trap that makes the skip logic safe:
// mergeBoundSources appends to a StreamSource's transforms in place, so the
// snapshot ensureStreamFor compares against must not share those objects with
// the config it merges into — or every merge would compare equal to itself and
// a newly declared binding would be skipped instead of written.
func TestCopySourcesIsDeep(t *testing.T) {
	orig := []*jetstream.StreamSource{{Name: "arke_events_p",
		SubjectTransforms: []jetstream.SubjectTransformConfig{{Source: "a", Destination: "a"}}}}
	snapshot := copySources(orig)

	merged := mergeBoundSources(orig, "arke_events_p", []string{"b"})
	require.Len(t, merged[0].SubjectTransforms, 2)
	assert.Len(t, snapshot[0].SubjectTransforms, 1,
		"the snapshot must not see a transform appended after it was taken")
	assert.False(t, sameSources(snapshot, merged),
		"a merge that added a binding must read as a difference")
}

// TestStreamConfigSatisfiedUnlimitedSentinel: JetStream normalizes an unset
// size limit to its -1 "unlimited" sentinel and echoes that back, so a config
// asking for 0 describes the same stream. Comparing the two literally made
// every assertion look like a change, defeating the skip entirely.
func TestStreamConfigSatisfiedUnlimitedSentinel(t *testing.T) {
	live := &jetstream.StreamConfig{Subjects: []string{"a"}, Replicas: 1, MaxBytes: -1}
	want := &jetstream.StreamConfig{Subjects: []string{"a"}, Replicas: 1, MaxBytes: 0}
	assert.True(t, streamConfigSatisfied(live, want),
		"an unset max-bytes and the server's -1 sentinel describe the same stream")

	// A real cap is still a difference in both directions.
	capped := &jetstream.StreamConfig{Subjects: []string{"a"}, Replicas: 1, MaxBytes: 1 << 20}
	assert.False(t, streamConfigSatisfied(live, capped))
	assert.False(t, streamConfigSatisfied(capped, want))
}

// TestHeadersSurviveARealBrokerRoundTrip is the end-to-end half of the header
// escaping: the helper tests prove the encoding is reversible and that it
// survives net/http's writer, but only a real publish and consume proves the
// connector actually routes headers through that encoding on both sides.
//
// Every name here round-trips through RabbitMQ unchanged. Without escaping the
// NATS wire format drops the punctuation and non-ASCII names outright and trims
// the padded values, silently — measured against a live broker of each kind as
// 15 of 38 names delivered.
func TestHeadersSurviveARealBrokerRoundTrip(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	sent := map[string]string{
		"Content-Type":    "application/vnd.example.event+json",
		"__namespace":     "tenant-a",
		"x-event-address": "events.orders",
		"traceparent":     "00-abc-def-01",
		"has space":       "spacekey",
		"has:colon":       "colonvalue",
		"has/slash":       "slashkey",
		"unicode-vàlue":   "héllo wörld",
		"中文":              "cjk",
		"padded-value":    "  keep my spaces  ",
		"tabbed-value":    "\tkeep\ttabs\t",
		"empty-value":     "",
	}

	addr := &pb.Address{Name: "events.hdrroundtrip", Subjects: []string{"probe"}}
	out := make(chan *pb.Message, 4)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, &pb.Source{
		Name: "events.hdrroundtrip.consumer", Type: pb.Source_QUEUE, Address: addr,
		Options: map[string]string{"Offset": "first"},
	}, out)
	time.Sleep(500 * time.Millisecond)

	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: addr, Headers: sent, Body: []byte("probe")}))

	m := recv(t, out)
	require.Nil(t, p.Ack(ctx, m.GetUuid()))

	got := m.GetHeaders()
	for k, want := range sent {
		v, ok := got[k]
		assert.True(t, ok, "header %q must be delivered, not silently dropped", k)
		assert.Equal(t, want, v, "header %q must arrive byte-for-byte", k)
	}
}

// TestTimestampHeaderIsSynthesized: on RabbitMQ the broker's own ingress
// interceptor stamps timestamp_in_ms on every incoming message, so an amqp091
// consumer always sees one. NATS has no interceptor, so the connector has to
// synthesize it from the JetStream store time or clients that read it see
// nothing.
//
// This was long recorded as a capability gap ("NATS has no concept of it") and
// parked in the per-broker skip list. That was wrong: JetStream carries a
// per-message timestamp in its metadata.
func TestTimestampHeaderIsSynthesized(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.timestamped", Subjects: []string{"probe"}}
	out := make(chan *pb.Message, 4)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, &pb.Source{
		Name: "events.timestamped.consumer", Type: pb.Source_QUEUE, Address: addr,
		Options: map[string]string{"Offset": "first"},
	}, out)
	time.Sleep(500 * time.Millisecond)

	before := time.Now().UnixMilli()
	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: addr,
		// A publisher-supplied value must be replaced, not kept: the RabbitMQ
		// interceptor this stands in for runs with overwrite = true.
		Headers: map[string]string{timestampHeaderName: "1"},
		Body:    []byte("probe")}))

	m := recv(t, out)
	require.Nil(t, p.Ack(ctx, m.GetUuid()))

	raw, ok := m.GetHeaders()[timestampHeaderName]
	require.True(t, ok, "every delivery must carry %s, as it does on RabbitMQ", timestampHeaderName)
	ms, err := strconv.ParseInt(raw, 10, 64)
	require.NoError(t, err, "%s must be integer milliseconds", timestampHeaderName)
	assert.GreaterOrEqual(t, ms, before, "must be the broker store time, not the publisher's value")
	assert.LessOrEqual(t, ms, time.Now().UnixMilli()+1000)
}

// TestSingleActiveConsumerDowngradeReleasesPinPromptly: a durable that drops
// SingleActiveConsumer must resume ordinary (non-pinned) delivery promptly.
// Without releaseStalePins, CreateOrUpdateConsumer clears the priority-group
// config immediately but the server's own pin persists as runtime state
// independent of that config (verified against the embedded server: unpinning
// AFTER the config is cleared errors "priority group does not exist for this
// consumer") — so a plain re-subscribe would silently receive nothing until
// NATSJS_SAC_PINNED_TTL (default 1m) elapsed, with no error anywhere. This
// pins the fix at the connector's default TTL, not a shortened test-only one,
// so it actually proves the wait is gone rather than just faster.
func TestSingleActiveConsumerDowngradeReleasesPinPromptly(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.sacdowngrade", Subjects: []string{"e"}}
	sac := sacSource("events.sacdowngrade.consumer", "events.sacdowngrade", "e")

	out := make(chan *pb.Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan *pb.Error, 1)
	go func() { errCh <- p.Subscribe(subCtx, sac, out) }()
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m0")}))
	m := recv(t, out)
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
	cancel()
	require.Nil(t, <-errCh)

	// Re-subscribe to the same durable without SingleActiveConsumer.
	plain := queueSource("events.sacdowngrade.consumer", "events.sacdowngrade", "e")
	out2 := make(chan *pb.Message, 1)
	subCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	go p.Subscribe(subCtx2, plain, out2)

	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m1")}))
	select {
	case m := <-out2:
		require.Nil(t, p.Ack(ctx, m.GetUuid()))
		assert.Equal(t, "m1", string(m.GetBody()))
	case <-time.After(5 * time.Second):
		t.Fatal("plain re-subscribe never received a message: the prior single-active pin was not released")
	}

	bd := p.getBrokerDetailsByIdentifier("test-client")
	stream, serr := bd.js.Stream(ctx, streamNameFor("events.sacdowngrade"))
	require.NoError(t, serr)
	cons, cerr := stream.Consumer(ctx, durableName(plain))
	require.NoError(t, cerr)
	assert.Empty(t, cons.CachedInfo().Config.PriorityGroups, "the downgraded consumer must carry no priority group")
}

// TestStreamDeliveryStampsCurrentOffset: amqp091's streamSubscribe stamps
// x-current-offset on every STREAM-source delivery from the RabbitMQ Streams
// consumer's own offset (amqp091.go:1279). The JetStream analogue is the
// message's own stream sequence in the Offset vocabulary SourceStats already
// uses, so a value read here and handed back as a source's Offset resumes at
// the same message.
func TestStreamDeliveryStampsCurrentOffset(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.curoffset", Subjects: []string{"e"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m0")}))
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m1")}))

	out := make(chan *pb.Message, 4)
	src := streamSource("events.curoffset.consumer", "events.curoffset", "first", "e")
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	m0 := recv(t, out)
	require.Nil(t, p.Ack(ctx, m0.GetUuid()))
	assert.Equal(t, "0", m0.GetHeaders()[currentOffsetHeaderName],
		"the first message in a stream must report offset 0, matching the RabbitMQ-Streams vocabulary")

	m1 := recv(t, out)
	require.Nil(t, p.Ack(ctx, m1.GetUuid()))
	assert.Equal(t, "1", m1.GetHeaders()[currentOffsetHeaderName])
}

// TestQueueDeliveryOmitsCurrentOffset: amqp091 only stamps x-current-offset on
// the RabbitMQ-Streams delivery path (streamSubscribe) — queueSubscribe never
// sets it — so a QUEUE source must not carry it either.
func TestQueueDeliveryOmitsCurrentOffset(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.noqcuroffset", Subjects: []string{"e"}}
	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m")}))

	out := make(chan *pb.Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, queueSource("events.noqcuroffset.consumer", "events.noqcuroffset", "e"), out)

	m := recv(t, out)
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
	_, ok := m.GetHeaders()[currentOffsetHeaderName]
	assert.False(t, ok, "x-current-offset is a STREAM-only header on amqp091 too; a queue source must not carry it")
}

// TestStreamGzipBodyIsDecompressed: amqp091's streamSubscribe decompresses a
// gzip-tagged body and strips the header (amqp091.go:1286-1292) — a proxy-side
// step needed because arke's own STREAM publish path transparently compresses
// bodies over the RabbitMQ-Streams client's ~1MiB framing limit, but applied
// to any body carrying the header regardless of who compressed it. Without
// this, a natsjs STREAM consumer sees the raw compressed bytes: body
// corruption, not just a missing header.
func TestStreamGzipBodyIsDecompressed(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.gzstream", Subjects: []string{"e"}}
	plain := []byte("the quick brown fox jumps over the lazy dog - repeated for compressibility - the quick brown fox")
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write(plain)
	require.NoError(t, err)
	require.NoError(t, gw.Close())

	src := streamSource("events.gzstream.consumer", "events.gzstream", "first", "e")
	out := make(chan *pb.Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, src, out)

	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: addr,
		Headers: map[string]string{transferEncodingHeaderName: "gzip"},
		Body:    buf.Bytes(),
	}))

	m := recv(t, out)
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
	assert.Equal(t, plain, m.GetBody(), "a STREAM consumer must see the decompressed body, matching amqp091's streamSubscribe")
	_, ok := m.GetHeaders()[transferEncodingHeaderName]
	assert.False(t, ok, "the Transfer-Encoding header must be stripped once the body is decompressed, matching amqp091")
}

// TestQueueGzipBodyPassesThroughUntouched: amqp091 only decompresses on the
// STREAM path; a QUEUE source's body and header must pass through exactly as
// published, on both connectors.
func TestQueueGzipBodyPassesThroughUntouched(t *testing.T) {
	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.gzqueue", Subjects: []string{"e"}}
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, gw.Close())
	gzipped := append([]byte(nil), buf.Bytes()...)

	out := make(chan *pb.Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, queueSource("events.gzqueue.consumer", "events.gzqueue", "e"), out)

	require.Nil(t, p.PublishOne(ctx, &pb.Message{
		Address: addr,
		Headers: map[string]string{transferEncodingHeaderName: "gzip"},
		Body:    gzipped,
	}))

	m := recv(t, out)
	require.Nil(t, p.Ack(ctx, m.GetUuid()))
	assert.Equal(t, gzipped, m.GetBody())
	assert.Equal(t, "gzip", m.GetHeaders()[transferEncodingHeaderName])
}

// TestDeliveryInjectsTraceHeadersWhenTracingEnabled: mirrors amqp091's
// queueSubscribe, which starts (or continues) a span for every delivery and,
// when tracing is enabled, writes its W3C trace context back into the
// consumed message's headers — so a distributed trace shows arke's own
// "received from broker" hop even when the publisher set no trace headers at
// all. tracing.Enabled() is a process-global flag flipped by
// InitTracerProvider, so this test brackets it carefully and restores the
// disabled state before returning, since other tests in this package assert
// exact header sets and would break under a leaked global.
func TestDeliveryInjectsTraceHeadersWhenTracingEnabled(t *testing.T) {
	require.NoError(t, os.Setenv(tracing.EnvTelemetryExporter, "stdout"))
	require.NoError(t, os.Setenv(tracing.EnvOtelSdkDisabled, "false"))
	_, err := tracing.InitTracerProvider()
	require.NoError(t, err)
	defer func() {
		_ = os.Setenv(tracing.EnvOtelSdkDisabled, "true")
		_, _ = tracing.InitTracerProvider()
	}()
	require.True(t, tracing.Enabled())

	s := runJetStreamServer(t)
	p, ctx := connectClient(t, s)

	addr := &pb.Address{Name: "events.traced", Subjects: []string{"e"}}
	out := make(chan *pb.Message, 1)
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go p.Subscribe(subCtx, queueSource("events.traced.consumer", "events.traced", "e"), out)

	require.Nil(t, p.PublishOne(ctx, &pb.Message{Address: addr, Body: []byte("m")}))
	m := recv(t, out)
	require.Nil(t, p.Ack(ctx, m.GetUuid()))

	assert.NotEmpty(t, m.GetHeaders()[tracing.HeaderTraceParent],
		"a consumed message must carry a traceparent when tracing is enabled, matching amqp091's queueSubscribe")
	assert.Contains(t, m.GetHeaders()[tracing.HeaderTraceParent], "-", "traceparent must be the W3C dashed format")
}
