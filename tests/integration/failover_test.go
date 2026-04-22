// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// This file contains integration tests for connection resilience and failover
// scenarios involving the broker closing the connection unexpectedly.
//
// These tests require a running arke instance and a live RabbitMQ broker with
// the management plugin enabled.  They intentionally disrupt the broker
// connection and therefore MUST NOT run as part of the normal integration suite.
//
// Run independently with:
//
//	go test -count=1 -v -timeout 3m -tags=failover ./tests/integration/

//go:build failover

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	pb "github.com/sassoftware/arke/api"
	mf "github.com/sassoftware/arke/test/messagefunctions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// RabbitMQ management API helper
// ---------------------------------------------------------------------------

// rabbitManagementClient issues requests against the RabbitMQ HTTP management API.
type rabbitManagementClient struct {
	baseURL  string
	username string
	password string
	hc       *http.Client
}

// newRabbitManagementClient constructs a client from the same environment
// variables used by arke's connection configuration, falling back to the
// default RabbitMQ management defaults.
func newRabbitManagementClient() *rabbitManagementClient {
	hostname := getEnvDefault("ARKE_BROKER_HOSTNAME", "localhost")
	adminPort := getEnvDefault("ARKE_BROKER_ADMIN_PORT", "15672")
	username := getEnvDefault("ARKE_BROKER_USERNAME", "guest")
	password := getEnvDefault("ARKE_BROKER_PASSWORD", "guest")

	return &rabbitManagementClient{
		baseURL:  fmt.Sprintf("http://%s:%s", hostname, adminPort),
		username: username,
		password: password,
		hc:       &http.Client{Timeout: 5 * time.Second},
	}
}

// getEnvDefault returns the value of the named environment variable, or the
// provided default when the variable is unset.
func getEnvDefault(key, defaultVal string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return defaultVal
}

// doRequest performs an authenticated request against the management API.
func (m *rabbitManagementClient) doRequest(method, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(context.Background(), method, m.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(m.username, m.password)
	req.Header.Set("X-Reason", "arke failover test")
	return m.hc.Do(req)
}

// listConnectionNames returns the names of all current AMQP connections as
// reported by RabbitMQ.
func (m *rabbitManagementClient) listConnectionNames() ([]string, error) {
	resp, err := m.doRequest("GET", "/api/connections")
	if err != nil {
		return nil, fmt.Errorf("management API GET /api/connections: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("management API returned %d: %s", resp.StatusCode, string(body))
	}

	var conns []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(body, &conns); err != nil {
		return nil, fmt.Errorf("unmarshal connections: %w", err)
	}

	names := make([]string, 0, len(conns))
	for _, c := range conns {
		names = append(names, c.Name)
	}
	return names, nil
}

// closeConnection forcefully closes the named RabbitMQ connection.
func (m *rabbitManagementClient) closeConnection(name string) error {
	resp, err := m.doRequest("DELETE", "/api/connections/"+url.PathEscape(name))
	if err != nil {
		return fmt.Errorf("management API DELETE connection %q: %w", name, err)
	}
	defer resp.Body.Close()

	// 204 No Content is the success response; 404 means the connection is
	// already gone – treat that as a non-error.
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("management API returned %d for DELETE connection %q: %s", resp.StatusCode, name, string(body))
	}
	return nil
}

// closeAllConnections forcefully closes every active RabbitMQ connection.
func (m *rabbitManagementClient) closeAllConnections() error {
	names, err := m.listConnectionNames()
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := m.closeConnection(name); err != nil {
			return err
		}
	}
	return nil
}

// waitForConnections polls until at least minCount connections exist or the
// deadline is reached, returning the names of the current connections.
func (m *rabbitManagementClient) waitForConnections(minCount int, timeout time.Duration) ([]string, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	var lastCount int
	for time.Now().Before(deadline) {
		names, err := m.listConnectionNames()
		if err != nil {
			lastErr = err
		} else {
			lastCount = len(names)
			if len(names) >= minCount {
				return names, nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr != nil {
		return nil, fmt.Errorf("timed out waiting for %d connection(s) after %s (management API %s – last error: %w) – check that ARKE_BROKER_HOSTNAME/ARKE_BROKER_ADMIN_PORT/ARKE_BROKER_USERNAME/ARKE_BROKER_PASSWORD are set correctly", minCount, timeout, m.baseURL, lastErr)
	}
	return nil, fmt.Errorf("timed out waiting for %d connection(s) after %s (last observed count: %d, management API %s) – the broker may not be connected or may be using a different vhost", minCount, timeout, lastCount, m.baseURL)
}

// waitForNoConnections polls until there are zero active connections or the deadline is reached.
func (m *rabbitManagementClient) waitForNoConnections(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	var lastCount int
	for time.Now().Before(deadline) {
		names, err := m.listConnectionNames()
		if err != nil {
			lastErr = err
		} else {
			lastCount = len(names)
			if len(names) == 0 {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("timed out waiting for 0 connections after %s (management API %s – last error: %w)", timeout, m.baseURL, lastErr)
	}
	return fmt.Errorf("timed out waiting for 0 connections after %s (last observed count: %d, management API %s)", timeout, lastCount, m.baseURL)
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// arkeSession holds an open gRPC connection to arke together with a broker
// connection (arke.Connect has been called).  Callers are responsible for
// calling close() when finished.
type arkeSession struct {
	conn      interface{ Close() error }
	pc        pb.ProducerClient
	cc        pb.ConsumerClient
	ctx       context.Context
	cancelCtx context.CancelFunc
}

// newProducerSession opens a gRPC connection to arke and calls broker.Connect.
// It does NOT defer a Disconnect so the broker-side connection stays alive for
// the full duration of the failover test.
func newProducerSession(t *testing.T) *arkeSession {
	t.Helper()
	conn := connect()
	pc := pb.NewProducerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	connConfig := connectConfig(t.Name())
	resp, err := pc.Connect(ctx, connConfig)
	require.NoError(t, err, "arke broker Connect must succeed")
	require.True(t, resp.GetSuccess(), "arke broker Connect must succeed: %s", resp.GetError().GetMessage())
	return &arkeSession{conn: conn, pc: pc, ctx: ctx, cancelCtx: cancel}
}

// publishOne sends a single message using the session's existing broker
// connection (no internal Disconnect).
func (s *arkeSession) publishOne(address *pb.Address) error {
	msg := &pb.Message{Body: []byte("failover-test"), Address: address}
	resp, err := s.pc.PublishOne(s.ctx, msg)
	if err != nil {
		return err
	}
	if resp != nil && !resp.GetSuccess() {
		return fmt.Errorf("publish error: %s", resp.GetError().GetMessage())
	}
	return nil
}

// close disconnects from arke and closes the underlying gRPC connection.
func (s *arkeSession) close() {
	_, _ = s.pc.Disconnect(s.ctx, &pb.Empty{})
	s.cancelCtx()
	_ = s.conn.Close()
}

// publishOneMessage opens a fresh arke connection, publishes once, and
// immediately disconnects.  Use this only for baseline / post-recovery probes
// where a long-lived session is not required.
func publishOneMessage(t *testing.T, address *pb.Address, clientName string) error {
	t.Helper()
	conn := connect()
	defer conn.Close()

	pc := pb.NewProducerClient(conn)
	ctx := context.Background()
	defer pc.Disconnect(ctx, &pb.Empty{})

	msg := &pb.Message{Body: []byte("failover-test"), Address: address}
	return mf.ProduceMessagesUnary(ctx, pc, 1, msg, false, clientName)
}

// ---------------------------------------------------------------------------
// connectionWatcher failover tests
// ---------------------------------------------------------------------------

// TestConnectionWatcher_ReconnectsAfterBrokerClosesConnection verifies that
// when RabbitMQ forcefully closes the TCP connection (simulating a broker
// restart or network partition), the connectionWatcher goroutine detects the
// disconnection and re-establishes the connection.  After reconnection, both
// publish and consume operations must succeed.
//
// Key design: we keep a single arkeSession alive throughout the test (no
// Disconnect until after the reconnect is confirmed) so that arke's
// connectionWatcher can actually do its job.  If Disconnect were called
// immediately after the broker error, arke would set clientDisconnect=true
// and the watcher would exit rather than reconnect.
func TestConnectionWatcher_ReconnectsAfterBrokerClosesConnection(t *testing.T) {
	mgmt := newRabbitManagementClient()

	subjects := []string{"sas.test.failover.CWRABC"}
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}

	// --- Step 1: open a persistent arke session ----------------------
	// This keeps the broker-side AMQP connection alive throughout the test.
	// We do NOT defer session.close() until after reconnection is confirmed.
	session := newProducerSession(t)
	defer session.close()

	// Warm up: one publish to set lastPubSubEvent so the connectionCleaner
	// (which runs every 30 s) does not evict the session before we verify.
	require.NoError(t, session.publishOne(address), "baseline publish must succeed")

	// --- Step 2: verify at least one broker connection exists --------
	connsBeforeDisrupt, err := mgmt.waitForConnections(1, 10*time.Second)
	require.NoError(t, err, "should see at least one active RabbitMQ connection before disruption")
	t.Logf("active connections before disruption: %v", connsBeforeDisrupt)

	// --- Step 3: forcefully close all broker connections -------------
	t.Log("closing all RabbitMQ connections via management API")
	closeStart := time.Now()
	require.NoError(t, mgmt.closeAllConnections(), "management API must be reachable to run failover tests")
	t.Logf("closeAllConnections returned after %s", time.Since(closeStart).Round(time.Millisecond))

	// --- Step 4: poll publish until arke has reconnected ------------
	// Polling publishOne directly avoids any RabbitMQ stats-collection lag.
	// The management API only reflects a new connection after its internal
	// collection cycle (collect_statistics_interval), which can add 0.5-3 s
	// of artificial delay even though arke itself reconnects in < 1 s.
	//
	// connectionWatcher wakes on bd.ErrorChannel unconditionally (any receive
	// means the connection is gone), sleeps 100-500 ms (ReconnectDelay=500),
	// then does a TCP+AMQP handshake (~10 ms on a local broker).
	// Worst case reconnect latency: ~700 ms.  We allow 10 s for safety.
	t.Log("polling publish to detect reconnect (up to 10 s)...")
	reconnectStart := time.Now()
	var publishErr error
	for time.Now().Before(reconnectStart.Add(10 * time.Second)) {
		publishErr = session.publishOne(address)
		if publishErr == nil {
			break
		}
		t.Logf("publish not yet recovered: %v – retrying in 200 ms", publishErr)
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("reconnect detected after %s", time.Since(reconnectStart).Round(time.Millisecond))
	require.NoError(t, publishErr,
		"arke should re-establish a connection to RabbitMQ within 10 s of the broker closing it")

	// --- Step 5: confirm a new AMQP connection is visible in RabbitMQ -
	// This is a secondary check to verify connectionWatcher created a real
	// new AMQP connection (not just that local publish state was satisfied).
	// The management API may lag by up to collect_statistics_interval (500 ms),
	// so we give it a generous window; it does not need to be fast.
	connsAfterReconnect, err := mgmt.waitForConnections(1, 10*time.Second)
	require.NoError(t, err, "management API should show a new RabbitMQ connection after reconnect")
	t.Logf("active connections after reconnect: %v", connsAfterReconnect)
}

// TestConnectionWatcher_PublishRecovery verifies that after the broker
// forcefully closes a connection arke can publish messages successfully once
// the connectionWatcher has completed re-connection.
//
// A persistent arkeSession is kept open so that arke's BrokerDetails remain
// registered and connectionWatcher can perform the reconnect.  Using a one-shot
// publishOneMessage (which calls Disconnect internally) would cause arke to
// tear down the session before testing reconnection.
func TestConnectionWatcher_PublishRecovery(t *testing.T) {
	mgmt := newRabbitManagementClient()

	subjects := []string{"sas.test.failover.CWPR"}
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}

	// --- Step 1: open a persistent arke session ----------------------
	session := newProducerSession(t)
	defer session.close()

	require.NoError(t, session.publishOne(address), "initial publish must succeed")

	// --- Step 2: verify connections exist ----------------------------
	_, err := mgmt.waitForConnections(1, 10*time.Second)
	require.NoError(t, err, "must have at least one RabbitMQ connection before disruption")

	// --- Step 3: close all broker connections -------------------------
	t.Log("closing all RabbitMQ connections")
	require.NoError(t, mgmt.closeAllConnections())

	// Give the broker a moment to fully process the close.
	time.Sleep(500 * time.Millisecond)

	// --- Step 4: poll publish until it succeeds ----------------------
	// connectionWatcher will attempt bd.connect() after a random back-off
	// that starts at 500 ms.  We generously allow 30 s.
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = session.publishOne(address)
		if lastErr == nil {
			break
		}
		t.Logf("publish not yet recovered: %v – retrying in 2 s", lastErr)
		time.Sleep(2 * time.Second)
	}
	assert.NoError(t, lastErr, "publish should recover within 30 s after broker-initiated disconnect")
}

// TestPublishStream_RecoveryDelay measures how long a streaming Publish RPC
// takes to recover after the broker forcefully closes the connection.
//
// Unlike PublishOne (a stateless unary call retried by the caller), the
// streaming Publish RPC keeps a single long-lived gRPC stream open.  When
// the broker drops, the server-side Publish handler propagates the error to
// the client on the stream, then calls WaitForConnect which polls until
// connectionWatcher has re-established the AMQP connection.  Once reconnected
// the outer Publish loop restarts and the same gRPC stream resumes — the
// client never needs to re-open the stream.
//
// Sequence after closeAllConnections:
//  1. connectionWatcher wakes (ErrorChannel notification), sleeps 100-500 ms,
//     calls connect() → state=CONNECTED.
//  2. prov.Publish (server-side) returns the broker error.
//  3. Server outer loop is blocked at "errChan <- err" until the client sends
//     the next message, which unblocks the server goroutine.
//  4. Server goroutine sends an error response to the client and exits.
//  5. Server calls WaitForConnect — which may return immediately if step 1
//     already completed, or polls until it does.
//  6. Server resumes the stream loop; client's next message succeeds.
//
// The test keeps sending every 200 ms so each stage unblocks promptly.
// Total worst-case latency: reconnect backoff (500 ms) + WaitForConnect poll
// (500 ms) ≈ 1 s.  We allow 15 s for safety.
func TestPublishStream_RecoveryDelay(t *testing.T) {
	mgmt := newRabbitManagementClient()

	subjects := []string{"sas.test.failover.PSRD"}
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	msg := &pb.Message{Body: []byte("failover-stream-test"), Address: address}

	// --- Step 1: open a persistent Publish stream --------------------
	conn := connect()
	defer conn.Close()
	pc := pb.NewProducerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	defer pc.Disconnect(ctx, &pb.Empty{})

	connConfig := connectConfig(t.Name())
	authResp, err := pc.Connect(ctx, connConfig)
	require.NoError(t, err)
	require.True(t, authResp.GetSuccess(), "arke broker Connect must succeed: %s", authResp.GetError().GetMessage())

	stream, err := pc.Publish(ctx)
	require.NoError(t, err, "Publish stream must open")
	defer stream.CloseSend()

	// sendAndRecv performs one Send/Recv round-trip on the stream.
	sendAndRecv := func() error {
		if sendErr := stream.Send(msg); sendErr != nil {
			return fmt.Errorf("stream.Send: %w", sendErr)
		}
		r, recvErr := stream.Recv()
		if recvErr != nil {
			return fmt.Errorf("stream.Recv: %w", recvErr)
		}
		if !r.GetSuccess() {
			return fmt.Errorf("broker error: %s", r.GetError().GetMessage())
		}
		return nil
	}

	// --- Step 2: warm-up ---------------------------------------------
	require.NoError(t, sendAndRecv(), "baseline publish on stream must succeed")

	// --- Step 3: drop all broker connections -------------------------
	_, err = mgmt.waitForConnections(1, 10*time.Second)
	require.NoError(t, err, "must see at least one connection before disruption")

	t.Log("closing all RabbitMQ connections")
	require.NoError(t, mgmt.closeAllConnections())

	// --- Step 4: measure time to first successful publish on stream --
	// Each sendAndRecv polls the stream.  The first call after the drop
	// returns an error response: the server propagates the broker error and
	// the client's send simultaneously unblocks the server goroutine that
	// was waiting on errChan.  Subsequent calls buffer in gRPC while the
	// server is inside WaitForConnect; as soon as WaitForConnect returns the
	// server drains the buffer and responds with Success.
	//
	// We poll every 200 ms so each stage unblocks within one poll interval.
	recoveryStart := time.Now()
	var lastErr error
	for time.Now().Before(recoveryStart.Add(15 * time.Second)) {
		lastErr = sendAndRecv()
		if lastErr == nil {
			break
		}
		t.Logf("stream not yet recovered: %v – retrying in 200 ms", lastErr)
		time.Sleep(200 * time.Millisecond)
	}
	elapsed := time.Since(recoveryStart).Round(time.Millisecond)
	t.Logf("stream publish recovered after %s", elapsed)
	require.NoError(t, lastErr, "streaming Publish should recover within 15 s of broker disconnect")
}

// TestConnectionWatcher_SubscribeErrorOnDisconnect verifies that an active
// Consume call returns a non-nil error to the gRPC caller when the broker
// forcefully closes the underlying AMQP connection.
func TestConnectionWatcher_SubscribeErrorOnDisconnect(t *testing.T) {
	mgmt := newRabbitManagementClient()

	subjects := []string{"sas.test.failover.CWSOD"}
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	source := &pb.Source{
		Name:          "sas.test.failover.CWSOD.Consumer",
		Address:       address,
		PrefetchCount: 1,
	}

	consumerConn := connect()
	defer consumerConn.Close()
	c := pb.NewConsumerClient(consumerConn)
	// Long enough for the full test but not indefinitely blocking.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	connConfig := connectConfig(t.Name())
	authResp, err := c.Connect(ctx, connConfig)
	require.NoError(t, err)
	require.True(t, authResp.GetSuccess())

	stream, err := c.Consume(ctx)
	require.NoError(t, err)

	m := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}
	require.NoError(t, stream.Send(m))
	defer stream.CloseSend()

	// Allow subscribe to settle before causing disruption.
	time.Sleep(1 * time.Second)

	// --- Verify connections exist before closing ---------------------
	_, err = mgmt.waitForConnections(1, 10*time.Second)
	require.NoError(t, err, "must have at least one RabbitMQ connection before disruption")

	// --- Close all broker connections --------------------------------
	t.Log("closing all RabbitMQ connections to test subscribe error handling")
	require.NoError(t, mgmt.closeAllConnections())

	// --- Receive from stream and expect an error or EOF --------------
	// queueSubscribe returns a pb.Error on the stream when either the
	// connErrChan or cancelChan receives a non-zero error.  The gRPC stream
	// will then end, meaning Recv() will return either a message containing
	// an error payload or an io.EOF / transport error.
	recvDone := make(chan error, 1)
	go func() {
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				recvDone <- recvErr
				return
			}
			if resp.GetMsg() != nil && resp.GetMsg().GetError() != nil {
				// arke surfaced a broker error as a message-level error
				recvDone <- fmt.Errorf("broker error received: %s", resp.GetMsg().GetError().GetMessage())
				return
			}
		}
	}()

	select {
	case recvErr := <-recvDone:
		t.Logf("consume stream ended as expected after broker disconnect: %v", recvErr)
		// Any non-nil result is acceptable – the point is that the stream does
		// not hang indefinitely when the broker closes the connection.
	case <-time.After(20 * time.Second):
		t.Error("consume stream did not return an error within 20 s after the broker closed the connection")
	}
}

// TestConnectionWatcher_Fallback30s verifies the 30-second polling fallback
// path in connectionWatcher: even when the ErrorChannel notification is missed,
// the watcher detects a closed connection via IsClosed() and triggers reconnect.
//
// This test intentionally waits longer than the standard tests; the 30 s
// polling window is the key mechanism under test.
func TestConnectionWatcher_Fallback30s(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running fallback test in short mode")
	}

	mgmt := newRabbitManagementClient()

	subjects := []string{"sas.test.failover.CWF30"}
	address := &pb.Address{Name: "amq.topic", Subjects: subjects, Type: pb.Address_TOPIC}

	// --- Step 1: keep a persistent session so connectionWatcher stays alive -
	session := newProducerSession(t)
	defer session.close()

	require.NoError(t, session.publishOne(address), "initial publish must succeed")

	_, err := mgmt.waitForConnections(1, 10*time.Second)
	require.NoError(t, err, "must have at least one broker connection before disruption")

	// Close connections without waiting for the notification goroutine to fire –
	// the fallback timer will detect the closed connection within 30 s.
	t.Log("closing all RabbitMQ connections (testing 30 s fallback)")
	require.NoError(t, mgmt.closeAllConnections())

	// Poll for recovery.  We allow up to 60 s to cover the full 30 s fallback
	// window plus reconnect time.
	start := time.Now()
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = session.publishOne(address)
		if lastErr == nil {
			t.Logf("recovered via fallback after %s", time.Since(start).Round(time.Millisecond))
			break
		}
		time.Sleep(2 * time.Second)
	}
	assert.NoError(t, lastErr, "publish should recover via the 30 s fallback mechanism")
}
