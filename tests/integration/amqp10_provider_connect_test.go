// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/provider/connectors/amqp"
	amqp091 "github.com/sassoftware/arke/internal/provider/connectors/amqp091"
	amqp10 "github.com/sassoftware/arke/internal/provider/connectors/rabbitmq/amqp10"
	"github.com/sassoftware/arke/internal/util"
	cfg "github.com/sassoftware/arke/test/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"
)

func Test_ProviderConnectsToBroker(t *testing.T) {
	hostname := os.Getenv("ARKE_BROKER_HOSTNAME")
	if hostname == "" {
		hostname = "localhost"
	}
	t.Setenv("ARKE_BROKER_HOSTNAME", hostname)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	clientAddr := fmt.Sprintf("test-connect-integration-%d", time.Now().UnixNano())
	ctx = peer.NewContext(ctx, &peer.Peer{Addr: mockPeerAddr{clientAddr: clientAddr}})

	clientIdentifier, err := util.SetClientIdentifier(ctx, "test-connect-integration")
	require.NoError(t, err)
	defer util.RemoveClientIdentifier(ctx)

	connConfig := cfg.ConnectionConfigurationFromEnv()
	t.Logf("cf: %+v", connConfig)
	connConfig.ClientName = clientIdentifier
	prov, err := provider.NewProvider(cfg.ConnectionConfigurationFromEnv().Provider)
	require.NoError(t, err)

	connectErr := prov.Connect(ctx, &connConfig, false)
	require.Nil(t, connectErr, "provider connect failed: %v", connectErr)
	assert.True(t, prov.ClientExists(clientIdentifier))
	assert.Eventually(t, func() bool {
		return prov.WaitForConnect(ctx)
	}, 2*time.Second, 100*time.Millisecond)

	source := &pb.Source{
		Name: "queue-name",
		Address: &pb.Address{
			Name:     "queue-name",
			Subjects: []string{},
			Type:     pb.Address_QUEUE,
		},
	}
	stats := prov.SourceStats(ctx, source)
	assert.Nil(t, stats.Error)
	assert.NotNil(t, stats)
	// Used UI to create the queue queue-name.quorum and publish msgs.
	// Ran this test and observed message_count and publish_rate.
	t.Logf("Source stats: %+v", stats)

	exchangeName := "arke-amqp10-bindings-" + uuid.NewString()
	queueName := exchangeName + "-queue"
	routingKeys := []string{"integration.subject.retained", "integration.subject.removed"}
	bindingSource := &pb.Source{
		Name:       queueName,
		Type:       pb.Source_QUEUE,
		AutoDelete: true,
		Address: &pb.Address{
			Name:     exchangeName,
			Type:     pb.Address_TOPIC,
			Subjects: routingKeys,
		},
	}
	username, password := amqp.GetUsernamePassword(&connConfig)
	management, err := amqp.NewManagementClient(ctx, amqp.GetMgmtEndpoint(&connConfig), username, password, nil)
	require.NoError(t, err)
	vhost := amqp.GetVhost(&connConfig)
	queueName = amqp.SourceName(bindingSource)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := deleteQueue(cleanupCtx, management, vhost, queueName); err != nil {
			t.Errorf("delete AMQP 1.0 binding test queue: %v", err)
		}
		exchangePath := "/api/exchanges/" + url.PathEscape(vhost) + "/" + url.PathEscape(exchangeName)
		_, statusCode, err := management.Request(cleanupCtx, http.MethodDelete, exchangePath)
		if err != nil {
			t.Errorf("delete AMQP 1.0 binding test exchange: %v", err)
		} else if statusCode != http.StatusNoContent && statusCode != http.StatusNotFound {
			t.Errorf("delete AMQP 1.0 binding test exchange returned %d", statusCode)
		}
	})

	provCtx := ctx
	go prov.Subscribe(provCtx, bindingSource, make(chan *pb.Message))
	bindingPath := fmt.Sprintf("/api/bindings/%s/e/%s/q/%s",
		url.PathEscape(vhost), url.PathEscape(exchangeName), url.PathEscape(queueName))
	type bindingInfo struct {
		Source          string `json:"source"`
		Destination     string `json:"destination"`
		DestinationType string `json:"destination_type"`
		RoutingKey      string `json:"routing_key"`
	}
	var bindings []bindingInfo
	var matchingBindings []bindingInfo
	var bindingErr error
	readMatchingBindings := func() bool {
		body, statusCode, err := management.Request(ctx, http.MethodGet, bindingPath)
		if err != nil {
			bindingErr = err
			return false
		}
		if statusCode != http.StatusOK {
			bindingErr = fmt.Errorf("management API GET bindings returned %d", statusCode)
			return false
		}
		bindingErr = json.Unmarshal(body, &bindings)
		if bindingErr != nil {
			return false
		}
		matchingBindings = matchingBindings[:0]
		for _, binding := range bindings {
			if binding.Source != exchangeName || binding.Destination != queueName || binding.DestinationType != "queue" {
				continue
			}
			matchingBindings = append(matchingBindings, binding)
		}
		return true
	}
	expectedBindings := func(subjects []string) []bindingInfo {
		bindings := make([]bindingInfo, 0, len(subjects))
		for _, subject := range subjects {
			bindings = append(bindings, bindingInfo{
				Source:          exchangeName,
				Destination:     queueName,
				DestinationType: "queue",
				RoutingKey:      subject,
			})
		}
		return bindings
	}
	require.Eventually(t, func() bool {
		return readMatchingBindings() && len(matchingBindings) == len(routingKeys)
	},
		5*time.Second, 100*time.Millisecond, "bindings unavailable: %v", bindingErr)
	require.ElementsMatch(t, expectedBindings(routingKeys), matchingBindings)

	bindingSource.Address.Subjects = []string{routingKeys[0]}
	prov.Disconnect(provCtx)
	provCtx = ctx

	pbErr := prov.Connect(provCtx, &connConfig, false)
	require.Nil(t, pbErr, "provider connect failed: %v", pbErr)
	go func() {
		prov.Subscribe(provCtx, bindingSource, nil)
	}()
	require.Eventually(t, func() bool {
		return readMatchingBindings() && len(matchingBindings) == len(bindingSource.Address.Subjects)
	},
		5*time.Second, 100*time.Millisecond, "stale binding cleanup unavailable: %v", bindingErr)
	require.ElementsMatch(t, expectedBindings(bindingSource.Address.Subjects), matchingBindings)

	prov.Disconnect(ctx)
	assert.False(t, prov.ClientExists(clientIdentifier))
}

func Test_AMQP10ProviderSubscribe(t *testing.T) {
	f := newAMQP10SubscribeFixture(t, 30*time.Second)
	assert.True(t, f.prov.ClientExists(f.clientID))

	f.publishWhenRouted("hello")
	f.expectMessage("hello", 15*time.Second)

	stats := f.prov.SourceStats(f.ctx, f.source)
	assert.Nil(t, stats.Error)

	f.prov.Disconnect(f.ctx)
	f.expectSubscribeReturns()
	assert.False(t, f.prov.ClientExists(f.clientID), "client should not exist after disconnect")
}

func Test_AMQP10ProviderSubscribeRecoversFromBrokerClose(t *testing.T) {
	f := newAMQP10SubscribeFixture(t, 90*time.Second)
	f.publishWhenRouted("before-drop")
	f.expectMessage("before-drop", 15*time.Second)

	closed := f.closeBrokerConnection()

	// The client library waits at least 5s before its first reconnect attempt.
	require.Eventually(t, func() bool {
		for _, name := range f.brokerConnections() {
			if !slices.Contains(closed, name) {
				return true
			}
		}
		return false
	}, 40*time.Second, 500*time.Millisecond, "client did not reconnect after the broker closed its connection")
	assert.True(t, f.prov.ClientExists(f.clientID))
	select {
	case err := <-f.subscribeDone:
		t.Fatalf("Subscribe returned during recovery: %v", err)
	default:
	}

	f.publishWhenRouted("after-drop")
	f.expectMessage("after-drop", 30*time.Second, "before-drop")

	f.prov.Disconnect(f.ctx)
	f.expectSubscribeReturns()
	assert.False(t, f.prov.ClientExists(f.clientID))
	assert.Eventually(t, func() bool { return len(f.brokerConnections()) == 0 },
		5*time.Second, 200*time.Millisecond, "Disconnect left a broker connection open")
}

// amqp10SubscribeFixture connects an AMQP 1.0 client and subscribes it to a new
// queue. The management API publishes messages and drops the connection.
type amqp10SubscribeFixture struct {
	t             *testing.T
	ctx           context.Context
	prov          provider.Provider
	clientID      string
	exchange      string
	vhost         string
	source        *pb.Source
	messages      chan *pb.Message
	subscribeDone chan *pb.Error
	endpoint      string
	username      string
	password      string
}

func newAMQP10SubscribeFixture(t *testing.T, timeout time.Duration) *amqp10SubscribeFixture {
	t.Helper()
	if os.Getenv("ARKE_BROKER_HOSTNAME") == "" {
		t.Setenv("ARKE_BROKER_HOSTNAME", "localhost")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	ctx = peer.NewContext(ctx, &peer.Peer{Addr: mockPeerAddr{clientAddr: fmt.Sprintf("test-amqp10-subscribe-%d", time.Now().UnixNano())}})
	clientID, err := util.SetClientIdentifier(ctx, "test-amqp10-subscribe")
	require.NoError(t, err)
	t.Cleanup(func() { util.RemoveClientIdentifier(ctx) })

	connConfig := cfg.ConnectionConfigurationFromEnv()
	connConfig.ClientName = clientID
	prov, err := provider.NewProvider("rabbitmq-amqp10")
	require.NoError(t, err)

	exchange := "arke-amqp10-subscribe-" + uuid.NewString()
	source := &pb.Source{
		Name:       exchange + "-queue",
		Type:       pb.Source_QUEUE,
		AutoDelete: true,
		Address: &pb.Address{
			Name:     exchange,
			Type:     pb.Address_TOPIC,
			Subjects: []string{"integration.subscribe"},
		},
	}
	username, password := amqp.GetUsernamePassword(&connConfig)
	endpoint := amqp.GetMgmtEndpoint(&connConfig)
	vhost := amqp.GetVhost(&connConfig)
	management, err := amqp.NewManagementClient(ctx, endpoint, username, password, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := deleteQueue(cleanupCtx, management, vhost, amqp.SourceName(source)); err != nil {
			t.Errorf("delete queue: %v", err)
		}
		_, statusCode, err := management.Request(cleanupCtx, http.MethodDelete, "/api/exchanges/"+url.PathEscape(vhost)+"/"+url.PathEscape(exchange))
		if err != nil || (statusCode != http.StatusNoContent && statusCode != http.StatusNotFound) {
			t.Errorf("delete exchange: status %d, error %v", statusCode, err)
		}
	})

	require.Nil(t, prov.Connect(ctx, &connConfig, false))
	t.Cleanup(func() { prov.Disconnect(ctx) })
	require.True(t, prov.WaitForConnect(ctx))

	f := &amqp10SubscribeFixture{
		t:             t,
		ctx:           ctx,
		prov:          prov,
		clientID:      clientID,
		exchange:      exchange,
		vhost:         vhost,
		source:        source,
		messages:      make(chan *pb.Message, 10),
		subscribeDone: make(chan *pb.Error, 1),
		endpoint:      endpoint,
		username:      username,
		password:      password,
	}
	go func() { f.subscribeDone <- prov.Subscribe(ctx, source, f.messages) }()
	return f
}

// managementRequest returns an error rather than failing the test so it is safe
// to call from require.Eventually callbacks.
func (f *amqp10SubscribeFixture) managementRequest(method, path string, body []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(f.ctx, method, f.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.SetBasicAuth(f.username, f.password)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return data, resp.StatusCode, err
}

// publish reports whether the broker routed the message to the queue.
func (f *amqp10SubscribeFixture) publish(payload string) bool {
	body, err := json.Marshal(map[string]any{
		"properties":       map[string]any{},
		"routing_key":      f.source.GetAddress().GetSubjects()[0],
		"payload":          payload,
		"payload_encoding": "string",
	})
	if err != nil {
		return false
	}
	path := "/api/exchanges/" + url.PathEscape(f.vhost) + "/" + url.PathEscape(f.exchange) + "/publish"
	data, status, err := f.managementRequest(http.MethodPost, path, body)
	if err != nil || status != http.StatusOK {
		return false
	}
	var result struct {
		Routed bool `json:"routed"`
	}
	return json.Unmarshal(data, &result) == nil && result.Routed
}

// publishWhenRouted waits for Subscribe to create the queue and binding.
func (f *amqp10SubscribeFixture) publishWhenRouted(payload string) {
	f.t.Helper()
	require.Eventually(f.t, func() bool { return f.publish(payload) },
		15*time.Second, 200*time.Millisecond, "message %q was never routed to the queue", payload)
}

func (f *amqp10SubscribeFixture) expectMessage(payload string, timeout time.Duration, redelivered ...string) {
	f.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case msg := <-f.messages:
			// Ack is not implemented yet, so the broker redelivers unacked messages.
			if slices.Contains(redelivered, string(msg.GetBody())) {
				continue
			}
			require.Equal(f.t, payload, string(msg.GetBody()))
			return
		case err := <-f.subscribeDone:
			f.t.Fatalf("Subscribe returned before %q was delivered: %v", payload, err)
		case <-deadline:
			f.t.Fatalf("timed out waiting for message %q", payload)
		}
	}
}

func (f *amqp10SubscribeFixture) expectSubscribeReturns() {
	f.t.Helper()
	select {
	case err := <-f.subscribeDone:
		assert.Nil(f.t, err)
	case <-time.After(10 * time.Second):
		f.t.Fatal("Subscribe did not return")
	}
}

// brokerConnections lists the names of this client's connections as the broker sees them.
func (f *amqp10SubscribeFixture) brokerConnections() []string {
	data, status, err := f.managementRequest(http.MethodGet, "/api/connections", nil)
	if err != nil || status != http.StatusOK {
		return nil
	}
	var connections []struct {
		Name        string `json:"name"`
		ContainerID string `json:"container_id"`
	}
	if json.Unmarshal(data, &connections) != nil {
		return nil
	}
	var names []string
	for _, connection := range connections {
		if connection.ContainerID == f.clientID {
			names = append(names, connection.Name)
		}
	}
	return names
}

// closeBrokerConnection force-closes this client's connections and returns their names.
func (f *amqp10SubscribeFixture) closeBrokerConnection() []string {
	f.t.Helper()
	// The management API lists a connection only after its next stats update.
	var names []string
	require.Eventually(f.t, func() bool {
		names = f.brokerConnections()
		return len(names) > 0
	}, 15*time.Second, 200*time.Millisecond, "broker has no connection for client %s", f.clientID)
	for _, name := range names {
		_, status, err := f.managementRequest(http.MethodDelete, "/api/connections/"+url.PathEscape(name), nil)
		require.NoError(f.t, err)
		require.Equal(f.t, http.StatusNoContent, status)
	}
	return names
}

func Test_AMQP10DeclareExchangeAllowsIncompatibleRedeclaration(t *testing.T) {
	hostname := os.Getenv("ARKE_BROKER_HOSTNAME")
	if hostname == "" {
		hostname = "localhost"
	}
	t.Setenv("ARKE_BROKER_HOSTNAME", hostname)

	connectionConfig := cfg.ConnectionConfigurationFromEnv()
	prov, err := provider.NewProvider(connectionConfig.Provider)
	require.NoError(t, err)

	connectClient := func(clientName string) (context.Context, context.CancelFunc) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		clientAddr := "test-amqp10-declare-" + uuid.NewString()
		ctx = peer.NewContext(ctx, &peer.Peer{Addr: mockPeerAddr{clientAddr: clientAddr}})
		clientIdentifier, err := util.SetClientIdentifier(ctx, clientName)
		require.NoError(t, err)

		connectionConfig := cfg.ConnectionConfigurationFromEnv()
		connectionConfig.ClientName = clientIdentifier
		require.Nil(t, prov.Connect(ctx, &connectionConfig, false))
		t.Cleanup(func() {
			prov.Disconnect(ctx)
			util.RemoveClientIdentifier(ctx)
			cancel()
		})
		return ctx, cancel
	}

	exchangeName := "arke-amqp10-redeclare-" + uuid.NewString()
	ctxA, _ := connectClient("test-amqp10-declare-a")
	ctxB, _ := connectClient("test-amqp10-declare-b")
	sourceA := &pb.Source{
		Name:        exchangeName + "-queue-a",
		Type:        pb.Source_QUEUE,
		DeclareOnly: true,
		Address: &pb.Address{
			Name:       exchangeName,
			Type:       pb.Address_TOPIC,
			AutoDelete: true,
		},
	}
	sourceB := &pb.Source{
		Name:        exchangeName + "-queue-b",
		Type:        pb.Source_QUEUE,
		DeclareOnly: true,
		Address: &pb.Address{
			Name:       exchangeName,
			Type:       pb.Address_TOPIC,
			AutoDelete: false,
		},
	}

	username, password := amqp.GetUsernamePassword(&connectionConfig)
	management, err := amqp.NewManagementClient(context.Background(), amqp.GetMgmtEndpoint(&connectionConfig), username, password, nil)
	require.NoError(t, err)
	vhost := amqp.GetVhost(&connectionConfig)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, source := range []*pb.Source{sourceA, sourceB} {
			if err := deleteQueue(cleanupCtx, management, vhost, amqp.SourceName(source)); err != nil {
				t.Errorf("delete queue %s: %v", source.GetName(), err)
			}
		}
		exchangePath := "/api/exchanges/" + url.PathEscape(vhost) + "/" + url.PathEscape(exchangeName)
		_, statusCode, err := management.Request(cleanupCtx, http.MethodDelete, exchangePath)
		if err != nil {
			t.Errorf("delete exchange %s: %v", exchangeName, err)
		} else if statusCode != http.StatusNoContent && statusCode != http.StatusNotFound {
			t.Errorf("delete exchange %s returned %d", exchangeName, statusCode)
		}
	})

	require.Nil(t, prov.Subscribe(ctxA, sourceA, nil))
	require.Nil(t, prov.Subscribe(ctxB, sourceB, nil))
}

type declaredQueueProperties struct {
	queueType            string
	expires              int64
	messageTTL           int64
	deadLetterExchange   string
	deadLetterRoutingKey string
	singleActiveConsumer bool
}

func queueManagementPath(vhost, queueName string) string {
	if vhost == "" {
		vhost = "/"
	}
	return "/api/queues/" + url.PathEscape(vhost) + "/" + url.PathEscape(queueName)
}

func deleteQueue(ctx context.Context, client *amqp.AMQPManagementClient, vhost, queueName string) error {
	_, statusCode, err := client.Request(ctx, http.MethodDelete, queueManagementPath(vhost, queueName))
	if err != nil {
		return err
	}
	if statusCode != http.StatusNoContent && statusCode != http.StatusNotFound {
		return fmt.Errorf("management API DELETE queue %q returned %d", queueName, statusCode)
	}
	return nil
}

func queueProperties(ctx context.Context, client *amqp.AMQPManagementClient, vhost, queueName string) (declaredQueueProperties, error) {
	data, statusCode, err := client.Request(ctx, http.MethodGet, queueManagementPath(vhost, queueName))
	if err != nil {
		return declaredQueueProperties{}, err
	}
	if statusCode != http.StatusOK {
		return declaredQueueProperties{}, fmt.Errorf("management API GET queue %q returned %d", queueName, statusCode)
	}

	var body struct {
		Type      string         `json:"type"`
		Arguments map[string]any `json:"arguments"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&body); err != nil {
		return declaredQueueProperties{}, err
	}
	intArgument := func(name string) (int64, error) {
		value, ok := body.Arguments[name]
		if !ok {
			return 0, fmt.Errorf("queue %q has no %s argument", queueName, name)
		}
		number, ok := value.(json.Number)
		if !ok {
			return 0, fmt.Errorf("queue %q argument %s is %T, not a number", queueName, name, value)
		}
		return number.Int64()
	}
	expires, err := intArgument("x-expires")
	if err != nil {
		return declaredQueueProperties{}, err
	}
	messageTTL, err := intArgument("x-message-ttl")
	if err != nil {
		return declaredQueueProperties{}, err
	}
	deadLetterExchange, ok := body.Arguments["x-dead-letter-exchange"].(string)
	if !ok {
		return declaredQueueProperties{}, fmt.Errorf("queue %q has no string x-dead-letter-exchange argument", queueName)
	}
	deadLetterRoutingKey, ok := body.Arguments["x-dead-letter-routing-key"].(string)
	if !ok {
		return declaredQueueProperties{}, fmt.Errorf("queue %q has no string x-dead-letter-routing-key argument", queueName)
	}
	singleActiveConsumer, ok := body.Arguments["x-single-active-consumer"].(bool)
	if !ok {
		return declaredQueueProperties{}, fmt.Errorf("queue %q has no boolean x-single-active-consumer argument", queueName)
	}
	return declaredQueueProperties{
		queueType:            body.Type,
		expires:              expires,
		messageTTL:           messageTTL,
		deadLetterExchange:   deadLetterExchange,
		deadLetterRoutingKey: deadLetterRoutingKey,
		singleActiveConsumer: singleActiveConsumer,
	}, nil
}

func integrationClientContext(t *testing.T, name string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	ctx = peer.NewContext(ctx, &peer.Peer{Addr: mockPeerAddr{clientAddr: name + "-" + uuid.NewString()}})
	_, err := util.SetClientIdentifier(ctx, name)
	require.NoError(t, err)
	t.Cleanup(func() {
		util.RemoveClientIdentifier(ctx)
		cancel()
	})
	return ctx
}

func Test_AMQP091AndAMQP10QueueDeclarationsMatch(t *testing.T) {
	if strings.EqualFold(os.Getenv("ARKE_PROVIDER_TLS"), "true") {
		t.Skip("queue declaration comparison uses the plain HTTP management endpoint")
	}
	if os.Getenv("ARKE_BROKER_HOSTNAME") == "" {
		t.Setenv("ARKE_BROKER_HOSTNAME", "localhost")
	}
	if os.Getenv("ARKE_BROKER_ADMIN_PORT") == "" {
		t.Setenv("ARKE_BROKER_ADMIN_PORT", "15672")
	}

	connectionConfig := cfg.ConnectionConfigurationFromEnv()
	username, password := amqp.GetUsernamePassword(&connectionConfig)
	management, err := amqp.NewManagementClient(context.Background(), amqp.GetMgmtEndpoint(&connectionConfig), username, password, nil)
	require.NoError(t, err)
	baseQueueName := "issue190-" + uuid.NewString()
	queueName091 := baseQueueName + "-091.quorum"
	queueName10 := baseQueueName + "-10.quorum"
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := deleteQueue(cleanupCtx, management, connectionConfig.GetTenant(), queueName091); err != nil {
			t.Errorf("delete AMQP 0.9.1 queue: %v", err)
		}
		if err := deleteQueue(cleanupCtx, management, connectionConfig.GetTenant(), queueName10); err != nil {
			t.Errorf("delete AMQP 1.0 queue: %v", err)
		}
	})

	provider091 := amqp091.NewAMQP091Provider()
	provider10 := amqp10.NewRabbitMQAMQP10Provider()
	ctx091 := integrationClientContext(t, "issue190-amqp091")
	ctx10 := integrationClientContext(t, "issue190-amqp10")
	t.Cleanup(func() {
		provider10.Disconnect(ctx10)
		provider091.Disconnect(ctx091)
	})
	require.Nil(t, provider091.Connect(ctx091, &connectionConfig, false))
	require.Nil(t, provider10.Connect(ctx10, &connectionConfig, false))

	newSource := func(name string) *pb.Source {
		return &pb.Source{
			Name:                 name,
			Type:                 pb.Source_QUEUE,
			SingleActiveConsumer: true,
			DeclareOnly:          true,
			Options: map[string]string{
				"MessageTTL":        "3210",
				"Expires":           "600000",
				"DeadLetterAddress": "issue190-dead-letter",
				"DeadLetterSubject": "issue190.route",
			},
			Address: &pb.Address{Name: "amq.direct", Type: pb.Address_QUEUE},
		}
	}

	// Compare durable quorum queues; the reserved address avoids exchange creation, binding, and consumption.
	source091 := newSource(baseQueueName + "-091")
	queueName091 = amqp.SourceName(source091)
	require.Nil(t, provider091.Subscribe(ctx091, source091, nil))

	source10 := newSource(baseQueueName + "-10")
	queueName10 = amqp.SourceName(source10)
	require.Nil(t, provider10.Subscribe(ctx10, source10, nil))

	properties091 := waitForQueueProperties(t, management, connectionConfig.GetTenant(), queueName091)
	properties10 := waitForQueueProperties(t, management, connectionConfig.GetTenant(), queueName10)
	expected := declaredQueueProperties{
		queueType:            "quorum",
		expires:              600000,
		messageTTL:           3210,
		deadLetterExchange:   "issue190-dead-letter",
		deadLetterRoutingKey: "issue190.route",
		singleActiveConsumer: true,
	}
	require.Equal(t, expected, properties091)
	require.Equal(t, expected, properties10)

	conflictingSource := newSource(baseQueueName + "-10")
	conflictingSource.Options["MessageTTL"] = "3211"
	conflictErr := provider10.Subscribe(ctx10, conflictingSource, nil)
	t.Logf("conflicting queue redeclaration result: %v", conflictErr)
	require.Nil(t, conflictErr)

	properties10 = waitForQueueProperties(t, management, connectionConfig.GetTenant(), queueName10)
	require.Equal(t, expected, properties10)
}

func waitForQueueProperties(t *testing.T, management *amqp.AMQPManagementClient, vhost, queueName string) declaredQueueProperties {
	t.Helper()
	var properties declaredQueueProperties
	var lastErr error
	require.Eventually(t, func() bool {
		properties, lastErr = queueProperties(context.Background(), management, vhost, queueName)
		return lastErr == nil
	}, 5*time.Second, 100*time.Millisecond, "queue %q properties unavailable: %v", queueName, lastErr)
	return properties
}
