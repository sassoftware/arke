// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package amqp091

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider/connectors/amqp"
	amqp10 "github.com/sassoftware/arke/internal/provider/connectors/rabbitmq/amqp10"
	"github.com/sassoftware/arke/internal/util"
	cfg "github.com/sassoftware/arke/test/config"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"
)

type declareQueuePeerAddr string

func (a declareQueuePeerAddr) Network() string { return "tcp" }
func (a declareQueuePeerAddr) String() string  { return string(a) }

type declaredQueueProperties struct {
	queueType            string
	expires              int64
	messageTTL           int64
	deadLetterExchange   string
	deadLetterRoutingKey string
	singleActiveConsumer bool
}

type rabbitQueueManagementClient struct {
	endpoint string
	vhost    string
	username string
	password string
	client   *amqp.AMQPManagementClient
}

func newRabbitQueueManagementClient(connectionConfig *pb.ConnectionConfiguration) (*rabbitQueueManagementClient, error) {
	vhost := connectionConfig.GetTenant()
	if vhost == "" {
		vhost = "/"
	}
	username, password := amqp.GetUsernamePassword(connectionConfig)
	endpoint := amqp.GetMgmtEndpoint(connectionConfig)
	client, err := amqp.NewManagementClient(context.Background(), endpoint, username, password, nil)
	if err != nil {
		return nil, err
	}
	return &rabbitQueueManagementClient{
		endpoint: endpoint,
		vhost:    vhost,
		username: username,
		password: password,
		client:   client,
	}, nil
}

func (m *rabbitQueueManagementClient) request(ctx context.Context, method, queueName string) ([]byte, int, error) {
	endpoint := m.endpoint + "/api/queues/" + url.PathEscape(m.vhost) + "/" + url.PathEscape(queueName)
	request, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	request.SetBasicAuth(m.username, m.password)
	return m.client.Do(request)
}

func (m *rabbitQueueManagementClient) deleteQueue(ctx context.Context, queueName string) error {
	_, statusCode, err := m.request(ctx, http.MethodDelete, queueName)
	if err != nil {
		return err
	}
	if statusCode != http.StatusNoContent && statusCode != http.StatusNotFound {
		return fmt.Errorf("management API DELETE queue %q returned %d", queueName, statusCode)
	}
	return nil
}

func (m *rabbitQueueManagementClient) queueProperties(ctx context.Context, queueName string) (declaredQueueProperties, error) {
	data, statusCode, err := m.request(ctx, http.MethodGet, queueName)
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
	ctx = peer.NewContext(ctx, &peer.Peer{Addr: declareQueuePeerAddr(name + "-" + uuid.NewString())})
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
	management, err := newRabbitQueueManagementClient(&connectionConfig)
	require.NoError(t, err)
	baseQueueName := "issue190-" + uuid.NewString()
	queueName091 := baseQueueName + "-091.quorum"
	queueName10 := baseQueueName + "-10.quorum"
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := management.deleteQueue(cleanupCtx, queueName091); err != nil {
			t.Errorf("delete AMQP 0.9.1 queue: %v", err)
		}
		if err := management.deleteQueue(cleanupCtx, queueName10); err != nil {
			t.Errorf("delete AMQP 1.0 queue: %v", err)
		}
	})

	provider091 := NewAMQP091Provider().(*amqp091provider)
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
	source091.Name = sourceName(source091)
	brokerDetails091, err := provider091.getBrokerDetails(ctx091)
	require.NoError(t, err)
	require.NoError(t, provider091.declareQueue(source091, brokerDetails091, true))

	source10 := newSource(baseQueueName + "-10")
	require.Nil(t, provider10.Subscribe(ctx10, source10, nil))

	properties091 := waitForQueueProperties(t, management, queueName091)
	properties10 := waitForQueueProperties(t, management, queueName10)
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
}

func waitForQueueProperties(t *testing.T, management *rabbitQueueManagementClient, queueName string) declaredQueueProperties {
	t.Helper()
	var properties declaredQueueProperties
	var lastErr error
	require.Eventually(t, func() bool {
		properties, lastErr = management.queueProperties(context.Background(), queueName)
		return lastErr == nil
	}, 5*time.Second, 100*time.Millisecond, "queue %q properties unavailable: %v", queueName, lastErr)
	return properties
}
