// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package amqp10

import (
	"fmt"
	"testing"
	"time"

	rabbitamqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	rabbitstream "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider/connectors/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests are package-local because the Stream connection shim is private.
// Run with:
//
//	go test -tags=integration ./internal/provider/connectors/rabbitmq/amqp10 \
//	  -run 'Test_(streamConnectionIntegrationLifecycle|streamConnectionGetStreamOffsetIntegration|amqp10ProviderConnectDoesNotRequireStreamConnection)' \
//	  -count=1 -v
func Test_streamConnectionIntegrationLifecycle(t *testing.T) {
	connection := newStreamConnection(amqp.GetStreamConnURL(newTestConnectionConfig()), nil, nil)
	assert.True(t, connection.IsClosed())

	require.NoError(t, connection.Connect())
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close AMQP 1.0 Stream connection: %v", err)
		}
	})
	assert.False(t, connection.IsClosed())

	require.NoError(t, connection.Close())
	assert.True(t, connection.IsClosed())
}

func Test_streamConnectionGetStreamOffsetIntegration(t *testing.T) {
	connection := newStreamConnection(amqp.GetStreamConnURL(newTestConnectionConfig()), nil, nil)
	require.NoError(t, connection.Connect())
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close AMQP 1.0 Stream connection: %v", err)
		}
	})

	streamName := fmt.Sprintf("arke-offset-%d", time.Now().UnixNano())
	environment, ok := connection.env.(interface {
		Client() *rabbitstream.Environment
	})
	require.True(t, ok)
	client := environment.Client()
	require.NotNil(t, client)
	require.NoError(t, client.DeclareStream(streamName, rabbitstream.NewStreamOptions()))
	t.Cleanup(func() {
		if err := client.DeleteStream(streamName); err != nil {
			t.Errorf("delete integration stream %s: %v", streamName, err)
		}
	})

	_, err := connection.GetStreamOffset(streamName)
	require.ErrorIs(t, err, rabbitstream.OffsetNotFoundError)

	producer, err := client.NewProducer(streamName, rabbitstream.NewProducerOptions())
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := producer.Close(); err != nil {
			t.Errorf("close integration stream producer: %v", err)
		}
	})
	confirmationCh := producer.NotifyPublishConfirmation()
	messages := []string{"first", "middle", "last"}
	for _, body := range messages {
		require.NoError(t, producer.Send(rabbitamqp.NewMessage([]byte(body))))
	}

	confirmed := 0
	confirmationTimeout := time.NewTimer(5 * time.Second)
	defer confirmationTimeout.Stop()
	for confirmed < len(messages) {
		select {
		case confirmations, ok := <-confirmationCh:
			require.True(t, ok, "publisher confirmation channel closed before all messages were confirmed")
			for _, confirmation := range confirmations {
				require.True(t, confirmation.IsConfirmed(), "stream message was not confirmed: %v", confirmation.GetError())
				confirmed++
			}
		case <-confirmationTimeout.C:
			t.Fatal("timed out waiting for stream publish confirmations")
		}
	}

	offset, err := connection.GetStreamOffset(streamName)

	require.NoError(t, err)
	assert.Equal(t, int64(len(messages)-1), offset)

	stats := &pb.SourceStats{}
	brokerDetails := &BrokerDetails{
		connectionConfig: newTestConnectionConfig(),
		mgmtClient:       &managementClientStub{stats: stats},
		StreamConnection: connection,
	}
	sourceStats := brokerDetails.getStreamOrQueueStats(&pb.Source{Name: streamName, Type: pb.Source_STREAM})
	assert.Equal(t, int64(len(messages)-1), sourceStats.GetLastOffset())
}

func Test_amqp10ProviderConnectDoesNotRequireStreamConnection(t *testing.T) {
	ctx, clientIdentifier := newTestProviderContext(t, "connect-without-stream")
	prov := newTestRabbitMQAMQP10Provider()

	connectErr := prov.Connect(ctx, newTestConnectionConfig(), false)
	if connectErr == nil {
		t.Cleanup(func() { prov.Disconnect(ctx) })
	}
	require.Nil(t, connectErr)

	brokerDetails := prov.getBrokerDetailsByIdentifier(clientIdentifier)
	require.NotNil(t, brokerDetails)
	require.NotNil(t, brokerDetails.StreamConnection)
	assert.True(t, brokerDetails.StreamConnection.IsClosed())
}
