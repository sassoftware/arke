// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type managementClientStub struct {
	vhost     string
	queueName string
	stats     *pb.SourceStats
	request   func(context.Context, string, string) ([]byte, int, error)
	requests  []managementRequest
}

type managementRequest struct {
	method string
	path   string
}

func (m *managementClientStub) Request(ctx context.Context, method, path string) ([]byte, int, error) {
	m.requests = append(m.requests, managementRequest{method: method, path: path})
	if m.request != nil {
		return m.request(ctx, method, path)
	}
	return []byte("[]"), http.StatusOK, nil
}

func (m *managementClientStub) SourceStats(vhost, queueName string) *pb.SourceStats {
	m.vhost = vhost
	m.queueName = queueName
	return m.stats
}

func newTestBrokerDetails() *BrokerDetails {
	return &BrokerDetails{
		ctx:              context.Background(),
		ClientIdentifier: "test-client",
		ConnectionConfig: &pb.ConnectionConfiguration{
			Host:       "localhost",
			Port:       5672,
			ClientName: "test-client",
			Credentials: &pb.Credentials{
				Username: "guest",
				Password: "guest",
			},
		},
	}
}

func Test_BrokerDetails_getStreamOrQueueStats(t *testing.T) {
	tests := []struct {
		name              string
		tenant            string
		source            *pb.Source
		expectedVhost     string
		expectedQueueName string
	}{
		{
			name:   "quorum queue appends suffix and uses configured tenant",
			tenant: "tenant-a",
			source: &pb.Source{
				Name: "orders",
				Type: pb.Source_QUEUE,
			},
			expectedVhost:     "tenant-a",
			expectedQueueName: "orders.quorum",
		},
		{
			name: "stream preserves name and defaults empty tenant",
			source: &pb.Source{
				Name: "events",
				Type: pb.Source_STREAM,
			},
			expectedVhost:     "/",
			expectedQueueName: "events",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bd := newTestBrokerDetails()
			bd.ConnectionConfig.Tenant = tt.tenant
			managementClient := &managementClientStub{stats: &pb.SourceStats{MessageCount: 7}}
			bd.mgmtClient = managementClient

			got := bd.getStreamOrQueueStats(tt.source)

			assert.Same(t, managementClient.stats, got)
			assert.Equal(t, tt.expectedVhost, managementClient.vhost)
			assert.Equal(t, tt.expectedQueueName, managementClient.queueName)
		})
	}
}

func Test_BrokerDetails_disconnect(t *testing.T) {
	t.Run("connected broker details disconnects lifecycle", func(t *testing.T) {
		conn := &rabbitMQAMQP10ConnectionMock{}
		bd := newTestBrokerDetails()
		conn.On("Close", bd.ctx).Return(nil).Once()
		bd.Connection = conn
		bd.State.Store(provider.CONNECTED)

		bd.disconnect()

		conn.AssertExpectations(t)
		assert.Nil(t, bd.Connection)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.State.Load())
		assert.True(t, bd.ClientDisconnect.Load())
	})

	t.Run("nil connection still disconnects lifecycle", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.State.Store(provider.CONNECTED)

		require.NotPanics(t, bd.disconnect)

		assert.Nil(t, bd.Connection)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.State.Load())
		assert.True(t, bd.ClientDisconnect.Load())
	})

	t.Run("disconnect is idempotent", func(t *testing.T) {
		conn := &rabbitMQAMQP10ConnectionMock{}
		bd := newTestBrokerDetails()
		conn.On("Close", bd.ctx).Return(nil).Once()
		bd.Connection = conn
		bd.State.Store(provider.CONNECTED)

		bd.disconnect()
		bd.disconnect()

		conn.AssertExpectations(t)
		assert.Nil(t, bd.Connection)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.State.Load())
	})

	t.Run("close error still disconnects lifecycle", func(t *testing.T) {
		conn := &rabbitMQAMQP10ConnectionMock{}
		bd := newTestBrokerDetails()
		conn.On("Close", bd.ctx).Return(errors.New("close failed")).Once()
		bd.Connection = conn
		bd.State.Store(provider.CONNECTED)

		bd.disconnect()

		conn.AssertExpectations(t)
		assert.Nil(t, bd.Connection)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.State.Load())
		assert.True(t, bd.ClientDisconnect.Load())
	})

	t.Run("already disconnected or closed broker details are noops", func(t *testing.T) {
		states := []uint32{provider.DISCONNECTED, provider.CLOSED}
		for _, state := range states {
			conn := &rabbitMQAMQP10ConnectionMock{}
			bd := newTestBrokerDetails()
			bd.Connection = conn
			bd.State.Store(state)

			bd.disconnect()

			conn.AssertNotCalled(t, "Close", mock.Anything)
			assert.Same(t, conn, bd.Connection)
			assert.Equal(t, state, bd.State.Load())
			assert.False(t, bd.ClientDisconnect.Load())
		}
	})
}

func Test_BrokerDetails_waitWhileConnecting(t *testing.T) {
	t.Run("returns immediately for terminal states", func(t *testing.T) {
		states := []uint32{provider.CONNECTED, provider.CLOSED, provider.DISCONNECTED}
		for _, state := range states {
			bd := newTestBrokerDetails()
			bd.State.Store(state)

			assert.Equal(t, state, bd.WaitWhileConnecting())
		}
	})

	t.Run("waits for a connecting broker to become connected", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.State.Store(provider.CONNECTING)
		result := make(chan uint32, 1)

		go func() {
			result <- bd.WaitWhileConnecting()
		}()

		time.Sleep(150 * time.Millisecond)
		bd.State.Store(provider.CONNECTED)

		select {
		case got := <-result:
			assert.Equal(t, uint32(provider.CONNECTED), got)
		case <-time.After(time.Second):
			t.Fatal("waitWhileConnecting did not return after connection state changed")
		}
	})

	t.Run("waits for a connecting broker to close", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.State.Store(provider.CONNECTING)
		result := make(chan uint32, 1)

		go func() {
			result <- bd.WaitWhileConnecting()
		}()

		time.Sleep(150 * time.Millisecond)
		bd.State.Store(provider.CLOSED)

		select {
		case got := <-result:
			assert.Equal(t, uint32(provider.CLOSED), got)
		case <-time.After(time.Second):
			t.Fatal("waitWhileConnecting did not return after connection closed")
		}
	})
}

func Test_BrokerDetails_watchConnection(t *testing.T) {
	t.Run("sets state to connected when the broker reports open", func(t *testing.T) {
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1)}

		go bd.watchConnection()
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateClosed{}, To: &rabbitmqamqp.StateOpen{}}
		close(bd.stateChannel)

		assert.Eventually(t, func() bool { return bd.State.Load() == provider.CONNECTED }, time.Second, 10*time.Millisecond)
	})

	t.Run("sets state to connecting when the broker reports reconnecting", func(t *testing.T) {
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1)}

		go bd.watchConnection()
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateClosed{}, To: &rabbitmqamqp.StateReconnecting{}}
		close(bd.stateChannel)

		assert.Eventually(t, func() bool { return bd.State.Load() == provider.CONNECTING }, time.Second, 10*time.Millisecond)
	})

	t.Run("sets state to closed and exits when the broker reports closed", func(t *testing.T) {
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1)}

		go bd.watchConnection()
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateOpen{}, To: &rabbitmqamqp.StateClosed{}}
		close(bd.stateChannel)

		assert.Eventually(t, func() bool { return bd.State.Load() == provider.CLOSED }, time.Second, 10*time.Millisecond)
	})

	t.Run("sets state to closed and exits when the broker reports closing", func(t *testing.T) {
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1)}

		go bd.watchConnection()
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateOpen{}, To: &rabbitmqamqp.StateClosing{}}
		close(bd.stateChannel)

		assert.Eventually(t, func() bool { return bd.State.Load() == provider.CLOSED }, time.Second, 10*time.Millisecond)
	})
}

func Test_BrokerDetails_connectInitializesEntityTracker(t *testing.T) {
	bd := newTestBrokerDetails()
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("WatchConnection", mock.Anything).Return(nil).Once()
	env := &rabbitMQAMQP10EnvironmentMock{}
	env.On("NewConnection", bd.ctx).Return(connection, nil).Once()
	bd.Env = env

	ok, err := bd.connect()

	require.True(t, ok)
	require.NoError(t, err)
	assert.NotNil(t, bd.KnownEntities)
	assert.False(t, bd.exchangeExists("exchange"))
	env.AssertExpectations(t)
	connection.AssertExpectations(t)
}
