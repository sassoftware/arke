// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"
	"unsafe"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
	"github.com/stretchr/testify/assert"
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

	t.Run("already disconnected or closed broker details without a connection are noops", func(t *testing.T) {
		states := []uint32{provider.DISCONNECTED, provider.CLOSED}
		for _, state := range states {
			bd := newTestBrokerDetails()
			bd.State.Store(state)

			bd.disconnect()

			assert.Equal(t, state, bd.State.Load())
			assert.False(t, bd.ClientDisconnect.Load())
		}
	})

	t.Run("closed broker details still close a reconnected connection", func(t *testing.T) {
		conn := &rabbitMQAMQP10ConnectionMock{}
		bd := newTestBrokerDetails()
		conn.On("Close", bd.ctx).Return(nil).Once()
		bd.Connection = conn
		bd.State.Store(provider.CLOSED)

		bd.disconnect()

		conn.AssertExpectations(t)
		assert.Nil(t, bd.Connection)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.State.Load())
		assert.True(t, bd.ClientDisconnect.Load())
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
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1), subscriptions: util.NewConcurrentMap()}

		go bd.watchConnection()
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateClosed{}, To: &rabbitmqamqp.StateOpen{}}
		close(bd.stateChannel)

		assert.Eventually(t, func() bool { return bd.State.Load() == provider.CONNECTED }, time.Second, 10*time.Millisecond)
	})

	t.Run("sets state to connecting when the broker reports reconnecting", func(t *testing.T) {
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1), subscriptions: util.NewConcurrentMap()}

		go bd.watchConnection()
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateClosed{}, To: &rabbitmqamqp.StateReconnecting{}}
		close(bd.stateChannel)

		assert.Eventually(t, func() bool { return bd.State.Load() == provider.CONNECTING }, time.Second, 10*time.Millisecond)
	})

	t.Run("sets state to closed and exits when the broker reports closed", func(t *testing.T) {
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1), subscriptions: util.NewConcurrentMap()}

		go bd.watchConnection()
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateOpen{}, To: &rabbitmqamqp.StateClosed{}}
		close(bd.stateChannel)

		assert.Eventually(t, func() bool { return bd.State.Load() == provider.CLOSED }, time.Second, 10*time.Millisecond)
	})

	t.Run("sets state to closed and exits when the broker reports closing", func(t *testing.T) {
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1), subscriptions: util.NewConcurrentMap()}

		go bd.watchConnection()
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateOpen{}, To: &rabbitmqamqp.StateClosing{}}
		close(bd.stateChannel)

		assert.Eventually(t, func() bool { return bd.State.Load() == provider.CLOSED }, time.Second, 10*time.Millisecond)
	})

	t.Run("closed with an error recovers and keeps watching", func(t *testing.T) {
		sub := &Subscription{}
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 3), subscriptions: util.NewConcurrentMap()}
		bd.subscriptions.Add("sub", sub)
		bd.setConnectionState(provider.CONNECTED)
		done := make(chan struct{})
		go func() {
			defer close(done)
			bd.watchConnection()
		}()

		dropped := closedWithError(errors.New("connection forced"))
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateOpen{}, To: dropped}
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: dropped, To: &rabbitmqamqp.StateReconnecting{}}
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateReconnecting{}, To: &rabbitmqamqp.StateOpen{}}

		// Closed(err) and Reconnecting are one reconnect cycle, so the generation is 1.
		require.Eventually(t, func() bool {
			state, generation := bd.connectionSnapshot()
			return state == provider.CONNECTED && generation == 1
		}, time.Second, 10*time.Millisecond)
		assert.Equal(t, subscribeStateRecovering, sub.takeEvent())
		select {
		case <-done:
			t.Fatal("watcher exited on a recoverable close")
		default:
		}
		close(bd.stateChannel)
		<-done
	})

	t.Run("closed without recovery stops subscriptions and exits", func(t *testing.T) {
		for name, to := range map[string]*rabbitmqamqp.StateClosed{
			"client close":       {},
			"attempts exhausted": closedWithError(rabbitmqamqp.ErrMaxReconnectAttemptsReached),
		} {
			t.Run(name, func(t *testing.T) {
				sub := &Subscription{}
				bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1), subscriptions: util.NewConcurrentMap()}
				bd.subscriptions.Add("sub", sub)
				bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateOpen{}, To: to}

				bd.watchConnection()

				assert.Equal(t, uint32(provider.CLOSED), bd.State.Load())
				assert.Equal(t, subscribeStateStopping, sub.takeEvent())
			})
		}
	})
}

// closedWithError builds a StateClosed carrying err; the library has no constructor.
func closedWithError(err error) *rabbitmqamqp.StateClosed {
	closed := &rabbitmqamqp.StateClosed{}
	field := reflect.ValueOf(closed).Elem().FieldByName("error")
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(err))
	return closed
}

func Test_BrokerDetails_setConnectionState(t *testing.T) {
	bd := newTestBrokerDetails()

	first := bd.setConnectionState(provider.CONNECTING)
	repeated := bd.setConnectionState(provider.CONNECTING)
	bd.setConnectionState(provider.CONNECTED)
	next := bd.setConnectionState(provider.CONNECTING)

	assert.Equal(t, uint64(1), first)
	assert.Equal(t, first, repeated)
	assert.Equal(t, uint64(2), next)
}

func Test_isRecoverableClose(t *testing.T) {
	assert.False(t, isRecoverableClose(nil))
	assert.False(t, isRecoverableClose(rabbitmqamqp.ErrMaxReconnectAttemptsReached))
	assert.False(t, isRecoverableClose(fmt.Errorf("wrapped: %w", rabbitmqamqp.ErrMaxReconnectAttemptsReached)))
	assert.True(t, isRecoverableClose(errors.New("connection forced")))
}

func Test_BrokerDetails_notifySubscriptions(t *testing.T) {
	first := &Subscription{}
	second := &Subscription{}
	subscriptions := util.NewConcurrentMap()
	subscriptions.Add("first", first)
	subscriptions.Add("second", second)
	subscriptions.Add("not-a-subscription", "ignored")
	subscriptions.Add("nil", nil)
	bd := &BrokerDetails{subscriptions: subscriptions}

	bd.notifySubscriptions(subscribeStateStopping, 0)

	assert.Equal(t, subscribeStateStopping, first.takeEvent())
	assert.Equal(t, subscribeStateStopping, second.takeEvent())
}
