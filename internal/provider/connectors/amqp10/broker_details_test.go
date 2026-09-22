package amqp10

import (
	"context"
	"errors"
	"testing"
	"time"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type amqp10ConnectionMock struct {
	closeCalled bool
	closeCount  int
	closeCtx    context.Context
	closeErr    error
}

func (m *amqp10ConnectionMock) Close(ctx context.Context) error {
	m.closeCalled = true
	m.closeCount++
	m.closeCtx = ctx
	return m.closeErr
}

func (m *amqp10ConnectionMock) WatchConnection(_ chan *rabbitmqamqp.StateChanged) {
	// Mock implementation does nothing
}

func (m *amqp10ConnectionMock) IsClosed() bool {
	return false
}

func (m *amqp10ConnectionMock) State() int {
	return 0
}

func newTestBrokerDetails() *BrokerDetails {
	return &BrokerDetails{
		ctx:              context.Background(),
		ClientIdentifier: "test-client",
		connectionConfig: &pb.ConnectionConfiguration{
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

func Test_BrokerDetails_disconnect(t *testing.T) {
	t.Run("connected broker details disconnects lifecycle", func(t *testing.T) {
		pubCtx, pubCancel := context.WithCancel(context.Background())
		conn := &amqp10ConnectionMock{}
		bd := newTestBrokerDetails()
		bd.Connection = conn
		bd.pubChannelCtx = pubCtx
		bd.pubChannelCancel = pubCancel
		bd.state.Store(provider.CONNECTED)

		bd.disconnect()

		assert.True(t, conn.closeCalled)
		assert.Equal(t, 1, conn.closeCount)
		assert.Nil(t, bd.Connection)
		assert.Equal(t, provider.DISCONNECTED, bd.state.Load())
		assert.True(t, bd.clientDisconnect.Load())
		assert.ErrorIs(t, pubCtx.Err(), context.Canceled)
	})

	t.Run("nil connection still disconnects lifecycle", func(t *testing.T) {
		pubCtx, pubCancel := context.WithCancel(context.Background())
		bd := newTestBrokerDetails()
		bd.pubChannelCtx = pubCtx
		bd.pubChannelCancel = pubCancel
		bd.state.Store(provider.CONNECTED)

		require.NotPanics(t, bd.disconnect)

		assert.Nil(t, bd.Connection)
		assert.Equal(t, provider.DISCONNECTED, bd.state.Load())
		assert.True(t, bd.clientDisconnect.Load())
		assert.ErrorIs(t, pubCtx.Err(), context.Canceled)
	})

	t.Run("disconnect is idempotent", func(t *testing.T) {
		conn := &amqp10ConnectionMock{}
		bd := newTestBrokerDetails()
		bd.Connection = conn
		bd.state.Store(provider.CONNECTED)

		bd.disconnect()
		bd.disconnect()

		assert.Equal(t, 1, conn.closeCount)
		assert.Nil(t, bd.Connection)
		assert.Equal(t, provider.DISCONNECTED, bd.state.Load())
	})

	t.Run("close error still disconnects lifecycle", func(t *testing.T) {
		conn := &amqp10ConnectionMock{closeErr: errors.New("close failed")}
		bd := newTestBrokerDetails()
		bd.Connection = conn
		bd.state.Store(provider.CONNECTED)

		bd.disconnect()

		assert.True(t, conn.closeCalled)
		assert.Nil(t, bd.Connection)
		assert.Equal(t, provider.DISCONNECTED, bd.state.Load())
		assert.True(t, bd.clientDisconnect.Load())
	})

	t.Run("already disconnected or closed broker details are noops", func(t *testing.T) {
		states := []uint32{provider.DISCONNECTED, provider.CLOSED}
		for _, state := range states {
			conn := &amqp10ConnectionMock{}
			bd := newTestBrokerDetails()
			bd.Connection = conn
			bd.state.Store(state)

			bd.disconnect()

			assert.Equal(t, 0, conn.closeCount)
			assert.Same(t, conn, bd.Connection)
			assert.Equal(t, state, bd.state.Load())
			assert.False(t, bd.clientDisconnect.Load())
		}
	})
}

func Test_BrokerDetails_waitWhileConnecting(t *testing.T) {
	t.Run("returns immediately for terminal states", func(t *testing.T) {
		states := []uint32{provider.CONNECTED, provider.CLOSED, provider.DISCONNECTED}
		for _, state := range states {
			bd := newTestBrokerDetails()
			bd.state.Store(state)

			assert.Equal(t, state, bd.waitWhileConnecting())
		}
	})

	t.Run("waits for a connecting broker to become connected", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.state.Store(provider.CONNECTING)
		result := make(chan int, 1)

		go func() {
			result <- bd.waitWhileConnecting()
		}()

		time.Sleep(150 * time.Millisecond)
		bd.state.Store(provider.CONNECTED)

		select {
		case got := <-result:
			assert.Equal(t, provider.CONNECTED, got)
		case <-time.After(time.Second):
			t.Fatal("waitWhileConnecting did not return after connection state changed")
		}
	})

	t.Run("waits for a connecting broker to close", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.state.Store(provider.CONNECTING)
		result := make(chan int, 1)

		go func() {
			result <- bd.waitWhileConnecting()
		}()

		time.Sleep(150 * time.Millisecond)
		bd.state.Store(provider.CLOSED)

		select {
		case got := <-result:
			assert.Equal(t, provider.CLOSED, got)
		case <-time.After(time.Second):
			t.Fatal("waitWhileConnecting did not return after connection closed")
		}
	})
}
