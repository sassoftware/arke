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

type rabbitmqAmqp10ConnectionMock struct {
	closeCalled bool
	closeCount  int
	closeCtx    context.Context
	closeErr    error
}

func (m *rabbitmqAmqp10ConnectionMock) Close(ctx context.Context) error {
	m.closeCalled = true
	m.closeCount++
	m.closeCtx = ctx
	return m.closeErr
}

func (m *rabbitmqAmqp10ConnectionMock) WatchConnection(_ chan *rabbitmqamqp.StateChanged) {
	// Mock implementation does nothing
}

func (m *rabbitmqAmqp10ConnectionMock) IsClosed() bool {
	return false
}

func (m *rabbitmqAmqp10ConnectionMock) State() int {
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
		conn := &rabbitmqAmqp10ConnectionMock{}
		bd := newTestBrokerDetails()
		bd.Connection = conn
		bd.state.Store(provider.CONNECTED)

		bd.disconnect()

		assert.True(t, conn.closeCalled)
		assert.Equal(t, 1, conn.closeCount)
		assert.Nil(t, bd.Connection)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.state.Load())
		assert.True(t, bd.clientDisconnect.Load())
	})

	t.Run("nil connection still disconnects lifecycle", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.state.Store(provider.CONNECTED)

		require.NotPanics(t, bd.disconnect)

		assert.Nil(t, bd.Connection)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.state.Load())
		assert.True(t, bd.clientDisconnect.Load())
	})

	t.Run("disconnect is idempotent", func(t *testing.T) {
		conn := &rabbitmqAmqp10ConnectionMock{}
		bd := newTestBrokerDetails()
		bd.Connection = conn
		bd.state.Store(provider.CONNECTED)

		bd.disconnect()
		bd.disconnect()

		assert.Equal(t, 1, conn.closeCount)
		assert.Nil(t, bd.Connection)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.state.Load())
	})

	t.Run("close error still disconnects lifecycle", func(t *testing.T) {
		conn := &rabbitmqAmqp10ConnectionMock{closeErr: errors.New("close failed")}
		bd := newTestBrokerDetails()
		bd.Connection = conn
		bd.state.Store(provider.CONNECTED)

		bd.disconnect()

		assert.True(t, conn.closeCalled)
		assert.Nil(t, bd.Connection)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.state.Load())
		assert.True(t, bd.clientDisconnect.Load())
	})

	t.Run("already disconnected or closed broker details are noops", func(t *testing.T) {
		states := []uint32{provider.DISCONNECTED, provider.CLOSED}
		for _, state := range states {
			conn := &rabbitmqAmqp10ConnectionMock{}
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

			assert.Equal(t, int(state), bd.waitWhileConnecting())
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

func Test_BrokerDetails_watchConnection(t *testing.T) {
	t.Run("sets state to connected when the broker reports open", func(t *testing.T) {
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1)}

		go bd.watchConnection()
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateClosed{}, To: &rabbitmqamqp.StateOpen{}}
		close(bd.stateChannel)

		assert.Eventually(t, func() bool { return bd.state.Load() == provider.CONNECTED }, time.Second, 10*time.Millisecond)
	})

	t.Run("sets state to connecting when the broker reports reconnecting", func(t *testing.T) {
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1)}

		go bd.watchConnection()
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateClosed{}, To: &rabbitmqamqp.StateReconnecting{}}
		close(bd.stateChannel)

		assert.Eventually(t, func() bool { return bd.state.Load() == provider.CONNECTING }, time.Second, 10*time.Millisecond)
	})

	t.Run("sets state to closed and exits when the broker reports closed", func(t *testing.T) {
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1)}

		go bd.watchConnection()
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateOpen{}, To: &rabbitmqamqp.StateClosed{}}
		close(bd.stateChannel)

		assert.Eventually(t, func() bool { return bd.state.Load() == provider.CLOSED }, time.Second, 10*time.Millisecond)
	})

	t.Run("sets state to closed and exits when the broker reports closing", func(t *testing.T) {
		bd := &BrokerDetails{stateChannel: make(chan *rabbitmqamqp.StateChanged, 1)}

		go bd.watchConnection()
		bd.stateChannel <- &rabbitmqamqp.StateChanged{From: &rabbitmqamqp.StateOpen{}, To: &rabbitmqamqp.StateClosing{}}
		close(bd.stateChannel)

		assert.Eventually(t, func() bool { return bd.state.Load() == provider.CLOSED }, time.Second, 10*time.Millisecond)
	})
}
