// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"errors"
	"testing"
	"time"

	"github.com/sassoftware/arke/internal/provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type streamEnvironmentStub struct {
	closed      bool
	closeErr    error
	closeCall   int
	connectCall int
	connectFunc func() error
}

func (e *streamEnvironmentStub) Connect() error {
	e.connectCall++
	var err error
	if e.connectFunc != nil {
		err = e.connectFunc()
	}
	if err == nil {
		e.closed = false
	}
	return err
}

func (e *streamEnvironmentStub) Close() error {
	if e.closed {
		return nil
	}
	e.closeCall++
	e.closed = true
	return e.closeErr
}

func (e *streamEnvironmentStub) IsClosed() bool {
	return e.closed
}

type streamConnectionStub struct {
	closed    bool
	closeErr  error
	closeCall int
}

func (c *streamConnectionStub) Connect() error {
	return nil
}

func (c *streamConnectionStub) Close() error {
	if c.closed {
		return nil
	}
	c.closeCall++
	if c.closeErr != nil {
		return c.closeErr
	}
	c.closed = true
	return nil
}

func (c *streamConnectionStub) IsClosed() bool {
	return c.closed
}

func Test_streamConnection_Connect(t *testing.T) {
	t.Run("connects the shared environment", func(t *testing.T) {
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, nil)
		environment := &streamEnvironmentStub{closed: true}
		connection.env = environment

		require.NoError(t, connection.Connect())

		assert.Equal(t, 1, environment.connectCall)
		assert.False(t, connection.IsClosed())
	})

	t.Run("failed environment connection is retryable", func(t *testing.T) {
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, nil)
		environment := &streamEnvironmentStub{closed: true}
		creationErr := errors.New("stream environment creation failed")
		environment.connectFunc = func() error {
			if environment.connectCall == 1 {
				return creationErr
			}
			return nil
		}
		connection.env = environment

		assert.ErrorIs(t, connection.Connect(), creationErr)
		require.NoError(t, connection.Connect())
		assert.Equal(t, 2, environment.connectCall)
	})

	t.Run("does not replace an existing open environment", func(t *testing.T) {
		environment := &streamEnvironmentStub{}
		connection := &streamConnection{env: environment}

		require.NoError(t, connection.Connect())
		assert.Zero(t, environment.connectCall)
	})
}

func Test_streamConnection_Close(t *testing.T) {
	t.Run("is safe before Connect", func(t *testing.T) {
		connection := &streamConnection{}
		require.NoError(t, connection.Close())
	})

	t.Run("repeated close is safe", func(t *testing.T) {
		environment := &streamEnvironmentStub{}
		connection := &streamConnection{env: environment}
		require.NoError(t, connection.Connect())

		require.NoError(t, connection.Close())
		require.NoError(t, connection.Close())

		assert.True(t, connection.IsClosed())
		assert.Equal(t, 1, environment.closeCall)
	})

	t.Run("propagates environment close error", func(t *testing.T) {
		closeErr := errors.New("stream environment close failed")
		environment := &streamEnvironmentStub{closeErr: closeErr}
		connection := &streamConnection{env: environment}

		assert.ErrorIs(t, connection.Close(), closeErr)
		require.NoError(t, connection.Close())
		assert.Equal(t, 1, environment.closeCall)
	})
}

func Test_streamConnection_IsClosed(t *testing.T) {
	t.Run("is closed before Connect", func(t *testing.T) {
		assert.True(t, (&streamConnection{}).IsClosed())
	})

	t.Run("returns environment state", func(t *testing.T) {
		connection := &streamConnection{env: &streamEnvironmentStub{}}
		assert.False(t, connection.IsClosed())

		connection.env.(*streamEnvironmentStub).closed = true
		assert.True(t, connection.IsClosed())
	})
}

func Test_streamConnection_ConnectAfterDisconnect(t *testing.T) {
	t.Run("does not create an environment after terminal disconnect", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.state.Store(provider.DISCONNECTED)
		bd.clientDisconnect.Store(true)
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, bd)
		environment := &streamEnvironmentStub{closed: true}
		connection.env = environment

		require.ErrorContains(t, connection.Connect(), "disconnected")

		assert.Zero(t, environment.connectCall)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.state.Load())
		assert.True(t, connection.IsClosed())
	})

	t.Run("closes a newly created environment if disconnect races with creation", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.state.Store(provider.CONNECTED)
		environment := &streamEnvironmentStub{closed: true}
		started := make(chan struct{})
		release := make(chan struct{})
		environment.connectFunc = func() error {
			close(started)
			<-release
			return nil
		}
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, bd)
		connection.env = environment
		bd.StreamConnection = connection
		connectResult := make(chan error, 1)
		go func() { connectResult <- connection.Connect() }()
		<-started

		disconnectResult := make(chan struct{})
		go func() {
			bd.disconnect()
			close(disconnectResult)
		}()
		assert.Eventually(t, bd.clientDisconnect.Load, time.Second, time.Millisecond)
		close(release)

		assert.ErrorContains(t, <-connectResult, "disconnected")
		select {
		case <-disconnectResult:
		case <-time.After(time.Second):
			t.Fatal("disconnect did not finish after stream setup completed")
		}
		assert.Equal(t, 1, environment.closeCall)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.state.Load())
	})

	t.Run("terminal disconnect from closed state prevents later stream connect", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.state.Store(provider.CLOSED)
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, bd)
		bd.disconnect()

		environment := &streamEnvironmentStub{closed: true}
		connection.env = environment

		require.ErrorContains(t, connection.Connect(), "disconnected")
		assert.Zero(t, environment.connectCall)
		assert.Equal(t, uint32(provider.CLOSED), bd.state.Load())
	})
}
