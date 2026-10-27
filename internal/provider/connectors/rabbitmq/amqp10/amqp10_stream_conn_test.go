// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/sassoftware/arke/internal/provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type streamEnvironmentStub struct {
	closed    bool
	closeErr  error
	closeCall int
}

func (e *streamEnvironmentStub) Close() error {
	e.closeCall++
	if e.closeErr == nil {
		e.closed = true
	}
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
	t.Run("creates and stores environment with configured URI and TLS", func(t *testing.T) {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		connection := newStreamConnection("rabbitmq-stream+tls://guest:guest@localhost:5552/%2F", tlsConfig, nil)
		environment := &streamEnvironmentStub{}
		originalFactory := newStreamEnvironmentFunc
		t.Cleanup(func() { newStreamEnvironmentFunc = originalFactory })
		calls := 0
		newStreamEnvironmentFunc = func(uri string, gotTLSConfig *tls.Config) (streamEnvironmentShim, error) {
			calls++
			assert.Equal(t, connection.streamURI, uri)
			assert.Same(t, tlsConfig, gotTLSConfig)
			return environment, nil
		}

		require.NoError(t, connection.Connect())

		assert.Equal(t, 1, calls)
		assert.Same(t, environment, connection.env)
		assert.False(t, connection.IsClosed())
	})

	t.Run("failed creation is retryable", func(t *testing.T) {
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, nil)
		environment := &streamEnvironmentStub{}
		originalFactory := newStreamEnvironmentFunc
		t.Cleanup(func() { newStreamEnvironmentFunc = originalFactory })
		calls := 0
		creationErr := errors.New("stream environment creation failed")
		newStreamEnvironmentFunc = func(string, *tls.Config) (streamEnvironmentShim, error) {
			calls++
			if calls == 1 {
				return nil, creationErr
			}
			return environment, nil
		}

		assert.ErrorIs(t, connection.Connect(), creationErr)
		assert.Nil(t, connection.env)
		require.NoError(t, connection.Connect())
		assert.Same(t, environment, connection.env)
		assert.Equal(t, 2, calls)
	})

	t.Run("cleans up a partially created environment", func(t *testing.T) {
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, nil)
		environment := &streamEnvironmentStub{}
		creationErr := errors.New("stream environment setup failed")
		originalFactory := newStreamEnvironmentFunc
		t.Cleanup(func() { newStreamEnvironmentFunc = originalFactory })
		newStreamEnvironmentFunc = func(string, *tls.Config) (streamEnvironmentShim, error) {
			return environment, creationErr
		}

		assert.ErrorIs(t, connection.Connect(), creationErr)
		assert.Nil(t, connection.env)
		assert.Equal(t, 1, environment.closeCall)
	})

	t.Run("does not replace an existing open environment", func(t *testing.T) {
		environment := &streamEnvironmentStub{}
		connection := &streamConnection{env: environment}
		originalFactory := newStreamEnvironmentFunc
		t.Cleanup(func() { newStreamEnvironmentFunc = originalFactory })
		newStreamEnvironmentFunc = func(string, *tls.Config) (streamEnvironmentShim, error) {
			t.Fatal("factory should not be called for an open environment")
			return nil, nil
		}

		require.NoError(t, connection.Connect())
		assert.Same(t, environment, connection.env)
	})
}

func Test_streamConnection_Close(t *testing.T) {
	t.Run("is safe before Connect", func(t *testing.T) {
		connection := &streamConnection{}
		require.NoError(t, connection.Close())
	})

	t.Run("repeated close is safe", func(t *testing.T) {
		environment := &streamEnvironmentStub{}
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, nil)
		originalFactory := newStreamEnvironmentFunc
		t.Cleanup(func() { newStreamEnvironmentFunc = originalFactory })
		newStreamEnvironmentFunc = func(string, *tls.Config) (streamEnvironmentShim, error) {
			return environment, nil
		}
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
		factoryCalls := 0
		originalFactory := newStreamEnvironmentFunc
		t.Cleanup(func() { newStreamEnvironmentFunc = originalFactory })
		newStreamEnvironmentFunc = func(string, *tls.Config) (streamEnvironmentShim, error) {
			factoryCalls++
			return &streamEnvironmentStub{}, nil
		}
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, bd)

		require.ErrorContains(t, connection.Connect(), "disconnected")

		assert.Zero(t, factoryCalls)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.state.Load())
		assert.True(t, connection.IsClosed())
	})

	t.Run("closes a newly created environment if disconnect races with creation", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.state.Store(provider.CONNECTED)
		environment := &streamEnvironmentStub{}
		started := make(chan struct{})
		release := make(chan struct{})
		originalFactory := newStreamEnvironmentFunc
		t.Cleanup(func() { newStreamEnvironmentFunc = originalFactory })
		newStreamEnvironmentFunc = func(string, *tls.Config) (streamEnvironmentShim, error) {
			close(started)
			<-release
			return environment, nil
		}
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, bd)
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
		assert.Nil(t, connection.env)
		assert.Equal(t, 1, environment.closeCall)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.state.Load())
	})

	t.Run("terminal disconnect from closed state prevents later stream connect", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.state.Store(provider.CLOSED)
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, bd)
		bd.disconnect()

		factoryCalls := 0
		originalFactory := newStreamEnvironmentFunc
		t.Cleanup(func() { newStreamEnvironmentFunc = originalFactory })
		newStreamEnvironmentFunc = func(string, *tls.Config) (streamEnvironmentShim, error) {
			factoryCalls++
			return &streamEnvironmentStub{}, nil
		}

		require.ErrorContains(t, connection.Connect(), "disconnected")
		assert.Zero(t, factoryCalls)
		assert.Equal(t, uint32(provider.CLOSED), bd.state.Load())
	})
}
