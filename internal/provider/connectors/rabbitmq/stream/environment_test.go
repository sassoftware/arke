// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"crypto/tls"
	"errors"
	"sync"
	"testing"

	rabbitstream "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type environmentStub struct {
	closed    bool
	closeErr  error
	closeCall int
}

func (e *environmentStub) Close() error {
	if e.closed {
		return nil
	}
	e.closeCall++
	e.closed = true
	return e.closeErr
}

func (e *environmentStub) IsClosed() bool {
	return e.closed
}

func (e *environmentStub) client() *rabbitstream.Environment {
	return nil
}

func replaceEnvironmentFactory(t *testing.T, factory func(environmentConfig) (environmentShim, error)) {
	t.Helper()
	originalFactory := newStreamEnvironment
	newStreamEnvironment = factory
	t.Cleanup(func() { newStreamEnvironment = originalFactory })
}

func TestEnvironment_IsClosedBeforeConnect(t *testing.T) {
	assert.True(t, New("rabbitmq-stream://localhost:5552/%2F", nil).IsClosed())
}

func TestEnvironment_Connect(t *testing.T) {
	t.Run("passes URI, TLS, and producer limits to the environment factory", func(t *testing.T) {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		connection := New("rabbitmq-stream+tls://guest:guest@localhost:5552/%2F", tlsConfig)
		environment := &environmentStub{}
		calls := 0
		replaceEnvironmentFactory(t, func(config environmentConfig) (environmentShim, error) {
			calls++
			assert.Equal(t, "rabbitmq-stream+tls://guest:guest@localhost:5552/%2F", config.uri)
			assert.Same(t, tlsConfig, config.tlsConfig)
			assert.Equal(t, 100, config.maxProducersPerClient)
			assert.Equal(t, 10, config.maxConsumersPerClient)
			return environment, nil
		})

		require.NoError(t, connection.Connect())
		assert.Equal(t, 1, calls)
		assert.False(t, connection.IsClosed())
	})

	t.Run("failed creation is retryable", func(t *testing.T) {
		connection := New("rabbitmq-stream://localhost:5552/%2F", nil)
		environment := &environmentStub{}
		creationErr := errors.New("stream environment creation failed")
		calls := 0
		replaceEnvironmentFactory(t, func(environmentConfig) (environmentShim, error) {
			calls++
			if calls == 1 {
				return nil, creationErr
			}
			return environment, nil
		})

		assert.ErrorIs(t, connection.Connect(), creationErr)
		assert.True(t, connection.IsClosed())
		require.NoError(t, connection.Connect())
		assert.False(t, connection.IsClosed())
		assert.Equal(t, 2, calls)
	})

	t.Run("cleans up a partially created environment", func(t *testing.T) {
		connection := New("rabbitmq-stream://localhost:5552/%2F", nil)
		environment := &environmentStub{}
		creationErr := errors.New("stream environment setup failed")
		replaceEnvironmentFactory(t, func(environmentConfig) (environmentShim, error) {
			return environment, creationErr
		})

		assert.ErrorIs(t, connection.Connect(), creationErr)
		assert.Equal(t, 1, environment.closeCall)
		assert.True(t, environment.closed)
		assert.True(t, connection.IsClosed())
	})

	t.Run("reports a nil environment", func(t *testing.T) {
		connection := New("rabbitmq-stream://localhost:5552/%2F", nil)
		replaceEnvironmentFactory(t, func(environmentConfig) (environmentShim, error) {
			return nil, nil
		})

		assert.ErrorContains(t, connection.Connect(), "nil environment")
	})

	t.Run("does not replace an open environment", func(t *testing.T) {
		connection := New("rabbitmq-stream://localhost:5552/%2F", nil)
		environment := &environmentStub{}
		calls := 0
		replaceEnvironmentFactory(t, func(environmentConfig) (environmentShim, error) {
			calls++
			return environment, nil
		})

		require.NoError(t, connection.Connect())
		require.NoError(t, connection.Connect())
		assert.Equal(t, 1, calls)
	})
}

func TestEnvironment_Close(t *testing.T) {
	t.Run("is safe before Connect", func(t *testing.T) {
		assert.NoError(t, New("rabbitmq-stream://localhost:5552/%2F", nil).Close())
	})

	t.Run("closes the underlying environment at most once", func(t *testing.T) {
		connection := New("rabbitmq-stream://localhost:5552/%2F", nil)
		environment := &environmentStub{}
		replaceEnvironmentFactory(t, func(environmentConfig) (environmentShim, error) {
			return environment, nil
		})
		require.NoError(t, connection.Connect())

		require.NoError(t, connection.Close())
		require.NoError(t, connection.Close())
		assert.True(t, connection.IsClosed())
		assert.Equal(t, 1, environment.closeCall)
	})

	t.Run("close before Connect prevents duplicate closure", func(t *testing.T) {
		connection := New("rabbitmq-stream://localhost:5552/%2F", nil)
		require.NoError(t, connection.Close())
		require.NoError(t, connection.Close())
		assert.True(t, connection.IsClosed())
	})

	t.Run("does not retry a failed physical close", func(t *testing.T) {
		closeErr := errors.New("stream environment close failed")
		environment := &environmentStub{closeErr: closeErr}
		connection := New("rabbitmq-stream://localhost:5552/%2F", nil)
		replaceEnvironmentFactory(t, func(environmentConfig) (environmentShim, error) {
			return environment, nil
		})
		require.NoError(t, connection.Connect())

		assert.ErrorIs(t, connection.Close(), closeErr)
		require.NoError(t, connection.Close())
		assert.True(t, connection.IsClosed())
		assert.Equal(t, 1, environment.closeCall)
	})
}

func TestEnvironment_ConcurrentLifecycle(t *testing.T) {
	connection := New("rabbitmq-stream://localhost:5552/%2F", nil)
	replaceEnvironmentFactory(t, func(environmentConfig) (environmentShim, error) {
		return &environmentStub{}, nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = connection.Connect()
				_ = connection.IsClosed()
				_ = connection.Close()
			}
		}()
	}
	wg.Wait()
}
