// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"crypto/tls"
	"errors"
	"testing"

	streamclient "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type environmentStub struct {
	closed     bool
	closeErr   error
	closeCalls int
}

func (e *environmentStub) Close() error {
	e.closeCalls++
	e.closed = true
	return e.closeErr
}

func (e *environmentStub) IsClosed() bool                                          { return e.closed }
func (e *environmentStub) DeclareStream(string, *streamclient.StreamOptions) error { return nil }
func (e *environmentStub) QueryOffset(string, string) (int64, error)               { return 0, nil }
func (e *environmentStub) StoreOffset(string, string, int64) error                 { return nil }
func (e *environmentStub) client() *streamclient.Environment                       { return nil }

func replaceEnvironmentFactory(t *testing.T, factory func(environmentConfig) (streamEnvironment, error)) {
	t.Helper()
	original := newStreamEnvironment
	newStreamEnvironment = factory
	t.Cleanup(func() { newStreamEnvironment = original })
}

func TestConnectionLifecycle(t *testing.T) {
	t.Run("starts closed and retries a failed connection", func(t *testing.T) {
		connection := NewConnection("rabbitmq-stream://localhost:5552/%2F", "client", nil)
		assert.True(t, connection.IsClosed())

		wantError := errors.New("environment setup failed")
		environment := &environmentStub{}
		calls := 0
		replaceEnvironmentFactory(t, func(config environmentConfig) (streamEnvironment, error) {
			calls++
			assert.Equal(t, "rabbitmq-stream://localhost:5552/%2F", config.uri)
			assert.Equal(t, 100, config.maxProducersPerClient)
			assert.Equal(t, 10, config.maxConsumersPerClient)
			if calls == 1 {
				return nil, wantError
			}
			return environment, nil
		})

		assert.ErrorIs(t, connection.Connect(), wantError)
		assert.True(t, connection.IsClosed())
		require.NoError(t, connection.Connect())
		assert.False(t, connection.IsClosed())
		assert.Equal(t, 2, calls)
	})

	t.Run("passes TLS configuration through", func(t *testing.T) {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		connection := NewConnection("rabbitmq-stream+tls://localhost:5552/%2F", "client", tlsConfig)
		replaceEnvironmentFactory(t, func(config environmentConfig) (streamEnvironment, error) {
			assert.Same(t, tlsConfig, config.tlsConfig)
			return &environmentStub{}, nil
		})
		require.NoError(t, connection.Connect())
	})

	t.Run("cleans up a partially created environment", func(t *testing.T) {
		connection := NewConnection("rabbitmq-stream://localhost:5552/%2F", "client", nil)
		environment := &environmentStub{}
		wantError := errors.New("partial setup failed")
		replaceEnvironmentFactory(t, func(environmentConfig) (streamEnvironment, error) {
			return environment, wantError
		})

		assert.ErrorIs(t, connection.Connect(), wantError)
		assert.Equal(t, 1, environment.closeCalls)
		assert.True(t, connection.IsClosed())
	})

	t.Run("close is safe before connect and idempotent", func(t *testing.T) {
		connection := NewConnection("rabbitmq-stream://localhost:5552/%2F", "client", nil)
		require.NoError(t, connection.Close())
		require.NoError(t, connection.Close())
		assert.True(t, connection.IsClosed())
		assert.Error(t, connection.Connect())
	})

	t.Run("closes a connected environment once", func(t *testing.T) {
		connection := NewConnection("rabbitmq-stream://localhost:5552/%2F", "client", nil)
		environment := &environmentStub{}
		replaceEnvironmentFactory(t, func(environmentConfig) (streamEnvironment, error) {
			return environment, nil
		})
		require.NoError(t, connection.Connect())

		require.NoError(t, connection.Close())
		require.NoError(t, connection.Close())
		assert.Equal(t, 1, environment.closeCalls)
		assert.True(t, connection.IsClosed())
	})
}
