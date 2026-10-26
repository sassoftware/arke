// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"crypto/tls"
	"errors"
	"testing"

	pb "github.com/sassoftware/arke/api"
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

func Test_getStreamConnectionString(t *testing.T) {
	tests := []struct {
		name       string
		config     *pb.ConnectionConfiguration
		tlsEnabled bool
		want       string
	}{
		{
			name:       "non TLS uses default tenant and stream port",
			config:     newTestConnectionConfig(),
			want:       "rabbitmq-stream://guest:guest@localhost:5552/%2F",
			tlsEnabled: false,
		},
		{
			name:       "TLS uses stream TLS scheme",
			config:     newTestConnectionConfig(),
			want:       "rabbitmq-stream+tls://guest:guest@localhost:5552/%2F",
			tlsEnabled: true,
		},
		{
			name: "credentials and tenant are URL escaped",
			config: &pb.ConnectionConfiguration{
				Host: "rabbit.example",
				Credentials: &pb.Credentials{
					Username: "user name@example",
					Password: "p+?ss/w",
				},
				Tenant: "tenant/a b",
			},
			want: "rabbitmq-stream://user+name%40example:p%2B%3Fss%2Fw@rabbit.example:5552/tenant%2Fa+b",
		},
		{
			name: "empty tenant defaults to slash",
			config: &pb.ConnectionConfiguration{
				Host:        "rabbit.example",
				Credentials: &pb.Credentials{Username: "user", Password: "secret"},
			},
			want: "rabbitmq-stream://user:secret@rabbit.example:5552/%2F",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, getStreamConnectionString(tt.config, tt.tlsEnabled))
		})
	}
}

func Test_streamConnection_Connect(t *testing.T) {
	t.Run("creates and stores environment with configured URI and TLS", func(t *testing.T) {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		connection := newStreamConnection("rabbitmq-stream+tls://guest:guest@localhost:5552/%2F", tlsConfig)
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
	})

	t.Run("failed creation is retryable", func(t *testing.T) {
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil)
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
		connection := &streamConnection{env: environment}

		require.NoError(t, connection.Close())
		require.NoError(t, connection.Close())

		assert.Equal(t, 1, environment.closeCall)
	})

	t.Run("propagates environment close error", func(t *testing.T) {
		closeErr := errors.New("stream environment close failed")
		environment := &streamEnvironmentStub{closeErr: closeErr}
		connection := &streamConnection{env: environment}

		assert.ErrorIs(t, connection.Close(), closeErr)
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
