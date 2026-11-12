// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"crypto/tls"
	"strings"
	"testing"

	ramqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestConnectionConfig() *pb.ConnectionConfiguration {
	return &pb.ConnectionConfiguration{
		Host:       "localhost",
		Port:       5672,
		ClientName: "test-client",
		Credentials: &pb.Credentials{
			Username: "guest",
			Password: "guest",
		},
	}
}

func Test_getAmqp10ConnOptions(t *testing.T) {
	config := newTestConnectionConfig()
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	clientIdentifier := "test-client"
	ctx, name := newTestProviderContext(t, clientIdentifier)
	assert.True(t, strings.HasPrefix(name, clientIdentifier), "name should have prefix %s", clientIdentifier)
	options, err := getRabbitMQAMQP10ConnOptions(ctx, config, tlsCfg)
	require.NoError(t, err)

	assert.NotNil(t, options.SASLType)
	assert.Same(t, tlsCfg, options.TLSConfig)
	require.NotNil(t, options.RecoveryConfiguration)
	assert.True(t, options.RecoveryConfiguration.ActiveRecovery)
}

func Test_rabbitMQAMQP10Connection_WatchConnectionRejectsNilConnection(t *testing.T) {
	connection := &rabbitMQAMQP10Connection{}

	err := connection.WatchConnection(make(chan *ramqp.StateChanged))

	require.EqualError(t, err, "connection is nil")
}

func Test_providerStateFromLifecycle(t *testing.T) {
	tests := []struct {
		name  string
		state ramqp.ILifeCycleState
		want  int
	}{
		{name: "open", state: &ramqp.StateOpen{}, want: provider.CONNECTED},
		{name: "reconnecting", state: &ramqp.StateReconnecting{}, want: provider.CONNECTING},
		{name: "closing", state: &ramqp.StateClosing{}, want: provider.CLOSED},
		{name: "closed", state: &ramqp.StateClosed{}, want: provider.CLOSED},
		{name: "unknown", state: nil, want: provider.DISCONNECTED},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, providerStateFromLifecycle(tt.state))
		})
	}
}

func Test_rabbitMQAMQP10Connection_IsClosed(t *testing.T) {
	t.Run("nil connection is closed", func(t *testing.T) {
		assert.True(t, (&rabbitMQAMQP10Connection{}).IsClosed())
	})

	t.Run("connection closed by the client stays closed", func(t *testing.T) {
		connection := &rabbitMQAMQP10Connection{}
		connection.closed.Store(true)
		assert.True(t, connection.IsClosed())
	})
}
