// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package amqp10

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This temporary test is package-local because the Stream shim is intentionally
// private and AMQP10 has no public Stream operation yet. Move it to the provider
// integration suite once later Stream operations provide a public path.
// Run with:
//
//	go test -tags=integration ./internal/provider/connectors/rabbitmq/amqp10 \
//	  -run 'Test_(streamConnectionIntegrationLifecycle|amqp10ProviderConnectDoesNotRequireStreamConnection)' \
//	  -count=1 -v
func Test_streamConnectionIntegrationLifecycle(t *testing.T) {
	connection := newStreamConnection(getStreamConnectionString(newTestConnectionConfig(), false), nil)
	assert.True(t, connection.IsClosed())

	require.NoError(t, connection.Connect())
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close AMQP 1.0 Stream connection: %v", err)
		}
	})
	assert.False(t, connection.IsClosed())

	require.NoError(t, connection.Close())
	assert.True(t, connection.IsClosed())
}

func Test_amqp10ProviderConnectDoesNotRequireStreamConnection(t *testing.T) {
	ctx, clientIdentifier := newTestProviderContext(t, "connect-without-stream")
	prov := newTestRabbitMQAMQP10Provider()
	originalFactory := newStreamEnvironmentFunc
	t.Cleanup(func() { newStreamEnvironmentFunc = originalFactory })
	streamFactoryCalls := 0
	newStreamEnvironmentFunc = func(string, *tls.Config) (streamEnvironmentShim, error) {
		streamFactoryCalls++
		return nil, assert.AnError
	}

	connectErr := prov.Connect(ctx, newTestConnectionConfig(), false)
	if connectErr == nil {
		t.Cleanup(func() { prov.Disconnect(ctx) })
	}
	require.Nil(t, connectErr)

	brokerDetails := prov.getBrokerDetailsByIdentifier(clientIdentifier)
	require.NotNil(t, brokerDetails)
	require.NotNil(t, brokerDetails.StreamConnection)
	assert.True(t, brokerDetails.StreamConnection.IsClosed())
	assert.Zero(t, streamFactoryCalls)
}
