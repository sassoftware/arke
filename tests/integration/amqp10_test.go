// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	amqp10 "github.com/sassoftware/arke/internal/provider/connectors/amqp10"
	"github.com/sassoftware/arke/internal/util"
	cfg "github.com/sassoftware/arke/test/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"
)

type amqp10IntegrationAddr struct {
	addr string
}

func (a amqp10IntegrationAddr) Network() string {
	return "tcp"
}

func (a amqp10IntegrationAddr) String() string {
	return a.addr
}

func Test_AMQP10ProviderConnectsToLiveRabbitMQ(t *testing.T) {
	hostname := os.Getenv("ARKE_BROKER_HOSTNAME")
	if hostname == "" {
		hostname = "localhost"
	}
	t.Setenv("ARKE_BROKER_HOSTNAME", hostname)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx = peer.NewContext(ctx, &peer.Peer{Addr: amqp10IntegrationAddr{addr: fmt.Sprintf("amqp10-connect-integration-%d", time.Now().UnixNano())}})
	clientIdentifier, err := util.SetClientIdentifier(ctx, "amqp10-connect-integration")
	require.NoError(t, err)
	defer util.RemoveClientIdentifier(ctx)

	connConfig := cfg.ConnectionConfigurationFromEnv()
	connConfig.Provider = "amqp10"
	connConfig.ClientName = clientIdentifier

	prov := amqp10.NewAMQP10Provider()

	connectErr := prov.Connect(ctx, &connConfig, false)
	require.Nil(t, connectErr, "provider connect failed: %v", connectErr)
	assert.True(t, prov.ClientExists(clientIdentifier))

	prov.Disconnect(ctx)
	assert.False(t, prov.ClientExists(clientIdentifier))
}

// compile-time check to ensure that amqp10IntegrationAddr implements the
// net.Addr interface.
var _ net.Addr = amqp10IntegrationAddr{}
