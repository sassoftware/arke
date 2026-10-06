// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package amqp10

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/util"
	cfg "github.com/sassoftware/arke/test/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"
)

func TestAMQP10DeclareExchangeIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	clientName := "test-amqp10-declare-" + uuid.NewString()
	ctx = peer.NewContext(ctx, &peer.Peer{Addr: rabbitMQAMQP10ProviderAddr{addr: clientName}})
	clientIdentifier, err := util.SetClientIdentifier(ctx, clientName)
	require.NoError(t, err)
	t.Cleanup(func() { util.RemoveClientIdentifier(ctx) })

	connectionConfig := cfg.ConnectionConfigurationFromEnv()
	connectionConfig.ClientName = clientIdentifier
	prov := NewRabbitMQAMQP10Provider().(*rabbitMQAMQP10Provider)
	require.Nil(t, prov.Connect(ctx, &connectionConfig, false))
	t.Cleanup(func() { prov.Disconnect(ctx) })

	bd, err := prov.getBrokerDetails(ctx)
	require.NoError(t, err)
	exchangeName := "arke-amqp10-declare-" + uuid.NewString()
	address := &pb.Address{Name: exchangeName, Type: pb.Address_TOPIC}
	management := bd.Connection.Management()
	exchangeDeleter, ok := management.(interface {
		DeleteExchange(context.Context, string) error
	})
	require.True(t, ok, "RabbitMQ management connection does not support exchange deletion")

	require.NoError(t, prov.declareExchange(address, bd))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		assert.NoError(t, exchangeDeleter.DeleteExchange(cleanupCtx, exchangeName))
	})
	require.NoError(t, prov.declareExchange(address, bd))
}
