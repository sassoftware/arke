// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
	cfg "github.com/sassoftware/arke/test/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"
)

func Test_ProviderConnectsToBroker(t *testing.T) {
	hostname := os.Getenv("ARKE_BROKER_HOSTNAME")
	if hostname == "" {
		hostname = "localhost"
	}
	t.Setenv("ARKE_BROKER_HOSTNAME", hostname)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientAddr := fmt.Sprintf("test-connect-integration-%d", time.Now().UnixNano())
	ctx = peer.NewContext(ctx, &peer.Peer{Addr: mockPeerAddr{clientAddr: clientAddr}})

	clientIdentifier, err := util.SetClientIdentifier(ctx, "test-connect-integration")
	require.NoError(t, err)
	defer util.RemoveClientIdentifier(ctx)

	connConfig := cfg.ConnectionConfigurationFromEnv()
	t.Logf("cf: %+v", connConfig)
	connConfig.ClientName = clientIdentifier
	prov, err := provider.NewProvider(cfg.ConnectionConfigurationFromEnv().Provider)
	require.NoError(t, err)

	connectErr := prov.Connect(ctx, &connConfig, false)
	require.Nil(t, connectErr, "provider connect failed: %v", connectErr)
	assert.True(t, prov.ClientExists(clientIdentifier))
	assert.Eventually(t, func() bool {
		return prov.WaitForConnect(ctx)
	}, 2*time.Second, 100*time.Millisecond)
	prov.Disconnect(ctx)
	assert.False(t, prov.ClientExists(clientIdentifier))
}

func Test_AMQP10DeclareExchangeAllowsIncompatibleRedeclaration(t *testing.T) {
	hostname := os.Getenv("ARKE_BROKER_HOSTNAME")
	if hostname == "" {
		hostname = "localhost"
	}
	t.Setenv("ARKE_BROKER_HOSTNAME", hostname)

	baseConfig := cfg.ConnectionConfigurationFromEnv()
	prov, err := provider.NewProvider(baseConfig.Provider)
	require.NoError(t, err)

	connectClient := func(clientName string) (context.Context, context.CancelFunc) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		clientAddr := "test-amqp10-declare-" + uuid.NewString()
		ctx = peer.NewContext(ctx, &peer.Peer{Addr: mockPeerAddr{clientAddr: clientAddr}})
		clientIdentifier, err := util.SetClientIdentifier(ctx, clientName)
		require.NoError(t, err)

		connectionConfig := cfg.ConnectionConfigurationFromEnv()
		connectionConfig.ClientName = clientIdentifier
		require.Nil(t, prov.Connect(ctx, &connectionConfig, false))
		t.Cleanup(func() {
			prov.Disconnect(ctx)
			util.RemoveClientIdentifier(ctx)
			cancel()
		})
		return ctx, cancel
	}

	exchangeName := "arke-amqp10-redeclare-" + uuid.NewString()
	ctxA, cancelA := connectClient("test-amqp10-declare-a")
	ctxB, cancelB := connectClient("test-amqp10-declare-b")

	// There is no public exchange-delete operation on this temporary Subscribe path.
	sourceA := &pb.Source{Address: &pb.Address{Name: exchangeName, Type: pb.Address_TOPIC, AutoDelete: true}}
	require.Nil(t, prov.Subscribe(ctxA, sourceA, nil))

	sourceB := &pb.Source{Address: &pb.Address{Name: exchangeName, Type: pb.Address_TOPIC, AutoDelete: false}}
	require.Nil(t, prov.Subscribe(ctxB, sourceB, nil))
	cancelA()
	cancelB()
}
