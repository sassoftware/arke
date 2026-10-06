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

	source := &pb.Source{
		Name: "queue-name",
		Address: &pb.Address{
			Name:     "queue-name",
			Subjects: []string{},
			Type:     pb.Address_QUEUE,
		},
	}
	stats := prov.SourceStats(ctx, source)
	assert.Nil(t, stats.Error)
	assert.NotNil(t, stats)
	// Used UI to create the queue queue-name.quorum and publish msgs.
	// Ran this test and observed message_count and publish_rate.
	t.Logf("Source stats: %+v", stats)
	prov.Disconnect(ctx)
	assert.False(t, prov.ClientExists(clientIdentifier))
}
