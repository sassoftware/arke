// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"context"
	"testing"
	"time"

	"github.com/sassoftware/arke/internal/provider"
	"github.com/stretchr/testify/assert"
)

func TestConnectionCleaner_ShutdownOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		connectionCleaner(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// cleaner exited as expected
	case <-time.After(5 * time.Second):
		t.Fatal("connectionCleaner did not exit after context cancellation")
	}
}

func TestConnectionCleaner_CleansInactiveConnections(t *testing.T) {
	cleanInterval = 1 * time.Second // Set a short interval for testing
	// This test would require significant setup to create mock connections and verify they are cleaned up.
	// For now, we will just ensure that the cleaner runs without error for a short period of time.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shutdownChan := make(chan bool)
	go func() {
		<-shutdownChan
	}()

	go connectionCleaner(ctx)
	provy, _ := provider.GetProvider("amqp091")
	prov := provy.(*amqp091provider)
	prov.connections.Add("test-conn", &BrokerDetails{
		ClientIdentifier: "test-conn",
		lastPubSubEvent:  time.Now().Add(-3 * time.Second), // Simulate inactivity
		shutdownChan:     shutdownChan,
		pubChannelCancel: cancel,
	})
	time.Sleep(2 * time.Second) // Allow cleaner to run for a bit
	assert.Equal(t, 0, prov.connections.Length(), "Expected inactive connection to be cleaned up")
}
