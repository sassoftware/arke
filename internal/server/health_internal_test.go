// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"testing"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
)

// clearHealthNotifiers empties the package-level notifier registry so each test
// starts from a known state and MonitorHealthChan does not fan out to notifiers
// left over from other tests.
func clearHealthNotifiers() {
	for _, addr := range healthNotifiers.GetList() {
		healthNotifiers.Delete(addr)
	}
}

func TestNotifyHealthRegistersAndReplacesReceiver(t *testing.T) {
	clearHealthNotifiers()
	defer clearHealthNotifiers()

	addr := "client-replace"

	first := make(chan pb.HealthStatus_Code, 1)
	notifyHealth(addr, first)

	// A single notifier is registered for the client address.
	got, ok := healthNotifiers.Get(addr)
	assert.True(t, ok, "receiver should be registered for the client address")
	assert.Equal(t, first, got)

	second := make(chan pb.HealthStatus_Code, 1)
	notifyHealth(addr, second)

	// Registering a second notifier for the same client must leave only the
	// new receiver registered, without closing the prior channel: the prior
	// owner (a still-running Check() call) is responsible for closing it.
	select {
	case _, open := <-first:
		assert.True(t, open, "prior receiver channel should not be closed by replacement")
	default:
	}

	got, ok = healthNotifiers.Get(addr)
	assert.True(t, ok)
	assert.Equal(t, second, got)
}

func TestMonitorHealthChanFansOutToNotifiers(t *testing.T) {
	clearHealthNotifiers()
	defer clearHealthNotifiers()

	// Register multiple notifiers so the test exercises the actual fan-out loop
	// over the registry, not just delivery to a single receiver.
	first := make(chan pb.HealthStatus_Code, 1)
	second := make(chan pb.HealthStatus_Code, 1)
	notifyHealth("client-fanout-1", first)
	notifyHealth("client-fanout-2", second)

	receiver := make(chan pb.HealthStatus_Code)
	done := make(chan struct{})
	go func() {
		MonitorHealthChan(receiver)
		close(done)
	}()

	receiver <- pb.HealthStatus_GOAWAY

	for _, tc := range []struct {
		name     string
		notifier chan pb.HealthStatus_Code
	}{
		{"first", first},
		{"second", second},
	} {
		select {
		case code := <-tc.notifier:
			assert.Equal(t, pb.HealthStatus_GOAWAY, code, "%s notifier should receive the broadcast code", tc.name)
		case <-time.After(time.Second):
			t.Fatalf("%s notifier did not receive the broadcast health code", tc.name)
		}
	}

	// Closing the source channel must end the monitor loop.
	close(receiver)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("MonitorHealthChan did not exit after its receiver was closed")
	}
}
