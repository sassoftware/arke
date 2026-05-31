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

func TestNotifyHealthRegistersReceiver(t *testing.T) {
	clearHealthNotifiers()
	defer clearHealthNotifiers()

	addr := "client-register"
	rec := make(chan pb.HealthStatus_Code, 1)
	notifyHealth(addr, rec)

	got, ok := healthNotifiers.Get(addr)
	assert.True(t, ok, "receiver should be registered for the client address")
	assert.Equal(t, rec, got)
}

func TestNotifyHealthReplacesAndClosesPriorReceiver(t *testing.T) {
	clearHealthNotifiers()
	defer clearHealthNotifiers()

	addr := "client-replace"
	first := make(chan pb.HealthStatus_Code, 1)
	notifyHealth(addr, first)

	second := make(chan pb.HealthStatus_Code, 1)
	notifyHealth(addr, second)

	// Registering a second notifier for the same client must close the prior one.
	_, open := <-first
	assert.False(t, open, "prior receiver channel should be closed on replacement")

	// ...and leave only the new receiver registered.
	got, ok := healthNotifiers.Get(addr)
	assert.True(t, ok)
	assert.Equal(t, second, got)
}

func TestMonitorHealthChanFansOutToNotifiers(t *testing.T) {
	clearHealthNotifiers()
	defer clearHealthNotifiers()

	notifier := make(chan pb.HealthStatus_Code, 1)
	notifyHealth("client-fanout", notifier)

	receiver := make(chan pb.HealthStatus_Code)
	done := make(chan struct{})
	go func() {
		MonitorHealthChan(receiver)
		close(done)
	}()

	receiver <- pb.HealthStatus_GOAWAY

	select {
	case code := <-notifier:
		assert.Equal(t, pb.HealthStatus_GOAWAY, code)
	case <-time.After(time.Second):
		t.Fatal("notifier did not receive the broadcast health code")
	}

	// Closing the source channel must end the monitor loop.
	close(receiver)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("MonitorHealthChan did not exit after its receiver was closed")
	}
}
