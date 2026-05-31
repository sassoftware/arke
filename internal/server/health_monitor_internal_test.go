// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"testing"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
)

// resetHealthNotifiers empties the package-level notifier registry so each test
// starts clean and MonitorHealthChan does not fan out to notifiers left over
// from other tests.
func resetHealthNotifiers() {
	for _, addr := range healthNotifiers.GetList() {
		healthNotifiers.Delete(addr)
	}
}

// TestMonitorHealthChanDoesNotBlockOnBusyReceiver verifies that one client whose
// reader is not ready cannot stall delivery to the others: the busy notifier's
// send is dropped and the ready client still receives the broadcast.
func TestMonitorHealthChanDoesNotBlockOnBusyReceiver(t *testing.T) {
	resetHealthNotifiers()
	defer resetHealthNotifiers()

	busy := make(chan pb.HealthStatus_Code)     // unbuffered, no reader -> never ready
	ready := make(chan pb.HealthStatus_Code, 1) // buffered -> always accepts
	notifyHealth("busy", busy)
	notifyHealth("ready", ready)

	receiver := make(chan pb.HealthStatus_Code)
	done := make(chan struct{})
	go func() {
		MonitorHealthChan(receiver)
		close(done)
	}()

	receiver <- pb.HealthStatus_GOAWAY

	select {
	case code := <-ready:
		assert.Equal(t, pb.HealthStatus_GOAWAY, code)
	case <-time.After(time.Second):
		t.Fatal("broadcast blocked on a busy receiver; ready client never got the code")
	}

	close(receiver)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("MonitorHealthChan did not exit after its receiver was closed")
	}
}

// TestMonitorHealthChanSurvivesClosedNotifier verifies that a notifier closed
// concurrently (client disconnect or re-registration) does not panic the monitor
// goroutine, and that delivery to other clients continues.
func TestMonitorHealthChanSurvivesClosedNotifier(t *testing.T) {
	resetHealthNotifiers()
	defer resetHealthNotifiers()

	closed := make(chan pb.HealthStatus_Code, 1)
	notifyHealth("closed", closed)
	close(closed) // simulate a client whose channel was closed on disconnect/re-register

	live := make(chan pb.HealthStatus_Code, 1)
	notifyHealth("live", live)

	receiver := make(chan pb.HealthStatus_Code)
	done := make(chan struct{})
	go func() {
		MonitorHealthChan(receiver)
		close(done)
	}()

	receiver <- pb.HealthStatus_GOAWAY

	select {
	case code := <-live:
		assert.Equal(t, pb.HealthStatus_GOAWAY, code)
	case <-time.After(time.Second):
		t.Fatal("monitor did not deliver to the live client (panic on the closed notifier?)")
	}

	close(receiver)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("MonitorHealthChan did not exit (panic on the closed notifier?)")
	}
}
