// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
)

// fakeHealthCheckStream is a minimal pb.Healthz_CheckServer whose lifetime is
// driven entirely by ctx, so tests can control when Check() unblocks. sendCount
// tracks how many health messages Check() has sent, so tests can verify the
// notifyHealthChan branch does (or doesn't) fire for a given stream.
type fakeHealthCheckStream struct {
	pb.Healthz_CheckServer
	ctx       context.Context
	sendCount atomic.Int32
}

func (f *fakeHealthCheckStream) Context() context.Context { return f.ctx }
func (f *fakeHealthCheckStream) Send(*pb.Health) error {
	f.sendCount.Add(1)
	return nil
}
func (f *fakeHealthCheckStream) Recv() (*pb.Health, error) {
	<-f.ctx.Done()
	return nil, f.ctx.Err()
}

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

// waitForNotifier polls healthNotifiers until addr's registered value differs
// from prior, returning the new value.
func waitForNotifier(t *testing.T, addr string, prior interface{}) interface{} {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if got, ok := healthNotifiers.Get(addr); ok && got != prior {
			return got
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for a new notifier registration for %s", addr)
			return nil
		case <-time.After(time.Millisecond):
		}
	}
}

// TestCheckReconnectDoesNotPanicOrLoseNewRegistration reproduces a client
// reconnecting while the server hasn't yet noticed the old stream is gone:
// two Check() calls register for the same clientAddr, the second replacing
// the first, and the first stream is then torn down.
func TestCheckReconnectDoesNotPanicOrLoseNewRegistration(t *testing.T) {
	clearHealthNotifiers()
	defer clearHealthNotifiers()

	addr := "client-reconnect"
	oldGetClientAddr := GetClientAddr
	GetClientAddr = func(context.Context) (string, error) { return addr, nil }
	defer func() { GetClientAddr = oldGetClientAddr }()

	hlthSrv := &HealthzServer{}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstStream := &fakeHealthCheckStream{ctx: firstCtx}
	firstDone := make(chan struct{})
	var firstPanic interface{}
	go func() {
		defer close(firstDone)
		defer func() { firstPanic = recover() }()
		_ = hlthSrv.Check(firstStream)
	}()

	firstNotifier := waitForNotifier(t, addr, nil)
	// initial connect message
	initialFirstSends := firstStream.sendCount.Load()

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	secondStream := &fakeHealthCheckStream{ctx: secondCtx}
	secondDone := make(chan struct{})
	var secondPanic interface{}
	go func() {
		defer close(secondDone)
		defer func() { secondPanic = recover() }()
		_ = hlthSrv.Check(secondStream)
	}()

	// The second Check() call replaces the registration for the same clientAddr.
	secondNotifier := waitForNotifier(t, addr, firstNotifier)
	initialSecondSends := secondStream.sendCount.Load()

	// Broadcast a code the way MonitorHealthChan would; only the current
	// registrant (second) should receive it and reach the notifyHealthChan
	// send branch, proving the stale (first) channel is neither written to
	// nor spun on.
	secondNotifier.(chan pb.HealthStatus_Code) <- pb.HealthStatus_UNHEALTHY

	assert.Eventually(t, func() bool {
		return secondStream.sendCount.Load() > initialSecondSends
	}, time.Second, time.Millisecond, "second stream should have received the broadcast health message")
	assert.Equal(t, initialFirstSends, firstStream.sendCount.Load(), "stale first stream must not receive or spin on the broadcast")

	// Tearing down the superseded first stream must not panic (double close)
	// or remove the second stream's registration.
	cancelFirst()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first Check() call did not return after its context was canceled")
	}
	assert.Nil(t, firstPanic, "Check() must not panic when a superseded stream ends")

	got, ok := healthNotifiers.Get(addr)
	assert.True(t, ok, "the second stream's registration should remain")
	assert.Equal(t, secondNotifier, got, "the second stream's registration must be untouched")

	cancelSecond()
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second Check() call did not return after its context was canceled")
	}
	assert.Nil(t, secondPanic, "Check() must not panic on normal shutdown")

	_, ok = healthNotifiers.Get(addr)
	assert.False(t, ok, "registration should be removed once the second stream ends")
}
