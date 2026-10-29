// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"sync"
	"testing"
	"time"

	"github.com/sassoftware/arke/internal/provider"
	"github.com/stretchr/testify/assert"
)

func TestCommonBrokerDetailsEntityTracker(t *testing.T) {
	var details CommonBrokerDetails

	tracker := details.EntityTracker()
	assert.NotNil(t, tracker)
	assert.Same(t, tracker, details.EntityTracker())

	tracker.AddExchange("exchange")
	assert.True(t, details.EntityTracker().ExchangeExists("exchange"))
}

func TestCommonBrokerDetailsStreamCountUpdatesActivity(t *testing.T) {
	var details CommonBrokerDetails
	initialActivity := time.Now().Add(-time.Minute)
	details.LastPubSubEvent = initialActivity

	details.IncrementStreamCount()
	assert.Equal(t, int64(1), details.ActiveStreams)
	incrementedAt := details.LastPubSubEventTime()
	assert.True(t, incrementedAt.After(initialActivity))

	details.DecrementStreamCount()
	assert.Equal(t, int64(0), details.ActiveStreams)
	assert.False(t, details.LastPubSubEventTime().Before(incrementedAt))
}

func TestCommonBrokerDetailsPubSubEventConcurrentAccess(t *testing.T) {
	var details CommonBrokerDetails
	var waitGroup sync.WaitGroup
	for range 4 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for range 100 {
				details.UpdateLastPubSubEvent()
				_ = details.LastPubSubEventTime()
			}
		}()
	}
	waitGroup.Wait()

	assert.False(t, details.LastPubSubEventTime().IsZero())
}

func TestCommonBrokerDetailsWaitWhileConnecting(t *testing.T) {
	t.Run("returns terminal states", func(t *testing.T) {
		var details CommonBrokerDetails
		for _, state := range []uint32{provider.CONNECTED, provider.CLOSED, provider.DISCONNECTED} {
			details.State.Store(state)
			assert.Equal(t, state, details.WaitWhileConnecting())
		}
	})

	t.Run("waits for connection state change", func(t *testing.T) {
		var details CommonBrokerDetails
		details.State.Store(provider.CONNECTING)
		result := make(chan uint32, 1)
		go func() {
			result <- details.WaitWhileConnecting()
		}()

		time.Sleep(150 * time.Millisecond)
		details.State.Store(provider.CONNECTED)

		select {
		case state := <-result:
			assert.Equal(t, uint32(provider.CONNECTED), state)
		case <-time.After(time.Second):
			t.Fatal("WaitWhileConnecting did not return after the state changed")
		}
	})
}
