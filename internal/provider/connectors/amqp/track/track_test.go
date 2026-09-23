// Copyright (c) 2026, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package track

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrackerTracksEntityTypesIndependently(t *testing.T) {
	tracker := New()

	assert.False(t, tracker.ExchangeExists("shared"))
	assert.False(t, tracker.QueueExists("shared"))
	assert.False(t, tracker.StreamExists("shared"))

	tracker.AddExchange("shared")
	tracker.AddQueue("shared")
	tracker.AddStream("shared")

	assert.True(t, tracker.ExchangeExists("shared"))
	assert.True(t, tracker.QueueExists("shared"))
	assert.True(t, tracker.StreamExists("shared"))
}

func TestTrackerReset(t *testing.T) {
	tracker := New()
	tracker.AddExchange("exchange")
	tracker.AddQueue("queue")
	tracker.AddStream("stream")

	tracker.Reset()

	assert.False(t, tracker.ExchangeExists("exchange"))
	assert.False(t, tracker.QueueExists("queue"))
	assert.False(t, tracker.StreamExists("stream"))
}

func TestTrackerConcurrentAccess(t *testing.T) {
	tracker := New()
	const workers = 8
	const iterations = 500
	var waitGroup sync.WaitGroup
	waitGroup.Add(workers)

	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer waitGroup.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				name := strconv.Itoa(worker) + "-" + strconv.Itoa(iteration)
				tracker.AddExchange(name)
				tracker.AddQueue(name)
				tracker.AddStream(name)
				_ = tracker.ExchangeExists(name)
				_ = tracker.QueueExists(name)
				_ = tracker.StreamExists(name)
			}
		}(worker)
	}

	waitGroup.Wait()
}
