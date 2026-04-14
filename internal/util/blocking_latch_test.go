// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_BlockingLatch(t *testing.T) {
	latch := NewBlockingLatch(context.Background(), 2)
	require.NoError(t, latch.Increment())
	require.NoError(t, latch.Increment())
	assert.Equal(t, uint(2), latch.Count())
	latch.Decrement()
	assert.Equal(t, uint(1), latch.Count())
	require.NoError(t, latch.Increment())
	assert.Equal(t, uint(2), latch.Count())
	go func(lt *BlockingLatch) {
		time.Sleep(1000 * time.Millisecond)
		lt.Decrement()
	}(latch)
	require.NoError(t, latch.Increment())
	assert.Equal(t, uint(2), latch.Count())

	go func(lt *BlockingLatch) {
		time.Sleep(1000 * time.Millisecond)
		lt.Decrement()
		lt.Decrement()
	}(latch)

	require.NoError(t, latch.WaitForEmpty())
	assert.Equal(t, uint(0), latch.Count())
}

func Test_BlockingLatch_WaitForEmpty_Cancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	latch := NewBlockingLatch(ctx, 2)
	require.NoError(t, latch.Increment())

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := latch.WaitForEmpty()
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, uint(1), latch.Count())
}

func Test_BlockingLatch_Increment_Cancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	latch := NewBlockingLatch(ctx, 1)
	require.NoError(t, latch.Increment())
	assert.Equal(t, uint(1), latch.Count())

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := latch.Increment()
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, uint(1), latch.Count())
}
