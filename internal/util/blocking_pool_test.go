// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type Obj struct {
	val int
}

func Test_BlockingPool(t *testing.T) {
	pool := NewBlockingPool(context.Background(), 10, func() any { return Obj{val: 1} })
	o := pool.Get().(Obj)
	assert.Equal(t, 1, o.val)
	o.val = 10
	err := pool.Put(o)
	assert.NoError(t, err)
	p := pool.Get().(Obj)
	assert.Equal(t, 10, p.val)
}

func Test_BlockingPoolCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := NewBlockingPool(ctx, 0, func() any { return Obj{val: 1} })
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	o := pool.Get()
	assert.Nil(t, o)
}

func Test_BlockingPoolLimit(t *testing.T) {
	pool := NewBlockingPool(context.Background(), 1, func() any { return Obj{val: 1} })
	o := pool.Get()
	assert.NotNil(t, o)
	go func() {
		time.Sleep(50 * time.Millisecond)
		err := pool.Put(o)
		assert.NoError(t, err)
	}()
	begin := time.Now()
	p := pool.Get()
	diff := time.Since(begin)
	assert.NotNil(t, p)
	assert.GreaterOrEqual(t, diff.Milliseconds(), int64(50))
}

func Test_BlockingPoolConcurrentLimit(t *testing.T) {
	// Test that concurrent Gets don't exceed the limit
	pool := NewBlockingPool(context.Background(), 5, func() any { return &Obj{val: 1} })
	objects := make(chan any, 10)

	// Try to get 10 objects concurrently with limit of 5
	for i := 0; i < 10; i++ {
		go func() {
			objects <- pool.Get()
		}()
	}

	// Get first 5 should succeed immediately
	retrieved := make([]any, 0, 5)
	for i := 0; i < 5; i++ {
		select {
		case obj := <-objects:
			retrieved = append(retrieved, obj)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Should have gotten 5 objects quickly")
		}
	}

	// 6th should block (nothing available in 50ms)
	select {
	case <-objects:
		t.Fatal("Should not get 6th object, pool limit is 5")
	case <-time.After(50 * time.Millisecond):
		// Expected - blocked waiting
	}

	// Return one object
	err := pool.Put(retrieved[0])
	assert.NoError(t, err)

	// Now 6th should succeed
	select {
	case obj := <-objects:
		assert.NotNil(t, obj)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Should get 6th object after returning one")
	}
}

func Test_BlockingPoolReuseFromPool(t *testing.T) {
	// Test that Get() prefers reusing from pool over creating new
	pool := NewBlockingPool(context.Background(), 10, func() any { return &Obj{val: 1} })

	// Get and modify an object
	o := pool.Get().(*Obj)
	o.val = 99

	// Put it back
	err := pool.Put(o)
	assert.NoError(t, err)

	// Next Get should return the same object from pool, not create new
	p := pool.Get().(*Obj)
	assert.Equal(t, 99, p.val, "Should reuse object from pool")
}

func Test_BlockingPoolPutWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := NewBlockingPool(ctx, 1, func() any { return &Obj{val: 1} })

	// Fill the pool to capacity
	o1 := pool.Get().(*Obj)
	err := pool.Put(o1)
	assert.NoError(t, err)

	// Pool is now full, cancel context
	cancel()

	// Try to put another object - should fail with context error
	o2 := &Obj{val: 2}
	err = pool.Put(o2)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func Test_BlockingPoolPutWhenFull(t *testing.T) {
	pool := NewBlockingPool(context.Background(), 2, func() any { return &Obj{val: 1} })

	// Fill the pool completely
	o1 := pool.Get().(*Obj)
	o2 := pool.Get().(*Obj)

	err := pool.Put(o1)
	assert.NoError(t, err)
	err = pool.Put(o2)
	assert.NoError(t, err)

	// Pool is now full, try to put one more
	o3 := &Obj{val: 3}
	err = pool.Put(o3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pool is full")
}

func Test_BlockingPoolPutNil(t *testing.T) {
	pool := NewBlockingPool(context.Background(), 10, func() any { return &Obj{val: 1} })

	// Put nil should return no error and do nothing
	err := pool.Put(nil)
	assert.Error(t, err)
}

func Test_BlockingPoolGetFromClosedChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := NewBlockingPool(ctx, 1, func() any { return &Obj{val: 1} })

	// Get and put an object to populate the pool
	o := pool.Get().(*Obj)
	err := pool.Put(o)
	assert.NoError(t, err)

	// Get it back to empty the pool
	_ = pool.Get()

	// Now close the channel
	close(pool.pool)

	// Get from closed empty pool should return nil
	result := pool.Get()
	assert.Nil(t, result)

	// Cancel context to clean up
	cancel()
}

func Test_BlockingPoolGetFromClosedChannelWhileWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := NewBlockingPool(ctx, 1, func() any { return &Obj{val: 1} })

	// Fill the pool to capacity
	o := pool.Get()
	assert.NotNil(t, o)

	// Now pool is at limit, next Get will block waiting
	result := make(chan any, 1)
	go func() {
		// This will block in the "when full wait" select
		result <- pool.Get()
	}()

	// Give goroutine time to enter the waiting select
	time.Sleep(50 * time.Millisecond)

	// Close the channel while the goroutine is waiting
	close(pool.pool)

	// Should return nil (covers line 54-56)
	select {
	case obj := <-result:
		assert.Nil(t, obj)
	case <-time.After(1 * time.Second):
		t.Fatal("Should have returned nil after channel closed")
	}
}

func Test_BlockingPoolDrain(t *testing.T) {
	// Fill the pool with 3 objects.
	pool := NewBlockingPool(context.Background(), 5, func() any { return &Obj{val: 1} })
	o1 := pool.Get().(*Obj)
	o2 := pool.Get().(*Obj)
	o3 := pool.Get().(*Obj)
	_ = pool.Put(o1)
	_ = pool.Put(o2)
	_ = pool.Put(o3)

	disposed := 0
	pool.Drain(func(item any) {
		disposed++
	})

	// All 3 idle items must have been drained.
	assert.Equal(t, 3, disposed)

	// After draining, the pool channel must be empty.
	select {
	case <-pool.pool:
		t.Fatal("expected pool to be empty after Drain")
	default:
	}

	// count was decremented: new Get() calls must succeed and create fresh
	// objects via the constructor.
	fresh := pool.Get()
	assert.NotNil(t, fresh)
}

func Test_BlockingPoolDrainEmpty(t *testing.T) {
	// Draining an empty pool is a no-op; dispose must not be called.
	pool := NewBlockingPool(context.Background(), 5, func() any { return &Obj{val: 1} })
	called := false
	pool.Drain(func(any) { called = true })
	assert.False(t, called)
}

func Test_BlockingPoolDrainCheckedOutNotAffected(t *testing.T) {
	// An item that has been checked out (not yet Put back) must not be
	// affected by Drain; it must still be usable after the drain.
	pool := NewBlockingPool(context.Background(), 2, func() any { return &Obj{val: 42} })
	checkedOut := pool.Get().(*Obj)

	// Put a second item so the pool has one idle entry.
	idle := pool.Get().(*Obj)
	_ = pool.Put(idle)

	disposed := 0
	pool.Drain(func(any) { disposed++ })
	assert.Equal(t, 1, disposed, "only the idle item should be drained")

	// The checked-out item remains valid.
	assert.Equal(t, 42, checkedOut.val)
}
