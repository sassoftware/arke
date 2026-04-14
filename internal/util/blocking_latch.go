// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"sync"
)

type BlockingLatch struct {
	ctx    context.Context
	count  uint
	max    uint
	lock   *sync.Mutex
	notMax *sync.Cond
}

func NewBlockingLatch(ctx context.Context, m uint) *BlockingLatch {
	lock := new(sync.Mutex)

	bl := &BlockingLatch{
		ctx:    ctx,
		count:  0,
		max:    m,
		lock:   lock,
		notMax: sync.NewCond(lock),
	}
	return bl
}

func (bl *BlockingLatch) WaitForEmpty() error {
	stop := context.AfterFunc(bl.ctx, func() {
		bl.notMax.Broadcast()
	})
	defer stop()

	bl.lock.Lock()
	defer bl.lock.Unlock()
	for {
		if bl.count == 0 {
			return nil
		}
		if err := bl.ctx.Err(); err != nil {
			return err
		}
		bl.notMax.Wait()
	}
}

func (bl *BlockingLatch) Count() uint {
	bl.lock.Lock()
	res := bl.count
	bl.lock.Unlock()

	return res
}

func (bl *BlockingLatch) SetMax(m uint) {
	bl.lock.Lock()
	bl.max = m
	bl.lock.Unlock()
}

func (bl *BlockingLatch) GetMax() uint {
	bl.lock.Lock()
	defer bl.lock.Unlock()
	return bl.max
}

func (bl *BlockingLatch) Decrement() {
	bl.lock.Lock()
	bl.count--
	bl.notMax.Signal()
	bl.lock.Unlock()
}

func (bl *BlockingLatch) Increment() error {
	stop := context.AfterFunc(bl.ctx, func() {
		bl.notMax.Broadcast()
	})
	defer stop()

	bl.lock.Lock()
	defer bl.lock.Unlock()
	for {
		if bl.count < bl.max {
			bl.count++
			return nil
		}
		if err := bl.ctx.Err(); err != nil {
			return err
		}
		bl.notMax.Wait()
	}
}
