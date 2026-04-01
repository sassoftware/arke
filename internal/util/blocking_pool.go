// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"fmt"
	"sync/atomic"
)

type BlockingPool struct {
	ctx  context.Context
	pool chan any
	New  func() any
	// Validate is called on every item before it is handed to a caller (Get)
	// or accepted back from a caller (Put).  Return true if the item is still
	// healthy, false to retire it and let the pool allocate a fresh one.
	// If Validate is nil all items are considered valid.
	Validate func(any) bool
	count    atomic.Int32
	limit    int32
}

func NewBlockingPool(ctx context.Context, limit int, constructor func() any) *BlockingPool {
	p := make(chan any, limit)
	return &BlockingPool{
		ctx:   ctx,
		pool:  p,
		limit: int32(limit), //nolint:gosec
		New:   constructor}
}

func (p *BlockingPool) Get() any {
	select {
	case e, ok := <-p.pool:
		if !ok {
			return nil
		}
		if p.Validate == nil || p.Validate(e) {
			return e
		}
		// Item is stale; retire it and fall through to the allocation loop.
		p.count.Add(-1)
	default:
		// Pool is empty
	}

	for {
		// Try to create new if under limit
		current := p.count.Load()
		if current < p.limit {
			if p.count.CompareAndSwap(current, current+1) {
				return p.New()
			}
			// retry if failed
			continue
		}

		// When full wait
		select {
		case <-p.ctx.Done():
			return nil
		case e, ok := <-p.pool:
			if !ok {
				return nil
			}
			if p.Validate == nil || p.Validate(e) {
				return e
			}
			// Stale item; retire it and keep waiting so count drops below
			// limit, allowing the next iteration to create a fresh one.
			p.count.Add(-1)
		}
	}
}

func (p *BlockingPool) Put(x any) error {
	if x == nil {
		Logger.Warn("cannot put nil value into pool")
		return fmt.Errorf("cannot put nil value into pool")
	}
	if p.Validate != nil && !p.Validate(x) {
		Logger.Debugf("item failed validation on Put, retiring from pool")
		p.count.Add(-1)
		return fmt.Errorf("item failed validation, retired from pool")
	}
	select {
	case p.pool <- x:
		return nil
	case <-p.ctx.Done():
		Logger.Debugf("Context cancelled, skipping Put operation to pool")
		return p.ctx.Err()
	default:
		Logger.Debugf("pool is full, cannot accept item")
		return fmt.Errorf("pool is full, cannot accept item")
	}
}
