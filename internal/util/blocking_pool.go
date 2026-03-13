// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"fmt"
	"sync/atomic"
)

type BlockingPool struct {
	ctx   context.Context
	pool  chan any
	New   func() any
	count atomic.Int32
	limit int32
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
		if ok {
			return e
		}
		return nil
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
			if ok {
				return e
			}
			return nil
		}
	}
}

func (p *BlockingPool) Put(x any) error {
	if x == nil {
		Logger.Warn("cannot put nil value into pool")
		return fmt.Errorf("cannot put nil value into pool")
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
