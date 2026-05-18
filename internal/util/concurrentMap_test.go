// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestItem struct {
	name string
}

func TestNewConcurrentMap(t *testing.T) {
	cMap := NewConcurrentMap()
	assert.NotNil(t, cMap)
}

func TestConcurrentMapAdd(t *testing.T) {
	cMap := NewConcurrentMap()
	testItem := TestItem{"test item"}
	testItem2 := TestItem{"test item 2"}
	assert.NotNil(t, cMap)
	cMap.Add("testItem", &testItem)
	cMap.Add("testItem2", testItem2)

	cItem, ok := cMap.Get("testItem")
	assert.True(t, ok)
	cItem2, ok := cMap.Get("testItem2")
	assert.True(t, ok)
	assert.Equal(t, cItem, &testItem)
	assert.Equal(t, cItem2, testItem2)

	assert.Equal(t, 2, cMap.Length())
}

func TestConcurrentMapDelete(t *testing.T) {
	cMap := NewConcurrentMap()
	assert.NotNil(t, cMap)
	testItem := TestItem{"test item"}
	cMap.Add("testItem", testItem)
	cItem, ok := cMap.Get("testItem")
	assert.True(t, ok)
	assert.Equal(t, cItem, testItem)
	cMap.Delete("testItem")
	cItem, ok = cMap.Get("tetstItem")
	assert.Nil(t, cItem)
	assert.False(t, ok)
}

func TestConcurrentMapGetList(t *testing.T) {
	cMap := NewConcurrentMap()
	assert.NotNil(t, cMap)
	testItem := TestItem{"test item"}
	cMap.Add("testItem", testItem)
	cMap.Add("testItem2", testItem)
	assert.Len(t, cMap.GetList(), 2)
}

// TestConcurrentMap_LengthIsRaceFree exercises Length() concurrently with
// Add/Delete under -race. Without the read-lock in Length(), the race detector
// flags the unsynchronized len(cm.items) read against the locked writes.
func TestConcurrentMap_LengthIsRaceFree(t *testing.T) {
	cMap := NewConcurrentMap()
	const workers = 8
	const iters = 500

	var wg sync.WaitGroup
	wg.Add(workers * 2)

	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				key := strconv.Itoa(id*iters + i)
				cMap.Add(key, i)
				cMap.Delete(key)
			}
		}(w)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_ = cMap.Length()
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, 0, cMap.Length())
}
