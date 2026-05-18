// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"sync"
)

// ConcurrentMap A map[string]interface{} with a mutex to prevent concurrent access errors
type ConcurrentMap struct {
	sync.RWMutex
	items map[string]interface{}
}

// NewConcurrentMap Creates a new ConcurrentMap
func NewConcurrentMap() *ConcurrentMap {
	return &ConcurrentMap{
		items: make(map[string]interface{}),
	}
}

// Add Add a key/value to the map
func (cm *ConcurrentMap) Add(key string, item interface{}) {
	cm.Lock()
	defer cm.Unlock()
	cm.items[key] = item
}

// Delete Delete a key from the map
func (cm *ConcurrentMap) Delete(key string) {
	cm.Lock()
	defer cm.Unlock()
	delete(cm.items, key)
}

// Get Return the value for a given key in the map
func (cm *ConcurrentMap) Get(key string) (interface{}, bool) {
	cm.RLock()
	defer cm.RUnlock()
	if val, ok := cm.items[key]; ok {
		return val, true
	}
	return nil, false
}

// GetList Return a list of all keys in the map
func (cm *ConcurrentMap) GetList() []string {
	cm.RLock()
	defer cm.RUnlock()
	var items []string
	for k := range cm.items {
		items = append(items, k)
	}
	return items
}

// Length Return the length of the map
func (cm *ConcurrentMap) Length() int {
	cm.RLock()
	defer cm.RUnlock()
	return len(cm.items)
}
