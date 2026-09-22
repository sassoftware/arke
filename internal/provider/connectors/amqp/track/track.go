// Package track stores the entities declared for one broker connection.
package track

import "sync"

// Tracker tracks exchanges, queues, and streams independently.
type Tracker struct {
	exchangesMu sync.RWMutex
	exchanges   map[string]struct{}
	queuesMu    sync.RWMutex
	queues      map[string]struct{}
	streamsMu   sync.RWMutex
	streams     map[string]struct{}
}

// New creates an empty entity tracker.
func New() *Tracker {
	return &Tracker{
		exchanges: make(map[string]struct{}),
		queues:    make(map[string]struct{}),
		streams:   make(map[string]struct{}),
	}
}

// ExchangeExists reports whether name is a known exchange.
func (t *Tracker) ExchangeExists(name string) bool {
	t.exchangesMu.RLock()
	defer t.exchangesMu.RUnlock()
	_, ok := t.exchanges[name]
	return ok
}

// AddExchange records a successfully declared exchange.
func (t *Tracker) AddExchange(name string) {
	t.exchangesMu.Lock()
	defer t.exchangesMu.Unlock()
	t.exchanges[name] = struct{}{}
}

// QueueExists reports whether name is a known queue.
func (t *Tracker) QueueExists(name string) bool {
	t.queuesMu.RLock()
	defer t.queuesMu.RUnlock()
	_, ok := t.queues[name]
	return ok
}

// AddQueue records a successfully declared queue.
func (t *Tracker) AddQueue(name string) {
	t.queuesMu.Lock()
	defer t.queuesMu.Unlock()
	t.queues[name] = struct{}{}
}

// StreamExists reports whether name is a known stream.
func (t *Tracker) StreamExists(name string) bool {
	t.streamsMu.RLock()
	defer t.streamsMu.RUnlock()
	_, ok := t.streams[name]
	return ok
}

// AddStream records a successfully declared stream.
func (t *Tracker) AddStream(name string) {
	t.streamsMu.Lock()
	defer t.streamsMu.Unlock()
	t.streams[name] = struct{}{}
}

// Reset removes all tracked entities.
func (t *Tracker) Reset() {
	t.exchangesMu.Lock()
	t.exchanges = make(map[string]struct{})
	t.exchangesMu.Unlock()

	t.queuesMu.Lock()
	t.queues = make(map[string]struct{})
	t.queuesMu.Unlock()

	t.streamsMu.Lock()
	t.streams = make(map[string]struct{})
	t.streamsMu.Unlock()
}
