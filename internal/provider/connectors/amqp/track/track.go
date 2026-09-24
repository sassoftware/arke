// Package track stores the entities declared for one broker connection.
package track

import "github.com/sassoftware/arke/internal/util"

// Tracker tracks exchanges, queues, and streams independently.
type Tracker struct {
	exchanges *util.ConcurrentMap
	queues    *util.ConcurrentMap
	streams   *util.ConcurrentMap
}

// New creates an empty entity tracker.
func New() *Tracker {
	return &Tracker{
		exchanges: util.NewConcurrentMap(),
		queues:    util.NewConcurrentMap(),
		streams:   util.NewConcurrentMap(),
	}
}

// ExchangeExists reports whether name is a known exchange.
func (t *Tracker) ExchangeExists(name string) bool {
	_, ok := t.exchanges.Get(name)
	return ok
}

// AddExchange records a successfully declared exchange.
func (t *Tracker) AddExchange(name string) {
	t.exchanges.Add(name, struct{}{})
}

// QueueExists reports whether name is a known queue.
func (t *Tracker) QueueExists(name string) bool {
	_, ok := t.queues.Get(name)
	return ok
}

// AddQueue records a successfully declared queue.
func (t *Tracker) AddQueue(name string) {
	t.queues.Add(name, struct{}{})
}

// StreamExists reports whether name is a known stream.
func (t *Tracker) StreamExists(name string) bool {
	_, ok := t.streams.Get(name)
	return ok
}

// AddStream records a successfully declared stream.
func (t *Tracker) AddStream(name string) {
	t.streams.Add(name, struct{}{})
}

// reset removes all tracked entities.
func (t *Tracker) reset() {
	// ConcurrentMap reset is package-private, so tracker-owned maps are cleared
	// through the public map operations.
	resetMap(t.exchanges)
	resetMap(t.queues)
	resetMap(t.streams)
}

func resetMap(entityMap *util.ConcurrentMap) {
	for _, key := range entityMap.GetList() {
		entityMap.Delete(key)
	}
}
