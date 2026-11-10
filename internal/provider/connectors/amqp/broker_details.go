// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/provider/connectors/amqp/track"
	"github.com/sassoftware/arke/internal/util"
)

// CommonBrokerDetails contains connection state shared by the RabbitMQ AMQP
// providers. Its exported fields keep direct access available after embedding
// in the provider packages. Provider-specific connection lifecycle state
// remains with each provider's BrokerDetails.
type CommonBrokerDetails struct {
	sync.Mutex

	ClientIdentifier string
	KnownEntities    *track.EntityTracker
	ActiveMessages   *util.ConcurrentMap
	State            atomic.Uint32
	ConnectionConfig *pb.ConnectionConfiguration
	ActiveStreams    atomic.Int64
	ClientDisconnect atomic.Bool
	LastPubSubEvent  time.Time

	lastPubSubEventMutex sync.Mutex
}

// EntityTracker returns the per-connection entity tracker, initializing it on
// first use.
func (bd *CommonBrokerDetails) EntityTracker() *track.EntityTracker {
	bd.Lock()
	defer bd.Unlock()
	if bd.KnownEntities == nil {
		bd.KnownEntities = track.New()
	}
	return bd.KnownEntities
}

// UpdateLastPubSubEvent records the time of the latest stream activity.
func (bd *CommonBrokerDetails) UpdateLastPubSubEvent() {
	bd.lastPubSubEventMutex.Lock()
	defer bd.lastPubSubEventMutex.Unlock()
	bd.LastPubSubEvent = time.Now()
}

// LastPubSubEventTime returns the time of the latest stream activity.
func (bd *CommonBrokerDetails) LastPubSubEventTime() time.Time {
	bd.lastPubSubEventMutex.Lock()
	defer bd.lastPubSubEventMutex.Unlock()
	return bd.LastPubSubEvent
}

// IncrementStreamCount records an active stream and refreshes the activity time.
func (bd *CommonBrokerDetails) IncrementStreamCount() {
	bd.ActiveStreams.Add(1)
	bd.UpdateLastPubSubEvent()
}

// DecrementStreamCount records a closed stream and refreshes the activity time.
func (bd *CommonBrokerDetails) DecrementStreamCount() {
	bd.ActiveStreams.Add(-1)
	bd.UpdateLastPubSubEvent()
}

// WaitWhileConnecting polls until the connection leaves CONNECTING or the
// existing 30-second wait period expires.
func (bd *CommonBrokerDetails) WaitWhileConnecting() uint32 {
	for start := time.Now(); time.Since(start) < 30*time.Second; {
		switch bd.State.Load() {
		case provider.CONNECTED:
			return provider.CONNECTED
		case provider.CONNECTING:
			time.Sleep(100 * time.Millisecond)
		case provider.CLOSED:
			return provider.CLOSED
		case provider.DISCONNECTED:
			return provider.DISCONNECTED
		}
	}
	return provider.DISCONNECTED
}
