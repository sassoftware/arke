// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"
	"sync"

	ramqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
)

type Subscription struct {
	ctx                context.Context
	source             *pb.Source
	bd                 *BrokerDetails
	publisher          *ramqp.Publisher
	state              *subscribeStateMachine
	messageCh          chan<- *pb.Message
	recoveryAttempts   int
	lastRecoveryError  *subscribeStateTransitionError
	consumerGeneration uint64

	eventMu           sync.Mutex
	pendingEvent      subscribeState
	pendingGeneration uint64
	eventWake         chan struct{}
}

// events returns the channel signalled when a connection event is pending.
func (s *Subscription) events() <-chan struct{} {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if s.eventWake == nil {
		s.eventWake = make(chan struct{}, 1)
	}
	return s.eventWake
}

// notify hands a connection-driven state request (Recovering or Stopping) to the
// subscription without blocking. The transition mutex is held while running()
// blocks, so events cannot go through transitionState. Stopping is never
// downgraded by a later Recovering request.
func (s *Subscription) notify(state subscribeState, generation uint64) {
	if state != subscribeStateRecovering && state != subscribeStateStopping {
		return
	}
	s.events()
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if s.pendingEvent != subscribeStateStopping && (state == subscribeStateStopping ||
		s.pendingEvent != subscribeStateRecovering ||
		generation > s.pendingGeneration) {
		s.pendingEvent = state
		s.pendingGeneration = generation
	}
	select {
	case s.eventWake <- struct{}{}:
	default:
	}
}

// takeEvent consumes the pending event without filtering by generation. Recovery
// uses it to honor Stopping while discarding redundant Recovering requests.
func (s *Subscription) takeEvent() subscribeState {
	state, _ := s.takePendingEvent()
	return state
}

// takeEventAfter consumes an event, ignoring Recovering requests covered by the
// consumer's generation. Newer reconnects and all Stopping requests must be handled.
func (s *Subscription) takeEventAfter(generation uint64) subscribeState {
	state, eventGeneration := s.takePendingEvent()
	if state == subscribeStateRecovering && eventGeneration <= generation {
		return subscribeStateNone
	}
	return state
}

// takePendingEvent atomically returns and clears the event and its generation,
// so a concurrent notification cannot be lost or paired with the wrong generation.
// A buffered wake-up may remain after the event is consumed.
func (s *Subscription) takePendingEvent() (subscribeState, uint64) {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	state := s.pendingEvent
	generation := s.pendingGeneration
	s.pendingEvent = subscribeStateNone
	s.pendingGeneration = 0
	return state, generation
}

// activeDelivery lets consumer teardown remove only handles owned by one subscription.
type activeDelivery struct {
	subscription *Subscription
	delivery     ramqp.IDeliveryContext
}

// peekStopping reports whether a Stopping request is pending, leaving it queued.
func (s *Subscription) peekStopping() bool {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	return s.pendingEvent == subscribeStateStopping
}
