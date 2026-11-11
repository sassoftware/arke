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
	ctx               context.Context
	source            *pb.Source
	bd                *BrokerDetails
	publisher         *ramqp.Publisher
	state             *subscribeStateMachine
	messageCh         chan<- *pb.Message
	recoveryAttempts  int
	lastRecoveryError *subscribeStateTransitionError

	eventMu      sync.Mutex
	pendingEvent subscribeState
	eventWake    chan struct{}
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
func (s *Subscription) notify(state subscribeState) {
	if state != subscribeStateRecovering && state != subscribeStateStopping {
		return
	}
	s.events()
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if s.pendingEvent != subscribeStateStopping {
		s.pendingEvent = state
	}
	select {
	case s.eventWake <- struct{}{}:
	default:
	}
}

// takeEvent returns and clears the pending event, or subscribeStateNone.
func (s *Subscription) takeEvent() subscribeState {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	state := s.pendingEvent
	s.pendingEvent = subscribeStateNone
	return state
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
