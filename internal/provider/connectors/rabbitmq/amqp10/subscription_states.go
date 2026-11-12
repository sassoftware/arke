// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	ramqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"github.com/sassoftware/arke/i18n"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/provider/connectors/amqp"
	"github.com/sassoftware/arke/internal/util"
)

const (
	consumerRecoveryDelay       = 100 * time.Millisecond
	consumerRecoveryMaxDelay    = 2 * time.Second
	maxConsumerRecoveryAttempts = 5
	consumerCloseTimeout        = 5 * time.Second
)

// consumerStableDuration is how long a consumer must survive before a later
// failure is treated as a new problem instead of part of an ongoing flap.
// Resetting on NewConsumer success alone would let a consumer that dies on
// every attach retry forever.
var consumerStableDuration = 30 * time.Second

// consumerSession owns a consumer and the worker goroutine receiving from it.
type consumerSession struct {
	consumer      rabbitMQAMQP10ConsumerShim
	queueName     string
	startedAt     time.Time
	incoming      chan ramqp.IDeliveryContext
	receiveErrors chan error
	cancel        context.CancelFunc
	receiveWG     sync.WaitGroup
}

func consumerRecoveryBackoff(attempt int) time.Duration {
	delay := consumerRecoveryDelay
	for i := 1; i < attempt && delay < consumerRecoveryMaxDelay; i++ {
		delay *= 2
	}
	if delay > consumerRecoveryMaxDelay {
		return consumerRecoveryMaxDelay
	}
	return delay
}

func (s *Subscription) configureStateTransitions() {
	s.state.registerTransitionFunc(subscribeStateStarting, s.starting)
	s.state.registerTransitionFunc(subscribeStateRunning, s.running)
	s.state.registerTransitionFunc(subscribeStateRecovering, s.recovering)
	s.state.registerTransitionFunc(subscribeStateStopping, s.stopping)
}

func (s *Subscription) starting() (subscribeState, *subscribeStateTransitionError) {
	if s.bd.ClientDisconnect.Load() {
		return subscribeStateStopped, nil
	}
	// validate subscription configuration
	s.bd.subscriptions.Add(subscriptionKey(s.source), s)
	_, s.consumerGeneration = s.bd.connectionSnapshot()
	return subscribeStateRunning, nil
}

func (s *Subscription) running() (subscribeState, *subscribeStateTransitionError) {
	if next := s.takeEventAfter(s.consumerGeneration); next != subscribeStateNone {
		s.resetRecoveryIfRecovering(next)
		return next, nil
	}
	state, generation := s.bd.connectionSnapshot()
	if state == provider.CLOSED || state == provider.DISCONNECTED {
		return subscribeStateStopping, nil
	}
	if state != provider.CONNECTED || generation > s.consumerGeneration {
		s.resetRecovery()
		return subscribeStateRecovering, nil
	}

	queueName := amqp.SourceName(s.source)
	consumer, next, transitionErr := s.createConsumer(queueName)
	if consumer == nil {
		return next, transitionErr
	}
	session := s.startReceiving(consumer, queueName)
	defer s.closeConsumer(session)
	return s.consume(session)
}

// createConsumer returns a nil consumer together with the next state when one
// cannot be created.
func (s *Subscription) createConsumer(queueName string) (rabbitMQAMQP10ConsumerShim, subscribeState, *subscribeStateTransitionError) {
	linkName := fmt.Sprintf("%s-consumer-%s", s.bd.ClientIdentifier, queueName)
	opts := &ramqp.ConsumerOptions{
		ReceiverLinkName: linkName,
		Id:               linkName,

		// Note that 0 means 256 credits, not unlimited as in amqp091
		InitialCredits: s.source.GetPrefetchCount(),
		SettleStrategy: ramqp.ExplicitSettle,
		SingleActiveConsumerStateChanged: func(consumer *ramqp.Consumer, isActive bool) {
			if s.source.GetSingleActiveConsumer() {
				queue, _ := consumer.GetQueue()
				logger.Debugf("SAC state changed for consumerID %s on queue %s: active=%v", consumer.Id(), queue, isActive)
			}
		},
		OnDeliveryRelease: nil, // TODO: ack ticket
	}
	conn := s.bd.connection()
	if conn == nil {
		// disconnect() clears the connection, so there is nothing to consume from.
		return nil, subscribeStateStopping, nil
	}
	consumer, err := conn.NewConsumer(s.ctx, queueName, opts)
	if err != nil {
		if s.ctx.Err() != nil || s.bd.ClientDisconnect.Load() {
			return nil, subscribeStateStopping, nil
		}
		s.lastRecoveryError = &subscribeStateTransitionError{
			err:              err,
			msg:              "an error occurred while creating the consumer",
			queue:            queueName,
			clientIdentifier: s.bd.ClientIdentifier,
		}
		// TODO: how to handle error => link with name 'xxx' already exists
		return nil, subscribeStateRecovering, nil
	}
	if consumer == nil {
		return nil, subscribeStateStopping, &subscribeStateTransitionError{
			err:              errors.New("connection returned a nil consumer"),
			msg:              "failed to create consumer",
			queue:            queueName,
			clientIdentifier: s.bd.ClientIdentifier,
		}
	}
	return consumer, subscribeStateNone, nil
}

func (s *Subscription) startReceiving(consumer rabbitMQAMQP10ConsumerShim, queueName string) *consumerSession {
	ctx, cancel := context.WithCancel(s.ctx)
	session := &consumerSession{
		consumer:      consumer,
		queueName:     queueName,
		startedAt:     time.Now(),
		incoming:      make(chan ramqp.IDeliveryContext, 1),
		receiveErrors: make(chan error, 1),
		cancel:        cancel,
	}
	session.receiveWG.Add(1)
	go session.receiveLoop(ctx)
	return session
}

func (cs *consumerSession) receiveLoop(ctx context.Context) {
	defer cs.receiveWG.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		delCtx, err := cs.consumer.Receive(ctx)
		if err != nil {
			select {
			case cs.receiveErrors <- err:
			case <-ctx.Done():
			}
			return
		}
		select {
		case cs.incoming <- delCtx:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Subscription) closeConsumer(session *consumerSession) {
	session.cancel()
	session.receiveWG.Wait()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), consumerCloseTimeout)
	defer closeCancel()
	if err := session.consumer.Close(closeCtx); err != nil {
		util.Logger.Warn(i18n.ConsumerCloseError, s.bd.ClientIdentifier, session.queueName, err.Error())
	}
	s.cleanupActiveDeliveries()
}

func (s *Subscription) recoverFromReceiveError(session *consumerSession, err error) subscribeState {
	if s.ctx.Err() != nil || s.bd.ClientDisconnect.Load() {
		return subscribeStateStopping
	}
	util.Logger.Error(i18n.ConsumerReceiveError, s.bd.ClientIdentifier, session.queueName, err.Error())
	if time.Since(session.startedAt) >= consumerStableDuration {
		s.resetRecovery()
	}
	s.lastRecoveryError = &subscribeStateTransitionError{
		err:              err,
		msg:              "an error occurred while receiving a message",
		queue:            session.queueName,
		clientIdentifier: s.bd.ClientIdentifier,
	}
	return subscribeStateRecovering
}

// consume runs until a connection event, receive failure, or cancellation ends
// the consumer, and returns the state to enter next.
func (s *Subscription) consume(session *consumerSession) (subscribeState, *subscribeStateTransitionError) {
	events := s.events()
	for {
		select {
		case <-s.ctx.Done():
			util.Logger.Info("subscription context canceled, stopping subscription")
			return subscribeStateStopping, nil
		case <-events:
			if next := s.takeEventAfter(s.consumerGeneration); next != subscribeStateNone {
				s.resetRecoveryIfRecovering(next)
				return next, nil
			}
		case err := <-session.receiveErrors:
			return s.recoverFromReceiveError(session, err), nil
		case delCtx := <-session.incoming:
			if next, leave := s.deliver(session, delCtx, events); leave {
				return next, nil
			}
		}
	}
}

// deliver forwards one delivery to the subscriber. It reports leave=true with
// the next state when the subscription must stop forwarding.
func (s *Subscription) deliver(session *consumerSession, delCtx ramqp.IDeliveryContext, events <-chan struct{}) (next subscribeState, leave bool) {
	m := delCtxToMessage(delCtx, s.source)
	if m == nil {
		return subscribeStateNone, false
	}
	s.bd.ActiveMessages.Add(m.Uuid, &activeDelivery{subscription: s, delivery: delCtx})
	for {
		select {
		case s.messageCh <- m:
			util.Logger.Debugf("message forwarded to channel: %s", m.Uuid)
			s.bd.consumed.Add(1)
			s.recoveryAttempts = 0
			s.lastRecoveryError = nil
			return subscribeStateNone, false
		case err := <-session.receiveErrors:
			return s.recoverFromReceiveError(session, err), true
		case <-events:
			// A wake-up can outlive its event, so keep forwarding when none is pending.
			if next := s.takeEventAfter(s.consumerGeneration); next != subscribeStateNone {
				s.bd.ActiveMessages.Delete(m.Uuid)
				s.resetRecoveryIfRecovering(next)
				return next, true
			}
		case <-s.ctx.Done():
			util.Logger.Info("subscription context canceled while forwarding message")
			s.bd.ActiveMessages.Delete(m.Uuid)
			return subscribeStateStopping, true
		}
	}
}

func (s *Subscription) resetRecovery() {
	s.recoveryAttempts = 0
	s.lastRecoveryError = nil
}

// resetRecoveryIfRecovering clears the failure count when a connection event,
// rather than a consumer failure, is what sends the subscription to Recovering.
func (s *Subscription) resetRecoveryIfRecovering(next subscribeState) {
	if next == subscribeStateRecovering {
		s.resetRecovery()
	}
}

func (s *Subscription) cleanupActiveDeliveries() {
	for _, uuid := range s.bd.ActiveMessages.GetList() {
		value, ok := s.bd.ActiveMessages.Get(uuid)
		if !ok {
			continue
		}
		delivery, ok := value.(*activeDelivery)
		if ok && delivery.subscription == s {
			s.bd.ActiveMessages.DeleteIfEqual(uuid, delivery)
		}
	}
}

func (s *Subscription) recovering() (subscribeState, *subscribeStateTransitionError) {
	if s.ctx.Err() != nil || s.bd.ClientDisconnect.Load() {
		return subscribeStateStopping, nil
	}
	// A Recovering event is moot while already recovering; only Stopping matters.
	if s.takeEvent() == subscribeStateStopping {
		return subscribeStateStopping, nil
	}
	s.recoveryAttempts++
	if s.recoveryAttempts >= maxConsumerRecoveryAttempts {
		if s.lastRecoveryError != nil {
			return subscribeStateStopping, s.lastRecoveryError
		}
		return subscribeStateStopping, &subscribeStateTransitionError{
			err:              errors.New("consumer recovery attempts exhausted"),
			msg:              "consumer recovery attempts exhausted",
			queue:            amqp.SourceName(s.source),
			clientIdentifier: s.bd.ClientIdentifier,
		}
	}
	timer := time.NewTimer(consumerRecoveryBackoff(s.recoveryAttempts))
	defer timer.Stop()
	events := s.events()
wait:
	for {
		select {
		case <-s.ctx.Done():
			return subscribeStateStopping, nil
		case <-events:
			if s.peekStopping() {
				return subscribeStateStopping, nil
			}
		case <-timer.C:
			break wait
		}
	}
	if s.bd.ClientDisconnect.Load() || s.peekStopping() {
		return subscribeStateStopping, nil
	}
	waitCtx, cancel := context.WithTimeout(s.ctx, provider.CONNECTTIMEOUT*time.Second)
	defer cancel()
	for s.bd.provider != nil && s.bd.provider.WaitForConnect(waitCtx) {
		state, generation := s.bd.connectionSnapshot()
		if state == provider.CONNECTED {
			s.consumerGeneration = generation
			return subscribeStateRunning, nil
		}
		if state == provider.CLOSED || state == provider.DISCONNECTED {
			break
		}
		if waitCtx.Err() != nil || s.bd.ClientDisconnect.Load() || s.peekStopping() {
			break
		}
	}
	if s.ctx.Err() != nil || s.bd.ClientDisconnect.Load() || s.peekStopping() {
		return subscribeStateStopping, nil
	}
	if s.lastRecoveryError != nil {
		return subscribeStateStopping, s.lastRecoveryError
	}
	return subscribeStateStopping, &subscribeStateTransitionError{
		err:              errors.New("broker connection is unavailable"),
		msg:              "failed to recover broker connection",
		queue:            amqp.SourceName(s.source),
		clientIdentifier: s.bd.ClientIdentifier,
	}
}

func (s *Subscription) stopping() (subscribeState, *subscribeStateTransitionError) {
	// Only remove our own entry; a newer subscription may now own this key.
	s.bd.subscriptions.DeleteIfEqual(subscriptionKey(s.source), s)
	return subscribeStateStopped, nil
}
