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
	return subscribeStateRunning, nil
}

func (s *Subscription) running() (subscribeState, *subscribeStateTransitionError) {
	// Entering running means the connection is usable, so a queued Recovering
	// request is stale. A queued Stopping request still applies.
	if s.takeEvent() == subscribeStateStopping {
		return subscribeStateStopping, nil
	}
	// create the consumer
	queueName := amqp.SourceName(s.source)
	linkName := fmt.Sprintf("%s-consumer-%s", s.bd.ClientIdentifier, queueName)
	opts := &ramqp.ConsumerOptions{
		ReceiverLinkName: linkName,
		Id:               linkName,

		// Note that 0 means 256 credits, not unlimited as in amqp091
		InitialCredits:                   s.source.GetPrefetchCount(),
		SettleStrategy:                   ramqp.ExplicitSettle,
		SingleActiveConsumerStateChanged: nil, // TODO
		ConsumerTimeout:                  0,   // TODO
		OnDeliveryRelease:                nil, // TODO
	}
	conn := s.bd.connection()
	if conn == nil {
		// disconnect() clears the connection, so there is nothing to consume from.
		return subscribeStateStopping, nil
	}
	consumer, err := conn.NewConsumer(s.ctx, queueName, opts)
	if err != nil {
		if s.ctx.Err() != nil || s.bd.ClientDisconnect.Load() {
			return subscribeStateStopping, nil
		}
		s.lastRecoveryError = &subscribeStateTransitionError{
			err:              err,
			msg:              "an error occurred while creating the consumer",
			queue:            queueName,
			clientIdentifier: s.bd.ClientIdentifier,
		}
		// TODO: how to handle error => link with name 'xxx' already exists
		return subscribeStateRecovering, nil
	}
	if consumer == nil {
		return subscribeStateStopping, &subscribeStateTransitionError{
			err:              errors.New("connection returned a nil consumer"),
			msg:              "failed to create consumer",
			queue:            queueName,
			clientIdentifier: s.bd.ClientIdentifier,
		}
	}
	startedAt := time.Now()
	consumerCtx, cancel := context.WithCancel(s.ctx)
	var receiveWG sync.WaitGroup

	cleanup := func() {
		cancel()
		receiveWG.Wait()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), consumerCloseTimeout)
		defer closeCancel()
		if err := consumer.Close(closeCtx); err != nil {
			util.Logger.Warn("error closing consumer", "error", err)
		}
		s.cleanupActiveDeliveries()
	}

	incoming := make(chan ramqp.IDeliveryContext, 1)
	receiveErrors := make(chan error, 1)
	receiveWG.Add(1)
	go func() {
		defer receiveWG.Done()
		for {
			if consumerCtx.Err() != nil {
				return
			}
			delCtx, err := consumer.Receive(consumerCtx)
			if err != nil {
				select {
				case receiveErrors <- err:
				case <-consumerCtx.Done():
				}
				return
			}
			select {
			case incoming <- delCtx:
			case <-consumerCtx.Done():
				return
			}
		}
	}()
	defer cleanup()

	recoverFromReceiveError := func(err error) (subscribeState, *subscribeStateTransitionError) {
		if s.ctx.Err() != nil || s.bd.ClientDisconnect.Load() {
			return subscribeStateStopping, nil
		}
		util.Logger.Error("error received from consumer", "error", err)
		if time.Since(startedAt) >= consumerStableDuration {
			s.resetRecovery()
		}
		s.lastRecoveryError = &subscribeStateTransitionError{
			err:              err,
			msg:              "an error occurred while receiving a message",
			queue:            queueName,
			clientIdentifier: s.bd.ClientIdentifier,
		}
		return subscribeStateRecovering, nil
	}

	events := s.events()
	for {
		select {
		case <-s.ctx.Done():
			util.Logger.Info("subscription context canceled, stopping subscription")
			return subscribeStateStopping, nil
		case <-events:
			if next := s.takeEvent(); next != subscribeStateNone {
				s.resetRecoveryIfRecovering(next)
				return next, nil
			}
		case err := <-receiveErrors:
			return recoverFromReceiveError(err)
		case delCtx := <-incoming:
			message := delCtx.Message()
			if message == nil {
				continue
			}
			m := delCtxToMessage(delCtx, s.source)
			if m == nil {
				continue
			}
			s.bd.ActiveMessages.Add(m.Uuid, &activeDelivery{subscription: s, delivery: delCtx})
		forward:
			for {
				select {
				case s.messageCh <- m:
					util.Logger.Debugf("message forwarded to channel: %s", m.Uuid)
					s.bd.consumed.Add(1)
					s.recoveryAttempts = 0
					s.lastRecoveryError = nil
					break forward
				case err := <-receiveErrors:
					return recoverFromReceiveError(err)
				case <-events:
					// A wake-up can outlive its event, so keep forwarding when none is pending.
					if next := s.takeEvent(); next != subscribeStateNone {
						s.bd.ActiveMessages.Delete(m.Uuid)
						s.resetRecoveryIfRecovering(next)
						return next, nil
					}
				case <-s.ctx.Done():
					util.Logger.Info("subscription context canceled while forwarding message")
					s.bd.ActiveMessages.Delete(m.Uuid)
					return subscribeStateStopping, nil
				}
			}
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
	if s.bd.provider == nil || !s.bd.provider.WaitForConnect(s.ctx) {
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
	return subscribeStateRunning, nil
}

func (s *Subscription) stopping() (subscribeState, *subscribeStateTransitionError) {
	// TODO
	// Only remove our own entry; a newer subscription may now own this key.
	s.bd.subscriptions.DeleteIfEqual(subscriptionKey(s.source), s)
	return subscribeStateStopped, nil
}
