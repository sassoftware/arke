// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
	s.state.registerTransitionFunc(subscribeStateStopped, s.stop)
}

func (s *Subscription) starting() (subscribeState, *subscribeStateTransitionError) {
	if s.bd.clientDisconnect.Load() {
		return subscribeStateStopped, nil
	}
	// validate subscription configuration
	s.bd.subscriptions.Add(subscriptionKey(s.source), s)
	return subscribeStateRunning, nil
}

func (s *Subscription) running() (subscribeState, *subscribeStateTransitionError) {
	// create the consumer
	queueName := amqp.SourceName(s.source)
	linkName := fmt.Sprintf("%s-consumer-%s", s.bd.ClientIdentifier, queueName)
	opts := &ramqp.ConsumerOptions{
		ReceiverLinkName:                 linkName,
		Id:                               linkName,
		InitialCredits:                   s.source.GetPrefetchCount(),
		SettleStrategy:                   ramqp.ExplicitSettle,
		SingleActiveConsumerStateChanged: nil, // TODO
		ConsumerTimeout:                  0,   // TODO
		OnDeliveryRelease:                nil, // TODO
	}
	consumer, err := s.bd.Connection.NewConsumer(s.ctx, queueName, opts)
	if err != nil {
		if s.ctx.Err() != nil || s.bd.clientDisconnect.Load() {
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
	s.consumer = consumer
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
		if s.ctx.Err() != nil || s.bd.clientDisconnect.Load() {
			return subscribeStateStopping, nil
		}
		util.Logger.Error("error received from consumer", "error", err)
		s.lastRecoveryError = &subscribeStateTransitionError{
			err:              err,
			msg:              "an error occurred while receiving a message",
			queue:            queueName,
			clientIdentifier: s.bd.ClientIdentifier,
		}
		return subscribeStateRecovering, nil
	}

	for {
		select {
		case <-s.ctx.Done():
			util.Logger.Info("subscription context canceled, stopping subscription")
			return subscribeStateStopping, nil
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
			s.bd.activeMessages.Add(m.Uuid, &activeDelivery{subscription: s, delivery: delCtx})
			select {
			case s.messageCh <- m:
				util.Logger.Debugf("message forwarded to channel: %s", m.Uuid)
				atomic.AddInt64(&s.bd.consumed, 1)
				s.recoveryAttempts = 0
				s.lastRecoveryError = nil
			case err := <-receiveErrors:
				return recoverFromReceiveError(err)
			case <-s.ctx.Done():
				util.Logger.Info("subscription context canceled while forwarding message")
				s.bd.activeMessages.Delete(m.Uuid)
				return subscribeStateStopping, nil
			}
		}
	}
}

func (s *Subscription) cleanupActiveDeliveries() {
	for _, uuid := range s.bd.activeMessages.GetList() {
		value, ok := s.bd.activeMessages.Get(uuid)
		if !ok {
			continue
		}
		delivery, ok := value.(*activeDelivery)
		if ok && delivery.subscription == s {
			s.bd.activeMessages.DeleteIfEqual(uuid, delivery)
		}
	}
}

func (s *Subscription) recovering() (subscribeState, *subscribeStateTransitionError) {
	if s.ctx.Err() != nil || s.bd.clientDisconnect.Load() {
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
	select {
	case <-s.ctx.Done():
		return subscribeStateStopping, nil
	case <-timer.C:
	}
	if s.bd.clientDisconnect.Load() {
		return subscribeStateStopping, nil
	}
	if s.bd.provider == nil || !s.bd.provider.WaitForConnect(s.ctx) {
		if s.ctx.Err() != nil || s.bd.clientDisconnect.Load() {
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
	s.bd.subscriptions.Delete(subscriptionKey(s.source))
	return subscribeStateStopped, nil
}

func (s *Subscription) stop() (subscribeState, *subscribeStateTransitionError) {
	// TODO
	return subscribeStateStopped, nil
}
