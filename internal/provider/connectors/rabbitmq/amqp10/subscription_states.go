// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"
	"fmt"
	"sync/atomic"

	ramqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"github.com/sassoftware/arke/internal/provider/connectors/amqp"
	"github.com/sassoftware/arke/internal/util"
)

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
		return subscribeStateRecovering, &subscribeStateTransitionError{
			err:              err,
			msg:              "an error occurred while creating the consumer",
			queue:            queueName,
			clientIdentifier: s.bd.ClientIdentifier,
		}
	}
	s.consumer = consumer
	consumerCtx, cancel := context.WithCancel(s.ctx)

	cleanup := func() {
		consumer.Close(consumerCtx)
		cancel()
	}

	incomingMessages := make(chan ramqp.IDeliveryContext)
	recvErrs := make(chan error)
	go func() {
		for {
			delCtx, err := consumer.Receive(consumerCtx)
			if err != nil {
				recvErrs <- err
				continue
			}
			incomingMessages <- delCtx
		}
	}()

	for {
		select {
		case <-s.ctx.Done():
			util.Logger.Info("subscription context canceled, stopping subscription")
			cleanup()
			return subscribeStateStopping, nil
		// check if consumerCtx is canceled
		case <-consumerCtx.Done():
			util.Logger.Info("consumer context canceled, stopping subscription")
			cleanup()
			return subscribeStateStopping, nil
		case err := <-recvErrs:
			util.Logger.Error("error received from consumer", "error", err)
			cleanup()
			return subscribeStateRecovering, &subscribeStateTransitionError{
				err:              err,
				msg:              "an error occurred while receiving a message",
				queue:            queueName,
				clientIdentifier: s.bd.ClientIdentifier,
			}
		case delCtx := <-incomingMessages:
			message := delCtx.Message()
			if message == nil {
				continue
			}
			m := delCtxToMessage(delCtx, s.source)
			if m != nil {
				s.bd.activeMessages.Add(m.Uuid, delCtx)
				s.messageCh <- m
				atomic.AddInt64(&s.bd.consumed, 1)
			}
		}
	}
}

func (s *Subscription) recovering() (subscribeState, *subscribeStateTransitionError) {
	// TODO
	return subscribeStateRecovering, nil
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
