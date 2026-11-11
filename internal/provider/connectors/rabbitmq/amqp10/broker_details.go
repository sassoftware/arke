// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"
	"crypto/tls"
	"sync"
	"sync/atomic"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/i18n"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"

	"github.com/sassoftware/arke/internal/provider/connectors/amqp"
)

// BrokerDetails struct houses connection specific information for the broker
type BrokerDetails struct {
	amqp.CommonBrokerDetails
	ctx        context.Context
	provider   provider.Provider
	mgmtClient amqp.ManagementClientShim
	Env        rabbitMQAMQP10EnvironmentShim

	// TODO: The only _real_ reason broker details needs a connection is for
	// exchange declaration. Otherwise the connection could just expose the
	// methods brokerdetails needs, like state, etc., and Env can manage closing it.
	Connection rabbitMQAMQP10ConnectionShim

	// TODO: Issue 199 - do we need to shim the publisher?
	// 		publisher needs to check connection lifecycle state before doing
	// 		anything, so probably need a shim.
	// 		RetryPublisher

	// TODO: Issue 199 - it's not clear that pubChannelCtx/pubChannelCancel will
	// 		be needed given how connections are managed.
	// Ctx used by both pubChannels and pubPCChannels
	// pubChannelCtx context.Context

	// Ctx cancellation function used by both pubChannels and pubPCChannels
	// pubChannelCancel context.CancelFunc

	// TODO: Issue 199 - consider if blocking pool is the right approach for
	//		managing publishers.
	// pubChannels   *util.BlockingPool
	// pubPCChannels *util.BlockingPool

	// TODO: Issue 216 - Create streamConnectionShim
	// StreamConnection streamConnectionShim

	stateChannel  chan *rabbitmqamqp.StateChanged
	subscriptions *util.ConcurrentMap

	tlsSkipVerify bool
	tlsConfig     *tls.Config
	consumed      atomic.Int64
	produced      atomic.Int64
	tlsEnabled    bool

	// TODO: Issue 204 - used by connection watcher and connection cleaner
	shutdownChan chan struct{}
	shutdownOnce sync.Once

	// watcherWG tracks the connectionWatcher goroutine so callers (notably
	// tests) can wait for it to exit after a Disconnect, rather than letting
	// it outlive the connection.
	watcherWG sync.WaitGroup
}

// TODO: Issue 204 - implement connection watcher to handle state changes and reconnections. See https://github.com/rabbitmq/rabbitmq-amqp-go-client/blob/101a3222e9e3b440b815286c1e5635595817fb6f/docs/examples/reliable/reliable.go#L80 for an example of how this should be implemented. See https://github.com/sassoftware/arke/blob/d386f6817d75e3e4964fe924345e1b6b5e243953/internal/provider/connectors/amqp091/amqp091.go#L1851-L1852 for existing logic to be handled. Below is a bare minimum for testing, so save the review for issue 204.
func (bd *BrokerDetails) watchConnection() {
	for {
		select {
		case <-bd.shutdownChan:
			return
		case change, ok := <-bd.stateChannel:
			if !ok || change == nil || change.To == nil {
				return
			}
			util.Logger.Info("connection %s state changed from %v to %v", bd.ClientIdentifier, change.From, change.To)

			switch change.To.(type) {
			case *rabbitmqamqp.StateOpen:
				// Subscriptions in Recovering wait for CONNECTED themselves.
				bd.State.Store(provider.CONNECTED)
			case *rabbitmqamqp.StateReconnecting:
				bd.State.Store(provider.CONNECTING)
				bd.notifySubscriptions(subscribeStateRecovering)
			case *rabbitmqamqp.StateClosed:
				bd.State.Store(provider.CLOSED)
				bd.notifySubscriptions(subscribeStateStopping)
				return
			case *rabbitmqamqp.StateClosing:
				bd.State.Store(provider.CLOSED)
				bd.notifySubscriptions(subscribeStateStopping)
				return
			}
		}
	}
}

func (bd *BrokerDetails) subscribe(ctx context.Context, source *pb.Source, mc chan<- *pb.Message) error {
	s := &Subscription{
		ctx:       ctx,
		bd:        bd,
		source:    source,
		state:     newSubscribeStateMachine(),
		messageCh: mc,
	}
	s.configureStateTransitions()
	err := s.state.transitionState(subscribeStateStarting)
	if err != nil {
		util.Logger.Debugf("failed to initialize subscription state to starting: %v", err)
		return err
	}
	return nil
}

// notifySubscriptions forwards a connection state change to every subscription.
func (bd *BrokerDetails) notifySubscriptions(newState subscribeState) {
	for _, subName := range bd.subscriptions.GetList() {
		s, ok := bd.subscriptions.Get(subName)
		if !ok {
			continue
		}
		if ss, ok := s.(*Subscription); ok && ss != nil {
			ss.notify(newState)
		}
	}
}

// connection returns the current connection, or nil after disconnect. Reads go
// through bd's lock because disconnect() clears the field concurrently.
func (bd *BrokerDetails) connection() rabbitMQAMQP10ConnectionShim {
	bd.Lock()
	defer bd.Unlock()
	return bd.Connection
}

func (bd *BrokerDetails) exchangeExists(name string) bool {
	return bd.EntityTracker().ExchangeExists(name)
}

func (bd *BrokerDetails) queueExists(name string) bool {
	return bd.EntityTracker().QueueExists(name)
}

func (bd *BrokerDetails) disconnect() {
	bd.Lock()
	defer bd.Unlock()

	if bd.State.Load() == provider.DISCONNECTED || bd.State.Load() == provider.CLOSED {
		return
	}

	bd.ClientDisconnect.Store(true)

	if bd.shutdownChan != nil {
		bd.shutdownOnce.Do(func() {
			close(bd.shutdownChan)
		})
	}

	// We don't call bd.Env.Close because all it does is close the connection, and
	// we have a connection shim. The shim will do other stuff in addition to closing
	// the actual connection.
	if bd.Connection != nil {
		err := bd.Connection.Close(bd.ctx)
		if err != nil {
			util.Logger.Warn(i18n.ErrorClosingConnection, bd.ClientIdentifier, err.Error())
		}
		bd.Connection = nil
	}

	bd.State.Store(provider.DISCONNECTED)
	util.Logger.Info(i18n.ClientDisconnect, bd.ClientIdentifier)
}

func (bd *BrokerDetails) getStreamOrQueueStats(source *pb.Source) *pb.SourceStats {
	// stats := &pb.SourceStats{
	// 	MessageCount:  0,
	// 	ConsumerCount: 0,
	// 	LastOffset:    0,
	// 	CurrentOffset: 0,
	// 	Name:          providerName, <- we don't use this in 091
	// 	PublishRate:   0,
	// 	DeliverRate:   0,
	// }

	queue := amqp.SourceName(source)
	vhost := amqp.GetVhost(bd.ConnectionConfig)
	stats := bd.mgmtClient.SourceStats(vhost, queue)
	// TODO: Issue 220/221 - Handle stream-specific stats
	return stats
}
