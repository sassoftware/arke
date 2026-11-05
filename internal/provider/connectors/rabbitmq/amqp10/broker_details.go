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
	"github.com/sassoftware/arke/internal/provider/connectors/amqp/track"
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

	stateChannel chan *rabbitmqamqp.StateChanged

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

			switch change.To.(type) {
			case *rabbitmqamqp.StateOpen:
				bd.State.Store(provider.CONNECTED)
			case *rabbitmqamqp.StateReconnecting:
				bd.State.Store(provider.CONNECTING)
			case *rabbitmqamqp.StateClosed:
				bd.State.Store(provider.CLOSED)
				return
			case *rabbitmqamqp.StateClosing:
				// TODO: Issue 204 - provider does not have a distinct CLOSING state, so we treat it as CLOSED (do we need a CLOSING state?)
				bd.State.Store(provider.CLOSED)
				return
			}
		}
	}
}

func (bd *BrokerDetails) exchangeExists(name string) bool {
	return bd.EntityTracker().ExchangeExists(name)
}

func (bd *BrokerDetails) queueExists(name string) bool {
	return bd.EntityTracker().QueueExists(name)
}

func (bd *BrokerDetails) connect() (bool, error) {
	if bd.ClientDisconnect.Load() {
		return false, nil
	}

	if bd.State.Load() == provider.CONNECTING {
		switch bd.WaitWhileConnecting() {
		case provider.CONNECTED:
			return true, nil
		case provider.CLOSED:
			return false, nil
		}
	}

	bd.Lock()
	defer bd.Unlock()
	if bd.State.Load() == provider.CONNECTED {
		return true, nil
	}

	bd.State.Store(provider.CONNECTING)
	bd.shutdownChan = make(chan struct{})

	// Reinitialize these maps early, we especially want to
	// ensure activeMessages gets cleared out before an Ack/Nacks
	// are sent from the client.
	bd.KnownEntities = track.New()
	bd.ActiveMessages = util.NewConcurrentMap()

	util.Logger.Info(i18n.ClientConnect, bd.ClientIdentifier, bd.ConnectionConfig.GetHost())
	conn, err := bd.Env.NewConnection(bd.ctx)
	if err != nil {
		util.Logger.Warn(i18n.ClientBrokerConnectError, err.Error(), bd.ClientIdentifier)
		bd.State.Store(provider.CLOSED)
		return false, err
	}
	bd.State.Store(provider.CONNECTED)

	// buf of 5 to account for possible quick state changes and avoid blocking
	bd.Connection = conn
	bd.stateChannel = make(chan *rabbitmqamqp.StateChanged, 5)
	err = bd.Connection.WatchConnection(bd.stateChannel)
	if err != nil {
		bd.disconnect()
		return false, err
	}
	go bd.watchConnection()

	util.Logger.Info(i18n.ClientConnected, bd.ClientIdentifier)

	// TODO: Issue 203
	// go bd.loadExchanges()

	return true, nil
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
