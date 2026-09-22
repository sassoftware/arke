package amqp10

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/i18n"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
)

// BrokerDetails struct houses connection specific information for the broker
type BrokerDetails struct {
	sync.Mutex
	ctx      context.Context
	provider provider.Provider
	Env      amqp10EnvironmentShim

	// TODO: The only _real_ reason broker details needs a connection is for
	// management (conn.Management()). Otherwise the connection could just
	// expose the methods brokerdetails needs, like state, etc., and Env can
	// manage closing it.
	Connection amqp10ConnectionShim

	// TODO: Issue 199 - do we need to shim the publisher?
	// 		publisher needs to check connection lifecycle state before doing
	// 		anything, so probably need a shim.
	// 		RetryPublisher

	// TODO: Issue 199 - it's not clear that pubChannelCtx/pubChannelCancel will
	// 		be needed given how connections are managed.
	// Ctx used by both pubChannels and pubPCChannels
	pubChannelCtx context.Context

	// Ctx cancellation function used by both pubChannels and pubPCChannels
	pubChannelCancel context.CancelFunc

	// TODO: Issue 199 - consider if blocking pool is the right approach for
	//		managing publishers.
	pubChannels   *util.BlockingPool
	pubPCChannels *util.BlockingPool

	// TODO: Issue 187 - Create streamConnectionShim
	// StreamConnection streamConnectionShim

	ClientIdentifier string
	knownExchanges   *util.ConcurrentMap
	knownQueues      *util.ConcurrentMap
	knownBindings    *util.ConcurrentMap
	activeMessages   *util.ConcurrentMap

	state        atomic.Uint32
	stateChannel chan *rabbitmqamqp.StateChanged

	connectionConfig *pb.ConnectionConfiguration
	tlsConfig        *tls.Config
	tlsSkipVerify    bool
	ActiveStreams    int64
	consumed         int64
	produced         int64
	clientDisconnect atomic.Bool
	lastPubSubEvent  time.Time
	tlsEnabled       bool

	// TODO: Issue 204 - used by connection watcher and connection cleaner
	shutdownChan chan bool

	// watcherWG tracks the connectionWatcher goroutine so callers (notably
	// tests) can wait for it to exit after a Disconnect, rather than letting
	// it outlive the connection.
	watcherWG sync.WaitGroup
}

// TODO: Issue 204 - implement connection watcher to handle state changes and reconnections. See https://github.com/rabbitmq/rabbitmq-amqp-go-client/blob/101a3222e9e3b440b815286c1e5635595817fb6f/docs/examples/reliable/reliable.go#L80 for an example of how this should be implemented. See https://github.com/sassoftware/arke/blob/d386f6817d75e3e4964fe924345e1b6b5e243953/internal/provider/connectors/amqp091/amqp091.go#L1851-L1852 for existing logic to be handled. Below is a bare minimum for testing, so save the review for issue 204.
func (bd *BrokerDetails) watchConnection() {
	for change := range bd.stateChannel {
		if change == nil || change.To == nil {
			continue
		}

		switch change.To.(type) {
		case *rabbitmqamqp.StateOpen:
			bd.state.Store(provider.CONNECTED)
		case *rabbitmqamqp.StateReconnecting:
			bd.state.Store(provider.CONNECTING)
		case *rabbitmqamqp.StateClosed:
			bd.state.Store(provider.CLOSED)
			return
		case *rabbitmqamqp.StateClosing:
			// TODO: Issue 204 - provider does not have a distinct CLOSING state, so we treat it as CLOSED (do we need a CLOSING state?)
			bd.state.Store(provider.CLOSED)
			return
		}
	}
}

func (bd *BrokerDetails) updateLastPubSubEvent() {
	// TODO: Issue 187 - Should this be protected by a mutex?
	bd.lastPubSubEvent = time.Now()
}

func (bd *BrokerDetails) incrementStreamCount() {
	atomic.AddInt64(&bd.ActiveStreams, 1)
	bd.updateLastPubSubEvent()
}

func (bd *BrokerDetails) decrementStreamCount() {
	atomic.AddInt64(&bd.ActiveStreams, -1)
	bd.updateLastPubSubEvent()
}

func (bd *BrokerDetails) waitWhileConnecting() int {
	for start := time.Now(); time.Since(start) < 30*time.Second; {
		switch bd.state.Load() {
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

func (bd *BrokerDetails) connect() (bool, error) {
	if bd.clientDisconnect.Load() {
		return false, nil
	}

	if bd.state.Load() == provider.CONNECTING {
		switch bd.waitWhileConnecting() {
		case provider.CONNECTED:
			return true, nil
		case provider.CLOSED:
			return false, nil
		}
	}

	bd.Lock()
	defer bd.Unlock()
	if bd.state.Load() == provider.CONNECTED {
		return true, nil
	}

	bd.state.Store(provider.CONNECTING)

	// Reinitialize these maps early, we especially want to
	// ensure activeMessages gets cleared out before an Ack/Nacks
	// are sent from the client.
	bd.knownExchanges = util.NewConcurrentMap()
	bd.knownQueues = util.NewConcurrentMap()
	bd.knownBindings = util.NewConcurrentMap()
	bd.activeMessages = util.NewConcurrentMap()

	util.Logger.Info(i18n.ClientConnect, bd.ClientIdentifier, bd.connectionConfig.GetHost())
	conn, err := bd.Env.NewConnection(bd.ctx)
	if err != nil {
		util.Logger.Warn(i18n.ClientBrokerConnectError, err.Error(), bd.ClientIdentifier)
		bd.state.Store(provider.CLOSED)
		return false, err
	}
	bd.state.Store(provider.CONNECTED)

	// buf of 5 to account for possible quick state changes and avoid blocking
	bd.Connection = conn
	bd.stateChannel = make(chan *rabbitmqamqp.StateChanged, 5)
	bd.Connection.WatchConnection(bd.stateChannel)

	go bd.watchConnection()

	util.Logger.Info(i18n.ClientConnected, bd.ClientIdentifier)

	// TODO: Issue 203
	// go bd.loadExchanges()

	return true, nil
}

func (bd *BrokerDetails) disconnect() {
	bd.Lock()
	defer bd.Unlock()

	if bd.state.Load() == provider.DISCONNECTED || bd.state.Load() == provider.CLOSED {
		return
	}

	bd.clientDisconnect.Store(true)
	if bd.pubChannelCancel != nil {
		bd.pubChannelCancel()
	}

	// We don't call bd.Env.Close because all it does is close the connection, and
	// we have a connection shim. The shim will do other stuff in addition to closing
	// the actual connection.
	//
	// We also leave b.Env alone because it's just a factory for new connections
	// and does not have any sort of actual connection to the broker.
	if bd.Connection != nil {
		err := bd.Connection.Close(bd.ctx)
		if err != nil {
			// TODO: Issue 187 - i18n
			util.Logger.Warn(fmt.Sprintf("Error closing connection for client %s: %s", bd.ClientIdentifier, err.Error()))
		}
		bd.Connection = nil
	}

	bd.state.Store(provider.DISCONNECTED)
	util.Logger.Info(i18n.ClientDisconnect, bd.ClientIdentifier)
}
