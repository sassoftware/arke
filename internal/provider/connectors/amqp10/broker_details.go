package amqp10

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/i18n"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
)

// BrokerDetails struct houses connection specific information for the broker
type BrokerDetails struct {
	sync.Mutex
	ctx        context.Context
	Env        amqp10EnvironmentShim
	Connection amqp10ConnectionShim

	ErrorChannel chan amqp10Error

	// TODO: PSEVT-259 - do we need to shim the publisher?
	// 		publisher needs to check connection lifecycle state before doing
	// 		anything, so probably need a shim.
	// 		RetryPublisher

	// TODO: PSEVT-259 - it's not clear that pubChannelCtx/pubChannelCancel will
	// 		be needed given how connections are managed.
	// Ctx used by both pubChannels and pubPCChannels
	pubChannelCtx context.Context

	// Ctx cancellation function used by both pubChannels and pubPCChannels
	pubChannelCancel context.CancelFunc

	// TODO: PSEVT-259 - consider blocking pool is the right approach for
	//		managing publishers.
	pubChannels   *util.BlockingPool
	pubPCChannels *util.BlockingPool

	// TODO: create streamConnectionShim
	// StreamConnection streamConnectionShim

	ClientIdentifier string
	knownExchanges   *util.ConcurrentMap
	knownQueues      *util.ConcurrentMap
	knownBindings    *util.ConcurrentMap
	activeMessages   *util.ConcurrentMap
	state            atomic.Uint32
	connectionConfig *pb.ConnectionConfiguration
	tlsConfig        *tls.Config
	tlsSkipVerify    bool
	ActiveStreams    int64
	consumed         int64
	produced         int64
	clientDisconnect atomic.Bool
	lastPubSubEvent  time.Time
	tlsEnabled       bool
	shutdownChan     chan bool

	// watcherWG tracks the connectionWatcher goroutine so callers (notably
	// tests) can wait for it to exit after a Disconnect, rather than letting
	// it outlive the connection.
	watcherWG sync.WaitGroup
}

func (bd *BrokerDetails) updateLastPubSubEvent() {
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

func (bd *BrokerDetails) waitWhileConnecting() uint32 {
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

	bd.Connection = conn
	bd.ErrorChannel = make(chan amqp10Error, 1)

	// TODO: PSEVT-257 - consider whether this is needed
	// bd.ErrorChannel = bd.Connection.NotifyClose(bd.ErrorChannel) // this looks unneeded but it aids in unit testing

	bd.state.Store(provider.CONNECTED)
	util.Logger.Info(i18n.ClientConnected, bd.ClientIdentifier)

	// TODO: PSEVT-256
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

	if bd.Connection != nil {
		if err := bd.Connection.Close(bd.ctx); err != nil {
			util.Logger.Warn(fmt.Sprintf("Error disconnecting client %s from broker: %s", bd.ClientIdentifier, err.Error()))
		}
		bd.Connection = nil
	}

	bd.state.Store(provider.DISCONNECTED)
	util.Logger.Info(i18n.ClientDisconnect, bd.ClientIdentifier)
}
