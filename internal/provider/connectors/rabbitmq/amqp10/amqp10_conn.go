package amqp10

import (
	"context"
	"crypto/tls"
	"time"

	goamqp "github.com/Azure/go-amqp"
	ramqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
)

// rabbitmqAmqp10ConnectionShim Shim so we can do unit testing
type rabbitmqAmqp10ConnectionShim interface {
	Close(context.Context) error
	WatchConnection(ch chan *ramqp.StateChanged)
	IsClosed() bool
	State() int
}

// rabbitmqAmqp10Connection A connection to the broker
type rabbitmqAmqp10Connection struct {
	provider         provider.Provider
	connStr          string
	connection       *ramqp.AmqpConnection
	connectionCtx    context.Context
	connectionCancel context.CancelFunc
	tlsCfg           *tls.Config
	state            int
	stateChannel     chan *ramqp.StateChanged
}

func getRabbitmqAmqp10ConnOptions(ctx context.Context, cf *pb.ConnectionConfiguration, tlsCfg *tls.Config) (*ramqp.AmqpConnOptions, error) {
	clientIdentifier, err := util.GetClientIdentifier(ctx)
	if err != nil {
		return nil, err
	}
	return &ramqp.AmqpConnOptions{
		Id:          clientIdentifier, // client-side - not sent to server
		ContainerID: clientIdentifier, // sent to server
		IdleTimeout: 10 * time.Second,
		HostName:    cf.GetHost(),
		SASLType: goamqp.SASLTypePlain(
			cf.GetCredentials().GetUsername(),
			cf.GetCredentials().GetPassword(),
		),
		TLSConfig: tlsCfg,
	}, nil
}

func (a *rabbitmqAmqp10Connection) WatchConnection(ch chan *ramqp.StateChanged) {
	a.connection.NotifyStatusChange(ch)
}

func (a *rabbitmqAmqp10Connection) IsClosed() bool {
	// TODO: Issue 204 - implement IsClosed based on connection state changes
	return false
}

func (a *rabbitmqAmqp10Connection) State() int {
	// TODO: Issue 204 - implement state retrieval based on connection state changes
	// but states should be provider.*
	// return a.connection.State()
	return 0
}

// TODO: Issue 204 - cancel context used by state channel (i.e., make sure connection watcher channel shuts down) and close state channel
func (a *rabbitmqAmqp10Connection) Close(_ context.Context) error {
	defer a.connectionCancel()
	return a.connection.Close(a.connectionCtx)
}
