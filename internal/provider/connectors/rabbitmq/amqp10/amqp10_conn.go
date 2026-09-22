package amqp10

import (
	"context"
	"crypto/tls"
	"time"

	goamqp "github.com/Azure/go-amqp"
	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
)

// amqp10ConnectionShim Shim so we can do unit testing
type amqp10ConnectionShim interface {
	Close(context.Context) error
	WatchConnection(ch chan *rabbitmqamqp.StateChanged)
	IsClosed() bool
	State() int
}

// amqp10Connection A connection to the broker
type amqp10Connection struct {
	provider         provider.Provider
	connStr          string
	connection       *rabbitmqamqp.AmqpConnection
	connectionCtx    context.Context
	connectionCancel context.CancelFunc
	tlsCfg           *tls.Config
	state            int
	stateChannel     chan *rabbitmqamqp.StateChanged
}

// TODO: Issue 187 - is the context parameter required at all?
func getAmqp10ConnOptions(ctx context.Context, cf *pb.ConnectionConfiguration, tlsCfg *tls.Config) (*rabbitmqamqp.AmqpConnOptions, error) {
	// TODO: Issue 187 - here are other options that should be set here
	clientIdentifier, err := util.GetClientIdentifier(ctx)
	if err != nil {
		return nil, err
	}
	return &rabbitmqamqp.AmqpConnOptions{
		Id:          clientIdentifier, // TODO: Issue 187 - verify this is the right value here
		ContainerID: clientIdentifier, // TODO: Issue 187 - verify this is the right value here
		IdleTimeout: 10 * time.Second,
		HostName:    cf.GetHost(),
		SASLType: goamqp.SASLTypePlain(
			cf.GetCredentials().GetUsername(),
			cf.GetCredentials().GetPassword(),
		),
		TLSConfig: tlsCfg,
	}, nil
}

func (a *amqp10Connection) WatchConnection(ch chan *rabbitmqamqp.StateChanged) {
	a.connection.NotifyStatusChange(ch)
}

func (a *amqp10Connection) IsClosed() bool {
	// TODO: Issue 204 - implement IsClosed based on connection state changes
	return false
}

func (a *amqp10Connection) State() int {
	// TODO: Issue 204 - implement state retrieval based on connection state changes
	// but states should be provider.*
	// return a.connection.State()
	return 0
}

// TODO: Issue 204 - cancel context used by state channel (i.e., make sure connection watcher channel shuts down) and close state channel
// TODO: Issue 187 - Should we be saving the connection ctx (as here) instead of passing it in to Close?
func (a *amqp10Connection) Close(_ context.Context) error {
	defer a.connectionCancel()
	return a.connection.Close(a.connectionCtx)
}
