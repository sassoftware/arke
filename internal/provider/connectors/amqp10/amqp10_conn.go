package amqp10

import (
	"context"
	"crypto/tls"
	"net"
	"net/url"
	"strconv"

	goamqp "github.com/Azure/go-amqp"
	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
)

// amqp10ConnectionShim Shim so we can do unit testing
type amqp10ConnectionShim interface {
	// TODO: finish impl
	// Connect() error
	Close(context.Context) error
	IsClosed() bool
	// NotifyClose(chan amqp10Error) chan amqp10Error
	State() int
}

type amqp10EnvironmentShim interface {
	NewConnection(context.Context) (amqp10ConnectionShim, error)
}

type amqp10Environment struct {
	ctx              context.Context
	environment      *rabbitmqamqp.Environment
	connectionConfig *pb.ConnectionConfiguration
	tlsConfig        *tls.Config
}

func (e *amqp10Environment) NewConnection(ctx context.Context) (amqp10ConnectionShim, error) {
	statusChan := make(chan *rabbitmqamqp.StateChanged, 10)
	connCtx, connCancel := context.WithCancel(ctx)
	conn, err := e.environment.NewConnection(connCtx)
	if err != nil {
		connCancel()
		return nil, err
	}
	conn.NotifyStatusChange(statusChan)

	return &amqp10Connection{
		connStr:          getConnURL(e.connectionConfig),
		connection:       conn,
		connectionCtx:    connCtx,
		connectionCancel: connCancel,
		tlsCfg:           e.tlsConfig,
		environment:      e,
		clientIdentifier: "",
		state:            0,
		stateChannel:     statusChan,
	}, nil
}

var newAmqp10Environment = func(ctx context.Context, tlsConfig *tls.Config, connURL string, options *rabbitmqamqp.AmqpConnOptions) amqp10EnvironmentShim {
	return &amqp10Environment{
		ctx:         ctx,
		tlsConfig:   tlsConfig,
		environment: rabbitmqamqp.NewEnvironment(connURL, options),
	}
}

// amqp10Connection A connection to the broker
type amqp10Connection struct {
	connStr          string
	connection       *rabbitmqamqp.AmqpConnection
	connectionCtx    context.Context
	connectionCancel context.CancelFunc
	tlsCfg           *tls.Config
	environment      amqp10EnvironmentShim
	clientIdentifier string
	state            int
	stateChannel     chan *rabbitmqamqp.StateChanged
}

// TODO: is the context parameter required at all?
func getAmqp10ConnOptions(_ context.Context, cf *pb.ConnectionConfiguration, tlsCfg *tls.Config) *rabbitmqamqp.AmqpConnOptions {
	// TODO: there are other options that should be set here
	return &rabbitmqamqp.AmqpConnOptions{
		HostName:  cf.GetHost(),
		SASLType:  goamqp.SASLTypePlain(cf.GetCredentials().GetUsername(), cf.GetCredentials().GetPassword()),
		TLSConfig: tlsCfg,
		Id:        cf.GetClientName(),
	}
}

func getConnURL(cf *pb.ConnectionConfiguration) string {
	username := cf.GetCredentials().GetUsername()
	password := cf.GetCredentials().GetPassword()
	host := cf.GetHost()
	port := cf.GetPort()
	scheme := "amqp"
	if cf.GetTls() {
		scheme = "amqps"
	}
	return (&url.URL{
		Scheme: scheme,
		User:   url.UserPassword(username, password),
		Host:   net.JoinHostPort(host, strconv.Itoa(int(port))),
	}).String()
}

func (a *amqp10Connection) IsClosed() bool {
	// TODO: implement
	// state := a.connection.State()
	return false
}

func (a *amqp10Connection) State() int {
	// TODO: implement
	// return a.connection.State()
	return 0
}

func (a *amqp10Connection) Close(_ context.Context) error {
	a.connectionCancel()
	return a.connection.Close(a.connectionCtx)
}
