package amqp10

import (
	"context"
	"crypto/tls"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
)

var newAmqp10EnvironmentFunc = newAmqp10Environment

type amqp10EnvironmentShim interface {
	NewConnection(context.Context) (amqp10ConnectionShim, error)
	Close(context.Context) error
}

type amqp10Environment struct {
	ctx              context.Context
	environment      *rabbitmqamqp.Environment
	connectionConfig *pb.ConnectionConfiguration
	tlsConfig        *tls.Config
}

func newAmqp10Environment(ctx context.Context, cf *pb.ConnectionConfiguration, tlsConfig *tls.Config, connURL string, options *rabbitmqamqp.AmqpConnOptions) (amqp10EnvironmentShim, error) {
	util.Logger.Debugf("Creating new AMQP 1.0 environment with URL: ->%s<-", connURL)
	return &amqp10Environment{
		ctx:              ctx,
		connectionConfig: cf,
		tlsConfig:        tlsConfig,
		environment:      rabbitmqamqp.NewEnvironment(connURL, options),
	}, nil
}

func (e *amqp10Environment) NewConnection(ctx context.Context) (amqp10ConnectionShim, error) {
	connCtx, connCancel := context.WithCancel(ctx)
	conn, err := e.environment.NewConnection(connCtx)
	if err != nil {
		connCancel()
		return nil, err
	}
	return &amqp10Connection{
		connStr:          getConnURL(e.connectionConfig),
		connection:       conn,
		connectionCtx:    connCtx,
		connectionCancel: connCancel,
		tlsCfg:           e.tlsConfig,
		state:            provider.CONNECTED,
	}, nil
}

func (e *amqp10Environment) Close(ctx context.Context) error {
	return e.environment.CloseConnections(e.ctx)
}
