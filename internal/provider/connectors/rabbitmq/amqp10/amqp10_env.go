// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"
	"crypto/tls"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/provider/connectors/amqp"
)

var newRabbitMQAMQP10EnvironmentFunc = newRabbitMQAMQP10Environment

type rabbitMQAMQP10EnvironmentShim interface {
	NewConnection(context.Context) (rabbitMQAMQP10ConnectionShim, error)
	Close(context.Context) error
}

type rabbitMQAMQP10Environment struct {
	rabbitMQAMQP10EnvironmentShim //nolint:unused
	ctx                           context.Context
	environment                   *rabbitmqamqp.Environment
	connectionConfig              *pb.ConnectionConfiguration
	tlsConfig                     *tls.Config
}

func newRabbitMQAMQP10Environment(ctx context.Context, cf *pb.ConnectionConfiguration, tlsConfig *tls.Config) (rabbitMQAMQP10EnvironmentShim, error) {
	connURL := amqp.GetConnURL(cf)
	options, err := getRabbitMQAMQP10ConnOptions(ctx, cf, tlsConfig)
	if err != nil {
		return nil, err
	}
	return &rabbitMQAMQP10Environment{
		ctx:              ctx,
		connectionConfig: cf,
		tlsConfig:        tlsConfig,
		environment:      rabbitmqamqp.NewEnvironment(connURL, options),
	}, nil
}

func (e *rabbitMQAMQP10Environment) NewConnection(ctx context.Context) (rabbitMQAMQP10ConnectionShim, error) {
	connCtx, connCancel := context.WithCancel(ctx)
	conn, err := e.environment.NewConnection(connCtx)
	if err != nil {
		connCancel()
		return nil, err
	}
	return &rabbitMQAMQP10Connection{
		connection:       conn,
		connectionCtx:    connCtx,
		connectionCancel: connCancel,
		tlsCfg:           e.tlsConfig,
		state:            provider.CONNECTED,
	}, nil
}
