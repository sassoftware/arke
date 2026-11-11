// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"
	"crypto/tls"
	"errors"
	"sync/atomic"
	"time"

	goamqp "github.com/Azure/go-amqp"
	ramqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
)

// rabbitMQAMQP10ConnectionShim Shim so we can do unit testing
type rabbitMQAMQP10ConsumerShim interface {
	Receive(context.Context) (ramqp.IDeliveryContext, error)
	Close(context.Context) error
}

type rabbitMQAMQP10ConnectionShim interface {
	Close(context.Context) error
	WatchConnection(ch chan *ramqp.StateChanged) error
	IsClosed() bool
	State() int
	DeclareExchange(context.Context, ramqp.IExchangeSpecification) (*ramqp.AmqpExchangeInfo, error)
	DeclareQueue(context.Context, ramqp.IQueueSpecification) (*ramqp.AmqpQueueInfo, error)
	DeclareBind(context.Context, ramqp.IBindingSpecification) (string, error)
	NewConsumer(ctx context.Context, queueName string, options ramqp.IConsumerOptions) (rabbitMQAMQP10ConsumerShim, error)
}

// rabbitMQAMQP10Connection A connection to the broker
type rabbitMQAMQP10Connection struct {
	rabbitMQAMQP10ConnectionShim //nolint:unused
	provider                     provider.Provider
	connection                   *ramqp.AmqpConnection
	connectionCtx                context.Context
	connectionCancel             context.CancelFunc
	tlsCfg                       *tls.Config
	state                        int
	stateChannel                 chan *ramqp.StateChanged
	closed                       atomic.Bool
}

func getRabbitMQAMQP10ConnOptions(ctx context.Context, cf *pb.ConnectionConfiguration, tlsCfg *tls.Config) (*ramqp.AmqpConnOptions, error) {
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

func (a *rabbitMQAMQP10Connection) WatchConnection(ch chan *ramqp.StateChanged) error {
	if a.connection == nil {
		return errors.New("connection is nil")
	}
	a.connection.NotifyStatusChange(ch)
	return nil
}

// IsClosed reports whether the connection is unusable. A reconnecting
// connection is not closed because the client library is recovering it.
func (a *rabbitMQAMQP10Connection) IsClosed() bool {
	if a.closed.Load() {
		return true
	}
	switch a.State() {
	case provider.CLOSED, provider.DISCONNECTED:
		return true
	default:
		return false
	}
}

// State maps the client library lifecycle state to a provider.* state.
func (a *rabbitMQAMQP10Connection) State() int {
	if a.connection == nil {
		return provider.DISCONNECTED
	}
	return providerStateFromLifecycle(a.connection.State())
}

func providerStateFromLifecycle(state ramqp.ILifeCycleState) int {
	switch state.(type) {
	case *ramqp.StateOpen:
		return provider.CONNECTED
	case *ramqp.StateReconnecting:
		return provider.CONNECTING
	case *ramqp.StateClosed, *ramqp.StateClosing:
		return provider.CLOSED
	default:
		return provider.DISCONNECTED
	}
}

func (a *rabbitMQAMQP10Connection) DeclareExchange(ctx context.Context, specification ramqp.IExchangeSpecification) (*ramqp.AmqpExchangeInfo, error) {
	return a.connection.Management().DeclareExchange(ctx, specification)
}

func (a *rabbitMQAMQP10Connection) DeclareQueue(ctx context.Context, specification ramqp.IQueueSpecification) (*ramqp.AmqpQueueInfo, error) {
	return a.connection.Management().DeclareQueue(ctx, specification)
}

func (a *rabbitMQAMQP10Connection) DeclareBind(ctx context.Context, specification ramqp.IBindingSpecification) (string, error) {
	return a.connection.Management().Bind(ctx, specification)
}

// TODO: Issue 204 - cancel context used by state channel (i.e., make sure connection watcher channel shuts down) and close state channel
func (a *rabbitMQAMQP10Connection) Close(_ context.Context) error {
	// The client library does not move to Closed on a client-initiated Close.
	a.closed.Store(true)
	defer a.connectionCancel()
	return a.connection.Close(a.connectionCtx)
}

func (a *rabbitMQAMQP10Connection) NewConsumer(ctx context.Context, queueName string, options ramqp.IConsumerOptions) (rabbitMQAMQP10ConsumerShim, error) {
	return a.connection.NewConsumer(ctx, queueName, options)
}
