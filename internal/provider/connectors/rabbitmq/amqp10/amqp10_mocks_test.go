// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"github.com/stretchr/testify/mock"
)

// rabbitMQAMQP10ConnectionMock is a mock implementation of the rabbitMQAMQP10ConnectionShim interface.
type rabbitMQAMQP10ConnectionMock struct {
	mock.Mock
}

func (m *rabbitMQAMQP10ConnectionMock) DeclareExchange(ctx context.Context, specification rabbitmqamqp.IExchangeSpecification) (*rabbitmqamqp.AmqpExchangeInfo, error) {
	args := m.Called(ctx, specification)
	var info *rabbitmqamqp.AmqpExchangeInfo
	if args.Get(0) != nil {
		info = args.Get(0).(*rabbitmqamqp.AmqpExchangeInfo)
	}
	return info, args.Error(1)
}

func (m *rabbitMQAMQP10ConnectionMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *rabbitMQAMQP10ConnectionMock) WatchConnection(ch chan *rabbitmqamqp.StateChanged) error {
	args := m.Called(ch)
	return args.Error(0)
}

func (m *rabbitMQAMQP10ConnectionMock) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *rabbitMQAMQP10ConnectionMock) State() int {
	args := m.Called()
	return args.Int(0)
}

// rabbitMQAMQP10EnvironmentMock is a mock implementation of the rabbitMQAMQP10EnvironmentShim interface.
type rabbitMQAMQP10EnvironmentMock struct {
	mock.Mock
}

func (m *rabbitMQAMQP10EnvironmentMock) NewConnection(ctx context.Context) (rabbitMQAMQP10ConnectionShim, error) {
	args := m.Called(ctx)
	conn := args.Get(0)
	if conn == nil {
		return nil, args.Error(1)
	}
	return conn.(rabbitMQAMQP10ConnectionShim), args.Error(1)
}

func (m *rabbitMQAMQP10EnvironmentMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
