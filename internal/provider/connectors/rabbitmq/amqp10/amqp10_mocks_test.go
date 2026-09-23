package amqp10

import (
	"context"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"github.com/stretchr/testify/mock"
)

// rabbitmqAmqp10ConnectionMock is a mock implementation of the rabbitmqAmqp10ConnectionShim interface.
type rabbitmqAmqp10ConnectionMock struct {
	mock.Mock
}

func (m *rabbitmqAmqp10ConnectionMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *rabbitmqAmqp10ConnectionMock) WatchConnection(ch chan *rabbitmqamqp.StateChanged) {
	m.Called(ch)
}

func (m *rabbitmqAmqp10ConnectionMock) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *rabbitmqAmqp10ConnectionMock) State() int {
	args := m.Called()
	return args.Int(0)
}

// rabbitmqAmqp10EnvironmentMock is a mock implementation of the rabbitmqAmqp10EnvironmentShim interface.
type rabbitmqAmqp10EnvironmentMock struct {
	mock.Mock
}

func (m *rabbitmqAmqp10EnvironmentMock) NewConnection(ctx context.Context) (rabbitmqAmqp10ConnectionShim, error) {
	args := m.Called(ctx)
	conn := args.Get(0)
	if conn == nil {
		return nil, args.Error(1)
	}
	return conn.(rabbitmqAmqp10ConnectionShim), args.Error(1)
}

func (m *rabbitmqAmqp10EnvironmentMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
