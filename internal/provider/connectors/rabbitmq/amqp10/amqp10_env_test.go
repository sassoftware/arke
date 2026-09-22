package amqp10

import (
	"context"
	"crypto/tls"
	"testing"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type rabbitmqAmqp10EnvironmentMock struct {
	newConnectionCalled bool
	newConnectionCtx    context.Context
	conn                rabbitmqAmqp10ConnectionShim
	err                 error
}

func (m *rabbitmqAmqp10EnvironmentMock) NewConnection(ctx context.Context) (rabbitmqAmqp10ConnectionShim, error) {
	m.newConnectionCalled = true
	m.newConnectionCtx = ctx
	return m.conn, m.err
}

func (m *rabbitmqAmqp10EnvironmentMock) WatchConnection(ch chan *rabbitmqamqp.StateChanged) {
	if m.conn != nil {
		m.conn.WatchConnection(ch)
	}
}

func (m *rabbitmqAmqp10EnvironmentMock) Close(ctx context.Context) error {
	return nil
}

func Test_newRabbitmqAmqp10Environment(t *testing.T) {
	ctx := context.Background()
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	options := &rabbitmqamqp.AmqpConnOptions{Id: "test-client", TLSConfig: tlsCfg}
	cf := newTestConnectionConfig()
	env, err := newRabbitmqAmqp10EnvironmentFunc(ctx, cf, tlsCfg, "amqp://guest:guest@localhost:5672", options)
	require.NoError(t, err)

	require.IsType(t, &rabbitmqAmqp10Environment{}, env)
	amqpEnv := env.(*rabbitmqAmqp10Environment)
	assert.Equal(t, ctx, amqpEnv.ctx)
	assert.Same(t, cf, amqpEnv.connectionConfig)
	assert.Same(t, tlsCfg, amqpEnv.tlsConfig)
	assert.NotNil(t, amqpEnv.environment)
}

func Test_amqp10Environment_NewConnection(t *testing.T) {
	t.Run("returns an error when the context is already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		env := &rabbitmqAmqp10Environment{
			connectionConfig: newTestConnectionConfig(),
			environment:      rabbitmqamqp.NewEnvironment("amqp://127.0.0.1:1", nil),
		}

		conn, err := env.NewConnection(ctx)

		assert.Nil(t, conn)
		assert.Error(t, err)
	})

	t.Run("returns an error when the endpoint cannot be reached", func(t *testing.T) {
		env := &rabbitmqAmqp10Environment{
			connectionConfig: newTestConnectionConfig(),
			environment:      rabbitmqamqp.NewEnvironment("amqp://127.0.0.1:1", nil),
		}

		conn, err := env.NewConnection(context.Background())

		assert.Nil(t, conn)
		assert.Error(t, err)
	})
}

func Test_amqp10Environment_Close(t *testing.T) {
	env := &rabbitmqAmqp10Environment{
		ctx:         context.Background(),
		environment: rabbitmqamqp.NewEnvironment("amqp://127.0.0.1:1", nil),
	}

	assert.NoError(t, env.Close(context.Background()))
}

func Test_amqp10Connection_IsClosed(t *testing.T) {
	conn := &rabbitmqAmqp10Connection{}

	assert.False(t, conn.IsClosed())
}

func Test_amqp10Connection_State(t *testing.T) {
	conn := &rabbitmqAmqp10Connection{}

	assert.Equal(t, 0, conn.State())
}
