package amqp10

import (
	"context"
	"crypto/tls"
	"testing"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_newRabbitMQAMQP10Environment(t *testing.T) {
	clientName := "test-client"
	ctx, clientIdentifier := newTestProviderContext(t, clientName)
	t.Logf("ClientIdentifier: %s", clientIdentifier)
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	cf := newTestConnectionConfig()
	env, err := newRabbitMQAMQP10EnvironmentFunc(ctx, cf, tlsCfg)
	require.NoError(t, err)

	require.IsType(t, &rabbitMQAMQP10Environment{}, env)
	amqpEnv := env.(*rabbitMQAMQP10Environment)
	assert.Equal(t, ctx, amqpEnv.ctx)
	assert.Contains(t, clientIdentifier, clientName)
	assert.Same(t, cf, amqpEnv.connectionConfig)
	assert.Same(t, tlsCfg, amqpEnv.tlsConfig)
	assert.NotNil(t, amqpEnv.environment)
}

func Test_rabbitMQAMQP10Environment_NewConnection(t *testing.T) {
	t.Run("returns an error when the context is already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		env := &rabbitMQAMQP10Environment{
			connectionConfig: newTestConnectionConfig(),
			environment:      rabbitmqamqp.NewEnvironment("amqp://127.0.0.1:1", nil),
		}

		conn, err := env.NewConnection(ctx)

		assert.Nil(t, conn)
		assert.Error(t, err)
	})

	t.Run("returns an error when the endpoint cannot be reached", func(t *testing.T) {
		env := &rabbitMQAMQP10Environment{
			connectionConfig: newTestConnectionConfig(),
			environment:      rabbitmqamqp.NewEnvironment("amqp://127.0.0.1:1", nil),
		}

		conn, err := env.NewConnection(context.Background())

		assert.Nil(t, conn)
		assert.Error(t, err)
	})
}

func Test_rabbitMQAMQP10Connection_IsClosed(t *testing.T) {
	conn := &rabbitMQAMQP10Connection{}

	assert.False(t, conn.IsClosed())
}

func Test_rabbitMQAMQP10Connection_State(t *testing.T) {
	conn := &rabbitMQAMQP10Connection{}

	assert.Equal(t, 0, conn.State())
}
