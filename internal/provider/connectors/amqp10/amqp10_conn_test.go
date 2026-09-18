package amqp10

import (
	"context"
	"crypto/tls"
	"testing"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type amqp10EnvironmentMock struct {
	newConnectionCalled bool
	newConnectionCtx    context.Context
	conn                amqp10ConnectionShim
	err                 error
}

func (m *amqp10EnvironmentMock) NewConnection(ctx context.Context) (amqp10ConnectionShim, error) {
	m.newConnectionCalled = true
	m.newConnectionCtx = ctx
	return m.conn, m.err
}

func newTestConnectionConfig() *pb.ConnectionConfiguration {
	return &pb.ConnectionConfiguration{
		Host:       "localhost",
		Port:       5672,
		ClientName: "test-client",
		Credentials: &pb.Credentials{
			Username: "guest",
			Password: "guest",
		},
	}
}

func Test_getConnURL(t *testing.T) {
	tests := []struct {
		name     string
		config   *pb.ConnectionConfiguration
		expected string
	}{
		{
			name:     "amqp connection URL",
			config:   newTestConnectionConfig(),
			expected: "amqp://guest:guest@localhost:5672",
		},
		{
			name: "amqps connection URL",
			config: &pb.ConnectionConfiguration{
				Host: "broker.example.com",
				Port: 5671,
				Tls:  true,
				Credentials: &pb.Credentials{
					Username: "guest",
					Password: "guest",
				},
			},
			expected: "amqps://guest:guest@broker.example.com:5671",
		},
		{
			name: "connection URL escapes credentials and brackets IPv6 host",
			config: &pb.ConnectionConfiguration{
				Host: "::1",
				Port: 5672,
				Credentials: &pb.Credentials{
					Username: "user@example.com",
					Password: "p@ss word",
				},
			},
			expected: "amqp://user%40example.com:p%40ss%20word@[::1]:5672",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, getConnURL(tt.config))
		})
	}
}

func Test_getAmqp10ConnOptions(t *testing.T) {
	config := newTestConnectionConfig()
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	options := getAmqp10ConnOptions(context.Background(), config, tlsCfg)

	assert.NotNil(t, options.SASLType)
	assert.Same(t, tlsCfg, options.TLSConfig)
	assert.Equal(t, config.GetClientName(), options.Id)
}

func Test_newAmqp10Environment(t *testing.T) {
	ctx := context.Background()
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	options := &rabbitmqamqp.AmqpConnOptions{Id: "test-client", TLSConfig: tlsCfg}

	env := newAmqp10Environment(ctx, tlsCfg, "amqp://guest:guest@localhost:5672", options)

	require.IsType(t, &amqp10Environment{}, env)
	amqpEnv := env.(*amqp10Environment)
	assert.Equal(t, ctx, amqpEnv.ctx)
	assert.Same(t, tlsCfg, amqpEnv.tlsConfig)
	assert.NotNil(t, amqpEnv.environment)
}

func Test_amqp10Connection_IsClosed(t *testing.T) {
	conn := &amqp10Connection{}

	assert.False(t, conn.IsClosed())
}

func Test_amqp10Connection_State(t *testing.T) {
	conn := &amqp10Connection{}

	assert.Equal(t, 0, conn.State())
}
