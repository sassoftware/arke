package amqp10

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"
)

type rabbitmqAmqp10ProviderAddr struct {
	addr string
}

func (a rabbitmqAmqp10ProviderAddr) Network() string {
	return "tcp"
}

func (a rabbitmqAmqp10ProviderAddr) String() string {
	return a.addr
}

func newTestProviderContext(t *testing.T, clientName string) (context.Context, string) {
	t.Helper()

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: rabbitmqAmqp10ProviderAddr{addr: fmt.Sprintf("%s-%d", clientName, time.Now().UnixNano())}})
	clientIdentifier, err := util.SetClientIdentifier(ctx, clientName)
	require.NoError(t, err)
	t.Cleanup(func() {
		util.RemoveClientIdentifier(ctx)
	})

	return ctx, clientIdentifier
}

func newTestAMQP10Provider() *rabbitmqAmqp10provider {
	return &rabbitmqAmqp10provider{
		connections: util.NewConcurrentMap(),
	}
}

func Test_NewAMQP10Provider(t *testing.T) {
	t.Run("initializes provider", func(t *testing.T) {
		t.Setenv(trustedCerts, "")

		prov := NewRabbitmqAMQP10Provider()

		require.IsType(t, &rabbitmqAmqp10provider{}, prov)
		amqp10Prov := prov.(*rabbitmqAmqp10provider)
		assert.NotNil(t, amqp10Prov.connections)
		assert.Equal(t, 0, amqp10Prov.connections.Length())
	})

	t.Run("ignores unreadable CA bundle", func(t *testing.T) {
		t.Setenv(trustedCerts, "does-not-exist.pem")

		prov := NewRabbitmqAMQP10Provider()

		require.IsType(t, &rabbitmqAmqp10provider{}, prov)
		amqp10Prov := prov.(*rabbitmqAmqp10provider)
		assert.NotNil(t, amqp10Prov.connections)
	})
}

func Test_amqp10provider_getBrokerDetails(t *testing.T) {
	t.Run("returns broker details for client identifier", func(t *testing.T) {
		ctx, clientIdentifier := newTestProviderContext(t, "get-broker-details")
		prov := newTestAMQP10Provider()
		bd := &BrokerDetails{ClientIdentifier: clientIdentifier}
		prov.connections.Add(clientIdentifier, bd)

		got, err := prov.getBrokerDetails(ctx)

		require.NoError(t, err)
		assert.Same(t, bd, got)
	})

	t.Run("returns error when client identifier is missing", func(t *testing.T) {
		prov := newTestAMQP10Provider()

		got, err := prov.getBrokerDetails(context.Background())

		assert.Nil(t, got)
		assert.EqualError(t, err, "could not retrieve client-id from context")
	})

	t.Run("returns error when broker details are missing", func(t *testing.T) {
		ctx, clientIdentifier := newTestProviderContext(t, "missing-broker-details")
		prov := newTestAMQP10Provider()

		got, err := prov.getBrokerDetails(ctx)

		assert.Nil(t, got)
		assert.EqualError(t, err, fmt.Sprintf("Broker details not found for client identifier: %s", clientIdentifier))
	})

}

func Test_amqp10provider_getBrokerDetailsByIdentifier(t *testing.T) {
	prov := newTestAMQP10Provider()
	brokerDetails := &BrokerDetails{ClientIdentifier: "client"}
	prov.connections.Add("client", brokerDetails)

	t.Run("returns broker details for an existing identifier", func(t *testing.T) {
		assert.Same(t, brokerDetails, prov.getBrokerDetailsByIdentifier("client"))
	})

	t.Run("returns nil for a missing identifier", func(t *testing.T) {
		assert.Nil(t, prov.getBrokerDetailsByIdentifier("missing"))
	})
}

type rabbitmqAmqp10EnvironmentCall struct {
	ctx       context.Context
	tlsConfig *tls.Config
	cf        *pb.ConnectionConfiguration
	connURL   string
	options   *rabbitmqamqp.AmqpConnOptions
}

func mockSpyAmqp10Environment(t *testing.T) *rabbitmqAmqp10EnvironmentCall {
	t.Helper()

	gotCall := &rabbitmqAmqp10EnvironmentCall{}
	conn := &rabbitmqAmqp10ConnectionMock{}
	conn.On("WatchConnection", mock.Anything).Return().Once()
	env := &rabbitmqAmqp10EnvironmentMock{}
	env.On("NewConnection", mock.Anything).Return(conn, nil).Once()
	originalNewAmqp10Environment := newRabbitmqAmqp10EnvironmentFunc
	newRabbitmqAmqp10EnvironmentFunc = func(ctx context.Context, cf *pb.ConnectionConfiguration, tlsConfig *tls.Config, connURL string, options *rabbitmqamqp.AmqpConnOptions) (rabbitmqAmqp10EnvironmentShim, error) {
		gotCall.ctx = ctx
		gotCall.cf = cf
		gotCall.tlsConfig = tlsConfig
		gotCall.connURL = connURL
		gotCall.options = options
		return env, nil
	}
	t.Cleanup(func() {
		newRabbitmqAmqp10EnvironmentFunc = originalNewAmqp10Environment
		conn.AssertExpectations(t)
		env.AssertExpectations(t)
	})

	return gotCall
}

func newTestCABundle(t *testing.T) string {
	t.Helper()

	caBundlePath := t.TempDir() + "/ca.pem"
	require.NoError(t, os.WriteFile(caBundlePath, []byte("not a cert"), 0600))
	return caBundlePath
}

func Test_amqp10provider_Connect(t *testing.T) {
	t.Run("non TLS connect does not create TLS config when CA bundle is configured", func(t *testing.T) {
		t.Setenv(trustedCerts, newTestCABundle(t))
		ctx, _ := newTestProviderContext(t, "connect-non-tls")
		prov := newTestAMQP10Provider()
		config := newTestConnectionConfig()
		gotCall := mockSpyAmqp10Environment(t)

		err := prov.Connect(ctx, config, true)

		require.Nil(t, err)
		assert.Nil(t, gotCall.tlsConfig)
	})

	t.Run("TLS connect creates verifying TLS config", func(t *testing.T) {
		ctx, _ := newTestProviderContext(t, "connect-tls")
		prov := newTestAMQP10Provider()
		config := newTestConnectionConfig()
		config.Tls = true
		gotCall := mockSpyAmqp10Environment(t)

		err := prov.Connect(ctx, config, false)

		require.Nil(t, err)
		require.NotNil(t, gotCall.tlsConfig)
		assert.False(t, gotCall.tlsConfig.InsecureSkipVerify)
	})

	t.Run("TLS connect can skip verification", func(t *testing.T) {
		ctx, _ := newTestProviderContext(t, "connect-tls-skip-verify")
		prov := newTestAMQP10Provider()
		config := newTestConnectionConfig()
		config.Tls = true
		gotCall := mockSpyAmqp10Environment(t)

		err := prov.Connect(ctx, config, true)

		require.Nil(t, err)
		require.NotNil(t, gotCall.tlsConfig)
		assert.True(t, gotCall.tlsConfig.InsecureSkipVerify)
	})

	t.Run("TLS connect loads CA bundle into TLS config", func(t *testing.T) {
		t.Setenv(trustedCerts, newTestCABundle(t))
		ctx, _ := newTestProviderContext(t, "connect-tls-ca")
		prov := newTestAMQP10Provider()
		config := newTestConnectionConfig()
		config.Tls = true
		gotCall := mockSpyAmqp10Environment(t)

		err := prov.Connect(ctx, config, false)

		require.Nil(t, err)
		require.NotNil(t, gotCall.tlsConfig)
		assert.NotNil(t, gotCall.tlsConfig.RootCAs)
		assert.False(t, gotCall.tlsConfig.InsecureSkipVerify)
	})
}

func Test_amqp10provider_ClientExists(t *testing.T) {
	prov := newTestAMQP10Provider()
	prov.connections.Add("client", &BrokerDetails{})

	assert.True(t, prov.ClientExists("client"))
	assert.False(t, prov.ClientExists("missing"))
}

func Test_amqp10provider_Disconnect(t *testing.T) {
	t.Run("does nothing when broker details are missing", func(t *testing.T) {
		prov := newTestAMQP10Provider()

		assert.NotPanics(t, func() {
			prov.Disconnect(context.Background())
		})
	})

	t.Run("closes connection and removes matching broker details", func(t *testing.T) {
		ctx, clientIdentifier := newTestProviderContext(t, "disconnect")
		prov := newTestAMQP10Provider()
		conn := &rabbitmqAmqp10ConnectionMock{}
		conn.On("Close", ctx).Return(nil).Once()
		bd := &BrokerDetails{ctx: ctx, ClientIdentifier: clientIdentifier, Connection: conn}
		bd.state.Store(provider.CONNECTED)
		prov.connections.Add(clientIdentifier, bd)

		prov.Disconnect(ctx)

		conn.AssertExpectations(t)
		assert.False(t, prov.ClientExists(clientIdentifier))
		assert.True(t, bd.clientDisconnect.Load())

		// Second disconnect should not panic and should not close the connection again
		prov.Disconnect(ctx)

		conn.AssertNumberOfCalls(t, "Close", 1)
		assert.False(t, prov.ClientExists(clientIdentifier))
	})
}

func Test_amqp10provider_WaitForConnect(t *testing.T) {
	t.Run("returns false when broker details are missing", func(t *testing.T) {
		prov := newTestAMQP10Provider()

		assert.False(t, prov.WaitForConnect(context.Background()))
	})

	t.Run("returns true when broker details are connected", func(t *testing.T) {
		ctx, clientIdentifier := newTestProviderContext(t, "wait-for-connect")
		prov := newTestAMQP10Provider()
		bd := &BrokerDetails{ClientIdentifier: clientIdentifier}
		bd.state.Store(provider.CONNECTED)
		prov.connections.Add(clientIdentifier, bd)

		assert.True(t, prov.WaitForConnect(ctx))
		assert.Equal(t, int64(0), bd.ActiveStreams)
	})

	t.Run("returns false when broker details disappear while waiting", func(t *testing.T) {
		ctx, clientIdentifier := newTestProviderContext(t, "wait-disconnect")
		prov := newTestAMQP10Provider()
		bd := &BrokerDetails{ClientIdentifier: clientIdentifier}
		bd.state.Store(provider.CONNECTING)
		prov.connections.Add(clientIdentifier, bd)

		resultChan := make(chan bool, 1)
		go func() {
			resultChan <- prov.WaitForConnect(ctx)
		}()

		require.Eventually(t, func() bool {
			return atomic.LoadInt64(&bd.ActiveStreams) == 1
		}, time.Second, 10*time.Millisecond)
		prov.connections.DeleteIfEqual(clientIdentifier, bd)

		select {
		case ok := <-resultChan:
			assert.False(t, ok)
		case <-time.After(2 * time.Second):
			t.Fatal("WaitForConnect did not return after broker details were removed")
		}
		assert.Equal(t, int64(0), atomic.LoadInt64(&bd.ActiveStreams))
	})
}

// These methods are left so we can get an accurate code coverage report as we
// go. When the last stub is removed, delete this test.
func Test_amqp10provider_StubbedMethods(t *testing.T) {
	prov := newTestAMQP10Provider()

	// TODO: Issue 199 - delete
	assert.Nil(t, prov.Publish(context.Background(), nil, nil))

	// TODO: Issue 200 - delete
	assert.Nil(t, prov.PublishOne(context.Background(), nil))

	// TODO: Issue 198 - delete
	assert.Nil(t, prov.Subscribe(context.Background(), nil, nil))

	// TODO: Issue ... - delete
	assert.Nil(t, prov.Ack(context.Background(), ""))

	// TODO: Issue ... - delete
	assert.Nil(t, prov.Nack(context.Background(), ""))

	// TODO: Issue 195 - delete
	assert.Nil(t, prov.Retry(context.Background(), nil, "", 0))

	// TODO: Issue 196 - delete
	assert.Nil(t, prov.DeadLetter(context.Background(), nil, ""))

	// TODO: Issue 197 - delete
	assert.Empty(t, prov.SupportedSourceOptions())

	// TODO: Issue 194 - delete
	assert.Empty(t, prov.Stats().Clients)

	// TODO: Issue 193 - delete
	assert.Equal(t, &pb.SourceStats{}, prov.SourceStats(context.Background(), nil))
}

func Test_sleepRandomReconnect(t *testing.T) {
	start := time.Now()

	sleepRandomReconnect()

	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
	assert.LessOrEqual(t, elapsed, time.Duration(provider.ReconnectDelay+100)*time.Millisecond)
}
