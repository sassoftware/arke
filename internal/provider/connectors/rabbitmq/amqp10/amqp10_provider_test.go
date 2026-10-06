// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync/atomic"
	"testing"
	"time"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/provider/connectors/amqp"
	"github.com/sassoftware/arke/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"
)

type rabbitMQAMQP10ProviderAddr struct {
	addr string
}

func (a rabbitMQAMQP10ProviderAddr) Network() string {
	return "tcp"
}

func (a rabbitMQAMQP10ProviderAddr) String() string {
	return a.addr
}

func newTestProviderContext(t *testing.T, clientName string) (context.Context, string) {
	t.Helper()

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: rabbitMQAMQP10ProviderAddr{addr: fmt.Sprintf("%s-%d", clientName, time.Now().UnixNano())}})
	clientIdentifier, err := util.SetClientIdentifier(ctx, clientName)
	require.NoError(t, err)
	t.Cleanup(func() {
		util.RemoveClientIdentifier(ctx)
	})

	return ctx, clientIdentifier
}

func newTestRabbitMQAMQP10Provider() *rabbitMQAMQP10Provider {
	return &rabbitMQAMQP10Provider{
		connections: util.NewConcurrentMap(),
	}
}

func Test_NewAMQP10Provider(t *testing.T) {
	t.Run("initializes provider", func(t *testing.T) {
		t.Setenv(provider.TrustedCerts, "")

		prov := NewRabbitMQAMQP10Provider()

		require.IsType(t, &rabbitMQAMQP10Provider{}, prov)
		amqp10Prov := prov.(*rabbitMQAMQP10Provider)
		assert.NotNil(t, amqp10Prov.connections)
		assert.Equal(t, 0, amqp10Prov.connections.Length())
	})

	t.Run("ignores unreadable CA bundle", func(t *testing.T) {
		t.Setenv(provider.TrustedCerts, "does-not-exist.pem")

		prov := NewRabbitMQAMQP10Provider()

		require.IsType(t, &rabbitMQAMQP10Provider{}, prov)
		amqp10Prov := prov.(*rabbitMQAMQP10Provider)
		assert.NotNil(t, amqp10Prov.connections)
	})
}

func Test_amqp10provider_getBrokerDetails(t *testing.T) {
	t.Run("returns broker details for client identifier", func(t *testing.T) {
		ctx, clientIdentifier := newTestProviderContext(t, "get-broker-details")
		prov := newTestRabbitMQAMQP10Provider()
		bd := &BrokerDetails{ClientIdentifier: clientIdentifier}
		prov.connections.Add(clientIdentifier, bd)

		got, err := prov.getBrokerDetails(ctx)

		require.NoError(t, err)
		assert.Same(t, bd, got)
	})

	t.Run("returns error when client identifier is missing", func(t *testing.T) {
		prov := newTestRabbitMQAMQP10Provider()

		got, err := prov.getBrokerDetails(context.Background())

		assert.Nil(t, got)
		assert.EqualError(t, err, "could not retrieve client-id from context")
	})

	t.Run("returns error when broker details are missing", func(t *testing.T) {
		ctx, clientIdentifier := newTestProviderContext(t, "missing-broker-details")
		prov := newTestRabbitMQAMQP10Provider()

		got, err := prov.getBrokerDetails(ctx)

		assert.Nil(t, got)
		assert.EqualError(t, err, fmt.Sprintf("broker details not found for client identifier: %s", clientIdentifier))
	})
}

func Test_amqp10provider_getBrokerDetailsByIdentifier(t *testing.T) {
	prov := newTestRabbitMQAMQP10Provider()
	brokerDetails := &BrokerDetails{ClientIdentifier: "client"}
	prov.connections.Add("client", brokerDetails)

	t.Run("returns broker details for an existing identifier", func(t *testing.T) {
		assert.Same(t, brokerDetails, prov.getBrokerDetailsByIdentifier("client"))
	})

	t.Run("returns nil for a missing identifier", func(t *testing.T) {
		assert.Nil(t, prov.getBrokerDetailsByIdentifier("missing"))
	})
}

type rabbitMQAMQP10EnvironmentCall struct {
	ctx       context.Context
	tlsConfig *tls.Config
	cf        *pb.ConnectionConfiguration
	connURL   string
	options   *rabbitmqamqp.AmqpConnOptions
}

func mockSpyAmqp10Environment(t *testing.T) *rabbitMQAMQP10EnvironmentCall {
	t.Helper()

	gotCall := &rabbitMQAMQP10EnvironmentCall{}
	conn := &rabbitMQAMQP10ConnectionMock{}
	conn.On("WatchConnection", mock.Anything).Return(nil).Once()
	env := &rabbitMQAMQP10EnvironmentMock{}
	env.On("NewConnection", mock.Anything).Return(conn, nil).Once()
	originalNewAmqp10Environment := newRabbitMQAMQP10EnvironmentFunc
	newRabbitMQAMQP10EnvironmentFunc = func(ctx context.Context, cf *pb.ConnectionConfiguration, tlsConfig *tls.Config) (rabbitMQAMQP10EnvironmentShim, error) {
		options, _ := getRabbitMQAMQP10ConnOptions(ctx, cf, tlsConfig)
		gotCall.ctx = ctx
		gotCall.cf = cf
		gotCall.tlsConfig = tlsConfig
		gotCall.connURL = amqp.GetConnURL(cf)
		gotCall.options = options
		return env, nil
	}
	t.Cleanup(func() {
		newRabbitMQAMQP10EnvironmentFunc = originalNewAmqp10Environment
		conn.AssertExpectations(t)
		env.AssertExpectations(t)
	})

	return gotCall
}

func newTestCABundle(t *testing.T) string {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	certificateTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "arke-test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, certificateTemplate, certificateTemplate, publicKey, privateKey)
	require.NoError(t, err)
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})

	caBundlePath := t.TempDir() + "/ca.pem"
	require.NoError(t, os.WriteFile(caBundlePath, certificatePEM, 0600))
	return caBundlePath
}

func Test_amqp10provider_Connect(t *testing.T) {
	t.Run("rejects nil connection configuration", func(t *testing.T) {
		prov := newTestRabbitMQAMQP10Provider()

		err := prov.Connect(context.Background(), nil, false)

		require.NotNil(t, err)
		assert.Equal(t, "connection configuration is required", err.GetMessage())
	})

	t.Run("rejects missing broker credentials", func(t *testing.T) {
		ctx, _ := newTestProviderContext(t, "connect-missing-credentials")
		prov := newTestRabbitMQAMQP10Provider()
		config := newTestConnectionConfig()
		config.Credentials = nil

		err := prov.Connect(ctx, config, false)

		require.NotNil(t, err)
		assert.Equal(t, "missing broker credentials", err.GetMessage())
	})

	t.Run("rejects missing client identifier", func(t *testing.T) {
		prov := newTestRabbitMQAMQP10Provider()

		err := prov.Connect(context.Background(), newTestConnectionConfig(), false)

		require.NotNil(t, err)
		assert.Equal(t, "could not retrieve client-id from context", err.GetMessage())
	})

	t.Run("propagates environment construction error", func(t *testing.T) {
		ctx, _ := newTestProviderContext(t, "connect-environment-error")
		prov := newTestRabbitMQAMQP10Provider()
		originalFactory := newRabbitMQAMQP10EnvironmentFunc
		t.Cleanup(func() { newRabbitMQAMQP10EnvironmentFunc = originalFactory })
		newRabbitMQAMQP10EnvironmentFunc = func(context.Context, *pb.ConnectionConfiguration, *tls.Config) (rabbitMQAMQP10EnvironmentShim, error) {
			return nil, fmt.Errorf("environment setup failed")
		}

		err := prov.Connect(ctx, newTestConnectionConfig(), false)

		require.NotNil(t, err)
		assert.Equal(t, "environment setup failed", err.GetMessage())
	})

	t.Run("propagates broker connection error", func(t *testing.T) {
		ctx, _ := newTestProviderContext(t, "connect-broker-error")
		prov := newTestRabbitMQAMQP10Provider()
		connectionErr := fmt.Errorf("broker connection failed")
		env := &rabbitMQAMQP10EnvironmentMock{}
		env.On("NewConnection", ctx).Return(nil, connectionErr).Once()
		originalFactory := newRabbitMQAMQP10EnvironmentFunc
		t.Cleanup(func() {
			newRabbitMQAMQP10EnvironmentFunc = originalFactory
			env.AssertExpectations(t)
		})
		newRabbitMQAMQP10EnvironmentFunc = func(context.Context, *pb.ConnectionConfiguration, *tls.Config) (rabbitMQAMQP10EnvironmentShim, error) {
			return env, nil
		}

		err := prov.Connect(ctx, newTestConnectionConfig(), false)

		require.NotNil(t, err)
		assert.Equal(t, connectionErr.Error(), err.GetMessage())
		assert.False(t, prov.ClientExists("connect-broker-error"))
	})

	t.Run("reuses an existing live connection", func(t *testing.T) {
		ctx, clientIdentifier := newTestProviderContext(t, "connect-existing")
		prov := newTestRabbitMQAMQP10Provider()
		conn := &rabbitMQAMQP10ConnectionMock{}
		conn.On("IsClosed").Return(false).Once()
		prov.connections.Add(clientIdentifier, &BrokerDetails{ClientIdentifier: clientIdentifier, Connection: conn})

		err := prov.Connect(ctx, newTestConnectionConfig(), false)

		require.Nil(t, err)
		conn.AssertExpectations(t)
	})

	t.Run("non TLS connect does not create TLS config when CA bundle is configured", func(t *testing.T) {
		t.Setenv(provider.TrustedCerts, newTestCABundle(t))
		ctx, _ := newTestProviderContext(t, "connect-non-tls")
		prov := newTestRabbitMQAMQP10Provider()
		config := newTestConnectionConfig()
		gotCall := mockSpyAmqp10Environment(t)

		err := prov.Connect(ctx, config, true)

		require.Nil(t, err)
		assert.Nil(t, gotCall.tlsConfig)
	})

	t.Run("TLS connect creates verifying TLS config", func(t *testing.T) {
		ctx, _ := newTestProviderContext(t, "connect-tls")
		prov := newTestRabbitMQAMQP10Provider()
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
		prov := newTestRabbitMQAMQP10Provider()
		config := newTestConnectionConfig()
		config.Tls = true
		gotCall := mockSpyAmqp10Environment(t)

		err := prov.Connect(ctx, config, true)

		require.Nil(t, err)
		require.NotNil(t, gotCall.tlsConfig)
		assert.True(t, gotCall.tlsConfig.InsecureSkipVerify)
	})

	t.Run("TLS connect loads CA bundle into TLS config", func(t *testing.T) {
		t.Setenv(provider.TrustedCerts, newTestCABundle(t))
		ctx, _ := newTestProviderContext(t, "connect-tls-ca")
		prov := newTestRabbitMQAMQP10Provider()
		config := newTestConnectionConfig()
		config.Tls = true
		gotCall := mockSpyAmqp10Environment(t)

		err := prov.Connect(ctx, config, false)

		require.Nil(t, err)
		require.NotNil(t, gotCall.tlsConfig)
		require.NotNil(t, gotCall.tlsConfig.RootCAs)
		assert.Len(t, gotCall.tlsConfig.RootCAs.Subjects(), 1)
		assert.False(t, gotCall.tlsConfig.InsecureSkipVerify)
	})
}

func Test_amqp10provider_ClientExists(t *testing.T) {
	prov := newTestRabbitMQAMQP10Provider()
	prov.connections.Add("client", &BrokerDetails{})

	assert.True(t, prov.ClientExists("client"))
	assert.False(t, prov.ClientExists("missing"))
}

func Test_amqp10provider_Disconnect(t *testing.T) {
	t.Run("does nothing when broker details are missing", func(t *testing.T) {
		prov := newTestRabbitMQAMQP10Provider()

		assert.NotPanics(t, func() {
			prov.Disconnect(context.Background())
		})
	})

	t.Run("closes connection and removes matching broker details", func(t *testing.T) {
		ctx, clientIdentifier := newTestProviderContext(t, "disconnect")
		prov := newTestRabbitMQAMQP10Provider()
		conn := &rabbitMQAMQP10ConnectionMock{}
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
		prov := newTestRabbitMQAMQP10Provider()

		assert.False(t, prov.WaitForConnect(context.Background()))
	})

	t.Run("returns true when broker details are connected", func(t *testing.T) {
		ctx, clientIdentifier := newTestProviderContext(t, "wait-for-connect")
		prov := newTestRabbitMQAMQP10Provider()
		bd := &BrokerDetails{ClientIdentifier: clientIdentifier}
		bd.state.Store(provider.CONNECTED)
		prov.connections.Add(clientIdentifier, bd)

		assert.True(t, prov.WaitForConnect(ctx))
		assert.Equal(t, int64(0), bd.ActiveStreams)
	})

	t.Run("returns false when broker details disappear while waiting", func(t *testing.T) {
		ctx, clientIdentifier := newTestProviderContext(t, "wait-disconnect")
		prov := newTestRabbitMQAMQP10Provider()
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
	prov := newTestRabbitMQAMQP10Provider()

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

	// TODO: Issue 194 - delete
	assert.Empty(t, prov.Stats().Clients)

	// TODO: Issue 193 - delete
	assert.NotNil(t, prov.SourceStats(context.Background(), nil))
}

func Test_SupportedSourceOptions(t *testing.T) {
	prov := NewRabbitMQAMQP10Provider()
	opts := prov.SupportedSourceOptions()
	assert.NotNil(t, opts)
	expected := map[string]bool{
		"MessageTTL":        true,
		"DeadLetterAddress": true,
		"DeadLetterSubject": true,
		"Expires":           true,
		"Offset":            true,
		"ConsumerGroup":     true,
	}

	assert.Equal(t, expected, opts)
}

func Test_SupportedStreamSourceOptions(t *testing.T) {
	assert.NotNil(t, supportedStreamSourceOptions)
	expected := map[string]bool{
		"Offset":        true,
		"MessageTTL":    true,
		"ConsumerGroup": true,
	}

	assert.Equal(t, expected, supportedStreamSourceOptions)
}

const testExchangeName = "exchange"

func Test_addressToExchangeSpecification(t *testing.T) {
	tests := []struct {
		name         string
		addressType  pb.Address_TargetType
		expectedType interface{}
	}{
		{name: "topic", addressType: pb.Address_TOPIC, expectedType: &rabbitmqamqp.TopicExchangeSpecification{}},
		{name: "headers", addressType: pb.Address_FILTER, expectedType: &rabbitmqamqp.HeadersExchangeSpecification{}},
		{name: "direct", addressType: pb.Address_QUEUE, expectedType: &rabbitmqamqp.DirectExchangeSpecification{}},
		{name: "stream", addressType: pb.Address_STREAM, expectedType: &rabbitmqamqp.CustomExchangeSpecification{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			address := &pb.Address{Name: testExchangeName, Type: test.addressType, AutoDelete: true}
			specification, err := addressToExchangeSpecification(address)

			require.NoError(t, err)
			require.IsType(t, test.expectedType, specification)
			assert.Equal(t, testExchangeName, exchangeSpecificationName(specification))
			assert.True(t, exchangeSpecificationAutoDelete(specification))
			if custom, ok := specification.(*rabbitmqamqp.CustomExchangeSpecification); ok {
				assert.Equal(t, "stream", custom.ExchangeTypeName)
			}
		})
	}

	_, err := addressToExchangeSpecification(&pb.Address{Name: testExchangeName, Type: pb.Address_TargetType(99)})
	assert.EqualError(t, err, "99 is not a valid address type")
}

func exchangeSpecificationName(specification rabbitmqamqp.IExchangeSpecification) string {
	switch specification := specification.(type) {
	case *rabbitmqamqp.TopicExchangeSpecification:
		return specification.Name
	case *rabbitmqamqp.HeadersExchangeSpecification:
		return specification.Name
	case *rabbitmqamqp.DirectExchangeSpecification:
		return specification.Name
	case *rabbitmqamqp.CustomExchangeSpecification:
		return specification.Name
	default:
		return ""
	}
}

func exchangeSpecificationAutoDelete(specification rabbitmqamqp.IExchangeSpecification) bool {
	switch specification := specification.(type) {
	case *rabbitmqamqp.TopicExchangeSpecification:
		return specification.IsAutoDelete
	case *rabbitmqamqp.HeadersExchangeSpecification:
		return specification.IsAutoDelete
	case *rabbitmqamqp.DirectExchangeSpecification:
		return specification.IsAutoDelete
	case *rabbitmqamqp.CustomExchangeSpecification:
		return specification.IsAutoDelete
	default:
		return false
	}
}

func Test_amqp10provider_declareExchange(t *testing.T) {
	address := &pb.Address{Name: testExchangeName, Type: pb.Address_TOPIC}
	management := &rabbitMQAMQP10ManagementMock{}
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("Management").Return(management)
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	management.On("DeclareExchange", bd.ctx, mock.AnythingOfType("*rabbitmqamqp.TopicExchangeSpecification")).Return(nil, nil).Once()

	require.NoError(t, prov.declareExchange(address, bd))
	assert.True(t, bd.exchangeExists(address.GetName()))
	management.AssertNumberOfCalls(t, "DeclareExchange", 1)
}

func Test_amqp10provider_declareExchangeSkipsReservedAndKnown(t *testing.T) {
	management := &rabbitMQAMQP10ManagementMock{}
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("Management").Return(management)
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()

	require.NoError(t, prov.declareExchange(&pb.Address{Name: "amq.direct", Type: pb.Address_QUEUE}, bd))
	bd.entityTracker().AddExchange("known")
	require.NoError(t, prov.declareExchange(&pb.Address{Name: "known", Type: pb.Address_TOPIC}, bd))
	management.AssertNotCalled(t, "DeclareExchange", mock.Anything, mock.Anything)
}

func Test_amqp10provider_declareExchangeRetriesAfterFailure(t *testing.T) {
	management := &rabbitMQAMQP10ManagementMock{}
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("Management").Return(management)
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	management.On("DeclareExchange", bd.ctx, mock.Anything).Return(nil, errors.New("exchange failed")).Twice()

	assert.EqualError(t, prov.declareExchange(&pb.Address{Name: "failed", Type: pb.Address_TOPIC}, bd), "exchange failed")
	assert.EqualError(t, prov.declareExchange(&pb.Address{Name: "failed", Type: pb.Address_TOPIC}, bd), "exchange failed")
	assert.False(t, bd.exchangeExists("failed"))
	management.AssertNumberOfCalls(t, "DeclareExchange", 2)
}

func Test_amqp10provider_declareExchangeAcceptsConcurrentSuccess(t *testing.T) {
	management := &rabbitMQAMQP10ManagementMock{}
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("Management").Return(management)
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	management.On("DeclareExchange", bd.ctx, mock.Anything).Run(func(mock.Arguments) {
		bd.entityTracker().AddExchange("race")
	}).Return(nil, errors.New("exchange failed")).Once()

	require.NoError(t, prov.declareExchange(&pb.Address{Name: "race", Type: pb.Address_TOPIC}, bd))
	management.AssertNumberOfCalls(t, "DeclareExchange", 1)
}

func Test_amqp10provider_declareExchangeReturnsPreconditionFailure(t *testing.T) {
	management := &rabbitMQAMQP10ManagementMock{}
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("Management").Return(management)
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	management.On("DeclareExchange", bd.ctx, mock.Anything).Run(func(mock.Arguments) {
		bd.entityTracker().AddExchange("incompatible")
	}).Return(nil, rabbitmqamqp.ErrPreconditionFailed).Once()

	err := prov.declareExchange(&pb.Address{Name: "incompatible", Type: pb.Address_TOPIC}, bd)

	require.ErrorIs(t, err, rabbitmqamqp.ErrPreconditionFailed)
	assert.True(t, bd.exchangeExists("incompatible"))
	management.AssertNumberOfCalls(t, "DeclareExchange", 1)
}

func Test_amqp10provider_declareExchangeConnectionIsolation(t *testing.T) {
	managementOne := &rabbitMQAMQP10ManagementMock{}
	connectionOne := &rabbitMQAMQP10ConnectionMock{}
	connectionOne.On("Management").Return(managementOne)
	bdOne := &BrokerDetails{ctx: context.Background(), Connection: connectionOne}
	managementOne.On("DeclareExchange", bdOne.ctx, mock.Anything).Return(nil, nil).Once()

	managementTwo := &rabbitMQAMQP10ManagementMock{}
	connectionTwo := &rabbitMQAMQP10ConnectionMock{}
	connectionTwo.On("Management").Return(managementTwo)
	bdTwo := &BrokerDetails{ctx: context.Background(), Connection: connectionTwo}
	managementTwo.On("DeclareExchange", bdTwo.ctx, mock.Anything).Return(nil, nil).Once()

	prov := newTestRabbitMQAMQP10Provider()
	require.NoError(t, prov.declareExchange(&pb.Address{Name: "shared", Type: pb.Address_TOPIC}, bdOne))
	assert.False(t, bdTwo.exchangeExists("shared"))
	require.NoError(t, prov.declareExchange(&pb.Address{Name: "shared", Type: pb.Address_TOPIC}, bdTwo))
	assert.True(t, bdOne.exchangeExists("shared"))
	assert.True(t, bdTwo.exchangeExists("shared"))
}
