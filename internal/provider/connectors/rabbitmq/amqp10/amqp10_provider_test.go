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
	"net/http"
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

const (
	testExchangeName           = "exchange"
	testBindingSubjectOne      = "one"
	testBindingSubjectTwo      = "two"
	testBindingSubjectKey      = "key"
	testBindingSubjectCreated  = "created"
	testParentAddressSubject   = "subject"
	testBindingMatchKind       = "kind"
	testBindingMatchEvent      = "event"
	testBindingMatchRegion     = "region"
	testBindingMatchWest       = "west"
	testBindingMatchAny        = "any"
	testBindingMatchAll        = "all"
	testBindingQuorumQueueName = "orders.quorum"
	testBindingStreamName      = "events"
	testBindingQueueName       = "orders"
	testBindingTemporaryQueue  = "binding-queue"
	testBindingExchangeName    = "orders-exchange"
	testBindingCallExchange    = "exchange"
	testBindingCallQueue       = "queue-declared"
	testBindingCallBinding     = "binding"
	testCleanupQueueName       = "cleanup-queue"
	testCleanupExchangeName    = "cleanup-exchange"
	testCleanupTenant          = "tenant-a"
	testCleanupBindingPath     = "/api/bindings/tenant-a/e/cleanup-exchange/q/cleanup-queue"
	testCleanupStaleProperties = "stale-key"
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

func newTestCABundle(t *testing.T) (string, *x509.Certificate) {
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
	certificate, err := x509.ParseCertificate(certificateDER)
	require.NoError(t, err)
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})

	caBundlePath := t.TempDir() + "/ca.pem"
	require.NoError(t, os.WriteFile(caBundlePath, certificatePEM, 0600))
	return caBundlePath, certificate
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
		caBundlePath, _ := newTestCABundle(t)
		t.Setenv(provider.TrustedCerts, caBundlePath)
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
		caBundlePath, caCertificate := newTestCABundle(t)
		t.Setenv(provider.TrustedCerts, caBundlePath)
		ctx, _ := newTestProviderContext(t, "connect-tls-ca")
		prov := newTestRabbitMQAMQP10Provider()
		config := newTestConnectionConfig()
		config.Tls = true
		gotCall := mockSpyAmqp10Environment(t)

		err := prov.Connect(ctx, config, false)

		require.Nil(t, err)
		require.NotNil(t, gotCall.tlsConfig)
		require.NotNil(t, gotCall.tlsConfig.RootCAs)
		_, verifyErr := caCertificate.Verify(x509.VerifyOptions{
			Roots:     gotCall.tlsConfig.RootCAs,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		})
		require.NoError(t, verifyErr)
		assert.False(t, gotCall.tlsConfig.InsecureSkipVerify)
	})
}

func Test_amqp10provider_ClientExists(t *testing.T) {
	prov := newTestRabbitMQAMQP10Provider()
	connected := &BrokerDetails{}
	connected.state.Store(provider.CONNECTED)
	prov.connections.Add("client", connected)

	assert.True(t, prov.ClientExists("client"))
	assert.False(t, prov.ClientExists("missing"))

	t.Run("returns false for a closed broker detail", func(t *testing.T) {
		closed := &BrokerDetails{}
		closed.state.Store(provider.CLOSED)
		prov.connections.Add("closed", closed)

		assert.False(t, prov.ClientExists("closed"))
	})

	t.Run("returns true while reconnecting", func(t *testing.T) {
		reconnecting := &BrokerDetails{}
		reconnecting.state.Store(provider.CONNECTING)
		prov.connections.Add("reconnecting", reconnecting)

		assert.True(t, prov.ClientExists("reconnecting"))
	})
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
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	connection.On("DeclareExchange", bd.ctx, mock.AnythingOfType("*rabbitmqamqp.TopicExchangeSpecification")).Return(nil, nil).Once()

	require.NoError(t, prov.declareExchange(address, bd))
	assert.True(t, bd.exchangeExists(address.GetName()))
	connection.AssertNumberOfCalls(t, "DeclareExchange", 1)
}

func Test_amqp10provider_declareExchangeReturnsInvalidAddressType(t *testing.T) {
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	address := &pb.Address{Name: testExchangeName, Type: pb.Address_TargetType(99)}

	err := prov.declareExchange(address, bd)

	assert.EqualError(t, err, "99 is not a valid address type")
	connection.AssertNotCalled(t, "DeclareExchange", mock.Anything, mock.Anything)
}

func Test_amqp10provider_declareExchangeSkipsReservedAndKnown(t *testing.T) {
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()

	require.NoError(t, prov.declareExchange(&pb.Address{Name: "amq.direct", Type: pb.Address_QUEUE}, bd))
	bd.entityTracker().AddExchange("known")
	require.NoError(t, prov.declareExchange(&pb.Address{Name: "known", Type: pb.Address_TOPIC}, bd))
	connection.AssertNotCalled(t, "DeclareExchange", mock.Anything, mock.Anything)
}

func Test_amqp10provider_declareExchangeRetriesAfterFailure(t *testing.T) {
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	connection.On("DeclareExchange", bd.ctx, mock.Anything).Return(nil, errors.New("exchange failed")).Twice()

	assert.EqualError(t, prov.declareExchange(&pb.Address{Name: "failed", Type: pb.Address_TOPIC}, bd), "exchange failed")
	assert.EqualError(t, prov.declareExchange(&pb.Address{Name: "failed", Type: pb.Address_TOPIC}, bd), "exchange failed")
	assert.False(t, bd.exchangeExists("failed"))
	connection.AssertNumberOfCalls(t, "DeclareExchange", 2)
}

func Test_amqp10provider_declareExchangeAcceptsConcurrentSuccess(t *testing.T) {
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	connection.On("DeclareExchange", bd.ctx, mock.Anything).Run(func(mock.Arguments) {
		bd.entityTracker().AddExchange("race")
	}).Return(nil, errors.New("exchange failed")).Once()

	require.NoError(t, prov.declareExchange(&pb.Address{Name: "race", Type: pb.Address_TOPIC}, bd))
	connection.AssertNumberOfCalls(t, "DeclareExchange", 1)
}

func Test_amqp10provider_subscribeDeclaresExchange(t *testing.T) {
	ctx, clientIdentifier := newTestProviderContext(t, "subscribe-declare")
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{ctx: ctx, Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	prov.connections.Add(clientIdentifier, bd)
	source := &pb.Source{Name: "subscribe", Type: pb.Source_QUEUE, Address: &pb.Address{Name: "subscribe", Type: pb.Address_TOPIC}}
	connection.On("DeclareExchange", bd.ctx, mock.Anything).Return(nil, nil).Once()
	connection.On("DeclareQueue", bd.ctx, mock.Anything).Return(nil, nil).Once()

	err := prov.Subscribe(ctx, source, nil)

	require.Nil(t, err)
	assert.True(t, bd.exchangeExists(source.GetAddress().GetName()))
	assert.True(t, bd.queueExists("subscribe.quorum"))
	connection.AssertNumberOfCalls(t, "DeclareExchange", 1)
	connection.AssertNumberOfCalls(t, "DeclareQueue", 1)
}

func Test_amqp10provider_subscribeReturnsDeclarationError(t *testing.T) {
	ctx, clientIdentifier := newTestProviderContext(t, "subscribe-declare-error")
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{ctx: ctx, Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	prov.connections.Add(clientIdentifier, bd)
	source := &pb.Source{Address: &pb.Address{Name: "subscribe-error", Type: pb.Address_TOPIC}}
	connection.On("DeclareExchange", bd.ctx, mock.Anything).Return(nil, errors.New("exchange failed")).Once()

	err := prov.Subscribe(ctx, source, nil)

	require.Equal(t, "exchange failed", err.GetMessage())
	assert.False(t, bd.exchangeExists(source.GetAddress().GetName()))
	connection.AssertNumberOfCalls(t, "DeclareExchange", 1)
}

func Test_amqp10provider_subscribeSkipsKnownExchange(t *testing.T) {
	ctx, clientIdentifier := newTestProviderContext(t, "subscribe-known-exchange")
	bd := &BrokerDetails{}
	bd.entityTracker().AddExchange("known")
	prov := newTestRabbitMQAMQP10Provider()
	prov.connections.Add(clientIdentifier, bd)
	source := &pb.Source{Name: "known", Type: pb.Source_QUEUE, Address: &pb.Address{Name: "known", Type: pb.Address_TOPIC}}
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd.Connection = connection
	bd.entityTracker().AddQueue("known.quorum")
	connection.On("DeclareQueue", bd.ctx, mock.Anything).Return(nil, nil).Once()

	require.Nil(t, prov.Subscribe(ctx, source, nil))
	assert.True(t, bd.exchangeExists("known"))
	connection.AssertNumberOfCalls(t, "DeclareQueue", 1)
}

func Test_amqp10provider_declareExchangeAcceptsPreconditionFailure(t *testing.T) {
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	connection.On("DeclareExchange", bd.ctx, mock.Anything).Return(nil, rabbitmqamqp.ErrPreconditionFailed).Once()

	err := prov.declareExchange(&pb.Address{Name: "incompatible", Type: pb.Address_TOPIC}, bd)

	require.NoError(t, err)
	assert.True(t, bd.exchangeExists("incompatible"))
	connection.AssertNumberOfCalls(t, "DeclareExchange", 1)
}

func Test_amqp10provider_declareExchangeConnectionIsolation(t *testing.T) {
	connectionOne := &rabbitMQAMQP10ConnectionMock{}
	bdOne := &BrokerDetails{ctx: context.Background(), Connection: connectionOne}
	connectionOne.On("DeclareExchange", bdOne.ctx, mock.Anything).Return(nil, nil).Once()

	connectionTwo := &rabbitMQAMQP10ConnectionMock{}
	bdTwo := &BrokerDetails{ctx: context.Background(), Connection: connectionTwo}
	connectionTwo.On("DeclareExchange", bdTwo.ctx, mock.Anything).Return(nil, nil).Once()

	prov := newTestRabbitMQAMQP10Provider()
	require.NoError(t, prov.declareExchange(&pb.Address{Name: "shared", Type: pb.Address_TOPIC}, bdOne))
	assert.False(t, bdTwo.exchangeExists("shared"))
	require.NoError(t, prov.declareExchange(&pb.Address{Name: "shared", Type: pb.Address_TOPIC}, bdTwo))
	assert.True(t, bdOne.exchangeExists("shared"))
	assert.True(t, bdTwo.exchangeExists("shared"))
}

func Test_queueSpecificationClassic(t *testing.T) {
	source := &pb.Source{
		Name:                 "temporary",
		Type:                 pb.Source_TEMPORARY,
		AutoDelete:           true,
		Exclusive:            true,
		SingleActiveConsumer: true,
		Options: map[string]string{
			"MessageTTL":        "1000",
			"Expires":           "2000",
			"DeadLetterAddress": "dead-letter",
			"DeadLetterSubject": "subject",
		},
	}

	specification, err := queueSpecification(source)

	require.NoError(t, err)
	classic, ok := specification.(*rabbitmqamqp.ClassicQueueSpecification)
	require.True(t, ok)
	assert.Equal(t, "temporary", classic.Name)
	assert.False(t, classic.IsAutoDelete)
	assert.False(t, classic.IsExclusive)
	assert.EqualValues(t, 1000, classic.MessageTTL)
	assert.EqualValues(t, 2000, classic.AutoExpire)
	assert.True(t, classic.SingleActiveConsumer)
	assert.Equal(t, "dead-letter", classic.DeadLetterExchange)
	assert.Equal(t, "subject", classic.DeadLetterRoutingKey)
}

func Test_queueSpecificationQuorumAndExpiryFallback(t *testing.T) {
	source := &pb.Source{Name: "orders", Type: pb.Source_QUEUE, AutoDelete: false}

	specification, err := queueSpecification(source)

	require.NoError(t, err)
	quorum, ok := specification.(*rabbitmqamqp.QuorumQueueSpecification)
	require.True(t, ok)
	assert.Equal(t, "orders.quorum", quorum.Name)
	assert.EqualValues(t, 0, quorum.AutoExpire)

	source.AutoDelete = true
	source.Name = "temporary"
	specification, err = queueSpecification(source)

	require.NoError(t, err)
	classic, ok := specification.(*rabbitmqamqp.ClassicQueueSpecification)
	require.True(t, ok)
	assert.EqualValues(t, (5 * time.Minute).Milliseconds(), classic.AutoExpire)

	source.AutoDelete = false
	source.Exclusive = true
	specification, err = queueSpecification(source)
	require.NoError(t, err)
	quorum, ok = specification.(*rabbitmqamqp.QuorumQueueSpecification)
	require.True(t, ok)
	assert.EqualValues(t, (5 * time.Minute).Milliseconds(), quorum.AutoExpire)

	source.Options = map[string]string{"Expires": "0"}
	specification, err = queueSpecification(source)
	require.NoError(t, err)
	quorum, ok = specification.(*rabbitmqamqp.QuorumQueueSpecification)
	require.True(t, ok)
	assert.Zero(t, quorum.AutoExpire)
}

func Test_queueSpecificationRejectsUnsupportedAndInvalidOptions(t *testing.T) {
	_, err := queueSpecification(&pb.Source{Type: pb.Source_QUEUE, Options: map[string]string{"Offset": "1"}})
	assert.EqualError(t, err, "Offset is an unsupported source option")

	_, err = queueSpecification(&pb.Source{Type: pb.Source_STREAM})
	assert.EqualError(t, err, "stream sources are not supported for queue declaration")

	_, err = queueSpecification(&pb.Source{Type: pb.Source_TargetType(99)})
	assert.EqualError(t, err, "source type 99 is not supported for queue declaration")

	_, err = queueSpecification(&pb.Source{Type: pb.Source_QUEUE, Options: map[string]string{"MessageTTL": "invalid"}})
	assert.EqualError(t, err, "value for MessageTTL option must be a quoted integer")

	_, err = queueSpecification(&pb.Source{Type: pb.Source_QUEUE, Options: map[string]string{"Expires": "invalid"}})
	assert.EqualError(t, err, "value for Expires option must be a quoted integer")
}

func Test_amqp10provider_declareQueueReturnsBrokerError(t *testing.T) {
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	connection.On("DeclareQueue", bd.ctx, mock.Anything).Return(nil, errors.New("queue failed")).Once()

	err := prov.declareQueue(&pb.Source{Name: "failed", Type: pb.Source_TEMPORARY}, bd)

	require.EqualError(t, err, "queue failed")
	assert.False(t, bd.queueExists("failed"))
	connection.AssertNumberOfCalls(t, "DeclareQueue", 1)
}

func Test_amqp10provider_declareQueueAcceptsPreconditionFailure(t *testing.T) {
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	connection.On("DeclareQueue", bd.ctx, mock.Anything).Return(nil, rabbitmqamqp.ErrPreconditionFailed).Once()

	err := prov.declareQueue(&pb.Source{Name: "already-exists", Type: pb.Source_TEMPORARY}, bd)

	require.NoError(t, err)
	assert.True(t, bd.queueExists("already-exists"))
	connection.AssertNumberOfCalls(t, "DeclareQueue", 1)
}

func Test_amqp10provider_subscribeDeclaresQueueAfterExchange(t *testing.T) {
	ctx, clientIdentifier := newTestProviderContext(t, "subscribe-queue-order")
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{ctx: ctx, Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	prov.connections.Add(clientIdentifier, bd)
	source := &pb.Source{Name: "subscribe-queue", Type: pb.Source_QUEUE, DeclareOnly: true, Address: &pb.Address{Name: "queue-address", Type: pb.Address_TOPIC}}
	bd.entityTracker().AddQueue("subscribe-queue.quorum")
	var calls []string
	connection.On("DeclareExchange", bd.ctx, mock.Anything).Run(func(mock.Arguments) {
		calls = append(calls, "exchange")
	}).Return(nil, nil).Once()
	connection.On("DeclareQueue", bd.ctx, mock.Anything).Run(func(mock.Arguments) {
		calls = append(calls, "queue")
	}).Return(nil, nil).Once()

	err := prov.Subscribe(ctx, source, nil)

	require.Nil(t, err)
	assert.Equal(t, []string{"exchange", "queue"}, calls)
	assert.True(t, bd.queueExists("subscribe-queue.quorum"))
}

func Test_amqp10provider_declareQueueRedeclaresKnownAndIsolatesTrackers(t *testing.T) {
	prov := newTestRabbitMQAMQP10Provider()
	connectionOne := &rabbitMQAMQP10ConnectionMock{}
	bdOne := &BrokerDetails{ctx: context.Background(), Connection: connectionOne}
	connectionTwo := &rabbitMQAMQP10ConnectionMock{}
	bdTwo := &BrokerDetails{ctx: context.Background(), Connection: connectionTwo}
	source := &pb.Source{Name: "shared", Type: pb.Source_QUEUE}
	bdOne.entityTracker().AddQueue("shared.quorum")
	connectionOne.On("DeclareQueue", bdOne.ctx, mock.Anything).Return(nil, nil).Once()
	connectionTwo.On("DeclareQueue", bdTwo.ctx, mock.Anything).Return(nil, nil).Once()

	require.NoError(t, prov.declareQueue(source, bdOne))
	assert.False(t, bdTwo.queueExists("shared.quorum"))
	require.NoError(t, prov.declareQueue(source, bdTwo))
	assert.True(t, bdTwo.queueExists("shared.quorum"))
	connectionOne.AssertNumberOfCalls(t, "DeclareQueue", 1)
	connectionTwo.AssertNumberOfCalls(t, "DeclareQueue", 1)
}

func Test_amqp10provider_declareBindings(t *testing.T) {
	tests := []struct {
		name     string
		source   *pb.Source
		expected []rabbitmqamqp.ExchangeToQueueBindingSpecification
	}{
		{
			name: "one subject without filters",
			source: &pb.Source{Name: testBindingTemporaryQueue, Type: pb.Source_TEMPORARY, Address: &pb.Address{
				Name: testExchangeName, Type: pb.Address_TOPIC, Subjects: []string{testBindingSubjectOne},
			}},
			expected: []rabbitmqamqp.ExchangeToQueueBindingSpecification{{
				SourceExchange: testExchangeName, DestinationQueue: testBindingTemporaryQueue, BindingKey: testBindingSubjectOne,
			}},
		},
		{
			name: "multiple subjects without filters",
			source: &pb.Source{Name: testBindingTemporaryQueue, Type: pb.Source_TEMPORARY, Address: &pb.Address{
				Name: testExchangeName, Type: pb.Address_TOPIC, Subjects: []string{testBindingSubjectOne, testBindingSubjectTwo},
			}},
			expected: []rabbitmqamqp.ExchangeToQueueBindingSpecification{
				{SourceExchange: testExchangeName, DestinationQueue: testBindingTemporaryQueue, BindingKey: testBindingSubjectOne},
				{SourceExchange: testExchangeName, DestinationQueue: testBindingTemporaryQueue, BindingKey: testBindingSubjectTwo},
			},
		},
		{
			name: "Filter_ANY uses x-match any",
			source: &pb.Source{Name: testBindingTemporaryQueue, Type: pb.Source_TEMPORARY, Address: &pb.Address{
				Name: testExchangeName, Type: pb.Address_FILTER, Subjects: []string{testBindingSubjectKey},
			}, Filters: []*pb.Filter{{Type: pb.Filter_ANY, Matches: []*pb.Match{{Name: testBindingMatchKind, Value: testBindingMatchEvent}}}}},
			expected: []rabbitmqamqp.ExchangeToQueueBindingSpecification{{
				SourceExchange: testExchangeName, DestinationQueue: testBindingTemporaryQueue, BindingKey: testBindingSubjectKey,
				Arguments: map[string]any{testBindingMatchKind: testBindingMatchEvent, amqp.BindingMatchHeader: testBindingMatchAny},
			}},
		},
		{
			name: "non-empty Filter_ALL uses x-match all",
			source: &pb.Source{Name: testBindingTemporaryQueue, Type: pb.Source_TEMPORARY, Address: &pb.Address{
				Name: testExchangeName, Type: pb.Address_FILTER, Subjects: []string{testBindingSubjectKey},
			}, Filters: []*pb.Filter{{Type: pb.Filter_ALL, Matches: []*pb.Match{{Name: testBindingMatchKind, Value: testBindingMatchEvent}}}}},
			expected: []rabbitmqamqp.ExchangeToQueueBindingSpecification{{
				SourceExchange: testExchangeName, DestinationQueue: testBindingTemporaryQueue, BindingKey: testBindingSubjectKey,
				Arguments: map[string]any{testBindingMatchKind: testBindingMatchEvent, amqp.BindingMatchHeader: testBindingMatchAll},
			}},
		},
		{
			name: "subjects and filters form a cross-product",
			source: &pb.Source{Name: testBindingTemporaryQueue, Type: pb.Source_TEMPORARY, Address: &pb.Address{
				Name: testExchangeName, Type: pb.Address_FILTER, Subjects: []string{testBindingSubjectOne, testBindingSubjectTwo},
			}, Filters: []*pb.Filter{
				{Type: pb.Filter_ANY, Matches: []*pb.Match{{Name: testBindingMatchKind, Value: testBindingMatchEvent}}},
				{Type: pb.Filter_ALL, Matches: []*pb.Match{{Name: testBindingMatchRegion, Value: testBindingMatchWest}}},
			}},
			expected: []rabbitmqamqp.ExchangeToQueueBindingSpecification{
				{SourceExchange: testExchangeName, DestinationQueue: testBindingTemporaryQueue, BindingKey: testBindingSubjectOne, Arguments: map[string]any{testBindingMatchKind: testBindingMatchEvent, amqp.BindingMatchHeader: testBindingMatchAny}},
				{SourceExchange: testExchangeName, DestinationQueue: testBindingTemporaryQueue, BindingKey: testBindingSubjectOne, Arguments: map[string]any{testBindingMatchRegion: testBindingMatchWest, amqp.BindingMatchHeader: testBindingMatchAll}},
				{SourceExchange: testExchangeName, DestinationQueue: testBindingTemporaryQueue, BindingKey: testBindingSubjectTwo, Arguments: map[string]any{testBindingMatchKind: testBindingMatchEvent, amqp.BindingMatchHeader: testBindingMatchAny}},
				{SourceExchange: testExchangeName, DestinationQueue: testBindingTemporaryQueue, BindingKey: testBindingSubjectTwo, Arguments: map[string]any{testBindingMatchRegion: testBindingMatchWest, amqp.BindingMatchHeader: testBindingMatchAll}},
			},
		},
		{
			name: "filters without subjects use an empty binding key",
			source: &pb.Source{Name: testBindingTemporaryQueue, Type: pb.Source_TEMPORARY, Address: &pb.Address{
				Name: testExchangeName, Type: pb.Address_FILTER,
			}, Filters: []*pb.Filter{{Type: pb.Filter_ALL, Matches: []*pb.Match{{Name: testBindingMatchKind, Value: testBindingMatchEvent}}}}},
			expected: []rabbitmqamqp.ExchangeToQueueBindingSpecification{{
				SourceExchange: testExchangeName, DestinationQueue: testBindingTemporaryQueue, BindingKey: "",
				Arguments: map[string]any{testBindingMatchKind: testBindingMatchEvent, amqp.BindingMatchHeader: testBindingMatchAll},
			}},
		},
		{
			name: "empty filters still create a binding with empty arguments",
			source: &pb.Source{Name: testBindingTemporaryQueue, Type: pb.Source_TEMPORARY, Address: &pb.Address{
				Name: testExchangeName, Type: pb.Address_FILTER,
			}, Filters: []*pb.Filter{{}}},
			expected: []rabbitmqamqp.ExchangeToQueueBindingSpecification{{
				SourceExchange: testExchangeName, DestinationQueue: testBindingTemporaryQueue, BindingKey: "", Arguments: map[string]any{},
			}},
		},
		{
			name: "no subjects and no filters create no binding",
			source: &pb.Source{Name: testBindingTemporaryQueue, Type: pb.Source_TEMPORARY, Address: &pb.Address{
				Name: testExchangeName, Type: pb.Address_FILTER,
			}},
		},
		{
			name: "quorum queue uses declared name",
			source: &pb.Source{Name: testBindingQueueName, Type: pb.Source_QUEUE, Address: &pb.Address{
				Name: testExchangeName, Type: pb.Address_TOPIC, Subjects: []string{testBindingSubjectKey},
			}},
			expected: []rabbitmqamqp.ExchangeToQueueBindingSpecification{{
				SourceExchange: testExchangeName, DestinationQueue: testBindingQuorumQueueName, BindingKey: testBindingSubjectKey,
			}},
		},
		{
			name: "classic queue uses its unmodified name",
			source: &pb.Source{Name: testBindingQueueName, Type: pb.Source_QUEUE, AutoDelete: true, Address: &pb.Address{
				Name: testExchangeName, Type: pb.Address_TOPIC, Subjects: []string{testBindingSubjectKey},
			}},
			expected: []rabbitmqamqp.ExchangeToQueueBindingSpecification{{
				SourceExchange: testExchangeName, DestinationQueue: testBindingQueueName, BindingKey: testBindingSubjectKey,
			}},
		},
		{
			name: "existing stream uses its declared name",
			source: &pb.Source{Name: testBindingStreamName, Type: pb.Source_STREAM, Address: &pb.Address{
				Name: testExchangeName, Type: pb.Address_TOPIC, Subjects: []string{testBindingSubjectKey},
			}},
			expected: []rabbitmqamqp.ExchangeToQueueBindingSpecification{{
				SourceExchange: testExchangeName, DestinationQueue: testBindingStreamName, BindingKey: testBindingSubjectKey,
			}},
		},
		{
			name: "stream address does not create a queue binding",
			source: &pb.Source{Name: testBindingStreamName, Type: pb.Source_STREAM, Address: &pb.Address{
				Name: testBindingStreamName, Type: pb.Address_STREAM, Subjects: []string{testBindingSubjectKey},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connection := &rabbitMQAMQP10ConnectionMock{}
			bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
			var got []rabbitmqamqp.ExchangeToQueueBindingSpecification
			if len(tt.expected) > 0 {
				connection.On("DeclareBind", bd.ctx, mock.Anything).Run(func(args mock.Arguments) {
					binding, ok := args.Get(1).(*rabbitmqamqp.ExchangeToQueueBindingSpecification)
					require.True(t, ok)
					got = append(got, *binding)
				}).Return("binding-path", nil).Times(len(tt.expected))
			}

			err := newTestRabbitMQAMQP10Provider().declareBindings(tt.source, bd)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
			connection.AssertNumberOfCalls(t, "DeclareBind", len(tt.expected))
		})
	}
}

func Test_amqp10provider_cleanupQueueBindings(t *testing.T) {
	t.Run("deletes stale routing keys and retains requested keys despite argument changes", func(t *testing.T) {
		source := &pb.Source{Name: testCleanupQueueName, Type: pb.Source_QUEUE, AutoDelete: true, Address: &pb.Address{
			Name: testCleanupExchangeName, Type: pb.Address_TOPIC,
		}}
		management := &managementClientStub{request: func(_ context.Context, method, _ string) ([]byte, int, error) {
			if method == http.MethodGet {
				return []byte(fmt.Sprintf(`[
					{"routing_key":"keep.one","properties_key":"keep-one","arguments":{"header":"old"}},
					{"routing_key":"keep.two","properties_key":"keep-two"},
					{"routing_key":"stale","properties_key":%q}
				]`, testCleanupStaleProperties)), http.StatusOK, nil
			}
			return nil, http.StatusNoContent, nil
		}}
		bd := &BrokerDetails{
			ctx:              context.Background(),
			mgmtClient:       management,
			connectionConfig: &pb.ConnectionConfiguration{Tenant: testCleanupTenant},
		}

		newTestRabbitMQAMQP10Provider().cleanupQueueBindings(source, source.GetAddress(), []string{"keep.one", "keep.two"}, bd)

		assert.Equal(t, []managementRequest{
			{method: http.MethodGet, path: testCleanupBindingPath},
			{method: http.MethodDelete, path: testCleanupBindingPath + "/" + testCleanupStaleProperties},
		}, management.requests)
	})

	t.Run("changed filter arguments with the same routing key are retained", func(t *testing.T) {
		source := &pb.Source{Name: testCleanupQueueName, Type: pb.Source_QUEUE, AutoDelete: true, Filters: []*pb.Filter{{
			Type: pb.Filter_ALL, Matches: []*pb.Match{{Name: "kind", Value: "new"}},
		}}, Address: &pb.Address{Name: testCleanupExchangeName, Type: pb.Address_FILTER, Subjects: []string{"same-key"}}}
		management := &managementClientStub{request: func(_ context.Context, method, _ string) ([]byte, int, error) {
			if method == http.MethodGet {
				return []byte(`[{"routing_key":"same-key","properties_key":"same-key","arguments":{"kind":"old"}}]`), http.StatusOK, nil
			}
			return nil, http.StatusNoContent, nil
		}}
		connection := &rabbitMQAMQP10ConnectionMock{}
		bd := &BrokerDetails{ctx: context.Background(), Connection: connection, mgmtClient: management, connectionConfig: &pb.ConnectionConfiguration{Tenant: testCleanupTenant}}
		connection.On("DeclareBind", bd.ctx, mock.Anything).Return("binding-path", nil).Once()

		err := newTestRabbitMQAMQP10Provider().bindAddressSubjects(source, source.GetAddress(), bd)

		require.NoError(t, err)
		assert.Equal(t, []managementRequest{{method: http.MethodGet, path: testCleanupBindingPath}}, management.requests)
		connection.AssertExpectations(t)
	})

	t.Run("no subjects and no filters deletes every existing queue binding", func(t *testing.T) {
		source := &pb.Source{Name: testCleanupQueueName, Type: pb.Source_QUEUE, AutoDelete: true, Address: &pb.Address{
			Name: testCleanupExchangeName, Type: pb.Address_TOPIC,
		}}
		management := &managementClientStub{request: func(_ context.Context, method, _ string) ([]byte, int, error) {
			if method == http.MethodGet {
				return []byte(`[
					{"routing_key":"one","properties_key":"one-key"},
					{"routing_key":"two","properties_key":"two-key"}
				]`), http.StatusOK, nil
			}
			return nil, http.StatusNoContent, nil
		}}
		bd := &BrokerDetails{ctx: context.Background(), mgmtClient: management, connectionConfig: &pb.ConnectionConfiguration{Tenant: testCleanupTenant}}
		connection := &rabbitMQAMQP10ConnectionMock{}
		bd.Connection = connection

		err := newTestRabbitMQAMQP10Provider().bindAddressSubjects(source, source.GetAddress(), bd)

		require.NoError(t, err)
		connection.AssertNotCalled(t, "DeclareBind", mock.Anything, mock.Anything)
		assert.Equal(t, []managementRequest{
			{method: http.MethodGet, path: testCleanupBindingPath},
			{method: http.MethodDelete, path: testCleanupBindingPath + "/one-key"},
			{method: http.MethodDelete, path: testCleanupBindingPath + "/two-key"},
		}, management.requests)
	})

	t.Run("filters without subjects retain the empty routing key", func(t *testing.T) {
		source := &pb.Source{Name: testCleanupQueueName, Type: pb.Source_QUEUE, AutoDelete: true, Filters: []*pb.Filter{{
			Type: pb.Filter_ANY, Matches: []*pb.Match{{Name: "kind", Value: "new"}},
		}}, Address: &pb.Address{Name: testCleanupExchangeName, Type: pb.Address_FILTER}}
		management := &managementClientStub{request: func(_ context.Context, method, _ string) ([]byte, int, error) {
			if method == http.MethodGet {
				return []byte(fmt.Sprintf(`[
					{"routing_key":"","properties_key":"empty-key","arguments":{"kind":"old"}},
					{"routing_key":"stale","properties_key":%q}
				]`, testCleanupStaleProperties)), http.StatusOK, nil
			}
			return nil, http.StatusNoContent, nil
		}}
		connection := &rabbitMQAMQP10ConnectionMock{}
		bd := &BrokerDetails{ctx: context.Background(), Connection: connection, mgmtClient: management, connectionConfig: &pb.ConnectionConfiguration{Tenant: testCleanupTenant}}
		connection.On("DeclareBind", bd.ctx, mock.Anything).Run(func(args mock.Arguments) {
			binding := args.Get(1).(*rabbitmqamqp.ExchangeToQueueBindingSpecification)
			assert.Empty(t, binding.BindingKey)
		}).Return("binding-path", nil).Once()

		err := newTestRabbitMQAMQP10Provider().bindAddressSubjects(source, source.GetAddress(), bd)

		require.NoError(t, err)
		assert.Equal(t, []managementRequest{
			{method: http.MethodGet, path: testCleanupBindingPath},
			{method: http.MethodDelete, path: testCleanupBindingPath + "/" + testCleanupStaleProperties},
		}, management.requests)
		connection.AssertExpectations(t)
	})
}

func Test_amqp10provider_subscribeSuppressesCleanupFailures(t *testing.T) {
	tests := []struct {
		name    string
		request func(context.Context, string, string) ([]byte, int, error)
		want    []managementRequest
	}{
		{
			name: "listing failure",
			request: func(context.Context, string, string) ([]byte, int, error) {
				return nil, http.StatusInternalServerError, nil
			},
			want: []managementRequest{{method: http.MethodGet, path: testCleanupBindingPath}},
		},
		{
			name: "deletion failure",
			request: func(_ context.Context, method, _ string) ([]byte, int, error) {
				if method == http.MethodGet {
					return []byte(fmt.Sprintf(`[{"routing_key":"stale","properties_key":%q}]`, testCleanupStaleProperties)), http.StatusOK, nil
				}
				return nil, http.StatusInternalServerError, nil
			},
			want: []managementRequest{
				{method: http.MethodGet, path: testCleanupBindingPath},
				{method: http.MethodDelete, path: testCleanupBindingPath + "/" + testCleanupStaleProperties},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, clientIdentifier := newTestProviderContext(t, "subscribe-cleanup-error")
			connection := &rabbitMQAMQP10ConnectionMock{}
			management := &managementClientStub{request: tt.request}
			bd := &BrokerDetails{
				ctx:              ctx,
				Connection:       connection,
				mgmtClient:       management,
				connectionConfig: &pb.ConnectionConfiguration{Tenant: testCleanupTenant},
			}
			prov := newTestRabbitMQAMQP10Provider()
			prov.connections.Add(clientIdentifier, bd)
			source := &pb.Source{Name: testCleanupQueueName, Type: pb.Source_QUEUE, AutoDelete: true, Address: &pb.Address{
				Name: testCleanupExchangeName, Type: pb.Address_TOPIC, Subjects: []string{"wanted"},
			}}
			connection.On("DeclareExchange", bd.ctx, mock.Anything).Return(nil, nil).Once()
			connection.On("DeclareQueue", bd.ctx, mock.Anything).Return(nil, nil).Once()
			connection.On("DeclareBind", bd.ctx, mock.Anything).Return("binding-path", nil).Once()

			err := prov.Subscribe(ctx, source, nil)

			require.Nil(t, err)
			assert.Equal(t, tt.want, management.requests)
			connection.AssertExpectations(t)
		})
	}
}

func Test_amqp10provider_declareBindingsParentAddress(t *testing.T) {
	connection := &rabbitMQAMQP10ConnectionMock{}
	management := &managementClientStub{}
	bd := &BrokerDetails{
		ctx:              context.Background(),
		Connection:       connection,
		mgmtClient:       management,
		connectionConfig: &pb.ConnectionConfiguration{Tenant: testCleanupTenant},
	}
	source := &pb.Source{Name: testBindingTemporaryQueue, Type: pb.Source_TEMPORARY, Address: &pb.Address{
		Name: "child", Type: pb.Address_TOPIC, Subjects: []string{testParentAddressSubject},
		ParentAddress: &pb.Address{Name: "parent", Type: pb.Address_TOPIC},
	}}
	var got []rabbitmqamqp.IBindingSpecification
	var calls []string
	connection.On("DeclareExchange", bd.ctx, mock.Anything).Run(func(mock.Arguments) {
		calls = append(calls, "declare-exchange")
	}).Return(nil, nil).Twice()
	connection.On("DeclareBind", bd.ctx, mock.Anything).Run(func(args mock.Arguments) {
		binding, ok := args.Get(1).(rabbitmqamqp.IBindingSpecification)
		require.True(t, ok)
		got = append(got, binding)
		switch binding.(type) {
		case *rabbitmqamqp.ExchangeToExchangeBindingSpecification:
			calls = append(calls, "bind-exchange")
		case *rabbitmqamqp.ExchangeToQueueBindingSpecification:
			calls = append(calls, "bind-queue")
		default:
			require.FailNow(t, "unexpected binding specification type")
		}
	}).Return("binding-path", nil).Twice()

	err := newTestRabbitMQAMQP10Provider().declareBindings(source, bd)

	require.NoError(t, err)
	require.Len(t, got, 2)
	e2e, ok := got[0].(*rabbitmqamqp.ExchangeToExchangeBindingSpecification)
	require.True(t, ok)
	assert.Equal(t, "parent", e2e.SourceExchange)
	assert.Equal(t, "child", e2e.DestinationExchange)
	assert.Equal(t, testParentAddressSubject, e2e.BindingKey)
	assert.Equal(t, &rabbitmqamqp.ExchangeToExchangeBindingSpecification{
		SourceExchange: "parent", DestinationExchange: "child", BindingKey: testParentAddressSubject, Arguments: nil,
	}, e2e)
	queue, ok := got[1].(*rabbitmqamqp.ExchangeToQueueBindingSpecification)
	require.True(t, ok)
	assert.Equal(t, "child", queue.SourceExchange)
	assert.Equal(t, testBindingTemporaryQueue, queue.DestinationQueue)
	assert.Equal(t, testParentAddressSubject, queue.BindingKey)
	assert.Equal(t, []string{"declare-exchange", "declare-exchange", "bind-exchange", "bind-queue"}, calls)
	assert.Equal(t, []managementRequest{{
		method: http.MethodGet, path: fmt.Sprintf("/api/bindings/%s/e/child/q/%s", testCleanupTenant, testBindingTemporaryQueue),
	}}, management.requests)
}

func Test_amqp10provider_subscribeDeclaresQueueBeforeBindings(t *testing.T) {
	ctx, clientIdentifier := newTestProviderContext(t, "subscribe-bind-order")
	connection := &rabbitMQAMQP10ConnectionMock{}
	var calls []string
	management := &managementClientStub{request: func(context.Context, string, string) ([]byte, int, error) {
		calls = append(calls, "cleanup")
		return []byte("[]"), http.StatusOK, nil
	}}
	bd := &BrokerDetails{ctx: ctx, Connection: connection, mgmtClient: management}
	prov := newTestRabbitMQAMQP10Provider()
	prov.connections.Add(clientIdentifier, bd)
	source := &pb.Source{Name: testBindingQueueName, Type: pb.Source_QUEUE, Address: &pb.Address{
		Name: testBindingExchangeName, Type: pb.Address_TOPIC, Subjects: []string{testBindingSubjectCreated},
	}}
	connection.On("DeclareExchange", bd.ctx, mock.Anything).Run(func(mock.Arguments) {
		calls = append(calls, testBindingCallExchange)
	}).Return(nil, nil).Once()
	connection.On("DeclareQueue", bd.ctx, mock.Anything).Run(func(mock.Arguments) {
		calls = append(calls, testBindingCallQueue)
	}).Return(nil, nil).Once()
	connection.On("DeclareBind", bd.ctx, mock.MatchedBy(func(specification rabbitmqamqp.IBindingSpecification) bool {
		binding, ok := specification.(*rabbitmqamqp.ExchangeToQueueBindingSpecification)
		return ok && binding.SourceExchange == testBindingExchangeName && binding.DestinationQueue == testBindingQuorumQueueName && binding.BindingKey == testBindingSubjectCreated
	})).Run(func(mock.Arguments) {
		calls = append(calls, testBindingCallBinding)
	}).Return("binding-path", nil).Once()

	err := prov.Subscribe(ctx, source, nil)

	require.Nil(t, err)
	assert.Equal(t, []string{testBindingCallExchange, testBindingCallQueue, testBindingCallBinding, "cleanup"}, calls)
}

func Test_amqp10provider_subscribeContinuesAfterBindingError(t *testing.T) {
	ctx, clientIdentifier := newTestProviderContext(t, "subscribe-bind-error")
	connection := &rabbitMQAMQP10ConnectionMock{}
	management := &managementClientStub{}
	bd := &BrokerDetails{ctx: ctx, Connection: connection, mgmtClient: management}
	prov := newTestRabbitMQAMQP10Provider()
	prov.connections.Add(clientIdentifier, bd)
	source := &pb.Source{Name: testBindingQueueName, Type: pb.Source_QUEUE, Address: &pb.Address{
		Name: testBindingExchangeName, Type: pb.Address_TOPIC, Subjects: []string{testBindingSubjectCreated},
	}}
	connection.On("DeclareExchange", bd.ctx, mock.Anything).Return(nil, nil).Once()
	connection.On("DeclareQueue", bd.ctx, mock.Anything).Return(nil, nil).Once()
	connection.On("DeclareBind", bd.ctx, mock.Anything).Return("", errors.New("bind failed")).Once()

	err := prov.Subscribe(ctx, source, nil)

	require.Nil(t, err)
	connection.AssertNumberOfCalls(t, "DeclareBind", 1)
	require.Len(t, management.requests, 1)
	assert.Equal(t, http.MethodGet, management.requests[0].method)
}

func Test_amqp10provider_subscribeContinuesAfterPartialBindingFailure(t *testing.T) {
	ctx, clientIdentifier := newTestProviderContext(t, "subscribe-partial-bind-error")
	connection := &rabbitMQAMQP10ConnectionMock{}
	bd := &BrokerDetails{ctx: ctx, Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	prov.connections.Add(clientIdentifier, bd)
	source := &pb.Source{Name: testBindingQueueName, Type: pb.Source_QUEUE, Address: &pb.Address{
		Name: testBindingExchangeName, Type: pb.Address_TOPIC, Subjects: []string{testBindingSubjectOne, testBindingSubjectTwo},
	}}
	var got []rabbitmqamqp.ExchangeToQueueBindingSpecification
	captureBinding := func(args mock.Arguments) {
		binding, ok := args.Get(1).(*rabbitmqamqp.ExchangeToQueueBindingSpecification)
		require.True(t, ok)
		got = append(got, *binding)
	}
	matchBindingKey := func(bindingKey string) interface{} {
		return mock.MatchedBy(func(specification rabbitmqamqp.IBindingSpecification) bool {
			binding, ok := specification.(*rabbitmqamqp.ExchangeToQueueBindingSpecification)
			return ok && binding.SourceExchange == testBindingExchangeName &&
				binding.DestinationQueue == testBindingQuorumQueueName && binding.BindingKey == bindingKey && binding.Arguments == nil
		})
	}
	connection.On("DeclareExchange", bd.ctx, mock.Anything).Return(nil, nil).Once()
	connection.On("DeclareQueue", bd.ctx, mock.Anything).Return(nil, nil).Once()
	connection.On("DeclareBind", bd.ctx, matchBindingKey(testBindingSubjectOne)).Run(captureBinding).Return("", errors.New("first binding failed")).Once()
	connection.On("DeclareBind", bd.ctx, matchBindingKey(testBindingSubjectTwo)).Run(captureBinding).Return("second-binding", nil).Once()

	err := prov.Subscribe(ctx, source, nil)

	require.Nil(t, err)
	assert.Equal(t, []rabbitmqamqp.ExchangeToQueueBindingSpecification{
		{SourceExchange: testBindingExchangeName, DestinationQueue: testBindingQuorumQueueName, BindingKey: testBindingSubjectOne},
		{SourceExchange: testBindingExchangeName, DestinationQueue: testBindingQuorumQueueName, BindingKey: testBindingSubjectTwo},
	}, got)
	connection.AssertNumberOfCalls(t, "DeclareExchange", 1)
	connection.AssertNumberOfCalls(t, "DeclareQueue", 1)
	connection.AssertNumberOfCalls(t, "DeclareBind", 2)
}
