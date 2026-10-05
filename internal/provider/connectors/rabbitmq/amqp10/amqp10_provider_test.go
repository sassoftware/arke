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
	assert.Equal(t, &pb.SourceStats{}, prov.SourceStats(context.Background(), nil))
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

func Test_amqp10provider_declareExchangeReturnsInvalidAddressType(t *testing.T) {
	management := &rabbitMQAMQP10ManagementMock{}
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("Management").Return(management)
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	address := &pb.Address{Name: testExchangeName, Type: pb.Address_TargetType(99)}

	err := prov.declareExchange(address, bd)

	assert.EqualError(t, err, "99 is not a valid address type")
	connection.AssertNotCalled(t, "Management")
	management.AssertNotCalled(t, "DeclareExchange", mock.Anything, mock.Anything)
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

func Test_amqp10provider_subscribeDeclaresExchange(t *testing.T) {
	ctx, clientIdentifier := newTestProviderContext(t, "subscribe-declare")
	management := &rabbitMQAMQP10ManagementMock{}
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("Management").Return(management).Once()
	bd := &BrokerDetails{ctx: ctx, Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	prov.connections.Add(clientIdentifier, bd)
	source := &pb.Source{Address: &pb.Address{Name: "subscribe", Type: pb.Address_TOPIC}}
	management.On("DeclareExchange", bd.ctx, mock.Anything).Return(nil, nil).Once()

	err := prov.Subscribe(ctx, source, nil)

	require.Nil(t, err)
	assert.True(t, bd.exchangeExists(source.GetAddress().GetName()))
	management.AssertNumberOfCalls(t, "DeclareExchange", 1)
}

func Test_amqp10provider_subscribeReturnsDeclarationError(t *testing.T) {
	ctx, clientIdentifier := newTestProviderContext(t, "subscribe-declare-error")
	management := &rabbitMQAMQP10ManagementMock{}
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("Management").Return(management).Once()
	bd := &BrokerDetails{ctx: ctx, Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	prov.connections.Add(clientIdentifier, bd)
	source := &pb.Source{Address: &pb.Address{Name: "subscribe-error", Type: pb.Address_TOPIC}}
	management.On("DeclareExchange", bd.ctx, mock.Anything).Return(nil, errors.New("exchange failed")).Once()

	err := prov.Subscribe(ctx, source, nil)

	require.Equal(t, "exchange failed", err.GetMessage())
	assert.False(t, bd.exchangeExists(source.GetAddress().GetName()))
	management.AssertNumberOfCalls(t, "DeclareExchange", 1)
}

func Test_amqp10provider_subscribeSkipsKnownExchange(t *testing.T) {
	ctx, clientIdentifier := newTestProviderContext(t, "subscribe-known-exchange")
	bd := &BrokerDetails{}
	bd.entityTracker().AddExchange("known")
	prov := newTestRabbitMQAMQP10Provider()
	prov.connections.Add(clientIdentifier, bd)
	source := &pb.Source{Address: &pb.Address{Name: "known", Type: pb.Address_TOPIC}}

	require.Nil(t, prov.Subscribe(ctx, source, nil))
	assert.True(t, bd.exchangeExists("known"))
}

func Test_amqp10provider_declareExchangeAcceptsPreconditionFailure(t *testing.T) {
	management := &rabbitMQAMQP10ManagementMock{}
	connection := &rabbitMQAMQP10ConnectionMock{}
	connection.On("Management").Return(management)
	bd := &BrokerDetails{ctx: context.Background(), Connection: connection}
	prov := newTestRabbitMQAMQP10Provider()
	management.On("DeclareExchange", bd.ctx, mock.Anything).Return(nil, rabbitmqamqp.ErrPreconditionFailed).Once()

	err := prov.declareExchange(&pb.Address{Name: "incompatible", Type: pb.Address_TOPIC}, bd)

	require.NoError(t, err)
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
