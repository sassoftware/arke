// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/i18n"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/provider/connectors/amqp"
	"github.com/sassoftware/arke/internal/util"
)

const (
	providerName string = "rabbitmq-amqp10"
)

var supportedSourceOptions = map[string]bool{
	"MessageTTL":        true,
	"DeadLetterAddress": true,
	"DeadLetterSubject": true,
	"Expires":           true,
	"Offset":            true,
	"ConsumerGroup":     true,
}
var supportedStreamSourceOptions = map[string]bool{"Offset": true, "MessageTTL": true, "ConsumerGroup": true}

func init() {
	provider.Register(providerName, NewRabbitMQAMQP10Provider)
}

type rabbitMQAMQP10Provider struct {
	connections *util.ConcurrentMap
}

func NewRabbitMQAMQP10Provider() provider.Provider {
	prov := &rabbitMQAMQP10Provider{
		connections: util.NewConcurrentMap(),
	}

	return prov
}

func (prov *rabbitMQAMQP10Provider) getBrokerDetails(ctx context.Context) (*BrokerDetails, error) {
	clientIdentifier, err := util.GetClientIdentifier(ctx)
	if err != nil {
		util.Logger.Warn(i18n.NoClientUUIDError, err.Error())
		return nil, err
	}
	if bd := prov.getBrokerDetailsByIdentifier(clientIdentifier); bd != nil {
		return bd, nil
	}

	return nil, fmt.Errorf("broker details not found for client identifier: %s", clientIdentifier)
}

func (prov *rabbitMQAMQP10Provider) getBrokerDetailsByIdentifier(clientIdentifier string) *BrokerDetails {
	if bd, ok := prov.connections.Get(clientIdentifier); ok {
		brokerDetails, ok := bd.(*BrokerDetails)
		if ok {
			return brokerDetails
		}
	}
	return nil
}

func (prov *rabbitMQAMQP10Provider) Connect(ctx context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error {
	if cf == nil {
		return &pb.Error{Message: "connection configuration is required"}
	}

	if cf.GetCredentials() == nil {
		return &pb.Error{Message: "missing broker credentials"}
	}

	clientIdentifier, err := util.GetClientIdentifier(ctx)
	if err != nil {
		util.Logger.Warn(i18n.NoClientUUIDError, err.Error())
		return &pb.Error{Message: err.Error()}
	}

	// Check if we already have an active connection for this client
	bd := prov.getBrokerDetailsByIdentifier(clientIdentifier)
	if bd != nil && bd.Connection != nil && !bd.Connection.IsClosed() {
		util.Logger.Debugf("client already connected: %s", clientIdentifier)
		return nil
	}

	var tlsConfig *tls.Config
	if cf.GetTls() {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: tlsSkipVerify, //nolint:gosec
		}
		caBundlePath := os.Getenv(provider.TrustedCerts)
		if caBundlePath != "" {
			caBundle, err := os.ReadFile(filepath.FromSlash(filepath.Clean("/" + strings.Trim(caBundlePath, "/")))) // #gosec G703
			if err == nil {
				tlsConfig.RootCAs = x509.NewCertPool()
				if !tlsConfig.RootCAs.AppendCertsFromPEM(caBundle) {
					return &pb.Error{Message: i18n.T(i18n.TLSCABundleError, caBundlePath)}
				}
			}
		}
	}
	env, err := newRabbitMQAMQP10EnvironmentFunc(ctx, cf, tlsConfig)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	endpoint := amqp.GetMgmtEndpoint(cf)
	username, password := amqp.GetUsernamePassword(cf)
	mgmtClient, err := amqp.NewManagementClient(ctx, endpoint, username, password, tlsConfig)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	bd = &BrokerDetails{
		ctx:              ctx,
		provider:         prov,
		mgmtClient:       mgmtClient,
		Env:              env,
		ClientIdentifier: clientIdentifier,
		tlsConfig:        tlsConfig,
		activeMessages:   util.NewConcurrentMap(),
		connectionConfig: cf,
		tlsSkipVerify:    tlsSkipVerify,
		ActiveStreams:    0,
		lastPubSubEvent:  time.Now(),
		shutdownChan:     make(chan struct{}),
	}
	ok, err := bd.connect()
	if !ok {
		return &pb.Error{Message: err.Error()}
	}

	// TODO: Issue 204 - setup lifecycle management to listen for state change
	prov.connections.Add(clientIdentifier, bd)
	return nil
}

func (prov *rabbitMQAMQP10Provider) ClientExists(clientIdentifier string) bool {
	value, ok := prov.connections.Get(clientIdentifier)
	if !ok {
		return false
	}

	bd, ok := value.(*BrokerDetails)
	if !ok {
		return false
	}

	switch bd.state.Load() {
	case provider.CONNECTED, provider.CONNECTING:
		return true
	default:
		return false
	}
}

func (prov *rabbitMQAMQP10Provider) Publish(context.Context, <-chan *pb.Message, chan<- *pb.Error) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10Provider) PublishOne(context.Context, *pb.Message) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10Provider) Subscribe(ctx context.Context, source *pb.Source, _ chan<- *pb.Message) *pb.Error {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	if err := prov.declareExchange(source.GetAddress(), bd); err != nil {
		return &pb.Error{Message: err.Error()}
	}
	if source.GetType() == pb.Source_QUEUE || source.GetType() == pb.Source_TEMPORARY {
		if err := prov.declareQueue(source, bd); err != nil {
			return &pb.Error{Message: err.Error()}
		}
	}
	return nil
}

func (prov *rabbitMQAMQP10Provider) Ack(context.Context, string) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10Provider) Nack(context.Context, string) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10Provider) Retry(context.Context, *pb.Source, string, int32) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10Provider) DeadLetter(context.Context, *pb.Source, string) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10Provider) Disconnect(ctx context.Context) {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return
	}
	bd.disconnect()

	prov.connections.DeleteIfEqual(bd.ClientIdentifier, bd)
}

func (prov *rabbitMQAMQP10Provider) WaitForConnect(ctx context.Context) bool {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return false
	}
	clientIdentifier := bd.ClientIdentifier

	// to prevent unwanted disconnects for a client with a single stream
	// we need to increment the stream count if we are waiting for provider connect
	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	for start := time.Now(); time.Since(start) < provider.CONNECTTIMEOUT*time.Second; {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		if bd.state.Load() == provider.CONNECTED {
			util.Logger.Info(i18n.ClientConnected, bd.ClientIdentifier)
			return true
		}
		bd, err = prov.getBrokerDetails(ctx)
		if err != nil {
			util.Logger.Info(i18n.ClientDetailsGone, clientIdentifier)
			return false
		}
		if bd.state.Load() == provider.CLOSED || bd.state.Load() == provider.DISCONNECTED {
			util.Logger.Info(i18n.ClientDisconnect, clientIdentifier)
			return false
		}

		provider.SleepRandomReconnect()
	}
	return false
}

func (prov *rabbitMQAMQP10Provider) Stats() *provider.Stats {
	// TODO: Issue 193
	return &provider.Stats{}
}

func (prov *rabbitMQAMQP10Provider) SourceStats(ctx context.Context, source *pb.Source) *pb.SourceStats {
	sourceStats := &pb.SourceStats{}
	if source.GetAddress().GetName() == "" {
		sourceStats.Error = &pb.Error{Message: "address name not defined"}
		return sourceStats
	}

	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		sourceStats.Error = &pb.Error{Message: err.Error()}
		return sourceStats
	}
	return bd.getStreamOrQueueStats(source)
}

// SupportedSourceOptions returns the source options supported by AMQP 1.0.
func (prov *rabbitMQAMQP10Provider) SupportedSourceOptions() map[string]bool {
	return supportedSourceOptions
}

func addressToExchangeSpecification(address *pb.Address) (rabbitmqamqp.IExchangeSpecification, error) {
	switch address.GetType() {
	case pb.Address_TOPIC:
		return &rabbitmqamqp.TopicExchangeSpecification{Name: address.GetName(), IsAutoDelete: address.GetAutoDelete(), Arguments: nil}, nil
	case pb.Address_FILTER:
		return &rabbitmqamqp.HeadersExchangeSpecification{Name: address.GetName(), IsAutoDelete: address.GetAutoDelete(), Arguments: nil}, nil
	case pb.Address_QUEUE:
		return &rabbitmqamqp.DirectExchangeSpecification{Name: address.GetName(), IsAutoDelete: address.GetAutoDelete(), Arguments: nil}, nil
	case pb.Address_STREAM:
		return &rabbitmqamqp.CustomExchangeSpecification{Name: address.GetName(), IsAutoDelete: address.GetAutoDelete(), ExchangeTypeName: "stream", Arguments: nil}, nil
	default:
		return nil, fmt.Errorf("%s is not a valid address type", address.GetType())
	}
}

func (prov *rabbitMQAMQP10Provider) declareExchange(address *pb.Address, bd *BrokerDetails) error {
	name := address.GetName()
	if strings.Contains(name, "amq.") || bd.exchangeExists(name) {
		return nil
	}

	specification, err := addressToExchangeSpecification(address)
	if err != nil {
		return err
	}
	_, err = bd.Connection.DeclareExchange(bd.ctx, specification)
	if err != nil {
		if errors.Is(err, rabbitmqamqp.ErrPreconditionFailed) {
			bd.entityTracker().AddExchange(name)
			return nil
		}
		if bd.exchangeExists(name) {
			return nil
		}
		return err
	}

	bd.entityTracker().AddExchange(name)
	return nil
}

func queueSpecification(source *pb.Source) (rabbitmqamqp.IQueueSpecification, error) {
	switch source.GetType() {
	case pb.Source_QUEUE, pb.Source_TEMPORARY:
	case pb.Source_STREAM:
		return nil, fmt.Errorf("%s is not a valid source type", source.GetType())
	default:
		return nil, fmt.Errorf("%s is not a valid source type", source.GetType())
	}

	name := amqp.SourceName(source)
	isQuorum := amqp.IsQuorum(source)
	messageTTL := int64(0)
	expires := int64(0)
	hasExpires := false
	deadLetterExchange := ""
	deadLetterRoutingKey := ""
	for option, value := range source.GetOptions() {
		switch option {
		case "MessageTTL":
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, errors.New("value for MessageTTL option must be a quoted integer")
			}
			messageTTL = parsed
		case "Expires":
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, errors.New("value for Expires option must be a quoted integer")
			}
			expires = parsed
			hasExpires = true
		case "DeadLetterAddress":
			deadLetterExchange = value
		case "DeadLetterSubject":
			deadLetterRoutingKey = value
		default:
			return nil, fmt.Errorf("%s is an unsupported source option", option)
		}
	}

	if (source.GetAutoDelete() || source.GetExclusive()) && !hasExpires {
		expires = (5 * time.Minute).Milliseconds()
	}

	// Match AMQP 0.9.1 by expressing auto-delete/exclusive behavior through expiry.
	// DLX exchanges and bindings are configured separately.
	if isQuorum {
		return &rabbitmqamqp.QuorumQueueSpecification{
			Name:                 name,
			AutoExpire:           expires,
			MessageTTL:           messageTTL,
			SingleActiveConsumer: source.GetSingleActiveConsumer(),
			DeadLetterExchange:   deadLetterExchange,
			DeadLetterRoutingKey: deadLetterRoutingKey,
		}, nil
	}
	return &rabbitmqamqp.ClassicQueueSpecification{
		Name:                 name,
		AutoExpire:           expires,
		MessageTTL:           messageTTL,
		SingleActiveConsumer: source.GetSingleActiveConsumer(),
		DeadLetterExchange:   deadLetterExchange,
		DeadLetterRoutingKey: deadLetterRoutingKey,
	}, nil
}

func (prov *rabbitMQAMQP10Provider) declareQueue(source *pb.Source, bd *BrokerDetails) error {
	name := amqp.SourceName(source)
	if bd.queueExists(name) {
		return nil
	}

	specification, err := queueSpecification(source)
	if err != nil {
		return err
	}
	_, declarationErr := bd.Connection.DeclareQueue(bd.ctx, specification)
	if declarationErr != nil {
		util.Logger.Warn(i18n.ClientQueueDeclareError, declarationErr.Error(), bd.ClientIdentifier)
		// TODO: Should we log or return queue declaration errors?
	}
	bd.entityTracker().AddQueue(name)
	return nil
}
