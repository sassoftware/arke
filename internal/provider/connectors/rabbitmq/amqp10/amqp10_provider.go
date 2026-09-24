package amqp10

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/i18n"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
)

const (
	providerName string = "rabbitmq-amqp10"
	trustedCerts string = "ARKE_TRUSTED_CA_CERTIFICATES_PEM_FILE"
)

type rabbitMQAMQP10provider struct {
	connections *util.ConcurrentMap
}

func init() {
	provider.Register(providerName, NewRabbitMQAMQP10Provider)
}

func NewRabbitMQAMQP10Provider() provider.Provider {
	prov := &rabbitMQAMQP10provider{
		connections: util.NewConcurrentMap(),
	}

	return prov
}

func (prov *rabbitMQAMQP10provider) getBrokerDetails(ctx context.Context) (*BrokerDetails, error) {
	clientIdentifier, err := util.GetClientIdentifier(ctx)
	if err != nil {
		util.Logger.Warn(i18n.NoClientUUIDError, err.Error())
		return nil, err
	}
	if bd := prov.getBrokerDetailsByIdentifier(clientIdentifier); bd != nil {
		return bd, nil
	}

	return nil, fmt.Errorf("Broker details not found for client identifier: %s", clientIdentifier)
}

func (prov *rabbitMQAMQP10provider) getBrokerDetailsByIdentifier(clientIdentifier string) *BrokerDetails {
	if bd, ok := prov.connections.Get(clientIdentifier); ok {
		brokerDetails, ok := bd.(*BrokerDetails)
		if ok {
			return brokerDetails
		}
	}
	return nil
}

func (prov *rabbitMQAMQP10provider) Connect(ctx context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error {
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
		caBundlePath := os.Getenv(trustedCerts)
		if caBundlePath != "" {
			caBundle, err := os.ReadFile(filepath.FromSlash(filepath.Clean("/" + strings.Trim(caBundlePath, "/"))))
			if err == nil {
				tlsConfig.RootCAs = x509.NewCertPool()
				if !tlsConfig.RootCAs.AppendCertsFromPEM(caBundle) {
					return &pb.Error{Message: fmt.Sprintf("Failed to parse TLS CA bundle at path: %s", caBundlePath)}
				}
			}
		}
	}
	opts, err := getRabbitMQAMQP10ConnOptions(ctx, cf, tlsConfig)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	util.Logger.Debugf("Env options: %+v", opts)
	connUrl := getConnURL(cf)
	env, err := newRabbitMQAMQP10EnvironmentFunc(ctx, cf, tlsConfig, connUrl, opts)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	bd = &BrokerDetails{
		ctx:              ctx,
		provider:         prov,
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

func (prov *rabbitMQAMQP10provider) ClientExists(clientIdentifier string) bool {
	// TODO: Issue 201 - not sure anything else needs to be done here
	_, ok := prov.connections.Get(clientIdentifier)
	return ok
}

func (prov *rabbitMQAMQP10provider) Publish(context.Context, <-chan *pb.Message, chan<- *pb.Error) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10provider) PublishOne(context.Context, *pb.Message) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10provider) Subscribe(context.Context, *pb.Source, chan<- *pb.Message) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10provider) Ack(context.Context, string) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10provider) Nack(context.Context, string) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10provider) Retry(context.Context, *pb.Source, string, int32) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10provider) DeadLetter(context.Context, *pb.Source, string) *pb.Error {
	return nil
}

func (prov *rabbitMQAMQP10provider) Disconnect(ctx context.Context) {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return
	}
	bd.disconnect()

	prov.connections.DeleteIfEqual(bd.ClientIdentifier, bd)
}

func (prov *rabbitMQAMQP10provider) SupportedSourceOptions() map[string]bool {
	return map[string]bool{}
}

func (prov *rabbitMQAMQP10provider) WaitForConnect(ctx context.Context) bool {
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

		sleepRandomReconnect()
	}
	return false
}

func (prov *rabbitMQAMQP10provider) Stats() *provider.Stats {
	// TODO: Issue 193
	return &provider.Stats{}
}

func (prov *rabbitMQAMQP10provider) SourceStats(context.Context, *pb.Source) *pb.SourceStats {
	// TODO: Issue 193
	return &pb.SourceStats{}
}

func sleepRandomReconnect() {
	util.SleepRandom(100, provider.ReconnectDelay)
}
