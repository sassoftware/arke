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

	rabbitmqamqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/i18n"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
)

const (
	providerName string = "amqp10"
	trustedCerts string = "ARKE_TRUSTED_CA_CERTIFICATES_PEM_FILE"
)

type amqp10provider struct {
	tlsConfig   *tls.Config
	connections *util.ConcurrentMap
}

func init() {
	provider.Register(providerName, NewAMQP10Provider)
}

func NewAMQP10Provider() provider.Provider {
	prov := &amqp10provider{
		connections: util.NewConcurrentMap(),
	}

	return prov
}

func (prov *amqp10provider) getBrokerDetails(ctx context.Context) (*BrokerDetails, error) {
	clientIdentifier, err := util.GetClientIdentifier(ctx)
	if err != nil {
		return nil, err
	}
	bdInterface, ok := prov.connections.Get(clientIdentifier)
	if !ok {
		// TODO: i18n
		return nil, fmt.Errorf("broker details not found for client identifier: %s", clientIdentifier)
	}
	bd, ok := bdInterface.(*BrokerDetails)
	if !ok {
		// TODO: i18n
		return nil, fmt.Errorf("invalid broker details type for client identifier: %s", clientIdentifier)
	}
	return bd, nil
}

func (prov *amqp10provider) Connect(ctx context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error {
	if cf == nil {
		// TODO: i18n
		return &pb.Error{Message: "connection configuration is required"}
	}

	clientIdentifier, err := util.GetClientIdentifier(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	if cf.GetCredentials() == nil {
		// TODO: i18n
		return &pb.Error{Message: "missing broker credentials"}
	}

	var tlsConfig *tls.Config
	if cf.GetTls() {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: tlsSkipVerify,
		}
		caBundlePath := os.Getenv(trustedCerts)
		if caBundlePath != "" {
			caBundle, err := os.ReadFile(filepath.FromSlash(filepath.Clean("/" + strings.Trim(caBundlePath, "/"))))
			if err == nil {
				tlsConfig.RootCAs = x509.NewCertPool()
				tlsConfig.RootCAs.AppendCertsFromPEM(caBundle)
			}
		}
	}
	pubChCtx := context.WithValue(context.Background(), CtxKey{name: "clientIdentifier"}, clientIdentifier)
	pubChCtx, pubChCancel := context.WithCancel(pubChCtx) //nolint:gosec
	bdCtx := context.Context(ctx)
	env := newAmqp10Environment(bdCtx, tlsConfig, cf.GetHost(), &rabbitmqamqp.AmqpConnOptions{})
	bd := &BrokerDetails{
		ctx:              bdCtx,
		Env:              env,
		ClientIdentifier: clientIdentifier,
		tlsConfig:        tlsConfig,
		activeMessages:   util.NewConcurrentMap(),
		connectionConfig: cf,
		tlsSkipVerify:    tlsSkipVerify,
		ActiveStreams:    0,
		lastPubSubEvent:  time.Now(),
		ErrorChannel:     make(chan amqp10Error, 1),
		shutdownChan:     make(chan bool, 1),
		pubChannelCtx:    pubChCtx,
		pubChannelCancel: pubChCancel,
	}
	ok, err := bd.connect()
	if !ok {
		return &pb.Error{Message: err.Error()}
	}

	// TODO: PSEVT-257 - setup lifecycle management to listen for state change
	prov.connections.Add(clientIdentifier, bd)
	return nil
}

func (prov *amqp10provider) ClientExists(clientIdentifier string) bool {
	// TODO: PSEVT-261 - not sure anything else needs to be done here
	_, ok := prov.connections.Get(clientIdentifier)
	return ok
}

func (prov *amqp10provider) Publish(context.Context, <-chan *pb.Message, chan<- *pb.Error) *pb.Error {
	return nil
}

func (prov *amqp10provider) PublishOne(context.Context, *pb.Message) *pb.Error {
	return nil
}

func (prov *amqp10provider) Subscribe(context.Context, *pb.Source, chan<- *pb.Message) *pb.Error {
	return nil
}

func (prov *amqp10provider) Ack(context.Context, string) *pb.Error {
	return nil
}

func (prov *amqp10provider) Nack(context.Context, string) *pb.Error {
	return nil
}

func (prov *amqp10provider) Retry(context.Context, *pb.Source, string, int32) *pb.Error {
	return nil
}

func (prov *amqp10provider) DeadLetter(context.Context, *pb.Source, string) *pb.Error {
	return nil
}

func (prov *amqp10provider) Disconnect(ctx context.Context) {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return
	}
	bd.disconnect()

	prov.connections.DeleteIfEqual(bd.ClientIdentifier, bd)
}

func (prov *amqp10provider) SupportedSourceOptions() map[string]bool {
	return map[string]bool{}
}

func (prov *amqp10provider) WaitForConnect(ctx context.Context) bool {
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
		if bd.state.Load() == provider.CONNECTED {
			util.Logger.Info(i18n.ClientConnected, bd.ClientIdentifier)
			return true
		}
		bd, err = prov.getBrokerDetails(ctx)
		if err != nil {
			util.Logger.Info(i18n.ClientDetailsGone, clientIdentifier)
			return false
		}

		sleepRandomReconnect()
	}
	return false
}

func (prov *amqp10provider) Stats() *provider.Stats {
	// TODO: PSEVT-212
	return &provider.Stats{}
}

func (prov *amqp10provider) SourceStats(context.Context, *pb.Source) *pb.SourceStats {
	// TODO: PSEVT-212
	return &pb.SourceStats{}
}

func sleepRandomReconnect() {
	util.SleepRandom(100, provider.ReconnectDelay)
}
