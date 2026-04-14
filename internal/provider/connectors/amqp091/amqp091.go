// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/i18n"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/util"
	"github.com/sassoftware/arke/internal/util/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const providerName string = "amqp091"
const trustedCerts = "ARKE_TRUSTED_CA_CERTIFICATES_PEM_FILE"
const streamOffsetHeaderName = "x-current-offset"
const retryCountHeaderName = "x-retry-count"
const rabbitReceivedTimeHeaderName = "x-opt-rabbitmq-received-time"
const timeStampInMSHeaderName = "timestamp_in_ms"
const transferEncodingHeaderName = "Transfer-Encoding"

const maxPubChannels = 10
const maxPubPCChannels = 10

const queueTypeClassic = "classic"
const queueTypeQuorum = "quorum"
const xMatchAny = "any"

var supportedSourceOptionsList = []string{"MessageTTL", "DeadLetterAddress", "DeadLetterSubject", "Expires", "Offset", "ConsumerGroup"}

var supportedSourceOptions map[string]bool
var supportedStreamSourceOptions = map[string]bool{"Offset": true, "MessageTTL": true, "ConsumerGroup": true}

// NewAmqpConn091 allow overriding the connection for mocking in tests
var NewAmqpConn091 = NewAmqp091Connection

// NewStreamConn allow overriding the connection for mocking in tests
var NewStreamConn = NewStreamConnection

// GetClientIdentifier Set function as a variable so we can replace the GetClientIdentifier method in unit tests
var GetClientIdentifier = util.GetClientIdentifier

type amqp091provider struct {
	tlsConfig   *tls.Config
	connections *util.ConcurrentMap
}

type rabbitMQManagementMetrics struct {
	MessageStats struct {
		PublishDetails struct {
			Rate float64 `json:"rate"`
		} `json:"publish_details"`
		DeliverDetails struct {
			Rate float64 `json:"rate"`
		} `json:"deliver_details"`
	} `json:"message_stats"`
	Consumers float64 `json:"consumers"`
	Messages  float64 `json:"messages"`
}

// BrokerDetails struct houses connection specific information for the broker
type BrokerDetails struct {
	sync.Mutex
	Connection   amqp091ConnectionShim
	ErrorChannel chan amqp091Error
	RetryChannel *amqp091ChannelShim

	// Ctx used by both pubChannels and pubPCChannels
	pubChannelCtx context.Context

	// Ctx cancellation function used by both pubChannels and pubPCChannels
	pubChannelCancel context.CancelFunc

	pubChannels      *util.BlockingPool
	pubPCChannels    *util.BlockingPool
	StreamConnection streamConnectionShim
	ClientIdentifier string
	knownExchanges   *util.ConcurrentMap
	knownQueues      *util.ConcurrentMap
	knownBindings    *util.ConcurrentMap
	activeMessages   *util.ConcurrentMap
	state            uint16
	connectionConfig *pb.ConnectionConfiguration
	tlsSkipVerify    bool
	ActiveStreams    int64
	consumed         int64
	produced         int64
	clientDisconnect bool
	lastPubSubEvent  time.Time
	tlsConfig        *tls.Config
	tlsEnabled       bool
	shutdownChan     chan bool
}

func init() {
	// Register this provider with the Provider factory.
	provider.Register(providerName, NewAMQP091Provider)

	supportedSourceOptions = make(map[string]bool)
	for _, option := range supportedSourceOptionsList {
		supportedSourceOptions[option] = true
	}
	if !strings.HasSuffix(os.Args[0], ".test") {
		go connectionCleaner(context.Background())
	}
}

/*
 * AMQP 0-9-1 provider code
 */

// NewAMQP091Provider returns a new amqp091 provider
func NewAMQP091Provider() provider.Provider {
	connections := util.NewConcurrentMap()
	prov := &amqp091provider{connections: connections}

	caBundlePath := os.Getenv(trustedCerts)
	prov.tlsConfig = &tls.Config{}

	if caBundlePath != "" {
		caBundle, err := os.ReadFile(filepath.FromSlash(filepath.Clean("/" + strings.Trim(caBundlePath, "/"))))
		if err == nil {
			prov.tlsConfig.RootCAs = x509.NewCertPool()
			prov.tlsConfig.RootCAs.AppendCertsFromPEM(caBundle)
		}
	}

	return prov
}

func (prov *amqp091provider) getBrokerDetails(ctx context.Context) (*BrokerDetails, error) {
	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		util.Logger.Warn(i18n.NoClientUUIDError, err.Error())
		return &BrokerDetails{}, err
	}

	if bd := prov.getBrokerDetailsByIdentifier(clientIdentifier); bd != nil {
		bd.tlsConfig = prov.tlsConfig
		return bd, nil
	}

	return &BrokerDetails{}, fmt.Errorf("could not retrieve broker details for this connection: %s", clientIdentifier)
}

func (prov *amqp091provider) getBrokerDetailsByIdentifier(clientIdentifier string) *BrokerDetails {
	if bd, ok := prov.connections.Get(clientIdentifier); ok {
		return bd.(*BrokerDetails)
	}
	return nil
}

func (prov *amqp091provider) ClientExists(clientIdentifier string) bool {
	if bd := prov.getBrokerDetailsByIdentifier(clientIdentifier); bd != nil {
		return true
	}
	return false
}

// Ack ack a message
func (prov *amqp091provider) Ack(ctx context.Context, msgid string) *pb.Error {
	defer func() *pb.Error {
		if err := recover(); err != nil {
			util.Logger.Debugf("recovered during Ack: %v", err)
			return &pb.Error{Message: fmt.Sprintf("%v", err), IsFatal: true}
		}
		return nil
	}()

	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	var span trace.Span
	defer func() {
		if span != nil {
			span.End()
		}
	}()

	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		switch rm := rmu.(type) {
		case amqp091Message:
			util.Logger.Tracef("Acking message %s with tag %d", msgid, rm.DeliveryTag)
			_, span = tracing.SpanFromHeaders(ctx, fromTableToMap(rm.Headers), "message ack", trace.SpanKindInternal)
			span.SetAttributes(attribute.String("messaging.message.id", msgid),
				attribute.String("messaging.client_id", bd.ClientIdentifier))
			span.AddEvent("provider acking message")

			err = rm.Ack()
		case streamMessage:
			rm.Ack()
		}
	} else {
		util.Logger.Trace(i18n.AckNoMessage, bd.ClientIdentifier, msgid)
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msgid)}
	}

	if err != nil {
		util.Logger.Warn(i18n.AckError, err.Error())

		bd.activeMessages.Delete(msgid)
		errMsg := &pb.Error{
			Message: err.Error(),
		}
		if span != nil {
			span.RecordError(err)
		}
		return errMsg
	}
	util.Logger.Trace(i18n.AckMessage, bd.ClientIdentifier, msgid)
	if span != nil {
		span.AddEvent("provider acked message successfully")
	}
	bd.activeMessages.Delete(msgid)
	return nil
}

// Nack nack a message
func (prov *amqp091provider) Nack(ctx context.Context, msgid string) *pb.Error {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	var span trace.Span
	defer func() {
		if span != nil {
			span.End()
		}
	}()

	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		switch rm := rmu.(type) {
		case amqp091Message:
			_, span = tracing.SpanFromHeaders(ctx, fromTableToMap(rm.Headers), "message nack", trace.SpanKindInternal)
			span.SetAttributes(attribute.String("messaging.message.id", msgid),
				attribute.String("messaging.client_id", bd.ClientIdentifier))
			span.AddEvent("provider nacking message")

			err = rm.Nack(false)
		case streamMessage:
			rm.Nack()
		}
	} else {
		util.Logger.Debug(i18n.NackNoMessage, bd.ClientIdentifier, msgid)
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msgid)}
	}

	if err != nil {
		util.Logger.Warn(i18n.NackError, err.Error())

		bd.activeMessages.Delete(msgid)
		errMsg := &pb.Error{
			Message: err.Error(),
		}
		if span != nil {
			span.RecordError(err)
		}
		return errMsg
	}
	util.Logger.Debug(i18n.NackMessage, bd.ClientIdentifier, msgid)
	if span != nil {
		span.AddEvent("provider nacked message successfully")
	}
	bd.activeMessages.Delete(msgid)
	return nil
}

func (prov *amqp091provider) Retry(ctx context.Context, origSource *pb.Source, msgid string, delay int32) *pb.Error {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	var retrySpan trace.Span
	_, retrySpan = tracing.SpanFromHeaders(ctx, nil, "message retry", trace.SpanKindInternal)
	defer func() {
		if retrySpan != nil {
			retrySpan.End()
		}
	}()

	origSource.Name = sourceName(origSource)

	retrySpan.SetAttributes(attribute.String("source.name", origSource.GetName()),
		attribute.String("messaging.client_id", bd.ClientIdentifier),
		attribute.String("messaging.message.id", msgid))
	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		switch rm := rmu.(type) {
		case streamMessage:
			// We may have work when we manually track offsets
		case amqp091Message:
			retrySpan.AddEvent("setting up retry")

			// setup exchange/queue/binding
			subjects := make([]string, 0)
			subjects = append(subjects, "#")
			options := map[string]string{"MessageTTL": strconv.Itoa(int(delay) * 1000), "DeadLetterAddress": ""}
			sourceName := fmt.Sprintf("%s.retry.%ds", origSource.GetAddress().GetName(), delay)

			retrySource := &pb.Source{
				Name:    sourceName,
				Options: options,
				Address: &pb.Address{
					Subjects: subjects,
					Type:     pb.Address_TOPIC,
					Name:     sourceName,
				},
			}

			if bd.RetryChannel == nil {
				bd.Lock()
				retryChannel, err := bd.Connection.NewChannel(false)
				if err != nil {
					bd.Unlock()
					return &pb.Error{Message: err.Error()}
				}
				bd.RetryChannel = &retryChannel
				bd.Unlock()
			}
			amqpChannel := *bd.RetryChannel

			defer func(bd *BrokerDetails) *pb.Error {
				if err := recover(); err != nil {
					bd.Lock()
					bd.RetryChannel = nil
					bd.Unlock()
					util.Logger.Debugf("recovered: %v", err)
					return &pb.Error{Message: fmt.Sprintf("%v", err), IsFatal: true}
				}
				return nil
			}(bd)

			_ = prov.declareExchange(retrySource.GetAddress(), bd, amqpChannel)

			retrySpan.AddEvent("retry address created")

			declareErr := prov.declareQueue(retrySource, bd, amqpChannel, false)
			if declareErr != nil {
				util.Logger.Debugf("Failed to declare retry queue [%s]", retrySource.GetName())
			}

			retrySpan.AddEvent("retry queue created")

			declareErr = prov.declareBinding(retrySource, bd, amqpChannel, false)
			if declareErr != nil {
				util.Logger.Debugf("Failed to bind retry queue [%s] to exchange [%s]", retrySource.GetName(), retrySource.GetAddress().GetName())
			}

			retrySpan.AddEvent("retry binding created")

			updateRetryCountHeader(&rm)

			retrySpan.AddEvent("retry count header updated")

			retryErr := amqpChannel.Publish(retrySource.Address.GetName(), origSource.GetName(), rm)

			if retryErr != nil {
				util.Logger.Debugf("Failed to publish retry message [%s], requeueing instead.", msgid)
				_ = rm.Nack(true)
			} else {
				_ = rm.Ack() // We ack the message to prevent it from requeueing or dead lettering
			}
			retrySpan.AddEvent("retry ack/nack complete")
		}
		util.Logger.Debug(i18n.RetryMessage, bd.ClientIdentifier, msgid, delay)
		bd.activeMessages.Delete(msgid)
	} else {
		util.Logger.Debug(i18n.RetryNoMessage, bd.ClientIdentifier, msgid)
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msgid)}
	}

	return nil
}

// Updates the x-retry-count header which tracks our retry attempts
func updateRetryCountHeader(rm *amqp091Message) {
	if rm.Headers == nil {
		rm.Headers = make(amqp091Table)
	}
	// initialize retry count to 0
	var retryCount int32
	// if the header already exists, set retryCount to that value
	if retryCountHeader, ok := rm.Headers[retryCountHeaderName]; ok {
		var typeOk bool
		// RabbitMQ stores int values as int32
		retryCount, typeOk = retryCountHeader.(int32)
		if !typeOk {
			util.Logger.Warn("Retry count header is not an int32, resetting to 1")
		}
	}
	// increment retry count by 1
	retryCount++
	// set the header to the new value
	util.Logger.Debugf("Updating %s to %d", retryCountHeaderName, retryCount)
	rm.Headers[retryCountHeaderName] = retryCount
}

// DeadLetter routes the message to a dead letter Address because all retries have failed
func (prov *amqp091provider) DeadLetter(ctx context.Context, _ *pb.Source, msgid string) *pb.Error {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		rm := rmu.(amqp091Message)
		util.Logger.Debugf("DeadLetter message with id [%s].", msgid)
		_ = rm.Nack(false) // Requeue set to false will cause the message to DeadLetter
		bd.activeMessages.Delete(msgid)
	} else {
		util.Logger.Debugf("DeadLetter message with id [%s] failed, message not found in active messages.", msgid)
		return &pb.Error{Message: fmt.Sprintf("DeadLetter message with id [%s] failed, message not found in active messages.", msgid)}
	}

	return nil
}

// Connect connect to rabbitmq
func (prov *amqp091provider) Connect(ctx context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error {
	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	activeMessages := util.NewConcurrentMap()
	pubChCtx := context.WithValue(context.Background(), CtxKey{name: "clientIdentifier"}, clientIdentifier)
	pubChCtx, pubChCancel := context.WithCancel(pubChCtx) //nolint:gosec
	bd := BrokerDetails{
		connectionConfig: cf,
		ClientIdentifier: clientIdentifier,
		ErrorChannel:     make(chan amqp091Error),
		activeMessages:   activeMessages,
		tlsSkipVerify:    tlsSkipVerify,
		tlsConfig:        prov.tlsConfig,
		produced:         0,
		consumed:         0,
		ActiveStreams:    0,
		clientDisconnect: false,
		lastPubSubEvent:  time.Now(),
		shutdownChan:     make(chan bool, 1),
		pubChannelCtx:    pubChCtx,
		pubChannelCancel: pubChCancel,
	}

	newPubChannel := func() any {
		newChan, _ := bd.Connection.NewChannel(false)
		if newChan == nil {
			return nil
		}
		util.Logger.Debug("Created new publish channel")
		return &newChan
	}

	validateChannel := func(item any) bool {
		ch, ok := item.(*amqp091ChannelShim)
		return ok && ch != nil && !(*ch).IsClosed()
	}

	bd.pubChannels = util.NewBlockingPool(
		pubChCtx,
		maxPubChannels,
		newPubChannel,
	)
	bd.pubChannels.Validate = validateChannel

	newPubPCChannel := func() any {
		newChan, _ := bd.Connection.NewChannel(true)
		if newChan == nil {
			return nil
		}
		util.Logger.Debug("Created new publish confirm channel")
		return &newChan
	}
	bd.pubPCChannels = util.NewBlockingPool(
		pubChCtx,
		maxPubPCChannels,
		newPubPCChannel,
	)
	bd.pubPCChannels.Validate = validateChannel

	_, bdErr := bd.connect()
	if bdErr != nil {
		util.Logger.Warn(i18n.BrokerConnectError, bdErr.Error())
		return &pb.Error{Message: bdErr.Error()}
	}
	prov.connections.Add(bd.ClientIdentifier, &bd)
	go bd.connectionWatcher()

	return nil
}

func (prov *amqp091provider) setupDeadLetter(ctx context.Context, origSource *pb.Source) *pb.Error {
	opts := origSource.GetOptions()
	if _, ok := opts["DeadLetterAddress"]; !ok {
		return nil
	}

	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	amqpChannel, err := bd.Connection.NewChannel(false)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	defer amqpChannel.Close()

	// setup exchange/queue/binding
	subjects := make([]string, 0)
	sourceName := fmt.Sprintf("%s.dlq", strings.Replace(origSource.GetName(), ".quorum", "", 1))
	subject := origSource.GetName()
	if dls, ok := opts["DeadLetterSubject"]; ok {
		subject = dls
	}

	subjects = append(subjects, subject)

	source := &pb.Source{
		Name: sourceName,
		Type: pb.Source_TEMPORARY,
		Address: &pb.Address{
			Subjects: subjects,
			Type:     pb.Address_TOPIC,
			Name:     opts["DeadLetterAddress"],
		},
	}

	_ = prov.declareExchange(source.GetAddress(), bd, amqpChannel)

	_ = prov.declareQueue(source, bd, amqpChannel, true)

	err = prov.declareBinding(source, bd, amqpChannel, true)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	return nil
}

func addressTypeToAmqpType(aType pb.Address_TargetType) (string, error) {
	var exchangeType string
	switch aType {
	case pb.Address_TOPIC:
		exchangeType = "topic"
	case pb.Address_FILTER:
		exchangeType = "headers"
	case pb.Address_QUEUE:
		exchangeType = "direct"
	case pb.Address_STREAM:
		exchangeType = "stream"
	default:
		util.Logger.Warn(i18n.AddressTypeError, aType.String())
		return "", fmt.Errorf("%s is not a valid address type", aType)
	}
	return exchangeType, nil
}

func sourceTypeToAmqpType(source *pb.Source) (string, error) {
	var queueType string
	if source.GetAutoDelete() {
		return queueTypeClassic, nil
	}
	switch source.GetType() { //nolint:exhaustive
	case pb.Source_QUEUE:
		queueType = queueTypeQuorum
	case pb.Source_TEMPORARY:
		queueType = queueTypeClassic
	default:
		return "", fmt.Errorf("%s is not a valid source type", source.GetType())
	}
	return queueType, nil
}

func (bd *BrokerDetails) exchangeKnown(name string) bool {
	_, ok := bd.knownExchanges.Get(name)
	return ok
}

func (bd *BrokerDetails) queueKnown(name string) bool {
	_, ok := bd.knownQueues.Get(name)
	return ok
}

func (bd *BrokerDetails) bindingKnown(name string) bool {
	_, ok := bd.knownBindings.Get(name)
	return ok
}

func (bd *BrokerDetails) updateLastPubSubEvent() {
	bd.lastPubSubEvent = time.Now()
}

func (bd *BrokerDetails) incrementStreamCount() {
	atomic.AddInt64(&bd.ActiveStreams, 1)
	bd.updateLastPubSubEvent()
}

func (bd *BrokerDetails) decrementStreamCount() {
	atomic.AddInt64(&bd.ActiveStreams, -1)
	bd.updateLastPubSubEvent()
}

func (prov *amqp091provider) declareExchange(address *pb.Address, bd *BrokerDetails, amqpChannel amqp091ChannelShim) error {
	// don't try to declare an exchange with amq. in the name
	if strings.Contains(address.GetName(), "amq.") {
		return nil
	}

	known := bd.exchangeKnown(address.GetName())

	if !known {
		exchangeType, err := addressTypeToAmqpType(address.GetType())

		if err != nil {
			return err
		}
		util.Logger.Info(i18n.ExchangeDeclare, address.GetName())

		err = amqpChannel.ExchangeDeclare(address.GetName(), exchangeType, address.GetAutoDelete())
		if err != nil {
			util.Logger.Warn(i18n.ExchangeDeclareError, err.Error())
			return err
		}

		bd.knownExchanges.Add(address.GetName(), true)
	}

	if parent := address.GetParentAddress(); parent != nil {
		known = bd.exchangeKnown(parent.GetName())
		if !known {
			err := prov.declareExchange(parent, bd, amqpChannel)
			if err != nil {
				util.Logger.Warn(i18n.ExchangeDeclareError, err.Error())
			}
			bd.knownExchanges.Add(parent.GetName(), true)
		}

		// Bind each subject from the Address exchange to the ParentAddress exchange
		for _, subject := range address.GetSubjects() {
			util.Logger.Info(i18n.ExchangeBind, address.GetName(), parent.GetName(), subject)
			err := amqpChannel.ExchangeBind(address.GetName(), subject, parent.GetName())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func sourceName(source *pb.Source) string {
	if isQuorum(source) && !strings.HasSuffix(source.GetName(), ".quorum") {
		return source.GetName() + ".quorum"
	}
	return source.GetName()
}

func isQuorum(source *pb.Source) bool {
	if source.GetAutoDelete() {
		return false
	}
	return source.GetType() == pb.Source_QUEUE
}

func (prov *amqp091provider) declareQueue(source *pb.Source, bd *BrokerDetails, amqpChannel amqp091ChannelShim, force bool) error {
	known := bd.queueKnown(source.GetName())
	if known && !force {
		return nil
	}

	args := make(amqp091Table)

	tmpType, mapErr := sourceTypeToAmqpType(source)
	if mapErr != nil {
		return errors.New(mapErr.Error())
	}
	args["x-queue-type"] = tmpType

	for option, value := range source.GetOptions() {
		switch option {
		case "MessageTTL":
			val, err := strconv.Atoi(value)
			if err != nil {
				return errors.New("value for MessageTTL option must be a quoted integer")
			}
			args["x-message-ttl"] = val
		case "Expires":
			val, err := strconv.Atoi(value)
			if err != nil {
				return errors.New("value for Expires option must be a quoted integer")
			}
			args["x-expires"] = val
		case "DeadLetterAddress":
			args["x-dead-letter-exchange"] = value
		case "DeadLetterSubject":
			args["x-dead-letter-routing-key"] = value
		default:
			return fmt.Errorf("%s is an unsupported source option", option)
		}
	}

	if source.SingleActiveConsumer {
		args["x-single-active-consumer"] = true
	}

	// if an AutoDelete source and x-expires is not set, set it to 5 minutes
	if _, hasExpires := args["x-expires"]; (source.GetAutoDelete() || source.GetExclusive()) && !hasExpires {
		args["x-expires"] = int((5 * time.Minute).Milliseconds())
	}

	// The AutoDelete and Exclusive parameters are now set to false because we have experienced issues
	// related to those features (eg. x-expires on an AutoDelete queue will cause it to be deleted even if
	// it has never had a consumer). Our clients currently also do not send Exclusive to arke. The client
	// libraries will remove Exclusive and change it to AutoDelete with a UUID appended to the source.Name.
	// A better alternative for how we use rabbit is to set both of these to false and set the x-expires
	// header like we do above.
	qErr := amqpChannel.QueueDeclare(source.GetName(), false, false, args)
	if qErr != nil {
		util.Logger.Warn(i18n.QueueDeclareError, qErr.Error())
	}
	bd.knownQueues.Add(source.GetName(), true)
	return nil
}

func (bd *BrokerDetails) getManagementClient() *http.Client {
	client := &http.Client{}

	if bd.tlsEnabled {
		tr := &http.Transport{TLSClientConfig: bd.tlsConfig}
		client = &http.Client{Transport: tr}
	}
	return client
}

func (bd *BrokerDetails) doManagementRequest(method, urn string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	body, err := bd.doManagementRequestWithoutMarshal(method, urn)

	if err != nil {
		return results, err
	}

	if len(body) > 0 {
		if marshErr := json.Unmarshal(body, &results); marshErr != nil {
			return results, marshErr
		}
	}

	return results, nil
}

func (bd *BrokerDetails) doManagementRequestWithoutMarshal(method, urn string) ([]byte, error) {
	client := bd.getManagementClient()
	proto := "http"
	if bd.tlsEnabled {
		proto = "https"
	}

	adminPort := bd.connectionConfig.GetAdminPort()
	if adminPort == 0 {
		adminPort = bd.connectionConfig.Port + 10000
	}
	host := bd.connectionConfig.Host

	rurl := fmt.Sprintf("%s://%s:%d%s", proto, host, adminPort, urn)
	req, _ := http.NewRequestWithContext(context.Background(), method, rurl, nil)
	req.SetBasicAuth(bd.connectionConfig.GetCredentials().GetUsername(), bd.connectionConfig.GetCredentials().GetPassword())
	req.Header.Add("Accept", "application/json")
	resp, respErr := client.Do(req)

	var body []byte
	var bodyErr error
	if resp != nil {
		defer resp.Body.Close()
		body, bodyErr = io.ReadAll(resp.Body)
	}

	if respErr != nil { //nolint:gocritic
		err := fmt.Errorf("Error retrieving bindings: %s", respErr.Error())
		return nil, err
	} else if resp == nil {
		err := fmt.Errorf("Error %s on management request %s: no response", method, rurl)
		return nil, err
	} else if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		err := fmt.Errorf("Error %s on management request %s: request returned a %d", method, rurl, resp.StatusCode)
		return body, err
	}

	if bodyErr != nil {
		return body, bodyErr
	}

	if resp.StatusCode == 204 {
		return body, nil
	}

	return body, nil
}

func (bd *BrokerDetails) getBindingKeysForSource(source *pb.Source) []map[string]interface{} {
	var results []map[string]interface{}

	exchange := url.QueryEscape(source.GetAddress().GetName())
	queue := url.QueryEscape(source.GetName())
	vhost := bd.connectionConfig.GetTenant()
	if vhost == "" {
		vhost = "/"
	}
	vhost = url.QueryEscape(vhost)

	urn := fmt.Sprintf("/api/bindings/%s/e/%s/q/%s/", vhost, exchange, queue)
	results, err := bd.doManagementRequest("GET", urn)

	if err != nil {
		util.Logger.Debugf("Error listing bindings for %s: %s", queue, err.Error())
	}

	return results
}

func (bd *BrokerDetails) deleteBindingByKeyFromSource(source *pb.Source, propKey string) error {
	exchange := url.QueryEscape(source.GetAddress().GetName())
	queue := url.QueryEscape(source.GetName())
	vhost := bd.connectionConfig.GetTenant()
	if vhost == "" {
		vhost = "/"
	}
	vhost = url.QueryEscape(vhost)

	urn := fmt.Sprintf("/api/bindings/%s/e/%s/q/%s/%s/", vhost, exchange, queue, propKey)
	_, err := bd.doManagementRequest("DELETE", urn)

	if err != nil {
		util.Logger.Debugf("Error deleting binding %s from %s: %s", propKey, queue, err.Error())
		return err
	}

	return nil
}

func (bd *BrokerDetails) cleanupBindings(source *pb.Source, subjects []string) []string {
	bindings := bd.getBindingKeysForSource(source)
	removed := make([]string, 0)
	for _, binding := range bindings {
		bindingExpected := false
		routingKey := binding["routing_key"].(string)
		for _, subject := range subjects {
			if routingKey == subject {
				bindingExpected = true
				break
			}
		}
		if !bindingExpected {
			util.Logger.Debugf("Deleting binding %s for routing key %s from %s\n", binding["properties_key"], binding["routing_key"], source.GetName())
			err := bd.deleteBindingByKeyFromSource(source, binding["properties_key"].(string))
			if err != nil {
				util.Logger.Debugf("Error deleting binding: %s", err.Error())
			} else {
				removed = append(removed, binding["properties_key"].(string))
			}
		}
	}
	return removed
}

func (prov *amqp091provider) declareBinding(source *pb.Source, bd *BrokerDetails, amqpChannel amqp091ChannelShim, force bool) error { //nolint:gocognit,unparam
	knownBindingKey := fmt.Sprintf("%s:%s", source.GetName(), strings.Join(source.Address.GetSubjects(), ":"))
	known := bd.bindingKnown(knownBindingKey)
	if known && !force {
		return nil
	}

	// If the address has subjects, bind to each subject.
	// But if the address has no subjects, bind without a subject. Don't do both.
	util.Logger.Info(i18n.Binding, source.GetName(), strings.Join(source.GetAddress().GetSubjects(), ","), source.GetAddress().GetName())

	matchHeadersList := make([]amqp091Table, 0)

	if source.GetAddress().GetType() == pb.Address_FILTER {
		for _, filter := range source.GetFilters() {
			matchHeaders := make(amqp091Table)
			matches := filter.GetMatches()
			for _, match := range matches {
				util.Logger.Debugf("match: %v", match)
				matchHeaders[match.GetName()] = match.GetValue()
			}

			if len(matchHeaders) > 0 {
				matchHeaders["x-match"] = "all"
				if filter.GetType() == pb.Filter_ANY {
					matchHeaders["x-match"] = xMatchAny
				}
			}

			if len(matchHeaders) > 0 {
				util.Logger.Debugf("Arguments (matches): %s", matchHeaders)
			}

			matchHeadersList = append(matchHeadersList, matchHeaders)
		}
	}

	subjects := source.GetAddress().GetSubjects()
	if len(subjects) == 0 && len(matchHeadersList) > 0 {
		// If subjects aren't included in the address, fake an empty one so
		// we ensure we bind unless we have no Filters
		subjects = append(subjects, "")
	}

	for _, subject := range subjects {
		if len(matchHeadersList) > 0 {
			for _, matchHeaders := range matchHeadersList {
				bErr := amqpChannel.QueueBind(source.GetName(), subject, source.GetAddress().GetName(), matchHeaders)
				if bErr != nil {
					util.Logger.Warn(i18n.QueueBindError, bErr.Error())
				}
			}
		} else {
			bErr := amqpChannel.QueueBind(source.GetName(), subject, source.GetAddress().GetName(), nil)
			if bErr != nil {
				util.Logger.Warn(i18n.QueueBindError, bErr.Error())
			}
		}
	}

	removed := bd.cleanupBindings(source, subjects)
	util.Logger.Tracef("removed %d bindings from %s", len(removed), source.GetName())

	bd.knownBindings.Add(knownBindingKey, true)
	return nil
}

// Subscribe subscribe to a stream of messages from the broker
func (prov *amqp091provider) Subscribe(ctx context.Context, source *pb.Source, messageChannel chan<- *pb.Message) *pb.Error {
	if source.GetAddress().GetName() == "" {
		return &pb.Error{Message: "address name not defined"}
	}

	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	bd.updateLastPubSubEvent()
	if source.GetType() == pb.Source_STREAM {
		return prov.streamSubscribe(ctx, bd, source, messageChannel)
	}
	return prov.queueSubscribe(ctx, bd, source, messageChannel)
}

func (prov *amqp091provider) queueSubscribe(ctx context.Context, bd *BrokerDetails, source *pb.Source, messageChannel chan<- *pb.Message) *pb.Error { //nolint:gocognit
	if bd.Connection.IsClosed() {
		return &pb.Error{Message: "connection to broker is closed"}
	}

	// AutoDelete queues are temporary, override what
	// we get from the client
	if source.GetAutoDelete() {
		source.Type = pb.Source_TEMPORARY
	}
	source.Name = sourceName(source)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var subSpan trace.Span
	_, subSpan = tracing.SpanFromHeaders(newCtx, nil, source.GetAddress().GetName()+" subscribe setup", trace.SpanKindInternal)
	subSpan.SetAttributes(attribute.String("source.name", source.GetName()),
		attribute.String("messaging.client_id", bd.ClientIdentifier))

	amqpChannel, err := bd.Connection.NewChannel(false)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	defer amqpChannel.Close()

	_ = prov.declareExchange(source.GetAddress(), bd, amqpChannel)

	subSpan.AddEvent("address created")

	err = prov.declareQueue(source, bd, amqpChannel, true)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	subSpan.AddEvent("queue created")

	err = prov.declareBinding(source, bd, amqpChannel, true)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	subSpan.AddEvent("binding created")

	prov.setupDeadLetter(ctx, source)

	subSpan.AddEvent("dead letter address created")

	if source.GetDeclareOnly() {
		// if we reach here, everything has succeeded and we should return from Consume if source.DeclareOnly = true
		return nil
	}

	if source.GetPrefetchCount() > 0 {
		err := amqpChannel.SetPrefetch(int(source.GetPrefetchCount()))
		// if SetPrefetch fails, we need to get out because this could
		// setup a firehose for a client who isn't expecting it
		if err != nil {
			return &pb.Error{Message: err.Error()}
		}
	}

	subSpan.AddEvent("starting consume")
	messages, err := amqpChannel.Consume(
		source.GetName(),      // queue name
		false,                 // auto-ack
		source.GetExclusive(), // exclusive
	)

	if err != nil {
		util.Logger.Warn(i18n.ClientSubscribeError, bd.ClientIdentifier, source.GetName(), err.Error())
		return &pb.Error{Message: err.Error()}
	}

	util.Logger.Info(i18n.ClientSubscribe, bd.ClientIdentifier, source.GetName())

	connErrChan := make(chan amqp091Error)
	connErrChan = bd.Connection.NotifyClose(connErrChan)
	cancelChan := make(chan amqp091Error)
	cancelChan = amqpChannel.NotifyClose(cancelChan)

	defer func() {
		// try to send on the channel and if we can't it's
		// probably not receiving on the other end for some
		// reason
		select {
		case connErrChan <- newAmqp091Error("Subscribe done", 2001):
			return
		default:
			return
		}
	}()

	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	defer func() *pb.Error {
		if err := recover(); err != nil {
			util.Logger.Debugf("recovered: %v", err)
			return &pb.Error{Message: fmt.Sprintf("%v", err), IsFatal: true}
		}
		return nil
	}()

	subSpan.AddEvent("ending subscribe setup")
	subSpan.End()

	for {
		select {
		case <-ctx.Done():
			return nil
		case cancelErr, ok := <-cancelChan:
			if !ok {
				util.Logger.Debugf("Channel to broker closed during subscribe %v", bd.ClientIdentifier)
				return &pb.Error{Message: "Channel to broker closed", IsFatal: true}
			}

			if cancelErr != (amqp091Error{}) {
				util.Logger.Debugf("Received channel notify for client during subscribe %v : %v", bd.ClientIdentifier, cancelErr)
				return &pb.Error{Message: cancelErr.Error()}
			} else if bd.state != provider.CONNECTED {
				// The connection was closed without an error on the channel, so this was expected.
				// TODO: Should we check for DISCONNECTED/CONNECTING as well?
				util.Logger.Debugf("Received channel state not connected during subscribe %v : %v", bd.ClientIdentifier, bd.state)
				return nil
			}
		case chanErr, ok := <-connErrChan:
			if !ok {
				util.Logger.Debugf("Connection to broke closed during subscribe %v", bd.ClientIdentifier)
				return &pb.Error{Message: "Connection to broker closed"}
			}

			if chanErr != (amqp091Error{}) {
				util.Logger.Debugf("Received connection notify for client during subscribe %v : %v", bd.ClientIdentifier, chanErr)
				return &pb.Error{Message: chanErr.Error()}
			} else if bd.state != provider.CONNECTED {
				// The connection was closed without an error on the channel, so this was expected.
				// TODO: Should we check for DISCONNECTED/CONNECTING as well?
				util.Logger.Debugf("Received connection state not connected during subscribe %v : %v", bd.ClientIdentifier, bd.state)
				return nil
			}
		case msg, ok := <-messages:
			if !ok {
				// Message channel closed
				return nil
			}
			// Sometimes we get a message with a DeliveryTag == 0, which is bad and I'm not sure
			// how this actually happens
			if msg.DeliveryTag == 0 {
				continue
			}

			messageUUID := util.GenUUID()
			headers := make(map[string]string)
			for header, value := range msg.Headers {
				// make everything a string
				headers[header] = fmt.Sprintf("%v", value)
			}
			if msg.ContentType != "" {
				headers["Content-Type"] = msg.ContentType
			}
			if msg.ContentEncoding != "" {
				headers["Content-Encoding"] = msg.ContentEncoding
			}
			message := &pb.Message{Uuid: messageUUID, Body: msg.Body, Headers: headers, Address: source.GetAddress()}

			_, span := tracing.SpanFromHeaders(ctx, message.GetHeaders(), source.GetAddress().GetName()+" received from broker", trace.SpanKindInternal)

			if tracing.Enabled() {
				span.SetAttributes(attribute.String("source.name", source.GetName()),
					attribute.String("messaging.client_id", bd.ClientIdentifier))

				message.Headers[tracing.HeaderTraceState] = span.SpanContext().TraceState().String()
				message.Headers[tracing.HeaderTraceParent] = fmt.Sprintf("00-%s-%s-%s",
					span.SpanContext().TraceID().String(),
					span.SpanContext().SpanID().String(),
					span.SpanContext().TraceFlags(),
				)
				msg.Headers[tracing.HeaderTraceState] = message.Headers[tracing.HeaderTraceState]
				msg.Headers[tracing.HeaderTraceParent] = message.Headers[tracing.HeaderTraceParent]
			}

			span.AddEvent("sending message from provider to server for consume")

			bd.activeMessages.Add(messageUUID, msg)
			messageChannel <- message
			atomic.AddInt64(&bd.consumed, 1)
			span.End()
		}
	}
}

func (prov *amqp091provider) streamSubscribe(ctx context.Context, bd *BrokerDetails, source *pb.Source, messageChannel chan<- *pb.Message) *pb.Error { //nolint:gocognit
	// Streams have a reduced set of supported options so they
	// are not validated by server
	validOptions := supportedStreamSourceOptions
	unsupported := make([]string, 0)
	options := source.GetOptions()
	for option := range options {
		if _, ok := validOptions[option]; !ok {
			util.Logger.Info(i18n.UnsupportedSourceOption, option)
			unsupported = append(unsupported, option)
		}
	}

	if len(unsupported) > 0 {
		errMsg := fmt.Sprintf("streams do not support the following source options: %s", unsupported)
		return &pb.Error{Message: errMsg}
	}

	if source.GetAutoDelete() || source.GetExclusive() {
		errMsg := "streams do not support AutoDelete or Exclusive"
		return &pb.Error{Message: errMsg}
	}

	strConnErr := prov.getStreamConnection(bd)
	if strConnErr != nil {
		return strConnErr
	}

	if bd.StreamConnection.IsClosed() {
		return &pb.Error{Message: "connection to broker is closed"}
	}

	offset := ""
	opts := source.GetOptions()
	if _, ok := opts["Offset"]; ok {
		offset = opts["Offset"]
	}
	var ttl int64
	if sTTL, ok := opts["MessageTTL"]; ok {
		val, err := strconv.ParseInt(sTTL, 10, 64)
		if err != nil {
			return &pb.Error{Message: "value for MessageTTL option must be a quoted integer"}
		}
		ttl = val
	}

	dErr := bd.StreamConnection.DeclareStream(source.GetName(), ttl)
	if dErr != nil {
		return &pb.Error{IsFatal: true, Message: fmt.Sprintf("failed to declare stream: %s", dErr.Error())}
	}

	if source.GetDeclareOnly() {
		// if we reach here, everything has succeeded and we should return from Consume if source.DeclareOnly = true
		return nil
	}

	latch := util.NewBlockingLatch(ctx, uint(source.GetPrefetchCount())) //nolint:gosec

	consumerName, cErr := bd.getConsumerName(source)
	if cErr != nil {
		return cErr
	}

	handleMessages := func(ctx stream.ConsumerContext, message *amqp.Message) {
		messageUUID := util.GenUUID()
		hdrs := fromStreamHeaders(message.ApplicationProperties)
		hdrs[streamOffsetHeaderName] = strconv.FormatInt(ctx.Consumer.GetOffset(), 10)

		addTimeStampHeader(hdrs)

		// Attempt to decompress stream messages if they are marked/compressed.
		data := message.GetData()

		if hdrs[transferEncodingHeaderName] == "gzip" {
			decompressedData, err := decompressBody(data)
			if err != nil {
				util.Logger.Debugf("Failed to decompress stream message: %v", err)
			} else {
				data = decompressedData
				delete(hdrs, transferEncodingHeaderName)
			}
		}

		m := &pb.Message{Uuid: messageUUID, Body: data,
			Headers: hdrs, Address: source.GetAddress()}

		consumerGroup := ""
		// if the consumer group option is set, format it for logging
		if consumerGroupOption, ok := source.GetOptions()["ConsumerGroup"]; ok && consumerGroupOption != "" {
			consumerGroup = fmt.Sprintf(" (%s)", consumerGroupOption)
		}

		// Increment the latch before we put the message on the channel
		// Increment will wait for Decrement to be called if we have hit the ceiling
		if err := latch.Increment(); err != nil {
			return
		}
		messageChannel <- m
		stm := streamMessage{Body: message.GetData(), Headers: m.GetHeaders()}
		stm.Ack = func() {
			latch.Decrement()
			if ctx.Consumer != nil {
				consumerOffset := ctx.Consumer.GetOffset()
				util.Logger.Tracef("Ack of message(%s) on stream %s%s with offset %d", messageUUID, ctx.Consumer.GetStreamName(), consumerGroup, consumerOffset)

				offsetErr := bd.StreamConnection.StoreOffset(source.GetName(), consumerName, consumerOffset)
				if offsetErr != nil {
					util.Logger.Debugf("Ack of message(%s) on stream %s%s failed : %s", messageUUID, ctx.Consumer.GetStreamName(), consumerGroup, offsetErr.Error())
				}
			}
		}
		stm.Nack = func() {
			util.Logger.Tracef("Nack of message(%s) on stream %s%s with offset %d", messageUUID, ctx.Consumer.GetStreamName(), consumerGroup, ctx.Consumer.GetOffset())
			latch.Decrement()
		}
		bd.activeMessages.Add(messageUUID, stm)
		atomic.AddInt64(&bd.consumed, 1)
	}

	consumer, _ := bd.StreamConnection.NewConsumer(source.GetName(), consumerName, offset, handleMessages, source.GetSingleActiveConsumer())
	bd.incrementStreamCount()
	defer bd.decrementStreamCount()
	<-ctx.Done()
	consumer.Close()
	return nil
}

func addTimeStampHeader(hdrs map[string]string) {
	if val, ok := hdrs[rabbitReceivedTimeHeaderName]; ok {
		hdrs[timeStampInMSHeaderName] = val
	}
}

// Disconnect disconnect from the broker
func (prov *amqp091provider) Disconnect(ctx context.Context) {
	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		return
	}

	prov.disconnectClientByIdentifier(clientIdentifier)
}

func (prov *amqp091provider) disconnectClientByIdentifier(clientIdentifier string) {
	var bd *BrokerDetails
	if bdu, ok := prov.connections.Get(clientIdentifier); ok {
		bd = bdu.(*BrokerDetails)
	} else {
		return
	}

	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()

	bd.pubChannelCancel()
	bd.clientDisconnect = true
	util.Logger.Info(i18n.ClientDisconnect, bd.ClientIdentifier)
	bd.shutdownChan <- true // shut down the connectionWatcher
	// close the client if it is still connected
	if bd.Connection != nil && !bd.Connection.IsClosed() {
		bd.Connection.Close()
	}

	if bd.StreamConnection != nil {
		bd.StreamConnection.Close()
	}
	prov.connections.Delete(clientIdentifier)

	bd = nil //nolint:wastedassign
}

// Publish publish a message to the broker
func (prov *amqp091provider) Publish(ctx context.Context, messageChannel <-chan *pb.Message, errChan chan<- *pb.Error) *pb.Error {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	bd.updateLastPubSubEvent()
	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	if bd.Connection.IsClosed() {
		return &pb.Error{Message: "connection to broker is closed"}
	}

	amqpChannel, err := bd.Connection.NewChannel(false)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	defer amqpChannel.Close()

	connErrChan := make(chan amqp091Error)
	connErrChan = bd.Connection.NotifyClose(connErrChan)
	cancelChan := make(chan amqp091Error)
	cancelChan = amqpChannel.NotifyClose(cancelChan)

	defer func() {
		// try to send on the channel and if we can't it's
		// probably not receiving on the other end for some
		// reason
		select {
		case connErrChan <- newAmqp091Error("Publish done", 2002):
			return
		default:
			return
		}
	}()

	for {
		select {
		case cancelErr, ok := <-cancelChan:
			if !ok {
				util.Logger.Debugf("Channel to broker closed during publish %v", bd.ClientIdentifier)
				return &pb.Error{Message: "Channel to broker closed"}
			}

			if cancelErr != (amqp091Error{}) {
				util.Logger.Debugf("Received channel notify for client during publish %v : %v", bd.ClientIdentifier, cancelErr)
				return &pb.Error{Message: cancelErr.Error()}
			} else if bd.state != provider.CONNECTED {
				// The connection was closed without an error on the channel, so this was expected.
				// TODO: Should we check for DISCONNECTED/CONNECTING as well?
				util.Logger.Debugf("Received channel state not connected during publish %v : %v", bd.ClientIdentifier, bd.state)
				return nil
			}
		case chanErr, ok := <-connErrChan:
			if !ok {
				util.Logger.Debugf("Connection to broker closed during publish %v", bd.ClientIdentifier)
				return &pb.Error{Message: "Connection to broker closed"}
			}

			if chanErr != (amqp091Error{}) {
				util.Logger.Debugf("Received connection notify for client during publish %v : %v", bd.ClientIdentifier, chanErr)
				retError := &pb.Error{Message: chanErr.Error()}
				return retError
			} else if bd.state != provider.CONNECTED {
				// The connection was closed without an error on the channel, so this was expected.
				// TODO: Should we check for DISCONNECTED/CONNECTING as well?
				util.Logger.Debugf("Received connection state not connected during publish %v : %v", bd.ClientIdentifier, bd.state)
				return nil
			}
		case message := <-messageChannel:
			if message == nil {
				// nil message means shut it down
				return nil
			}
			if message.GetConfirm() {
				errChan <- &pb.Error{Message: "Unsupported: Publish does not support publish confirmation"}
			}
			mCtx := context.Background()
			errChan <- prov.prepareAndSend(mCtx, message, bd, amqpChannel)
		}
	}
}

func (prov *amqp091provider) PublishOne(ctx context.Context, msg *pb.Message) *pb.Error {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	bd.updateLastPubSubEvent()

	var pubErr *pb.Error
	switch msg.GetAddress().GetType() { //nolint:exhaustive
	case pb.Address_STREAM:
		pubErr = prov.publishOneStream(ctx, msg, bd)
	default:
		pubErr = prov.publishOneQueue(ctx, msg, bd)
	}

	return pubErr
}

func (prov *amqp091provider) publishOneQueue(ctx context.Context, msg *pb.Message, bd *BrokerDetails) *pb.Error {
	if bd.Connection.IsClosed() {
		return &pb.Error{Message: "connection to broker is closed"}
	}
	if msg.GetPublishId() > 0 {
		return &pb.Error{Message: "Message deduplication is not supported by Queue types"}
	}

	var amc any
	if msg.GetConfirm() {
		amc = bd.pubPCChannels.Get()
	} else {
		amc = bd.pubChannels.Get()
	}
	if amc == nil {
		return &pb.Error{Message: "failed to get channel from pool"}
	}
	amqpChannel := amc.(*amqp091ChannelShim)
	if msg.GetConfirm() {
		defer func() {
			if err := bd.pubPCChannels.Put(amqpChannel); err != nil {
				util.Logger.Debugf("Failed to return channel to pool: %s", err.Error())
				(*amqpChannel).Close()
			}
		}()
	} else {
		defer func() {
			if err := bd.pubChannels.Put(amqpChannel); err != nil {
				util.Logger.Debugf("Failed to return channel to pool: %s", err.Error())
				(*amqpChannel).Close()
			}
		}()
	}

	return prov.prepareAndSend(ctx, msg, bd, *amqpChannel)
}

func (prov *amqp091provider) publishOneStream(ctx context.Context, msg *pb.Message, bd *BrokerDetails) *pb.Error {
	strConnErr := prov.getStreamConnection(bd)
	if strConnErr != nil {
		return strConnErr
	}

	if bd.StreamConnection.IsClosed() {
		return &pb.Error{Message: "connection to broker is closed"}
	}

	if msg.GetPublishId() > 0 && msg.GetPublisherName() == "" {
		return &pb.Error{Message: "PublisherName not set on message, PublisherName is required when PublishID is set"}
	}

	publisher := bd.StreamConnection.GetPublisher(msg.GetAddress().GetName(), msg.GetPublisherName(), msg.GetConfirm())
	if publisher == nil {
		return &pb.Error{Message: "connected to broker, but failed to create a stream publisher"}
	}
	defer bd.StreamConnection.PutPublisher(msg.GetConfirm(), publisher)

	return prov.streamPrepareAndSend(ctx, msg, bd, publisher)
}

func (prov *amqp091provider) getStreamConnection(bd *BrokerDetails) *pb.Error {
	// Not all of our clients are using streams, so we only connect if streams are used.
	if bd.StreamConnection == nil {
		connStr := getStreamConnectionString(bd)
		bd.Lock()
		bd.StreamConnection = NewStreamConn(connStr, bd.ClientIdentifier, bd.tlsConfig)
		bd.Unlock()
		connErr := bd.StreamConnection.Connect()
		if connErr != nil {
			return &pb.Error{Message: fmt.Sprintf("failed to create stream connection to broker: %s", connErr.Error())}
		}
	}
	return nil
}

func (prov *amqp091provider) prepareAndSend(ctx context.Context, msg *pb.Message, bd *BrokerDetails, amqpChannel amqp091ChannelShim) *pb.Error {
	_, span := tracing.SpanFromHeaders(ctx, msg.GetHeaders(), msg.GetAddress().GetName()+" publish", trace.SpanKindProducer)

	span.SetAttributes(attribute.String("subject.name", msg.GetAddress().GetSubjects()[0]),
		attribute.String("messaging.client_id", bd.ClientIdentifier))

	address := msg.GetAddress()
	deliveryMode := 1
	if msg.GetPersistent() {
		deliveryMode = 2
	}

	_ = prov.declareExchange(msg.GetAddress(), bd, amqpChannel)
	span.AddEvent("address created")

	amqpMessage := amqp091Message{}
	amqpMessage.Body = msg.GetBody()
	amqpMessage.DeliveryMode = deliveryMode

	headers := amqp091Table{}

	for headerName, headerValue := range msg.GetHeaders() {
		headers[headerName] = headerValue
		switch headerName {
		case "Content-Type":
			amqpMessage.ContentType = headerValue
		case "Content-Encoding":
			amqpMessage.ContentEncoding = headerValue
		}
	}

	amqpMessage.Headers = headers

	err := amqpChannel.Publish(
		address.GetName(),        // exchange
		address.GetSubjects()[0], // routing key
		amqpMessage)

	span.AddEvent("message published to broker")

	if err != nil {
		util.Logger.Warn(i18n.PublishError, err.Error())
		span.RecordError(err)
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}

	util.Logger.Trace(i18n.ClientPublished, bd.ClientIdentifier)
	atomic.AddInt64(&bd.produced, 1)
	span.End()

	return nil
}

func (prov *amqp091provider) streamPrepareAndSend(ctx context.Context, msg *pb.Message, bd *BrokerDetails, publisher streamPublisherShim) *pb.Error {
	_, span := tracing.SpanFromHeaders(ctx, msg.GetHeaders(), msg.GetAddress().GetName()+" publish", trace.SpanKindProducer)
	defer span.End()

	span.SetAttributes(attribute.String("subject.name", msg.GetAddress().GetSubjects()[0]),
		attribute.String("messaging.client_id", bd.ClientIdentifier))

	strMsg := streamMessage{Body: msg.GetBody()}
	strMsg.Headers = msg.GetHeaders()
	strMsg.PublishID = msg.GetPublishId()
	err := publisher.Publish(strMsg)
	span.AddEvent("message published to stream")

	if err != nil {
		util.Logger.Warn(i18n.PublishError, err.Error())
		span.RecordError(err)
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		return errMsg
	}

PCLoop:
	// We do not need to wait for a confirmation if
	// confirm is false
	for msg.Confirm {
		select {
		// Not setting a timer here because the Publisher should
		// timeout after 5 seconds
		case <-ctx.Done():
			return &pb.Error{Message: "Publish interrupted before confirmation received", IsFatal: true}
		case status := <-publisher.GetPCChannel():
			// Should we check message ID here?
			if status.IsConfirmed() {
				break PCLoop
			} else {
				return &pb.Error{Message: status.GetError().Error()}
			}
		}
	}
	util.Logger.Trace(i18n.ClientPublished, bd.ClientIdentifier)
	atomic.AddInt64(&bd.produced, 1)
	return nil
}

// SupportSourceOptions returns a map[string]bool of support options for Source.Options
func (prov *amqp091provider) SupportedSourceOptions() map[string]bool {
	return supportedSourceOptions
}

// WaitForConnect returns true if connected, false if connection fails
func (prov *amqp091provider) WaitForConnect(ctx context.Context) bool {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return false
	}

	// to prevent unwanted disconnects for a client with a single stream
	// we need to increment the stream count if we are waiting for provider connect
	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	for start := time.Now(); time.Since(start) < provider.CONNECTTIMEOUT*time.Second; {
		if bd.state == provider.CONNECTED {
			util.Logger.Info(i18n.ClientConnected, bd.ClientIdentifier)
			return true
		}
		bd, err = prov.getBrokerDetails(ctx)
		if err != nil {
			util.Logger.Info(i18n.ClientDetailsGone, bd.ClientIdentifier)
			return false
		}

		sleepRandomReconnect()
	}
	return false
}

func (bd *BrokerDetails) updateStatsForStream(source *pb.Source, stats *pb.SourceStats) {
	// create a new consumer with a fake consumer group name so we can find the offset at 'last'
	fakeConsumerName := "arkeSourceStatsConsumer"
	handleMessages := func(cc stream.ConsumerContext, _ *amqp.Message) {
		if cc.Consumer != nil {
			// store the offset so requests to QueryOffset will be correct
			_ = bd.StreamConnection.StoreOffset(source.GetName(), fakeConsumerName, cc.Consumer.GetOffset())
		}
	}

	cons, err := bd.StreamConnection.NewConsumer(source.GetName(), fakeConsumerName, "last", handleMessages, false)

	if err == nil {
		defer cons.Close()
		offset, oErr := bd.StreamConnection.GetLastOffset(source.GetName(), fakeConsumerName)
		stats.LastOffset = offset
		if oErr != nil {
			stats.Error = &pb.Error{
				Message: oErr.Error(),
			}
			return
		}

		consumerName, cErr := bd.getConsumerName(source)
		if cErr != nil {
			stats.Error = cErr
			return
		}
		// Ignore the error here, we will get an error if we have never stored
		// an offset(aka. never consumed)
		offset, _ = bd.StreamConnection.GetLastOffset(source.GetName(), consumerName)
		stats.CurrentOffset = offset
	} else {
		util.Logger.Debugf("failed to create new arkeSourceStatsConsumer for stats: %s", err.Error())
	}
}

func (bd *BrokerDetails) getConsumerName(source *pb.Source) (string, *pb.Error) {
	consumerName := source.GetName()
	if source.GetSingleActiveConsumer() {
		if cg, ok := source.GetOptions()["ConsumerGroup"]; ok && cg != "" {
			consumerName = cg
		} else {
			util.Logger.Debugf("%s requested single active consumer on stream %s but source.Options['ConsumerGroup'] was not set",
				bd.ClientIdentifier, source.GetName())
			emsg := "single active consumer requested but no ConsumerGroup option set"
			return "", &pb.Error{Message: emsg}
		}
	}
	return consumerName, nil
}

func (bd *BrokerDetails) getStreamOrQueueStats(source *pb.Source) *pb.SourceStats {
	stats := &pb.SourceStats{}
	var results map[string]interface{}

	queue := url.QueryEscape(sourceName(source))
	vhost := bd.connectionConfig.GetTenant()
	if vhost == "" {
		vhost = "/"
	}
	vhost = url.QueryEscape(vhost)

	urn := fmt.Sprintf("/api/queues/%s/%s", vhost, queue)
	body, err := bd.doManagementRequestWithoutMarshal("GET", urn)
	if marshErr := json.Unmarshal(body, &results); marshErr != nil {
		stats.Error = &pb.Error{Message: marshErr.Error()}
		return stats
	}
	util.Logger.Debugf("Stats results from management API for %s: %s", queue, string(body))

	// consumer count comes from the API and is accurate
	if consumersCountRaw, ok := results["consumers"]; ok {
		stats.ConsumerCount = int32(consumersCountRaw.(float64))
	}

	var mgmtStats rabbitMQManagementMetrics
	if err := json.Unmarshal(body, &mgmtStats); err != nil {
		util.Logger.Debugf("Failed to unmarshal management API response into RabbitMQManagementMetrics struct: %s", err.Error())
	} else {
		stats.PublishRate = float32(mgmtStats.MessageStats.PublishDetails.Rate)
		stats.DeliverRate = float32(mgmtStats.MessageStats.DeliverDetails.Rate)
	}

	switch source.Type { //nolint:exhaustive
	case pb.Source_QUEUE:
		stats.MessageCount = int64(mgmtStats.Messages)
	case pb.Source_STREAM:
		bd.updateStatsForStream(source, stats)
	}

	if err != nil {
		util.Logger.Debugf("Error retrieving queue/stream from management API %s: %s", queue, err.Error())
		stats.Error = &pb.Error{Message: err.Error()}
		return stats
	}

	return stats
}

func (prov *amqp091provider) SourceStats(ctx context.Context, source *pb.Source) *pb.SourceStats {
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

	if source.GetType() == pb.Source_STREAM {
		prov.getStreamConnection(bd)
	}

	return bd.getStreamOrQueueStats(source)
}

func sleepRandomReconnect() {
	util.SleepRandom(500, provider.ReconnectDelay)
}

// connectionWatcher Called at the end of BrokerDetails.connect(), we monitor the bd.ErrorChannel and try to reconnect
// if we get an error on the channel. Receiving nil on the channel means we've closed because of the client
func (bd *BrokerDetails) connectionWatcher() {
	for !bd.clientDisconnect {
		select {
		case <-bd.shutdownChan:
			return
		case err, ok := <-bd.ErrorChannel:
			bd.Lock()
			if !ok || (err != (amqp091Error{}) && err.Code() != 0) {
				bd.state = provider.DISCONNECTED
				sleepRandomReconnect()
				bd.Unlock()
				// Ignore this error because we will reconnect in 30 seconds
				bd.connect() //nolint:errcheck
				continue
			}
			bd.Unlock()
		case <-time.After(30 * time.Second):
			// if we never get an error on the bd.ErrorChannel, try again after 30 seconds
			// this is to help deal with race condition where we're not listening on the bd.ErrorChannel
			// when there is an error on the connection
			if bd.Connection.IsClosed() {
				bd.Lock()
				bd.state = provider.DISCONNECTED
				bd.Unlock()
				// Ignore this error because we will reconnect in 30 seconds
				bd.connect() //nolint:errcheck
			}
			continue
		}
	}
}

// waitWhileConnecting waits up to 30 seconds for an in-progress connection attempt to resolve.
// It returns the resulting state: CONNECTED, CLOSED, or DISCONNECTED (also used for timeouts).
func (bd *BrokerDetails) waitWhileConnecting() uint16 {
	for start := time.Now(); time.Since(start) < 30*time.Second; {
		switch bd.state {
		case provider.CONNECTED:
			return provider.CONNECTED
		case provider.CONNECTING:
			time.Sleep(100 * time.Millisecond)
		case provider.CLOSED:
			return provider.CLOSED
		case provider.DISCONNECTED:
			return provider.DISCONNECTED
		}
	}
	return provider.DISCONNECTED
}

func (bd *BrokerDetails) connect() (bool, error) {
	if bd.clientDisconnect {
		return false, nil
	}

	if bd.state == provider.CONNECTING {
		switch bd.waitWhileConnecting() {
		case provider.CONNECTED:
			return true, nil
		case provider.CLOSED:
			return false, nil
		}
	}

	bd.Lock()
	defer bd.Unlock()
	if bd.state == provider.CONNECTED {
		return true, nil
	}

	bd.state = provider.CONNECTING
	var conn amqp091ConnectionShim
	var err error

	// Reinitialize these maps early, we especially want to
	// ensure activeMessages gets cleared out before an Ack/Nacks
	// are sent from the client.
	bd.knownExchanges = util.NewConcurrentMap()
	bd.knownQueues = util.NewConcurrentMap()
	bd.knownBindings = util.NewConcurrentMap()
	bd.activeMessages = util.NewConcurrentMap()

	cf := bd.connectionConfig

	var tenant = cf.GetTenant()
	if tenant == "" {
		tenant = "/"
	}

	util.Logger.Info(i18n.ClientConnect, bd.ClientIdentifier, cf.GetHost())

	scheme := "amqp"

	// Use TLS in these scenarios:
	// * ConnectionConfiguration.TLS = true
	// * ConnectionConfiguration.CaCertificate is not empty
	if cf.GetTls() || len(cf.GetCaCertificate()) > 0 {
		bd.tlsEnabled = true
		scheme = "amqps"
	}

	var connStr string

	// skip verification if true
	if bd.tlsEnabled && bd.tlsSkipVerify {
		util.Logger.Debugf("%s connecting with TLS enabled but verification off: %s:%d", bd.ClientIdentifier, cf.GetHost(), cf.GetPort())
		bd.tlsConfig.InsecureSkipVerify = true // deepcode ignore TooPermissiveTrustManager: PSGO-2002
	} else if !bd.tlsEnabled { // no tls
		util.Logger.Debugf("%s connecting without TLS: %s:%d", bd.ClientIdentifier, cf.GetHost(), cf.GetPort())
	}

	connStr = fmt.Sprintf("%s://%s:%s@%s:%d/%s", scheme, cf.GetCredentials().GetUsername(),
		cf.GetCredentials().GetPassword(), cf.GetHost(), cf.GetPort(), tenant)

	conn = NewAmqpConn091(connStr, bd.ClientIdentifier, bd.tlsConfig)
	err = conn.Connect()

	if err != nil {
		util.Logger.Warn(i18n.BrokerConnectError, err.Error())
		bd.state = provider.CLOSED
		return false, err
	}

	bd.Connection = conn
	bd.ErrorChannel = make(chan amqp091Error)
	bd.ErrorChannel = bd.Connection.NotifyClose(bd.ErrorChannel) // this looks unneeded but it aids in unit testing
	bd.state = provider.CONNECTED

	util.Logger.Info(i18n.ClientConnected, bd.ClientIdentifier)

	// pre-load the list of exchanges to help prevent declaration
	// errors (PSGO-2001)
	bd.loadExchanges()

	return true, nil
}

func (bd *BrokerDetails) loadExchanges() {
	var results []map[string]interface{}

	vhost := bd.connectionConfig.GetTenant()
	if vhost == "" {
		vhost = "/"
	}
	vhost = url.QueryEscape(vhost)

	urn := fmt.Sprintf("/api/exchanges/%s", vhost)
	body, err := bd.doManagementRequestWithoutMarshal("GET", urn)
	if err != nil {
		util.Logger.Debugf("Error retrieving exchanges from management API: %s", err.Error())
		return
	}
	if marshErr := json.Unmarshal(body, &results); marshErr != nil {
		util.Logger.Debugf("Error unmarshaling exchanges from management API: %s", err.Error())
		return
	}
	util.Logger.Debugf("Loaded exchanges from management API: %s", string(body))

	for _, exchange := range results {
		if name, ok := exchange["name"].(string); ok {
			util.Logger.Debugf("Adding Exchange to known list: %s", name)
			bd.knownExchanges.Add(name, true)
		}
	}
}

func (prov *amqp091provider) Stats() *provider.Stats {
	stats := &provider.Stats{}
	stats.Clients = make([]*provider.ClientStats, 0)
	for _, connID := range prov.connections.GetList() {
		clientStat := &provider.ClientStats{}
		connRaw, exists := prov.connections.Get(connID)
		if !exists {
			continue
		}
		conn := connRaw.(*BrokerDetails)
		clientStat.ID = conn.ClientIdentifier
		clientStat.ActiveMessages = conn.activeMessages.Length()
		clientStat.Streams = int(conn.ActiveStreams)
		clientStat.Produced = int(conn.produced)
		clientStat.Consumed = int(conn.consumed)
		stats.Clients = append(stats.Clients, clientStat)
	}
	return stats
}
