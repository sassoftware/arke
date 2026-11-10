// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Package stream wraps the RabbitMQ Stream client without provider state.
package stream

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	streamclient "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/sassoftware/arke/internal/util"
)

const (
	maxProducersPerClient = 100
	maxProducersPerPool   = 10
	maxConsumersPerClient = 10
)

var errConnectionClosed = errors.New("stream connection is closed")

// Message is a provider-neutral RabbitMQ Stream message.
type Message struct {
	Body            []byte
	ContentType     string
	ContentEncoding string
	Headers         map[string]string
	PublishID       int64
}

// Delivery is a normalized message delivered from a Stream consumer.
type Delivery struct {
	Message    Message
	StreamName string
	Offset     int64
}

// Confirmation reports the result of publishing one message.
type Confirmation struct {
	Confirmed    bool
	PublishingID int64
	Err          error
}

// Publisher publishes messages to a stream.
type Publisher interface {
	Publish(Message) error
	Confirmations() <-chan Confirmation
	StreamName() string
	PublisherName() string
	Close() error
}

// Consumer represents an active Stream consumer.
type Consumer interface {
	Close() error
}

// Connection owns the RabbitMQ Stream environment and its transport helpers.
type Connection interface {
	Connect() error
	Close() error
	IsClosed() bool
	GetPublisher(streamName, publisherName string, confirm bool) Publisher
	PutPublisher(confirm bool, publisher Publisher)
	NewConsumer(streamName, consumerName, offset string, handler func(Delivery), singleActive bool) (Consumer, error)
	DeclareStream(streamName string, ttl int64) error
	GetConsumerOffset(streamName, consumerName string) (int64, error)
	StoreOffset(streamName, consumerName string, offset int64) error
	GetStreamOffset(streamName string) (int64, error)
}

// NewConnection creates a lazy Stream connection. The URI is passed through to
// the RabbitMQ client so providers can retain ownership of URI construction.
func NewConnection(uri, clientIdentifier string, tlsConfig *tls.Config) Connection {
	ctx, cancel := context.WithCancel(context.Background())
	return &connection{
		uri:              uri,
		tlsConfig:        tlsConfig,
		clientIdentifier: clientIdentifier,
		publishers:       util.NewConcurrentMap(),
		ctx:              ctx,
		cancel:           cancel,
	}
}

type connection struct {
	mu               sync.Mutex
	env              streamEnvironment
	uri              string
	tlsConfig        *tls.Config
	clientIdentifier string
	clientDisconnect atomic.Bool
	publishers       *util.ConcurrentMap
	ctx              context.Context
	cancel           context.CancelFunc
}

type publisher struct {
	streamName    string
	publisherName string
	producer      reliableProducer
	confirmations chan Confirmation
}

type consumer struct {
	consumer reliableConsumer
}

func (c *connection) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clientDisconnect.Load() {
		return errConnectionClosed
	}
	if c.env != nil && !c.env.IsClosed() {
		return nil
	}

	env, err := newStreamEnvironment(environmentConfig{
		uri:                   c.uri,
		tlsConfig:             c.tlsConfig,
		maxProducersPerClient: maxProducersPerClient,
		maxConsumersPerClient: maxConsumersPerClient,
	})
	if err != nil {
		if env != nil && !env.IsClosed() {
			if closeErr := env.Close(); closeErr != nil {
				return fmt.Errorf("%w (closing partially created stream environment: %v)", err, closeErr)
			}
		}
		return err
	}
	if env == nil {
		return errors.New("stream environment factory returned a nil environment")
	}
	c.env = env
	return nil
}

func (c *connection) Close() error {
	c.mu.Lock()
	if c.clientDisconnect.Swap(true) {
		c.mu.Unlock()
		return nil
	}
	c.cancel()
	env := c.env
	c.mu.Unlock()
	if env == nil || env.IsClosed() {
		return nil
	}
	return env.Close()
}

func (c *connection) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientDisconnect.Load() || c.env == nil || c.env.IsClosed()
}

func (c *connection) PutPublisher(confirm bool, pub Publisher) {
	if pub == nil {
		return
	}
	key := publisherKey(pub.StreamName(), pub.PublisherName(), confirm)
	c.mu.Lock()
	if c.clientDisconnect.Load() {
		c.mu.Unlock()
		_ = pub.Close()
		return
	}
	poolValue, ok := c.publishers.Get(key)
	if !ok {
		c.mu.Unlock()
		_ = pub.Close()
		return
	}
	err := poolValue.(*util.BlockingPool).Put(pub)
	c.mu.Unlock()
	if err != nil {
		util.Logger.Debugf("Failed to return publisher to pool: %s", err.Error())
		_ = pub.Close()
	}
}

func (c *connection) GetPublisher(streamName, publisherName string, confirm bool) Publisher {
	if c.IsClosed() {
		return nil
	}
	key := publisherKey(streamName, publisherName, confirm)
	poolValue, ok := c.publishers.Get(key)
	if !ok {
		limit := maxProducersPerPool
		if publisherName != "" {
			// The broker only supports one publisher for each publisherName.
			limit = 1
		}
		poolValue = util.NewBlockingPool(context.WithValue(c.ctx, publisherPoolContextKey{}, key), limit,
			func() any { return c.newPublisher(streamName, publisherName, confirm) })
		c.publishers.Add(key, poolValue)
	}
	pool := poolValue.(*util.BlockingPool)
	var pub any
	for i := 0; pub == nil && i < 10; i++ {
		pub = pool.Get()
		if pub == nil {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if pub == nil {
		return nil
	}
	return pub.(Publisher)
}

type publisherPoolContextKey struct{}

func publisherKey(streamName, publisherName string, confirm bool) string {
	key := fmt.Sprintf("%s-%s", streamName, publisherName)
	if confirm {
		key += "-confirm"
	}
	return key
}

func (c *connection) newPublisher(streamName, publisherName string, confirm bool) Publisher {
	if c.clientDisconnect.Load() {
		return nil
	}
	var confirmations chan Confirmation
	options := streamclient.NewProducerOptions().SetClientProvidedName(c.clientIdentifier)
	if publisherName != "" {
		options.SetProducerName(publisherName)
	}
	handler := func([]*streamclient.ConfirmationStatus) {
		// Confirmation results are intentionally discarded when confirms are disabled.
	}
	if confirm {
		options.SetConfirmationTimeOut(5 * time.Second)
		confirmations = make(chan Confirmation, 1)
		handler = func(statuses []*streamclient.ConfirmationStatus) {
			for _, status := range statuses {
				confirmations <- Confirmation{
					Confirmed:    status.IsConfirmed(),
					PublishingID: status.GetPublishingId(),
					Err:          status.GetError(),
				}
			}
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clientDisconnect.Load() || c.env == nil || c.env.IsClosed() {
		return nil
	}
	producer, err := newReliableProducer(c.env.client(), streamName, options, handler)
	if err != nil {
		util.Logger.Debugf("Error creating publisher: %v", err)
		return nil
	}
	return &publisher{streamName: streamName, publisherName: publisherName, producer: producer, confirmations: confirmations}
}

func (c *connection) NewConsumer(streamName, consumerName, offset string, handler func(Delivery), singleActive bool) (Consumer, error) {
	if handler == nil {
		return nil, errors.New("stream message handler is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clientDisconnect.Load() || c.env == nil || c.env.IsClosed() {
		return nil, errConnectionClosed
	}
	lastOffset, err := c.env.QueryOffset(consumerName, streamName)
	if err != nil {
		lastOffset = -1
	}
	specifiedOffset, err := parseOffset(offset, lastOffset)
	if err != nil {
		return nil, err
	}
	singleActiveConsumer := &streamclient.SingleActiveConsumer{}
	if singleActive {
		singleActiveConsumer.SetEnabled(true)
		singleActiveConsumer.ConsumerUpdate = func(name string, _ bool) streamclient.OffsetSpecification {
			util.Logger.Debugf("client %s with consumer %s on stream %s promoted to active consumer", c.clientIdentifier, consumerName, name)
			consumerOffset, queryErr := c.env.QueryOffset(consumerName, name)
			if queryErr != nil {
				return streamclient.OffsetSpecification{}.First()
			}
			return streamclient.OffsetSpecification{}.Offset(consumerOffset + 1)
		}
	}
	util.Logger.Debugf("Creating consumer %s for stream %s with offset %d", consumerName, streamName, specifiedOffset)
	clientHandler := func(ctx streamclient.ConsumerContext, msg *amqp.Message) {
		if msg == nil {
			return
		}
		delivery := Delivery{
			Message:    fromStreamMessage(msg),
			StreamName: streamName,
		}
		if ctx.Consumer != nil {
			delivery.StreamName = ctx.Consumer.GetStreamName()
			delivery.Offset = ctx.Consumer.GetOffset()
		}
		handler(delivery)
	}
	consumerClient, err := newReliableConsumer(c.env.client(), streamName,
		streamclient.NewConsumerOptions().SetManualCommit().
			SetClientProvidedName(c.clientIdentifier).
			SetConsumerName(consumerName).
			SetOffset(specifiedOffset).
			SetSingleActiveConsumer(singleActiveConsumer), clientHandler)
	if err != nil {
		return nil, err
	}
	return &consumer{consumer: consumerClient}, nil
}

func (c *connection) DeclareStream(streamName string, ttl int64) error {
	options := &streamclient.StreamOptions{}
	if ttl > 0 {
		options.SetMaxAge(time.Duration(ttl) * time.Second)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clientDisconnect.Load() || c.env == nil || c.env.IsClosed() {
		return errConnectionClosed
	}
	return c.env.DeclareStream(streamName, options)
}

func (c *connection) GetConsumerOffset(streamName, consumerName string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clientDisconnect.Load() || c.env == nil || c.env.IsClosed() {
		return 0, errConnectionClosed
	}
	offset, err := c.env.QueryOffset(consumerName, streamName)
	util.Logger.Debugf("GetConsumerOffset (%s)(%s)(%d) [%v]", consumerName, streamName, offset, err)
	return offset, err
}

func (c *connection) StoreOffset(streamName, consumerName string, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clientDisconnect.Load() || c.env == nil || c.env.IsClosed() {
		return errConnectionClosed
	}
	util.Logger.Tracef("StoreOffset (%s)(%s)(%d)", consumerName, streamName, offset)
	return c.env.StoreOffset(consumerName, streamName, offset)
}

func (c *connection) GetStreamOffset(streamName string) (int64, error) {
	const consumerName = "arkeSourceStatsConsumer"
	consumerClient, err := c.NewConsumer(streamName, consumerName, "last", func(delivery Delivery) {
		_ = c.StoreOffset(streamName, consumerName, delivery.Offset)
	}, false)
	if err != nil {
		return 0, err
	}
	defer consumerClient.Close()
	return c.GetConsumerOffset(streamName, consumerName)
}

func (p *publisher) Publish(msg Message) error {
	compressed, err := compressMessage(msg)
	if err != nil {
		util.Logger.Debugf("Compression failed, sending uncompressed message: %v", err)
	} else {
		msg = compressed
	}
	return p.producer.Send(toStreamMessage(msg))
}

func (p *publisher) Confirmations() <-chan Confirmation { return p.confirmations }
func (p *publisher) StreamName() string                 { return p.streamName }
func (p *publisher) PublisherName() string              { return p.publisherName }
func (p *publisher) Close() error                       { return p.producer.Close() }
func (c *consumer) Close() error                        { return c.consumer.Close() }

func fromStreamMessage(msg *amqp.Message) Message {
	result := Message{Body: msg.GetData(), Headers: headersFromStream(msg.ApplicationProperties)}
	if result.Headers[transferEncodingHeader] == "gzip" {
		decompressed, err := decompressBody(result.Body)
		if err != nil {
			util.Logger.Debugf("Failed to decompress stream message: %v", err)
		} else {
			result.Body = decompressed
			delete(result.Headers, transferEncodingHeader)
		}
	}
	if properties := msg.Properties; properties != nil {
		result.ContentType = properties.ContentType
		result.ContentEncoding = properties.ContentEncoding
	}
	return result
}

func toStreamMessage(msg Message) message.StreamMessage {
	streamMessage := amqp.NewMessage(msg.Body)
	streamMessage.ApplicationProperties = make(map[string]interface{}, len(msg.Headers))
	for key, value := range msg.Headers {
		streamMessage.ApplicationProperties[key] = value
	}
	streamMessage.Properties = &amqp.MessageProperties{ContentEncoding: msg.ContentEncoding, ContentType: msg.ContentType}
	if msg.PublishID > 0 {
		streamMessage.SetPublishingId(msg.PublishID)
	}
	_, _ = streamMessage.MarshalBinary()
	return streamMessage
}

func headersFromStream(orig map[string]interface{}) map[string]string {
	headers := make(map[string]string, len(orig))
	for key, value := range orig {
		headers[key] = value.(string)
	}
	return headers
}
