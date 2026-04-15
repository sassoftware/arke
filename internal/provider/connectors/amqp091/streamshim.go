// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/sassoftware/arke/internal/util"
)

type streamConnectionShim interface {
	Connect() error
	Close() error
	IsClosed() bool
	GetPublisher(streamName, publisherName string, confirm bool) streamPublisherShim
	PutPublisher(confirm bool, publisher streamPublisherShim)
	NewConsumer(streamName string, consumerName string, offset string, handler stream.MessagesHandler, singleActive bool) (streamConsumerShim, error)
	DeclareStream(streamName string, ttl int64) error
	GetLastOffset(streamName string, consumerName string) (int64, error)
	StoreOffset(streamName string, consumerName string, offset int64) error
}

type streamConnection struct {
	env              *stream.Environment
	envLock          sync.Mutex
	maxProducers     int
	maxConsumers     int
	connStr          string
	tlsCfg           *tls.Config
	clientIdentifier string
	clientDisconnect atomic.Bool
	publishers       *util.ConcurrentMap
	ctx              context.Context
	cancel           context.CancelFunc
}

type streamPublisherShim interface {
	Publish(streamMessage) error
	GetPCChannel() chan streamMessageResponseShim
	GetStreamName() string
	GetPublisherName() string
	Close() error
}

type streamPublisher struct {
	streamName    string
	publisherName string
	publisher     *ha.ReliableProducer
	pcChannel     chan streamMessageResponseShim
}

type streamConsumerShim interface {
	Close() error
}

type streamConsumer struct {
	consumer     *ha.ReliableConsumer
	streamName   string
	consumerName string
}

type streamMessage struct {
	Body            []byte
	ContentType     string
	ContentEncoding string
	Headers         map[string]string
	Ack             func()
	Nack            func()
	PublishID       int64
}

type streamMessageResponseShim interface {
	IsConfirmed() bool
	GetPublishingId() int64
	GetError() error
	GetMessage() message.StreamMessage
}

func (sc *streamConnection) Connect() error {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetMaxProducersPerClient(sc.getMaxProducers()).
			SetMaxConsumersPerClient(sc.getMaxConsumers()).
			SetUri(sc.connStr).
			SetTLSConfig(sc.tlsCfg))

	if err != nil {
		return err
	}
	sc.env = env

	return nil
}

func (sc *streamConnection) Close() error {
	sc.clientDisconnect.Store(true)
	sc.envLock.Lock()
	defer sc.envLock.Unlock()
	sc.cancel()
	if sc.env.IsClosed() {
		return nil
	}
	return sc.env.Close()
}

func (sc *streamConnection) IsClosed() bool {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()
	return sc.env.IsClosed()
}

func (sc *streamConnection) PutPublisher(confirm bool, pub streamPublisherShim) {
	key := genPublisherKey(pub.GetStreamName(), pub.GetPublisherName(), confirm)
	pool, ok := sc.publishers.Get(key)
	if ok {
		if err := pool.(*util.BlockingPool).Put(pub); err != nil {
			util.Logger.Debugf("Failed to return publisher to pool: %s", err.Error())
			pub.Close()
		}
	}
}

func (sc *streamConnection) GetPublisher(streamName, publisherName string, confirm bool) streamPublisherShim {
	key := genPublisherKey(streamName, publisherName, confirm)
	pool, ok := sc.publishers.Get(key)
	if !ok {
		limit := maxPoolProducers
		if publisherName != "" {
			// The broker only supports one publisher per publisherName
			limit = 1
		}
		pool = util.NewBlockingPool(context.WithValue(sc.ctx, CtxKey{name: poolKeyName}, key), limit,
			func() any {
				return sc.newPublisher(streamName, publisherName, confirm)
			},
		)
		sc.publishers.Add(key, pool)
	}
	var pub any
	i := 0
	// Sometimes we fail to get a producer, we will
	// retry to prevent failures
	for pub == nil && i < 10 {
		pub = pool.(*util.BlockingPool).Get()
		i++
		if pub == nil {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if pub == nil {
		return nil
	}
	return pub.(streamPublisher)
}

func genPublisherKey(streamName, publisherName string, confirm bool) string {
	key := fmt.Sprintf("%s-%s", streamName, publisherName)
	if confirm {
		key = fmt.Sprintf("%s-%s", key, "confirm")
	}
	return key
}

func (sc *streamConnection) newPublisher(streamName, publisherName string, confirm bool) streamPublisherShim {
	if sc.clientDisconnect.Load() {
		// not returning an error here because we are likely
		// shutting down this connection
		return nil
	}
	var pcChan chan streamMessageResponseShim
	options := stream.NewProducerOptions().SetClientProvidedName(sc.clientIdentifier)
	if publisherName != "" {
		options.SetProducerName(publisherName)
	}
	handler := func(_ []*stream.ConfirmationStatus) {}
	if confirm {
		options.SetConfirmationTimeOut(5 * time.Second)
		pcChan = make(chan streamMessageResponseShim, 1)
		handler = func(messageStatus []*stream.ConfirmationStatus) {
			for _, msgStatus := range messageStatus {
				pcChan <- msgStatus
			}
		}
	}

	sc.envLock.Lock()
	defer sc.envLock.Unlock()
	producer, err := ha.NewReliableProducer(sc.env, streamName, options, handler)
	if err != nil {
		util.Logger.Debugf("Error creating publisher : %v\n", err)
		return nil
	}

	return streamPublisher{streamName: streamName, publisherName: publisherName, publisher: producer, pcChannel: pcChan}
}

func (sc *streamConnection) NewConsumer(streamName string, consumerName string, offset string, handler stream.MessagesHandler, singleActive bool) (streamConsumerShim, error) {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()
	// QueryOffset returns an error if the consumer has yet to store an
	// offset, so we ignore any errors and use the offset returned which
	// is 0 on error
	lastOffset, oErr := sc.env.QueryOffset(consumerName, streamName)
	if oErr != nil {
		lastOffset = -1
	}
	sOffset, qErr := toStreamOffset(offset, lastOffset)
	if qErr != nil {
		return nil, qErr
	}

	sac := &stream.SingleActiveConsumer{}
	if singleActive {
		sac.SetEnabled(true)
		cuf := func(streamName string, _ bool) stream.OffsetSpecification {
			util.Logger.Debugf("client %s with consumer %s on stream %s promoted to active consumer", sc.clientIdentifier, consumerName, streamName)

			offset, err := sc.env.QueryOffset(consumerName, streamName)
			if err != nil {
				return stream.OffsetSpecification{}.First()
			}

			// if the offset is found, start at the next offset so we
			// don't get a repeat message
			return stream.OffsetSpecification{}.Offset(offset + 1)
		}
		sac.ConsumerUpdate = cuf
	}

	util.Logger.Debugf("Creating consumer %s for stream %s with offset %d", consumerName, streamName, sOffset)

	consumer, err := ha.NewReliableConsumer(
		sc.env,
		streamName,
		stream.NewConsumerOptions().
			SetManualCommit(). // disable auto commit
			SetClientProvidedName(sc.clientIdentifier).
			SetConsumerName(consumerName).
			SetOffset(sOffset).
			SetSingleActiveConsumer(sac),
		handler)
	if err != nil {
		return nil, err
	}
	return &streamConsumer{consumer: consumer, streamName: streamName, consumerName: consumerName}, nil
}

func (sc *streamConnection) DeclareStream(streamName string, ttl int64) error {
	opts := &stream.StreamOptions{}
	if ttl > 0 {
		dTTL := time.Duration(ttl * int64(time.Second))
		opts.SetMaxAge(dTTL)
	}
	sc.envLock.Lock()
	defer sc.envLock.Unlock()
	return sc.env.DeclareStream(streamName, opts)
}

func (sc *streamConnection) GetLastOffset(streamName string, consumerName string) (int64, error) {
	offset, qErr := sc.env.QueryOffset(consumerName, streamName)
	util.Logger.Debugf("GetLastOffset (%s)(%s)(%d) [%v]", consumerName, streamName, offset, qErr)
	return offset, qErr
}

func (sc *streamConnection) StoreOffset(streamName string, consumerName string, offset int64) error {
	util.Logger.Tracef("StoreOffset (%s)(%s)(%d)", consumerName, streamName, offset)
	return sc.env.StoreOffset(consumerName, streamName, offset)
}

func (sc *streamConnection) getMaxProducers() int {
	if sc.maxProducers < 1 {
		return 1
	}
	return sc.maxProducers
}

func (sc *streamConnection) getMaxConsumers() int {
	if sc.maxConsumers < 1 {
		return 1
	}
	return sc.maxConsumers
}

func (sp streamPublisher) GetPublisherName() string {
	return sp.publisherName
}

func (sp streamPublisher) GetStreamName() string {
	return sp.streamName
}

func (sp streamPublisher) Publish(msg streamMessage) error {
	compressedMsg, err := compressMessage(msg)
	if err != nil {
		util.Logger.Debugf("Compression failed, sending uncompressed message: %v", err)
	} else {
		msg = compressedMsg
	}
	return sp.publisher.Send(toStreamMessage(msg))
}

func (sp streamPublisher) Close() error {
	return sp.publisher.Close()
}

func (sp streamPublisher) GetPCChannel() chan streamMessageResponseShim {
	return sp.pcChannel
}

func (scc *streamConsumer) Close() error {
	return scc.consumer.Close()
}
