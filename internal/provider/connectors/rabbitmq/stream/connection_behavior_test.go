// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	streamclient "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type behaviorEnvironmentStub struct {
	closed       bool
	queryOffset  int64
	queryErr     error
	queryCalls   [][2]string
	storeOffset  int64
	storeArgs    [][3]any
	storeErr     error
	declarations []streamDeclaration
	declareErr   error
}

type streamDeclaration struct {
	name    string
	options *streamclient.StreamOptions
}

func (e *behaviorEnvironmentStub) Close() error {
	e.closed = true
	return nil
}

func (e *behaviorEnvironmentStub) IsClosed() bool { return e.closed }

func (e *behaviorEnvironmentStub) DeclareStream(name string, options *streamclient.StreamOptions) error {
	e.declarations = append(e.declarations, streamDeclaration{name: name, options: options})
	return e.declareErr
}

func (e *behaviorEnvironmentStub) QueryOffset(consumerName, streamName string) (int64, error) {
	e.queryCalls = append(e.queryCalls, [2]string{consumerName, streamName})
	return e.queryOffset, e.queryErr
}

func (e *behaviorEnvironmentStub) StoreOffset(consumerName, streamName string, offset int64) error {
	e.storeArgs = append(e.storeArgs, [3]any{consumerName, streamName, offset})
	e.storeOffset = offset
	if e.storeErr == nil {
		e.queryOffset = offset
	}
	return e.storeErr
}

func (*behaviorEnvironmentStub) client() *streamclient.Environment { return nil }

type producerStub struct {
	sent       []message.StreamMessage
	sendErr    error
	closeCalls int
}

func (p *producerStub) Send(msg message.StreamMessage) error {
	p.sent = append(p.sent, msg)
	return p.sendErr
}

func (p *producerStub) Close() error {
	p.closeCalls++
	return nil
}

type consumerStub struct {
	closeCalls int
}

func (c *consumerStub) Close() error {
	c.closeCalls++
	return nil
}

func newConnectedBehaviorConnection(t *testing.T) (*connection, *behaviorEnvironmentStub) {
	t.Helper()
	environment := &behaviorEnvironmentStub{queryOffset: 7}
	replaceEnvironmentFactory(t, func(environmentConfig) (streamEnvironment, error) {
		return environment, nil
	})
	conn := NewConnection("rabbitmq-stream://localhost:5552/%2F", "test-client", nil).(*connection)
	require.NoError(t, conn.Connect())
	t.Cleanup(func() { _ = conn.Close() })
	return conn, environment
}

func replaceProducerFactory(t *testing.T, factory func(*streamclient.Environment, string, *streamclient.ProducerOptions, ha.ConfirmMessageHandler) (reliableProducer, error)) {
	t.Helper()
	previous := newReliableProducer
	newReliableProducer = factory
	t.Cleanup(func() { newReliableProducer = previous })
}

func replaceConsumerFactory(t *testing.T, factory func(*streamclient.Environment, string, *streamclient.ConsumerOptions, streamclient.MessagesHandler) (reliableConsumer, error)) {
	t.Helper()
	previous := newReliableConsumer
	newReliableConsumer = factory
	t.Cleanup(func() { newReliableConsumer = previous })
}

func TestPublisherPoolAndConfirmations(t *testing.T) {
	conn, _ := newConnectedBehaviorConnection(t)
	var created []*producerStub
	var options []*streamclient.ProducerOptions
	var handlers []ha.ConfirmMessageHandler
	replaceProducerFactory(t, func(_ *streamclient.Environment, _ string, opts *streamclient.ProducerOptions, handler ha.ConfirmMessageHandler) (reliableProducer, error) {
		producer := &producerStub{}
		created = append(created, producer)
		options = append(options, opts)
		handlers = append(handlers, handler)
		return producer, nil
	})

	pub := conn.GetPublisher("events", "named-producer", true)
	require.NotNil(t, pub)
	assert.Equal(t, "events", pub.StreamName())
	assert.Equal(t, "named-producer", pub.PublisherName())
	require.Len(t, options, 1)
	assert.Equal(t, "named-producer", options[0].Name)
	assert.Equal(t, "test-client", options[0].ClientProvidedName)
	assert.Equal(t, 5*time.Second, options[0].ConfirmationTimeOut)

	require.NoError(t, pub.Publish(Message{Body: []byte("event"), Headers: map[string]string{"kind": "created"}}))
	require.Len(t, created[0].sent, 1)
	assert.Equal(t, []byte("event"), created[0].sent[0].GetData()[0])
	assert.Equal(t, map[string]any{"kind": "created"}, created[0].sent[0].GetApplicationProperties())

	require.Len(t, handlers, 1)
	handlers[0]([]*streamclient.ConfirmationStatus{{}})
	select {
	case confirmation := <-pub.Confirmations():
		assert.False(t, confirmation.Confirmed)
		assert.Zero(t, confirmation.PublishingID)
		assert.NoError(t, confirmation.Err)
	case <-time.After(time.Second):
		t.Fatal("confirmation callback did not deliver a result")
	}

	conn.PutPublisher(true, pub)
	reused := conn.GetPublisher("events", "named-producer", true)
	assert.Same(t, pub, reused)
	assert.Len(t, created, 1, "a returned publisher should be reused from its pool")
	conn.PutPublisher(true, reused)
}

func TestGetPublisherEnforcesPoolLimits(t *testing.T) {
	tests := []struct {
		name          string
		publisherName string
		limit         int
	}{
		{name: "named publisher", publisherName: "deduplicated", limit: 1},
		{name: "unnamed publisher pool", limit: maxProducersPerPool},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn, _ := newConnectedBehaviorConnection(t)
			var created atomic.Int64
			replaceProducerFactory(t, func(_ *streamclient.Environment, _ string, _ *streamclient.ProducerOptions, _ ha.ConfirmMessageHandler) (reliableProducer, error) {
				created.Add(1)
				return &producerStub{}, nil
			})

			publishers := make([]Publisher, 0, test.limit)
			for i := 0; i < test.limit; i++ {
				publisher := conn.GetPublisher("events", test.publisherName, false)
				require.NotNil(t, publisher)
				publishers = append(publishers, publisher)
			}
			assert.EqualValues(t, test.limit, created.Load())

			started := make(chan struct{})
			result := make(chan Publisher, 1)
			go func() {
				close(started)
				result <- conn.GetPublisher("events", test.publisherName, false)
			}()
			<-started
			select {
			case <-result:
				t.Fatal("publisher pool returned an extra producer before one was released")
			case <-time.After(40 * time.Millisecond):
			}
			assert.EqualValues(t, test.limit, created.Load())

			conn.PutPublisher(false, publishers[0])
			select {
			case reused := <-result:
				assert.Same(t, publishers[0], reused)
				conn.PutPublisher(false, reused)
			case <-time.After(time.Second):
				t.Fatal("waiting publisher was not released after returning a producer")
			}
			for _, publisher := range publishers[1:] {
				conn.PutPublisher(false, publisher)
			}
		})
	}
}

func TestPublisherDisconnectBehavior(t *testing.T) {
	conn, _ := newConnectedBehaviorConnection(t)
	producer := &producerStub{}
	replaceProducerFactory(t, func(_ *streamclient.Environment, _ string, _ *streamclient.ProducerOptions, _ ha.ConfirmMessageHandler) (reliableProducer, error) {
		return producer, nil
	})

	pub := conn.GetPublisher("events", "", false)
	require.NotNil(t, pub)
	require.NoError(t, conn.Close())
	assert.Nil(t, conn.GetPublisher("events", "", false))
	conn.PutPublisher(false, pub)
	assert.Equal(t, 1, producer.closeCalls, "publisher returned after disconnect should be closed")
}

func TestConnectionConsumerOperations(t *testing.T) {
	conn, _ := newConnectedBehaviorConnection(t)
	var options *streamclient.ConsumerOptions
	var clientHandler streamclient.MessagesHandler
	client := &consumerStub{}
	replaceConsumerFactory(t, func(_ *streamclient.Environment, streamName string, opts *streamclient.ConsumerOptions, handler streamclient.MessagesHandler) (reliableConsumer, error) {
		assert.Equal(t, "events", streamName)
		options = opts
		clientHandler = handler
		return client, nil
	})
	deliveries := make(chan Delivery, 1)
	consumerClient, err := conn.NewConsumer("events", "workers", "continue", func(delivery Delivery) {
		deliveries <- delivery
	}, true)
	require.NoError(t, err)
	require.NotNil(t, options)
	assert.Equal(t, "workers", options.ConsumerName)
	assert.Equal(t, "test-client", options.ClientProvidedName)
	assert.Equal(t, streamclient.OffsetSpecification{}.Offset(8), options.Offset)
	require.NotNil(t, options.SingleActiveConsumer)
	assert.True(t, options.SingleActiveConsumer.Enabled)
	assert.Equal(t, streamclient.OffsetSpecification{}.Offset(8), options.SingleActiveConsumer.ConsumerUpdate("events", true))

	clientHandler(streamclient.ConsumerContext{}, makeClientMessage(t, []byte("received"), map[string]interface{}{"custom": "value"}))
	select {
	case delivery := <-deliveries:
		assert.Equal(t, "events", delivery.StreamName)
		assert.Zero(t, delivery.Offset)
		assert.Equal(t, []byte("received"), delivery.Message.Body)
		assert.Equal(t, map[string]string{"custom": "value"}, delivery.Message.Headers)
	case <-time.After(time.Second):
		t.Fatal("consumer callback did not deliver the normalized message")
	}

	require.NoError(t, consumerClient.Close())
	assert.Equal(t, 1, client.closeCalls)
}

func TestConnectionConsumerPromotionFallsBackToFirst(t *testing.T) {
	conn, environment := newConnectedBehaviorConnection(t)
	environment.queryErr = errors.New("offset unavailable")
	var options *streamclient.ConsumerOptions
	replaceConsumerFactory(t, func(_ *streamclient.Environment, _ string, opts *streamclient.ConsumerOptions, _ streamclient.MessagesHandler) (reliableConsumer, error) {
		options = opts
		return &consumerStub{}, nil
	})

	_, err := conn.NewConsumer("events", "workers", "continue", func(Delivery) {}, true)
	require.NoError(t, err)
	require.NotNil(t, options)
	assert.Equal(t, streamclient.OffsetSpecification{}.First(), options.SingleActiveConsumer.ConsumerUpdate("events", true))
}

func TestConnectionConsumerPromotionUsesCapturedEnvironment(t *testing.T) {
	oldEnvironment := newConcurrentOffsetEnvironment()
	oldEnvironment.setOffset("workers", "events", 20)
	newEnvironment := newConcurrentOffsetEnvironment()
	newEnvironment.setOffset("workers", "events", 99)
	environments := []streamEnvironment{oldEnvironment, newEnvironment}
	var environmentIndex atomic.Int32
	replaceEnvironmentFactory(t, func(environmentConfig) (streamEnvironment, error) {
		return environments[environmentIndex.Add(1)-1], nil
	})

	conn := NewConnection("rabbitmq-stream://localhost:5552/%2F", "test-client", nil).(*connection)
	require.NoError(t, conn.Connect())
	t.Cleanup(func() { _ = conn.Close() })
	var options *streamclient.ConsumerOptions
	replaceConsumerFactory(t, func(_ *streamclient.Environment, _ string, opts *streamclient.ConsumerOptions, _ streamclient.MessagesHandler) (reliableConsumer, error) {
		options = opts
		return &consumerStub{}, nil
	})
	_, err := conn.NewConsumer("events", "workers", "next", func(Delivery) {}, true)
	require.NoError(t, err)
	require.NotNil(t, options)
	oldQueryCount := oldEnvironment.queryCount("workers", "events")
	oldEnvironment.blockNextQuery("workers", "events")
	oldEnvironment.closed.Store(true)

	promotionResult := make(chan streamclient.OffsetSpecification, 1)
	go func() {
		promotionResult <- options.SingleActiveConsumer.ConsumerUpdate("events", true)
	}()
	select {
	case <-oldEnvironment.queryStarted:
	case <-time.After(time.Second):
		t.Fatal("promotion callback did not query its environment")
	}

	// Reconnect while the callback is using the original environment. The
	// callback must not race with or switch to the replacement environment.
	require.NoError(t, conn.Connect())
	close(oldEnvironment.releaseQuery)
	select {
	case offset := <-promotionResult:
		assert.Equal(t, streamclient.OffsetSpecification{}.First(), offset)
	case <-time.After(time.Second):
		t.Fatal("promotion callback did not finish")
	}
	assert.Equal(t, oldQueryCount+1, oldEnvironment.queryCount("workers", "events"))
	assert.Zero(t, newEnvironment.queryCount("workers", "events"))
}

func TestConnectionConsumerCreationError(t *testing.T) {
	conn, _ := newConnectedBehaviorConnection(t)
	wantErr := errors.New("consumer creation failed")
	replaceConsumerFactory(t, func(_ *streamclient.Environment, _ string, _ *streamclient.ConsumerOptions, _ streamclient.MessagesHandler) (reliableConsumer, error) {
		return nil, wantErr
	})

	consumerClient, err := conn.NewConsumer("events", "workers", "next", func(Delivery) {}, false)
	assert.Nil(t, consumerClient)
	assert.ErrorIs(t, err, wantErr)
}

func TestConnectionStreamDeclarationAndOffsets(t *testing.T) {
	conn, environment := newConnectedBehaviorConnection(t)
	require.NoError(t, conn.DeclareStream("events", 45))
	require.NoError(t, conn.DeclareStream("unlimited", 0))
	require.Len(t, environment.declarations, 2)
	assert.Equal(t, "events", environment.declarations[0].name)
	assert.Equal(t, 45*time.Second, environment.declarations[0].options.MaxAge)
	assert.Zero(t, environment.declarations[1].options.MaxAge)

	offset, err := conn.GetConsumerOffset("events", "workers")
	require.NoError(t, err)
	assert.Equal(t, int64(7), offset)
	require.NoError(t, conn.StoreOffset("events", "workers", 11))
	assert.Equal(t, [][3]any{{"workers", "events", int64(11)}}, environment.storeArgs)
	assert.Equal(t, int64(11), environment.storeOffset)

	environment.queryOffset = 15
	consumer := &consumerStub{}
	var streamStatsOptions *streamclient.ConsumerOptions
	replaceConsumerFactory(t, func(_ *streamclient.Environment, _ string, options *streamclient.ConsumerOptions, _ streamclient.MessagesHandler) (reliableConsumer, error) {
		streamStatsOptions = options
		return consumer, nil
	})
	streamOffset, err := conn.GetStreamOffset("events")
	require.NoError(t, err)
	assert.Equal(t, int64(15), streamOffset)
	require.NotNil(t, streamStatsOptions)
	assert.Equal(t, "arkeSourceStatsConsumer", streamStatsOptions.ConsumerName)
	assert.Equal(t, streamclient.OffsetSpecification{}.Last(), streamStatsOptions.Offset)
	assert.Equal(t, 1, consumer.closeCalls)
}

func TestConnectionOffsetAndDeclarationErrors(t *testing.T) {
	conn, environment := newConnectedBehaviorConnection(t)
	wantErr := errors.New("broker request failed")
	environment.queryErr = wantErr
	_, err := conn.GetConsumerOffset("events", "workers")
	assert.ErrorIs(t, err, wantErr)
	assert.ErrorIs(t, conn.StoreOffset("events", "workers", 3), wantErr)
	assert.Empty(t, environment.storeArgs)

	environment.queryErr = nil
	environment.storeErr = wantErr
	assert.ErrorIs(t, conn.StoreOffset("events", "workers", 9), wantErr)

	environment.declareErr = wantErr
	assert.ErrorIs(t, conn.DeclareStream("events", 0), wantErr)
}

func TestConnectionStoreOffsetDoesNotMoveBackwards(t *testing.T) {
	conn, environment := newConnectedBehaviorConnection(t)
	environment.queryOffset = 11

	require.NoError(t, conn.StoreOffset("events", "workers", 7))
	assert.Empty(t, environment.storeArgs)
	assert.Equal(t, int64(11), environment.queryOffset)

	require.NoError(t, conn.StoreOffset("events", "workers", 12))
	assert.Equal(t, [][3]any{{"workers", "events", int64(12)}}, environment.storeArgs)
	assert.Equal(t, int64(12), environment.queryOffset)
}

func TestConnectionOffsetLocksArePerConsumer(t *testing.T) {
	t.Run("same consumer offsets remain monotonic", func(t *testing.T) {
		environment := newConcurrentOffsetEnvironment()
		environment.setOffset("workers", "events", 0)
		environment.blockNextQuery("workers", "events")
		conn := newConnectionWithOffsetEnvironment(t, environment)

		firstResult := make(chan error, 1)
		go func() { firstResult <- conn.StoreOffset("events", "workers", 20) }()
		select {
		case <-environment.queryStarted:
		case <-time.After(time.Second):
			t.Fatal("first offset update did not query the current offset")
		}
		<-environment.queryEvents

		secondStarted := make(chan struct{})
		secondResult := make(chan error, 1)
		go func() {
			close(secondStarted)
			secondResult <- conn.StoreOffset("events", "workers", 10)
		}()
		<-secondStarted
		select {
		case key := <-environment.queryEvents:
			t.Fatalf("same-consumer query %v ran while the prior update was in progress", key)
		case <-time.After(50 * time.Millisecond):
		}

		close(environment.releaseQuery)
		require.NoError(t, <-firstResult)
		require.NoError(t, <-secondResult)
		assert.Equal(t, int64(20), environment.offset("workers", "events"))
	})

	t.Run("different consumers can update concurrently", func(t *testing.T) {
		environment := newConcurrentOffsetEnvironment()
		environment.setOffset("workers-a", "events", 0)
		environment.setOffset("workers-b", "events", 0)
		environment.blockNextQuery("workers-a", "events")
		conn := newConnectionWithOffsetEnvironment(t, environment)

		firstResult := make(chan error, 1)
		go func() { firstResult <- conn.StoreOffset("events", "workers-a", 20) }()
		select {
		case <-environment.queryStarted:
		case <-time.After(time.Second):
			t.Fatal("first consumer update did not query the current offset")
		}

		secondResult := make(chan error, 1)
		go func() { secondResult <- conn.StoreOffset("events", "workers-b", 10) }()
		select {
		case err := <-secondResult:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("an update for another consumer was blocked")
		}

		close(environment.releaseQuery)
		require.NoError(t, <-firstResult)
		assert.Equal(t, int64(20), environment.offset("workers-a", "events"))
		assert.Equal(t, int64(10), environment.offset("workers-b", "events"))
	})
}

type concurrentOffsetEnvironment struct {
	mu             sync.Mutex
	closed         atomic.Bool
	offsets        map[[2]string]int64
	queryCounts    map[[2]string]int
	queryEvents    chan [2]string
	blockedQueries map[[2]string]bool
	blockNext      map[[2]string]bool
	queryStarted   chan struct{}
	releaseQuery   chan struct{}
}

func newConcurrentOffsetEnvironment() *concurrentOffsetEnvironment {
	return &concurrentOffsetEnvironment{
		offsets:        make(map[[2]string]int64),
		queryCounts:    make(map[[2]string]int),
		queryEvents:    make(chan [2]string, 32),
		blockedQueries: make(map[[2]string]bool),
		blockNext:      make(map[[2]string]bool),
		queryStarted:   make(chan struct{}),
		releaseQuery:   make(chan struct{}),
	}
}

func (e *concurrentOffsetEnvironment) Close() error {
	e.closed.Store(true)
	return nil
}

func (e *concurrentOffsetEnvironment) IsClosed() bool { return e.closed.Load() }

func (*concurrentOffsetEnvironment) DeclareStream(string, *streamclient.StreamOptions) error {
	return nil
}

func (e *concurrentOffsetEnvironment) QueryOffset(consumerName, streamName string) (int64, error) {
	key := [2]string{consumerName, streamName}
	e.mu.Lock()
	e.queryCounts[key]++
	offset, ok := e.offsets[key]
	block := e.blockNext[key] && !e.blockedQueries[key]
	if block {
		e.blockedQueries[key] = true
	}
	e.mu.Unlock()
	e.queryEvents <- key
	if block {
		close(e.queryStarted)
		<-e.releaseQuery
	}
	if e.closed.Load() {
		return 0, errConnectionClosed
	}
	if !ok {
		return 0, streamclient.OffsetNotFoundError
	}
	return offset, nil
}

func (e *concurrentOffsetEnvironment) StoreOffset(consumerName, streamName string, offset int64) error {
	e.mu.Lock()
	e.offsets[[2]string{consumerName, streamName}] = offset
	e.mu.Unlock()
	return nil
}

func (*concurrentOffsetEnvironment) client() *streamclient.Environment { return nil }

func (e *concurrentOffsetEnvironment) setOffset(consumerName, streamName string, offset int64) {
	e.mu.Lock()
	e.offsets[[2]string{consumerName, streamName}] = offset
	e.mu.Unlock()
}

func (e *concurrentOffsetEnvironment) offset(consumerName, streamName string) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.offsets[[2]string{consumerName, streamName}]
}

func (e *concurrentOffsetEnvironment) queryCount(consumerName, streamName string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.queryCounts[[2]string{consumerName, streamName}]
}

func (e *concurrentOffsetEnvironment) blockNextQuery(consumerName, streamName string) {
	e.mu.Lock()
	e.blockNext[[2]string{consumerName, streamName}] = true
	e.mu.Unlock()
}

func newConnectionWithOffsetEnvironment(t *testing.T, environment *concurrentOffsetEnvironment) *connection {
	t.Helper()
	replaceEnvironmentFactory(t, func(environmentConfig) (streamEnvironment, error) {
		return environment, nil
	})
	conn := NewConnection("rabbitmq-stream://localhost:5552/%2F", "test-client", nil).(*connection)
	require.NoError(t, conn.Connect())
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
