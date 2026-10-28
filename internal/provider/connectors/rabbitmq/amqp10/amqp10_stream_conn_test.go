// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	rabbitstream "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/sassoftware/arke/internal/provider"
	"github.com/sassoftware/arke/internal/provider/connectors/rabbitmq/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type streamEnvironmentStub struct {
	closed         bool
	closeErr       error
	closeCall      int
	connectCall    int
	connectFunc    func() error
	consumer       stream.Consumer
	consumerErr    error
	queryOffset    int64
	queryErr       error
	storeErr       error
	storedOffset   int64
	storedName     string
	storedStream   string
	options        *rabbitstream.ConsumerOptions
	handler        rabbitstream.MessagesHandler
	handlerDelay   time.Duration
	consumerStream string
}

func (e *streamEnvironmentStub) Connect() error {
	e.connectCall++
	var err error
	if e.connectFunc != nil {
		err = e.connectFunc()
	}
	if err == nil {
		e.closed = false
	}
	return err
}

func (e *streamEnvironmentStub) Close() error {
	if e.closed {
		return nil
	}
	e.closeCall++
	e.closed = true
	return e.closeErr
}

func (e *streamEnvironmentStub) IsClosed() bool {
	return e.closed
}

func (e *streamEnvironmentStub) NewConsumer(streamName string, handler rabbitstream.MessagesHandler, options *rabbitstream.ConsumerOptions) (stream.Consumer, error) {
	e.consumerStream = streamName
	e.handler = handler
	e.options = options
	if e.consumerErr != nil {
		return nil, e.consumerErr
	}
	if handler != nil && e.consumer != nil {
		deliver := func() {
			handler(testStreamConsumerContext(e.queryOffset), &amqp.Message{})
		}
		if e.handlerDelay > 0 {
			go func() {
				time.Sleep(e.handlerDelay)
				deliver()
			}()
		} else {
			deliver()
		}
	}
	return e.consumer, nil
}

func (e *streamEnvironmentStub) QueryOffset(string, string) (int64, error) {
	if e.queryErr != nil {
		return 0, e.queryErr
	}
	return e.storedOffset, nil
}

func (e *streamEnvironmentStub) StoreOffset(consumerName string, streamName string, offset int64) error {
	e.storedName = consumerName
	e.storedStream = streamName
	e.storedOffset = offset
	return e.storeErr
}

type streamOffsetConsumerStub struct {
	closeCalls int
	closeErr   error
}

func (c *streamOffsetConsumerStub) Close() error {
	c.closeCalls++
	return c.closeErr
}

func testStreamConsumerContext(offset int64) rabbitstream.ConsumerContext {
	consumer := &rabbitstream.Consumer{}
	value := reflect.ValueOf(consumer).Elem().FieldByName("currentOffset")
	reflect.NewAt(value.Type(), unsafe.Pointer(value.UnsafeAddr())).Elem().SetInt(offset)
	mutex := reflect.ValueOf(consumer).Elem().FieldByName("mutex")
	reflect.NewAt(mutex.Type(), unsafe.Pointer(mutex.UnsafeAddr())).Elem().Set(reflect.ValueOf(&sync.RWMutex{}))
	return rabbitstream.ConsumerContext{Consumer: consumer}
}

type streamConnectionStub struct {
	closed    bool
	closeErr  error
	closeCall int
	offset    int64
	offsetErr error
}

func (c *streamConnectionStub) Connect() error {
	return nil
}

func (c *streamConnectionStub) Close() error {
	if c.closed {
		return nil
	}
	c.closeCall++
	if c.closeErr != nil {
		return c.closeErr
	}
	c.closed = true
	return nil
}

func (c *streamConnectionStub) IsClosed() bool {
	return c.closed
}

func (c *streamConnectionStub) GetStreamOffset(string) (int64, error) {
	return c.offset, c.offsetErr
}

func Test_streamConnection_Connect(t *testing.T) {
	t.Run("connects the shared environment", func(t *testing.T) {
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, nil)
		environment := &streamEnvironmentStub{closed: true}
		connection.env = environment

		require.NoError(t, connection.Connect())

		assert.Equal(t, 1, environment.connectCall)
		assert.False(t, connection.IsClosed())
	})

	t.Run("failed environment connection is retryable", func(t *testing.T) {
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, nil)
		environment := &streamEnvironmentStub{closed: true}
		creationErr := errors.New("stream environment creation failed")
		environment.connectFunc = func() error {
			if environment.connectCall == 1 {
				return creationErr
			}
			return nil
		}
		connection.env = environment

		assert.ErrorIs(t, connection.Connect(), creationErr)
		require.NoError(t, connection.Connect())
		assert.Equal(t, 2, environment.connectCall)
	})

	t.Run("does not replace an existing open environment", func(t *testing.T) {
		environment := &streamEnvironmentStub{}
		connection := &streamConnection{env: environment}

		require.NoError(t, connection.Connect())
		assert.Zero(t, environment.connectCall)
	})
}

func Test_streamConnection_Close(t *testing.T) {
	t.Run("is safe before Connect", func(t *testing.T) {
		connection := &streamConnection{}
		require.NoError(t, connection.Close())
	})

	t.Run("repeated close is safe", func(t *testing.T) {
		environment := &streamEnvironmentStub{}
		connection := &streamConnection{env: environment}
		require.NoError(t, connection.Connect())

		require.NoError(t, connection.Close())
		require.NoError(t, connection.Close())

		assert.True(t, connection.IsClosed())
		assert.Equal(t, 1, environment.closeCall)
	})

	t.Run("propagates environment close error", func(t *testing.T) {
		closeErr := errors.New("stream environment close failed")
		environment := &streamEnvironmentStub{closeErr: closeErr}
		connection := &streamConnection{env: environment}

		assert.ErrorIs(t, connection.Close(), closeErr)
		require.NoError(t, connection.Close())
		assert.Equal(t, 1, environment.closeCall)
	})
}

func Test_streamConnection_IsClosed(t *testing.T) {
	t.Run("is closed before Connect", func(t *testing.T) {
		assert.True(t, (&streamConnection{}).IsClosed())
	})

	t.Run("returns environment state", func(t *testing.T) {
		connection := &streamConnection{env: &streamEnvironmentStub{}}
		assert.False(t, connection.IsClosed())

		connection.env.(*streamEnvironmentStub).closed = true
		assert.True(t, connection.IsClosed())
	})
}

func Test_streamConnection_GetStreamOffset(t *testing.T) {
	t.Run("returns an environment connection error", func(t *testing.T) {
		connectErr := errors.New("stream environment connection failed")
		environment := &streamEnvironmentStub{closed: true, connectFunc: func() error { return connectErr }}
		connection := &streamConnection{env: environment}

		_, err := connection.GetStreamOffset("orders")

		assert.ErrorIs(t, err, connectErr)
	})

	t.Run("returns the stored offset and closes the temporary consumer", func(t *testing.T) {
		consumer := &streamOffsetConsumerStub{}
		environment := &streamEnvironmentStub{consumer: consumer, queryOffset: 17}
		connection := &streamConnection{env: environment}

		offset, err := connection.GetStreamOffset("orders")

		require.NoError(t, err)
		assert.Equal(t, int64(17), offset)
		assert.Equal(t, "orders", environment.consumerStream)
		assert.Equal(t, statsConsumerName, environment.options.ConsumerName)
		assert.Equal(t, statsConsumerName, environment.options.ClientProvidedName)
		assert.Equal(t, "last", environment.options.Offset.String())
		assert.Equal(t, statsConsumerName, environment.storedName)
		assert.Equal(t, "orders", environment.storedStream)
		assert.Equal(t, int64(17), environment.storedOffset)
		assert.Equal(t, 1, consumer.closeCalls)
	})

	t.Run("waits for asynchronous tail delivery before querying the offset", func(t *testing.T) {
		consumer := &streamOffsetConsumerStub{}
		environment := &streamEnvironmentStub{consumer: consumer, queryOffset: 19, handlerDelay: 10 * time.Millisecond}
		connection := &streamConnection{env: environment}

		offset, err := connection.GetStreamOffset("orders")

		require.NoError(t, err)
		assert.Equal(t, int64(19), offset)
		assert.Equal(t, int64(19), environment.storedOffset)
		assert.Equal(t, 1, consumer.closeCalls)
	})

	t.Run("preserves the empty stream error and closes the temporary consumer", func(t *testing.T) {
		consumer := &streamOffsetConsumerStub{}
		environment := &streamEnvironmentStub{consumer: consumer, queryErr: errors.New("Offset not found")}
		connection := &streamConnection{env: environment}

		_, err := connection.GetStreamOffset("empty")

		require.EqualError(t, err, "Offset not found")
		assert.Equal(t, 1, consumer.closeCalls)
	})

	t.Run("does not close a consumer that failed to be created", func(t *testing.T) {
		consumerErr := errors.New("consumer creation failed")
		environment := &streamEnvironmentStub{consumerErr: consumerErr}
		connection := &streamConnection{env: environment}

		_, err := connection.GetStreamOffset("orders")

		assert.ErrorIs(t, err, consumerErr)
	})

	t.Run("returns a close error after successful retrieval", func(t *testing.T) {
		closeErr := errors.New("consumer close failed")
		consumer := &streamOffsetConsumerStub{closeErr: closeErr}
		environment := &streamEnvironmentStub{consumer: consumer, queryOffset: 17}
		connection := &streamConnection{env: environment}

		_, err := connection.GetStreamOffset("orders")

		assert.ErrorIs(t, err, closeErr)
		assert.Equal(t, 1, consumer.closeCalls)
	})
}

func Test_streamConnection_ConnectAfterDisconnect(t *testing.T) {
	t.Run("does not create an environment after terminal disconnect", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.state.Store(provider.DISCONNECTED)
		bd.clientDisconnect.Store(true)
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, bd)
		environment := &streamEnvironmentStub{closed: true}
		connection.env = environment

		require.ErrorContains(t, connection.Connect(), "disconnected")

		assert.Zero(t, environment.connectCall)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.state.Load())
		assert.True(t, connection.IsClosed())
	})

	t.Run("closes a newly created environment if disconnect races with creation", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.state.Store(provider.CONNECTED)
		environment := &streamEnvironmentStub{closed: true}
		started := make(chan struct{})
		release := make(chan struct{})
		environment.connectFunc = func() error {
			close(started)
			<-release
			return nil
		}
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, bd)
		connection.env = environment
		bd.StreamConnection = connection
		connectResult := make(chan error, 1)
		go func() { connectResult <- connection.Connect() }()
		<-started

		disconnectResult := make(chan struct{})
		go func() {
			bd.disconnect()
			close(disconnectResult)
		}()
		assert.Eventually(t, bd.clientDisconnect.Load, time.Second, time.Millisecond)
		close(release)

		assert.ErrorContains(t, <-connectResult, "disconnected")
		select {
		case <-disconnectResult:
		case <-time.After(time.Second):
			t.Fatal("disconnect did not finish after stream setup completed")
		}
		assert.Equal(t, 1, environment.closeCall)
		assert.Equal(t, uint32(provider.DISCONNECTED), bd.state.Load())
	})

	t.Run("terminal disconnect from closed state prevents later stream connect", func(t *testing.T) {
		bd := newTestBrokerDetails()
		bd.state.Store(provider.CLOSED)
		connection := newStreamConnection("rabbitmq-stream://guest:guest@localhost:5552/%2F", nil, bd)
		bd.disconnect()

		environment := &streamEnvironmentStub{closed: true}
		connection.env = environment

		require.ErrorContains(t, connection.Connect(), "disconnected")
		assert.Zero(t, environment.connectCall)
		assert.Equal(t, uint32(provider.CLOSED), bd.state.Load())
	})
}
