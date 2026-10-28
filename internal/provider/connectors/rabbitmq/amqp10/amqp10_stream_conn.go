// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	rabbitstream "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/sassoftware/arke/internal/provider/connectors/rabbitmq/stream"
)

const (
	statsConsumerName = "arkeSourceStatsConsumer"
	streamOffsetWait  = time.Second
)

type streamConnectionShim interface {
	Connect() error
	Close() error
	IsClosed() bool
	GetStreamOffset(string) (int64, error)
}

type streamEnvironmentShim interface {
	Connect() error
	Close() error
	IsClosed() bool
	NewConsumer(string, rabbitstream.MessagesHandler, *rabbitstream.ConsumerOptions) (stream.Consumer, error)
	QueryOffset(string, string) (int64, error)
	StoreOffset(string, string, int64) error
}

type streamConnection struct {
	envLock       sync.Mutex
	env           streamEnvironmentShim
	brokerDetails *BrokerDetails
}

func newStreamConnection(uri string, tlsConfig *tls.Config, brokerDetails *BrokerDetails) *streamConnection {
	return &streamConnection{env: stream.New(uri, tlsConfig), brokerDetails: brokerDetails}
}

func (sc *streamConnection) Connect() error {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()

	if sc.brokerDetails != nil && sc.brokerDetails.clientDisconnect.Load() {
		return errors.New("provider has been disconnected")
	}
	if sc.env == nil {
		return errors.New("stream environment is required")
	}
	if !sc.env.IsClosed() {
		return nil
	}

	if err := sc.env.Connect(); err != nil {
		return err
	}

	if sc.brokerDetails != nil {
		sc.brokerDetails.Lock()
		if sc.brokerDetails.clientDisconnect.Load() {
			sc.brokerDetails.Unlock()
			if closeErr := sc.env.Close(); closeErr != nil {
				return fmt.Errorf("provider has been disconnected (closing newly created stream environment: %v)", closeErr)
			}
			return errors.New("provider has been disconnected")
		}
		sc.brokerDetails.Unlock()
	}

	return nil
}

func (sc *streamConnection) Close() error {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()

	if sc.env == nil || sc.env.IsClosed() {
		return nil
	}
	return sc.env.Close()
}

func (sc *streamConnection) IsClosed() bool {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()

	return sc.env == nil || sc.env.IsClosed()
}

func (sc *streamConnection) GetStreamOffset(streamName string) (offset int64, resultErr error) {
	if err := sc.Connect(); err != nil {
		return 0, err
	}

	sc.envLock.Lock()
	defer sc.envLock.Unlock()
	if sc.env == nil || sc.env.IsClosed() {
		return 0, errors.New("stream environment is not connected")
	}
	env := sc.env

	messageDelivered := make(chan struct{}, 1)
	handler := func(cc rabbitstream.ConsumerContext, _ *amqp.Message) {
		if cc.Consumer != nil {
			_ = env.StoreOffset(statsConsumerName, streamName, cc.Consumer.GetOffset())
			select {
			case messageDelivered <- struct{}{}:
			default:
			}
		}
	}

	consumer, err := env.NewConsumer(streamName, handler, rabbitstream.NewConsumerOptions().
		SetManualCommit().
		SetConsumerName(statsConsumerName).
		SetClientProvidedName(statsConsumerName).
		SetOffset(rabbitstream.OffsetSpecification{}.Last()))
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := consumer.Close(); closeErr != nil {
			if resultErr == nil {
				resultErr = closeErr
			} else {
				resultErr = fmt.Errorf("%w (closing stats consumer: %v)", resultErr, closeErr)
			}
		}
	}()

	timer := time.NewTimer(streamOffsetWait)
	defer timer.Stop()
	select {
	case <-messageDelivered:
	case <-timer.C:
	}

	return env.QueryOffset(statsConsumerName, streamName)
}
