// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"crypto/tls"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	streamclient "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type environmentConfig struct {
	uri                   string
	tlsConfig             *tls.Config
	maxProducersPerClient int
	maxConsumersPerClient int
}

type streamEnvironment interface {
	Close() error
	IsClosed() bool
	DeclareStream(string, *streamclient.StreamOptions) error
	QueryOffset(string, string) (int64, error)
	StoreOffset(string, string, int64) error
	client() *streamclient.Environment
}

type clientEnvironment struct {
	*streamclient.Environment
}

func (e clientEnvironment) client() *streamclient.Environment { return e.Environment }

type reliableProducer interface {
	Send(message.StreamMessage) error
	Close() error
}

type reliableConsumer interface {
	Close() error
}

var newStreamEnvironment = func(config environmentConfig) (streamEnvironment, error) {
	env, err := streamclient.NewEnvironment(streamclient.NewEnvironmentOptions().
		SetMaxProducersPerClient(config.maxProducersPerClient).
		SetMaxConsumersPerClient(config.maxConsumersPerClient).
		SetUri(config.uri).
		SetTLSConfig(config.tlsConfig))
	if env == nil {
		return nil, err
	}
	return clientEnvironment{Environment: env}, err
}

var newReliableProducer = func(env *streamclient.Environment, streamName string,
	options *streamclient.ProducerOptions, handler ha.ConfirmMessageHandler) (reliableProducer, error) {
	return ha.NewReliableProducer(env, streamName, options, handler)
}

var newReliableConsumer = func(env *streamclient.Environment, streamName string,
	options *streamclient.ConsumerOptions, handler streamclient.MessagesHandler) (reliableConsumer, error) {
	return ha.NewReliableConsumer(env, streamName, options, handler)
}
