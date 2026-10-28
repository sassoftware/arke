// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Package stream owns the physical RabbitMQ Stream client environment.
package stream

import (
	"crypto/tls"
	"errors"
	"fmt"
	"sync"

	rabbitstream "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const (
	maxProducersPerClient = 100
	maxConsumersPerClient = 10
)

type environmentShim interface {
	Close() error
	IsClosed() bool
	NewConsumer(string, rabbitstream.MessagesHandler, *rabbitstream.ConsumerOptions) (Consumer, error)
	QueryOffset(string, string) (int64, error)
	StoreOffset(string, string, int64) error
	client() *rabbitstream.Environment
}

type Consumer interface {
	Close() error
}

type clientEnvironment struct {
	*rabbitstream.Environment
}

func (e clientEnvironment) client() *rabbitstream.Environment {
	return e.Environment
}

func (e clientEnvironment) NewConsumer(streamName string, handler rabbitstream.MessagesHandler, options *rabbitstream.ConsumerOptions) (Consumer, error) {
	return e.Environment.NewConsumer(streamName, handler, options)
}

func (e clientEnvironment) QueryOffset(consumerName string, streamName string) (int64, error) {
	return e.Environment.QueryOffset(consumerName, streamName)
}

func (e clientEnvironment) StoreOffset(consumerName string, streamName string, offset int64) error {
	return e.Environment.StoreOffset(consumerName, streamName, offset)
}

type environmentConfig struct {
	uri                   string
	tlsConfig             *tls.Config
	maxProducersPerClient int
	maxConsumersPerClient int
}

type Environment struct {
	mu        sync.Mutex
	uri       string
	tlsConfig *tls.Config
	env       environmentShim
	closed    bool
}

var newStreamEnvironment = func(config environmentConfig) (environmentShim, error) {
	options := rabbitstream.NewEnvironmentOptions().
		SetMaxProducersPerClient(config.maxProducersPerClient).
		SetMaxConsumersPerClient(config.maxConsumersPerClient).
		SetUri(config.uri).
		SetTLSConfig(config.tlsConfig)
	env, err := rabbitstream.NewEnvironment(options)
	if env == nil {
		return nil, err
	}
	return clientEnvironment{Environment: env}, err
}

func New(uri string, tlsConfig *tls.Config) *Environment {
	return &Environment{uri: uri, tlsConfig: tlsConfig}
}

func (e *Environment) Connect() error {
	if e == nil {
		return errors.New("stream environment is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.env != nil && !e.closed && !e.env.IsClosed() {
		return nil
	}

	env, err := newStreamEnvironment(environmentConfig{
		uri:                   e.uri,
		tlsConfig:             e.tlsConfig,
		maxProducersPerClient: maxProducersPerClient,
		maxConsumersPerClient: maxConsumersPerClient,
	})
	if err != nil {
		if env != nil && !env.IsClosed() {
			if closeErr := env.Close(); closeErr != nil {
				err = fmt.Errorf("%w (closing partially created stream environment: %v)", err, closeErr)
			}
		}
		return err
	}
	if env == nil {
		return errors.New("stream environment factory returned a nil environment")
	}

	e.env = env
	e.closed = false
	return nil
}

func (e *Environment) Close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.env == nil || e.closed || e.env.IsClosed() {
		e.closed = true
		return nil
	}
	e.closed = true
	return e.env.Close()
}

func (e *Environment) IsClosed() bool {
	if e == nil {
		return true
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.env == nil || e.closed || e.env.IsClosed()
}

// Client returns the underlying Stream client environment for Stream operations.
func (e *Environment) Client() *rabbitstream.Environment {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.env == nil {
		return nil
	}
	return e.env.client()
}

func (e *Environment) getConnectedEnvironment() (environmentShim, error) {
	if e == nil {
		return nil, errors.New("stream environment is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.env == nil || e.closed || e.env.IsClosed() {
		return nil, errors.New("stream environment is not connected")
	}
	return e.env, nil
}

func (e *Environment) NewConsumer(streamName string, handler rabbitstream.MessagesHandler, options *rabbitstream.ConsumerOptions) (Consumer, error) {
	env, err := e.getConnectedEnvironment()
	if err != nil {
		return nil, err
	}
	return env.NewConsumer(streamName, handler, options)
}

func (e *Environment) QueryOffset(consumerName string, streamName string) (int64, error) {
	env, err := e.getConnectedEnvironment()
	if err != nil {
		return 0, err
	}
	return env.QueryOffset(consumerName, streamName)
}

func (e *Environment) StoreOffset(consumerName string, streamName string, offset int64) error {
	env, err := e.getConnectedEnvironment()
	if err != nil {
		return err
	}
	return env.StoreOffset(consumerName, streamName, offset)
}
