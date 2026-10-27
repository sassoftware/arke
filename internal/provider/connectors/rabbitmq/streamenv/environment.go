// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Package streamenv owns the physical RabbitMQ Stream client environment.
package streamenv

import (
	"crypto/tls"
	"errors"
	"fmt"
	"sync"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const (
	maxProducersPerClient = 100
	maxConsumersPerClient = 10
)

type environmentShim interface {
	Close() error
	IsClosed() bool
	client() *stream.Environment
}

type clientEnvironment struct {
	*stream.Environment
}

func (e clientEnvironment) client() *stream.Environment {
	return e.Environment
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
	options := stream.NewEnvironmentOptions().
		SetMaxProducersPerClient(config.maxProducersPerClient).
		SetMaxConsumersPerClient(config.maxConsumersPerClient).
		SetUri(config.uri).
		SetTLSConfig(config.tlsConfig)
	env, err := stream.NewEnvironment(options)
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
func (e *Environment) Client() *stream.Environment {
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
