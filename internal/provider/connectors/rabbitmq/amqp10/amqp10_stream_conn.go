// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"crypto/tls"
	"errors"
	"fmt"
	"sync"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const (
	maxStreamProducers = 100
	maxStreamConsumers = 10
)

type streamConnectionShim interface {
	Connect() error
	Close() error
	IsClosed() bool
}

type streamEnvironmentShim interface {
	Close() error
	IsClosed() bool
}

type streamConnection struct {
	envLock       sync.Mutex
	env           streamEnvironmentShim
	envClosed     bool
	streamURI     string
	tlsConfig     *tls.Config
	brokerDetails *BrokerDetails
}

var newStreamEnvironmentFunc = func(uri string, tlsConfig *tls.Config) (streamEnvironmentShim, error) {
	options := stream.NewEnvironmentOptions().
		SetMaxProducersPerClient(maxStreamProducers).
		SetMaxConsumersPerClient(maxStreamConsumers).
		SetUri(uri).
		SetTLSConfig(tlsConfig)
	return stream.NewEnvironment(options)
}

func newStreamConnection(uri string, tlsConfig *tls.Config, brokerDetails *BrokerDetails) *streamConnection {
	return &streamConnection{streamURI: uri, tlsConfig: tlsConfig, brokerDetails: brokerDetails}
}

func (sc *streamConnection) Connect() error {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()

	if sc.brokerDetails != nil && sc.brokerDetails.clientDisconnect.Load() {
		return errors.New("provider has been disconnected")
	}
	if sc.env != nil && !sc.envClosed && !sc.env.IsClosed() {
		return nil
	}

	env, err := newStreamEnvironmentFunc(sc.streamURI, sc.tlsConfig)
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

	if sc.brokerDetails != nil {
		sc.brokerDetails.Lock()
		if sc.brokerDetails.clientDisconnect.Load() {
			sc.brokerDetails.Unlock()
			if closeErr := env.Close(); closeErr != nil {
				return fmt.Errorf("provider has been disconnected (closing newly created stream environment: %v)", closeErr)
			}
			return errors.New("provider has been disconnected")
		}
		sc.env = env
		sc.envClosed = false
		sc.brokerDetails.Unlock()
		return nil
	}

	sc.env = env
	sc.envClosed = false
	return nil
}

func (sc *streamConnection) Close() error {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()

	if sc.env == nil || sc.envClosed || sc.env.IsClosed() {
		sc.envClosed = true
		return nil
	}
	sc.envClosed = true
	return sc.env.Close()
}

func (sc *streamConnection) IsClosed() bool {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()

	return sc.env == nil || sc.env.IsClosed()
}
