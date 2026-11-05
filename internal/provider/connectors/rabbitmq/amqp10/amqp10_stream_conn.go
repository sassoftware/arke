// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"crypto/tls"
	"errors"
	"fmt"
	"sync"

	"github.com/sassoftware/arke/internal/provider/connectors/rabbitmq/stream"
)

type streamConnectionShim interface {
	Connect() error
	Close() error
	IsClosed() bool
}

type streamEnvironmentShim interface {
	Connect() error
	Close() error
	IsClosed() bool
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
