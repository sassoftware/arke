// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"sync"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	pb "github.com/sassoftware/arke/api"
)

const (
	streamPort         = 5552
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
	envLock   sync.Mutex
	env       streamEnvironmentShim
	streamURI string
	tlsConfig *tls.Config
}

var newStreamEnvironmentFunc = func(uri string, tlsConfig *tls.Config) (streamEnvironmentShim, error) {
	options := stream.NewEnvironmentOptions().
		SetMaxProducersPerClient(maxStreamProducers).
		SetMaxConsumersPerClient(maxStreamConsumers).
		SetUri(uri).
		SetTLSConfig(tlsConfig)
	return stream.NewEnvironment(options)
}

func newStreamConnection(uri string, tlsConfig *tls.Config) *streamConnection {
	return &streamConnection{streamURI: uri, tlsConfig: tlsConfig}
}

func getStreamConnectionString(cf *pb.ConnectionConfiguration, tlsEnabled bool) string {
	tenant := cf.GetTenant()
	if tenant == "" {
		tenant = "/"
	}

	scheme := "rabbitmq-stream"
	if tlsEnabled {
		scheme += "+tls"
	}

	credentials := cf.GetCredentials()
	return fmt.Sprintf("%s://%s:%s@%s:%d/%s", scheme,
		url.QueryEscape(credentials.GetUsername()),
		url.QueryEscape(credentials.GetPassword()),
		cf.GetHost(), streamPort, url.QueryEscape(tenant))
}

func (sc *streamConnection) Connect() error {
	sc.envLock.Lock()
	defer sc.envLock.Unlock()

	if sc.env != nil && !sc.env.IsClosed() {
		return nil
	}

	env, err := newStreamEnvironmentFunc(sc.streamURI, sc.tlsConfig)
	if err != nil {
		return err
	}
	sc.env = env
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
