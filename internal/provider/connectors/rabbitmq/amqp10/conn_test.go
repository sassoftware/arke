// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"crypto/tls"
	"strings"
	"testing"

	ramqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestConnectionConfig() *pb.ConnectionConfiguration {
	return &pb.ConnectionConfiguration{
		Host:       "localhost",
		Port:       5672,
		ClientName: "test-client",
		Credentials: &pb.Credentials{
			Username: "guest",
			Password: "guest",
		},
	}
}

func Test_getAmqp10ConnOptions(t *testing.T) {
	config := newTestConnectionConfig()
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	clientIdentifier := "test-client"
	ctx, name := newTestProviderContext(t, clientIdentifier)
	assert.True(t, strings.HasPrefix(name, clientIdentifier), "name should have prefix %s", clientIdentifier)
	options, err := getRabbitMQAMQP10ConnOptions(ctx, config, tlsCfg)
	require.NoError(t, err)

	assert.NotNil(t, options.SASLType)
	assert.Same(t, tlsCfg, options.TLSConfig)
}

func Test_rabbitMQAMQP10Connection_WatchConnectionRejectsNilConnection(t *testing.T) {
	connection := &rabbitMQAMQP10Connection{}

	err := connection.WatchConnection(make(chan *ramqp.StateChanged))

	require.EqualError(t, err, "connection is nil")
}
