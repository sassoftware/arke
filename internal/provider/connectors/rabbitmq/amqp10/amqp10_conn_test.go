package amqp10

import (
	"crypto/tls"
	"strings"
	"testing"

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
	options, err := getAmqp10ConnOptions(ctx, config, tlsCfg)
	require.NoError(t, err)

	assert.NotNil(t, options.SASLType)
	assert.Same(t, tlsCfg, options.TLSConfig)
}
