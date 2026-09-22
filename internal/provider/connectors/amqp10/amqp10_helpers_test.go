package amqp10

import (
	"testing"

	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
)

func Test_getConnURL(t *testing.T) {
	tests := []struct {
		name     string
		config   *pb.ConnectionConfiguration
		expected string
	}{
		{ // nolint:gosec
			name:     "amqp connection URL",
			config:   newTestConnectionConfig(),
			expected: "amqp://guest:guest@localhost:5672",
		},
		{ // nolint:gosec
			name: "amqps connection URL",
			config: &pb.ConnectionConfiguration{
				Host: "broker.example.com",
				Port: 5671,
				Tls:  true,
				Credentials: &pb.Credentials{
					Username: "guest",
					Password: "guest",
				},
			},
			expected: "amqps://guest:guest@broker.example.com:5671",
		},
		{
			name: "connection URL escapes credentials and brackets IPv6 host",
			config: &pb.ConnectionConfiguration{
				Host: "::1",
				Port: 5672,
				Credentials: &pb.Credentials{
					Username: "user@example.com",
					Password: "p@ss word",
				},
			},
			expected: "amqp://user%40example.com:p%40ss%20word@[::1]:5672",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, getConnURL(tt.config))
		})
	}
}
