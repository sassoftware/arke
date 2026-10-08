// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"sync"
	"testing"

	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
)

func Test_getAdminPort(t *testing.T) {
	tests := []struct {
		name     string
		config   *pb.ConnectionConfiguration
		envPort  string
		expected int32
	}{
		{
			name:     "configured port takes precedence over environment",
			config:   &pb.ConnectionConfiguration{AdminPort: 8080},
			envPort:  "9090",
			expected: 8080,
		},
		{
			name:     "parses trimmed environment port",
			config:   &pb.ConnectionConfiguration{},
			envPort:  " 18080 ",
			expected: 18080,
		},
		{
			name:     "invalid environment port uses default",
			config:   &pb.ConnectionConfiguration{},
			envPort:  "not-a-port",
			expected: defaultAdminPort,
		},
		{
			name:     "out of range environment port uses default",
			config:   &pb.ConnectionConfiguration{},
			envPort:  "2147483648",
			expected: defaultAdminPort,
		},
		{
			name:     "zero environment port uses default",
			config:   &pb.ConnectionConfiguration{},
			envPort:  "0",
			expected: defaultAdminPort,
		},
		{
			name:     "unset environment and nil config use default",
			envPort:  "",
			expected: defaultAdminPort,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(EnvAdminPortName, tt.envPort)
			envAdminPortOnce = sync.Once{}
			envAdminPortValue = 0

			assert.Equal(t, tt.expected, getAdminPort(tt.config))
		})
	}

	envAdminPortOnce = sync.Once{}
	envAdminPortValue = 0
}

func Test_GetConnURL(t *testing.T) {
	tests := []struct {
		name     string
		config   *pb.ConnectionConfiguration
		expected string
	}{
		{ //nolint:gosec
			name: "amqp connection URL",
			config: &pb.ConnectionConfiguration{
				Host:       "localhost",
				Port:       5672,
				ClientName: "test-client",
				Credentials: &pb.Credentials{
					Username: "guest",
					Password: "guest",
				},
			},
			expected: "amqp://guest:guest@localhost:5672/",
		},
		{ //nolint:gosec
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
			expected: "amqps://guest:guest@broker.example.com:5671/",
		},
		{ //nolint:gosec
			name: "tenant",
			config: &pb.ConnectionConfiguration{
				Host:   "broker.example.com",
				Port:   5671,
				Tls:    true,
				Tenant: "my-tenant",
				Credentials: &pb.Credentials{
					Username: "guest",
					Password: "guest",
				},
			},
			expected: "amqps://guest:guest@broker.example.com:5671/my-tenant",
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
			expected: "amqp://user%40example.com:p%40ss%20word@[::1]:5672/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, GetConnURL(tt.config))
		})
	}
}

func Test_GetVhost(t *testing.T) {
	tests := []struct {
		name     string
		config   *pb.ConnectionConfiguration
		expected string
	}{
		{
			name:     "defaults to root vhost",
			config:   &pb.ConnectionConfiguration{},
			expected: "/",
		},
		{
			name:     "returns configured tenant",
			config:   &pb.ConnectionConfiguration{Tenant: "tenant-a"},
			expected: "tenant-a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, GetVhost(tt.config))
		})
	}
}

func Test_GetMgmtEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		config   *pb.ConnectionConfiguration
		expected string
	}{
		{
			name:     "http with default port",
			config:   &pb.ConnectionConfiguration{Host: "localhost"},
			expected: "http://localhost:15672",
		},
		{
			name: "https with default port",
			config: &pb.ConnectionConfiguration{
				Host: "broker.example.com",
				Tls:  true,
			},
			expected: "https://broker.example.com:15672",
		},
		{
			name: "http with configured port",
			config: &pb.ConnectionConfiguration{
				Host:      "broker.example.com",
				AdminPort: 8080,
			},
			expected: "http://broker.example.com:8080",
		},
		{
			name: "https with configured port",
			config: &pb.ConnectionConfiguration{
				Host:      "broker.example.com",
				AdminPort: 8443,
				Tls:       true,
			},
			expected: "https://broker.example.com:8443",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, GetMgmtEndpoint(tt.config))
		})
	}
}

func Test_GetUsernamePassword(t *testing.T) {
	config := &pb.ConnectionConfiguration{
		Credentials: &pb.Credentials{
			Username: "test-user",
			Password: "test-password",
		},
	}

	username, password := GetUsernamePassword(config)

	assert.Equal(t, "test-user", username)
	assert.Equal(t, "test-password", password)
}

func Test_SourceName(t *testing.T) {
	tests := []struct {
		name     string
		source   *pb.Source
		expected string
	}{
		{
			name:     "adds quorum suffix to queue",
			source:   &pb.Source{Name: "orders", Type: pb.Source_QUEUE},
			expected: "orders.quorum",
		},
		{
			name:     "does not duplicate quorum suffix",
			source:   &pb.Source{Name: "orders.quorum", Type: pb.Source_QUEUE},
			expected: "orders.quorum",
		},
		{
			name: "does not add quorum suffix to auto-delete queue",
			source: &pb.Source{
				Name:       "orders",
				Type:       pb.Source_QUEUE,
				AutoDelete: true,
			},
			expected: "orders",
		},
		{
			name:     "does not add quorum suffix to stream",
			source:   &pb.Source{Name: "orders", Type: pb.Source_STREAM},
			expected: "orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, SourceName(tt.source))
		})
	}
}

func Test_IsQuorum(t *testing.T) {
	tests := []struct {
		name     string
		source   *pb.Source
		expected bool
	}{
		{
			name:     "non-auto-delete queue is quorum",
			source:   &pb.Source{Type: pb.Source_QUEUE},
			expected: true,
		},
		{
			name:     "auto-delete queue is not quorum",
			source:   &pb.Source{Type: pb.Source_QUEUE, AutoDelete: true},
			expected: false,
		},
		{
			name:     "temporary queue is not quorum",
			source:   &pb.Source{Type: pb.Source_TEMPORARY},
			expected: false,
		},
		{
			name:     "stream is not quorum",
			source:   &pb.Source{Type: pb.Source_STREAM},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsQuorum(tt.source))
		})
	}
}
