// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"testing"

	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
)

func TestGetenv(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		defaultValue string
		envValue     string
		setEnv       bool
		expected     string
	}{
		{
			name:         "returns default when env var not set",
			key:          "TEST_UNSET_VAR",
			defaultValue: "default_value",
			setEnv:       false,
			expected:     "default_value",
		},
		{
			name:         "returns env value when set",
			key:          "TEST_SET_VAR",
			defaultValue: "default_value",
			envValue:     "custom_value",
			setEnv:       true,
			expected:     "custom_value",
		},
		{
			name:         "returns empty string when env var is empty",
			key:          "TEST_EMPTY_VAR",
			defaultValue: "default_value",
			envValue:     "",
			setEnv:       true,
			expected:     "",
		},
		{
			name:         "returns default when key is empty",
			key:          "",
			defaultValue: "default_value",
			setEnv:       false,
			expected:     "default_value",
		},
		{
			name:         "handles special characters in env value",
			key:          "TEST_SPECIAL_VAR",
			defaultValue: "default",
			envValue:     "value!@#$%^&*()",
			setEnv:       true,
			expected:     "value!@#$%^&*()",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Ensure clean state
			os.Unsetenv(tt.key)
			defer os.Unsetenv(tt.key)

			if tt.setEnv {
				os.Setenv(tt.key, tt.envValue)
			}

			result := getenv(tt.key, tt.defaultValue)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConnectionConfigurationFromEnv_Defaults(t *testing.T) {
	// Clean up all broker-related env vars
	envVars := []string{
		"ARKE_BROKER_HOSTNAME",
		"ARKE_BROKER_PORT",
		"ARKE_BROKER_ADMIN_PORT",
		"ARKE_BROKER_USERNAME",
		"ARKE_BROKER_PASSWORD",
		"ARKE_BROKER_TYPE",
		"ARKE_BROKER_TENANT",
	}

	for _, v := range envVars {
		os.Unsetenv(v)
	}
	defer func() {
		for _, v := range envVars {
			os.Unsetenv(v)
		}
	}()

	config := ConnectionConfigurationFromEnv()

	assert.Equal(t, "rabbitmq", config.Host)
	assert.Equal(t, int32(5672), config.Port)
	assert.Equal(t, int32(0), config.AdminPort)
	assert.Equal(t, "guest", config.Credentials.Username)
	assert.Equal(t, "guest", config.Credentials.Password)
	assert.Equal(t, "amqp091", config.Provider)
	assert.Equal(t, "/", config.Tenant)
}

func TestConnectionConfigurationFromEnv_CustomValues(t *testing.T) {
	// Set custom environment variables
	os.Setenv("ARKE_BROKER_HOSTNAME", "custom-broker.example.com")
	os.Setenv("ARKE_BROKER_PORT", "5671")
	os.Setenv("ARKE_BROKER_ADMIN_PORT", "15672")
	os.Setenv("ARKE_BROKER_USERNAME", "admin")
	os.Setenv("ARKE_BROKER_PASSWORD", "secret123")
	os.Setenv("ARKE_BROKER_TYPE", "amqp10")
	os.Setenv("ARKE_BROKER_TENANT", "/custom-tenant")

	defer func() {
		os.Unsetenv("ARKE_BROKER_HOSTNAME")
		os.Unsetenv("ARKE_BROKER_PORT")
		os.Unsetenv("ARKE_BROKER_ADMIN_PORT")
		os.Unsetenv("ARKE_BROKER_USERNAME")
		os.Unsetenv("ARKE_BROKER_PASSWORD")
		os.Unsetenv("ARKE_BROKER_TYPE")
		os.Unsetenv("ARKE_BROKER_TENANT")
	}()

	config := ConnectionConfigurationFromEnv()

	assert.Equal(t, "custom-broker.example.com", config.Host)
	assert.Equal(t, int32(5671), config.Port)
	assert.Equal(t, int32(15672), config.AdminPort)
	assert.Equal(t, "admin", config.Credentials.Username)
	assert.Equal(t, "secret123", config.Credentials.Password)
	assert.Equal(t, "amqp10", config.Provider)
	assert.Equal(t, "/custom-tenant", config.Tenant)
}

func TestConnectionConfigurationFromEnv_PartialCustomValues(t *testing.T) {
	// Set only some custom values, others should use defaults
	os.Setenv("ARKE_BROKER_HOSTNAME", "my-broker.local")
	os.Setenv("ARKE_BROKER_USERNAME", "myuser")
	os.Setenv("ARKE_BROKER_TYPE", "rabbitmq")

	defer func() {
		os.Unsetenv("ARKE_BROKER_HOSTNAME")
		os.Unsetenv("ARKE_BROKER_USERNAME")
		os.Unsetenv("ARKE_BROKER_TYPE")
	}()

	config := ConnectionConfigurationFromEnv()

	assert.Equal(t, "my-broker.local", config.Host)
	assert.Equal(t, int32(5672), config.Port)   // default
	assert.Equal(t, int32(0), config.AdminPort) // default
	assert.Equal(t, "myuser", config.Credentials.Username)
	assert.Equal(t, "guest", config.Credentials.Password) // default
	assert.Equal(t, "rabbitmq", config.Provider)
	assert.Equal(t, "/", config.Tenant) // default
}

func TestConnectionConfigurationFromEnv_PortParsing(t *testing.T) {
	tests := []struct {
		name          string
		port          string
		adminPort     string
		expectedPort  int32
		expectedAdmin int32
	}{
		{
			name:          "standard ports",
			port:          "5672",
			adminPort:     "15672",
			expectedPort:  5672,
			expectedAdmin: 15672,
		},
		{
			name:          "custom high ports",
			port:          "9999",
			adminPort:     "19999",
			expectedPort:  9999,
			expectedAdmin: 19999,
		},
		{
			name:          "low port numbers",
			port:          "80",
			adminPort:     "443",
			expectedPort:  80,
			expectedAdmin: 443,
		},
		{
			name:          "admin port zero",
			port:          "5672",
			adminPort:     "0",
			expectedPort:  5672,
			expectedAdmin: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("ARKE_BROKER_PORT", tt.port)
			os.Setenv("ARKE_BROKER_ADMIN_PORT", tt.adminPort)

			defer func() {
				os.Unsetenv("ARKE_BROKER_PORT")
				os.Unsetenv("ARKE_BROKER_ADMIN_PORT")
			}()

			config := ConnectionConfigurationFromEnv()

			assert.Equal(t, tt.expectedPort, config.Port)
			assert.Equal(t, tt.expectedAdmin, config.AdminPort)
		})
	}
}

func TestConnectionConfigurationFromEnv_CredentialsNotNil(t *testing.T) {
	os.Unsetenv("ARKE_BROKER_USERNAME")
	os.Unsetenv("ARKE_BROKER_PASSWORD")

	config := ConnectionConfigurationFromEnv()

	assert.NotNil(t, config.Credentials, "Credentials should never be nil")
	assert.Equal(t, "guest", config.Credentials.Username)
	assert.Equal(t, "guest", config.Credentials.Password)
}

func TestConnectionConfigurationFromEnv_BrokerTypes(t *testing.T) {
	tests := []struct {
		name         string
		brokerType   string
		expectedType string
	}{
		{
			name:         "amqp091 broker",
			brokerType:   "amqp091",
			expectedType: "amqp091",
		},
		{
			name:         "amqp10 broker",
			brokerType:   "amqp10",
			expectedType: "amqp10",
		},
		{
			name:         "rabbitmq broker",
			brokerType:   "rabbitmq",
			expectedType: "rabbitmq",
		},
		{
			name:         "custom broker type",
			brokerType:   "custom-broker",
			expectedType: "custom-broker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("ARKE_BROKER_TYPE", tt.brokerType)
			defer os.Unsetenv("ARKE_BROKER_TYPE")

			config := ConnectionConfigurationFromEnv()

			assert.Equal(t, tt.expectedType, config.Provider)
		})
	}
}

func TestConnectionConfigurationFromEnv_TenantValues(t *testing.T) {
	tests := []struct {
		name           string
		tenant         string
		expectedTenant string
	}{
		{
			name:           "default root tenant",
			tenant:         "",
			expectedTenant: "/",
		},
		{
			name:           "custom tenant path",
			tenant:         "/my-tenant",
			expectedTenant: "/my-tenant",
		},
		{
			name:           "multi-level tenant path",
			tenant:         "/org/team/project",
			expectedTenant: "/org/team/project",
		},
		{
			name:           "simple tenant name",
			tenant:         "production",
			expectedTenant: "production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.tenant != "" {
				os.Setenv("ARKE_BROKER_TENANT", tt.tenant)
				defer os.Unsetenv("ARKE_BROKER_TENANT")
			} else {
				os.Unsetenv("ARKE_BROKER_TENANT")
			}

			config := ConnectionConfigurationFromEnv()

			assert.Equal(t, tt.expectedTenant, config.Tenant)
		})
	}
}

func TestConnectionConfigurationFromEnv_EmptyStringValues(t *testing.T) {
	// Set all vars to empty strings
	os.Setenv("ARKE_BROKER_HOSTNAME", "")
	os.Setenv("ARKE_BROKER_USERNAME", "")
	os.Setenv("ARKE_BROKER_PASSWORD", "")
	os.Setenv("ARKE_BROKER_TYPE", "")
	os.Setenv("ARKE_BROKER_TENANT", "")

	defer func() {
		os.Unsetenv("ARKE_BROKER_HOSTNAME")
		os.Unsetenv("ARKE_BROKER_USERNAME")
		os.Unsetenv("ARKE_BROKER_PASSWORD")
		os.Unsetenv("ARKE_BROKER_TYPE")
		os.Unsetenv("ARKE_BROKER_TENANT")
	}()

	config := ConnectionConfigurationFromEnv()

	// When set to empty string, they should be empty, not defaults
	assert.Equal(t, "", config.Host)
	assert.Equal(t, "", config.Credentials.Username)
	assert.Equal(t, "", config.Credentials.Password)
	assert.Equal(t, "", config.Provider)
	assert.Equal(t, "", config.Tenant)
}

func TestConnectionConfigurationFromEnv_HostnameVariations(t *testing.T) {
	tests := []struct {
		name         string
		hostname     string
		expectedHost string
	}{
		{
			name:         "localhost",
			hostname:     "localhost",
			expectedHost: "localhost",
		},
		{
			name:         "IP address",
			hostname:     "192.168.1.100",
			expectedHost: "192.168.1.100",
		},
		{
			name:         "fully qualified domain name",
			hostname:     "broker.prod.example.com",
			expectedHost: "broker.prod.example.com",
		},
		{
			name:         "simple hostname",
			hostname:     "rabbitmq-server",
			expectedHost: "rabbitmq-server",
		},
		{
			name:         "hostname with port (though port should be separate)",
			hostname:     "broker:5672",
			expectedHost: "broker:5672",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("ARKE_BROKER_HOSTNAME", tt.hostname)
			defer os.Unsetenv("ARKE_BROKER_HOSTNAME")

			config := ConnectionConfigurationFromEnv()

			assert.Equal(t, tt.expectedHost, config.Host)
		})
	}
}

func TestConnectionConfigurationFromEnv_ReturnsCorrectType(t *testing.T) {
	config := ConnectionConfigurationFromEnv()

	assert.NotNil(t, config.Credentials)
	assert.IsType(t, &pb.Credentials{}, config.Credentials)
}
