// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"testing"

	pb "github.com/sassoftware/arke/api"
	iamqp "github.com/sassoftware/arke/internal/provider/connectors/amqp"
	"github.com/stretchr/testify/assert"
)

func Test_getStreamConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		bd       *BrokerDetails
		expected string
	}{
		{
			name: "without tenant and TLS",
			bd: &BrokerDetails{
				CommonBrokerDetails: iamqp.CommonBrokerDetails{
					ConnectionConfig: &pb.ConnectionConfiguration{
						Credentials: &pb.Credentials{
							Username: "user",
							Password: "pass",
						},
						Host: "localhost",
					},
				},
				tlsEnabled: false,
			},
			expected: "rabbitmq-stream://user:pass@localhost:5552/%2F",
		},
		{
			name: "with tenant and without TLS",
			bd: &BrokerDetails{
				CommonBrokerDetails: iamqp.CommonBrokerDetails{
					ConnectionConfig: &pb.ConnectionConfiguration{
						Credentials: &pb.Credentials{
							Username: "user",
							Password: "pass",
						},
						Host:   "localhost",
						Tenant: "tenant1",
					},
				},
				tlsEnabled: false,
			},
			expected: "rabbitmq-stream://user:pass@localhost:5552/tenant1",
		},
		{
			name: "without tenant and with TLS",
			bd: &BrokerDetails{
				CommonBrokerDetails: iamqp.CommonBrokerDetails{
					ConnectionConfig: &pb.ConnectionConfiguration{
						Credentials: &pb.Credentials{
							Username: "user",
							Password: "pass",
						},
						Host: "localhost",
					},
				},
				tlsEnabled: true,
			},
			expected: "rabbitmq-stream+tls://user:pass@localhost:5552/%2F",
		},
		{
			name: "with tenant and TLS",
			bd: &BrokerDetails{
				CommonBrokerDetails: iamqp.CommonBrokerDetails{
					ConnectionConfig: &pb.ConnectionConfiguration{
						Credentials: &pb.Credentials{
							Username: "user",
							Password: "pass",
						},
						Host:   "localhost",
						Tenant: "tenant1",
					},
				},
				tlsEnabled: true,
			},
			expected: "rabbitmq-stream+tls://user:pass@localhost:5552/tenant1",
		},
		{
			name: "escapechar tenant and TLS",
			bd: &BrokerDetails{
				CommonBrokerDetails: iamqp.CommonBrokerDetails{
					ConnectionConfig: &pb.ConnectionConfiguration{
						Credentials: &pb.Credentials{
							Username: "user",
							Password: "pass",
						},
						Host:   "localhost",
						Tenant: "/",
					},
				},
				tlsEnabled: false,
			},
			expected: "rabbitmq-stream://user:pass@localhost:5552/%2F",
		},
		{
			name: "escapechar tenant and TLS",
			bd: &BrokerDetails{
				CommonBrokerDetails: iamqp.CommonBrokerDetails{
					ConnectionConfig: &pb.ConnectionConfiguration{
						Credentials: &pb.Credentials{
							Username: "user",
							Password: "pass",
						},
						Host:   "localhost",
						Tenant: "/",
					},
				},
				tlsEnabled: true,
			},
			expected: "rabbitmq-stream+tls://user:pass@localhost:5552/%2F",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getStreamConnectionString(tt.bd)
			assert.Equal(t, tt.expected, result)
		})
	}
}
