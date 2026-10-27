// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"testing"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
)

func Test_toStreamOffset(t *testing.T) {
	off, err := toStreamOffset("first", 0)
	assert.Equal(t, stream.OffsetSpecification{}.First(), off)
	assert.Nil(t, err)

	off, err = toStreamOffset("FIRST", 0)
	assert.Equal(t, stream.OffsetSpecification{}.First(), off)
	assert.Nil(t, err)

	off, err = toStreamOffset("continue", 100)
	assert.Equal(t, stream.OffsetSpecification{}.Offset(101), off)
	assert.Nil(t, err)

	off, err = toStreamOffset("continue", 0)
	assert.Equal(t, stream.OffsetSpecification{}.Offset(1), off)
	assert.Nil(t, err)

	off, err = toStreamOffset("continue", -1)
	assert.Equal(t, stream.OffsetSpecification{}.Offset(0), off)
	assert.Nil(t, err)

	off, err = toStreamOffset("0", 100)
	assert.Equal(t, stream.OffsetSpecification{}.Offset(0), off)
	assert.Nil(t, err)

	off, err = toStreamOffset("next", 100)
	assert.Equal(t, stream.OffsetSpecification{}.Next(), off)
	assert.Nil(t, err)

	off, err = toStreamOffset("", 100)
	assert.Equal(t, stream.OffsetSpecification{}.Next(), off)
	assert.Nil(t, err)

	_, err = toStreamOffset("invalid", 100)
	assert.NotNil(t, err)
}

func Test_toStreamMessage(t *testing.T) {
	origMsg := streamMessage{
		Body:            []byte("test body"),
		Headers:         map[string]string{testHeader1: testValue1, testHeader2: testValue2},
		ContentEncoding: "utf-8",
		ContentType:     "text/plain",
	}

	expectedMsg := amqp.NewMessage(origMsg.Body)
	expectedMsg.ApplicationProperties = map[string]interface{}{testHeader1: testValue1, testHeader2: testValue2}
	expectedMsg.Properties = &amqp.MessageProperties{
		ContentEncoding: "utf-8",
		ContentType:     "text/plain",
	}

	resultMsg := toStreamMessage(origMsg)

	assert.Equal(t, expectedMsg.ApplicationProperties, resultMsg.GetApplicationProperties())
	assert.Equal(t, expectedMsg.Properties.ContentEncoding, resultMsg.GetMessageProperties().ContentEncoding)
	assert.Equal(t, expectedMsg.Properties.ContentType, resultMsg.GetMessageProperties().ContentType)
}

func Test_toStreamHeaders(t *testing.T) {
	origHeaders := map[string]string{
		testHeader1: testValue1,
		testHeader2: testValue2,
		"header3":   testValue3,
	}

	expectedHeaders := map[string]interface{}{
		testHeader1: testValue1,
		testHeader2: testValue2,
		"header3":   testValue3,
	}

	resultHeaders := toStreamHeaders(origHeaders)
	assert.Equal(t, expectedHeaders, resultHeaders)
}

func Test_getStreamConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		bd       *BrokerDetails
		expected string
	}{
		{
			name: "without tenant and TLS",
			bd: &BrokerDetails{
				connectionConfig: &pb.ConnectionConfiguration{
					Credentials: &pb.Credentials{
						Username: testUser,
						Password: testPass,
					},
					Host: testLocalhost,
				},
				tlsEnabled: false,
			},
			expected: "rabbitmq-stream://user:pass@localhost:5552/%2F",
		},
		{
			name: "with tenant and without TLS",
			bd: &BrokerDetails{
				connectionConfig: &pb.ConnectionConfiguration{
					Credentials: &pb.Credentials{
						Username: testUser,
						Password: testPass,
					},
					Host:   testLocalhost,
					Tenant: "tenant1",
				},
				tlsEnabled: false,
			},
			expected: "rabbitmq-stream://user:pass@localhost:5552/tenant1",
		},
		{
			name: "without tenant and with TLS",
			bd: &BrokerDetails{
				connectionConfig: &pb.ConnectionConfiguration{
					Credentials: &pb.Credentials{
						Username: testUser,
						Password: testPass,
					},
					Host: testLocalhost,
				},
				tlsEnabled: true,
			},
			expected: "rabbitmq-stream+tls://user:pass@localhost:5552/%2F",
		},
		{
			name: "with tenant and TLS",
			bd: &BrokerDetails{
				connectionConfig: &pb.ConnectionConfiguration{
					Credentials: &pb.Credentials{
						Username: testUser,
						Password: testPass,
					},
					Host:   testLocalhost,
					Tenant: "tenant1",
				},
				tlsEnabled: true,
			},
			expected: "rabbitmq-stream+tls://user:pass@localhost:5552/tenant1",
		},
		{
			name: "escapechar tenant and TLS",
			bd: &BrokerDetails{
				connectionConfig: &pb.ConnectionConfiguration{
					Credentials: &pb.Credentials{
						Username: testUser,
						Password: testPass,
					},
					Host:   testLocalhost,
					Tenant: "/",
				},
				tlsEnabled: false,
			},
			expected: "rabbitmq-stream://user:pass@localhost:5552/%2F",
		},
		{
			name: "escapechar tenant and TLS",
			bd: &BrokerDetails{
				connectionConfig: &pb.ConnectionConfiguration{
					Credentials: &pb.Credentials{
						Username: testUser,
						Password: testPass,
					},
					Host:   testLocalhost,
					Tenant: "/",
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
