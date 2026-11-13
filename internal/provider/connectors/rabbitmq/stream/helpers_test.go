// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"testing"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	streamclient "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_parseOffset(t *testing.T) {
	cases := []struct {
		offset string
		last   int64
		want   streamclient.OffsetSpecification
		bad    bool
	}{
		{offset: "first", want: streamclient.OffsetSpecification{}.First()},
		{offset: "FIRST", want: streamclient.OffsetSpecification{}.First()},
		{offset: "continue", last: 100, want: streamclient.OffsetSpecification{}.Offset(101)},
		{offset: "continue", want: streamclient.OffsetSpecification{}.Offset(1)},
		{offset: "continue", last: -1, want: streamclient.OffsetSpecification{}.Offset(0)},
		{offset: "0", last: 100, want: streamclient.OffsetSpecification{}.Offset(0)},
		{offset: "last", want: streamclient.OffsetSpecification{}.Last()},
		{offset: "next", want: streamclient.OffsetSpecification{}.Next()},
		{offset: "", want: streamclient.OffsetSpecification{}.Next()},
		{offset: "invalid", bad: true},
	}
	for _, tc := range cases {
		t.Run(tc.offset, func(t *testing.T) {
			got, err := parseOffset(tc.offset, tc.last)
			if tc.bad {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func Test_toStreamMessage(t *testing.T) {
	original := Message{
		Body:            []byte("test body"),
		Headers:         map[string]string{"header1": "value1", "header2": "value2"},
		ContentEncoding: "utf-8",
		ContentType:     "text/plain",
		PublishID:       12,
	}
	result := toStreamMessage(original)
	assert.Equal(t, map[string]interface{}{"header1": "value1", "header2": "value2"}, result.GetApplicationProperties())
	assert.Equal(t, "utf-8", result.GetMessageProperties().ContentEncoding)
	assert.Equal(t, "text/plain", result.GetMessageProperties().ContentType)
	assert.Equal(t, int64(12), result.GetPublishingId())
}

func Test_fromStreamMessageDecompressesMarkedBody(t *testing.T) {
	original := []byte("stream message body")
	compressed, err := compressBody(original)
	assert.NoError(t, err)
	clientMessage := makeClientMessage(t, compressed, map[string]interface{}{transferEncodingHeader: "gzip", "custom": "value"})
	result := fromStreamMessage(clientMessage)
	assert.Equal(t, original, result.Body)
	assert.Equal(t, map[string]string{"custom": "value"}, result.Headers)
}

func Test_fromStreamMessageConvertsNonStringApplicationProperties(t *testing.T) {
	clientMessage := makeClientMessage(t, []byte("message"), map[string]interface{}{
		"enabled": true,
		"retries": int32(3),
	})

	result := fromStreamMessage(clientMessage)

	assert.Equal(t, map[string]string{"enabled": "true", "retries": "3"}, result.Headers)
}

func makeClientMessage(t *testing.T, body []byte, headers map[string]interface{}) *amqp.Message {
	t.Helper()

	message := amqp.NewMessage(body)
	message.ApplicationProperties = headers
	wireMessage, err := message.MarshalBinary()
	require.NoError(t, err)

	clientMessage := &amqp.Message{}
	require.NoError(t, clientMessage.UnmarshalBinary(wireMessage))
	return clientMessage
}
