// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"testing"

	amqp "github.com/Azure/go-amqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_DelCtxToMessage_NilMessage(t *testing.T) {
	delivery := &subscriptionDeliveryStub{}

	assert.Nil(t, delCtxToMessage(delivery, &pb.Source{}))
}

func Test_DelCtxToMessage_MapsBodyAndHeaders(t *testing.T) {
	contentType := "application/json"
	contentEncoding := "gzip"
	address := &pb.Address{Name: "events"}
	delivery := &subscriptionDeliveryStub{message: &amqp.Message{
		Data: [][]byte{[]byte("payload")},
		ApplicationProperties: map[string]any{
			"trace-id":           "trace-123",
			"attempt":            3,
			headerContentType:     "application/property",
			headerContentEncoding: "identity",
		},
		Properties: &amqp.MessageProperties{
			ContentType:     &contentType,
			ContentEncoding: &contentEncoding,
		},
	}}

	message := delCtxToMessage(delivery, &pb.Source{Address: address})

	require.NotNil(t, message)
	assert.Equal(t, []byte("payload"), message.Body)
	assert.Equal(t, map[string]string{
		"trace-id":           "trace-123",
		"attempt":            "3",
		headerContentType:     contentType,
		headerContentEncoding: contentEncoding,
	}, message.Headers)
	assert.Equal(t, address, message.Address)
	assert.NotEmpty(t, message.Uuid)
	assert.Zero(t, message.PublishId)
	assert.Empty(t, message.PublisherName)
}

func Test_DelCtxToMessage_HandlesMissingBodyAndProperties(t *testing.T) {
	delivery := &subscriptionDeliveryStub{message: &amqp.Message{}}

	message := delCtxToMessage(delivery, &pb.Source{})

	require.NotNil(t, message)
	assert.Empty(t, message.Body)
	assert.Empty(t, message.Headers)
	assert.NotNil(t, message.Headers)
}