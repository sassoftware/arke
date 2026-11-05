// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"testing"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
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
		Headers:         map[string]string{"header1": "value1", "header2": "value2"},
		ContentEncoding: "utf-8",
		ContentType:     "text/plain",
	}

	expectedMsg := amqp.NewMessage(origMsg.Body)
	expectedMsg.ApplicationProperties = map[string]interface{}{"header1": "value1", "header2": "value2"}
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
		"header1": "value1",
		"header2": "value2",
		"header3": "value3",
	}

	expectedHeaders := map[string]interface{}{
		"header1": "value1",
		"header2": "value2",
		"header3": "value3",
	}

	resultHeaders := toStreamHeaders(origHeaders)
	assert.Equal(t, expectedHeaders, resultHeaders)
}
