// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	rabbitstream "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/sassoftware/arke/internal/provider/connectors/rabbitmq/stream"
	"github.com/sassoftware/arke/internal/util"
)

const maxPoolProducers = 10
const poolKeyName = "PoolName"

// NewStreamConnection Create a new streamConnection object with a connection string and tls config
func NewStreamConnection(connStr string, clientIdentifier string, tlsCfg *tls.Config) streamConnectionShim {
	ctx, cancel := context.WithCancel(context.Background())
	return &streamConnection{
		env:              stream.New(connStr, tlsCfg),
		clientIdentifier: clientIdentifier,
		publishers:       util.NewConcurrentMap(),
		ctx:              ctx,
		cancel:           cancel,
	}
}

type CtxKey struct {
	name string
}

func toStreamMessage(origMsg streamMessage) message.StreamMessage {
	msg := amqp.NewMessage(origMsg.Body)
	msg.ApplicationProperties = toStreamHeaders(origMsg.Headers)
	msg.Properties = &amqp.MessageProperties{ContentEncoding: origMsg.ContentEncoding,
		ContentType: origMsg.ContentType}
	if origMsg.PublishID > 0 {
		msg.SetPublishingId(origMsg.PublishID)
	}
	_, _ = msg.MarshalBinary()
	return msg
}

func toStreamHeaders(orig map[string]string) map[string]interface{} {
	intMap := make(map[string]interface{})
	for key, value := range orig {
		intMap[key] = value
	}
	return intMap
}

func fromStreamHeaders(orig map[string]interface{}) map[string]string {
	sMap := make(map[string]string)
	for key, value := range orig {
		sMap[key] = value.(string)
	}
	return sMap
}

func toStreamOffset(offset string, lastOffset int64) (rabbitstream.OffsetSpecification, error) {
	switch strings.ToLower(offset) {
	case "first":
		// start consuming from 0
		return rabbitstream.OffsetSpecification{}.First(), nil
	case "continue":
		// Valid values for lastOffset:
		// -1 : Start from the beginning of the stream
		// 0 : We have processed only 1 message from the stream, start from 1
		// > 0 : We have processed more than 1 message, start from the next
		// message because the offset is 0 based.
		lastOffset++
		return rabbitstream.OffsetSpecification{}.Offset(lastOffset), nil
	case "last":
		return rabbitstream.OffsetSpecification{}.Last(), nil
	case "next", "":
		return rabbitstream.OffsetSpecification{}.Next(), nil
	}
	pOffset, pErr := strconv.Atoi(offset)
	if pErr == nil {
		return rabbitstream.OffsetSpecification{}.Offset(int64(pOffset)), nil
	}
	return rabbitstream.OffsetSpecification{}, fmt.Errorf("invalid offset: %s", offset)
}
