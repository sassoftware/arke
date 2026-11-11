// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"fmt"

	"github.com/Azure/go-amqp"
	ramqp "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/util"
)

const (
	headerContentType     = "Content-Type"
	headerContentEncoding = "Content-Encoding"
)

var clientIdentifierCtxKey = CtxKey{name: "clientIdentifier"}

type CtxKey struct {
	name string
}

func subscriptionKey(source *pb.Source) string {
	return source.GetAddress().Name + "/" + source.GetName()
}

func delCtxToMessage(delCtx ramqp.IDeliveryContext, source *pb.Source) *pb.Message {
	message := delCtx.Message()
	if message == nil {
		return nil
	}
	headers := make(map[string]string)
	for header, value := range delCtx.Message().ApplicationProperties {
		switch value := value.(type) {
		case string:
			headers[header] = value
		default:
			headers[header] = fmt.Sprintf("%v", value)
		}
	}

	// later, annotations[x-stream-offset] to x-current-offset

	// TODO: should these properties take precedence over (user) ApplicationProperties?
	props := message.Properties
	if props == nil {
		props = &amqp.MessageProperties{}
	}
	if ct, ok := props.ContentType, props.ContentType != nil; ok {
		headers[headerContentType] = *ct
	}
	if ce, ok := props.ContentEncoding, props.ContentEncoding != nil; ok {
		headers[headerContentEncoding] = *ce
	}

	body := make([]byte, 0)
	if message.Data != nil {
		for _, b := range message.Data {
			body = append(body, b...)
		}
	}
	msg := &pb.Message{
		Body:    body,
		Uuid:    util.GenUUID(),
		Headers: headers,
		Address: source.GetAddress(),

		// TODO: how to get the publish id
		PublishId: 0,

		// TODO: how to get the publish name
		PublisherName: "",
	}

	return msg
}
