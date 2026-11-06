// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import "github.com/Azure/go-amqp"

// rabbitMQAMQP10Table Simple map
type rabbitMQAMQP10Table map[string]any

// rabbitMQAMQP10Message Structure of a message
type rabbitMQAMQP10Message struct {
	delivery        any
	Body            []byte
	DeliveryMode    int
	ContentType     string
	ContentEncoding string
	Headers         rabbitMQAMQP10Table
	DeliveryTag     uint64
}

func fromAmqpMessage(orig *amqp.Message) *rabbitMQAMQP10Message {
	msg := &rabbitMQAMQP10Message{
		delivery: orig,
	}
	// TODO: populate other fields from orig
	return msg
}

// SetDelivery convenience method for unit tests
func (msg *rabbitMQAMQP10Message) SetDelivery(delivery any) {
	msg.delivery = delivery
}
