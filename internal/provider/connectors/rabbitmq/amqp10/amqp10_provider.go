// Copyright (c) 2026, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

var supportedSourceOptions = map[string]bool{
	"MessageTTL":        true,
	"DeadLetterAddress": true,
	"DeadLetterSubject": true,
	"Expires":           true,
	"Offset":            true,
	"ConsumerGroup":     true,
}
var supportedStreamSourceOptions = map[string]bool{"Offset": true, "MessageTTL": true, "ConsumerGroup": true}

// TODO - Issue 187 - Implement the remaining Provider methods and register the RabbitMQ AMQP 1.0 provider.
type rabbitMQAMQP10Provider struct{}

// NewRabbitMQAMQP10Provider returns a RabbitMQ AMQP 1.0 provider instance.
func NewRabbitMQAMQP10Provider() *rabbitMQAMQP10Provider {
	return &rabbitMQAMQP10Provider{}
}

// SupportedSourceOptions returns the source options supported by AMQP 1.0.
func (prov *rabbitMQAMQP10Provider) SupportedSourceOptions() map[string]bool {
	return supportedSourceOptions
}
