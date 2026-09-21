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

type amqp10provider struct{}

// NewAMQP10Provider returns an AMQP 1.0 provider instance.
func NewAMQP10Provider() *amqp10provider {
	return &amqp10provider{}
}

// SupportedSourceOptions returns the source options supported by AMQP 1.0.
func (prov *amqp10provider) SupportedSourceOptions() map[string]bool {
	return supportedSourceOptions
}
