// Copyright (c) 2026, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"errors"

	"github.com/sassoftware/arke/internal/provider/connectors/amqp/track"
)

var supportedSourceOptions = map[string]bool{
	"MessageTTL":        true,
	"DeadLetterAddress": true,
	"DeadLetterSubject": true,
	"Expires":           true,
	"Offset":            true,
	"ConsumerGroup":     true,
}
var supportedStreamSourceOptions = map[string]bool{
	"Offset":        true,
	"MessageTTL":    true,
	"ConsumerGroup": true,
}

// declarationClient is the temporary seam for declaration methods that will be
// implemented by the AMQP 1.0 provider in issues 190 and 191.
type declarationClient interface {
	DeclareExchange(string) error
	DeclareQueue(string) error
	DeclareStream(string) error
}

// TODO - Issue 187 - Implement the remaining Provider methods and register the
// RabbitMQ AMQP 1.0 provider. The declaration methods below are intentionally
// small so they can be reconciled with the broker client introduced by that work.
type rabbitMQAMQP10Provider struct {
	tracker      *track.Tracker
	declarations declarationClient
}

// TODO - Move entity tracking to the AMQP 1.0 per-connection state when that connection model is implemented.

// NewRabbitMQAMQP10Provider returns a RabbitMQ AMQP 1.0 provider instance.
func NewRabbitMQAMQP10Provider() *rabbitMQAMQP10Provider {
	return newRabbitMQAMQP10Provider(nil)
}

func newRabbitMQAMQP10Provider(declarations declarationClient) *rabbitMQAMQP10Provider {
	return &rabbitMQAMQP10Provider{
		tracker:      track.New(),
		declarations: declarations,
	}
}

// SupportedSourceOptions returns the source options supported by AMQP 1.0.
func (prov *rabbitMQAMQP10Provider) SupportedSourceOptions() map[string]bool {
	return supportedSourceOptions
}

func (prov *rabbitMQAMQP10Provider) declareExchange(name string) error {
	if prov.tracker.ExchangeExists(name) {
		return nil
	}
	if prov.declarations == nil {
		return errors.New("AMQP 1.0 exchange declaration is not implemented; see issue 191")
	}
	if err := prov.declarations.DeclareExchange(name); err != nil {
		return err
	}
	prov.tracker.AddExchange(name)
	return nil
}

func (prov *rabbitMQAMQP10Provider) declareQueue(name string) error {
	if prov.tracker.QueueExists(name) {
		return nil
	}
	if prov.declarations == nil {
		return errors.New("AMQP 1.0 queue declaration is not implemented; see issue 190")
	}
	if err := prov.declarations.DeclareQueue(name); err != nil {
		return err
	}
	prov.tracker.AddQueue(name)
	return nil
}

func (prov *rabbitMQAMQP10Provider) declareStream(name string) error {
	if prov.tracker.StreamExists(name) {
		return nil
	}
	if prov.declarations == nil {
		return errors.New("AMQP 1.0 stream declaration is not implemented; see issue 189")
	}
	if err := prov.declarations.DeclareStream(name); err != nil {
		return err
	}
	prov.tracker.AddStream(name)
	return nil
}
