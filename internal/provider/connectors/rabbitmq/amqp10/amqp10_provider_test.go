// Copyright (c) 2026, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type declarationClientMock struct {
	exchanges   int
	queues      int
	streams     int
	exchangeErr error
	queueErr    error
	streamErr   error
}

func (m *declarationClientMock) DeclareExchange(string) error {
	m.exchanges++
	return m.exchangeErr
}

func (m *declarationClientMock) DeclareQueue(string) error {
	m.queues++
	return m.queueErr
}

func (m *declarationClientMock) DeclareStream(string) error {
	m.streams++
	return m.streamErr
}

func Test_SupportedSourceOptions(t *testing.T) {
	prov := NewRabbitMQAMQP10Provider()
	opts := prov.SupportedSourceOptions()
	assert.NotNil(t, opts)
	expected := map[string]bool{
		"MessageTTL":        true,
		"DeadLetterAddress": true,
		"DeadLetterSubject": true,
		"Expires":           true,
		"Offset":            true,
		"ConsumerGroup":     true,
	}

	assert.Equal(t, expected, opts)
}

func Test_SupportedStreamSourceOptions(t *testing.T) {
	assert.NotNil(t, supportedStreamSourceOptions)
	expected := map[string]bool{
		"Offset":        true,
		"MessageTTL":    true,
		"ConsumerGroup": true,
	}

	assert.Equal(t, expected, supportedStreamSourceOptions)
}

func TestDeclarationsTrackSuccessfulEntitiesAndSkipKnownNames(t *testing.T) {
	client := &declarationClientMock{}
	prov := newRabbitMQAMQP10Provider(client)

	assert.NoError(t, prov.declareExchange("shared"))
	assert.NoError(t, prov.declareExchange("shared"))
	assert.NoError(t, prov.declareQueue("shared"))
	assert.NoError(t, prov.declareQueue("shared"))
	assert.NoError(t, prov.declareStream("shared"))
	assert.NoError(t, prov.declareStream("shared"))

	assert.Equal(t, 1, client.exchanges)
	assert.Equal(t, 1, client.queues)
	assert.Equal(t, 1, client.streams)
}

func TestDeclarationsDoNotTrackFailures(t *testing.T) {
	client := &declarationClientMock{
		exchangeErr: errors.New("exchange failed"),
		queueErr:    errors.New("queue failed"),
		streamErr:   errors.New("stream failed"),
	}
	prov := newRabbitMQAMQP10Provider(client)

	assert.EqualError(t, prov.declareExchange("exchange"), "exchange failed")
	assert.EqualError(t, prov.declareQueue("queue"), "queue failed")
	assert.EqualError(t, prov.declareStream("stream"), "stream failed")
	assert.False(t, prov.tracker.ExchangeExists("exchange"))
	assert.False(t, prov.tracker.QueueExists("queue"))
	assert.False(t, prov.tracker.StreamExists("stream"))
	assert.Equal(t, 1, client.exchanges)
	assert.Equal(t, 1, client.queues)
	assert.Equal(t, 1, client.streams)
}
