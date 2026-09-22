// Copyright (c) 2026, SAS Institute Inc., Cary, NC, USA. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp10

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SupportedSourceOptions(t *testing.T) {
	prov := NewAMQP10Provider()
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
