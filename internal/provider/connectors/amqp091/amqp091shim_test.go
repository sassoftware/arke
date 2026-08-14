// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_Amqp091Message_Nack_AcquiredCount(t *testing.T) {
	tests := []struct {
		name             string
		initialRequeue   bool
		headers          amqp091Table
		expectedRequeue  bool
		maxAcquiredCount int32
	}{
		{
			name:            "requeue false without headers remains false",
			initialRequeue:  false,
			headers:         nil,
			expectedRequeue: false,
		},
		{
			name:            "requeue true without headers remains true",
			initialRequeue:  true,
			headers:         nil,
			expectedRequeue: true,
		},
		{
			name:           "requeue true with int32 acquired count below limit remains true",
			initialRequeue: true,
			headers: amqp091Table{
				"x-acquired-count": int32(5),
			},
			expectedRequeue: true,
		},
		{
			name:           "requeue true with int32 acquired count equal to limit remains true",
			initialRequeue: true,
			headers: amqp091Table{
				"x-acquired-count": int32(20),
			},
			expectedRequeue: true,
		},
		{
			name:           "requeue true with int32 acquired count above limit becomes false",
			initialRequeue: true,
			headers: amqp091Table{
				"x-acquired-count": int32(21),
			},
			expectedRequeue: false,
		},
		{
			name:           "requeue true with int64 acquired count above limit becomes false",
			initialRequeue: true,
			headers: amqp091Table{
				"x-acquired-count": int64(25),
			},
			expectedRequeue: false,
		},
		{
			name:           "requeue true with int64 acquired count below limit remains true",
			initialRequeue: true,
			headers: amqp091Table{
				"x-acquired-count": int64(10),
			},
			expectedRequeue: true,
		},
		{
			name:           "x-acquired-count is a string(invalid) should be true",
			initialRequeue: true,
			headers: amqp091Table{
				"x-acquired-count": "25",
			},
			expectedRequeue: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mMock := &mock.Mock{}
			mMock.On("Nack", false, tt.expectedRequeue).Return(nil)

			msg := &amqp091Message{
				Headers:  tt.headers,
				delivery: mMock,
			}

			err := msg.Nack(tt.initialRequeue)
			assert.NoError(t, err)
			mMock.AssertExpectations(t)
		})
	}
}
