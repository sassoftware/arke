// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"testing"
	"time"

	"github.com/go-openapi/testify/v2/assert"
)

func Test_SleepRandomReconnect(t *testing.T) {
	start := time.Now()

	SleepRandomReconnect()

	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
	assert.LessOrEqual(t, elapsed, time.Duration(ReconnectDelay+100)*time.Millisecond)
}
