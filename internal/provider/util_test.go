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
