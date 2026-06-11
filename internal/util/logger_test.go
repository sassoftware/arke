// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/sassoftware/arke/i18n"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArkeLogger_InfoT(t *testing.T) {
	tests := []struct {
		name        string
		messageID   string
		args        []interface{}
		expectedMsg string
	}{
		{
			name:        "multiple params",
			messageID:   i18n.Binding,
			args:        []interface{}{"queue-name", "key-name", "exchange-name"},
			expectedMsg: "Binding to Queue/Key/Exchange: queue-name/key-name/exchange-name",
		},
		{
			name:        "params given but not needed",
			messageID:   i18n.RateLimiterInitialized2,
			args:        []interface{}{"arg1", "arg2"},
			expectedMsg: "Rate limiter has been initialized",
		},
	}

	for _, tt := range tests {
		t.Setenv(EnvLogLevel, "info")
		t.Run(tt.name, func(t *testing.T) {
			logger, cleanup := GetTestLoggerWithCleanup()
			defer cleanup()

			ResetLogger()

			logger.Info(tt.messageID, tt.args...)
			logOutput := string(logger.GetOutput())
			t.Logf("Log Output: %s", logOutput)
			assert.Contains(t, logOutput, tt.expectedMsg)
		})
	}
}

func TestArkeLogger_Levels(t *testing.T) {
	t.Setenv(EnvLogLevel, "trace")

	logger, cleanup := GetTestLoggerWithCleanup()
	defer cleanup()

	logger.Trace("trace_message")
	logger.Tracef("tracef_message: %s", "formatted")
	logger.Debug("debug_message")
	logger.Debugf("debugf_message: %s", "formatted")
	logger.Info("info_message")
	logger.Warn("warn_message")
	logger.Error("error_message")

	logOutput := string(logger.GetOutput())
	t.Logf("Log Output: %s", logOutput)
	assert.Contains(t, logOutput, "trace_message")
	assert.Contains(t, logOutput, "tracef_message: formatted")
	assert.Contains(t, logOutput, "debug_message")
	assert.Contains(t, logOutput, "debugf_message: formatted")
	assert.Contains(t, logOutput, "info_message")
	assert.Contains(t, logOutput, "warn_message")
	assert.Contains(t, logOutput, "error_message")
}

func TestArkeLogger_fields(t *testing.T) {
	t.Setenv(EnvLogFormat, "json")

	logger, cleanup := GetTestLoggerWithCleanup()
	defer cleanup()

	logger.Info("hi")
	expectedLogTime := time.Now()

	logOutput := string(logger.GetOutput())
	t.Logf("Log Output: %s", logOutput)

	data := map[string]interface{}{}
	require.NoError(t, json.Unmarshal([]byte(logOutput), &data))
	assert.Equal(t, "info", data["level"])
	assert.NotNil(t, data["source"])
	assert.Equal(t, 1.0, data["version"])

	if data["timeStamp"] != nil {
		actualLogTime, err := time.Parse(time.RFC3339Nano, data["timeStamp"].(string))
		require.NoError(t, err)
		// ci fails if we don't convert to UTC times
		// compare times down to the 100ms (12.3999 == 12.3000)
		assert.Equal(t, actualLogTime.UTC().Truncate(100*time.Millisecond), expectedLogTime.UTC().Truncate(100*time.Millisecond))
	} else {
		assert.NotNil(t, data["timeStamp"], "missing timeStamp field")
	}

	// make sure zerolog.CallerSkipFrameCount is set properly
	logMatch := regexp.MustCompile(`util/logger_test.go:\d+`)
	assert.Regexp(t, logMatch, data["caller"])
	assert.Equal(t, "hi", data["message"])
}
