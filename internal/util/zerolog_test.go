// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"os"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testLogLevelInfo = "info"
const testLogLevelInvalid = "invalid"

func TestLoadAndParseLevel(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected zerolog.Level
	}{
		{
			name:     "valid trace level (mixed case)",
			envValue: "TRAcE",
			expected: zerolog.TraceLevel,
		},
		{
			name:     "valid trace level",
			envValue: "trace",
			expected: zerolog.TraceLevel,
		},
		{
			name:     "valid debug level",
			envValue: "debug",
			expected: zerolog.DebugLevel,
		},
		{
			name:     "valid info level",
			envValue: testLogLevelInfo,
			expected: zerolog.InfoLevel,
		},
		{
			name:     "valid warn level",
			envValue: "warn",
			expected: zerolog.WarnLevel,
		},
		{
			name:     "valid error level",
			envValue: "error",
			expected: zerolog.ErrorLevel,
		},
		{
			name:     "invalid level defaults to info",
			envValue: testLogLevelInvalid,
			expected: zerolog.InfoLevel,
		},
		{
			name:     "empty env defaults to info",
			envValue: "",
			expected: zerolog.InfoLevel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(EnvLogLevel, tt.envValue)
			gotLevel, err := loadAndParseLevel()
			if tt.envValue == testLogLevelInvalid {
				assert.NotNil(t, err, "expected error for invalid log level")
			} else {
				assert.Nil(t, err, "did not expect error for valid log level")
			}
			assert.Equal(t, tt.expected, gotLevel)
		})
	}
}

func TestCreateFileLogger(t *testing.T) {
	tests := []struct {
		name      string
		logLevel  string
		logFormat string
		setupEnv  func()
	}{
		{
			name:      "creates logger with default settings",
			logLevel:  testLogLevelInfo,
			logFormat: "",
			setupEnv: func() {
				t.Setenv(EnvLogLevel, testLogLevelInfo)
				t.Setenv(EnvLogFormat, "")
			},
		},
		{
			name:      "creates logger with debug level",
			logLevel:  "debug",
			logFormat: "",
			setupEnv: func() {
				t.Setenv(EnvLogLevel, "debug")
			},
		},
		{
			name:      "creates logger with term format",
			logLevel:  testLogLevelInfo,
			logFormat: "term",
			setupEnv: func() {
				t.Setenv(EnvLogLevel, testLogLevelInfo)
				t.Setenv(EnvLogFormat, "term")
			},
		},
		{
			name:      "creates logger with invalid level defaults to info",
			logLevel:  testLogLevelInvalid,
			logFormat: "",
			setupEnv: func() {
				t.Setenv(EnvLogLevel, testLogLevelInvalid)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup
			tt.setupEnv()
			zloggerSyncOnce = sync.Once{}
			zlogger = nil
			oldDefaultLogWriter := LogOutputFile
			defer func() {
				LogOutputFile = oldDefaultLogWriter
			}()
			r, w, _ := os.Pipe()
			LogOutputFile = w

			// test
			l := createFileLogger(LogOutputFile)
			expLevel, err := zerolog.ParseLevel(tt.logLevel)
			if err != nil {
				expLevel = zerolog.InfoLevel
			}
			assert.Equal(t, expLevel, l.GetLevel(), "logger level should match expected level %s", tt.logLevel)

			l.Info().Msg("Test log message")
			err = w.Close()
			require.Nil(t, err)
			loggerOutput := make([]byte, 1024)
			bytesRead, _ := r.Read(loggerOutput)
			assert.Contains(t, string(loggerOutput[:bytesRead]), "Test log message")
		})
	}
}
