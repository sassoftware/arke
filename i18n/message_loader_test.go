// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package i18n

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type getLocaleFileTestCase struct {
	filePrefix    string
	locale        string
	expectedFiles []string
}

const testValue2 = "value2"

func TestGetLocaleFiles(t *testing.T) {
	filePrefix := messageFilePrefix
	cases := []getLocaleFileTestCase{
		{
			filePrefix: filePrefix,
			locale:     "en-US",
			expectedFiles: []string{
				fmt.Sprintf("resources/%s_en_US.properties", filePrefix),
				fmt.Sprintf("resources/%s_en.properties", filePrefix),
				fmt.Sprintf("resources/%s.properties", filePrefix),
			},
		},
		{
			filePrefix: filePrefix,
			locale:     "zh-Hans",
			expectedFiles: []string{
				fmt.Sprintf("resources/%s_zh-Hans.properties", filePrefix),
				fmt.Sprintf("resources/%s.properties", filePrefix),
			},
		},
		{
			filePrefix: filePrefix,
			locale:     "zh-SG",
			expectedFiles: []string{
				fmt.Sprintf("resources/%s_zh-Hans.properties", filePrefix),
				fmt.Sprintf("resources/%s.properties", filePrefix),
			},
		},
		{
			filePrefix: filePrefix,
			locale:     "zh-Hant",
			expectedFiles: []string{
				fmt.Sprintf("resources/%s_zh-Hant.properties", filePrefix),
				fmt.Sprintf("resources/%s.properties", filePrefix),
			},
		},
		{
			filePrefix: filePrefix,
			locale:     "zh-MO",
			expectedFiles: []string{
				fmt.Sprintf("resources/%s_zh-Hant.properties", filePrefix),
				fmt.Sprintf("resources/%s.properties", filePrefix),
			},
		},
		{
			filePrefix: filePrefix,
			locale:     "zh-TW",
			expectedFiles: []string{
				fmt.Sprintf("resources/%s_zh-Hant.properties", filePrefix),
				fmt.Sprintf("resources/%s.properties", filePrefix),
			},
		},
		{
			filePrefix: filePrefix,
			locale:     "de-DE",
			expectedFiles: []string{
				fmt.Sprintf("resources/%s_de_DE.properties", filePrefix),
				fmt.Sprintf("resources/%s_de.properties", filePrefix),
				fmt.Sprintf("resources/%s.properties", filePrefix),
			},
		},
	}
	for _, c := range cases {
		t.Run(c.locale, func(t *testing.T) {
			gotFiles := getLocaleFileNames(c.filePrefix, c.locale)
			assert.Equal(t, c.expectedFiles, gotFiles)
		})
	}
}

func TestNewPropertiesBundle(t *testing.T) { //nolint:gocognit
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
		checks  func(t *testing.T, pb *propertiesBundle)
	}{
		{
			name:    "nil data",
			data:    nil,
			wantErr: false,
			checks: func(t *testing.T, pb *propertiesBundle) {
				if pb == nil {
					t.Fatal("expected non-nil bundle")
				}
				if len(pb.messages) != 0 {
					t.Errorf("expected empty messages, got %d", len(pb.messages))
				}
			},
		},
		{
			name:    "empty data",
			data:    []byte(""),
			wantErr: false,
			checks: func(t *testing.T, pb *propertiesBundle) {
				if len(pb.messages) != 0 {
					t.Errorf("expected empty messages, got %d", len(pb.messages))
				}
			},
		},
		{
			name:    "single property",
			data:    []byte("key1=value1"),
			wantErr: false,
			checks: func(t *testing.T, pb *propertiesBundle) {
				if pb.messages["key1"] != "value1" {
					t.Errorf("expected 'value1', got '%s'", pb.messages["key1"])
				}
			},
		},
		{
			name:    "multiple properties",
			data:    []byte("key1=value1\nkey2=value2\nkey3=value3"),
			wantErr: false,
			checks: func(t *testing.T, pb *propertiesBundle) {
				if len(pb.messages) != 3 {
					t.Errorf("expected 3 messages, got %d", len(pb.messages))
				}
				if pb.messages["key2"] != testValue2 {
					t.Errorf("expected 'value2', got '%s'", pb.messages["key2"])
				}
			},
		},
		{
			name:    "skip comments",
			data:    []byte("# This is a comment\nkey1=value1\n# Another comment\nkey2=value2"),
			wantErr: false,
			checks: func(t *testing.T, pb *propertiesBundle) {
				if len(pb.messages) != 2 {
					t.Errorf("expected 2 messages, got %d", len(pb.messages))
				}
			},
		},
		{
			name:    "skip empty lines",
			data:    []byte("key1=value1\n\n\nkey2=value2"),
			wantErr: false,
			checks: func(t *testing.T, pb *propertiesBundle) {
				if len(pb.messages) != 2 {
					t.Errorf("expected 2 messages, got %d", len(pb.messages))
				}
			},
		},
		{
			name:    "whitespace trimming",
			data:    []byte("  key1  =  value1  \n  key2=value2  "),
			wantErr: false,
			checks: func(t *testing.T, pb *propertiesBundle) {
				if pb.messages["key1"] != "value1" {
					t.Errorf("expected 'value1', got '%s'", pb.messages["key1"])
				}
				if pb.messages["key2"] != testValue2 {
					t.Errorf("expected 'value2', got '%s'", pb.messages["key2"])
				}
			},
		},
		{
			name:    "value with equals sign",
			data:    []byte("key1=value=with=equals"),
			wantErr: false,
			checks: func(t *testing.T, pb *propertiesBundle) {
				if pb.messages["key1"] != "value=with=equals" {
					t.Errorf("expected 'value=with=equals', got '%s'", pb.messages["key1"])
				}
			},
		},
		{
			name:    "missing value",
			data:    []byte("key1=\nkey2=value2"),
			wantErr: false,
			checks: func(t *testing.T, pb *propertiesBundle) {
				if len(pb.messages) != 2 {
					t.Errorf("expected 2 messages, got %d", len(pb.messages))
				}
				if pb.messages["key1"] != "" {
					t.Errorf("expected empty value, got '%s'", pb.messages["key1"])
				}
			},
		},
		{
			name:    "no equals sign",
			data:    []byte("key1\nkey2=value2"),
			wantErr: false,
			checks: func(t *testing.T, pb *propertiesBundle) {
				if len(pb.messages) != 1 {
					t.Errorf("expected 1 message, got %d", len(pb.messages))
				}
				if pb.messages["key2"] != testValue2 {
					t.Errorf("expected 'value2', got '%s'", pb.messages["key2"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bundleSyncOnce = sync.Once{}
			bundle = nil
			bundleErr = nil
			pb, err := newPropertiesBundle(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("newPropertiesBundle() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.checks != nil {
				tt.checks(t, pb)
			}
		})
	}
}

func TestT(t *testing.T) {
	bundleSyncOnce = sync.Once{}
	oldBundle := bundle
	defer func() {
		bundle = oldBundle
	}()
	bundle = &propertiesBundle{
		messages: map[string]string{
			"no_params":       "This is a test message.",
			"one_param":       "Hello, {0}!",
			"repeated_param":  "Value: {0}, Again: {0}",
			"multiple_params": "First: {0}, Second: {1}, Third: {2}, First again: {0}",
		},
	}
	tests := []struct {
		name      string
		messageID string
		args      []interface{}
		want      string
	}{
		{
			name:      "no parameters",
			messageID: "no_params",
			args:      nil,
			want:      "This is a test message.",
		},
		{
			name:      "one parameter",
			messageID: "one_param",
			args:      []interface{}{"World"},
			want:      "Hello, World!",
		},
		{
			name:      "repeated parameter",
			messageID: "repeated_param",
			args:      []interface{}{"42"},
			want:      "Value: 42, Again: 42",
		},
		{
			name:      "multiple parameters",
			messageID: "multiple_params",
			args:      []interface{}{"A", "B", "C"},
			want:      "First: A, Second: B, Third: C, First again: A",
		},
		{
			name:      "missing message ID",
			messageID: "missing_id",
			args:      []interface{}{"X"},
			want:      "missing_id",
		},
		{
			name:      "extra arguments",
			messageID: "multiple_params",
			args:      []interface{}{"A", "B", "C", "D"},
			want:      "First: A, Second: B, Third: C, First again: A",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := T(tt.messageID, tt.args...)
			assert.Equal(t, tt.want, got)
		})
	}
}
