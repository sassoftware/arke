// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"
)

func TestGetTelemetryEnabled(t *testing.T) {
	// A true value for OTEL_SDK_DISABLED is the only way to disable the SDK according to the docs,
	// even though this env var is not used in the Go SDK
	// https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/#general-sdk-configuration

	//    OTEL_SDK_DISABLED | true  | false |  ''  | other
	// getTelementryEnabled | false | true  | true | true
	t.Setenv(EnvOtelSdkDisabled, "true")
	enabled := getTelemetryEnabled()
	assert.False(t, enabled)

	t.Setenv(EnvOtelSdkDisabled, "false")
	enabled = getTelemetryEnabled()
	assert.True(t, enabled)

	t.Setenv(EnvOtelSdkDisabled, "")
	enabled = getTelemetryEnabled()
	assert.True(t, enabled)

	t.Setenv(EnvOtelSdkDisabled, "other")
	enabled = getTelemetryEnabled()
	assert.True(t, enabled)
}

func Test_InitTracerProvider_disabled(t *testing.T) {
	t.Setenv(EnvOtelSdkDisabled, "true")
	tp, err := InitTracerProvider()
	assert.Nil(t, tp)
	assert.Nil(t, err)
}

func Test_InitTracerProvider_enabled(t *testing.T) {
	t.Setenv(EnvOtelSdkDisabled, "false")
	tp, err := InitTracerProvider()
	assert.NotNil(t, tp)
	assert.Nil(t, err)
}

func Test_getTelemetryCollectorAddress_unset(t *testing.T) {
	t.Setenv(EnvOtelExporterOtlpEndpoint, "")
	addr := getTelemetryCollectorAddress()
	assert.Equal(t, "localhost:4317", addr)
}

func Test_getTelemetryCollectorAddress_set(t *testing.T) {
	t.Setenv(EnvOtelExporterOtlpEndpoint, "localhost:12345")
	addr := getTelemetryCollectorAddress()
	assert.Equal(t, "localhost:12345", addr)
}

// Test_initResource_returnsSamePointer guards against a regression where
// initResource declared the *sdkresource.Resource locally; the sync.Once body
// would only run on the first call, leaving subsequent calls to return a
// freshly-declared nil. With the package-level tracingResource, every call
// must return the same non-nil pointer.
func Test_initResource_returnsSamePointer(t *testing.T) {
	r1 := initResource()
	r2 := initResource()
	assert.NotNil(t, r1)
	assert.Same(t, r1, r2)
}

func Test_SpanFromHeaders(t *testing.T) {
	t.Setenv(EnvOtelSdkDisabled, "false")
	tp, err := InitTracerProvider()
	assert.NotNil(t, tp)
	assert.Nil(t, err)

	ctx := context.Background()
	headers := make(map[string]string)
	headers[HeaderTraceParent] = "00-80e1afed08e019fc1110464cfa66635c-7a085853722dc6d2-01"
	spanName := "testSpan"
	kind := trace.SpanKindConsumer
	var span trace.Span
	_, span = SpanFromHeaders(ctx, headers, spanName, kind)
	defer span.End()

	assert.True(t, span.IsRecording())
}
