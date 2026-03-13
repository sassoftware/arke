// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package tracing

import (
	"context"
	"os"
	"strconv"
	"sync"

	"github.com/sassoftware/arke/i18n"
	"github.com/sassoftware/arke/internal/util"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var initResourcesOnce sync.Once
var tracingEnabled bool

const (
	TraceHeaderName   = "X-B3-TraceId"
	SpanHeaderName    = "X-B3-SpanId"
	HeaderTraceParent = "traceparent"
	HeaderTraceState  = "tracestate"

	EnvOtelSdkDisabled          = "OTEL_SDK_DISABLED"
	EnvTelemetryExporter        = "ARKE_TELEMETRY_EXPORTER"
	EnvOtelExporterOtlpEndpoint = "OTEL_EXPORTER_OTLP_ENDPOINT"
)

func getTelemetryCollectorAddress() string {
	if addr := os.Getenv(EnvOtelExporterOtlpEndpoint); addr != "" {
		return addr
	}

	return "localhost:4317"
}

// Unless OTEL_SDK_DISABLED is explicitly set to true (disabled=true), telemetry is enabled.
// https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/#general-sdk-configuration
func getTelemetryEnabled() bool {
	tracingEnabled = true
	if e := os.Getenv(EnvOtelSdkDisabled); e != "" {
		disabled, err := strconv.ParseBool(e)
		if err != nil {
			tracingEnabled = true
		} else {
			tracingEnabled = !disabled
		}
	}
	return tracingEnabled
}

func initResource() *sdkresource.Resource {
	var resource *sdkresource.Resource
	initResourcesOnce.Do(func() {
		resource, _ = sdkresource.New(
			context.Background(),
			sdkresource.WithTelemetrySDK(),
			sdkresource.WithOS(),
			sdkresource.WithProcess(),
			sdkresource.WithContainer(),
			sdkresource.WithHost(),
			sdkresource.WithAttributes(
				semconv.ServiceNameKey.String("arke"),
				attribute.String("application", "arke"),
			),
		)
	})
	return resource
}

func Enabled() bool {
	return tracingEnabled
}

func InitTracerProvider() (*sdktrace.TracerProvider, error) {
	if getTelemetryEnabled() {
		ctx := context.Background()

		var exporter sdktrace.SpanExporter
		var err error
		if os.Getenv(EnvTelemetryExporter) == "stdout" {
			util.Logger.Debug("Initializing OpenTelemetry exporter to stdout")
			exporter, err = stdouttrace.New()
		} else {
			util.Logger.Debug("Initializing OpenTelemetry exporter to grpc")
			otelAddr := getTelemetryCollectorAddress()
			exporter, err = otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(otelAddr), otlptracegrpc.WithInsecure())
		}
		if err != nil {
			util.Logger.Info(i18n.FailedInitTelemetryExporter, err.Error())
			return nil, err
		}

		bsp := sdktrace.NewBatchSpanProcessor(exporter)

		tp := sdktrace.NewTracerProvider(
			sdktrace.WithSpanProcessor(bsp),
			sdktrace.WithResource(initResource()),
		)

		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagation.TraceContext{})
		return tp, nil
	}
	otel.SetTracerProvider(noop.NewTracerProvider())
	return nil, nil
}

func SpanFromHeaders(ctx context.Context, headers map[string]string, spanName string, kind trace.SpanKind) (context.Context, trace.Span) {
	tracer := otel.Tracer("arke")
	carrier := propagation.MapCarrier(headers)
	ctx = otel.GetTextMapPropagator().Extract(
		ctx, carrier)

	return tracer.Start(ctx, spanName, trace.WithSpanKind(kind))
}
