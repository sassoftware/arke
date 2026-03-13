// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/sassoftware/arke/internal/util"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	ArkeAddress = "localhost:50051"
)

var kacp = keepalive.ClientParameters{
	Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
	Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: true,             // send pings even without active streams
}

// ConnectToArke connects to the Arke gRPC server specified by ArkeAddress.
// The connection is checked for nil before returning.
func ConnectToArke(withTLS bool) (*grpc.ClientConn, error) {
	var creds credentials.TransportCredentials
	if withTLS {
		creds = credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec
	} else {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(
		ArkeAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kacp),
		grpc.WithTransportCredentials(creds),
	)
	if err == nil && conn == nil {
		return nil, fmt.Errorf("got a nil connection")
	}
	util.Logger.Debugf("Connected to Arke at %s with TLS=%v", ArkeAddress, withTLS)
	return conn, err
}

var initResourcesOnce sync.Once

func InitResource(serviceNameKey string) *sdkresource.Resource {
	var resource *sdkresource.Resource
	initResourcesOnce.Do(func() {
		extraResources, _ := sdkresource.New(
			context.Background(),
			sdkresource.WithOS(),
			sdkresource.WithProcess(),
			sdkresource.WithContainer(),
			sdkresource.WithHost(),
			sdkresource.WithAttributes(
				semconv.ServiceNameKey.String(serviceNameKey),
			),
		)
		resource, _ = sdkresource.Merge(
			sdkresource.Default(),
			extraResources,
		)
	})
	return resource
}

func InitTracerProvider(serviceKeyName string) (*sdktrace.TracerProvider, error) {
	ctx := context.Background()

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	bsp := sdktrace.NewBatchSpanProcessor(exporter)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(bsp),
		sdktrace.WithResource(InitResource(serviceKeyName)),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, nil
}
