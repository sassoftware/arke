// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package prometheus

import (
	"context"
	"strings"
	"time"

	"github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/metrics"
	"github.com/sassoftware/arke/internal/metrics/prometheus"
	"github.com/sassoftware/arke/internal/util"
	"google.golang.org/grpc"
)

const statusError = "error"

// methodMap maps gRPC full method names to a more friendly format
// that can be used as a Prometheus label value.
// This avoids allocations that would occur with string manipulation
// on every gRPC request.
// The keys are the gRPC full method names and the values are the
// corresponding friendly names.
// This map should be updated whenever new gRPC methods are added
// to the arke service.
var methodMap = map[string]string{
	api.Producer_Publish_FullMethodName:    "arke.Producer.Publish",
	api.Producer_Connect_FullMethodName:    "arke.Producer.Connect",
	api.Producer_Disconnect_FullMethodName: "arke.Producer.Disconnect",
	api.Producer_PublishOne_FullMethodName: "arke.Producer.PublishOne",

	api.Consumer_Consume_FullMethodName:     "arke.Consumer.Consume",
	api.Consumer_Connect_FullMethodName:     "arke.Consumer.Connect",
	api.Consumer_Disconnect_FullMethodName:  "arke.Consumer.Disconnect",
	api.Consumer_SourceStats_FullMethodName: "arke.Consumer.SourceStats",

	api.Healthz_Check_FullMethodName: "arke.Healthz.Check",

	"/grpc.health.v1.Health/Check": "grpc.health.v1.Health.Check",
}

// UnaryInterceptor unary grpc interceptor
func UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// allocations_test.go shows this is the fastest way to do this and without
	// allocations, though the tests themselves do not actually call this method.
	fullMethod, ok := methodMap[info.FullMethod]
	if !ok {
		// fallback to string manipulation if not found in map
		// this should be rare since most calls should be in the map
		// but this will handle any future methods that are added
		// without needing to update the map
		// This fallback is more expensive, so we prefer the map lookup
		util.Logger.Debugf("Method %s not found in methodMap, using string manipulation", info.FullMethod)
		fullMethod = strings.TrimPrefix(info.FullMethod, "/")
		fullMethod = strings.ReplaceAll(fullMethod, "/", ".")
	}

	labelset := metrics.NewLabelSet()
	labelset.AddLabel("method", fullMethod)
	status := "ok"

	start := time.Now()

	m, err := handler(ctx, req)
	if err != nil {
		util.Logger.Debugf("RPC failed with error %s", err.Error())
		status = statusError
	}

	elapsed := float32(time.Since(start).Nanoseconds()) / float32(time.Millisecond)

	labelset.AddLabel("status", status)
	prometheus.Stats.Sink.AddSampleWithLabels(metrics.RequestElapsedSummary, elapsed, labelset.Labels)
	prometheus.Stats.Sink.IncrCounterWithLabels(metrics.RequestTotalCounter, 1, labelset.Labels)
	return m, err
}

type wrappedStream struct {
	grpc.ServerStream
	GRPCMethod string
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	labelset := metrics.NewLabelSet()
	labelset.AddLabel("method", w.GRPCMethod)
	status := "ok"

	err := w.ServerStream.RecvMsg(m)
	if err != nil {
		status = statusError
	}

	labelset.AddLabel("status", status)
	prometheus.Stats.Sink.IncrCounterWithLabels(metrics.RecvMsgCounter, 1, labelset.Labels)

	return err
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	labelset := metrics.NewLabelSet()
	labelset.AddLabel("method", w.GRPCMethod)
	status := "ok"

	err := w.ServerStream.SendMsg(m)
	if err != nil {
		status = statusError
	}

	labelset.AddLabel("status", status)
	prometheus.Stats.Sink.IncrCounterWithLabels(metrics.SendMsgCounter, 1, labelset.Labels)

	return err
}

func newWrappedStream(s grpc.ServerStream, method string) grpc.ServerStream {
	return &wrappedStream{s, method}
}

// StreamInterceptor stream grpc interceptor
func StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	fullMethod := strings.TrimPrefix(info.FullMethod, "/")
	fullMethod = strings.ReplaceAll(fullMethod, "/", ".")

	labelset := metrics.NewLabelSet()
	labelset.AddLabel("method", fullMethod)
	status := "ok"

	// increment this before we call the long running handler
	prometheus.Stats.Sink.IncrCounterWithLabels(metrics.RequestTotalCounter, 1, labelset.Labels)

	start := time.Now()

	err := handler(srv, newWrappedStream(ss, fullMethod))
	if err != nil {
		util.Logger.Debugf("RPC failed with error %s", err.Error())
		status = statusError
	}

	elapsed := float32(time.Since(start).Nanoseconds()) / float32(time.Millisecond)
	labelset.AddLabel("status", status)
	prometheus.Stats.Sink.AddSampleWithLabels(metrics.RequestElapsedSummary, elapsed, labelset.Labels)
	prometheus.Stats.Sink.IncrCounterWithLabels(metrics.RequestTotalCounter, 1, labelset.Labels)

	return err
}
