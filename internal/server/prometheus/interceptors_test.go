// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package prometheus

import (
	"context"
	"errors"
	"testing"

	"github.com/armon/go-metrics/prometheus"
	"github.com/sassoftware/arke/api"
	prometheusmetrics "github.com/sassoftware/arke/internal/metrics/prometheus"
	"google.golang.org/grpc"
)

// mockServerStream implements grpc.ServerStream for testing
type mockServerStream struct {
	grpc.ServerStream
	recvErr error
	sendErr error
}

func (m *mockServerStream) RecvMsg(_ interface{}) error {
	return m.recvErr
}

func (m *mockServerStream) SendMsg(_ interface{}) error {
	return m.sendErr
}

func (m *mockServerStream) Context() context.Context {
	return context.Background()
}

func setupTestSink(t *testing.T) {
	t.Helper()
	if prometheusmetrics.Stats.Sink == nil {
		prometheusmetrics.Stats.Sink = &prometheus.PrometheusSink{}
	}
}

func TestUnaryInterceptor_SuccessWithKnownMethod(t *testing.T) {
	setupTestSink(t)

	info := &grpc.UnaryServerInfo{
		FullMethod: api.Producer_Publish_FullMethodName,
	}

	handler := func(_ context.Context, _ interface{}) (interface{}, error) {
		return "response", nil
	}

	resp, err := UnaryInterceptor(context.Background(), nil, info, handler)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp != "response" {
		t.Errorf("expected 'response', got %v", resp)
	}
}

func TestUnaryInterceptor_ErrorWithKnownMethod(t *testing.T) {
	setupTestSink(t)

	info := &grpc.UnaryServerInfo{
		FullMethod: api.Consumer_Consume_FullMethodName,
	}

	expectedErr := errors.New("handler error")
	handler := func(_ context.Context, _ interface{}) (interface{}, error) {
		return nil, expectedErr
	}

	resp, err := UnaryInterceptor(context.Background(), nil, info, handler)
	if err != expectedErr {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
	if resp != nil {
		t.Errorf("expected nil response, got %v", resp)
	}
}

func TestUnaryInterceptor_UnknownMethodFallback(t *testing.T) {
	setupTestSink(t)

	info := &grpc.UnaryServerInfo{
		FullMethod: "/some.unknown.Service/UnknownMethod",
	}

	handler := func(_ context.Context, _ interface{}) (interface{}, error) {
		return "ok", nil
	}

	resp, err := UnaryInterceptor(context.Background(), nil, info, handler)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp != "ok" {
		t.Errorf("expected 'ok', got %v", resp)
	}
}

func TestUnaryInterceptor_PassesRequestToHandler(t *testing.T) {
	setupTestSink(t)

	info := &grpc.UnaryServerInfo{
		FullMethod: api.Healthz_Check_FullMethodName,
	}

	req := "test-request"
	var receivedReq interface{}
	handler := func(_ context.Context, r interface{}) (interface{}, error) {
		receivedReq = r
		return nil, nil
	}

	_, _ = UnaryInterceptor(context.Background(), req, info, handler)
	if receivedReq != req {
		t.Errorf("expected handler to receive %v, got %v", req, receivedReq)
	}
}

func TestUnaryInterceptor_PassesContextToHandler(t *testing.T) {
	setupTestSink(t)

	info := &grpc.UnaryServerInfo{
		FullMethod: api.Healthz_Check_FullMethodName,
	}

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "ctx-value")
	var receivedCtx context.Context
	handler := func(c context.Context, _ interface{}) (interface{}, error) {
		receivedCtx = c
		return nil, nil
	}

	_, _ = UnaryInterceptor(ctx, nil, info, handler)
	if receivedCtx.Value(ctxKey{}) != "ctx-value" {
		t.Errorf("expected context value to be passed to handler")
	}
}

func TestStreamInterceptor_Success(t *testing.T) {
	setupTestSink(t)

	info := &grpc.StreamServerInfo{
		FullMethod: "/arke.Producer/Connect",
	}

	ss := &mockServerStream{}
	handler := func(_ interface{}, _ grpc.ServerStream) error {
		return nil
	}

	err := StreamInterceptor(nil, ss, info, handler)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestStreamInterceptor_Error(t *testing.T) {
	setupTestSink(t)

	info := &grpc.StreamServerInfo{
		FullMethod: "/arke.Consumer/Consume",
	}

	ss := &mockServerStream{}
	expectedErr := errors.New("stream error")
	handler := func(_ interface{}, _ grpc.ServerStream) error {
		return expectedErr
	}

	err := StreamInterceptor(nil, ss, info, handler)
	if err != expectedErr {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func TestStreamInterceptor_WrapsStream(t *testing.T) {
	setupTestSink(t)

	info := &grpc.StreamServerInfo{
		FullMethod: "/arke.Producer/Connect",
	}

	ss := &mockServerStream{}
	var receivedStream grpc.ServerStream
	handler := func(_ interface{}, stream grpc.ServerStream) error {
		receivedStream = stream
		return nil
	}

	_ = StreamInterceptor(nil, ss, info, handler)
	if receivedStream == ss {
		t.Error("expected handler to receive a wrapped stream, not the original")
	}
	if _, ok := receivedStream.(*wrappedStream); !ok {
		t.Errorf("expected *wrappedStream, got %T", receivedStream)
	}
}

func TestWrappedStream_RecvMsg_Success(t *testing.T) {
	setupTestSink(t)

	ms := &mockServerStream{recvErr: nil}
	ws := &wrappedStream{ServerStream: ms, GRPCMethod: "arke.Producer.Publish"}

	err := ws.RecvMsg(nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestWrappedStream_RecvMsg_Error(t *testing.T) {
	setupTestSink(t)

	expectedErr := errors.New("recv error")
	ms := &mockServerStream{recvErr: expectedErr}
	ws := &wrappedStream{ServerStream: ms, GRPCMethod: "arke.Producer.Publish"}

	err := ws.RecvMsg(nil)
	if err != expectedErr {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func TestWrappedStream_SendMsg_Success(t *testing.T) {
	setupTestSink(t)

	ms := &mockServerStream{sendErr: nil}
	ws := &wrappedStream{ServerStream: ms, GRPCMethod: "arke.Consumer.Consume"}

	err := ws.SendMsg(nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestWrappedStream_SendMsg_Error(t *testing.T) {
	setupTestSink(t)

	expectedErr := errors.New("send error")
	ms := &mockServerStream{sendErr: expectedErr}
	ws := &wrappedStream{ServerStream: ms, GRPCMethod: "arke.Consumer.Consume"}

	err := ws.SendMsg(nil)
	if err != expectedErr {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
}

func TestNewWrappedStream_ReturnsWrappedStream(t *testing.T) {
	ms := &mockServerStream{}
	method := "arke.Producer.Publish"

	ws := newWrappedStream(ms, method)
	wrapped, ok := ws.(*wrappedStream)
	if !ok {
		t.Fatalf("expected *wrappedStream, got %T", ws)
	}
	if wrapped.GRPCMethod != method {
		t.Errorf("expected GRPCMethod %s, got %s", method, wrapped.GRPCMethod)
	}
}

func TestMethodMap_ContainsExpectedMethods(t *testing.T) {
	expectedMethods := []string{
		api.Producer_Publish_FullMethodName,
		api.Producer_Connect_FullMethodName,
		api.Producer_Disconnect_FullMethodName,
		api.Producer_PublishOne_FullMethodName,
		api.Consumer_Consume_FullMethodName,
		api.Consumer_Connect_FullMethodName,
		api.Consumer_Disconnect_FullMethodName,
		api.Consumer_SourceStats_FullMethodName,
		api.Healthz_Check_FullMethodName,
		"/grpc.health.v1.Health/Check",
	}

	for _, method := range expectedMethods {
		if _, ok := methodMap[method]; !ok {
			t.Errorf("methodMap missing entry for %s", method)
		}
	}
}

func TestMethodMap_FriendlyNameFormat(t *testing.T) {
	for grpcMethod, friendlyName := range methodMap {
		if len(friendlyName) == 0 {
			t.Errorf("methodMap entry for %s has empty friendly name", grpcMethod)
		}
		// friendly names should not start with "/"
		if friendlyName[0] == '/' {
			t.Errorf("methodMap entry for %s has friendly name starting with '/': %s", grpcMethod, friendlyName)
		}
	}
}

func TestStreamInterceptor_MethodNameFormatting(t *testing.T) {
	setupTestSink(t)

	info := &grpc.StreamServerInfo{
		FullMethod: "/some.Service/Method",
	}

	ss := &mockServerStream{}
	var receivedStream grpc.ServerStream
	handler := func(_ interface{}, stream grpc.ServerStream) error {
		receivedStream = stream
		return nil
	}

	_ = StreamInterceptor(nil, ss, info, handler)

	ws, ok := receivedStream.(*wrappedStream)
	if !ok {
		t.Fatalf("expected *wrappedStream, got %T", receivedStream)
	}

	expectedMethod := "some.Service.Method"
	if ws.GRPCMethod != expectedMethod {
		t.Errorf("expected GRPCMethod %s, got %s", expectedMethod, ws.GRPCMethod)
	}
}

func TestUnaryInterceptor_AllKnownMethods(t *testing.T) {
	setupTestSink(t)

	for grpcMethod, friendlyName := range methodMap {
		t.Run(friendlyName, func(t *testing.T) {
			info := &grpc.UnaryServerInfo{FullMethod: grpcMethod}
			handler := func(_ context.Context, _ interface{}) (interface{}, error) {
				return nil, nil
			}
			_, err := UnaryInterceptor(context.Background(), nil, info, handler)
			if err != nil {
				t.Errorf("unexpected error for method %s: %v", grpcMethod, err)
			}
		})
	}
}
