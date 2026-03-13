// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package messagefunctions

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	pb "github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/util/tracing"
	cfg "github.com/sassoftware/arke/test/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func connectConfig(clientName string) *pb.ConnectionConfiguration {
	connConfig := cfg.ConnectionConfigurationFromEnv()

	providerTLS := strings.ToLower(os.Getenv("ARKE_PROVIDER_TLS"))

	switch providerTLS {
	case "sendca":
		cacert, err := os.ReadFile("certs/testca/ca_certificate.pem")
		if err != nil {
			log.Fatalf("Error reading provider CA cert: %v", err)
		}
		connConfig.Tls = true
		connConfig.CaCertificate = cacert
	case "true":
		connConfig.Tls = true
	}

	connConfig.ClientName = clientName

	return &connConfig
}

func ProduceSendMessages(ctx context.Context, c pb.ProducerClient, cnt int, message *pb.Message, clientName string) error {
	connConfig := pb.ConnectionConfiguration{}
	switch clientName {
	case "simple_producer":
		connConfig = cfg.ConnectionConfigurationFromEnv()
		connConfig.ClientName = "simple_producer"
	default:
		connConfig = *connectConfig(clientName)
	}

	defer func() {
		_, _ = c.Disconnect(ctx, &pb.Empty{})
	}()

	authResp, err := c.Connect(ctx, &connConfig)

	if err != nil {
		return err
	}
	if !authResp.GetSuccess() {
		return errors.New(authResp.GetError().GetMessage())
	}
	// fmt.Printf("Publisher connected - connConfig: %+v\n", &connConfig)
	// fmt.Printf("Publisher connected - authResp: %+v\n", authResp)

	stream, err := c.Publish(ctx)
	if err != nil {
		// fmt.Printf("got an err on publish(): %v", err)
		return err
	}
	for i := 0; i < cnt; i++ {
		err = stream.Send(message)
		if err != nil {
			// fmt.Println(err)
			return err
		}
		r, err := stream.Recv()
		if err != nil && err == io.EOF {
			return nil
		}
		if r != nil && !r.GetSuccess() {
			return errors.New(r.GetError().GetMessage())
		}
	}
	return nil
}

func ProduceMessagesUnary(ctx context.Context, c pb.ProducerClient, cnt int, message *pb.Message, includePubID bool, clientName string) error {
	connConfig := connectConfig(clientName)

	defer func() {
		_, _ = c.Disconnect(ctx, &pb.Empty{})
	}()

	authResp, err := c.Connect(ctx, connConfig)
	if err != nil {
		// fmt.Printf("error calling pb.ProducerClient.Connect: %v\n", err)
		return err
	}
	if !authResp.GetSuccess() {
		return errors.New(authResp.GetError().GetMessage())
	}
	// fmt.Printf("Publisher connected - connConfig: %+v\n", connConfig)
	// fmt.Printf("Publisher connected - authResp: %+v\n", authResp)
	return ProduceMessagesUnaryWOConnect(ctx, c, cnt, message, includePubID)
}

func ProduceMessagesUnaryWOConnect(ctx context.Context, c pb.ProducerClient, cnt int, message *pb.Message, includePubID bool) error {
	for i := 0; i < cnt; i++ {
		if includePubID {
			// PublishID must start at 1
			pubID := i + 1
			message.PublishId = int64(pubID)
		}
		message.Body = []byte(fmt.Sprintf("%d of %d", i, cnt))
		resp, err := c.PublishOne(ctx, message)
		if err != nil {
			// fmt.Printf("error calling pb.ProducerClient.PublishOne: %v\n", err)
			return err
		}
		if resp != nil && !resp.GetSuccess() {
			return errors.New(resp.GetError().GetMessage())
		}
	}
	return nil
}

func ParallelProduceMessages(worker int, producer int, c pb.ProducerClient, ctx *context.Context, cnt int, cwg *sync.WaitGroup) {
	message := "test " + time.Now().String()

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy")
	address := &pb.Address{Name: "sastest.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	stream, _ := c.Publish(*ctx)
	msg := &pb.Message{Body: []byte(message), Address: address, Persistent: true, Headers: make(map[string]string)}

	for i := 0; i < cnt; i++ {
		ctx := context.Background()
		tracer := otel.Tracer("test_producer")
		var span trace.Span
		_, span = tracer.Start(ctx, "publish message", trace.WithSpanKind(trace.SpanKindProducer))
		// TODO figure out how to inject this stuff into the heders
		msg.Headers[tracing.TraceHeaderName] = span.SpanContext().TraceID().String()
		msg.Headers[tracing.SpanHeaderName] = span.SpanContext().SpanID().String()
		msg.Headers[tracing.HeaderTraceParent] = fmt.Sprintf("00-%s-%s-%s", span.SpanContext().TraceID().String(), span.SpanContext().SpanID().String(), span.SpanContext().TraceFlags())
		msg.Headers[tracing.HeaderTraceState] = span.SpanContext().TraceState().String()
		err := stream.Send(msg)
		span.AddEvent("message sent by client")
		if err != nil {
			log.Fatalf("worker %d/%d: failed to send message: %v", worker, producer, err)
		}
		resp, err := stream.Recv()
		span.AddEvent("response received from server")
		if !resp.GetSuccess() {
			log.Printf("worker %d/%d: publish failed: %v", worker, producer, err)
		}
		log.Printf("worker %d/%d: Successfully produced message", worker, producer)
		span.End()
	}
	stream.CloseSend() //nolint:errcheck
	cwg.Done()
}

// produceMessagesUnary produces messages using unary gRPC calls
// cnt: number of messages to produce
func ParallelProduceMessagesUnary(c pb.ProducerClient, cnt int, cwg *sync.WaitGroup) {
	message := "test " + time.Now().String()

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy")
	address := &pb.Address{Name: "sastest.topic", Subjects: subjects, Type: pb.Address_TOPIC}
	msg := &pb.Message{Body: []byte(message), Address: address, Persistent: true, Headers: make(map[string]string)}

	for i := 0; i < cnt; i++ {
		ctx := context.Background()
		tracer := otel.Tracer("test_producer")
		var span trace.Span
		_, span = tracer.Start(ctx, "publish message", trace.WithSpanKind(trace.SpanKindProducer))
		// TODO figure out how to inject this stuff into the heders
		msg.Headers[tracing.TraceHeaderName] = span.SpanContext().TraceID().String()
		msg.Headers[tracing.SpanHeaderName] = span.SpanContext().SpanID().String()
		msg.Headers[tracing.HeaderTraceParent] = fmt.Sprintf("00-%s-%s-%s", span.SpanContext().TraceID().String(), span.SpanContext().SpanID().String(), span.SpanContext().TraceFlags())
		msg.Headers[tracing.HeaderTraceState] = span.SpanContext().TraceState().String()
		resp, err := c.PublishOne(ctx, msg)
		span.AddEvent("message sent by client")
		if err != nil {
			log.Fatalf("failed to send message: %v", err)
		}

		span.AddEvent("response received from server")
		if !resp.GetSuccess() {
			log.Printf("publish failed: %v", err)
		}
		log.Printf("Successfully produced message %d/%d", i+1, cnt)
		span.End()
	}
	cwg.Done()
}
