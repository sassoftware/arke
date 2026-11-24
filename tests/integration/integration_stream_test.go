// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build integration || stream

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/sassoftware/arke/api"
	mf "github.com/sassoftware/arke/test/messagefunctions"
	"github.com/stretchr/testify/assert"
)

func TestProduceTwoStreamConsumeTwoCompressed(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 10
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	options := make(map[string]string)
	options["MessageTTL"] = "120"
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.PubOne")
	address := &pb.Address{Name: "sas.test.stream.TPTSCT", Subjects: subjects, Type: pb.Address_STREAM}
	source := &pb.Source{Name: "sas.test.stream.TPTSCT", Address: address, PrefetchCount: 1,
		Type: pb.Source_STREAM, Options: options}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	// Second consumer
	done2 := make(chan bool)
	clientConnected2 := make(chan bool)
	address2 := &pb.Address{Name: "sas.test.stream.TPTSCT2", Subjects: subjects, Type: pb.Address_STREAM}
	source2 := &pb.Source{Name: "sas.test.stream.TPTSCT2", Address: address2, PrefetchCount: 1,
		Type: pb.Source_STREAM, Options: options}
	c2 := pb.NewConsumerClient(consumerConnection)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	defer c2.Disconnect(ctx2, &pb.Empty{})
	messages2 := make(chan *pb.Message)

	go consumeMessages(consumerConnection, c2, ctx2, messages2, done2, clientConnected2, source2, defaultHandler, t)
	<-clientConnected2

	// Create a large payload exceeding the compression threshold
	payloadSize := 2 * 1024 * 1024
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	message := &pb.Message{Body: payload, Address: address, Headers: map[string]string{"test-header": "compressed-pathway"}}

	connConfig := connectConfig(t.Name())
	defer pc.Disconnect(pctx, &pb.Empty{})
	pc.Connect(pctx, connConfig)

	// Publish messages to first stream without modifying the payload
	for i := 0; i < expectedMessageCount; i++ {
		resp, err := pc.PublishOne(pctx, message)
		assert.Nil(t, err)
		if resp != nil {
			assert.True(t, resp.GetSuccess())
		}
	}

	// Publish messages to second stream
	message.Address = address2
	for i := 0; i < expectedMessageCount; i++ {
		resp, err := pc.PublishOne(pctx, message)
		assert.Nil(t, err)
		if resp != nil {
			assert.True(t, resp.GetSuccess())
		}
	}

	msgCount := 0
	breakLoop := false
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case msg := <-messages:
			// Verify the message body was decompressed and matches the original payload
			assert.Equal(t, payload, msg.Body, "Message body should match the original payload after decompression")
			assert.Equal(t, payloadSize, len(msg.Body), "Message body size should match the original payload size")
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)

	msgCount2 := 0
	breakLoop = false
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case msg := <-messages2:
			// Verify the message body was decompressed and matches the original payload
			assert.Equal(t, payload, msg.Body, "Message body should match the original payload after decompression")
			assert.Equal(t, payloadSize, len(msg.Body), "Message body size should match the original payload size")
			msgCount2++
		case <-done2:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount2)
}

func TestProduceOneStreamConsumeOne(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 100
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	options := make(map[string]string)
	options["MessageTTL"] = "120"
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.PubOne")
	address := &pb.Address{Name: "sas.test.stream.TPOSCO", Subjects: subjects, Type: pb.Address_STREAM}
	source := &pb.Source{Name: "sas.test.stream.TPOSCO", Address: address, PrefetchCount: 1,
		Type: pb.Source_STREAM, Options: options}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err := mf.ProduceMessagesUnary(pctx, pc, expectedMessageCount, message, false, t.Name())
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceTwoStreamConsumeTwo(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 10
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	options := make(map[string]string)
	options["MessageTTL"] = "120"
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.PubOne")
	address := &pb.Address{Name: "sas.test.stream.TPTSCT", Subjects: subjects, Type: pb.Address_STREAM}
	source := &pb.Source{Name: "sas.test.stream.TPTSCT", Address: address, PrefetchCount: 1,
		Type: pb.Source_STREAM, Options: options}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	// Second consumer
	done2 := make(chan bool)
	clientConnected2 := make(chan bool)
	address2 := &pb.Address{Name: "sas.test.stream.TPTSCT2", Subjects: subjects, Type: pb.Address_STREAM}
	source2 := &pb.Source{Name: "sas.test.stream.TPTSCT2", Address: address2, PrefetchCount: 1,
		Type: pb.Source_STREAM, Options: options}
	c2 := pb.NewConsumerClient(consumerConnection)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	defer c2.Disconnect(ctx2, &pb.Empty{})
	messages2 := make(chan *pb.Message)

	go consumeMessages(consumerConnection, c2, ctx2, messages2, done2, clientConnected2, source2, defaultHandler, t)
	<-clientConnected2

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	connConfig := connectConfig(t.Name())
	defer pc.Disconnect(pctx, &pb.Empty{})
	pc.Connect(pctx, connConfig)

	err := mf.ProduceMessagesUnaryWOConnect(pctx, pc, expectedMessageCount, message, false)
	assert.Nil(t, err)

	message.Address = address2
	err = mf.ProduceMessagesUnaryWOConnect(pctx, pc, expectedMessageCount, message, false)
	assert.Nil(t, err)

	msgCount := 0
	breakLoop := false
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)

	msgCount2 := 0
	breakLoop = false
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case <-messages2:
			msgCount2++
		case <-done2:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount2)
}

func TestConsumeContinueOffset(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	options := make(map[string]string)
	options["MessageTTL"] = "120"
	options["Offset"] = "continue"
	subjects := make([]string, 0)
	streamName := fmt.Sprintf("sas.test.stream.TCCO-%s", uuid.New().String())
	subjects = append(subjects, "sas.test.proxy.PubOne")
	address := &pb.Address{Name: streamName, Subjects: subjects, Type: pb.Address_STREAM}
	source := &pb.Source{Name: streamName, Address: address, PrefetchCount: 1,
		Type: pb.Source_STREAM, Options: options}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	connConfig := connectConfig(t.Name())
	defer pc.Disconnect(pctx, &pb.Empty{})
	pc.Connect(pctx, connConfig)

	err := mf.ProduceMessagesUnaryWOConnect(pctx, pc, expectedMessageCount, message, false)
	assert.Nil(t, err)

	msgCount := 0
	breakLoop := false
	for start := time.Now(); time.Since(start) < 5*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(5 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	// Ensure we recieved the first batch published
	assert.Equal(t, expectedMessageCount, msgCount)
	c.Disconnect(ctx, &pb.Empty{})

	expectedMessageCount = 10
	c2 := pb.NewConsumerClient(consumerConnection)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	defer c2.Disconnect(ctx2, &pb.Empty{})
	messages2 := make(chan *pb.Message)

	go consumeMessages(consumerConnection, c2, ctx2, messages2, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	err = mf.ProduceMessagesUnaryWOConnect(pctx, pc, expectedMessageCount, message, false)
	assert.Nil(t, err)

	msgCount2 := 0
	breakLoop = false
	for start := time.Now(); time.Since(start) < 5*time.Second; {
		select {
		case <-messages2:
			msgCount2++
		case <-done:
			breakLoop = true
		case <-time.After(5 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	// Ensure we only received the second batch published
	assert.Equal(t, expectedMessageCount, msgCount2)
}

func TestProduceStreamWithDeduplication(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 10
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	options := make(map[string]string)
	options["MessageTTL"] = "120"
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.PubOne")
	streamName := fmt.Sprintf("sas.test.stream.TPSWD.%s", uuid.New().String())
	address := &pb.Address{Name: streamName, Subjects: subjects, Type: pb.Address_STREAM}
	source := &pb.Source{Name: streamName, Address: address, PrefetchCount: 1,
		Type: pb.Source_STREAM, Options: options}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	// Give the stream time to be fully created
	time.Sleep(1 * time.Second)

	message := &pb.Message{Body: []byte("mymessage"), Address: address, Confirm: true, PublisherName: streamName}

	err := mf.ProduceMessagesUnary(pctx, pc, expectedMessageCount, message, true, t.Name())
	assert.Nil(t, err)
	err = mf.ProduceMessagesUnary(pctx, pc, expectedMessageCount, message, true, t.Name())
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 5*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(5 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceStreamWithDeduplicationFailWithOutProducerName(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.PubOne")
	streamName := fmt.Sprintf("sas.test.stream.TPSWDFWOPN.%s", uuid.New().String())
	address := &pb.Address{Name: streamName, Subjects: subjects, Type: pb.Address_STREAM}

	message := &pb.Message{Body: []byte("mymessage"), Address: address, Confirm: true}

	err := mf.ProduceMessagesUnary(pctx, pc, expectedMessageCount, message, true, t.Name())
	assert.Contains(t, err.Error(), "PublisherName not set on message, PublisherName is required when PublishID is set")
}

func TestProduceOneStreamConsumeOneWithConfirm(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 100
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	options := make(map[string]string)
	options["MessageTTL"] = "120"
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.PubOne")
	address := &pb.Address{Name: "sas.test.stream.TPOSCOWC", Subjects: subjects, Type: pb.Address_STREAM}
	source := &pb.Source{Name: "sas.test.stream.TPOSCOWC", Address: address, PrefetchCount: 1,
		Type: pb.Source_STREAM, Options: options}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address, Confirm: true}

	err := mf.ProduceMessagesUnary(pctx, pc, expectedMessageCount, message, false, t.Name())
	assert.Nil(t, err)

	msgCount := 0

	breakLoop := false
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount)
}

func TestProduceStreamConsumePrefetch(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 25
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)

	done := make(chan bool)
	clientConnected := make(chan bool)
	// consume before we produce

	consumerConnection := connect()
	defer consumerConnection.Close()
	options := make(map[string]string)
	options["MessageTTL"] = "120"
	subjects := make([]string, 0)
	subjects = append(subjects, "sas.test.proxy.PubOne")
	streamName := fmt.Sprintf("sas.test.stream.TPSCP.%s", uuid.New().String())
	address := &pb.Address{Name: streamName, Subjects: subjects, Type: pb.Address_STREAM}
	source := &pb.Source{Name: streamName, Address: address, PrefetchCount: 10,
		Type: pb.Source_STREAM, Options: options}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	var counter uint32
	prefetchHandler := func(msg *pb.Message) (int, error) {
		atomic.AddUint32(&counter, 1)
		time.Sleep(1500 * time.Millisecond)
		return 0, nil
	}
	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, prefetchHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("mymessage"), Address: address, Confirm: true}

	err := mf.ProduceMessagesUnary(pctx, pc, expectedMessageCount, message, false, t.Name())
	assert.Nil(t, err)

	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, 10, int(counter))
	time.Sleep(1500 * time.Millisecond)
	assert.Equal(t, 20, int(counter))
}

func TestConsumeDeclareOnly(t *testing.T) {
	var declareOnlyTests = []struct {
		addressType pb.Address_TargetType
		sourceType  pb.Source_TargetType
		addressName string
		sourceName  string
	}{
		{pb.Address_TOPIC, pb.Source_QUEUE, "amq.topic", "sas.test.proxy.TCDO.queue"},
		{pb.Address_STREAM, pb.Source_STREAM, "amq.mystream", "sas.test.proxy.TCDO.stream"},
	}

	for _, dot := range declareOnlyTests {
		testUUID := uuid.New().String()
		uniqueSourceName := fmt.Sprintf("%s.%s", dot.sourceName, testUUID)

		connConfig := connectConfig(t.Name())
		subjects := make([]string, 0)
		subjects = append(subjects, uniqueSourceName)
		address := &pb.Address{Name: dot.addressName, Subjects: subjects, Type: dot.addressType}

		consumerConnection := connect()
		defer consumerConnection.Close()

		source := &pb.Source{Name: uniqueSourceName, Address: address, PrefetchCount: 5, DeclareOnly: true, Type: dot.sourceType}
		c := pb.NewConsumerClient(consumerConnection)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		defer c.Disconnect(ctx, &pb.Empty{})

		connResp, err := c.Connect(ctx, connConfig)
		assert.Nil(t, err)
		assert.NotNil(t, connResp)
		assert.True(t, connResp.GetSuccess())

		consumerStream, err := c.Consume(ctx)
		assert.Nil(t, err)

		cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}
		err = consumerStream.Send(cnsm)
		assert.Nil(t, err)

		cr, _ := consumerStream.Recv()
		assert.NotNil(t, cr.GetDeclareOnlyResponse())
		assert.Nil(t, cr.GetError())
		dor := cr.GetDeclareOnlyResponse()
		assert.Nil(t, dor.Error)
		assert.True(t, dor.GetSuccess())
	}
}

func TestConsumeSourceStats(t *testing.T) {
	testUUID := uuid.New().String()

	var declareOnlyTests = []struct {
		addressType           pb.Address_TargetType
		sourceType            pb.Source_TargetType
		name                  string
		expectedConsumerCount int32
		expectedMessageCount  int64
		publishMessageCount   int
	}{
		{pb.Address_STREAM, pb.Source_STREAM, fmt.Sprintf("%s.%s", "sas.test.proxy.TCSS.stream", testUUID), 0, 0, 5},
		{pb.Address_STREAM, pb.Source_STREAM, fmt.Sprintf("%s.%s", "sas.test.proxy.TCSS.stream.zero", testUUID), 0, 0, 0},
		{pb.Address_TOPIC, pb.Source_QUEUE, fmt.Sprintf("%s.%s", "sas.test.proxy.TCSS.queue", testUUID), 0, 4, 4},
		{pb.Address_TOPIC, pb.Source_QUEUE, fmt.Sprintf("%s.%s", "sas.test.proxy.TCSS.queue.zero", testUUID), 0, 0, 0},
	}

	for _, dot := range declareOnlyTests {
		// set up the consumer with DeclareOnly=true because we don't want to consume anything
		timeout := 15 * time.Second
		connConfig := connectConfig(t.Name())
		consumerConnection := connect()
		defer consumerConnection.Close()

		// must publish to Address.Name equal to Source.Name of the consumer
		subjects := make([]string, 0)
		subjects = append(subjects, dot.name)
		address := &pb.Address{Name: dot.name, Subjects: subjects, Type: dot.addressType}
		source := &pb.Source{Name: dot.name, Address: address, PrefetchCount: 5,
			DeclareOnly: true, Type: dot.sourceType}

		c := pb.NewConsumerClient(consumerConnection)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		defer c.Disconnect(ctx, &pb.Empty{})

		connResp, err := c.Connect(ctx, connConfig)
		assert.Nil(t, err)
		assert.NotNil(t, connResp)
		assert.True(t, connResp.GetSuccess())

		consumerStream, err := c.Consume(ctx)
		assert.Nil(t, err)

		cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}
		err = consumerStream.Send(cnsm)
		assert.Nil(t, err)

		cr, _ := consumerStream.Recv()
		assert.NotNil(t, cr.GetDeclareOnlyResponse())
		assert.Nil(t, cr.GetError())
		dor := cr.GetDeclareOnlyResponse()
		assert.Nil(t, dor.Error)
		assert.True(t, dor.GetSuccess())

		if dot.publishMessageCount > 0 {
			// set up the producer so we can send messages
			producerConnection := connect()
			defer producerConnection.Close()
			pc := pb.NewProducerClient(producerConnection)
			pctx := context.Background()
			defer pc.Disconnect(pctx, &pb.Empty{})

			message := &pb.Message{Body: []byte("mybody1"), Address: address}

			if dot.addressType == pb.Address_TOPIC {
				err = mf.ProduceSendMessages(pctx, pc, dot.publishMessageCount, message, t.Name())
			} else {
				err = mf.ProduceMessagesUnary(pctx, pc, dot.publishMessageCount, message, false, t.Name())
			}
			assert.Nil(t, err)
		}

		// request SourceStats a few times over 45 seconds to see if the rabbit API is available now
		start := time.Now()
		var stats *pb.SourceStats
		var ssErr error
		for time.Since(start) <= timeout {
			stats, ssErr = c.SourceStats(ctx, source)
			expectedOffset := dot.publishMessageCount
			if expectedOffset > 0 {
				expectedOffset--
			}
			if ssErr == nil && ((stats.MessageCount == int64(dot.publishMessageCount)) || (stats.GetLastOffset() >= int64(expectedOffset))) {
				t.Logf("%s: got stats for messageCount=%d", dot.name, stats.MessageCount)
				break
			}
			time.Sleep(1 * time.Second)
		}

		assert.Nil(t, ssErr)
		assert.NotNil(t, stats)
		if dot.publishMessageCount == 0 && dot.addressType == pb.Address_STREAM {
			assert.Equal(t, "Offset not found", stats.GetError().GetMessage())
		} else {
			assert.Nil(t, stats.Error)
		}
		assert.Equal(t, dot.expectedConsumerCount, stats.ConsumerCount, dot.name)
		assert.Equal(t, dot.expectedMessageCount, stats.MessageCount, dot.name)
		assert.Equal(t, int64(0), stats.GetCurrentOffset(), dot.name)
		if dot.publishMessageCount > 0 && dot.sourceType == pb.Source_STREAM {
			assert.Greater(t, stats.LastOffset, int64(0), dot.name)
		}
	}

	for _, dot := range declareOnlyTests {
		if dot.sourceType != pb.Source_STREAM {
			continue
		}

		messages := make(chan *pb.Message)
		done := make(chan bool)
		clientConnected := make(chan bool)

		consumerConnection := connect()
		defer consumerConnection.Close()
		c := pb.NewConsumerClient(consumerConnection)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		// NOTE: do NOT defer cancel() here — we call it explicitly below so the
		// goroutine exits before we query SourceStats.
		defer c.Disconnect(ctx, &pb.Empty{})

		address := &pb.Address{Name: dot.name, Type: dot.addressType}
		source := &pb.Source{SingleActiveConsumer: true, Name: dot.name, Address: address, PrefetchCount: 5, Type: dot.sourceType, Options: map[string]string{"Offset": "first", "ConsumerGroup": "MyGroupName"}}
		go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
		<-clientConnected

		msgCount := 0

		breakLoop := false
		for start := time.Now(); time.Since(start) < 5*time.Second; {
			select {
			case <-messages:
				msgCount++
			case <-done:
				breakLoop = true
			case <-time.After(1 * time.Second):
				breakLoop = true
			}
			if breakLoop {
				break
			}
		}

		// Cancel context to prompt disconnect and remove the consumer from the broker.
		cancel()
		select {
		case <-done:
			// goroutine has exited; its deferred c.Disconnect() has run
		case <-time.After(10 * time.Second):
			t.Log("timed out waiting for consumer goroutine to exit")
		}

		// Use a fresh connection to query SourceStats after the consumer disconnected.
		statsConn := connect()
		defer statsConn.Close()
		statsC := pb.NewConsumerClient(statsConn)
		statsCtx, statsCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer statsCancel()
		defer statsC.Disconnect(statsCtx, &pb.Empty{})
		statsConnResp, statsConnErr := statsC.Connect(statsCtx, connectConfig(t.Name()))
		assert.Nil(t, statsConnErr)
		assert.True(t, statsConnResp.GetSuccess())

		// Poll until the consumer count settles: the RabbitMQ management API can
		// lag briefly behind the actual connection state.
		var stats *pb.SourceStats
		var ssErr error
		ccPollDeadline := time.Now().Add(5 * time.Second)
		for {
			stats, ssErr = statsC.SourceStats(statsCtx, source)
			if ssErr == nil && stats.ConsumerCount == dot.expectedConsumerCount {
				break
			}
			if time.Now().After(ccPollDeadline) {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		assert.Nil(t, ssErr)
		assert.NotNil(t, stats)
		assert.Equal(t, dot.expectedConsumerCount, stats.ConsumerCount)
		assert.Equal(t, dot.expectedMessageCount, stats.MessageCount)
		assert.Equal(t, dot.publishMessageCount, msgCount)
		expectedOffset := dot.publishMessageCount
		if dot.publishMessageCount > 0 {
			expectedOffset--
			assert.Nil(t, stats.Error)
		} else {
			assert.Equal(t, "Offset not found", stats.GetError().GetMessage())
		}
		assert.Equal(t, int64(expectedOffset), stats.LastOffset, dot.name)
		assert.Equal(t, int64(expectedOffset), stats.GetCurrentOffset(), dot.name)
		assert.Equal(t, stats.GetLastOffset(), stats.GetCurrentOffset(), dot.name)
	}

	for _, dot := range declareOnlyTests {
		if dot.sourceType != pb.Source_STREAM {
			continue
		}
		if dot.publishMessageCount > 0 {
			// must publish to Address.Name equal to Source.Name of the consumer
			subjects := make([]string, 0)
			subjects = append(subjects, dot.name)
			address := &pb.Address{Name: dot.name, Subjects: subjects, Type: dot.addressType}
			source := &pb.Source{SingleActiveConsumer: true, Name: dot.name, Address: address, PrefetchCount: 5, Type: dot.sourceType, Options: map[string]string{"Offset": "first", "ConsumerGroup": "MyGroupName"}}

			// set up the producer so we can send messages
			producerConnection := connect()
			defer producerConnection.Close()
			pc := pb.NewProducerClient(producerConnection)
			pctx := context.Background()
			defer pc.Disconnect(pctx, &pb.Empty{})

			message := &pb.Message{Body: []byte("mybody1"), Address: address}

			err := mf.ProduceMessagesUnary(pctx, pc, dot.publishMessageCount, message, false, t.Name())
			assert.Nil(t, err)

			consumerConnection := connect()
			defer consumerConnection.Close()
			connConfig := connectConfig(t.Name())
			c := pb.NewConsumerClient(consumerConnection)
			connResp, err := c.Connect(pctx, connConfig)
			assert.Nil(t, err)
			assert.NotNil(t, connResp)
			assert.True(t, connResp.GetSuccess())
			defer c.Disconnect(pctx, &pb.Empty{})

			// Poll until LastOffset > CurrentOffset, confirming new messages are reflected
			// and the consumer group position is behind the stream tail.
			var stats *pb.SourceStats
			var ssErr error
			pollDeadline := time.Now().Add(15 * time.Second)
			for {
				stats, ssErr = c.SourceStats(pctx, source)
				if ssErr == nil && stats.GetLastOffset() > stats.GetCurrentOffset() {
					break
				}
				if time.Now().After(pollDeadline) {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			assert.Nil(t, ssErr)
			assert.NotNil(t, stats)
			// LastOffset is the stream's tail; CurrentOffset is the consumer group's
			// committed position. After publishing new messages, the stream has advanced
			// past what the group consumed, so LastOffset must be greater.
			assert.Greater(t, stats.GetLastOffset(), stats.GetCurrentOffset(), dot.name)
		}
	}
}

func TestConsumeSourceStatsGroup(t *testing.T) {
	testUUID := uuid.New().String()

	var declareOnlyTests = []struct {
		addressType           pb.Address_TargetType
		sourceType            pb.Source_TargetType
		name                  string
		expectedConsumerCount int32
		expectedMessageCount  int64
		publishMessageCount   int
	}{
		{pb.Address_STREAM, pb.Source_STREAM, fmt.Sprintf("%s.%s", "sas.test.proxy.TCSS.stream", testUUID), 0, 0, 5},
		{pb.Address_STREAM, pb.Source_STREAM, fmt.Sprintf("%s.%s", "sas.test.proxy.TCSS.stream.zero", testUUID), 0, 0, 0},
		{pb.Address_TOPIC, pb.Source_QUEUE, fmt.Sprintf("%s.%s", "sas.test.proxy.TCSS.queue", testUUID), 0, 4, 4},
		{pb.Address_TOPIC, pb.Source_QUEUE, fmt.Sprintf("%s.%s", "sas.test.proxy.TCSS.queue.zero", testUUID), 0, 0, 0},
	}

	for _, dot := range declareOnlyTests {
		// set up the consumer with DeclareOnly=true because we don't want to consume anything
		timeout := 15 * time.Second
		connConfig := connectConfig(t.Name())
		consumerConnection := connect()
		defer consumerConnection.Close()

		// must publish to Address.Name equal to Source.Name of the consumer
		subjects := make([]string, 0)
		subjects = append(subjects, dot.name)
		address := &pb.Address{Name: dot.name, Subjects: subjects, Type: dot.addressType}
		source := &pb.Source{Name: dot.name, Address: address, PrefetchCount: 5,
			DeclareOnly: true, Type: dot.sourceType}
		// put two identical sources in the Sources request
		sources := &pb.Sources{Sources: []*pb.Source{source, source}}

		c := pb.NewConsumerClient(consumerConnection)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		defer c.Disconnect(ctx, &pb.Empty{})

		connResp, err := c.Connect(ctx, connConfig)
		assert.Nil(t, err)
		assert.NotNil(t, connResp)
		assert.True(t, connResp.GetSuccess())

		consumerStream, err := c.Consume(ctx)
		assert.Nil(t, err)

		cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}
		err = consumerStream.Send(cnsm)
		assert.Nil(t, err)

		cr, _ := consumerStream.Recv()
		assert.NotNil(t, cr.GetDeclareOnlyResponse())
		assert.Nil(t, cr.GetError())
		dor := cr.GetDeclareOnlyResponse()
		assert.Nil(t, dor.Error)
		assert.True(t, dor.GetSuccess())

		if dot.publishMessageCount > 0 {
			// set up the producer so we can send messages
			producerConnection := connect()
			defer producerConnection.Close()
			pc := pb.NewProducerClient(producerConnection)
			pctx := context.Background()
			defer pc.Disconnect(pctx, &pb.Empty{})

			message := &pb.Message{Body: []byte("mybody1"), Address: address}

			if dot.addressType == pb.Address_TOPIC {
				err = mf.ProduceSendMessages(pctx, pc, dot.publishMessageCount, message, t.Name())
			} else {
				err = mf.ProduceMessagesUnary(pctx, pc, dot.publishMessageCount, message, false, t.Name())
			}
			assert.Nil(t, err)
		}

		// request SourceStats a few times over 45 seconds to see if the rabbit API is available now
		start := time.Now()
		var statsCollection *pb.SourceStatsCollection
		var ssErr error
		for time.Since(start) <= timeout {
			statsCollection, ssErr = c.SourceStatsGroup(ctx, sources)
			expectedOffset := dot.publishMessageCount
			if expectedOffset > 0 {
				expectedOffset--
			}
			done := true
			for _, stats := range statsCollection.Stats {
				if ssErr == nil && ((stats.MessageCount == int64(dot.publishMessageCount)) || (stats.GetLastOffset() >= int64(expectedOffset))) {
					t.Logf("%s: got stats for messageCount=%d", dot.name, stats.MessageCount)
				} else {
					done = false
				}
			}
			if done {
				break
			}
			time.Sleep(1 * time.Second)
		}

		assert.Nil(t, ssErr)
		assert.NotNil(t, statsCollection)
		for _, stats := range statsCollection.Stats {
			if dot.publishMessageCount == 0 && dot.addressType == pb.Address_STREAM {
				assert.Equal(t, "Offset not found", stats.GetError().GetMessage())
			} else {
				assert.Nil(t, stats.Error)
			}
			assert.Equal(t, dot.expectedConsumerCount, stats.ConsumerCount, dot.name)
			assert.Equal(t, dot.expectedMessageCount, stats.MessageCount, dot.name)
			assert.Equal(t, int64(0), stats.GetCurrentOffset(), dot.name)
			if dot.publishMessageCount > 0 && dot.sourceType == pb.Source_STREAM {
				assert.Greater(t, stats.LastOffset, int64(0), dot.name)
			}
		}
	}

	for _, dot := range declareOnlyTests {
		if dot.sourceType != pb.Source_STREAM {
			continue
		}

		messages := make(chan *pb.Message)
		done := make(chan bool)
		clientConnected := make(chan bool)

		consumerConnection := connect()
		defer consumerConnection.Close()
		c := pb.NewConsumerClient(consumerConnection)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		// NOTE: do NOT defer cancel() here — we call it explicitly below so the
		// goroutine exits before we query SourceStatsGroup.
		defer c.Disconnect(ctx, &pb.Empty{})

		address := &pb.Address{Name: dot.name, Type: dot.addressType}
		source := &pb.Source{SingleActiveConsumer: true, Name: dot.name, Address: address, PrefetchCount: 5, Type: dot.sourceType, Options: map[string]string{"Offset": "first", "ConsumerGroup": "MyGroupName"}}
		// put two identical sources in the Sources request
		sources := &pb.Sources{Sources: []*pb.Source{source, source}}
		go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
		<-clientConnected

		msgCount := 0

		breakLoop := false
		for start := time.Now(); time.Since(start) < 5*time.Second; {
			select {
			case <-messages:
				msgCount++
			case <-done:
				breakLoop = true
			case <-time.After(1 * time.Second):
				breakLoop = true
			}
			if breakLoop {
				break
			}
		}

		// Cancel context to prompt disconnect and remove the consumer from the broker.
		cancel()
		select {
		case <-done:
			// goroutine has exited; its deferred c.Disconnect() has run
		case <-time.After(10 * time.Second):
			t.Log("timed out waiting for consumer goroutine to exit")
		}

		// Use a fresh connection to query SourceStatsGroup after the consumer disconnected.
		statsConn := connect()
		defer statsConn.Close()
		statsC := pb.NewConsumerClient(statsConn)
		statsCtx, statsCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer statsCancel()
		defer statsC.Disconnect(statsCtx, &pb.Empty{})
		statsConnResp, statsConnErr := statsC.Connect(statsCtx, connectConfig(t.Name()))
		assert.Nil(t, statsConnErr)
		assert.True(t, statsConnResp.GetSuccess())

		// Poll until all consumer counts settle: the RabbitMQ management API can
		// lag briefly behind the actual connection state.
		var statsCollection *pb.SourceStatsCollection
		var ssErr error
		ccPollDeadline := time.Now().Add(5 * time.Second)
		for {
			statsCollection, ssErr = statsC.SourceStatsGroup(statsCtx, sources)
			allMatch := ssErr == nil
			if allMatch {
				for _, stats := range statsCollection.GetStats() {
					if stats.ConsumerCount != dot.expectedConsumerCount {
						allMatch = false
						break
					}
				}
			}
			if allMatch {
				break
			}
			if time.Now().After(ccPollDeadline) {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		assert.Nil(t, ssErr)
		assert.NotNil(t, statsCollection)
		for _, stats := range statsCollection.Stats {
			assert.NotNil(t, stats)
			assert.Equal(t, dot.expectedConsumerCount, stats.ConsumerCount)
			assert.Equal(t, dot.expectedMessageCount, stats.MessageCount)
			assert.Equal(t, dot.publishMessageCount, msgCount)
			expectedOffset := dot.publishMessageCount
			if dot.publishMessageCount > 0 {
				expectedOffset--
				assert.Nil(t, stats.Error)
			} else {
				assert.Equal(t, "Offset not found", stats.GetError().GetMessage())
			}
			assert.Equal(t, int64(expectedOffset), stats.LastOffset, dot.name)
			assert.Equal(t, int64(expectedOffset), stats.GetCurrentOffset(), dot.name)
			assert.Equal(t, stats.GetLastOffset(), stats.GetCurrentOffset(), dot.name)
		}
	}

	for _, dot := range declareOnlyTests {
		if dot.sourceType != pb.Source_STREAM {
			continue
		}
		if dot.publishMessageCount > 0 {
			// must publish to Address.Name equal to Source.Name of the consumer
			subjects := make([]string, 0)
			subjects = append(subjects, dot.name)
			address := &pb.Address{Name: dot.name, Subjects: subjects, Type: dot.addressType}
			source := &pb.Source{SingleActiveConsumer: true, Name: dot.name, Address: address, PrefetchCount: 5, Type: dot.sourceType, Options: map[string]string{"Offset": "first", "ConsumerGroup": "MyGroupName"}}
			// put two identical sources in the Sources request
			sources := &pb.Sources{Sources: []*pb.Source{source, source}}

			// set up the producer so we can send messages
			producerConnection := connect()
			defer producerConnection.Close()
			pc := pb.NewProducerClient(producerConnection)
			pctx := context.Background()
			defer pc.Disconnect(pctx, &pb.Empty{})

			message := &pb.Message{Body: []byte("mybody1"), Address: address}

			err := mf.ProduceMessagesUnary(pctx, pc, dot.publishMessageCount, message, false, t.Name())
			assert.Nil(t, err)

			consumerConnection := connect()
			defer consumerConnection.Close()
			connConfig := connectConfig(t.Name())
			c := pb.NewConsumerClient(consumerConnection)
			connResp, err := c.Connect(pctx, connConfig)
			assert.Nil(t, err)
			assert.NotNil(t, connResp)
			assert.True(t, connResp.GetSuccess())
			defer c.Disconnect(pctx, &pb.Empty{})

			// Poll until LastOffset > CurrentOffset for every entry in the collection,
			// confirming new messages are reflected and the consumer group is behind the tail.
			var statsCollection *pb.SourceStatsCollection
			var ssErr error
			pollDeadline := time.Now().Add(15 * time.Second)
			for {
				statsCollection, ssErr = c.SourceStatsGroup(pctx, sources)
				allGood := ssErr == nil
				if allGood {
					for _, stats := range statsCollection.GetStats() {
						if stats.GetLastOffset() <= stats.GetCurrentOffset() {
							allGood = false
							break
						}
					}
				}
				if allGood {
					break
				}
				if time.Now().After(pollDeadline) {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			assert.Nil(t, ssErr)
			assert.NotNil(t, statsCollection)
			for _, stats := range statsCollection.Stats {
				assert.Greater(t, stats.GetLastOffset(), stats.GetCurrentOffset(), dot.name)
			}
		}
	}
}

func TestProduceFailsIfNoStream(t *testing.T) {
	ctx := context.Background()
	conn := connect()
	defer conn.Close()

	connConfig := connectConfig(t.Name())
	c := pb.NewProducerClient(conn)
	defer c.Disconnect(ctx, &pb.Empty{})

	connResp, err := c.Connect(ctx, connConfig)
	assert.Nil(t, err, "should not get an error connecting: %v", err)
	assert.True(t, connResp.GetSuccess(), "should get a successful connection")

	name := "sas.test.proxy.ProducerFailsIfNoStream." + uuid.New().String()
	options := make(map[string]string)
	options["MessageTTL"] = "120"
	subjects := make([]string, 0)
	subjects = append(subjects, name)
	address := &pb.Address{Name: "sas.test.stream.TPFINS." + uuid.New().String(), Subjects: subjects, Type: pb.Address_STREAM}

	pctx := context.Background()
	expectedMessageCount := 100
	message := &pb.Message{Body: []byte("mymessage"), Address: address}

	err = mf.ProduceMessagesUnary(pctx, c, expectedMessageCount, message, false, t.Name())
	assert.NotNil(t, err, "should get error when no stream: %v", err)
}

func Test_SourceStatsNoStream(t *testing.T) {
	tid := uuid.New().String()

	timeout := 15 * time.Second
	connConfig := connectConfig(t.Name())
	consumerConnection := connect()
	defer consumerConnection.Close()

	subjects := make([]string, 0)
	subjects = append(subjects, "noname-"+tid)
	address := &pb.Address{Name: "noname-" + tid, Subjects: subjects, Type: pb.Address_STREAM}
	source := &pb.Source{Name: "noname-" + tid, Address: address, PrefetchCount: 5,
		DeclareOnly: true, Type: pb.Source_STREAM}

	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	connResp, err := c.Connect(ctx, connConfig)
	assert.Nil(t, err)
	assert.NotNil(t, connResp)
	assert.True(t, connResp.GetSuccess())

	var stats *pb.SourceStats
	var ssErr error
	stats, ssErr = c.SourceStats(ctx, source)

	assert.Nil(t, ssErr)
	assert.NotNil(t, stats)
	assert.NotNil(t, stats.Error)
	assert.Contains(t, stats.Error.GetMessage(), "request returned a 404")

	// verified 404 when no created yet, now create it and get stats.
	// we should get an "Offset not found" error in the stats.Error
	strm, cErr := c.Consume(ctx)
	cnsm := &pb.Consume{Msg: &pb.Consume_Src{Src: source}}
	err = strm.Send(cnsm)
	assert.Nil(t, err)
	assert.Nil(t, cErr)
	strm.CloseSend()

	start := time.Now()
	for time.Since(start) <= timeout {
		stats, ssErr = c.SourceStats(ctx, source)
		assert.Nil(t, ssErr)
		assert.NotNil(t, stats)
		// emsg := stats.Error.GetMessage()
		if strings.Contains(stats.GetError().GetMessage(), "request returned a 404") {
			time.Sleep(1 * time.Second)
			continue
		}
		assert.Equal(t, "Offset not found", stats.GetError().GetMessage())
		break
	}
}
func TestStreamHeaderReceivedTimeEqualsTimestampInMs(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 1
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)
	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()
	subjects := []string{"sas.test.proxy.HeaderCheck"}
	streamName := "sas.test.stream.HeaderCheck"
	address := &pb.Address{Name: streamName, Subjects: subjects, Type: pb.Address_STREAM}
	header := make(map[string]string)
	header["x-opt-rabbitmq-received-time"] = "test-header"
	source := &pb.Source{Name: streamName, Address: address, PrefetchCount: 1, Type: pb.Source_STREAM}
	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	message := &pb.Message{Body: []byte("header-check-message"), Address: address, Headers: header}

	err := mf.ProduceMessagesUnary(pctx, pc, expectedMessageCount, message, false, t.Name())
	assert.Nil(t, err)

	var received *pb.Message
	breakLoop := false
	for start := time.Now(); time.Since(start) < 5*time.Second; {
		select {
		case m := <-messages:
			received = m
			breakLoop = true
		case <-done:
			breakLoop = true
		case <-time.After(2 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.NotNil(t, received, "Did not receive a message from stream")
	headers := received.GetHeaders()
	receivedTime, ok1 := headers["x-opt-rabbitmq-received-time"]
	timestampInMs, ok2 := headers["timestamp_in_ms"]
	t.Logf("headers: %+v", headers)
	assert.True(t, ok1, "header 'x-opt-rabbitmq-received-time' not found")
	assert.True(t, ok2, "header 'timestamp_in_ms' not found")
	assert.Equal(t, receivedTime, timestampInMs, "header values do not match")
}

func TestProduceOneStreamConsumeOneViaTopicExchange(t *testing.T) {
	producerConnection := connect()
	defer producerConnection.Close()
	expectedMessageCount := 10
	pc := pb.NewProducerClient(producerConnection)
	pctx := context.Background()
	defer pc.Disconnect(pctx, &pb.Empty{})

	messages := make(chan *pb.Message)
	done := make(chan bool)
	clientConnected := make(chan bool)

	consumerConnection := connect()
	defer consumerConnection.Close()

	options := make(map[string]string)
	options["MessageTTL"] = "120"

	// Use a unique stream name so parallel or repeated test runs do not conflict.
	streamName := fmt.Sprintf("sas.test.stream.ViaTopicExchange.%s", uuid.New().String())
	subject := "sas.test.proxy.ViaTopicExchange"

	// Key difference from TestProduceOneStreamConsumeOne: the address type is
	// TOPIC, not STREAM.  The provider must declare a binding from amq.topic
	// (with the given routing key / subject) to the stream so that messages
	// published to the exchange reach the stream consumer.
	consumerAddress := &pb.Address{
		Name:     "amq.topic",
		Subjects: []string{subject},
		Type:     pb.Address_TOPIC,
	}
	source := &pb.Source{
		Name:          streamName,
		Address:       consumerAddress,
		PrefetchCount: 1,
		Type:          pb.Source_STREAM,
		Options:       options,
	}

	c := pb.NewConsumerClient(consumerConnection)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer c.Disconnect(ctx, &pb.Empty{})

	go consumeMessages(consumerConnection, c, ctx, messages, done, clientConnected, source, defaultHandler, t)
	<-clientConnected

	// Publish to the TOPIC exchange using the same routing key.  The binding
	// declared during Subscribe routes these messages into the stream.
	publishAddress := &pb.Address{
		Name:     "amq.topic",
		Subjects: []string{subject},
		Type:     pb.Address_TOPIC,
	}
	message := &pb.Message{Body: []byte("mymessage"), Address: publishAddress}

	err := mf.ProduceMessagesUnary(pctx, pc, expectedMessageCount, message, false, t.Name())
	assert.Nil(t, err, "publishing to amq.topic should succeed: %v", err)

	msgCount := 0
	breakLoop := false
	for start := time.Now(); time.Since(start) < 5*time.Second; {
		select {
		case <-messages:
			msgCount++
		case <-done:
			breakLoop = true
		case <-time.After(5 * time.Second):
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}
	assert.Equal(t, expectedMessageCount, msgCount,
		"all messages published to amq.topic must arrive at the stream consumer via the binding")
}
