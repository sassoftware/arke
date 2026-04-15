package main

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/sassoftware/arke/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Consumer handles consuming messages
type Consumer struct {
	ID      int
	conn    *Connection
	config  *StreamConfig
	stream  api.Consumer_ConsumeClient
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	stopped bool
}

// NewConsumer creates a new consumer
func NewConsumer(id int, conn *Connection, config *StreamConfig) *Consumer {
	ctx, cancel := context.WithCancel(conn.ctx)
	return &Consumer{
		ID:     id,
		conn:   conn,
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start starts the consumer
func (c *Consumer) Start() error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		Logger.Printf("Consumer %d: cannot start - already stopped", c.ID)
		return fmt.Errorf("consumer already stopped")
	}
	c.mu.Unlock()

	Logger.Printf("Consumer %d: starting (source: %s, type: %v)",
		c.ID, c.config.SourceName, c.config.SourceType)

	// Create consume stream
	stream, err := c.conn.ConsumerClient.Consume(c.ctx)
	if err != nil {
		Logger.Printf("Consumer %d: failed to create consume stream: %v", c.ID, err)
		return fmt.Errorf("failed to create consume stream: %w", err)
	}
	c.stream = stream

	// Determine address type based on source type
	// Queue sources use TOPIC addresses, streams use STREAM addresses
	addressType := api.Address_TOPIC
	if c.config.SourceType == api.Source_STREAM {
		addressType = api.Address_STREAM
	}

	options := map[string]string{}
	if c.config.SourceType == api.Source_STREAM {
		options["Offset"] = "next"
	}

	// Send source
	source := &api.Source{
		Name: c.config.SourceName,
		Type: c.config.SourceType,
		Address: &api.Address{
			Name:     c.config.AddressName,
			Type:     addressType,
			Subjects: []string{c.config.Subject},
		},
		PrefetchCount: 40,
		Options:       options,
	}

	err = stream.Send(&api.Consume{
		Msg: &api.Consume_Src{Src: source},
	})
	if err != nil {
		return fmt.Errorf("failed to send source: %w", err)
	}

	// Start consuming
	go c.consume()

	return nil
}

// consume receives and acknowledges messages
func (c *Consumer) consume() {
	for {
		resp, err := c.stream.Recv()
		if err != nil {
			if err == io.EOF || status.Code(err) == codes.Canceled {
				return
			}
			Logger.Printf("Consumer %d: receive error: %v", c.ID, err)
			return
		}

		switch r := resp.Resp.(type) {
		case *api.ConsumeResponse_Msg:
			// Acknowledge the message
			ack := &api.MessageConsumed{
				Uuid: r.Msg.Uuid,
				Nack: false,
			}

			err := c.stream.Send(&api.Consume{
				Msg: &api.Consume_Ack{Ack: ack},
			})
			if err != nil {
				if status.Code(err) != codes.Canceled {
					Logger.Printf("Consumer %d: failed to send ack: %v", c.ID, err)
				}
				return
			}

			c.conn.metrics.IncrementConsumed()

		case *api.ConsumeResponse_ConsumedResponse:
			// Message was acknowledged
			if !r.ConsumedResponse.Success && r.ConsumedResponse.Error != nil {
				Logger.Printf("Consumer %d: ack failed: %s", c.ID, r.ConsumedResponse.Error.Message)
			}

		case *api.ConsumeResponse_Error:
			// Error occurred
			Logger.Printf("Consumer %d: consume error (fatal=%v): %s", c.ID, r.Error.IsFatal, r.Error.Message)
			if r.Error.IsFatal {
				return
			}
		}
	}
}

// Stop stops the consumer
func (c *Consumer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopped {
		return
	}

	Logger.Printf("Consumer %d: stopping", c.ID)

	c.stopped = true
	if c.stream != nil {
		c.stream.CloseSend()
	}
	c.cancel()
}

// AddConsumers adds consumers to a connection
func (c *Connection) AddConsumers(config *StreamConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Type != ConnectionTypeConsumer {
		return fmt.Errorf("cannot add consumers to publisher connection")
	}

	startID := len(c.consumers)
	for i := 0; i < config.NumConsumerStreams; i++ {
		consumer := NewConsumer(startID+i, c, config)
		if err := consumer.Start(); err != nil {
			return fmt.Errorf("failed to start consumer %d: %w", i, err)
		}
		c.consumers = append(c.consumers, consumer)
	}

	return nil
}

// ScaleConsumers scales the number of consumers
func (c *Connection) ScaleConsumers(config *StreamConfig) error {
	c.mu.Lock()
	currentCount := len(c.consumers)
	c.mu.Unlock()

	if config.NumConsumerStreams > currentCount {
		// Add consumers
		newConfig := *config
		newConfig.NumConsumerStreams = config.NumConsumerStreams - currentCount
		return c.AddConsumers(&newConfig)
	} else if config.NumConsumerStreams < currentCount {
		// Remove consumers
		c.mu.Lock()
		for i := config.NumConsumerStreams; i < currentCount; i++ {
			if c.consumers[i] != nil {
				c.consumers[i].Stop()
			}
		}
		c.consumers = c.consumers[:config.NumConsumerStreams]
		c.mu.Unlock()
	}

	return nil
}
