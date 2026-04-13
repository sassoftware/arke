package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/sassoftware/arke/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Publisher handles publishing messages
type Publisher struct {
	ID              int
	conn            *Connection
	config          *StreamConfig
	stream          api.Producer_PublishClient
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.Mutex
	stopped         bool
	completed       bool // Set to true when message count is reached
	messageCount    int
	messageTemplate *MessageTemplate // loaded from file if MessageFile is specified
}

// NewPublisher creates a new publisher
func NewPublisher(id int, conn *Connection, config *StreamConfig) *Publisher {
	ctx, cancel := context.WithCancel(conn.ctx)
	return &Publisher{
		ID:     id,
		conn:   conn,
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start starts the publisher
func (p *Publisher) Start() error {
	p.mu.Lock()
	if p.stopped || p.completed {
		p.mu.Unlock()
		Logger.Printf("Publisher %d: cannot start - already stopped or completed", p.ID)
		return fmt.Errorf("publisher already stopped or completed")
	}
	p.mu.Unlock()

	Logger.Printf("Publisher %d: starting (rate limit: %d msg/s, message count: %d, continuous: %v)",
		p.ID, p.config.PublishRateLimit, p.config.MessageCount, p.config.IsContinuous)

	// Load message template from file if specified
	if p.config.MessageFile != "" {
		Logger.Printf("Publisher %d: loading message template from %s", p.ID, p.config.MessageFile)
		template, err := LoadMessageFromFile(p.config.MessageFile)
		if err != nil {
			Logger.Printf("Publisher %d: failed to load message file: %v", p.ID, err)
			return fmt.Errorf("failed to load message file: %w", err)
		}
		p.messageTemplate = template
		Logger.Printf("Publisher %d: loaded message template with %d headers, body length: %d",
			p.ID, len(template.Headers), len(template.Body))
	}

	// First, create a declare-only consumer if this is for publishing
	if err := p.declareSource(); err != nil {
		Logger.Printf("Publisher %d: failed to declare source: %v", p.ID, err)
		return fmt.Errorf("failed to declare source: %w", err)
	}

	// STREAM sources use the unary PublishOne RPC; QUEUE sources use the
	// bidirectional Publish stream for better throughput.
	if p.config.SourceType != api.Source_STREAM {
		stream, err := p.conn.ProducerClient.Publish(p.ctx)
		if err != nil {
			Logger.Printf("Publisher %d: failed to create publish stream: %v", p.ID, err)
			return fmt.Errorf("failed to create publish stream: %w", err)
		}
		p.stream = stream
		Logger.Printf("Publisher %d: publish stream created successfully", p.ID)
	} else {
		Logger.Printf("Publisher %d: using PublishOne for stream source", p.ID)
	}

	// Start publishing
	go p.publish()

	return nil
}

// declareSource creates a declare-only consume stream to ensure the source exists
func (p *Publisher) declareSource() error {
	// Create consume stream
	consumeStream, err := p.conn.ConsumerClient.Consume(p.ctx)
	if err != nil {
		return err
	}

	// Determine address type based on source type
	// Queue sources use TOPIC addresses, streams use STREAM addresses
	addressType := api.Address_TOPIC
	if p.config.SourceType == api.Source_STREAM {
		addressType = api.Address_STREAM
	}

	// Send source with declare_only = true
	source := &api.Source{
		Name:        p.config.SourceName,
		Type:        p.config.SourceType,
		DeclareOnly: true,
		Address: &api.Address{
			Name:     p.config.AddressName,
			Type:     addressType,
			Subjects: []string{p.config.Subject},
		},
	}

	err = consumeStream.Send(&api.Consume{
		Msg: &api.Consume_Src{Src: source},
	})
	if err != nil {
		consumeStream.CloseSend()
		return err
	}

	// Wait for declare-only response
	resp, err := consumeStream.Recv()
	if err != nil {
		consumeStream.CloseSend()
		return err
	}

	switch r := resp.Resp.(type) {
	case *api.ConsumeResponse_DeclareOnlyResponse:
		if !r.DeclareOnlyResponse.Success {
			consumeStream.CloseSend()
			return fmt.Errorf("declare failed: %s", r.DeclareOnlyResponse.Error.Message)
		}
	case *api.ConsumeResponse_Error:
		consumeStream.CloseSend()
		return fmt.Errorf("declare error: %s", r.Error.Message)
	}

	// Close the consume stream
	consumeStream.CloseSend()

	return nil
}

// publish sends messages
func (p *Publisher) publish() {
	// Setup rate limiting if configured
	var ticker *time.Ticker
	var tickerChan <-chan time.Time
	if p.config.PublishRateLimit > 0 {
		interval := time.Second / time.Duration(p.config.PublishRateLimit)
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
		tickerChan = ticker.C
	}

	for {
		// If rate limiting is enabled, wait for ticker
		if tickerChan != nil {
			select {
			case <-p.ctx.Done():
				return
			case <-tickerChan:
				// Continue to send message
			}
		} else {
			// No rate limiting, check context
			select {
			case <-p.ctx.Done():
				return
			default:
				// Continue to send message
			}
		}

		p.mu.Lock()
		if p.stopped {
			p.mu.Unlock()
			return
		}

		// Check if we've reached the message limit
		if !p.config.IsContinuous && p.messageCount >= p.config.MessageCount {
			p.completed = true
			p.mu.Unlock()
			Logger.Printf("Publisher %d: reached message limit (%d messages)", p.ID, p.messageCount)
			p.Stop()
			return
		}

		// Queue sources use TOPIC addresses, streams use STREAM addresses
		addressType := api.Address_TOPIC
		if p.config.SourceType == api.Source_STREAM {
			addressType = api.Address_STREAM
		}

		// Build message from template or generate default
		var msgBody []byte
		var msgHeaders map[string]string
		if p.messageTemplate != nil {
			body, headers := p.messageTemplate.GetBodyAndHeaders()
			msgBody = []byte(body)
			msgHeaders = headers
		} else {
			msgBody = []byte(fmt.Sprintf("Load test message %d from publisher %d", p.messageCount, p.ID))
		}

		msg := &api.Message{
			Body:    msgBody,
			Headers: msgHeaders,
			Address: &api.Address{
				Name:     p.config.AddressName,
				Type:     addressType,
				Subjects: []string{p.config.Subject},
			},
			Persistent: true,
		}
		p.mu.Unlock()

		var publishSuccess bool
		if p.stream != nil {
			// QUEUE path: bidirectional Publish stream
			err := p.stream.Send(msg)
			if err != nil {
				Logger.Printf("Publisher %d: send error at message %d: %v", p.ID, p.messageCount, err)
				if status.Code(err) == codes.Canceled || err == io.EOF {
					return
				}
				return
			}
			resp, err := p.stream.Recv()
			if err != nil {
				Logger.Printf("Publisher %d: receive error after message %d: %v", p.ID, p.messageCount, err)
				if err == io.EOF || status.Code(err) == codes.Canceled {
					return
				}
				return
			}
			publishSuccess = resp.Success
			if !resp.Success {
				if resp.Error != nil {
					Logger.Printf("Publisher %d: publish failed for message %d: %s", p.ID, p.messageCount, resp.Error.Message)
				} else {
					Logger.Printf("Publisher %d: publish failed for message %d (no error details)", p.ID, p.messageCount)
				}
			}
		} else {
			// STREAM path: unary PublishOne
			resp, err := p.conn.ProducerClient.PublishOne(p.ctx, msg)
			if err != nil {
				Logger.Printf("Publisher %d: PublishOne error at message %d: %v", p.ID, p.messageCount, err)
				if status.Code(err) == codes.Canceled {
					return
				}
				return
			}
			publishSuccess = resp.Success
			if !resp.Success {
				if resp.Error != nil {
					Logger.Printf("Publisher %d: PublishOne failed for message %d: %s", p.ID, p.messageCount, resp.Error.Message)
				} else {
					Logger.Printf("Publisher %d: PublishOne failed for message %d (no error details)", p.ID, p.messageCount)
				}
			}
		}
		_ = publishSuccess

		// Successfully sent and received response - increment counter
		p.mu.Lock()
		p.messageCount++
		p.conn.metrics.IncrementPublished()
		p.mu.Unlock()
	}
}

// Stop stops the publisher
func (p *Publisher) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return
	}

	Logger.Printf("Publisher %d: stopping (sent %d messages)", p.ID, p.messageCount)

	p.stopped = true
	if p.stream != nil {
		p.stream.CloseSend()
	}
	p.cancel()
}

// AddPublishers adds publishers to a connection
func (c *Connection) AddPublishers(config *StreamConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Type != ConnectionTypePublisher {
		return fmt.Errorf("cannot add publishers to consumer connection")
	}

	startID := len(c.publishers)
	for i := 0; i < config.NumPublisherStreams; i++ {
		publisher := NewPublisher(startID+i, c, config)
		if err := publisher.Start(); err != nil {
			return fmt.Errorf("failed to start publisher %d: %w", i, err)
		}
		c.publishers = append(c.publishers, publisher)
	}

	return nil
}

// ScalePublishers scales the number of publishers and updates their configuration
func (c *Connection) ScalePublishers(config *StreamConfig) error {
	c.mu.Lock()
	// Count only active publishers (not completed)
	activeCount := 0
	needsRestart := false
	for _, p := range c.publishers {
		p.mu.Lock()
		if !p.completed {
			activeCount++
			// Check if config has changed (rate limit, etc.)
			if p.config.PublishRateLimit != config.PublishRateLimit {
				needsRestart = true
			}
		}
		p.mu.Unlock()
	}
	totalCount := len(c.publishers)
	c.mu.Unlock()

	// If configuration has changed, restart all active publishers
	if needsRestart && config.NumPublisherStreams == activeCount {
		Logger.Printf("Connection %d: restarting %d publishers with new config (rate limit: %d)",
			c.ID, activeCount, config.PublishRateLimit)

		c.mu.Lock()
		// Stop all active publishers
		for _, p := range c.publishers {
			p.mu.Lock()
			if !p.completed {
				p.mu.Unlock()
				p.Stop()
			} else {
				p.mu.Unlock()
			}
		}
		// Clear the publisher list
		c.publishers = nil
		c.mu.Unlock()

		// Add new publishers with updated config
		return c.AddPublishers(config)
	}

	if config.NumPublisherStreams > activeCount {
		// Add new publishers (only if not in continuous mode or if we need more active ones)
		newConfig := *config
		newConfig.NumPublisherStreams = config.NumPublisherStreams - activeCount
		return c.AddPublishers(&newConfig)
	} else if config.NumPublisherStreams < totalCount {
		// Remove publishers from the end (prioritize removing completed ones)
		c.mu.Lock()
		// First, identify which publishers to remove
		toRemove := totalCount - config.NumPublisherStreams
		removed := 0

		// Remove from end, stopping any that are still active
		for i := totalCount - 1; i >= 0 && removed < toRemove; i-- {
			if c.publishers[i] != nil {
				c.publishers[i].Stop()
				removed++
			}
		}
		c.publishers = c.publishers[:config.NumPublisherStreams]
		c.mu.Unlock()
	}

	return nil
}
