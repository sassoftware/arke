package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sassoftware/arke/api"
)

// Config holds the configuration for the arke server and broker
type Config struct {
	// Arke server settings
	ArkeHost string `json:"arke_host"`
	ArkePort int    `json:"arke_port"`
	ArkeTLS  bool   `json:"arke_tls"`

	// Broker settings
	BrokerHost     string `json:"broker_host"`
	BrokerPort     int    `json:"broker_port"`
	BrokerUsername string `json:"broker_username"`
	BrokerPassword string `json:"broker_password"`
	BrokerTLS      bool   `json:"broker_tls"`
	BrokerProvider string `json:"broker_provider"`

	// Connection settings
	NumPublisherConnections int `json:"num_publisher_connections"`
	NumConsumerConnections  int `json:"num_consumer_connections"`
}

// ConnectionType represents whether connection is Publisher or Consumer
type ConnectionType int

const (
	ConnectionTypePublisher ConnectionType = iota
	ConnectionTypeConsumer
)

// StreamConfig holds the configuration for a stream
type StreamConfig struct {
	NumPublisherStreams int                   `json:"num_publisher_streams"`
	NumConsumerStreams  int                   `json:"num_consumer_streams"`
	SourceType          api.Source_TargetType `json:"source_type"`
	SourceName          string                `json:"source_name"`
	AddressName         string                `json:"address_name"`
	Subject             string                `json:"subject"`
	MessageCount        int                   `json:"message_count"` // 0 means continuous
	IsContinuous        bool                  `json:"is_continuous"`
	PublishRateLimit    int                   `json:"publish_rate_limit"` // messages per second per stream, 0 means no limit
	MessageFile         string                `json:"message_file"`       // path to JSON file containing message template
}

// SavedConfig combines Config and StreamConfig for saving/loading
type SavedConfig struct {
	Config       *Config       `json:"config"`
	StreamConfig *StreamConfig `json:"stream_config"`
}

// SaveConfig saves the configuration to a JSON file
func SaveConfig(filename string, config *Config, streamConfig *StreamConfig) error {
	savedConfig := &SavedConfig{
		Config:       config,
		StreamConfig: streamConfig,
	}

	data, err := json.MarshalIndent(savedConfig, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// LoadConfig loads the configuration from a JSON file
func LoadConfig(filename string) (*Config, *StreamConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var savedConfig SavedConfig
	if err := json.Unmarshal(data, &savedConfig); err != nil {
		return nil, nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	return savedConfig.Config, savedConfig.StreamConfig, nil
}

// Metrics tracks the published and consumed messages
type Metrics struct {
	mu sync.RWMutex

	PublishedCount int64
	ConsumedCount  int64
	PublishRate    float64
	ConsumeRate    float64
	lastPublished  int64
	lastConsumed   int64
	lastUpdate     time.Time
}

// Update calculates the current rates
func (m *Metrics) Update() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(m.lastUpdate).Seconds()
	if elapsed == 0 {
		return
	}

	publishedDiff := m.PublishedCount - m.lastPublished
	consumedDiff := m.ConsumedCount - m.lastConsumed

	m.PublishRate = float64(publishedDiff) / elapsed
	m.ConsumeRate = float64(consumedDiff) / elapsed

	m.lastPublished = m.PublishedCount
	m.lastConsumed = m.ConsumedCount
	m.lastUpdate = now
}

// IncrementPublished increments the published count
func (m *Metrics) IncrementPublished() {
	m.mu.Lock()
	m.PublishedCount++
	m.mu.Unlock()
}

// IncrementConsumed increments the consumed count
func (m *Metrics) IncrementConsumed() {
	m.mu.Lock()
	m.ConsumedCount++
	m.mu.Unlock()
}

// GetStats returns the current stats
func (m *Metrics) GetStats() (published, consumed int64, pubRate, conRate float64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.PublishedCount, m.ConsumedCount, m.PublishRate, m.ConsumeRate
}

// MessageTemplate represents a message template loaded from a JSON file
type MessageTemplate struct {
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`

	// Optimized fields for @id@ replacement (parsed once during load)
	hasUUIDPlaceholder bool                // true if body or headers contain @id@
	bodyParts          []string            // split body around @id@ for fast assembly
	headerParts        map[string][]string // split header values around @id@ for fast assembly
	headersWithUUID    []string            // list of header keys that contain @id@
}

// LoadMessageFromFile loads a message template from a JSON file
func LoadMessageFromFile(filePath string) (*MessageTemplate, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read message file: %w", err)
	}

	var msg MessageTemplate
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("failed to parse message JSON: %w", err)
	}

	// Check for @id@ placeholder in body and parse once
	if strings.Contains(msg.Body, "@id@") {
		msg.hasUUIDPlaceholder = true
		msg.bodyParts = strings.Split(msg.Body, "@id@")
	}

	// Check for @id@ placeholder in headers and parse once
	msg.headerParts = make(map[string][]string)
	for key, value := range msg.Headers {
		if strings.Contains(value, "@id@") {
			msg.hasUUIDPlaceholder = true
			msg.headerParts[key] = strings.Split(value, "@id@")
			msg.headersWithUUID = append(msg.headersWithUUID, key)
		}
	}

	return &msg, nil
}

// GetBodyAndHeaders returns the message body and headers, replacing @id@ with a UUID if present
// This method generates one UUID and uses it for all replacements in both body and headers
func (mt *MessageTemplate) GetBodyAndHeaders() (string, map[string]string) {
	if !mt.hasUUIDPlaceholder {
		return mt.Body, mt.Headers
	}

	// Generate one UUID for this message (used for all @id@ occurrences)
	id := uuid.New().String()

	// Assemble body from pre-parsed parts
	body := mt.Body
	if mt.bodyParts != nil {
		body = strings.Join(mt.bodyParts, id)
	}

	// Assemble headers - copy base headers and replace UUID placeholders
	headers := make(map[string]string, len(mt.Headers))
	for key, value := range mt.Headers {
		if parts, hasUUID := mt.headerParts[key]; hasUUID {
			headers[key] = strings.Join(parts, id)
		} else {
			headers[key] = value
		}
	}

	return body, headers
}

// ConnectionState represents the state of a connection
type ConnectionState struct {
	ID     int
	Active bool
	Type   ConnectionType
}
