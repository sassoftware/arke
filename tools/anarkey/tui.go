package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/sassoftware/arke/api"
)

// Color styles
var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#7D56F4")).
			Background(lipgloss.Color("#282828")).
			Padding(0, 1)

	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#00D7FF"))

	boxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#874BFD")).
			Padding(0, 1)

	cursorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF5F87")).
			Bold(true)

	labelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FAFAFA"))

	valueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00FF87")).
			Bold(true)

	metricLabelStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#00D7FF"))

	metricValueStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFD700")).
				Bold(true)

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF0000")).
			Bold(true)

	subtleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#626262"))
)

// Screen represents different screens in the TUI
type Screen int

const (
	ScreenArkeConfig Screen = iota
	ScreenBrokerConfig
	ScreenConnectionConfig
	ScreenStreamConfig
	ScreenRunning
)

// Model represents the bubbletea model
type Model struct {
	screen       Screen
	config       *Config
	streamConfig *StreamConfig
	connManager  *ConnectionManager
	metrics      *Metrics

	// UI state
	cursor  int
	editing bool // true when editing a field
	inputs  map[string]string
	err     error
	width   int
	height  int

	// Running state
	ctx    context.Context
	cancel context.CancelFunc
}

// tickMsg is sent periodically to update metrics
type tickMsg time.Time

// errMsg is sent when an error occurs
type errMsg struct{ err error }

func (e errMsg) Error() string { return e.err.Error() }

// NewModel creates a new bubbletea model
func NewModel() *Model {
	ctx, cancel := context.WithCancel(context.Background())
	metrics := &Metrics{
		lastUpdate: time.Now(),
	}

	return &Model{
		screen: ScreenArkeConfig,
		config: &Config{
			BrokerProvider:          "amqp091",
			NumPublisherConnections: 1,
			NumConsumerConnections:  1,
		},
		streamConfig: &StreamConfig{
			NumPublisherStreams: 1,
			NumConsumerStreams:  1,
			SourceType:          api.Source_QUEUE,
			MessageCount:        10000,
		},
		metrics: metrics,
		inputs:  make(map[string]string),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// NewModelWithConfig creates a new bubbletea model with pre-loaded configuration
func NewModelWithConfig(config *Config, streamConfig *StreamConfig) *Model {
	ctx, cancel := context.WithCancel(context.Background())
	metrics := &Metrics{
		lastUpdate: time.Now(),
	}

	model := &Model{
		screen:       ScreenRunning, // Start at running screen immediately
		config:       config,
		streamConfig: streamConfig,
		metrics:      metrics,
		inputs:       make(map[string]string),
		ctx:          ctx,
		cancel:       cancel,
		cursor:       0,
		editing:      false,
	}

	// Initialize connection manager and start the load test
	Logger.Printf("=== Starting Load Test (from config file) ===")
	Logger.Printf("Arke: %s:%d (TLS: %v)", config.ArkeHost, config.ArkePort, config.ArkeTLS)
	Logger.Printf("Broker: %s:%d (TLS: %v)", config.BrokerHost, config.BrokerPort, config.BrokerTLS)
	Logger.Printf("Publisher Connections: %d, Consumer Connections: %d",
		config.NumPublisherConnections, config.NumConsumerConnections)
	Logger.Printf("Publisher Streams/Conn: %d, Consumer Streams/Conn: %d",
		streamConfig.NumPublisherStreams, streamConfig.NumConsumerStreams)
	Logger.Printf("Source: %s (%v), Rate Limit: %d msg/s",
		streamConfig.SourceName, streamConfig.SourceType, streamConfig.PublishRateLimit)

	// Create connection manager
	model.connManager = NewConnectionManager(config, metrics)

	// Create publisher connections (each creates its own TCP connection to Arke)
	for i := 0; i < config.NumPublisherConnections; i++ {
		if err := model.connManager.CreateConnection(ctx, ConnectionTypePublisher); err != nil {
			Logger.Printf("Error creating publisher connection: %v", err)
			model.err = err
		}
	}

	// Create consumer connections (each creates its own TCP connection to Arke)
	for i := 0; i < config.NumConsumerConnections; i++ {
		if err := model.connManager.CreateConnection(ctx, ConnectionTypeConsumer); err != nil {
			Logger.Printf("Error creating consumer connection: %v", err)
			model.err = err
		}
	}

	// Add streams to each connection
	for _, conn := range model.connManager.GetConnections() {
		if conn.Type == ConnectionTypePublisher {
			if streamConfig.NumPublisherStreams > 0 {
				cfg := &StreamConfig{
					NumPublisherStreams: streamConfig.NumPublisherStreams,
					SourceType:          streamConfig.SourceType,
					SourceName:          streamConfig.SourceName,
					AddressName:         streamConfig.AddressName,
					Subject:             streamConfig.Subject,
					MessageCount:        streamConfig.MessageCount,
					IsContinuous:        streamConfig.IsContinuous,
					MessageFile:         streamConfig.MessageFile,
					PublishRateLimit:    streamConfig.PublishRateLimit,
				}
				if err := conn.AddPublishers(cfg); err != nil {
					Logger.Printf("Error adding publishers: %v", err)
					model.err = err
				}
			}
		} else {
			if streamConfig.NumConsumerStreams > 0 {
				cfg := &StreamConfig{
					NumConsumerStreams: streamConfig.NumConsumerStreams,
					SourceType:         streamConfig.SourceType,
					SourceName:         streamConfig.SourceName,
					AddressName:        streamConfig.AddressName,
					Subject:            streamConfig.Subject,
				}
				if err := conn.AddConsumers(cfg); err != nil {
					Logger.Printf("Error adding consumers: %v", err)
					model.err = err
				}
			}
		}
	}

	return model
}

// Init initializes the model
func (m *Model) Init() tea.Cmd {
	// If we're starting on the Running screen (loaded from config),
	// begin the metrics ticker immediately
	if m.screen == ScreenRunning {
		return tick()
	}
	return nil
}

// Update handles messages
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKeyPress(msg)

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tickMsg:
		if m.screen == ScreenRunning && m.metrics != nil {
			m.metrics.Update()
			return m, tick()
		}
		return m, nil

	case errMsg:
		m.err = msg.err
		return m, nil
	}

	return m, nil
}

// handleKeyPress handles keyboard input
func (m *Model) handleKeyPress(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		if m.connManager != nil {
			m.connManager.Close()
		}
		m.cancel()
		return m, tea.Quit

	case "up":
		// Only navigate when not editing
		if !m.editing && m.cursor > 0 {
			m.cursor--
		}

	case "down":
		// Only navigate when not editing
		if !m.editing {
			maxCursor := m.getMaxCursor()
			if m.cursor < maxCursor {
				m.cursor++
			}
		}

	case "tab":
		// Jump to Continue/Start button when not editing (only on screens with continue button)
		if !m.editing && m.screen != ScreenRunning {
			m.cursor = m.getMaxCursor()
		}

	case "enter":
		return m.handleEnter()

	case "backspace":
		return m.handleBackspace()

	case "esc":
		// Exit editing mode without saving
		if m.editing {
			m.editing = false
		}

	default:
		// Handle text input only when editing
		return m.handleTextInput(msg.String())
	}

	return m, nil
}

// handleEnter handles the enter key
func (m *Model) handleEnter() (tea.Model, tea.Cmd) {
	switch m.screen {
	case ScreenArkeConfig:
		maxCursor := m.getMaxCursor()
		if m.cursor == maxCursor {
			// Continue button pressed
			if m.validateArkeConfig() {
				m.screen = ScreenBrokerConfig
				m.cursor = 0
				m.editing = false
			}
		} else {
			// Toggle editing mode for the current field
			m.editing = !m.editing
		}

	case ScreenBrokerConfig:
		maxCursor := m.getMaxCursor()
		if m.cursor == maxCursor {
			// Continue button pressed
			if m.validateBrokerConfig() {
				m.screen = ScreenConnectionConfig
				m.cursor = 0
				m.editing = false
			}
		} else {
			// Toggle editing mode for the current field
			m.editing = !m.editing
		}

	case ScreenConnectionConfig:
		maxCursor := m.getMaxCursor()
		if m.cursor == maxCursor {
			// Continue button pressed
			if m.validateConnectionConfig() {
				m.screen = ScreenStreamConfig
				m.cursor = 0
				m.editing = false
			}
		} else {
			// Toggle editing mode for the current field
			m.editing = !m.editing
		}

	case ScreenStreamConfig:
		maxCursor := m.getMaxCursor()
		if m.cursor == maxCursor {
			// Start button pressed
			if m.validateStreamConfig() {
				m.editing = false
				return m, m.startLoad()
			}
		} else {
			// Toggle editing mode for the current field
			m.editing = !m.editing
		}

	case ScreenRunning:
		// Running screen has no continue button, all cursor positions are editable fields
		// Toggle editing mode and apply changes when exiting edit mode
		if m.editing {
			// Exiting edit mode - apply the change
			m.editing = false
			return m.handleRunningCommand()
		} else {
			// Entering edit mode
			m.editing = true
		}
	}

	return m, nil
}

// handleBackspace handles the backspace key
func (m *Model) handleBackspace() (tea.Model, tea.Cmd) {
	if !m.editing {
		return m, nil
	}
	field := m.getCurrentField()
	if field != "" {
		current := m.inputs[field]
		if len(current) > 0 {
			m.inputs[field] = current[:len(current)-1]
		}
	}
	return m, nil
}

// getMaxCursor returns the maximum cursor position for the current screen
func (m *Model) getMaxCursor() int {
	switch m.screen {
	case ScreenArkeConfig:
		return 3 // arke_host, arke_port, arke_tls, + Continue button
	case ScreenBrokerConfig:
		return 5 // broker_host, broker_port, broker_username, broker_password, broker_tls, + Continue button
	case ScreenConnectionConfig:
		return 2 // num_publisher_connections, num_consumer_connections, + Continue button
	case ScreenStreamConfig:
		return 10 // num_publisher_streams, num_consumer_streams, source_type, source_name, address_name, subject, message_count, publish_rate_limit, message_file, save_config_file, + Start button
	case ScreenRunning:
		return 4 // runtime_publisher_connections, runtime_consumer_connections, runtime_publisher_streams, runtime_consumer_streams, runtime_publish_rate_limit (no continue button)
	}
	return 0
}

// handleTextInput handles text input
func (m *Model) handleTextInput(s string) (tea.Model, tea.Cmd) {
	if !m.editing {
		return m, nil
	}
	if len(s) == 1 {
		field := m.getCurrentField()
		if field != "" {
			m.inputs[field] += s
		}
	}
	return m, nil
}

// getCurrentField returns the current input field based on cursor position
func (m *Model) getCurrentField() string {
	switch m.screen {
	case ScreenArkeConfig:
		fields := []string{"arke_host", "arke_port", "arke_tls"}
		if m.cursor < len(fields) {
			return fields[m.cursor]
		}
	case ScreenBrokerConfig:
		fields := []string{"broker_host", "broker_port", "broker_username", "broker_password", "broker_tls"}
		if m.cursor < len(fields) {
			return fields[m.cursor]
		}
	case ScreenConnectionConfig:
		fields := []string{"num_publisher_connections", "num_consumer_connections"}
		if m.cursor < len(fields) {
			return fields[m.cursor]
		}
	case ScreenStreamConfig:
		fields := []string{"num_publisher_streams", "num_consumer_streams", "source_type", "source_name", "address_name", "subject", "message_count", "publish_rate_limit", "message_file", "save_config_file"}
		if m.cursor < len(fields) {
			return fields[m.cursor]
		}
	case ScreenRunning:
		fields := []string{"runtime_publisher_connections", "runtime_consumer_connections", "runtime_publisher_streams", "runtime_consumer_streams", "runtime_publish_rate_limit"}
		if m.cursor < len(fields) {
			return fields[m.cursor]
		}
	}
	return ""
}

// validateArkeConfig validates the arke configuration
func (m *Model) validateArkeConfig() bool {
	m.config.ArkeHost = m.getInput("arke_host", "localhost")
	m.config.ArkePort = m.getInputInt("arke_port", 50051)
	m.config.ArkeTLS = m.getInput("arke_tls", "false") == "true"
	return true
}

// validateBrokerConfig validates the broker configuration
func (m *Model) validateBrokerConfig() bool {
	m.config.BrokerHost = m.getInput("broker_host", "localhost")
	m.config.BrokerPort = m.getInputInt("broker_port", 5672)
	m.config.BrokerUsername = m.getInput("broker_username", "guest")
	m.config.BrokerPassword = m.getInput("broker_password", "guest")
	m.config.BrokerTLS = m.getInput("broker_tls", "false") == "true"
	return true
}

// validateConnectionConfig validates the connection configuration
func (m *Model) validateConnectionConfig() bool {
	m.config.NumPublisherConnections = m.getInputInt("num_publisher_connections", 1)
	m.config.NumConsumerConnections = m.getInputInt("num_consumer_connections", 1)
	return true
}

// validateStreamConfig validates the stream configuration
func (m *Model) validateStreamConfig() bool {
	m.streamConfig.NumPublisherStreams = m.getInputInt("num_publisher_streams", 1)
	m.streamConfig.NumConsumerStreams = m.getInputInt("num_consumer_streams", 1)
	sourceType := m.getInput("source_type", "queue")
	if sourceType == "stream" {
		m.streamConfig.SourceType = api.Source_STREAM
	} else {
		m.streamConfig.SourceType = api.Source_QUEUE
	}
	m.streamConfig.SourceName = m.getInput("source_name", "test-source")
	m.streamConfig.AddressName = m.getInput("address_name", "test-address")
	m.streamConfig.Subject = m.getInput("subject", "test.routing.key")
	msgCount := m.getInputInt("message_count", 10000)
	if msgCount == 0 {
		m.streamConfig.IsContinuous = true
		m.streamConfig.MessageCount = 0
	} else {
		m.streamConfig.IsContinuous = false
		m.streamConfig.MessageCount = msgCount
	}
	m.streamConfig.PublishRateLimit = m.getInputInt("publish_rate_limit", 0)
	m.streamConfig.MessageFile = m.getInput("message_file", "")

	// Save config if filename provided
	saveFile := m.getInput("save_config_file", "")
	if saveFile != "" {
		if err := SaveConfig(saveFile, m.config, m.streamConfig); err != nil {
			Logger.Printf("Failed to save config: %v", err)
			m.err = fmt.Errorf("failed to save config: %w", err)
			return false
		}
		Logger.Printf("Configuration saved to %s", saveFile)
	}

	return true
}

// getInput gets an input value or returns a default
func (m *Model) getInput(key, defaultVal string) string {
	if val, ok := m.inputs[key]; ok && val != "" {
		return val
	}
	return defaultVal
}

// getInputInt gets an input value as int or returns a default
func (m *Model) getInputInt(key string, defaultVal int) int {
	if val, ok := m.inputs[key]; ok && val != "" {
		var result int
		fmt.Sscanf(val, "%d", &result)
		if result >= 0 {
			return result
		}
	}
	return defaultVal
}

// startLoad starts the load test
func (m *Model) startLoad() tea.Cmd {
	return func() tea.Msg {
		Logger.Printf("=== Starting Load Test ===")
		Logger.Printf("Arke: %s:%d (TLS: %v)", m.config.ArkeHost, m.config.ArkePort, m.config.ArkeTLS)
		Logger.Printf("Broker: %s:%d (TLS: %v)", m.config.BrokerHost, m.config.BrokerPort, m.config.BrokerTLS)
		Logger.Printf("Publisher Connections: %d, Consumer Connections: %d",
			m.config.NumPublisherConnections, m.config.NumConsumerConnections)
		Logger.Printf("Publisher Streams/Conn: %d, Consumer Streams/Conn: %d",
			m.streamConfig.NumPublisherStreams, m.streamConfig.NumConsumerStreams)
		Logger.Printf("Source: %s (%v), Rate Limit: %d msg/s",
			m.streamConfig.SourceName, m.streamConfig.SourceType, m.streamConfig.PublishRateLimit)

		// Create connection manager
		m.connManager = NewConnectionManager(m.config, m.metrics)

		// Create publisher connections (each creates its own TCP connection to Arke)
		for i := 0; i < m.config.NumPublisherConnections; i++ {
			if err := m.connManager.CreateConnection(m.ctx, ConnectionTypePublisher); err != nil {
				return errMsg{err}
			}
		}

		// Create consumer connections (each creates its own TCP connection to Arke)
		for i := 0; i < m.config.NumConsumerConnections; i++ {
			if err := m.connManager.CreateConnection(m.ctx, ConnectionTypeConsumer); err != nil {
				return errMsg{err}
			}
		}

		// Add streams to each connection
		for _, conn := range m.connManager.GetConnections() {
			if conn.Type == ConnectionTypePublisher {
				if m.streamConfig.NumPublisherStreams > 0 {
					config := &StreamConfig{
						NumPublisherStreams: m.streamConfig.NumPublisherStreams,
						SourceType:          m.streamConfig.SourceType,
						SourceName:          m.streamConfig.SourceName,
						AddressName:         m.streamConfig.AddressName,
						Subject:             m.streamConfig.Subject,
						MessageCount:        m.streamConfig.MessageCount,
						IsContinuous:        m.streamConfig.IsContinuous,
						MessageFile:         m.streamConfig.MessageFile,
					}
					if err := conn.AddPublishers(config); err != nil {
						return errMsg{err}
					}
				}
			} else {
				if m.streamConfig.NumConsumerStreams > 0 {
					config := &StreamConfig{
						NumConsumerStreams: m.streamConfig.NumConsumerStreams,
						SourceType:         m.streamConfig.SourceType,
						SourceName:         m.streamConfig.SourceName,
						AddressName:        m.streamConfig.AddressName,
						Subject:            m.streamConfig.Subject,
					}
					if err := conn.AddConsumers(config); err != nil {
						return errMsg{err}
					}
				}
			}
		}

		m.screen = ScreenRunning
		m.cursor = 0 // Reset cursor for running screen
		m.editing = false
		return tick()()
	}
}

// handleRunningCommand handles commands in running mode
func (m *Model) handleRunningCommand() (tea.Model, tea.Cmd) {
	field := m.getCurrentField()
	if field == "" {
		return m, nil
	}

	switch field {
	case "runtime_publisher_connections":
		numConns := m.getInputInt("runtime_publisher_connections", m.config.NumPublisherConnections)
		if numConns != m.config.NumPublisherConnections {
			oldCount := m.config.NumPublisherConnections
			m.config.NumPublisherConnections = numConns
			if err := m.connManager.ScaleConnections(m.ctx, numConns, ConnectionTypePublisher); err != nil {
				m.err = err
			} else {
				// If we added connections, add streams to them
				if numConns > oldCount && m.streamConfig.NumPublisherStreams > 0 {
					for _, conn := range m.connManager.GetConnections() {
						if conn.Type == ConnectionTypePublisher && len(conn.publishers) == 0 {
							config := &StreamConfig{
								NumPublisherStreams: m.streamConfig.NumPublisherStreams,
								SourceType:          m.streamConfig.SourceType,
								SourceName:          m.streamConfig.SourceName,
								AddressName:         m.streamConfig.AddressName,
								Subject:             m.streamConfig.Subject,
								MessageCount:        m.streamConfig.MessageCount,
								IsContinuous:        m.streamConfig.IsContinuous,
								PublishRateLimit:    m.streamConfig.PublishRateLimit,
								MessageFile:         m.streamConfig.MessageFile,
							}
							if err := conn.AddPublishers(config); err != nil {
								m.err = err
							}
						}
					}
				}
			}
			m.inputs["runtime_publisher_connections"] = ""
		}

	case "runtime_consumer_connections":
		numConns := m.getInputInt("runtime_consumer_connections", m.config.NumConsumerConnections)
		if numConns != m.config.NumConsumerConnections {
			oldCount := m.config.NumConsumerConnections
			m.config.NumConsumerConnections = numConns
			if err := m.connManager.ScaleConnections(m.ctx, numConns, ConnectionTypeConsumer); err != nil {
				m.err = err
			} else {
				// If we added connections, add streams to them
				if numConns > oldCount && m.streamConfig.NumConsumerStreams > 0 {
					for _, conn := range m.connManager.GetConnections() {
						if conn.Type == ConnectionTypeConsumer && len(conn.consumers) == 0 {
							config := &StreamConfig{
								NumConsumerStreams: m.streamConfig.NumConsumerStreams,
								SourceType:         m.streamConfig.SourceType,
								SourceName:         m.streamConfig.SourceName,
								AddressName:        m.streamConfig.AddressName,
								Subject:            m.streamConfig.Subject,
							}
							if err := conn.AddConsumers(config); err != nil {
								m.err = err
							}
						}
					}
				}
			}
			m.inputs["runtime_consumer_connections"] = ""
		}

	case "runtime_publisher_streams":
		numStreams := m.getInputInt("runtime_publisher_streams", m.streamConfig.NumPublisherStreams)
		if numStreams != m.streamConfig.NumPublisherStreams {
			m.streamConfig.NumPublisherStreams = numStreams
			// Scale all publisher connections
			for _, conn := range m.connManager.GetConnections() {
				if conn.Type == ConnectionTypePublisher {
					config := &StreamConfig{
						NumPublisherStreams: numStreams,
						SourceType:          m.streamConfig.SourceType,
						SourceName:          m.streamConfig.SourceName,
						AddressName:         m.streamConfig.AddressName,
						Subject:             m.streamConfig.Subject,
						MessageCount:        m.streamConfig.MessageCount,
						IsContinuous:        m.streamConfig.IsContinuous,
						PublishRateLimit:    m.streamConfig.PublishRateLimit,
						MessageFile:         m.streamConfig.MessageFile,
					}
					if err := conn.ScalePublishers(config); err != nil {
						m.err = err
					}
				}
			}
			m.inputs["runtime_publisher_streams"] = ""
		}

	case "runtime_consumer_streams":
		numStreams := m.getInputInt("runtime_consumer_streams", m.streamConfig.NumConsumerStreams)
		if numStreams != m.streamConfig.NumConsumerStreams {
			m.streamConfig.NumConsumerStreams = numStreams
			// Scale all consumer connections
			for _, conn := range m.connManager.GetConnections() {
				if conn.Type == ConnectionTypeConsumer {
					config := &StreamConfig{
						NumConsumerStreams: numStreams,
						SourceType:         m.streamConfig.SourceType,
						SourceName:         m.streamConfig.SourceName,
						AddressName:        m.streamConfig.AddressName, Subject: m.streamConfig.Subject}
					if err := conn.ScaleConsumers(config); err != nil {
						m.err = err
					}
				}
			}
			m.inputs["runtime_consumer_streams"] = ""
		}

	case "runtime_publish_rate_limit":
		rateLimit := m.getInputInt("runtime_publish_rate_limit", m.streamConfig.PublishRateLimit)
		if rateLimit != m.streamConfig.PublishRateLimit {
			m.streamConfig.PublishRateLimit = rateLimit
			// Restart all publishers with new rate limit
			for _, conn := range m.connManager.GetConnections() {
				if conn.Type == ConnectionTypePublisher {
					config := &StreamConfig{
						NumPublisherStreams: m.streamConfig.NumPublisherStreams,
						SourceType:          m.streamConfig.SourceType,
						SourceName:          m.streamConfig.SourceName,
						AddressName:         m.streamConfig.AddressName,
						Subject:             m.streamConfig.Subject,
						MessageCount:        m.streamConfig.MessageCount,
						IsContinuous:        m.streamConfig.IsContinuous,
						PublishRateLimit:    rateLimit,
						MessageFile:         m.streamConfig.MessageFile,
					}
					if err := conn.ScalePublishers(config); err != nil {
						m.err = err
					}
				}
			}
			m.inputs["runtime_publish_rate_limit"] = ""
		}
	}

	return m, nil
}

// tick returns a command that sends a tick message
func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// View renders the UI
func (m *Model) View() string {
	switch m.screen {
	case ScreenArkeConfig:
		return m.renderArkeConfig()
	case ScreenBrokerConfig:
		return m.renderBrokerConfig()
	case ScreenConnectionConfig:
		return m.renderConnectionConfig()
	case ScreenStreamConfig:
		return m.renderStreamConfig()
	case ScreenRunning:
		return m.renderRunning()
	}
	return ""
}

// renderArkeConfig renders the arke configuration screen
func (m *Model) renderArkeConfig() string {
	var b strings.Builder
	title := titleStyle.Render("⚡ Arke Load Tool - Arke Server")
	b.WriteString(title + "\n\n")

	type field struct {
		name       string
		key        string
		prompt     string
		defaultVal string
	}

	fields := []field{
		{"Arke Hostname", "arke_host", "Arke hostname", "localhost"},
		{"Arke Port", "arke_port", "Arke port", "50051"},
		{"Arke TLS", "arke_tls", "TLS (true/false)", "false"},
	}

	for i, f := range fields {
		var cursor string
		if m.cursor == i {
			if m.editing {
				cursor = cursorStyle.Render("✎ ")
			} else {
				cursor = cursorStyle.Render("❯ ")
			}
		} else {
			cursor = "  "
		}
		value := m.getInput(f.key, f.defaultVal)
		label := labelStyle.Render(f.prompt + ":")
		val := valueStyle.Render(value)
		b.WriteString(fmt.Sprintf("%s%s %s\n", cursor, label, val))
	}

	// Add Continue button
	b.WriteString("\n")
	var continueButton string
	if m.cursor == len(fields) {
		continueButton = cursorStyle.Render("❯ ") + valueStyle.Render("[Continue]")
	} else {
		continueButton = "  " + labelStyle.Render("[Continue]")
	}
	b.WriteString(continueButton + "\n")

	if m.err != nil {
		b.WriteString("\n" + errorStyle.Render(fmt.Sprintf("Error: %v", m.err)) + "\n")
	}

	b.WriteString("\n" + subtleStyle.Render("↑/↓: Navigate • Tab: Jump to Continue • Enter: Edit/Select • Esc: Cancel edit • Ctrl+C: Quit") + "\n")
	return b.String()
}

// renderBrokerConfig renders the broker configuration screen
func (m *Model) renderBrokerConfig() string {
	var b strings.Builder
	title := titleStyle.Render("🔌 Arke Load Tool - Broker Config")
	b.WriteString(title + "\n\n")

	type field struct {
		name       string
		key        string
		prompt     string
		defaultVal string
	}

	fields := []field{
		{"Broker Hostname", "broker_host", "Broker hostname", "localhost"},
		{"Broker Port", "broker_port", "Broker port", "5672"},
		{"Username", "broker_username", "Username", "guest"},
		{"Password", "broker_password", "Password", "guest"},
		{"Broker TLS", "broker_tls", "TLS (true/false)", "false"},
	}

	for i, f := range fields {
		var cursor string
		if m.cursor == i {
			if m.editing {
				cursor = cursorStyle.Render("✎ ")
			} else {
				cursor = cursorStyle.Render("❯ ")
			}
		} else {
			cursor = "  "
		}
		value := m.getInput(f.key, f.defaultVal)
		// Mask password
		if f.key == "broker_password" && value != "" {
			value = strings.Repeat("•", len(value))
		}
		label := labelStyle.Render(f.prompt + ":")
		val := valueStyle.Render(value)
		b.WriteString(fmt.Sprintf("%s%s %s\n", cursor, label, val))
	}

	// Add Continue button
	b.WriteString("\n")
	var continueButton string
	if m.cursor == len(fields) {
		continueButton = cursorStyle.Render("❯ ") + valueStyle.Render("[Continue]")
	} else {
		continueButton = "  " + labelStyle.Render("[Continue]")
	}
	b.WriteString(continueButton + "\n")

	if m.err != nil {
		b.WriteString("\n" + errorStyle.Render(fmt.Sprintf("Error: %v", m.err)) + "\n")
	}

	b.WriteString("\n" + subtleStyle.Render("↑/↓: Navigate • Tab: Jump to Continue • Enter: Edit/Select • Esc: Cancel edit • Ctrl+C: Quit") + "\n")
	return b.String()
}

// renderConnectionConfig renders the connection configuration screen
func (m *Model) renderConnectionConfig() string {
	var b strings.Builder
	title := titleStyle.Render("🔗 Arke Load Tool - Connection Config")
	b.WriteString(title + "\n\n")

	type field struct {
		name       string
		key        string
		prompt     string
		defaultVal string
	}

	fields := []field{
		{"Publisher Connections", "num_publisher_connections", "Number of publisher connections", "1"},
		{"Consumer Connections", "num_consumer_connections", "Number of consumer connections", "1"},
	}

	for i, f := range fields {
		cursor := "  "
		if m.cursor == i {
			if m.editing {
				cursor = cursorStyle.Render("✎ ")
			} else {
				cursor = cursorStyle.Render("❯ ")
			}
		} else {
			cursor = "  "
		}
		value := m.getInput(f.key, f.defaultVal)
		label := labelStyle.Render(f.prompt + ":")
		val := valueStyle.Render(value)
		b.WriteString(fmt.Sprintf("%s%s %s\n", cursor, label, val))
	}

	// Add Continue button
	b.WriteString("\n")
	var continueButton string
	if m.cursor == len(fields) {
		continueButton = cursorStyle.Render("❯ ") + valueStyle.Render("[Continue]")
	} else {
		continueButton = "  " + labelStyle.Render("[Continue]")
	}
	b.WriteString(continueButton + "\n")

	if m.err != nil {
		b.WriteString("\n" + errorStyle.Render(fmt.Sprintf("Error: %v", m.err)) + "\n")
	}

	b.WriteString("\n" + subtleStyle.Render("↑/↓: Navigate • Tab: Jump to Continue • Enter: Edit/Select • Esc: Cancel edit • Ctrl+C: Quit") + "\n")
	return b.String()
}

// renderStreamConfig renders the stream configuration screen
func (m *Model) renderStreamConfig() string {
	var b strings.Builder
	title := titleStyle.Render("📊 Arke Load Tool - Stream Config")
	b.WriteString(title + "\n\n")

	type field struct {
		name       string
		key        string
		prompt     string
		defaultVal string
	}

	fields := []field{
		{"Publisher Streams", "num_publisher_streams", "Number of publisher streams per connection", "1"},
		{"Consumer Streams", "num_consumer_streams", "Number of consumer streams per connection", "1"},
		{"Source Type", "source_type", "Source type (queue/stream)", "queue"},
		{"Source Name", "source_name", "Source name", "test-source"},
		{"Address Name", "address_name", "Address name", "test-address"},
		{"Subject", "subject", "Routing key/subject", "test.routing.key"},
		{"Message Count", "message_count", "Messages per stream (0=continuous)", "10000"},
		{"Publish Rate Limit", "publish_rate_limit", "Messages/sec per stream (0=unlimited)", "0"},
		{"Message File", "message_file", "JSON message file (optional)", ""},
		{"Save Config File", "save_config_file", "Save config to file (optional)", ""},
	}

	for i, f := range fields {
		cursor := "  "
		if m.cursor == i {
			if m.editing {
				cursor = cursorStyle.Render("✎ ")
			} else {
				cursor = cursorStyle.Render("❯ ")
			}
		} else {
			cursor = "  "
		}
		value := m.getInput(f.key, f.defaultVal)
		label := labelStyle.Render(f.prompt + ":")
		val := valueStyle.Render(value)
		b.WriteString(fmt.Sprintf("%s%s %s\n", cursor, label, val))
	}

	// Add Start button
	b.WriteString("\n")
	var startButton string
	if m.cursor == len(fields) {
		startButton = cursorStyle.Render("❯ ") + valueStyle.Render("[Start Load Test]")
	} else {
		startButton = "  " + labelStyle.Render("[Start Load Test]")
	}
	b.WriteString(startButton + "\n")

	if m.err != nil {
		b.WriteString("\n" + errorStyle.Render(fmt.Sprintf("Error: %v", m.err)) + "\n")
	}

	b.WriteString("\n" + subtleStyle.Render("↑/↓: Navigate • Tab: Jump to Start • Enter: Edit/Start • Esc: Cancel edit • Ctrl+C: Quit") + "\n")
	return b.String()
}

// renderRunning renders the running screen with metrics
func (m *Model) renderRunning() string {
	var b strings.Builder
	title := titleStyle.Render("🚀 Arke Load Tool - Running")
	b.WriteString(title + "\n\n")

	pub, con, pubRate, conRate := m.metrics.GetStats()

	// Count connections by type
	var publisherConns, consumerConns int
	for _, conn := range m.connManager.GetConnections() {
		if conn.Type == ConnectionTypePublisher {
			publisherConns++
		} else {
			consumerConns++
		}
	}

	// Connection info
	pubConnLabel := metricLabelStyle.Render("Publisher Connections:")
	pubConnValue := metricValueStyle.Render(fmt.Sprintf("%d", publisherConns))
	pubStreamValue := valueStyle.Render(fmt.Sprintf("(Streams per: %d)", m.streamConfig.NumPublisherStreams))
	b.WriteString(fmt.Sprintf("%s %s %s\n", pubConnLabel, pubConnValue, pubStreamValue))

	conConnLabel := metricLabelStyle.Render("Consumer Connections: ")
	conConnValue := metricValueStyle.Render(fmt.Sprintf("%d", consumerConns))
	conStreamValue := valueStyle.Render(fmt.Sprintf("(Streams per: %d)", m.streamConfig.NumConsumerStreams))
	b.WriteString(fmt.Sprintf("%s %s %s\n\n", conConnLabel, conConnValue, conStreamValue))

	// Metrics box
	metricsHeader := headerStyle.Render("📈 Metrics")
	b.WriteString(metricsHeader + "\n")
	b.WriteString(strings.Repeat("─", 40) + "\n")

	pubLabel := metricLabelStyle.Render("Published: ")
	pubValue := metricValueStyle.Render(fmt.Sprintf("%d messages", pub))
	pubRateValue := valueStyle.Render(fmt.Sprintf("(%.2f msg/s)", pubRate))
	b.WriteString(fmt.Sprintf("%s%s %s\n", pubLabel, pubValue, pubRateValue))

	conLabel := metricLabelStyle.Render("Consumed:  ")
	conValue := metricValueStyle.Render(fmt.Sprintf("%d messages", con))
	conRateValue := valueStyle.Render(fmt.Sprintf("(%.2f msg/s)", conRate))
	b.WriteString(fmt.Sprintf("%s%s %s\n\n", conLabel, conValue, conRateValue))

	// Runtime controls
	controlsHeader := headerStyle.Render("⚙️  Runtime Controls")
	b.WriteString(controlsHeader + "\n")
	b.WriteString(strings.Repeat("─", 40) + "\n")

	type field struct {
		name       string
		key        string
		prompt     string
		defaultVal string
	}

	fields := []field{
		{"Publisher Connections", "runtime_publisher_connections", "Publisher connections", fmt.Sprintf("%d", publisherConns)},
		{"Consumer Connections", "runtime_consumer_connections", "Consumer connections", fmt.Sprintf("%d", consumerConns)},
		{"Publisher Streams", "runtime_publisher_streams", "Publisher streams/conn", fmt.Sprintf("%d", m.streamConfig.NumPublisherStreams)},
		{"Consumer Streams", "runtime_consumer_streams", "Consumer streams/conn", fmt.Sprintf("%d", m.streamConfig.NumConsumerStreams)},
		{"Publish Rate Limit", "runtime_publish_rate_limit", "Msgs/sec per stream (0=unlimited)", fmt.Sprintf("%d", m.streamConfig.PublishRateLimit)},
	}

	for i, f := range fields {
		cursor := "  "
		if m.cursor == i {
			if m.editing {
				cursor = cursorStyle.Render("✎ ")
			} else {
				cursor = cursorStyle.Render("❯ ")
			}
		} else {
			cursor = "  "
		}
		value := m.getInput(f.key, f.defaultVal)
		label := labelStyle.Render(f.prompt + ":")
		val := valueStyle.Render(value)
		b.WriteString(fmt.Sprintf("%s%s %s\n", cursor, label, val))
	}

	if m.err != nil {
		b.WriteString("\n" + errorStyle.Render(fmt.Sprintf("Error: %v", m.err)) + "\n")
	}

	b.WriteString("\n" + subtleStyle.Render("↑/↓: Navigate • Enter: Edit/Apply • Esc: Cancel edit • Ctrl+C: Quit") + "\n")
	return b.String()
}
