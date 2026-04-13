package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/sassoftware/arke/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ConnectionManager manages multiple connections to the arke server
type ConnectionManager struct {
	config      *Config
	connections []*Connection
	mu          sync.RWMutex
	grpcConn    *grpc.ClientConn
	metrics     *Metrics
}

// Connection represents a single connection to arke
type Connection struct {
	ID             int
	Type           ConnectionType
	grpcConn       *grpc.ClientConn
	ProducerClient api.ProducerClient
	ConsumerClient api.ConsumerClient
	config         *Config
	metrics        *Metrics
	publishers     []*Publisher
	consumers      []*Consumer
	mu             sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(config *Config, metrics *Metrics) *ConnectionManager {
	return &ConnectionManager{
		config:      config,
		connections: make([]*Connection, 0),
		metrics:     metrics,
	}
}

// createGRPCConnection establishes a new gRPC connection to the arke server
func (cm *ConnectionManager) createGRPCConnection() (*grpc.ClientConn, error) {
	address := fmt.Sprintf("%s:%d", cm.config.ArkeHost, cm.config.ArkePort)

	var opts []grpc.DialOption
	if cm.config.ArkeTLS {
		// Skip certificate verification for development tool
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to arke server: %w", err)
	}

	Logger.Printf("Established new gRPC connection to arke server at %s", address)
	return conn, nil
}

// CreateConnection creates a new connection
func (cm *ConnectionManager) CreateConnection(ctx context.Context, connType ConnectionType) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	Logger.Printf("Creating %v connection to broker %s:%d", connType, cm.config.BrokerHost, cm.config.BrokerPort)

	// Each connection needs its own TCP connection to Arke
	grpcConn, err := cm.createGRPCConnection()
	if err != nil {
		Logger.Printf("Failed to create gRPC connection: %v", err)
		return fmt.Errorf("failed to create gRPC connection: %w", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	conn := &Connection{
		ID:             len(cm.connections),
		Type:           connType,
		grpcConn:       grpcConn,
		ProducerClient: api.NewProducerClient(grpcConn),
		ConsumerClient: api.NewConsumerClient(grpcConn),
		config:         cm.config,
		metrics:        cm.metrics,
		publishers:     make([]*Publisher, 0),
		consumers:      make([]*Consumer, 0),
		ctx:            connCtx,
		cancel:         cancel,
	}

	Logger.Printf("Connection %d: created with type %v", conn.ID, connType)

	// Connect to broker
	connConfig := &api.ConnectionConfiguration{
		Host:     cm.config.BrokerHost,
		Port:     int32(cm.config.BrokerPort),
		Provider: cm.config.BrokerProvider,
		Tls:      cm.config.BrokerTLS,
		Credentials: &api.Credentials{
			Username: cm.config.BrokerUsername,
			Password: cm.config.BrokerPassword,
		},
	}

	if connType == ConnectionTypePublisher {
		Logger.Printf("Connection %d: connecting as PRODUCER", conn.ID)
		resp, err := conn.ProducerClient.Connect(ctx, connConfig)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to connect producer: %w", err)
		}
		if !resp.Success {
			cancel()
			return fmt.Errorf("producer connection failed: %s", resp.Error.Message)
		}
	} else {
		Logger.Printf("Connection %d: connecting as CONSUMER", conn.ID)
		resp, err := conn.ConsumerClient.Connect(ctx, connConfig)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to connect consumer: %w", err)
		}
		if !resp.Success {
			cancel()
			return fmt.Errorf("consumer connection failed: %s", resp.Error.Message)
		}
	}

	cm.connections = append(cm.connections, conn)

	Logger.Printf("Connection %d (%v) created successfully", conn.ID, connType)

	return nil
}

// ScaleConnections scales the number of connections
func (cm *ConnectionManager) ScaleConnections(ctx context.Context, count int, connType ConnectionType) error {
	Logger.Printf("Scaling %v connections to %d", connType, count)

	cm.mu.Lock()
	// Count existing connections of this type
	var existingConns []*Connection
	var otherConns []*Connection
	for _, conn := range cm.connections {
		if conn.Type == connType {
			Logger.Printf("ScaleConnections: found existing %v connection (ID: %d)", connType, conn.ID)
			existingConns = append(existingConns, conn)
		} else {
			Logger.Printf("ScaleConnections: found other type %v connection (ID: %d)", conn.Type, conn.ID)
			otherConns = append(otherConns, conn)
		}
	}
	currentCount := len(existingConns)
	Logger.Printf("ScaleConnections: current %v count: %d, target: %d", connType, currentCount, count)
	cm.mu.Unlock()

	if count > currentCount {
		// Add connections
		for i := currentCount; i < count; i++ {
			if err := cm.CreateConnection(ctx, connType); err != nil {
				return err
			}
		}
	} else if count < currentCount {
		// Remove connections of this type from the end
		cm.mu.Lock()
		for i := count; i < currentCount; i++ {
			if existingConns[i] != nil {
				existingConns[i].Close()
			}
		}
		// Rebuild connections list with remaining connections of this type + all other type connections
		cm.connections = append(existingConns[:count], otherConns...)
		cm.mu.Unlock()
	}

	return nil
}

// GetConnections returns all connections
func (cm *ConnectionManager) GetConnections() []*Connection {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.connections
}

// Close closes all connections
func (cm *ConnectionManager) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for _, conn := range cm.connections {
		if conn != nil {
			conn.Close()
		}
	}
}

// Close closes the connection
func (c *Connection) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Stop all publishers and consumers
	for _, p := range c.publishers {
		p.Stop()
	}
	for _, cons := range c.consumers {
		cons.Stop()
	}

	// Disconnect
	if c.Type == ConnectionTypePublisher && c.ProducerClient != nil {
		c.ProducerClient.Disconnect(c.ctx, &api.Empty{})
	} else if c.Type == ConnectionTypeConsumer && c.ConsumerClient != nil {
		c.ConsumerClient.Disconnect(c.ctx, &api.Empty{})
	}

	// Close the gRPC connection
	if c.grpcConn != nil {
		c.grpcConn.Close()
	}

	c.cancel()
}
