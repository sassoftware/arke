// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package ratelimiter

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/sassoftware/arke/api"
	"github.com/sassoftware/arke/internal/util"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/peer"
)

func resetRateLimitEnv() {
	_ = os.Unsetenv(EnvRateLimitEnforced)
	_ = os.Unsetenv(EnvRateLimitBucketSize)
	_ = os.Unsetenv(EnvRateLimitRefill)
	_ = os.Unsetenv(EnvRateLimitMaxAge)
	os.Setenv(util.EnvLogLevel, "DEBUG")
}

func TestInitializeClientLimitManager(t *testing.T) {
	tests := []struct {
		name                       string
		bucketSize                 int
		refillInterval             time.Duration
		maxAgeStaleClients         time.Duration
		enforced                   bool
		shouldBeNil                bool
		expectedBucketSize         int
		expectedFillInterval       time.Duration
		expectedMaxAgeStaleClients time.Duration
	}{
		{
			name:                       "ZeroValues",
			bucketSize:                 0,
			refillInterval:             0,
			maxAgeStaleClients:         0,
			enforced:                   true,
			shouldBeNil:                true,
			expectedBucketSize:         20,
			expectedFillInterval:       1 * time.Second,
			expectedMaxAgeStaleClients: 5 * time.Minute,
		},
		{
			name:                       "NonZeroValues",
			bucketSize:                 30,
			refillInterval:             30 * time.Second,
			maxAgeStaleClients:         30 * time.Minute,
			enforced:                   true,
			shouldBeNil:                false,
			expectedBucketSize:         30,
			expectedFillInterval:       30 * time.Second,
			expectedMaxAgeStaleClients: 30 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetRateLimitEnv()
			clm, err := NewClientLimitManager(tt.bucketSize, tt.refillInterval, tt.maxAgeStaleClients, tt.enforced)
			if tt.shouldBeNil {
				assert.NotNil(t, err)
				assert.Nil(t, clm)
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, clm)
				if clm.bucketSize != tt.expectedBucketSize {
					t.Errorf("InitializeClientLimitManager() bucketSize = %d; want %d", clm.bucketSize, tt.expectedBucketSize)
				}
				if clm.fillInterval != tt.expectedFillInterval {
					t.Errorf("InitializeClientLimitManager() fillInterval = %v; want %v", clm.fillInterval, tt.expectedFillInterval)
				}
				if clm.maxAgeStaleClients != tt.expectedMaxAgeStaleClients {
					t.Errorf("InitializeClientLimitManager() maxAgeStaleClients = %v; want %v", clm.maxAgeStaleClients, tt.expectedMaxAgeStaleClients)
				}
			}
		})
	}
}

func TestCullStaleClients(t *testing.T) {
	tests := []struct {
		name               string
		bucketSize         int
		refillInterval     time.Duration
		maxAgeStaleClients time.Duration
		enforced           bool
		clientLastActive   []time.Time
		expectedClients    int
	}{
		{
			name:               "NoStaleClients",
			bucketSize:         20,
			refillInterval:     1 * time.Second,
			maxAgeStaleClients: 5 * time.Minute,
			enforced:           true,
			clientLastActive:   []time.Time{time.Now(), time.Now().Add(-1 * time.Minute)},
			expectedClients:    2,
		},
		{
			name:               "SomeStaleClients",
			bucketSize:         20,
			refillInterval:     1 * time.Second,
			maxAgeStaleClients: 5 * time.Minute,
			enforced:           true,
			clientLastActive:   []time.Time{time.Now().Add(-10 * time.Minute), time.Now()},
			expectedClients:    1,
		},
		{
			name:               "AllStaleClients",
			bucketSize:         20,
			refillInterval:     1 * time.Second,
			maxAgeStaleClients: 5 * time.Minute,
			enforced:           true,
			clientLastActive:   []time.Time{time.Now().Add(-10 * time.Minute), time.Now().Add(-6 * time.Minute)},
			expectedClients:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetRateLimitEnv()
			clm, err := NewClientLimitManager(tt.bucketSize, tt.refillInterval, tt.maxAgeStaleClients, tt.enforced)
			assert.Nil(t, err, "unexpected error: %v", err)
			for i, lastActive := range tt.clientLastActive {
				clientID := fmt.Sprintf("client-%d", i)
				clm.clients.Add(clientID, &clientLimiter{
					limiter:            rate.NewLimiter(rate.Every(tt.refillInterval), tt.bucketSize),
					lastConnectionTime: lastActive,
				})
			}

			clm.cullStaleClients()

			if got := clm.clients.Length(); got != tt.expectedClients {
				t.Errorf("cullStaleClients() = %d clients; want %d", got, tt.expectedClients)
			}
		})
	}
}
func TestLimit(t *testing.T) {
	tests := []struct {
		name               string
		useClientAddr      bool
		bucketSize         int
		refillInterval     time.Duration
		maxAgeStaleClients time.Duration
		enforced           bool
		clientRequests     int
		expectedError      error
	}{
		{
			name: "NoLimit",
			// no client id, so it can't be tracked
			bucketSize:         20,
			refillInterval:     1 * time.Second,
			maxAgeStaleClients: 5 * time.Minute,
			enforced:           true,
			clientRequests:     200,
			expectedError:      nil,
		},
		{
			name:               "WithinLimit",
			useClientAddr:      true,
			bucketSize:         20,
			refillInterval:     1 * time.Second,
			maxAgeStaleClients: 5 * time.Minute,
			enforced:           true,
			clientRequests:     19,
			expectedError:      nil,
		},
		{
			name:               "ExceedLimit",
			useClientAddr:      true,
			bucketSize:         100,
			refillInterval:     10 * time.Second,
			maxAgeStaleClients: 5 * time.Minute,
			enforced:           true,
			clientRequests:     200,
			expectedError:      ErrTooManyRequests,
		},
	}

	for testIndex, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetRateLimitEnv()
			clm, err := NewClientLimitManager(tt.bucketSize, tt.refillInterval, tt.maxAgeStaleClients, tt.enforced)
			assert.Nil(t, err, "unexpected error: %v", err)
			ctx := context.Background()
			clientIdentifier := "anonymous"
			if tt.useClientAddr {
				clientAddr := fmt.Sprintf("1.1.1.%d", testIndex)
				ctx = newPeerContext(clientAddr)
				clientIdentifier, err = util.SetClientIdentifier(ctx, clientAddr)
				assert.Nil(t, err, "unexpected error setting client id %s: %v", clientAddr, err)
			}
			t.Logf("Testing rate limits for client %s", clientIdentifier)
			for i := 0; i < tt.clientRequests; i++ {
				err = clm.Limit(ctx)
			}

			if err != tt.expectedError {
				t.Errorf("Limit() error = %v; want %v", err, tt.expectedError)
			}
		})
	}
}
func TestStartClientCull(t *testing.T) {
	tests := []struct {
		name               string
		bucketSize         int
		refillInterval     time.Duration
		maxAgeStaleClients time.Duration
		enforced           bool
		clientLastActive   []time.Time
		expectedClients    int
	}{
		{
			name:               "NoStaleClients",
			bucketSize:         20,
			refillInterval:     1 * time.Second,
			maxAgeStaleClients: 5 * time.Second,
			enforced:           true,
			clientLastActive:   []time.Time{time.Now().Add(3 * time.Second), time.Now().Add(5 * time.Second)},
			expectedClients:    2,
		},
		{
			name:               "SomeStaleClients",
			bucketSize:         20,
			refillInterval:     1 * time.Second,
			maxAgeStaleClients: 5 * time.Second,
			enforced:           true,
			clientLastActive:   []time.Time{time.Now().Add(-10 * time.Second), time.Now().Add(8 * time.Second)},
			expectedClients:    1,
		},
		{
			name:               "AllStaleClients",
			bucketSize:         20,
			refillInterval:     1 * time.Second,
			maxAgeStaleClients: 5 * time.Second,
			enforced:           true,
			clientLastActive:   []time.Time{time.Now().Add(-8 * time.Second), time.Now().Add(-9 * time.Second)},
			expectedClients:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetRateLimitEnv()
			clm, err := NewClientLimitManager(tt.bucketSize, tt.refillInterval, tt.maxAgeStaleClients, tt.enforced)
			assert.Nil(t, err, "unexpected error: %v", err)

			for i, lastActive := range tt.clientLastActive {
				clientAddr := fmt.Sprintf("1.1.1.%d", i)
				clm.clients.Add(clientAddr, &clientLimiter{
					limiter:            rate.NewLimiter(rate.Every(tt.refillInterval), tt.bucketSize),
					lastConnectionTime: lastActive,
				})
			}
			ctx := context.Background()

			go clm.StartClientCull(ctx)
			t.Log("waiting for culling to happen")
			time.Sleep(tt.maxAgeStaleClients + 2) // Wait for culling to happen
			ctx.Done()
			time.Sleep(2 * time.Second) // Wait for shutdown to complete

			if got := clm.clients.Length(); got != tt.expectedClients {
				t.Errorf("StartClientCull() = %d clients; want %d", got, tt.expectedClients)
			}
		})
	}
}

func TestLimitPublish(t *testing.T) {
	tests := []struct {
		name       string
		methodName string
		expected   bool
	}{
		{
			name:       "PublishMethod",
			methodName: api.Producer_Publish_FullMethodName,
			expected:   true,
		},
		{
			name:       "NonPublishMethod",
			methodName: "/some/other/method",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callMeta := interceptors.NewServerCallMeta(tt.methodName, nil, nil)
			result := LimitMethods(context.Background(), callMeta)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewClientLimitManager(t *testing.T) {
	tests := []struct {
		name                       string
		bucketSize                 int
		refillInterval             time.Duration
		maxAgeStaleClients         time.Duration
		enforced                   bool
		expectedError              bool
		expectedBucketSize         int
		expectedFillInterval       time.Duration
		expectedMaxAgeStaleClients time.Duration
	}{
		{
			name:                       "ValidEnvVariables",
			bucketSize:                 25,
			refillInterval:             2 * time.Second,
			maxAgeStaleClients:         5 * time.Minute,
			expectedError:              false,
			enforced:                   true,
			expectedBucketSize:         25,
			expectedFillInterval:       2 * time.Second,
			expectedMaxAgeStaleClients: 5 * time.Minute,
		},
		{
			name:               "MissingEnvVariables",
			bucketSize:         0,
			refillInterval:     0 * time.Second,
			maxAgeStaleClients: 0 * time.Minute,
			enforced:           true,
			expectedError:      true,
		},
		{
			name:               "InvalidBucketSize",
			bucketSize:         -1,
			refillInterval:     2 * time.Second,
			maxAgeStaleClients: 5 * time.Minute,
			enforced:           true,
			expectedError:      true,
		},
		{
			name:               "InvalidRefillInterval",
			bucketSize:         25,
			refillInterval:     0 * time.Second,
			maxAgeStaleClients: 5 * time.Minute,
			enforced:           true,
			expectedError:      true,
		},
		{
			name:               "InvalidMaxAgeStaleClients",
			bucketSize:         25,
			refillInterval:     2 * time.Minute,
			maxAgeStaleClients: 0,
			enforced:           true,
			expectedError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetRateLimitEnv()

			clm, err := NewClientLimitManager(tt.bucketSize, tt.refillInterval, tt.maxAgeStaleClients, tt.enforced)
			if tt.expectedError {
				assert.NotNil(t, err)
				assert.Nil(t, clm)
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, clm)
				assert.Equal(t, tt.expectedBucketSize, clm.bucketSize)
				assert.Equal(t, tt.expectedFillInterval, clm.fillInterval)
				assert.Equal(t, tt.expectedMaxAgeStaleClients, clm.maxAgeStaleClients)
			}
		})
	}
}

func newPeerContext(clientAddr string) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{Addr: mockPeerAddr{clientAddr: clientAddr}})
}

type mockPeerAddr struct {
	clientAddr string
}

func (m mockPeerAddr) Network() string {
	return "tcp"
}
func (m mockPeerAddr) String() string {
	return m.clientAddr
}
