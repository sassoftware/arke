// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/peer"
)

type fakeAddr struct {
	net.Addr
	s       string
	network string
}

func (f fakeAddr) String() string {
	return f.s
}
func (f fakeAddr) Network() string {
	return f.network
}

func TestGenUUID(t *testing.T) {
	uuidStr := GenUUID()
	fmt.Println(uuidStr)
	assert.NotNil(t, uuidStr)
	id, err := uuid.Parse(uuidStr)
	assert.IsType(t, uuid.UUID{}, id)
	assert.Nil(t, err)
}

func Test_GetClientAddr(t *testing.T) {
	p := &peer.Peer{}
	p.Addr = fakeAddr{s: "127.0.0.1", network: "tcp"}
	ctx := peer.NewContext(context.Background(), p)

	addr, err := GetClientAddr(ctx)
	assert.Nil(t, err)
	assert.Equal(t, addr, "127.0.0.1")
}

func Test_SetClientIdentifier(t *testing.T) {
	p := &peer.Peer{}
	p.Addr = fakeAddr{s: "127.0.0.1", network: "tcp"}
	ctx := peer.NewContext(context.Background(), p)
	id, err := SetClientIdentifier(ctx, "unitTest")
	assert.Contains(t, id, "unitTest-")
	assert.Nil(t, err)

	getID, err := GetClientIdentifier(ctx)
	assert.Equal(t, id, getID)
	assert.Nil(t, err)

	p.Addr = fakeAddr{}
	ctx = peer.NewContext(context.Background(), p)
	getID, err = GetClientIdentifier(ctx)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "could not retrieve client-id from context")
	assert.Equal(t, "", getID)
}

func Test_RemoveClientIdentifier(t *testing.T) {
	p := &peer.Peer{}
	p.Addr = fakeAddr{s: "127.0.0.1", network: "tcp"}
	ctx := peer.NewContext(context.Background(), p)
	id, err := SetClientIdentifier(ctx, "unitTest")
	assert.Contains(t, id, "unitTest-")
	assert.Nil(t, err)

	RemoveClientIdentifier(ctx)

	getID, err := GetClientIdentifier(ctx)
	assert.Equal(t, "", getID)
	assert.NotNil(t, err)
	assert.Equal(t, "could not find client identifier", err.Error())
}

func Test_TestProcessStats(t *testing.T) {
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		go MonitorProcessStats()
		time.Sleep(1 * time.Second)
		ps := GetProcessStats()
		assert.NotNil(t, ps)
	}
}

func Test_GetConfig(t *testing.T) {
	testVar := "TEST_ENV_VAR"
	defer os.Unsetenv(testVar)

	// Default value int
	intVal := GetConfig(testVar, 11)
	assert.Equal(t, 11, intVal)

	// Default value bool
	boolVal := GetConfig(testVar, true)
	assert.Equal(t, true, boolVal)

	// Default value string
	strVal := GetConfig(testVar, "test")
	assert.Equal(t, "test", strVal)

	// Env var int
	os.Setenv(testVar, "10")
	intVal = GetConfig(testVar, 11)
	assert.Equal(t, 10, intVal)

	// Env var bool
	os.Setenv(testVar, "true")
	boolVal = GetConfig(testVar, false)
	assert.Equal(t, true, boolVal)

	// Env var string
	os.Setenv(testVar, "foo")
	strVal = GetConfig(testVar, "bar")
	assert.Equal(t, "foo", strVal)
}

func TestServceNameFromClientAddr(t *testing.T) {
	tests := []struct {
		name       string
		clientAddr string
		expected   string
	}{
		{
			name:       "standard service name with random suffix",
			clientAddr: "arke-service-abc123def",
			expected:   "arke-service",
		},
		{
			name:       "single part service with numeric suffix",
			clientAddr: "webserver-12345",
			expected:   "webserver",
		},
		{
			name:       "multi-part service name with numeric suffix",
			clientAddr: "user-auth-service-9876xyz",
			expected:   "user-auth-service",
		},
		{
			name:       "service with multiple hyphens and numbers in suffix",
			clientAddr: "my-cool-service-a1b2c3",
			expected:   "my-cool-service",
		},
		{
			name:       "service name only (no numeric suffix)",
			clientAddr: "simple-service",
			expected:   "simple-service",
		},
		{
			name:       "single word service name only",
			clientAddr: "service",
			expected:   "service",
		},
		{
			name:       "numeric suffix immediately after first token",
			clientAddr: "app-123-more",
			expected:   "app",
		},
		{
			name:       "mixed alphanumeric in suffix",
			clientAddr: "api-gateway-7a8b9c",
			expected:   "api-gateway",
		},
		{
			name:       "empty string",
			clientAddr: "",
			expected:   "",
		},
		{
			name:       "only numbers",
			clientAddr: "12345",
			expected:   "",
		},
		{
			name:       "alphanumeric first token",
			clientAddr: "service1-test",
			expected:   "",
		},
		{
			name:       "kubernetes pod style naming",
			clientAddr: "deployment-name-5f4d8c9b7-xk8qs",
			expected:   "deployment-name",
		},
		{
			name:       "service with underscore becomes multiple parts",
			clientAddr: "my_service-abc123",
			expected:   "my_service",
		},
		{
			name:       "long service name",
			clientAddr: "very-long-service-name-with-many-parts-123abc",
			expected:   "very-long-service-name-with-many-parts",
		},
		{
			name:       "service with number in middle but not at start of token",
			clientAddr: "api-v2-service-abc123",
			expected:   "api",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ServceNameFromClientAddr(tt.clientAddr)
			assert.Equal(t, tt.expected, result, "ServceNameFromClientAddr(%q) should return %q, got %q", tt.clientAddr, tt.expected, result)
		})
	}
}
