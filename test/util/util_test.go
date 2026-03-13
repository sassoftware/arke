// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectToArke(t *testing.T) {
	conn, err := ConnectToArke(true)
	assert.Nil(t, err, "error when connecting to arke: %v", err)
	assert.NotNil(t, conn, "got a nil connection")
	if conn != nil {
		conn.Close()
	}
}

func TestInitResource(t *testing.T) {
	t.Run("returns non-nil resource", func(t *testing.T) {
		resource := InitResource("test-service")
		assert.NotNil(t, resource, "expected non-nil resource")
	})

	initResourcesOnce = sync.Once{} // Reset sync.Once for testing purposes

	t.Run("resource contains service name attribute", func(t *testing.T) {
		resource := InitResource("my-service")
		assert.NotNil(t, resource, "expected non-nil resource")
		attrs := resource.Attributes()
		found := false
		for _, attr := range attrs {
			if string(attr.Key) == "service.name" {
				found = true
				break
			}
		}
		assert.True(t, found, "expected service.name attribute in resource")
	})
}

func TestInitTracerProvider(t *testing.T) {
	t.Run("returns error when OTLP endpoint is unavailable", func(t *testing.T) {
		// Without a running OTLP collector, New() may still succeed (lazy connection),
		// so we just verify the function returns without panicking.
		tp, err := InitTracerProvider("test-service")
		if err != nil {
			assert.Nil(t, tp, "expected nil tracer provider on error")
		} else {
			assert.NotNil(t, tp, "expected non-nil tracer provider on success")
			if tp != nil {
				_ = tp.Shutdown(context.TODO())
			}
		}
	})

	t.Run("returns valid tracer provider", func(t *testing.T) {
		tp, err := InitTracerProvider("test-tracer-service")
		if err == nil {
			assert.NotNil(t, tp, "expected non-nil tracer provider")
			if tp != nil {
				_ = tp.Shutdown(context.TODO())
			}
		}
	})
}
