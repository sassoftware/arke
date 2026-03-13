// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package prometheus

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/sassoftware/arke/internal/metrics"
	"github.com/sassoftware/arke/internal/provider"
	_ "github.com/sassoftware/arke/internal/provider/connectors"
	"github.com/stretchr/testify/assert"
)

func Test_Metrics(t *testing.T) {
	var tests = []struct {
		name       string
		metricName []string
		metricDesc string
		key        string
		expected   bool
	}{
		{
			name:       "sperate key - match",
			metricName: []string{"arke", "gauge", "one"},
			metricDesc: "description for gauge1",
			key:        "arke_gauge_one",
			expected:   true,
		},
		{
			name:       "one key - match",
			metricName: []string{"arke_gauge_two"},
			metricDesc: "description for gauge2",
			key:        "arke_gauge_two",
			expected:   true,
		},
		{
			name:       "key - not match",
			metricName: []string{"arke", "gauge", "three"},
			metricDesc: "description for gauge3",
			key:        "arke_gauge_two",
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newArkeGauge(tt.metricName, tt.metricDesc)
			assert.NotNil(t, g)
			key := strings.Join(tt.metricName, "_")
			assert.True(t, strings.Contains(key, "_"))
			matches := strings.Contains(g.Desc().String(), tt.key)
			if matches != tt.expected {
				t.Errorf("Unexpected result for gauge %s and its description: %s.",
					tt.metricName, tt.metricDesc)
			}

			c := newArkeCounter(tt.metricName, tt.metricDesc)
			assert.NotNil(t, c)
			matches = strings.Contains(c.Desc().String(), tt.key)
			if matches != tt.expected {
				t.Errorf("Unexpected result for counter %s and its description: %s.",
					tt.metricName, tt.metricDesc)
			}

			s := newArkeSample(tt.metricName, tt.metricDesc)
			assert.NotNil(t, s)
			matches = strings.Contains(s.Desc().String(), tt.key)
			if matches != tt.expected {
				t.Errorf("Unexpected result for sample %s and its description: %s.",
					tt.metricName, tt.metricDesc)
			}
		})
	}

	assert.Equal(t, metrics.ClientActMessageGauge, []string{"arke", "client", "active", "messages"})
	assert.Equal(t, metrics.ClientStreamsGauge, []string{"arke", "client", "streams"})
	assert.Equal(t, metrics.ClientConsumedGauge, []string{"arke", "client", "consumed"})
	assert.Equal(t, metrics.ClientProducedGauge, []string{"arke", "client", "produced"})
	assert.Equal(t, metrics.RequestElapsedSummary, []string{"arke", "request", "elapsed"})
	assert.Equal(t, metrics.RequestTotalCounter, []string{"arke", "request", "total"})
	assert.Equal(t, metrics.RecvMsgCounter, []string{"arke", "recvmsg", "total"})
	assert.Equal(t, metrics.SendMsgCounter, []string{"arke", "sendmsg", "total"})
}

func Test_gatherClientStats(t *testing.T) {
	_, _ = provider.GetProvider("amqp091")
	gatherClientStats()
	assert.NotEmpty(t, Stats)
}

func Test_Serve(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":50052")
	assert.Nil(t, err)
	defer lis.Close()
	go Serve(ctx, &lis)

	req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost:50052/metrics", nil)
	assert.Nil(t, err)

	client := &http.Client{}
	res, err := client.Do(req)

	assert.Nil(t, err)
	defer res.Body.Close()
	assert.Equal(t, 200, res.StatusCode)
	body, _ := io.ReadAll(res.Body)
	sbody := string(body)
	assert.Contains(t, sbody, "arke_client_active_messages")
	assert.Contains(t, sbody, "go_info")
	assert.Contains(t, sbody, "go_sync_mutex_wait_total_seconds_total")
	assert.Contains(t, sbody, "go_memstats_next_gc_bytes")

	// pprof not enabled
	req, err = http.NewRequestWithContext(ctx, "GET", "http://localhost:50052/debug/pprof/", nil)
	assert.Nil(t, err)

	res, err = client.Do(req)

	assert.Nil(t, err)
	defer res.Body.Close()
	assert.Equal(t, 404, res.StatusCode)
	body, _ = io.ReadAll(res.Body)
	assert.Contains(t, string(body), "404 page not found")
}

func Test_ServePprofEnabled(t *testing.T) {
	os.Setenv(EnvPprofEnabled, "true")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":50053")
	assert.Nil(t, err)
	defer lis.Close()
	go Serve(ctx, &lis)

	req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost:50053/metrics", nil)
	assert.Nil(t, err)

	client := &http.Client{}
	res, err := client.Do(req)

	assert.Nil(t, err)
	defer res.Body.Close()
	assert.Equal(t, 200, res.StatusCode)
	body, _ := io.ReadAll(res.Body)
	sbody := string(body)
	assert.Contains(t, sbody, "arke_client_active_messages")
	assert.Contains(t, sbody, "go_info")
	assert.Contains(t, sbody, "go_sync_mutex_wait_total_seconds_total")
	assert.Contains(t, sbody, "go_memstats_next_gc_bytes")

	req, err = http.NewRequestWithContext(ctx, "GET", "http://localhost:50053/debug/pprof/", nil)
	assert.Nil(t, err)

	res, err = client.Do(req)

	assert.Nil(t, err)
	defer res.Body.Close()
	assert.Equal(t, 200, res.StatusCode)
	body, _ = io.ReadAll(res.Body)
	assert.Contains(t, string(body), "full goroutine stack dump")
}
