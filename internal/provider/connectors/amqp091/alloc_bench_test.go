// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package amqp091

// Microbenchmarks for the three heap-allocation fixes.
// Run with:
//   go test -bench=. -benchmem -count=6 ./internal/provider/connectors/amqp091/

import (
	"net/http"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ---------- Fix 1: toAmqpTable / fromAmqpTable ----------

var sinkTable amqp.Table
var sinkLocal amqp091Table

func makeTestTable(n int) amqp091Table {
	t := make(amqp091Table, n)
	for i := range n {
		t[string(rune('a'+i))] = i
	}
	return t
}

func BenchmarkToAmqpTable(b *testing.B) {
	src := makeTestTable(8)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		sinkTable = toAmqpTable(src)
	}
}

func BenchmarkFromAmqpTable(b *testing.B) {
	src := amqp.Table{"x-retry-count": int32(3), "Content-Type": "application/json"}
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		sinkLocal = fromAmqpTable(src)
	}
}

// ---------- Fix 2: getManagementClient ----------

// old — allocates a new *http.Client every call
func oldGetManagementClient(tlsEnabled bool) *http.Client {
	if tlsEnabled {
		tr := &http.Transport{}
		return &http.Client{Transport: tr, Timeout: 5 * time.Second}
	}
	return &http.Client{Timeout: 5 * time.Second}
}

// new — one-time init via the patched BrokerDetails method
var sinkHTTP *http.Client

func BenchmarkGetManagementClient_Old(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		sinkHTTP = oldGetManagementClient(false)
	}
}

func BenchmarkGetManagementClient_New(b *testing.B) {
	bd := &BrokerDetails{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		sinkHTTP = bd.getManagementClient()
	}
}

// ---------- Fix 3: BrokerDetails pointer vs value ----------

// Simulate what the compiler sees for the value case: construct the struct and
// immediately take its address (the five-escape pattern).
var sinkBD *BrokerDetails

func BenchmarkBrokerDetailsValue(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		bd := BrokerDetails{ //nolint:exhaustruct
			ClientIdentifier: "bench-client",
			ErrorChannel:     make(chan amqp091Error, 1),
			shutdownChan:     make(chan bool, 1),
			lastPubSubEvent:  time.Now(),
		}
		sinkBD = &bd // triggers the escape
	}
}

func BenchmarkBrokerDetailsPointer(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		sinkBD = &BrokerDetails{ //nolint:exhaustruct
			ClientIdentifier: "bench-client",
			ErrorChannel:     make(chan amqp091Error, 1),
			shutdownChan:     make(chan bool, 1),
			lastPubSubEvent:  time.Now(),
		}
	}
}

// ---------- sync.Once baseline (for Fix 3 context) ----------
var onceResult int

func BenchmarkSyncOnce(b *testing.B) {
	var once sync.Once
	b.ReportAllocs()
	for range b.N {
		once.Do(func() { onceResult++ })
	}
}
