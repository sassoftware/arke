// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package prometheus

import (
	"context"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	met "github.com/hashicorp/go-metrics"
	promet "github.com/hashicorp/go-metrics/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sassoftware/arke/internal/metrics"
	"github.com/sassoftware/arke/internal/provider"
)

type stats struct {
	met.Metrics
	Sink *promet.PrometheusSink
}

// Stats global Stats variable for access to the sinks
var (
	Stats    *stats
	registry *prometheus.Registry
)

const EnvPprofEnabled = "ARKE_PPROF_ENABLED"

func init() {
	Stats = &stats{}

	registry = prometheus.NewRegistry()
	registry.MustRegister(collectors.NewBuildInfoCollector())
	registry.MustRegister(collectors.NewGoCollector(
		collectors.WithGoCollectorRuntimeMetrics(collectors.GoRuntimeMetricsRule{Matcher: regexp.MustCompile("/.*")}),
	))
	// The go-metrics library does not support setting a help on metrics with their PrometheusSink.
	// Continue to pass our expected help text along until we can implement a proper fix for this,
	// but the help in the metrics output will be just the key for now.
	registry.MustRegister(newArkeGauge(metrics.ClientActMessageGauge, "Number of active messages to be processed."))
	registry.MustRegister(newArkeGauge(metrics.ClientStreamsGauge, "Number of client active streams."))
	registry.MustRegister(newArkeGauge(metrics.ClientConsumedGauge, "Total number of client requests have been consumed."))
	registry.MustRegister(newArkeGauge(metrics.ClientProducedGauge, "Total number of client requests have been produced."))
	registry.MustRegister(newArkeSample(metrics.RequestElapsedSummary, "The request elapsed time."))
	registry.MustRegister(newArkeCounter(metrics.RequestTotalCounter, "Total number of requests processed."))
	registry.MustRegister(newArkeCounter(metrics.RecvMsgCounter, "Total number of stream messages have been received."))
	registry.MustRegister(newArkeCounter(metrics.SendMsgCounter, "Total number of stream messages have been sent."))

	opts := promet.PrometheusOpts{
		Registerer: registry,
		Expiration: 60 * time.Second,
		Name:       "arke_sink",
	}
	Stats.Sink, _ = promet.NewPrometheusSinkFrom(opts)

	promConf := met.DefaultConfig("")
	promConf.EnableHostname = false
	met.NewGlobal(promConf, Stats.Sink) //nolint:errcheck
}

func pprofEnabled() bool {
	enabled, err := strconv.ParseBool(os.Getenv(EnvPprofEnabled))
	if err != nil {
		enabled = false
	}

	return enabled
}

func setupServer() *http.Server {
	promHandler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})

	mux := http.NewServeMux()
	mux.Handle("/metrics", gatherClientStatsHandler(promHandler))

	if pprofEnabled() {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	metricsServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 30 * time.Second,
	}

	return metricsServer
}

// Serve Create a new HTTP server and Serve metrics requests
func Serve(ctx context.Context, lis *net.Listener) {
	metricsServer := setupServer()

	go metricsServer.Serve(*lis) //nolint:errcheck

	<-ctx.Done()
	metricsServer.Shutdown(ctx) //nolint:errcheck
}

func gatherClientStatsHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatherClientStats()
		h.ServeHTTP(w, r)
	})
}

// FIXME?
// I don't particularly like only collecting these client level stats when the metrics handler
// is called because it can and will lead to us never logging stats for some clients.
// We can even have incorrect statistics if part way through a client producing/consuming the
// metrics are collected, but they disconnect before the next metrics collection.
// I also don't like the idea of keeping a separate record of stats from every for an extended
// period of time even if they have disconnected.
// Maybe client level stats don't matter much as long as they're regarded as an incomplete look
// into the current state of only connected clients.
func gatherClientStats() {
	providers := provider.RegisteredProviders()
	for _, providerName := range provider.RegisteredProviders().GetList() {
		provRaw, exists := providers.Get(providerName)
		if !exists {
			continue
		}
		prov := provRaw.(provider.Provider)
		pstats := prov.Stats()
		for _, client := range pstats.Clients {
			clientID := strings.ReplaceAll(client.ID, ".", "-")
			labelset := metrics.NewLabelSet()
			labelset.AddLabel("ClientIdentifier", clientID)

			Stats.Sink.SetGaugeWithLabels(metrics.ClientActMessageGauge, float32(client.ActiveMessages), labelset.Labels)
			Stats.Sink.SetGaugeWithLabels(metrics.ClientStreamsGauge, float32(client.Streams), labelset.Labels)
			Stats.Sink.SetGaugeWithLabels(metrics.ClientConsumedGauge, float32(client.Consumed), labelset.Labels)
			Stats.Sink.SetGaugeWithLabels(metrics.ClientProducedGauge, float32(client.Produced), labelset.Labels)
		}
	}
}

func newArkeGauge(parts []string, _ string) prometheus.Gauge {
	key := strings.Join(parts, "_")
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: key,
		Help: key,
	})
	return g
}

func newArkeSample(parts []string, _ string) prometheus.Summary {
	key := strings.Join(parts, "_")
	g := prometheus.NewSummary(prometheus.SummaryOpts{
		Name: key,
		Help: key,
	})
	return g
}

func newArkeCounter(parts []string, _ string) prometheus.Counter {
	key := strings.Join(parts, "_")
	g := prometheus.NewCounter(prometheus.CounterOpts{
		Name: key,
		Help: key,
	})
	return g
}
