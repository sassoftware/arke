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

// clientStatsInterval controls how often connected-client stats are refreshed
// independent of Prometheus scrapes. It is shorter than the sink Expiration
// (60s) so that gauges for clients that connect and disconnect between two
// scrapes are still captured at least once, as long as the client is connected
// when a refresh fires. Clients whose entire lifetime falls between two refresh
// ticks are still not captured; see gatherClientStats.
const clientStatsInterval = 15 * time.Second

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
	go collectClientStats(ctx, clientStatsInterval)

	<-ctx.Done()
	metricsServer.Shutdown(ctx) //nolint:errcheck
}

// collectClientStats periodically gathers per-client stats until ctx is
// cancelled, independent of /metrics scrapes. Its purpose is to surface a
// client that connects and disconnects between two scrapes: a client connected
// when a tick fires has its gauge written to the sink, which (because the sink
// expires entries only after Expiration, not on disconnect) survives to be
// emitted on the next scrape even though the client is already gone.
//
// This only adds coverage when the scrape interval is longer than the tick
// interval; if Prometheus scrapes more frequently, the on-scrape gather already
// captures the same clients at equal-or-finer granularity and this is a no-op.
// It does not guarantee every client is recorded — a client whose lifetime fits
// entirely between two ticks is still missed. Disconnected clients age out of
// the sink via its Expiration.
func collectClientStats(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			gatherClientStats()
		}
	}
}

func gatherClientStatsHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatherClientStats()
		h.ServeHTTP(w, r)
	})
}

// gatherClientStats refreshes the per-client gauges for every client that is
// connected at the moment it runs. It is called from two places: on each
// /metrics scrape (so a scrape always reflects the latest values) and on a
// periodic interval via collectClientStats (so clients that connect and
// disconnect between scrapes are still refreshed, provided they are connected
// when a tick fires).
//
// This is a sampling approach and is inherently incomplete: a client whose
// entire lifetime falls between two collections is never recorded, because by
// the next collection it is already gone from the provider's connection set.
// This is by design. The per-client gauges are a best-effort snapshot of
// *currently connected* clients, kept deliberately cardinality-bounded: the
// ClientIdentifier label embeds a per-connection hash (see
// util.SetClientIdentifier), so series for departed clients must age out of the
// sink via its Expiration rather than accumulate forever. Promoting these to
// per-client counters to capture short-lived clients would defeat that bound
// and grow cardinality without limit.
//
// Exact, complete throughput is already available without sampling from the
// aggregate counters arke_recvmsg_total / arke_sendmsg_total, which are
// incremented per message in the gRPC interceptors and never miss a client.
//
// Stats for disconnected clients are not removed here; they age out of the sink
// via its Expiration.
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
