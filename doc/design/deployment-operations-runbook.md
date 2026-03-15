# Deployment and Operations Runbook

## Purpose

This document covers everything an operator needs to deploy, configure,
monitor, and troubleshoot Arke in production. It assumes a Kubernetes
environment, though most sections apply equally to bare-metal and VM
deployments.

---

## Container Image

A Docker image can be built from:

```dockerfile
FROM rockylinux:9
COPY build/linux/arke /opt/bin/
ENV PATH=$PATH:/opt/bin
ENV ARKE_PORT=50051
CMD [ "arke" ]
```

Build the Linux binary and image:

```bash
make linux          # produces build/linux/arke
docker build -t arke:latest .
```

---

## Environment Variables Reference

All configuration is provided through environment variables. No config file is required.

### Core

| Variable | Default | Required | Description |
| --- | --- | --- | --- |
| `ARKE_PORT` | `50051` | No | TCP port for both gRPC and Prometheus HTTP |
| `ARKE_CERT_FILE` | *(none)* | Paired with `ARKE_CERT_KEY` | Path to PEM TLS certificate (enables TLS) |
| `ARKE_CERT_KEY` | *(none)* | Paired with `ARKE_CERT_FILE` | Path to PEM TLS private key |

### Rate Limiting

All three numeric variables must be set to valid positive integers for rate
limiting to activate. Setting only some of them disables rate limiting and
logs a warning.

| Variable | Default | Description |
| --- | --- | --- |
| `ARKE_RATE_LIMIT_BUCKET_SIZE` | *(disabled)* | Token bucket capacity per client |
| `ARKE_RATE_LIMIT_REFILL_SECONDS` | *(disabled)* | Seconds between bucket refill events (1 token per interval) |
| `ARKE_RATE_LIMIT_MAX_AGE_STALE_CLIENTS` | *(disabled)* | Seconds of inactivity before a client's rate-limit state is evicted |
| `ARKE_RATE_LIMIT_ENFORCED` | `false` | `true` = reject over-limit requests with `RESOURCE_EXHAUSTED`; `false` = log only |

### Backend TLS (Broker Connection)

| Variable | Description |
| --- | --- |
| `ARKE_TRUSTED_CA_CERTIFICATES_PEM_FILE` | PEM file with additional CA certificates trusted when connecting to the broker |

### Observability

| Variable | Default | Description |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | gRPC endpoint of the OTLP collector for traces |
| `OTEL_SDK_DISABLED` | `false` | `true` disables OpenTelemetry tracing entirely |

### Kubernetes / HPA

| Variable | Default | Description |
| --- | --- | --- |
| `ARKE_HPA_NAME` | `arke` | Name of the HorizontalPodAutoscaler Arke monitors for scale-up events. Override with `WithHpaName()` in code or via the environment equivalent if exposed. |

---

## Kubernetes Deployment Checklist

### 1. Resources and Limits

Arke sets `GOMEMLIMIT` to 90% of the cgroup memory limit automatically. Set
container `resources.limits.memory` to match your expected load. Start with:

```yaml
resources:
  requests:
    cpu: "250m"
    memory: "256Mi"
  limits:
    cpu: "1000m"
    memory: "512Mi"
```

### 2. Ports

Only a single port is needed. Both gRPC and Prometheus metrics are served on
the same port via `cmux`.

```yaml
ports:
  - name: arke
    containerPort: 50051   # or whatever PORT is set to
    protocol: TCP
```

### 3. Liveness and Readiness Probes

Use the standard gRPC health protocol. The `arke` service is registered with
status `SERVING` on startup.

```yaml
livenessProbe:
  grpc:
    port: 50051
    service: arke
  initialDelaySeconds: 5
  periodSeconds: 15

readinessProbe:
  grpc:
    port: 50051
    service: arke
  initialDelaySeconds: 3
  periodSeconds: 10
```

Alternatively, use `grpc_health_probe` as an exec probe for older Kubernetes versions.

### 4. TLS Certificates (Secret mount)

```yaml
env:
  - name: ARKE_CERT_FILE
    value: /etc/arke/tls/tls.crt
  - name: ARKE_CERT_KEY
    value: /etc/arke/tls/tls.key
volumeMounts:
  - name: arke-tls
    mountPath: /etc/arke/tls
    readOnly: true
volumes:
  - name: arke-tls
    secret:
      secretName: arke-tls-secret
```

### 5. HPA Configuration

Arke broadcasts `GOAWAY` to connected clients when it detects an HPA scale-up.
The HPA itself should be configured based on CPU or custom metrics:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: arke     # must match the ARKE_HPA_NAME env var (default: "arke")
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: arke
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

### 6. Prometheus Scraping

Prometheus metrics are served at `http://<pod-ip>:<PORT>/metrics`. Add a
`PodMonitor` for scraping the metrics:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: arke
  labels:
    release: prometheus   # match your Prometheus Operator selector
spec:
  selector:
    matchLabels:
      app: arke
  podMetricsEndpoints:
    - port: arke          # matches the named port in the Pod spec
      path: /metrics
      scheme: http
      enableHttp2: false  # disable http2 because it requires http 1.1
```

---

## Graceful Shutdown

Arke handles `SIGINT` (Ctrl-C, `kubectl delete pod`, Kubernetes preStop). On signal:

1. `grpc.Server.Stop()` is called, draining in-flight RPCs.
2. The `cmux` listener is closed.
3. The OTLP tracer provider flushes pending spans.
4. The process exits.

Kubernetes sends `SIGTERM` before killing a pod. To align, map `SIGTERM` →
graceful stop. If your base image or init system does not propagate signals to
PID 1 correctly, add a `preStop` lifecycle hook:

```yaml
lifecycle:
  preStop:
    exec:
      command: ["sleep", "5"]
```

This gives Kubernetes time to deregister the pod from the service before
traffic is cut.

---

## Metrics, Tracing, and Health

### Prometheus Metrics

Scrape endpoint: `http://<host>:<port>/metrics`

Key metrics to alert on:

| Metric | Alert condition |
| --- | --- |
| `grpc_server_handled_total{grpc_code="ResourceExhausted"}` | Rate-limit rejections — tune `ARKE_RATE_LIMIT_BUCKET_SIZE` / `ARKE_RATE_LIMIT_REFILL_SECONDS` |
| `grpc_server_handled_total{grpc_code="Unavailable"}` | Broker connectivity issues |
| `process_resident_memory_bytes` | Approaching container memory limit |
| `go_goroutines` | Unexpected goroutine growth (leak indicator) |

### OpenTelemetry Traces

Configure `OTEL_EXPORTER_OTLP_ENDPOINT` to point at your collector (Jaeger,
Tempo, etc.). Trace context is propagated using both W3C `traceparent`
`tracestate` and B3 headers, so Arke integrates with both W3C-native and
legacy tracing stacks.

Disable entirely with `OTEL_SDK_DISABLED=true` if you do not have a collector
— leaving the exporter pointing at a non-existent endpoint causes connection
errors that are logged but do not affect functionality.

### Health Check

The custom `Healthz.Check` stream reports:

| Field | Description |
| --- | --- |
| `code` | `OK` / `UNHEALTHY` / `GOAWAY` |
| `cpu_availability` | Percentage of CPU headroom (0–100) |
| `memory_availability` | Percentage of memory headroom (0–100) |

`UNHEALTHY` is set when memory usage exceeds ~90% or CPU has been consistently
high for an extended period. `GOAWAY` is set by the HPA monitor.

---

## Profiling

CPU and memory profiling are available via command-line flags (not environment variables):

```bash
arke --cpuprofile cpu.out --memprofile mem.out
```

These write `pprof`-compatible files. Analyse with:

```bash
go tool pprof cpu.out
go tool pprof mem.out
```

Do not run with profiling enabled in production under sustained load — the
files grow unbounded until the process exits.

---

## Troubleshooting

### Clients receive `RESOURCE_EXHAUSTED`

Rate limiting is enforced. Check current settings:

1. Review `ARKE_RATE_LIMIT_BUCKET_SIZE`, `ARKE_RATE_LIMIT_REFILL_SECONDS`,
`ARKE_RATE_LIMIT_ENFORCED`.
2. If `ARKE_RATE_LIMIT_ENFORCED=false` (default), violations are logged but
not rejected — check logs for rate-limit warnings.
3. Increase `ARKE_RATE_LIMIT_BUCKET_SIZE` or decrease
`ARKE_RATE_LIMIT_REFILL_SECONDS` to raise the effective limit.

### Clients receive `Unavailable` on Connect

1. Verify the backend broker is reachable from the Arke pod:
  `kubectl exec <pod> -- nc -zv <broker-host> <broker-port>`.
2. Check TLS configuration: if the broker requires TLS, ensure
`ConnectionConfiguration.Tls = true` and that
`SAS_ARKE_TRUSTED_CA_CERTIFICATES_PEM_FILE` points to the correct CA bundle.
3. Check broker credentials in `ConnectionConfiguration.Credentials`.

### Connections not cleaned up (connectionMap growing)

The connection watcher runs every 30 seconds. If `prov.ClientExists()` in the
broker provider is not correctly tracking disconnected clients, entries
accumulate. Check provider-specific logs for `"Provider says client X does not
exist"` to confirm the watcher is running.

### Clients not receiving GOAWAY during scale-up

1. Confirm the HPA name matches `ARKE_HPA_NAME` (default: `arke`).
2. Verify the Arke pod has RBAC permission to read the HPA resource.
3. Check logs for `"Monitoring Horizontal Pod Autoscaler"` at startup to
confirm the goroutine started.

### High goroutine count

Use the `/metrics` endpoint or `go tool pprof` to identify which goroutine
stacks are accumulating. Each active `Consume` stream creates up to 4
goroutines (recv, subscribe, delivery, periodic ack). A goroutine count of `4
× active_subscriptions + baseline` is expected. Leak patterns typically show
`subscribe` or `delivery` goroutines blocking on closed channels — check
provider `Subscribe` implementations for correct context cancellation handling.

---

## Log Levels

Arke uses `zerolog`. Log level is controlled by standard `zerolog` environment
conventions. Useful levels:

| Level | Use |
| --- | --- |
| `debug` | Verbose connection events, goroutine lifecycle |
| `info` | Startup, normal operation |
| `warn` | Non-fatal errors (rate limit violations, broker reconnects) |
| `error` / `fatal` | Startup failures, unrecoverable errors |

Set the log level in your Kubernetes deployment:

```yaml
env:
  - name: ARKE_LOG_LEVEL     # or however zerolog is initialized in internal/util/logger.go
    value: "debug"
```
