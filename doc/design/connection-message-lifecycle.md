# Connection and Message Lifecycle

## Purpose

This document traces the full lifecycle of a client session in Arke — from the
initial TCP connection through message exchange to clean-up — including error
paths, concurrency model, and Kubernetes-specific behaviors.

---

## Session Phases

```text
Phase 1 ── TCP / TLS handshake
Phase 2 ── gRPC Connect RPC  (Producer or Consumer service)
Phase 3 ── Produce or Consume streaming RPCs
Phase 4 ── Disconnect / context cancellation
Phase 5 ── Background cleanup (connection watcher)
```

---

## Phase 1: TCP / TLS Handshake

When `Arke.Serve()` starts, it creates a single TCP listener. `cmux` sits in
front of gRPC, distinguishing HTTP/1.x (Prometheus scrapes) from all other
traffic (gRPC / HTTP/2):

```text
TCP accept
    │
    ├── HTTP/1.x  →  Prometheus metrics handler (internal/metrics/prometheus)
    └── Other     →  grpc.Server
```

If `ARKE_CERT_FILE` and `ARKE_CERT_KEY` are set, the listener is wrapped in
`tls.NewListener` with ALPN protocols `h2` and `http/1.1`, so both gRPC and
Prometheus remain reachable on the same TLS port.

gRPC keepalive parameters applied at the server level:

| Parameter | Value | Effect |
| --- | --- | --- |
| `KeepaliveParams.Time` | 20 s | Sends a ping if no frames seen for 20 s |
| `KeepaliveParams.Timeout` | 60 s | Closes connection if no ping acknowledgement |
| `KeepaliveParams.MaxConnectionIdle` | 5 min | Closes connections with no open streams after 5 min |
| `EnforcementPolicy.MinTime` | 5 s | Terminates clients that ping more often than every 5 s |
| `EnforcementPolicy.PermitWithoutStream` | true | Allows keepalive pings on idle connections |

---

## Phase 2: Connect RPC

Both `ProducerServer` and `ConsumerServer` expose an identical `Connect` entry
point which calls `brokerConnect`:

<!-- markdownlint-disable MD013 -->
```text
client.Connect(ConnectionConfiguration)
    │
    └─► brokerConnect(ctx, cfg, tlsSkipVerify)
            │
            ├─► GetClientIdentifier(ctx)          ← derived from gRPC peer address
            │
            ├─► provider.GetProvider(cfg.Provider) ← singleton, created on first call
            │
            ├─► already connected? (connectionMap lookup)
            │       YES → return existing ConnectResponse (idempotent)
            │       NO  →
            │            prov.Connect(ctx, cfg, tlsSkipVerify)
            │                └─► records per-client state in provider
            │            connectionMap.Add(clientIdentifier, cfg)
            │
            └─► return ConnectResponse{Success: true}
```
<!-- markdownlint-emable MD013 -->

**Idempotency:** If the same `clientIdentifier` is already in `connectionMap`,
Arke returns a success response without creating a second broker connection.
Clients may call `Connect` again after a transient error without risk.

**Client identifier:** The identifier is derived from the gRPC peer address
(host:port). In a load-balanced environment, each TCP connection gets its own
identifier because the source ephemeral port differs.

---

## Phase 3a: Produce Lifecycle

### `PublishOne` (unary)

```text
client.PublishOne(Message)
    │
    ├─► findProvider(ctx)      ← looks up prov via clientIdentifier in connectionMap
    ├─► validate: exactly 1 subject in Message.Address
    └─► prov.PublishOne(ctx, msg)
            └─► return MessageResponse{Success: true|false}
```

### `Publish` (client-streaming)

```text
client.Publish(stream)  → sends many Messages
    │
    ├─► findProvider(ctx)
    │
    └─► prov.Publish(ctx, inChan, errChan)    ← runs in goroutine
            │                                 │
            │  stream.Recv() loop ────────────►│ inChan
            │  reads errChan for per-msg errors│
            └─► MessageResponse sent per-error │
                Final MessageResponse on EOF   │
```

`inChan` is a buffered channel (`cap=10`). The Recv loop and the provider
publish loop run concurrently; back-pressure is applied when the channel is
full.

---

## Phase 3b: Consume Lifecycle

`Consume` is a bidirectional streaming RPC. A single stream carries control
messages (Source, Ack/Nack) from the client and data messages back to the
client.

### State machine

```text
                   ┌─────────────────────────────┐
            ──────►│  WAITING FOR SOURCE          │
                   │  stream.Recv() blocked       │
                   └──────────┬──────────────────-┘
                              │ Consume{Src: Source}
                              ▼
                   ┌─────────────────────────────┐
                   │  SUBSCRIBING                 │
                   │  prov.Subscribe goroutine    │◄── messages from broker
                   │  delivery goroutine          │──► stream.Send to client
                   └──────────┬──────────────────-┘
                              │ Consume{Ack/Nack}
                              ▼
                   ┌─────────────────────────────┐
                   │  ACK / NACK processing       │
                   │  (goroutine per ack)         │
                   └──────────────────────────────┘
```

**Only one Source per stream.** Sending a second `Consume{Src}` message
returns an error response; the subscription continues unchanged.

### Goroutine topology inside `Consume`

```text
ConsumerServer.Consume()  ← main goroutine, consumeLoop
    │
    ├── recv goroutine          stream.Recv() → recvChan
    │
    ├── subscribe goroutine     prov.Subscribe(ctx, src, messageChannel)
    │       └── blocks until prov.WaitForConnect(ctx) returns true
    │
    ├── delivery goroutine      messageChannel → stream.Send (protected by mutex)
    │
    └── ack goroutines          one goroutine per Ack/Nack/Retry/DeadLetter message
```

All goroutines share `loopCtx` (derived from the stream context). Cancelling
`loopCtx` (via `loopCancel()` in the deferred cleanup) shuts down all
subordinate goroutines. A `stopForLoop` channel provides a second kill switch
for errors surfaced from goroutines that cannot return through the main select.

### Ack/Nack decision tree

```text
Consume{Ack} received
    │
    ├── ack.Uuid == ""          → error response, no broker call
    ├── ack.Nack && Delay > 0   → prov.Retry(ctx, src, uuid, delay)
    ├── ack.Nack && DeadLetterAddress in source.Options
    │                           → prov.DeadLetter(ctx, src, uuid)
    │                             (fall back to Nack if DeadLetter fails)
    ├── ack.Nack                → prov.Nack(ctx, uuid)
    └── (default)              → prov.Ack(ctx, uuid)

Result → ConsumeResponse{ConsumedResponse{Success, Uuid, Error?}}
```

---

## Phase 4: Disconnect

### Explicit Disconnect RPC

```text
client.Disconnect()
    └─► prov.Disconnect(ctx)       ← tears down broker channel for this client
        connectionMap.Delete(clientIdentifier)
```

### Implicit disconnect (context cancellation)

When the client drops the TCP connection or the gRPC stream ends abnormally:

- The `stream.Context()` is cancelled.
- All goroutines inside `Consume` detect `<-ctx.Done()` or `<-loopCtx.Done()`
and exit.
- `defer loopCancel()` and `defer close(stopForLoop)` run in `Consume`.
- The provider is not explicitly disconnected at this point — stale connections
are detected by the connection watcher (Phase 5).

---

## Phase 5: Background Cleanup (Connection Watcher)

A goroutine started in `server.init()` polls `connectionMap` every 30 seconds:

```text
for each clientIdentifier in connectionMap:
    provider.GetProvider(providerType)
        └── prov.ClientExists(clientIdentifier)
                NO  → connectionMap.Delete(clientIdentifier)
```

This handles the case where a client disappears without calling `Disconnect`
and the context cancellation path left the `connectionMap` entry behind.

---

## Health Monitoring and GOAWAY

The `Healthz.Check` bidirectional stream is independent of Producer/Consumer
sessions. Clients may open it at startup and keep it open for the process
lifetime.

```text
client.Healthz.Check(stream)
    │
    ├── notifyHealth(clientAddr, notifyHealthChan)    ← registers for internal broadcasts
    │
    ├── immediate send: HealthStatus{Code: OK, cpu%, mem%}
    │
    ├── recv goroutine
    │       ├── HealthCheck{Uuid}  → respond with current stats
    │       └── HealthStatus       → logged (future use)
    │
    └── notify loop (select)
            ├── <-notifyHealthChan  → send HealthStatus to client
            └── <-ctx.Done()       → exit
```

### Health status codes

| Code | Meaning | Recommended client action |
| --- | --- | --- |
| `OK` (0) | CPU and memory within thresholds | Continue normally |
| `UNHEALTHY` (1) | Resource pressure (CPU > 80% sustained or memory > 90%) | Reduce request rate; do not disconnect |
| `GOAWAY` (2) | HPA scale-up detected | Disconnect and reconnect (a new replica is available) |

### HPA-driven GOAWAY flow

```text
util.MonitorHPA goroutine
    └── detects desired replicas > current replicas
            └── healthChan <- HealthStatus_GOAWAY
                    │
                    └── server.MonitorHealthChan
                            └── for each registered notifier:
                                    notifier <- GOAWAY
                                        └── HealthzServer.Check sends GOAWAY to client
```

Handling `GOAWAY` is not mandatory, but clients that ignore it will remain
pinned to the current pod while new pods sit idle.

---

## Concurrency Safety Summary

| Shared resource | Protection mechanism |
| --- | --- |
| `connectionMap` | `util.ConcurrentMap` (sync.RWMutex internally) |
| `healthNotifiers` | `util.ConcurrentMap` |
| `stream.Send` in Consume | `streamSender` mutex (per-stream) |
| Provider singleton | `providerOnce` (sync.Once-like, atomic flag + mutex) |
| Per-client provider state | Keyed by `clientIdentifier`; provider implementation is responsible |
