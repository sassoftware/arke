# Provider / Connector Interface Contract

## Purpose

This document specifies the contract that any backend message broker
connector must satisfy to be registered with Arke. It is the primary
reference for contributors who want to add support for a new broker
(e.g. Kafka, MQTT, Azure Service Bus).

---

## Overview

Arke's broker independence is provided by the `internal/provider` package,
which defines a `Provider` interface and a global registry. At startup, one
or more connector packages call `provider.Register()` from their `init()`
functions. When a client connects, the `server` package calls
`provider.GetProvider(type)` to retrieve (or lazily create) the matching
singleton.

<!-- markdownlint-disable MD013 -->
```text
provider.Register("amqp091", amqp091Factory)   ← called in amqp091/connectors.go init()

provider.GetProvider("amqp091")
  └── if not cached → provider.NewProvider("amqp091")
        └── providerOnce.Do(amqp091Factory())  ← exactly once per provider type
              └── cached in registeredProviders map
```
<!-- markdownlint-enable MD013 -->

---

## The `Provider` Interface

Location: [`internal/provider/provider.go`](../../internal/provider/provider.go)

<!-- markdownlint-disable MD013 -->
```go
type Provider interface {
    // Connect establishes a connection to the backend broker using the supplied
    // ConnectionConfiguration. tlsSkipVerify controls whether TLS certificate
    // validation is skipped on the broker connection.
    Connect(ctx context.Context, cfg *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error

    // Disconnect tears down the broker connection for the client identified by
    // the context.
    Disconnect(ctx context.Context)

    // WaitForConnect blocks until the broker connection for the current client
    // is established or the context is cancelled. Returns true if connected.
    WaitForConnect(ctx context.Context) bool

    // Publish receives messages from inChan and publishes them to the broker.
    // Errors encountered per-message are written to errChan. Returns a fatal
    // *pb.Error if the stream itself fails.
    Publish(ctx context.Context, inChan <-chan *pb.Message, errChan chan<- *pb.Error) *pb.Error

    // PublishOne publishes a single message synchronously.
    PublishOne(ctx context.Context, msg *pb.Message) *pb.Error

    // Subscribe attaches to the given Source and writes received messages to
    // outChan. Blocks until the source is exhausted, the context is cancelled,
    // or a fatal error occurs.
    Subscribe(ctx context.Context, src *pb.Source, outChan chan<- *pb.Message) *pb.Error

    // Ack acknowledges successful processing of the message with the given UUID.
    Ack(ctx context.Context, uuid string) *pb.Error

    // Nack negatively acknowledges the message with the given UUID, returning
    // it to the broker for redelivery or discard.
    Nack(ctx context.Context, uuid string) *pb.Error

    // Retry requeues the message with the given UUID after requeueDelay seconds.
    Retry(ctx context.Context, src *pb.Source, uuid string, requeueDelay int32) *pb.Error

    // DeadLetter moves the message with the given UUID to the dead-letter
    // destination configured on src.
    DeadLetter(ctx context.Context, src *pb.Source, uuid string) *pb.Error

    // SupportedSourceOptions returns a map of Source option keys that this
    // provider understands. The server validates incoming Source.Options
    // against this map and rejects unknown options before calling Subscribe.
    SupportedSourceOptions() map[string]bool

    // ClientExists returns true if the broker still considers the given
    // client identifier active. Used by the connection watcher to evict dead
    // sessions from connectionMap.
    ClientExists(clientID string) bool

    // Stats returns current per-client metrics (active messages, streams,
    // produced/consumed counts).
    Stats() *Stats

    // SourceStats returns metrics for an individual Source (queue depth,
    // stream offset, etc.).
    SourceStats(ctx context.Context, src *pb.Source) *pb.SourceStats
}
```
<!-- markdownlint-enable MD013 -->

---

## Factory Registration

Every connector package must expose a zero-argument factory function and
register it during package initialisation:

```go
// internal/provider/connectors/mybroker/connectors.go

package mybroker

import "github.com/sassoftware/arke/internal/provider"

func init() {
    provider.Register("mybroker", func() provider.Provider {
        return newMyBrokerProvider()
    })
}
```

The `provider.Register` function is idempotent — duplicate registrations
for the same name are silently ignored.

---

## Activating a Connector

A connector is activated by blank-importing its package in the
[connectors.go](../../internal/provider/connectors/connectors.go) file:

```go
import (
    _ "github.com/sassoftware/arke/internal/provider/connectors/mybroker"
)
```

Without this import the `init()` function never runs and
`provider.GetProvider("mybroker")` returns an error.

---

## Provider Lifecycle

<!-- markdownlint-disable MD013 -->
```text
client Connect(cfg) RPC
    │
    └─► brokerConnect(ctx, cfg, tlsSkipVerify)
            │
            ├─► provider.GetProvider(cfg.Provider)  ← singleton, shared across clients
            │
            └─► prov.Connect(ctx, cfg, tlsSkipVerify)
                    │  (stores per-client state keyed by clientIdentifier)
                    └─► WaitForConnect(ctx)  ← future RPCs block here until ready

... (Publish / Subscribe RPCs use the same provider singleton) ...

client Disconnect() RPC  OR  context cancellation
    └─► prov.Disconnect(ctx)  ← client-scoped teardown; provider singleton lives on
```
<!-- markdownlint-enable MD013 -->

**Important:** The `Provider` singleton is shared across all clients of
that broker type. Implementations must store any per-client state keyed by
the `clientIdentifier` extracted from the context
(`util.GetClientIdentifier(ctx)`), not as struct-level fields.

---

## Source Options Contract

`SupportedSourceOptions()` must return every key that `Subscribe` reads
from `pb.Source.Options`. The server rejects subscription requests that
contain keys not present in this map, surfacing the error to the client
before the provider is called.

Defined option keys for the current AMQP 0.9.1 provider:

| Key | Type | Description |
| --- | --- | --- |
| `MessageTTL` | string (ms) | Per-queue message TTL |
| `Expires` | string (ms) | Queue expiry when unused |
| `DeadLetterAddress` | string | Exchange for dead-lettered messages |
| `DeadLetterSubject` | string | Routing key for dead-lettered messages |
| `Offset` | string | Stream starting offset (Streams only) |
| `ConsumerGroup` | string | Consumer group name (Streams only) |

New connectors should document their supported options in the same table
format here and export accurate keys from `SupportedSourceOptions()`.

---

## Error Handling Conventions

- Methods return `*pb.Error`, never a Go `error`. A `nil` return means success.
- `pb.Error.IsFatal = true` signals to the server that the stream must be
  closed and the client notified.
- `pb.Error.IsFatal = false` represents a per-message error; the stream
  may continue.
- Implementations must not panic. If a panic is unavoidable, wrap the
  goroutine body with `util.RecoverPanic()`.

---

## Concurrency Requirements

<!-- markdownlint-disable MD013 -->
| Expectation | Detail |
| --- | --- |
| `Connect` / `Disconnect` | Must be safe to call from multiple goroutines concurrently (different clients) |
| `Publish` / `PublishOne` / `Subscribe` | Called from a single goroutine per client but possibly many clients at once |
| `Ack` / `Nack` / `Retry` / `DeadLetter` | May be called concurrently from the ack-handling goroutine in `ConsumerServer.Consume` |
| `Stats` / `SourceStats` | Read-only; must not block indefinitely |
| `ClientExists` | Read-only; called from the connection watcher goroutine every 30 s |
<!-- markdownlint-enable MD013 -->

---

## Testing a New Connector

1. **Unit tests** – Place in
   `internal/provider/connectors/<name>/<name>_test.go`. Mock the broker
   client using an interface shim (see `amqp091shim.go` for the pattern).
2. **Integration tests** – Add a Docker Compose service for the broker in
   `tests/integration/docker-compose*.yml` and ensure the integration
   tests run successful against all supported brokers.
3. **Registration smoke test** – Verify
   `provider.GetProvider("<name>")` returns a non-nil provider after
   blank-importing the connector package.
