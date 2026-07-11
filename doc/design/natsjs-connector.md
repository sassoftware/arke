# NATS JetStream Connector

## Purpose

This document describes the `natsjs` connector, a backend provider that lets
Arke run against [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream)
in place of (or alongside) RabbitMQ / AMQP 0.9.1. It is a worked example of
the contract in
[provider-connector-interface.md](provider-connector-interface.md): a second
`provider.Provider` registered under the name `natsjs` and selected per
connection via `ConnectionConfiguration.provider`.

The connector covers the publish / subscribe / ack / delayed-retry /
dead-letter / dedup / header-filter paths an AMQP client drives through Arke.
It maps each onto a native JetStream primitive where one exists and onto a
small amount of stateless proxy-side translation where it does not.

The guiding rule that keeps the translation honest:

> Arke may do stateless translation and orchestration of broker primitives. It
> must not become the system of record. Persistence, routing, and HA stay in
> the broker; dead-letter orchestration, delayed-retry, retry-count
> bookkeeping, and header filtering may move into the proxy.

## Subject and topology mapping

AMQP routes via an exchange plus routing-key bindings. NATS routes via subjects
with token wildcards. The connector maps the two as follows
(`helpers.go:publishSubjectFor` / `filterSubjectsFor`):

| AMQP concept | NATS mapping |
| --- | --- |
| exchange / address name | subject root (dots kept; it is a prefix) |
| — | `~` delimiter token, always inserted after the root |
| routing key | appended subject tokens |
| `#` (zero-or-more words) | `>` (NATS allows `>` only as the final token) |
| `*` (exactly one word) | `*` |

A message published to address `events.orders` with routing key
`region.us.created` travels on subject `events.orders.~.region.us.created`;
an empty routing key maps to the bare prefix `events.orders.~`. A JetStream
stream named `arke_<address>` captures `<address>.~` and `<address>.~.>`, and
each consumer filters on the mapped source subjects.

The `~` delimiter is what keeps distinct addresses' streams disjoint, which
JetStream requires: two subjects may belong to at most one stream. Address
names are themselves dotted, so two addresses can sit in a prefix
relationship (`events.orders` and `events.orders.filtered`); without the
delimiter their capture wildcards overlap, whichever stream is created first
wins, and the other address fails every publish and subscribe with `subjects
overlap with an existing stream` (err 10065). With it, the token after the
shared prefix differs (`~` vs `filtered`) and sanitization guarantees no
address token ever equals `~`, so any two distinct roots are disjoint. The
delimiter also prevents cross-address message leakage — without it,
publishing routing key `filtered.x` to `events.orders` is indistinguishable
from publishing `x` to `events.orders.filtered`.

NATS subjects are stricter than AMQP routing keys, so each token is also
sanitized. In binding patterns (`translateWildcards`): a non-terminal `#`
becomes `*` (NATS `>` is tail-only), illegal characters (space, tab, `*`,
`>`) inside a literal token become `_`, and empty tokens (from `a..b` or a
trailing dot) are dropped. A pattern whose trailing `#` became `>` also gets
the zero-word variant as a second filter subject (AMQP `#` matches zero or
more words, NATS `>` one or more, so binding `a.#` must match routing key
`a`). On the publish side routing keys are literal — AMQP gives `*`/`#`
meaning only in bindings, and NATS forbids wildcard tokens in published
subjects — so wildcard characters are sanitized to `_` like any other illegal
character. In address names, empty tokens and tokens equal to `~` are
replaced with `_` rather than dropped, so distinct names keep distinct roots.

This subject scheme is part of the connector's persistence contract:
retained messages are stored under these subjects for the life of the
stream's retention limits, so any future change to the encoding is a
breaking change for deployed data (`CreateOrUpdateStream` moves the stream's
captured subjects forward, after which messages stored under the previous
encoding no longer match any consumer filter and age out). Treat this layout
as canonical: external tooling that reads or writes JetStream subjects
directly must use the same mapping, including the `~` delimiter.

AMQP headers-exchange routing has no NATS subject equivalent. It is reproduced
proxy-side in `evaluateFilters`: multiple `Filter`s are OR'd (each is a
separate binding), and within a single filter the matches combine per
`Filter.Type` (`ALL` = and, `ANY` = or).

## Consumers: durable vs ephemeral

The connector chooses the consumer kind from the source
(`helpers.go:durableName`):

- Durable (work-queue) consumers use a stable name and persist across client
  reconnects, so a backlog published while the consumer is disconnected is
  redelivered on reconnect, matching RabbitMQ durable-queue semantics. Used
  for non-transient `QUEUE` sources, `STREAM` sources with a `ConsumerGroup`,
  and any `SingleActiveConsumer`.
- Ephemeral consumers auto-expire after an inactivity threshold. Used for
  auto-delete / exclusive / `TEMPORARY` sources, which clients commonly use
  for per-instance, transient subscriptions. They are also deleted eagerly
  when their subscription ends (or never starts — a declare-only call, or a
  failure to begin consuming), so the threshold only has to cover unclean
  exits and churning transient clients cannot accumulate dead consumers
  against the server's per-stream consumer limit.

When a subscription ends, its delivered-but-unresolved messages are released:
their acks could only have arrived on the consume stream that just closed, so
the connector drops its claim on them and (for durable consumers) naks them
so they redeliver promptly. This matches RabbitMQ, which requeues a closed
channel's unacked deliveries immediately; without the nak they would only
redeliver after the full ack wait.

The `DeliverPolicy` is taken from the `Offset` option, mirroring the amqp091
connector's offset vocabulary so both accept the same values: `first`/`continue`
-> deliver all, `last` -> the final message, `next` (or unset) -> deliver new,
and an absolute number -> start at that stream sequence. Any other value fails
the subscribe (as in amqp091's offset parsing) rather than silently starting
the consumer at a different position than it asked for. The offset only
applies on first creation: JetStream fixes a durable's start position when the
consumer is created, so a re-subscribe that requests a different offset logs a
warning and resumes from the durable's stored ack position — use a new durable
(source or `ConsumerGroup` name) to reposition. Only that start-position
conflict is absorbed; any other error creating or updating the consumer fails
the subscribe, since resuming a consumer whose configuration silently differs
from the requested one would consume the wrong way with no signal. Numeric
offsets are JetStream stream sequence numbers (as surfaced by `SourceStats`)
and are not portable across brokers.

## Ack, retry, and dead-letter mapping

The server translates a client's ack / nack / requeue into provider calls; the
connector maps those onto JetStream primitives:

| Client action | Provider call | NATS JetStream primitive |
| --- | --- | --- |
| ack | `Ack` | `msg.Ack()` |
| nack, `requeue_delay > 0` | `Retry(delay)` | `msg.NakWithDelay(delay)` |
| nack, `requeue_delay == 0`, DLA set | `DeadLetter` | publish to DLQ + `Term()` |
| nack, `requeue_delay == 0`, no DLA | `Nack` | `msg.Nak()` |

`NakWithDelay` is the native replacement for RabbitMQ's per-message-TTL +
dead-letter retry-queue idiom; JetStream increments the delivery count, which
the connector surfaces back to the client as the `x-retry-count` header (see
below). JetStream has no native dead-letter exchange, so `DeadLetter`
republishes the message to the configured dead-letter subject and then
`Term()`s it to stop redelivery. RabbitMQ performs that move broker-side;
here it is two proxy-side steps, so ordering is what protects the data: the
original is terminated only after the DLQ publish succeeds. If the DLQ stream
cannot be ensured or published to, `DeadLetter` returns an error and leaves
the message ack-pending — the server then falls back to a nack, so the
message is redelivered and dead-lettering is retried instead of the message
being lost. The DLQ copy carries a `Nats-Msg-Id` derived from the original's
stream sequence, so a retried dead-letter of the same message deduplicates in
the DLQ within its dedup window.

## Retry-count header

Clients that count retries via an `x-retry-count` header (rather than
RabbitMQ's `x-death`) work unchanged: `handleDelivery` synthesizes
`x-retry-count` from JetStream's `NumDelivered` metadata. This is the key piece
that lets an existing AMQP retry policy run against NATS with no `x-death`
equivalent.

## Deduplication

Publish-side dedup maps onto JetStream's `Nats-Msg-Id` plus the stream
`Duplicates` window. When a message carries a publisher name and/or publish id,
the connector sets `Nats-Msg-Id` so re-publishes within the window are
collapsed.

## Retention: work-queue vs append-log

This is the most important behavioral difference between the two brokers. A
RabbitMQ queue is a work queue: a message is deleted the instant a consumer
acks it. A JetStream stream is an append log: with the default `LimitsPolicy`
an acked message stays in the log and is only evicted when a configured limit
(`MaxAge`, `MaxBytes`, `MaxMsgs`) is reached.

To stop a stream growing without bound, `ensureStream` sets:

- `MaxAge` (default 72h, override with `NATSJS_STREAM_MAX_AGE`) — the time
  guard. It is the natural map for AMQP `MessageTTL` / `Expires` (both
  durations). It is mutable, so it also reins in streams created before a limit
  existed.
- `MaxBytes` (default unlimited, override with `NATSJS_STREAM_MAX_BYTES`) — an
  optional hard storage cap. With `discard=old` it only evicts near the cap, so
  recent backlog is preserved as long as possible.

There is a tension to respect: `MaxAge` evicts purely by clock, even with free
disk. If it is shorter than the longest tolerable consumer-outage window it
would silently delete a down consumer's backlog and regress the durable-queue
behavior. The default therefore exceeds any realistic outage, and `MaxBytes`
is intended as the primary disk guard.

## Operational resilience

Three mechanisms harden the connector against a cold or busy broker. All are
sized for a clustered (replicated) server, where creating topology also means
forming a raft group per stream and per durable consumer:

- **Bounded topology calls.** Stream and consumer creation carry an explicit
  deadline (`NATSJS_API_TIMEOUT`, default 30s) instead of the JetStream
  client's built-in 5s default. First-touch creation of a replicated stream
  has to finish raft formation and storage allocation before the API call
  returns, which can exceed 5s on cold or network-attached storage.
- **Collapsed concurrent creation.** `ensureStream` calls for the same stream
  are collapsed provider-wide: when many clients (re)connect at once — a
  proxy restart, a mass reconnect after a broker outage — exactly one
  `CreateOrUpdateStream` per stream is in flight at a time, and concurrent
  callers share its result. Failures are not cached, and success is still
  memoized per connection, so a fresh connection re-asserts its topology.
- **Consumer liveness.** Consumers run with a 5s idle heartbeat and a consume
  error handler. If the server stops serving a consumer's pulls (a broker
  restart, or a just-created consumer whose raft group is not yet serving),
  the missed heartbeat is logged at warn level and the client re-issues its
  pull request after roughly twice the heartbeat. With library defaults the
  same stall would go unlogged and take ~30s per detection cycle.

## Configuration

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `NATSJS_STREAM_REPLICAS` | `1` | Stream replication factor. Set to `3` against a clustered server for the HA equivalent of quorum queues. |
| `NATSJS_STREAM_MAX_AGE` | `72h` | Max age before messages are evicted (Go duration; `0` = keep forever). |
| `NATSJS_STREAM_MAX_BYTES` | `0` (unlimited) | Hard per-stream storage cap in bytes. |
| `NATSJS_API_TIMEOUT` | `30s` | Deadline for JetStream management API calls (stream / consumer creation). Go duration. |

TLS and credentials come from the standard `ConnectionConfiguration` (`Tls`,
`Credentials`) and the server's `tlsSkipVerify` flag, exactly as for the AMQP
connector; the broker certificate is verified against the system trust store.

## Feature-parity matrix

Legend: **Native** = NATS does it; **Proxy** = rebuilt in the connector;
**Drop** = not carried forward.

| RabbitMQ / Arke feature | Disposition | Notes |
| --- | --- | --- |
| Topic routing + wildcard bindings | Native | NATS subjects + `*`/`>`; `#`->`>`, `*`->`*`. |
| Per-subscriber ephemeral queue | Native | Ephemeral consumer with inactivity threshold. |
| Durable work queue (reconnect resumes backlog) | Native | Durable consumer for non-transient / consumer-group / single-active sources. |
| Publish confirms | Native | `js.PublishMsg` returns a `PubAck`. |
| Message dedup (`publish_id` + `publisher_name`) | Native | `Nats-Msg-Id` + stream `Duplicates` window. |
| Streams: offsets, start position | Native | JetStream is a log; `DeliverPolicy` maps `Offset`. |
| Single active consumer | Native | Consumer config (needs a durable name). |
| Prefetch / QoS | Native | `MaxAckPending`; prefetch 0 (AMQP "unlimited") maps to unlimited (-1). |
| HA / quorum queues | Native | JetStream R3 (Raft) via `NATSJS_STREAM_REPLICAS`. |
| Delayed retry (per-msg TTL + DLX idiom) | Proxy -> Native | Replaced with `NakWithDelay`. |
| Retry-count header (`x-retry-count`) | Proxy | Synthesized from JetStream `NumDelivered`. |
| Dead-letter (DLX) | Proxy | No native DLX; republish to DLQ subject then `Term()`. |
| Header-filter exchange (`Filter` / `Match`) | Proxy | NATS routes on subject; evaluated in `evaluateFilters` (see limitations). |
| `MessageTTL` / `Expires` | Partial | Mapped onto stream-level `MaxAge` (see limitations). |
| Source stats (depth / consumers) | Native | JetStream stream / consumer `Info`. |
| Publish / deliver rates in stats | Proxy | Sampled: counter deltas between `SourceStats` calls (see limitations). |
| RabbitMQ management HTTP API | Drop | Replaced by the JetStream API over NATS itself. |

## Source options

Per the connector-interface contract, `SupportedSourceOptions()` advertises
the `Source.Options` keys the connector accepts — the same list as the
amqp091 connector, so existing client sources validate unchanged:

| Key | Type | Description |
| --- | --- | --- |
| `MessageTTL` | string (ms) | Accepted for compatibility; not applied per source — retention is the stream-wide `NATSJS_STREAM_MAX_AGE` (see Known limitations). A warning is logged at subscribe time. |
| `Expires` | string (ms) | Accepted for compatibility; not applied — streams are shared per address root and are not deleted when a source goes unused. A warning is logged at subscribe time. |
| `DeadLetterAddress` | string | Address whose stream receives dead-lettered messages. |
| `DeadLetterSubject` | string | Routing key for dead-lettered messages. |
| `Offset` | string | Stream starting offset (`first`, `continue`, `last`, `next`, or a numeric sequence). |
| `ConsumerGroup` | string | Durable consumer group name (Streams only). |

## Known limitations

- **Per-source TTL fidelity.** The connector uses one stream per address
  root, so a per-source `MessageTTL` / `Expires` cannot be mapped onto the
  shared stream without one source's value flapping another's retention.
  Both options are therefore accepted but not applied, and a subscribe that
  sets either logs a warning naming the source — silent divergence in data
  retention is the one place a client must not have to read a design doc to
  notice. Retention comes from the stream-wide `NATSJS_STREAM_MAX_AGE` /
  `NATSJS_STREAM_MAX_BYTES` configuration. True per-source TTL (or switching
  queue sources to a delete-on-ack policy such as `WorkQueuePolicy` /
  `InterestPolicy`) needs a
  per-source stream topology, and `Retention` is immutable, so that is a
  stream-recreate migration rather than an in-place change.
- **Publish / deliver rates are sampled, not native.** JetStream exposes
  absolute counters, not rates, so `SourceStats` differences them between
  successive calls: the first call after a (re)connect returns zero, the
  publish rate covers the whole address stream rather than a single binding,
  and sources without a durable consumer report a zero deliver rate. The
  message count for a durable source is the consumer's backlog (undelivered
  plus unacked, with `current_offset` from its ack floor) rather than the
  stream depth — the stream retains acked messages under its retention
  limits, so its depth keeps growing after consumers catch up, which would
  mislead anything using message count as queue length (e.g. consumer
  autoscaling). Sources without a durable consumer fall back to the stream
  view.
- **Header filters are evaluated proxy-side.** NATS routes on subjects only,
  so a source's header `Filter`s cannot narrow what the server delivers:
  every message matching the source's subject filters is delivered to the
  connector, which evaluates the headers and acks non-matching messages
  without forwarding them. Bandwidth and CPU between broker and proxy
  therefore scale with the subject-matched traffic, not the header-matched
  traffic. That is fine when header filters refine an already-narrow subject,
  but a high-volume address consumed almost entirely through header filters
  should be remodeled onto routing keys (subjects) instead.
- **Dead-letter is a proxy-side republish.** The DLQ is an ordinary stream
  fed by the connector; there is no advisory-driven re-consumption. A failed
  DLQ publish fails the dead-letter call — the message stays in flight and is
  redelivered — rather than dropping the message.
- **Connection authentication.** The connector supports user/password and TLS
  today; NKEYs / JWT auth are a natural follow-up for production deployments.

## Testing

Unit tests for the pure mapping logic (subject / wildcard translation,
durable-name selection, header-filter evaluation) live in `helpers_test.go`.
Behavioral tests in `natsjs_test.go` drive the publish / subscribe / ack /
retry / dead-letter / dedup paths against an in-process JetStream-enabled
`nats-server`, so they run without an external broker.

## Migration approach

Run `natsjs` as a second provider alongside `amqp091` — the `provider` field
in `ConnectionConfiguration` already supports per-connection selection. Migrate
one low-risk message class, measure cost and latency, then expand. Because the
proxy absorbs the broker-specific translation, the migration is incremental and
reversible, and there is no need to remove the AMQP connector to begin.
