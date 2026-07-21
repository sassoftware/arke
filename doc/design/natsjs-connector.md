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

The stream for an address is asserted on use: a subscribe (or declare-only
call) creates it, and so does a publish — with one exception. A unary publish
to a `STREAM` address requires the stream to exist already, exactly as
amqp091's stream publisher refuses a stream nobody declared: declaring a
stream is its readers' job, and auto-creating it would turn a typo'd address
name into a junk stream storing messages no reader will ever see. The
streaming publish path does not check, matching amqp091, which sends every
address type over an auto-declared exchange without error.

Stream names (and durable consumer names, which JetStream validates the same
way) may not contain `.`, whitespace, `*`, `>`, `/` or `\`, so they are
derived from the address (or source / consumer-group) name by swapping dots
for underscores: `events.orders` becomes `arke_events_orders`. That
replacement alone is ambiguous the moment a name contains a literal `_` —
`a.b` and `a_b` would both read `arke_a_b`, and two addresses colliding onto
one stream name silently reconfigure each other's stream — so such names
take an escaped form instead, under the disjoint `arke-` prefix, in which
every `_` starts a two-character escape code (`_d` for `.`, `_u` for `_`,
and codes for the other characters JetStream rejects in names). Distinct
names therefore always yield distinct streams and durables, while names free
of underscores keep the readable historical form.

The `~` delimiter is what keeps distinct addresses' streams disjoint, which
JetStream requires: two subjects may belong to at most one stream. Address
names are themselves dotted, so two addresses can sit in a prefix
relationship (`events.orders` and `events.orders.filtered`); without the
delimiter their capture wildcards overlap, whichever stream is created first
wins, and the other address fails every publish and subscribe with `subjects
overlap with an existing stream` (err 10065). With it, the token after the
shared prefix differs (`~` vs `filtered`) and token escaping (below)
guarantees no address token ever equals `~`, so any two distinct roots are
disjoint. The delimiter also prevents cross-address message leakage —
without it, publishing routing key `filtered.x` to `events.orders` is
indistinguishable from publishing `x` to `events.orders.filtered`.

NATS subjects are stricter than AMQP routing keys, so each address and
routing-key token is escaped — injectively, so distinct AMQP names never
merge onto one subject. A token free of reserved characters (`~`,
whitespace, `*`, `>`, `#`) passes through unchanged; every conventional
dotted name keeps its readable, historical form. Any other token takes an
escaped form marked by a leading `~` — no plain token can start with `~`,
since `~` is itself reserved — in which each reserved character becomes a
two-character `~` code (`~~` for `~`, `~w` space, `~t` tab, `~a` `*`, `~g`
`>`, `~h` `#`, plus codes for the remaining whitespace characters); an empty
address token (from consecutive dots) becomes `~e`. The escaped form decodes
unambiguously, which is what makes the mapping injective: a lossy
replacement (`_` for every illegal character) would merge distinct addresses
— `a.~.b`, `a..b`, `a.*.b` and `a._.b` — onto one root, and addresses that
share a root share a stream: each receives the other's traffic and each
ensure reconfigures the other's stream. The same escaping keeps distinct
routing keys distinct, so a binding on `a_b` no longer also matches
published keys `a b` or `a*b`.

In binding patterns (`translateWildcards`): runs of consecutive `#` collapse
into one first (they are equivalent in AMQP, and `#.#` must keep the
zero-word match its trailing `#` provides), a non-terminal `#` becomes `*`
(NATS `>` is tail-only), literal tokens are escaped exactly like published
tokens (so a literal binding matches exactly the published keys it matches
on RabbitMQ), and empty tokens (from `a..b` or a trailing dot) are dropped —
on the publish side too, so both sides agree. A pattern whose trailing `#`
became `>` also gets the zero-word variant as a second filter subject (AMQP
`#` matches zero or more words, NATS `>` one or more, so binding `a.#` must
match routing key `a`). On the publish side routing keys are literal — AMQP
gives `*`/`#` meaning only in bindings, and NATS forbids wildcard tokens in
published subjects — so wildcard characters are escaped like any other
reserved character. For `Address_QUEUE` (AMQP direct-exchange parity),
binding subjects are exact routing keys rather than topic patterns: `#` and
`*` are escaped literals and match only themselves, instead of matching as
wildcards.

A redundant binding set — a wildcard binding alongside a specific key it
already covers, such as `orders.#` with `orders.created` — is legal in AMQP,
where a message is routed to the queue once no matter how many bindings
match. JetStream instead rejects a consumer whose filter subjects overlap
(one being a subset of another), so the connector collapses the mapped
filters to the widest set before creating the consumer; the surviving
wildcard filter matches everything the dropped filters did. Intersecting
patterns where neither contains the other are kept as-is.

A source whose address carries *no* subjects declares no bindings and so
receives nothing, matching amqp091 (which binds nothing at all in that case).
A JetStream consumer must carry at least one filter subject, so "nothing" is
expressed as a filter on a subject no published subject can contain — a token
the escaping above can never emit. Three cases are deliberately not that:

- an empty binding key (`""`, as distinct from no keys) is a literal, and
  selects the empty routing key only, exactly as it matches on a topic or
  direct exchange;
- a `STREAM` source is not bound to its address at all — amqp091 reads a
  RabbitMQ stream by name and never declares a binding — so it reads the
  whole log;
- a headers address with filters selects the whole address, because a headers
  exchange ignores routing keys and the header match decides. amqp091 binds a
  single `""` key there purely to have somewhere to hang the header
  arguments; `evaluateFilters` is this connector's stand-in for those.

A headers address selects the whole address whether or not it declares
binding keys, for that same reason: a headers exchange matches every one of
its bindings on their header arguments alone and never reads the routing key,
so keys declared on one decide nothing and must not narrow the consumer.
They still travel to the parent in an address-to-address binding, where the
*parent's* type decides how they are matched — a topic parent matches them as
routing keys, a headers parent ignores them in turn.

### Address-to-address binding

An address may name a `ParentAddress`, binding it to that parent: what is
published to the parent under the child's binding keys is routed on to the
child, and reaches the child's own consumers. On a broker that routes, this is
a routing rule. Here the stream *is* the storage, so the equivalent is to
source the bound subjects out of the parent's stream into the child's
(`ensureStreamFor`), keeping the subject each message was published under
rather than transforming it. That means only the parent's stream ever listens
on the parent's subjects — no two streams claim one subject space — while a
message routed in from the parent is indistinguishable, to a consumer, from
one published to the child directly. The child's consumers filter on both
their address's subjects and the bound parent subjects.

The binding keys are the child's own subjects, matched by the *parent*
exchange's type, so they translate exactly like any other binding. Bindings
accumulate: declaring one never removes another, as on RabbitMQ, so a second
subscriber binding a different key on the same address adds to the set rather
than replacing it. Nothing unbinds — a binding outlives the subscription that
declared it, as an AMQP exchange-to-exchange binding outlives the client that
bound it. That holds for assertions carrying no binding at all, which is the
ordinary case: a publisher names the bound address directly and so has no
parent to declare. Because JetStream replaces a stream's source set wholesale
on update, every assertion reads the current set and writes it back, rather
than sending only what the caller happens to know about.

Reading and writing back makes an assertion a read-modify-write, and the lock
that serializes it is process-local while Arke runs more than one replica. So
an assertion writes only when it would actually change something: it compares
the live configuration against the one it built and returns early if the
stream already satisfies it. That takes the overwhelmingly common case — a
bare assertion against a steady-state stream, which is every publisher's
first touch and every reconnect — out of the race entirely, leaving only
callers that genuinely change the configuration, which for bindings are
symmetric (both are adding). A stream created before a limit existed is still
retrofitted: its configuration differs, so the update runs. Two concurrent
*binding* declarations across replicas remain a real, much narrower window;
closing it would need a compare-and-swap JetStream does not offer.

This subject scheme — and the stream/durable name encoding above — is part
of the connector's persistence contract: retained messages are stored under
these subjects for the life of the stream's retention limits, so any future
change to the encoding is a breaking change for deployed data
(`CreateOrUpdateStream` moves the stream's captured subjects forward, after
which messages stored under the previous encoding no longer match any
consumer filter and age out; a renamed stream or durable simply strands the
old one and its state). Treat this layout as canonical: external tooling
that reads or writes JetStream subjects directly must use the same mapping,
including the `~` delimiter. Deployments whose address or source names
contain `_`, `/` or `\` predating the escaped name form had those names
colliding or rejected outright, so the escaped form changes only names that
were already broken. Likewise, addresses and routing keys containing
reserved subject characters (whitespace, `*`, `>`, `#`, `~`, consecutive
dots) predating token escaping either collided with each other's `_`
spellings or produced invalid subjects, so escaping changes the stored
subjects only where the previous mapping was already wrong; names and keys
made of conventional tokens map exactly as before.

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
  for per-instance, transient subscriptions — and for `STREAM` sources
  without a `ConsumerGroup`, whose subscribers are independent readers of
  the shared log, each positioned by its own `Offset` (RabbitMQ stream
  consumers do not compete). The exception is a group-less `STREAM` source
  asking for `Offset: continue`, which is a request to resume where that
  source last stopped and so needs a position the broker keeps between
  subscriptions: it gets a durable named after the source, the way amqp091
  answers `continue` from RabbitMQ Streams' server-side offset tracking
  (keyed by consumer name). Every other offset positions the reader from the
  log itself and stays ephemeral. They are also deleted eagerly
  when their subscription ends (or never starts — a declare-only call, or a
  failure to begin consuming), so the threshold only has to cover unclean
  exits and churning transient clients cannot accumulate dead consumers
  against the server's per-stream consumer limit.

A source with `SingleActiveConsumer` additionally maps onto a pinned-client
priority group (nats-server 2.11+): the server pins the first subscriber to
pull and delivers only to it, standbys' pulls wait, and when the pinned
client stops pulling for `NATSJS_SAC_PINNED_TTL` the pin moves to a standby.
The durable such a source attaches to is named after its `ConsumerGroup`
option when set: the group is the identity the instances coordinate through
(amqp091 uses it as the single-active consumer reference the same way), so
sources that share one name but split their subscribers into groups — one
independently ordered group per partition of a shared stream — get one
pinned consumer per group instead of collapsing onto a single pin that
starves every other group. Without a group, a single-active queue source
falls back to its own name (all instances of a single-active queue share the
queue by definition), and a single-active stream source is rejected at
subscribe time, as in amqp091.
That reproduces RabbitMQ's single-active-consumer semantics — ordered
processing across competing instances with automatic failover — natively,
with two caveats. Failover is bounded by the pinned TTL rather than
instantaneous on disconnect, and the TTL must comfortably exceed the pull
re-issue cadence (~30s with the client defaults) or the pin flaps. And on a
server that predates priority groups the config fields are silently dropped,
so single-active cannot be enforced; the connector detects that from the
effective consumer config and logs a warning that consumers will compete.
Priority-group config is update-mutable, so a durable created before its
source set `SingleActiveConsumer` is upgraded in place, keeping its name and
ack position. The reverse transition needs an extra step: a durable coming
OFF `SingleActiveConsumer` has its pin released explicitly (`UnpinConsumer`)
before the update clears the priority-group config, because unpinning AFTER
the group is gone from the config is rejected by the server ("priority group
does not exist for this consumer"). Skipping that step leaves the previous
pin as server-side state independent of the declared config, silently
blocking ordinary delivery to the downgraded consumer for up to
`NATSJS_SAC_PINNED_TTL` with no error anywhere.

When a subscription ends, its delivered-but-unresolved messages are released:
their acks could only have arrived on the consume stream that just closed, so
the connector drops its claim on them and (for durable consumers) naks them
so they redeliver promptly. This matches RabbitMQ, which requeues a closed
channel's unacked deliveries immediately; without the nak they would only
redeliver after the full ack wait.

The `DeliverPolicy` is taken from the `Offset` option, mirroring the amqp091
connector's offset vocabulary so both accept the same values: `first` ->
deliver all, `last` -> the final message, `next` (or unset) -> deliver new,
`continue` -> resume from the position the source's durable holds (deliver all
on its first creation, when there is no position yet — RabbitMQ answers a
`continue` with no stored offset the same way), and an absolute number ->
start at that offset. Any other value fails the subscribe (as in amqp091's
offset parsing) rather than silently starting the consumer at a different
position than it asked for. The offset only applies on first creation:
JetStream fixes a durable's start position when the consumer is created, so a
re-subscribe that requests a different offset logs a warning and resumes from
the durable's stored ack position — use a new durable (source or
`ConsumerGroup` name) to reposition. Only that start-position conflict is
absorbed; any other error creating or updating the consumer fails the
subscribe, since resuming a consumer whose configuration silently differs from
the requested one would consume the wrong way with no signal.

A numeric offset counts from 0, naming a message's position in the log the way
a RabbitMQ Stream offset does — not the raw JetStream sequence, which counts
from 1. The connector converts in both directions, so an offset read from
`SourceStats` and handed back as a source's `Offset` names the same message it
did on either broker. The value is still not portable *across* brokers: offset
7 is the eighth message of whichever log is being read, and two brokers' logs
hold different messages.

## Ack, retry, and dead-letter mapping

The server translates a client's ack / nack / requeue into provider calls; the
connector maps those onto JetStream primitives:

| Client action | Provider call | NATS JetStream primitive |
| --- | --- | --- |
| ack | `Ack` | `msg.Ack()` |
| nack, `requeue_delay > 0` | `Retry(delay)` | `msg.NakWithDelay(delay)` |
| nack, `requeue_delay == 0`, DLA set | `DeadLetter` | publish to DLQ + `Term()` |
| nack, `requeue_delay == 0`, no DLA | `Nack` | `msg.Term()` |

Note which primitive a plain nack maps to. A nack on this contract is a
rejection, not a requeue: amqp091 answers the same call with
`Delivery.Nack(requeue=false)`, so RabbitMQ drops the message, or moves it to
the queue's dead-letter exchange — which the server here asks for explicitly
through `DeadLetter` instead. JetStream's `Nak` means the opposite, redeliver
now, so using it for a nack turns a single nacked message into an unbounded
delivery loop: a client that rejected a message rejects each redelivery
straight back, at thousands of deliveries a second for as long as the
subscription lives. `Term` carries the intended meaning — stop redelivering —
and `Retry` remains the way to ask for redelivery. The one exception is the
server's fallback nack after a failed `DeadLetter` (below), whose purpose is
to put the message back so dead-lettering can be retried; `DeadLetter` marks
the message before returning that error, and a nack of a marked message naks.

A delivered message that is neither acked nor nacked redelivers after the
consumer's ack wait (`NATSJS_ACK_WAIT`, default 30s). That value sets one
dial between two failure modes: a crashed client's in-flight messages are
stuck until the ack wait passes before another consumer gets them (shorter
is better), while a healthy consumer that holds a message longer than the
ack wait — including time spent queued in the client-side pull buffer behind
a large prefetch — gets a duplicate redelivery (longer is better). The
default favors failover; RabbitMQ's equivalent consumer timeout defaults to
30 minutes, so deployments with legitimately slow consumers should raise it.

`NakWithDelay` is the native replacement for RabbitMQ's per-message-TTL +
dead-letter retry-queue idiom; JetStream increments the delivery count, which
the connector surfaces back to the client as the `x-retry-count` header (see
below). JetStream has no native dead-letter exchange, so `DeadLetter`
republishes the message to the dead-letter address — under the message's
original routing key unless `DeadLetterSubject` overrides it, exactly as
RabbitMQ dead-letters under the original key unless
`x-dead-letter-routing-key` is set — and then `Term()`s it to stop
redelivery. RabbitMQ performs that move broker-side;
here it is two proxy-side steps, so ordering is what protects the data: the
original is terminated only after the DLQ publish succeeds. If the DLQ stream
cannot be ensured or published to, `DeadLetter` returns an error and leaves
the message ack-pending — the server then falls back to a nack, so the
message is redelivered and dead-lettering is retried instead of the message
being lost. A present but empty `DeadLetterAddress` is treated the same way:
the connector returns an error and leaves the original in flight rather than
terminating it without a DLQ publish.

That fallback nack is delayed by `NATSJS_DEADLETTER_RETRY_DELAY` (default 5s)
rather than issued immediately. The retry runs the same dead-letter attempt
against the same configuration, so a failure that does not clear on its own —
a `DeadLetterAddress` that cannot be resolved into a stream, or one set to the
empty string — would otherwise spin the message between server and client as
fast as the client can nack it, and nothing bounds that loop (`MaxDeliver` is
deliberately left unset so a slow consumer is never silently cut off). Pacing
it keeps a transient failure recovering promptly while a permanent one costs
one redelivery per interval instead of hundreds a second.

The DLQ copy carries a `Nats-Msg-Id`
derived from the original's stream sequence, so a retried dead-letter of the
same message deduplicates in the DLQ within its dedup window, and the
`x-retry-count` the consumer saw when it gave the message up — RabbitMQ's
broker-side move preserves the death trail in `x-death`, and the retry count
is this connector's equivalent (a plain republish would lose it).

### Rejected alternative: advisory-driven dead-lettering

JetStream publishes advisories when a consumer exhausts `MaxDeliver`
(`$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>`) or when a
message is terminated (`…MSG_TERMINATED.…`), and a common NATS idiom builds a
"dead-letter queue" as an ordinary stream capturing those subjects. That
idiom was considered and rejected for this connector.

An advisory carries metadata, not the message: its payload names the origin
`stream_seq`, so a reader has to fetch the original out of the primary stream
to see the body or headers. A dead-letter consumer would then have to speak
two protocols — advisory JSON, then a stream fetch — where its AMQP
counterpart just consumes the dead-lettered message off a queue. It also
inherits the primary stream's retention: once `MaxAge` evicts the original,
the advisory still resolves to nothing, so the dead-letter record decays into
a dangling pointer exactly when it is most likely to be read.

Republishing the message itself keeps the DLQ a normal address that an
unmodified AMQP client can consume, preserves the body, headers and
`x-retry-count`, and gives the dead-letter copy retention independent of the
original. The cost is that the move is two proxy-side steps rather than one
broker-side one, which the ordering above is designed to make safe.

The advisory subjects would become relevant only if this connector set
`MaxDeliver`, as the hook for routing a delivery-exhausted message into the
same republish path instead of letting the consumer silently skip it. It does
not set `MaxDeliver` — see the redelivery limitation below.

## Retry-count header

Clients that count retries via an `x-retry-count` header (rather than
RabbitMQ's `x-death`) work unchanged: `handleDelivery` synthesizes
`x-retry-count` from JetStream's `NumDelivered` metadata. This is the key piece
that lets an existing AMQP retry policy run against NATS with no `x-death`
equivalent.

## Deduplication

Publish-side dedup maps onto JetStream's `Nats-Msg-Id` plus the stream
`Duplicates` window. When a message carries a positive publish id, it must
also carry a publisher name (matching the stream-publish contract of the AMQP
connector); the connector sets `Nats-Msg-Id` from that pair so re-publishes
within the window are collapsed. A publisher name by itself does not enable
deduplication.

Dedup belongs to `STREAM` addresses, and only to them. JetStream would happily
deduplicate on any address — dedup here is a property of the stream, not of
the address type — but RabbitMQ gets it from the Streams client, which is the
only thing the AMQP connector reaches for a `STREAM` address. So the two paths
mirror that connector rather than what JetStream can do:

- a **unary publish** (`PublishOne`) carrying a publish id for any other
  address type — `TOPIC`, `QUEUE` or `FILTER` — is refused, because the AMQP
  connector refuses it. `TOPIC` is the case to watch: it is the protobuf zero
  value, so an address that never sets a type is refused too;
- a **streaming publish** carrying one *ignores* it, because the AMQP
  connector's streaming path hands every non-`STREAM` address to a plain
  channel publish that drops the id on the floor. Refusing would be stricter
  than RabbitMQ, and honouring it would hand the client a guarantee that
  disappears the moment it runs there.

## Retention: work-queue vs append-log

This is the most important behavioral difference between the two brokers. A
RabbitMQ queue is a work queue: a message is deleted the instant a consumer
acks it. A JetStream stream is an append log: with the default `LimitsPolicy`
an acked message stays in the log and is only evicted when a configured limit
(`MaxAge`, `MaxBytes`, `MaxMsgs`) is reached.

To stop a stream growing without bound, `ensureStream` sets:

- `MaxAge` (default 72h, override with `NATSJS_STREAM_MAX_AGE`) — the time
  guard. It is the natural map for AMQP `MessageTTL` (a message-age duration;
  `Expires` is queue disuse and maps to the consumer instead — see Source
  options). It is mutable, so it also reins in streams created before a limit
  existed.
- `MaxBytes` (default unlimited, override with `NATSJS_STREAM_MAX_BYTES`) — an
  optional hard storage cap. With `discard=old` it only evicts near the cap, so
  recent backlog is preserved as long as possible.

There is a tension to respect: `MaxAge` evicts purely by clock, even with free
disk. If it is shorter than the longest tolerable consumer-outage window it
would silently delete a down consumer's backlog and regress the durable-queue
behavior. The default therefore exceeds any realistic outage, and `MaxBytes`
is intended as the primary disk guard.

## Design decision: storage at the address, not the source

The connector maps an address to a stream and a source to a consumer, so
message storage sits one level higher than AMQP puts it. In AMQP an exchange
is a stateless routing function and the queue owns storage *and* per-queue
policy — `MessageTTL`, max-length, dead-lettering, delete-on-ack. In
JetStream the stream owns storage and policy, and a consumer is only a
cursor over it. Mapping address to stream therefore trades per-source policy
away, and this section records why that trade is deliberate, because the
obvious alternative is more attractive in outline than in fact.

The consequences accepted are: per-source `MessageTTL` and max-length cannot
be honored (a shared stream cannot carry one source's duration without
flapping it for every other source on the same address); acked messages stay
in the log until a limit evicts them rather than being deleted on ack; and
publish-rate statistics are per address rather than per source.

A fourth consequence is subtler and has no clean answer: a transient source
becomes an ephemeral consumer, and no start position for it is faithful. In
AMQP the existence of a queue defines what is retained *for that queue* — a
transient queue holds nothing from before it was declared, and is deleted
when its last consumer leaves. A stream instead retains everything the
address received, regardless of who is listening. `DeliverNew` is therefore
wrong whenever a transient source stands in for a queue that ought to have
existed already and accumulated: a dead-letter target consumed only after
the fact is the clearest case, where the messages are in the stream and the
ephemeral consumer starts past them. `DeliverAll` is wrong in the opposite
direction, replaying history a freshly declared transient queue could never
have held, bounded only by `MaxAge`. The connector picks `DeliverNew`
because replaying a busy address's whole retained log into a temporary
consumer is the more damaging error, but this is a trade rather than a
mapping. A source that must not miss earlier messages should be durable —
non-auto-delete, or carrying a consumer group — which is the AMQP-faithful
way to say the queue exists independently of its consumers.

The alternative is a stream per source. Because two streams may not listen
on overlapping subjects, each source stream cannot simply capture its bound
subjects — sources binding `orders.*` and `orders.created` would collide.
It would instead have to *source* from a per-address origin stream with a
subject filter, the same mechanism address-to-address binding uses above.
That buys per-source `MaxAge` and max-length, delete-on-ack via
`WorkQueuePolicy`, and per-source statistics. It was rejected for five
reasons, the first of which is decisive:

- **It does not buy the feature that motivates it.** The usual reason to
  want per-source TTL is AMQP's expire-into-the-dead-letter-exchange idiom,
  and JetStream cannot express it at any topology. There is no advisory for
  expiry: the advisory set covers consumer max-deliveries, nak, term,
  lifecycle and leader elections, but nothing fires when a limit evicts a
  message. Subject delete markers are not a substitute — they are a
  key-value feature, carrying no message body, emitted only for the last
  message on a subject, and stamped `Nats-Rollup: sub`, which purges the
  subject. Per-source streams would give TTL *deletion*, never TTL *routing*.
- **Publish confirmation would weaken.** Sourcing is asynchronous, so a
  confirmed publish would mean "durable in the origin stream", not "durable
  in the bound source" — a real regression against AMQP, where a publisher
  confirm covers every bound queue.
- **It multiplies cost per message and per source.** Every message is stored
  once more (origin plus each source), every source adds a replication group,
  and on a replicated stream with synchronous flushing the sourcing hop is an
  additional synchronous write per message per source, on the path that
  already bounds throughput.
- **`WorkQueuePolicy` is narrower than a queue.** It rejects multiple
  unfiltered consumers (err 10099), non-unique filtered consumers (10100),
  and any consumer that is not deliver-all (10101), so the delete-on-ack prize
  comes with constraints a queue does not have.
- **Origin retention becomes a correctness dependency.** If the origin's
  `MaxAge` elapses before a lagging source stream has sourced a message, the
  message is lost silently — a failure mode the current topology does not have.

Switching the shared stream to `InterestPolicy` was also considered, as a
cheaper route to delete-on-ack that keeps one stream per address. It is
wrong here: interest is evaluated at publish time, so a message published
before any consumer exists is discarded immediately. That would break
`Offset: first` replay and any stream-typed source, which exist precisely to
read a retained log. Queue-like and log-like sources share an address
stream and want opposite retention; `LimitsPolicy` is the only policy correct
for both, and over-retention is a storage cost rather than a correctness one.

Revisit this decision if a deployment needs per-source expiry or max-length,
or if delete-on-ack becomes a storage problem that `MaxAge` and `MaxBytes`
cannot contain — and only if the weaker publish-confirm guarantee is
acceptable there. Note that `Retention` cannot be changed into or out of
`WorkQueuePolicy` on a live stream, so any such move is a stream-recreate
migration, not a configuration change.

## Operational resilience

Five mechanisms harden the connector against a cold or busy broker. All are
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
  callers share its result. Collapsing is scoped to the broker endpoint plus
  the connection's credential identity, so distinct accounts on one server
  (disjoint JetStream state and permissions) never share an outcome.
  Failures are not cached, and success is still memoized per connection, so
  a fresh connection re-asserts its topology.
- **Consumer liveness.** Consumers run with a 5s idle heartbeat and a consume
  error handler. If the server stops serving a consumer's pulls (a broker
  restart, or a just-created consumer whose raft group is not yet serving),
  the missed heartbeat is logged at warn level and the client re-issues its
  pull request after roughly twice the heartbeat. With library defaults the
  same stall would go unlogged and take ~30s per detection cycle.
- **Stale-topology recovery.** A stream can be deleted out from under a
  live connection — an operator reset, a storage wipe — and a NATS client
  outlives broker state changes that would sever an AMQP connection, so the
  memoized "already ensured" answer can go stale. When a publish gets "no
  response from stream" or a subscribe finds the stream missing, the
  connection drops its memoized entry, re-asserts the stream, and retries
  once (the failed publish attempt was not stored, so the retry cannot
  duplicate it), instead of failing every call until the client reconnects.
  A failed dead-letter publish likewise drops the entry so the retry that
  follows re-creates the DLQ stream.
- **Consumer-loss recovery.** The same class of event can take a live
  subscription's server-side consumer: deleted administratively, or expired
  (ephemeral inactivity threshold) during an outage the client connection
  survives. The client library either stops delivering silently — it treats
  "consumer deleted" as terminal — or keeps pulling a consumer that no
  longer answers, and RabbitMQ has no analogous state: a deleted queue
  closes its consumers' channel. When the consume machinery stops on its
  own, when an authoritative consumer-gone error arrives, or when a bounded
  single-flight probe (triggered by consume errors such as missed
  heartbeats or unanswered pulls, and trusted only for an explicit "not
  found" answer) confirms the consumer is gone, `Subscribe` ends with a
  non-fatal error. The client's re-subscribe then recreates the consumer —
  and, after a storage wipe, the stream. A recreated durable starts from
  its configured `Offset`, like a re-declared queue starts empty.

One teardown case deliberately does NOT end `Subscribe`: a client-initiated
`Disconnect` while its consume stream is still open. Ending the subscription
would end the caller's whole consume stream, but the amqp091 connector
leaves that stream open — its subscribe loop just goes quiet once the AMQP
connection closes — so a client that disconnects and then acks straggler
in-flight messages gets each ack answered with a "could not retrieve broker
details" failure rather than end-of-stream. `Subscribe` blocks until the
stream's own context ends; delivery cannot resume either way, because the
disconnect already stopped the consume machinery and drained the connection.

## Configuration

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `NATSJS_STREAM_REPLICAS` | `1` | Stream replication factor. Set to `3` against a clustered server for the HA equivalent of quorum queues. |
| `NATSJS_STREAM_MAX_AGE` | `72h` | Max age before messages are evicted (Go duration; `0` = keep forever). |
| `NATSJS_STREAM_MAX_BYTES` | `0` (unlimited) | Hard per-stream storage cap in bytes. |
| `NATSJS_API_TIMEOUT` | `30s` | Deadline for JetStream management API calls (stream / consumer creation). Go duration. |
| `NATSJS_ACK_WAIT` | `30s` | How long the server waits for an ack before redelivering a message. Go duration. |
| `NATSJS_SAC_PINNED_TTL` | `1m` | Single-active-consumer failover deadline: how long the pinned client may go without pulling before a standby takes over. Go duration. |
| `NATSJS_DEADLETTER_RETRY_DELAY` | `5s` | How long to wait before redelivering a message whose dead-letter attempt failed, so a permanently-failing dead-letter cannot spin. Go duration. |

TLS and credentials come from the standard `ConnectionConfiguration` (`Tls`,
`Credentials`) and the server's `tlsSkipVerify` flag, exactly as for the AMQP
connector; the broker certificate is verified against the system trust store.

## Feature-parity matrix

Legend: **Native** = NATS does it; **Proxy** = rebuilt in the connector;
**Drop** = not carried forward.

| RabbitMQ / Arke feature | Disposition | Notes |
| --- | --- | --- |
| Topic routing + wildcard bindings | Native | NATS subjects + `*`/`>`; `#`->`>`, `*`->`*`. |
| Per-subscriber ephemeral queue | Native | Ephemeral consumer with inactivity threshold (per subscriber — see limitations). |
| Durable work queue (reconnect resumes backlog) | Native | Durable consumer for non-transient / consumer-group / single-active sources. |
| Publish confirms | Native | `js.PublishMsg` returns a `PubAck`. |
| Message dedup (`publish_id` + `publisher_name`) | Native | `Nats-Msg-Id` + stream `Duplicates` window. |
| Streams: offsets, start position | Native | JetStream is a log; `DeliverPolicy` maps `Offset`. |
| Single active consumer | Native | Pinned-client priority group on the durable (nats-server 2.11+); standby takes over within `NATSJS_SAC_PINNED_TTL`. |
| Prefetch / QoS | Native | `MaxAckPending`; prefetch 0 (AMQP "unlimited") maps to unlimited (-1). The Arke gRPC server raises a prefetch below 1 to 1 before any provider sees it, so the unlimited mapping applies to direct provider use. |
| HA / quorum queues | Native | JetStream R3 (Raft) via `NATSJS_STREAM_REPLICAS`. |
| Delayed retry (per-msg TTL + DLX idiom) | Proxy -> Native | Replaced with `NakWithDelay`. |
| Retry-count header (`x-retry-count`) | Proxy | Synthesized from JetStream `NumDelivered`. |
| Dead-letter (DLX) | Proxy | No native DLX; republish to DLQ subject then `Term()`. |
| Header-filter exchange (`Filter` / `Match`) | Proxy | NATS routes on subject; evaluated in `evaluateFilters` (see limitations). |
| Address-to-address binding (`ParentAddress`) | Native | The child's stream sources the bound subjects from the parent's, keeping each message's subject. |
| `MessageTTL` (per-queue message TTL) | Drop | Accepted but not applied; retention is the stream-level `MaxAge` (see limitations). |
| `Expires` (queue disuse expiry) | Native | Consumer `InactiveThreshold`; the consumer is deleted after that long without an attached client. |
| Dead-letter on message expiry | Drop | RabbitMQ expires a message off a queue into its DLX; per-source TTL is not applied here, so nothing expires per source to dead-letter (see limitations). |
| Source stats (depth / consumers) | Native | JetStream stream / consumer `Info`. |
| Max message size | Partial | NATS caps a payload at the server's `max_payload` (1MB default) where RabbitMQ's `max_message_size` default is 16MB (see limitations). |
| Publish / deliver rates in stats | Proxy | Sampled: counter deltas between `SourceStats` calls (see limitations). |
| RabbitMQ management HTTP API | Drop | Replaced by the JetStream API over NATS itself. |
| Distributed tracing (`traceparent`/`tracestate`) | Native | Delivery starts (or continues) a span and writes its W3C trace context back into the consumed message, same as amqp091's `queueSubscribe`. |
| Stream reader position header (`x-current-offset`) | Native | STREAM-source deliveries only, matching amqp091's `streamSubscribe`; the message's own stream sequence in the `Offset` vocabulary. |
| Gzip body passthrough (`Transfer-Encoding: gzip`) | Native | STREAM-source deliveries only, matching amqp091's `streamSubscribe`: a gzip-tagged body is decompressed and the header stripped. natsjs never compresses on publish — `max_payload` has no equivalent to the RabbitMQ-Streams client's ~1MiB ceiling amqp091 works around — so this only ever undoes a publisher's own compression. |

## Source options

Per the connector-interface contract, `SupportedSourceOptions()` advertises
the `Source.Options` keys the connector accepts — the same list as the
amqp091 connector, so existing client sources validate unchanged:

| Key | Type | Description |
| --- | --- | --- |
| `MessageTTL` | string (ms) | Accepted for compatibility; not applied per source — retention is the stream-wide `NATSJS_STREAM_MAX_AGE` (see Known limitations). A warning is logged at subscribe time. |
| `Expires` | string (ms) | How long the source may go without an attached consumer before the broker deletes it (AMQP `x-expires`), mapped to the consumer's `InactiveThreshold`. Unset keeps the defaults: transient sources expire after 5 minutes (the same default amqp091 applies to auto-delete/exclusive queues), durable sources never expire. Deletion removes the consumer and its ack state only; the messages stay in the shared stream under its own retention. A non-integer or non-positive value fails the subscribe. |
| `DeadLetterAddress` | string | Address whose stream receives dead-lettered messages. |
| `DeadLetterSubject` | string | Routing-key override for dead-lettered messages; when unset, the copy keeps the message's original routing key (RabbitMQ's default dead-letter behavior). |
| `Offset` | string | Stream starting offset (`first`, `continue`, `last`, `next`, or a number counting from 0, as reported by `SourceStats`). |
| `ConsumerGroup` | string | Durable consumer group name (stream sources; also names the durable that single-active instances coordinate through, and is required for a single-active stream source). |

## Known limitations

- **Per-source TTL fidelity, and expiry-driven dead-lettering.** The
  connector uses one stream per address root, so a per-source `MessageTTL`
  cannot be mapped onto the shared stream without one source's value
  flapping another's retention. That option is therefore accepted but not
  applied, and a subscribe that sets it logs a warning naming the source —
  silent divergence in data retention is the one place a client must not
  have to read a design doc to notice. (`Expires` is different: it governs
  the *source's* lifetime, not its messages', and consumers are per-source,
  so it maps cleanly onto the consumer's `InactiveThreshold` and IS
  applied — see Source options.) Retention comes from the stream-wide
  `NATSJS_STREAM_MAX_AGE` / `NATSJS_STREAM_MAX_BYTES` configuration. True
  per-source TTL (or switching queue sources to a delete-on-ack policy such
  as `WorkQueuePolicy` / `InterestPolicy`) needs a per-source stream
  topology, and `Retention` is immutable, so that is a stream-recreate
  migration rather than an in-place change.

  A consequence worth stating on its own: RabbitMQ's per-queue TTL doubles as
  a routing mechanism, expiring a message off its queue and *into* the
  queue's dead-letter exchange, which is the classic "unprocessed after N
  seconds, send it to the DLQ" idiom. Since no per-source TTL is applied
  here, nothing expires per source and so nothing is dead-lettered by expiry;
  a message is dead-lettered only when a client nacks it with a dead-letter
  address set. Retention limits do eventually evict a message, but eviction
  deletes it — there is no expiry hook to route from. Clients relying on
  expiry-to-DLQ need an explicit nack, or the per-source topology above.
- **Payloads above the server's `max_payload`.** NATS enforces a maximum
  message size server-side (`max_payload`, 1MB by default, 64MB hard
  ceiling), where RabbitMQ's `max_message_size` defaults to 16MB. A message
  in between publishes on RabbitMQ and is rejected here with `maximum payload
  exceeded`. This is a server setting the connector cannot negotiate around,
  so a deployment carrying large messages has to raise `max_payload` to cover
  them (NATS advises staying at or below 8MB, since a large message is held
  whole in memory on every hop) or move the payload out of the message.
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
  view. A `STREAM` source reports no message count at all, as on amqp091:
  the readers of one retained log have no common backlog, and their reading
  is the offset pair instead. A stream with no offset to report answers with
  RabbitMQ's own `Offset not found` error rather than a silent zero. A
  durable source's consumer count is likewise per source: the number
  of clients with an open pull request on its consumer — a client working
  through a full pull buffer can briefly read as zero — while sources
  without a durable consumer report the stream-wide consumer count, which
  spans every source on the address.
- **Header filters are evaluated proxy-side.** NATS routes on subjects only,
  so a source's header `Filter`s cannot narrow what the server delivers:
  every message matching the source's subject filters is delivered to the
  connector, which evaluates the headers and acks non-matching messages
  without forwarding them. Bandwidth and CPU between broker and proxy
  therefore scale with the subject-matched traffic, not the header-matched
  traffic. That is fine when header filters refine an already-narrow subject,
  but a high-volume address consumed almost entirely through header filters
  should be remodeled onto routing keys (subjects) instead. Because this
  filtering is per consumer, competing subscribers of one durable must share
  the same header filter: a RabbitMQ headers binding is queue-wide, but here
  each consumer applies only its own filter, so a message the server hands to
  the "wrong" subscriber would be dropped rather than reaching the one that
  wanted it. A second live subscriber whose header filter differs from the
  durable's is therefore rejected (subject filters, which the server enforces,
  still update the shared consumer in place).
- **Dead-letter is a proxy-side republish.** The DLQ is an ordinary stream
  fed by the connector; there is no advisory-driven re-consumption (see
  "Rejected alternative: advisory-driven dead-lettering"). A failed DLQ
  publish fails the dead-letter call — the message stays in flight and is
  redelivered — rather than dropping the message.
- **Redelivery is unbounded.** The connector does not set `MaxDeliver`, so a
  message that is delivered and then neither acked nor nacked — a consumer
  that wedges, or crashes mid-handler in a loop — is redelivered every
  `NATSJS_ACK_WAIT` for as long as the subscription lives. RabbitMQ quorum
  queues apply a delivery limit (20 by default on 4.x) and, on reaching it,
  dead-letter the message if the queue has a dead-letter exchange or drop it
  if it does not. The difference is deliberate: the two brokers count
  deliveries differently, because RabbitMQ's delayed-retry idiom republishes
  the message through a TTL queue and a dead-letter exchange, which resets its
  delivery count, while `Retry` here is a `NakWithDelay` that increments the
  same `NumDelivered` a delivery limit would cap. A `MaxDeliver` chosen to
  match RabbitMQ's limit would therefore cut off *retries* that RabbitMQ
  allows without bound — breaking clients whose retry policy deliberately
  retries far more than 20 times — and separating the two cases would need
  per-message state the broker does not keep. Retaining the message is also
  the safer divergence: nothing is lost, and the stall is visible as the
  consumer's `num_pending`. Deployments that want a ceiling should alert on
  redelivery rate rather than ask the broker to discard.
- **Transient sources are per-subscriber.** A transient (auto-delete /
  exclusive / TEMPORARY) source maps to an ephemeral consumer created per
  subscription, so two clients subscribing with the same transient source
  name each receive every message. Consumers of one auto-delete AMQP queue
  would instead compete for its messages, and an exclusive AMQP queue would
  reject the second consumer outright. In practice clients generate unique
  names for transient sources (the usual exclusive-queue idiom), which is
  why this has not warranted the parity fix: a shared named consumer with an
  inactivity threshold standing in for the queue's expiry, plus rejecting a
  second subscriber when the source is exclusive.
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
