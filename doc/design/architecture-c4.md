# Arke — C4 Architecture Diagrams

<!-- markdownlint-disable MD013 -->

## Level 1 — System Context

```mermaid
C4Context
    title System Context — Arke

    Person(client, "Client Application", "Any application that needs to produce or consume messages")

    System(arke, "Arke", "Message broker proxy with a gRPC front-end. Abstracts broker-specific protocols from client applications.")

    System_Ext(rabbitmq, "RabbitMQ", "Message broker. Supports AMQP 0.9.1 queues and RabbitMQ Streams.")
    System_Ext(prometheus, "Prometheus", "Scrapes runtime and throughput metrics from Arke.")
    System_Ext(otlp, "OTLP Collector", "Receives distributed traces from Arke via gRPC.")

    Rel(client, arke, "Produce / Consume / Health", "gRPC (TLS optional)")
    Rel(arke, rabbitmq, "Publish / Subscribe / Ack / Nack", "AMQP 0.9.1 / RabbitMQ Streams")
    Rel(prometheus, arke, "Scrapes metrics", "HTTP /metrics")
    Rel(arke, otlp, "Exports traces", "OTLP gRPC")
```

---

## Level 2 — Container

```mermaid
C4Container
    title Container — Arke Process

    Person(client, "Client Application")
    System_Ext(rabbitmq, "RabbitMQ")
    System_Ext(prometheus, "Prometheus")
    System_Ext(otlp, "OTLP Collector")

    Container_Boundary(arke, "Arke") {
        Container(arke_process, "Arke Server Process", "Go", "Single server binary. Registers Producer, Consumer, custom Healthz, standard gRPC health, reflection, and channelz services. Uses cmux to serve gRPC and HTTP /metrics on the same listener, and internally wires rate limiting, provider lookup, AMQP connectivity, and tracing.")
    }

    Rel(client, arke_process, "Produce / Consume / Health / Reflection", "gRPC")
    Rel(arke_process, rabbitmq, "Publish / Subscribe / Ack / Nack", "AMQP 0.9.1 / RabbitMQ Streams")
    Rel(prometheus, arke_process, "Scrapes /metrics", "HTTP")
    Rel(arke_process, otlp, "Exports spans", "OTLP gRPC")
```

---

## Level 3 — Component (gRPC Server)

```mermaid
C4Component
    title Component — gRPC Server

    Container_Boundary(grpc_server, "gRPC Server") {
        Component(producer_svc, "ProducerServer", "Go struct / pb.ProducerServer", "Implements Connect, Publish, PublishOne, and Disconnect RPCs for message producers.")
        Component(consumer_svc, "ConsumerServer", "Go struct / pb.ConsumerServer", "Implements Connect, Consume, Disconnect, SourceStats, and SourceStatsGroup RPCs for message consumers.")
        Component(healthz_svc, "HealthzServer", "Go struct / pb.HealthzServer", "Implements the custom bidirectional Healthz.Check stream. Sends initial health state, responds to client health checks, and emits GOAWAY notifications.")
        Component(grpc_health_svc, "gRPC Health Service", "Go / grpc/health", "Exposes the standard grpc.health.v1.Health service with SERVING status.")
        Component(conn_watcher, "ConnectionWatcher", "Go goroutine", "Runs every 30 s. Calls each registered provider to detect and evict stale client connections.")
        Component(rate_limiter, "ClientLimitManager", "Go / token-bucket (golang.org/x/time/rate)", "Per-client token-bucket rate limiter. Applied as a gRPC unary + stream interceptor to Connect and Publish/Consume RPCs.")
        Component(tracing_helpers, "Tracing Helpers", "Go / OpenTelemetry", "Initializes the process tracer provider and creates spans from propagated message headers inside server and connector code.")
        Component(prom_interceptor, "Prometheus Interceptors", "Go / internal metrics", "Custom gRPC interceptors that record request totals, stream send/recv counts, and per-RPC latency summaries.")
        Component(metrics_http, "Metrics HTTP Endpoint", "Go / net/http + Prometheus", "Serves /metrics on the HTTP side of the cmux listener and gathers live provider stats on scrape.")
        Component(health_fanout, "Health Notification Fanout", "Go goroutine", "Broadcasts internal health events, including HPA-triggered GOAWAY notifications, to connected Healthz streams.")
    }

    Container_Ext(provider_layer, "Provider Layer")
    Person_Ext(client, "Client Application")
    System_Ext(prometheus, "Prometheus")
    System_Ext(otlp, "OTLP Collector")

    Rel(client, producer_svc, "Produce RPCs", "gRPC")
    Rel(client, consumer_svc, "Consume RPCs", "gRPC")
    Rel(client, healthz_svc, "Custom health stream", "gRPC")
    Rel(client, grpc_health_svc, "Standard health check", "gRPC")
    Rel(producer_svc, rate_limiter, "Enforces per-client limits")
    Rel(consumer_svc, rate_limiter, "Enforces per-client limits")
    Rel(producer_svc, tracing_helpers, "Creates spans from message headers")
    Rel(consumer_svc, tracing_helpers, "Creates spans from message headers")
    Rel(producer_svc, prom_interceptor, "Records metrics")
    Rel(consumer_svc, prom_interceptor, "Records metrics")
    Rel(healthz_svc, health_fanout, "Registers for internal health notifications")
    Rel(producer_svc, provider_layer, "Delegates broker operations")
    Rel(consumer_svc, provider_layer, "Delegates broker operations")
    Rel(conn_watcher, provider_layer, "Checks client liveness every 30 s")
    Rel(prometheus, metrics_http, "Scrapes /metrics", "HTTP")
    Rel(tracing_helpers, otlp, "Exports spans", "OTLP gRPC")
```

---

## Level 3 — Component (Provider Layer)

```mermaid
C4Component
    title Component — Provider Layer

    Container_Boundary(provider_layer, "Provider Layer") {
        Component(provider_iface, "Provider Interface", "Go interface", "Broker-agnostic contract: Connect, Disconnect, Publish, PublishOne, Subscribe, Ack, Nack, Retry, DeadLetter, SupportedSourceOptions, WaitForConnect, Stats, ClientExists, and SourceStats.")
        Component(provider_registry, "Provider Registry", "Go / provider package", "Caches Provider singletons keyed by provider-type string and returns them to server code.")
        Component(factory_registry, "Factory Registry", "Go / ConcurrentMap", "Thread-safe map of Factory functions keyed by provider-type string. Populated by connector init() functions at startup.")
        Component(connector_init, "Connector Plugin (amqp091)", "Go init()", "Self-registers the amqp091 factory with the Factory Registry via Go init() at program startup.")
    }

    Container_Ext(grpc_server, "gRPC Server")
    Container_Ext(amqp_connector, "AMQP 0.9.1 Connector")

    Rel(grpc_server, provider_iface, "Calls broker operations through")
    Rel(provider_iface, provider_registry, "Looks up Provider singleton by provider type")
    Rel(provider_registry, factory_registry, "Instantiates singleton Provider via registered Factory")
    Rel(factory_registry, connector_init, "Factory registered by")
    Rel(connector_init, amqp_connector, "Creates instance of")
```

---

## Level 3 — Component (AMQP 0.9.1 Connector)

```mermaid
C4Component
    title Component — AMQP 0.9.1 Connector

    Container_Boundary(connector, "AMQP 0.9.1 Connector") {
        Component(amqp091prov, "amqp091provider", "Go struct", "Implements the Provider interface. Entry point for all broker operations.")
        Component(brokerdetails, "BrokerDetails", "Go struct", "Per-client state: AMQP connection, Stream connection reference, channel pools, active messages map, retry/dead-letter config.")
        Component(connwatcher, "Connection Watcher", "Go goroutine", "Monitors the AMQP error channel and triggers reconnection on failure.")
        Component(queueshim, "AMQP Channel Shim", "Go interface", "Wraps amqp091 channels. Enables mocking in unit tests.")
        Component(ratelatch, "Blocking Latch", "Go", "Applies back-pressure when the prefetch window is full for stream consumers.")
        Component(deadletter, "Dead Letter Handler", "Go", "Routes exhausted-retry messages to a configured dead-letter exchange/queue.")
    }

    Component(stream_package, "RabbitMQ Stream Package", "Go package", "Provider-neutral wrapper for RabbitMQ Stream connections, publishers, consumers, offsets, message conversion, and compression.")

    Container_Ext(provider, "Provider Layer")
    System_Ext(rabbitmq, "RabbitMQ")

    Rel(provider, amqp091prov, "Connect / Publish / Subscribe / Ack / Nack / Retry / DeadLetter")
    Rel(amqp091prov, brokerdetails, "Reads and mutates per-client state")
    Rel(amqp091prov, connwatcher, "Spawns on connect")
    Rel(amqp091prov, queueshim, "Queue publish / consume / ack")
    Rel(amqp091prov, stream_package, "Stream declare / publish / consume / offset store")
    Rel(amqp091prov, deadletter, "Routes failed messages")
    Rel(amqp091prov, ratelatch, "Applies consumer prefetch back-pressure")
    Rel(queueshim, rabbitmq, "AMQP 0.9.1")
    Rel(stream_package, rabbitmq, "RabbitMQ Streams protocol")
    Rel(connwatcher, rabbitmq, "Reconnects on error")
```

---

## Level 4a — Code (Server — gRPC Services & Provider Registry)

```mermaid
classDiagram
    direction TB

    namespace pb {
        class UnimplementedProducerServer {
            <<protobuf>>
            +Connect()
            +Publish()
            +PublishOne()
            +Disconnect()
        }
        class UnimplementedConsumerServer {
            <<protobuf>>
            +Connect()
            +Consume()
            +Disconnect()
            +SourceStats()
            +SourceStatsGroup()
        }
        class UnimplementedHealthzServer {
            <<protobuf>>
            +Check()
        }
    }

    class ProducerServer {
        +TLSSkipVerify bool
        +Connect(ctx, cfg) ConnectResponse
        +Publish(stream) error
        +PublishOne(ctx, msg) MessageResponse
        +Disconnect(ctx, empty) Empty
    }

    class ConsumerServer {
        +TLSSkipVerify bool
        +Connect(ctx, cfg) ConnectResponse
        +Consume(stream) error
        +Disconnect(ctx, empty) Empty
        +SourceStats(ctx, source) SourceStats
        +SourceStatsGroup(ctx, sources) SourceStatsCollection
    }

    class HealthzServer {
        +Check(stream) error
    }

    class GRPCHealthService {
        <<grpc/health>>
        +Check(service) HealthCheckResponse
    }

    class streamSender {
        -stream pb.Consumer_ConsumeServer
        +Send(ConsumeResponse) error
    }

    class consumeRecv {
        +err error
        +msg pb.Consume
    }

    class ClientLimitManager {
        <<ratelimiter>>
        -clients ConcurrentMap
        -bucketSize int
        -fillInterval time.Duration
        -maxAgeStaleClients time.Duration
        -enforced bool
        +Limit(ctx) error
        +StartClientCull(ctx)
        -cullStaleClients()
    }

    class clientLimiter {
        <<ratelimiter>>
        -limiter rate.Limiter
        -lastConnectionTime time.Time
    }

    class ProviderRegistry {
        <<provider package>>
        -registeredProviderTypes ConcurrentMap
        -registeredProviders ConcurrentMap
        +Register(name, Factory)
        +GetProvider(type) Provider
        +NewProvider(type) Provider
    }

    class providerOnce {
        <<provider package>>
        -m sync.Mutex
        -done uint32
        +Do(Factory) Provider
    }

    class Factory {
        <<type alias>>
        func() Provider
    }

    class Provider {
        <<interface>>
        +Connect() / Disconnect()
        +Publish() / PublishOne()
        +Subscribe() / Ack() / Nack()
        +Retry() / DeadLetter()
        +SupportedSourceOptions() / WaitForConnect()
        +Stats() / ClientExists() / SourceStats()
    }

    class ConnectionWatcher {
        <<goroutine>>
        Runs every 30s
        Calls TrimConnectionList()
        Removes dead clients from connectionMap
    }

    UnimplementedProducerServer <|-- ProducerServer : embeds
    UnimplementedConsumerServer <|-- ConsumerServer : embeds
    UnimplementedHealthzServer <|-- HealthzServer : embeds

    ProducerServer ..> ProviderRegistry : findProvider()
    ConsumerServer ..> ProviderRegistry : findProvider()
    ConsumerServer --> streamSender : creates per Consume() call
    ConsumerServer --> consumeRecv : receives from stream.Recv goroutine

    ProducerServer ..> ClientLimitManager : rate-checked via interceptor
    ConsumerServer ..> ClientLimitManager : rate-checked via interceptor
    HealthzServer ..> GRPCHealthService : complements standard health surface
    ClientLimitManager "1" --> "n" clientLimiter : one per client identifier

    ProviderRegistry --> providerOnce : singleton guard per provider type
    ProviderRegistry --> Factory : looks up to instantiate
    ProviderRegistry --> Provider : caches singleton
    providerOnce ..> Factory : calls once to create Provider

    ConnectionWatcher ..> ProviderRegistry : GetProvider() to check ClientExists()

    note for ProviderRegistry "Connectors register via init():\nblank import server → connectors → amqp091\namqp091.init() calls provider.Register().\nPer-client broker state is held inside connector BrokerDetails, not in the provider registry."
```

---

## Level 4b — Code (AMQP 0.9.1 Connector — Key Types)

```mermaid
classDiagram
    direction TB

    class Provider {
        <<interface>>
        +Connect(ctx, cfg, tlsSkipVerify) Error
        +Disconnect(ctx)
        +Publish(ctx, msgChan, errChan) Error
        +PublishOne(ctx, msg) Error
        +Subscribe(ctx, source, msgChan) Error
        +Ack(ctx, msgid) Error
        +Nack(ctx, msgid) Error
        +Retry(ctx, source, msgid, delay) Error
        +DeadLetter(ctx, source, msgid) Error
        +SupportedSourceOptions() map
        +WaitForConnect(ctx) bool
        +ClientExists(clientIdentifier) bool
        +Stats() Stats
        +SourceStats(ctx, source) SourceStats
    }

    class amqp091provider {
        -tlsConfig tls.Config
        -connections ConcurrentMap
        +Connect()
        +Publish()
        +Subscribe()
        +Ack() / Nack() / Retry() / DeadLetter()
        -queueSubscribe()
        -streamSubscribe()
        -declareExchange()
        -declareQueue()
        -declareBinding()
        -setupDeadLetter()
    }

    class BrokerDetails {
        +Connection amqp091ConnectionShim
        +StreamConnection stream.Connection
        +ClientIdentifier string
        +ActiveStreams int64
        -pubChannels BlockingPool
        -pubPCChannels BlockingPool
        -activeMessages ConcurrentMap
        -knownExchanges ConcurrentMap
        -knownQueues ConcurrentMap
        -knownBindings ConcurrentMap
        -state uint16
        -shutdownChan chan bool
        -lastPubSubEvent time.Time
    }

    class amqp091ConnectionShim {
        <<interface>>
        +Connect() error
        +Close() error
        +IsClosed() bool
        +NewChannel(confirm bool) amqp091ChannelShim
        +StandbyChannel() amqp091ChannelShim
        +NotifyClose(chan) chan
    }

    class amqp091Connection {
        -connection amqp.Connection
        -connStr string
        -standbyChannel amqp091Channel
        +Connect()
        +NewChannel()
        +StandbyChannel()
    }

    class amqp091ChannelShim {
        <<interface>>
        +Publish(exchange, key, msg) error
        +Consume(queue, autoAck, exclusive) chan
        +QueueDeclare(name, durable, autoDelete, args) error
        +ExchangeDeclare(name, kind, durable) error
        +QueueBind(queue, key, exchange, args) error
        +SetPrefetch(count) error
        +Close() error
        +NotifyClose(chan) chan
    }

    class amqp091Channel {
        -channel amqp.Channel
        -connection amqp091Connection
        -prefetch int
        -confirm bool
        +Publish()
        +Consume()
        +QueueDeclare()
    }

    class StreamConnectionInterface {
        <<interface>>
        +Connect() error
        +Close() error
        +IsClosed() bool
        +NewConsumer(stream, consumer, offset, handler, singleActive) Consumer
        +DeclareStream(name, ttl) error
        +GetPublisher(stream, name, confirm) Publisher
        +PutPublisher(confirm, publisher)
        +StoreOffset(stream, consumer, offset) error
        +GetConsumerOffset(stream, consumer) (int64, error)
        +GetStreamOffset(stream) (int64, error)
    }

    class StreamConnectionImpl {
        -env RabbitMQ Stream Environment
        -publishers ConcurrentMap
        -clientIdentifier string
        +NewConsumer()
        +DeclareStream()
        +StoreOffset()
        +GetPublisher() / PutPublisher()
    }

    class StreamConsumerInterface {
        <<interface>>
        +Close() error
    }

    class StreamConsumerImpl {
        -consumer ha.ReliableConsumer
        +Close()
    }

    class StreamPublisherInterface {
        <<interface>>
        +Publish(Message) error
        +Confirmations() channel
        +Close() error
        +StreamName() string
        +PublisherName() string
    }

    class StreamPublisherImpl {
        -publisher ha.ReliableProducer
        -streamName string
        -publisherName string
        -confirmations chan
        +Publish()
        +Close()
    }

    class StreamMessage {
        +Body []byte
        +Headers map
        +ContentType string
        +ContentEncoding string
        +PublishID int64
    }

    class StreamDelivery {
        +Message Message
        +StreamName string
        +Offset int64
    }

    class activeStreamMessage {
        +Ack func()
        +Nack func()
    }

    Provider <|.. amqp091provider : implements
    amqp091provider "1" --> "n" BrokerDetails : manages per-client

    BrokerDetails --> amqp091ConnectionShim : Connection
    BrokerDetails --> StreamConnectionInterface : StreamConnection

    amqp091ConnectionShim <|.. amqp091Connection : implements
    amqp091Connection --> amqp091ChannelShim : creates
    amqp091ChannelShim <|.. amqp091Channel : implements

    StreamConnectionInterface <|.. StreamConnectionImpl : internal implementation
    StreamConnectionImpl --> StreamConsumerInterface : creates
    StreamConnectionImpl --> StreamPublisherInterface : manages pool
    StreamConsumerInterface <|.. StreamConsumerImpl : internal implementation
    StreamPublisherInterface <|.. StreamPublisherImpl : internal implementation
    StreamPublisherInterface ..> StreamMessage : publishes
    StreamConsumerInterface ..> StreamDelivery : delivers
    amqp091provider ..> activeStreamMessage : tracks Ack/Nack state
```

<!-- markdownlint-enable MD013 -->
