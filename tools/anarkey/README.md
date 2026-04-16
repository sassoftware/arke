# Anarkey

An interactive load testing tool for the Arke message broker proxy,
built with [Bubble Tea](https://github.com/charmbracelet/bubbletea).

## Features

- **Interactive TUI**: User-friendly terminal interface for configuration and monitoring
- **Dynamic Connection Management**: Scale connections up or down during runtime
- **Publisher/Consumer Support**: Test both message publishing and consumption
- **Stream/Queue Support**: Works with both queues and streams
- **Real-time Metrics**: Monitor message counts and throughput rates
- **Flexible Message Generation**: Configure message count or run continuously
- **Debug Logging**: Automatic logging to `logs/` directory for troubleshooting

## Building

```bash
go build -o build/anarkey .
```

or

```bash
make build
```

## Usage

Run the tool:

```bash
./build/anarkey
```

Or with a saved configuration file to skip configuration screens:

```bash
./build/anarkey -config my-config.json
```

### Configuration Screens

The tool will guide you through several configuration screens:

### 1. Arke Server Configuration

Configure connection to the Arke proxy server:

- **Arke Hostname**: The hostname or IP of the Arke server
(default: `localhost`)
- **Arke Port**: The gRPC port (default: `50051`)
- **Arke TLS**: Enable TLS for Arke connection (`true`/`false`)

### 2. Broker Configuration

Configure the message broker that Arke connects to:

- **Broker Hostname**: The hostname or IP of the message broker
(default: `localhost`)
- **Broker Port**: The broker port (default: `5672` for RabbitMQ)
- **Username**: Broker authentication username (default: `guest`)
- **Password**: Broker authentication password (default: `guest`)
- **Broker TLS**: Enable TLS for broker connection (`true`/`false`)

### 3. Connection Configuration

Configure the load test connections for both publishers and consumers:

- **Number of Publisher Connections**: How many publisher connections to
create (default: `1`)
- **Number of Consumer Connections**: How many consumer connections to
create (default: `1`)

### 4. Stream Configuration

Configure the publish/consume streams for both types:

- **Number of Publisher Streams**: How many publisher streams per publisher
connection (default: `1`)
- **Number of Consumer Streams**: How many consumer streams per consumer
connection (default: `1`)
- **Source Type**: Either `queue` or `stream`
- **Source Name**: The name of the source/queue/stream (default: `test-source`)
- **Address Name**: The address name for routing (default: `test-address`)
- **Subject**: The routing key/subject for message routing (default:
`test.routing.key`)
  - For queue sources: Used as the routing key to bind the queue to the
  topic exchange
  - For stream sources: Used as the subject for stream addressing
- **Message Count**: Number of messages to publish per publisher stream, or
`0` for continuous (default: `100`)
- **Publish Rate Limit**: Maximum messages per second per publisher stream,
or `0` for unlimited (default: `0`)
  - When set to `0`: Publishers send messages as fast as possible
  - When set to a positive integer: Each publisher stream is limited to that
  many messages per second
  - Example: Setting `1000` limits each publisher stream to 1000 msg/s
- **Message File**: Optional path to a JSON file containing a message template
with headers and body (default: empty)
  - When left blank: Auto-generated messages are used (e.g., "Load test
  message 1 from publisher 0")
  - When specified: All publishers use the message template from the file
- **Save Config File**: Optional path to save all configuration settings
to a JSON file (default: empty)
  - When specified: Configuration is saved to the file for later reuse
  - Can be loaded with `./build/anarkey -config <filename>` to skip
  configuration screens

**Note**: When using `queue` as the source type, the Address type is
automatically set to `TOPIC` to enable routing key functionality. Stream
sources use `STREAM` as the Address type.

#### Saving and Loading Configurations

You can save your configuration settings to a JSON file for reuse:

1. On the Stream Configuration screen, enter a filename in the "Save Config
File" field (e.g., `my-test-config.json`)
2. Complete the configuration and start the load test
3. The configuration will be saved to the specified file

To reuse a saved configuration:

```bash
./build/anarkey -config my-test-config.json
```

This will load all settings from the file and immediately start the load test,
taking you directly to the Running screen.

#### Example Configurations

Two ready-to-use configuration files are included:

- **[example-config-queue.json](example-config-queue.json)** — Queue-based
load test with 5 publisher connections (10 streams each), 3 consumer connections
(5 streams each), a rate limit of 1000 msg/s per stream, and a total of
10000 messages per publisher stream.
- **[example-config-stream.json](example-config-stream.json)** — Stream-based
load test with 1 publisher connection and 1 consumer connection, running
continuously at 2000 msg/s per stream.

Both files reference [example-message.json](example-message.json) as the message
template. Run either directly with:

```bash
./build/anarkey -config example-config-queue.json
./build/anarkey -config example-config-stream.json
```

#### Custom Message Files

You can provide a JSON file to customize the message content and headers.
The JSON file should have the following format:

<!-- markdownlint-disable MD013 -->
```json
{
  "headers": {
    "Content-Type": "application/json",
    "X-Custom-Header": "custom-value",
    "Priority": "high"
  },
  "body": "{\"order_id\": \"12345\", \"customer\": \"John Doe\", \"items\": [{\"sku\": \"ABC-123\", \"quantity\": 2}], \"total\": 49.99}"
}
```
<!-- markdownlint-enable MD013 -->

**Fields:**

- `headers` (object, optional): Key-value pairs for message headers
- `body` (string, required): The message body content (can be JSON string,
plain text, etc.)

**UUID Replacement:**

The body (and header values) support a special `@id@` placeholder that will
be replaced with a unique UUID for each published message:

- Use `@id@` anywhere in the body or header values
- Each occurrence is replaced with the same UUID per message
- A new UUID is generated for each message published
- Parsing is optimized - the body is only parsed once when the file is loaded

**Example with UUID:**

```json
{
  "headers": {
    "x-correlation-id": "@id@",
    "source": "load-test"
  },
  "body": "{\"id\":\"@id@\",\"event\":\"test.event\",\"data\":{\"uniqueId\":\"@id@\"}}"
}
```

**Example:** See [example-message.json](example-message.json) for a complete example.

When a message file is provided:

- All publishers will use the same message template
- Headers from the file are included in every published message
- The body content is sent as-is (no message counter is added)
- If `@id@` placeholders are present, each message gets a unique UUID
- The file is loaded once when publishers start (optimized parsing)

### 5. Running Screen

Once started, the tool displays real-time metrics and allows dynamic
connection and stream scaling:

- Publisher and consumer connection counts with streams per connection
- Total published messages and rate (msg/s)
- Total consumed messages and rate (msg/s)
- **Runtime Connection Scaling**: Adjust the number of publisher and consumer connections
  - Use arrow keys or j/k to move between fields
  - Type the desired number of connections for publishers or consumers
  - Enter `0` to close all connections of that type
  - Press Enter to apply changes immediately
  - New connections are automatically configured with the current stream settings
- **Runtime Stream Scaling**: Adjust publisher and consumer stream counts per connection
  - Use arrow keys or j/k to move between the stream count fields
  - Type the desired number of streams for publishers or consumers
  - Enter `0` to stop all streams of that type
  - Press Enter to apply the changes to all connections of that type
  - Changes take effect immediately without restarting
- **Runtime Rate Limiting**: Adjust publisher rate limits in real-time
  - Use arrow keys or j/k to navigate to the "Publish Rate Limit" field
  - Type the desired rate in messages per second (or `0` for unlimited)
  - Press Enter to apply the new rate limit to all publisher streams
  - All publisher streams are restarted with the new rate limit
  - Changes take effect immediately

Press `q` or `Ctrl+C` to stop the load test and exit.

## Navigation

- **Arrow Keys** or **j/k**: Move between fields
- **Text Input**: Type values directly into fields
- **Backspace**: Delete characters
- **Enter**: Proceed to next screen or start the load test
- **q** or **Ctrl+C**: Quit the application

## Architecture

The load tool consists of several key components:

- **TUI (tui.go)**: Bubble Tea-based terminal user interface
- **Connection Manager (connection.go)**: Manages gRPC connections to Arke
- **Publisher (publisher.go)**: Handles message publishing with declare-only pre-declaration
- **Consumer (consumer.go)**: Handles message consumption and acknowledgment
- **Metrics (types.go)**: Tracks message counts and calculates rates

## Publisher Behavior

For publisher connections:

1. Creates a declare-only consume stream to ensure the source exists
2. Closes the declare-only stream
3. Opens publish streams and begins sending messages
4. Each message is tracked and metrics are updated in real-time
5. **Rate Limiting**:
   - When `PublishRateLimit` is set to `0`: Messages are sent as fast as possible
   - When `PublishRateLimit` is a positive integer: Messages are rate-limited
   using a ticker
   - The rate limit is per publisher stream (e.g., 1000 msg/s with 5 streams = 5000
   total msg/s)
   - Rate limits can be changed at runtime, which restarts all publisher streams
   with the new limit
6. **Completion Behavior**:
   - If a non-zero message count is specified, the publisher stream stops after sending
   that many messages
   - Completed publishers are marked as finished and will **not be restarted**
   - When scaling publisher streams, only active (non-completed) publishers are counted
   - Continuous publishers (message count = 0) run indefinitely until manually stopped

## Consumer Behavior

For consumer connections:

1. Creates consume streams with the configured source
2. Receives messages and automatically acknowledges them
3. Updates metrics for each consumed message

## Example Scenarios

### High-Volume Publisher and Consumer Test

```text
Publisher Connections: 5
Consumer Connections: 3
Publisher Streams per Connection: 10
Consumer Streams per Connection: 5
Message Count: 0 (continuous)
```

### Stream Testing with Multiple Publishers

```text
Publisher Connections: 3
Consumer Connections: 2
Publisher Streams per Connection: 5
Consumer Streams per Connection: 3
Source Type: stream
Message Count: 10000
```

### Balanced Load Test

```text
Publisher Connections: 2
Consumer Connections: 2
Publisher Streams per Connection: 3
Consumer Streams per Connection: 3
Source Type: queue
Message Count: 5000
```

### Runtime Scaling Example

Start with minimal configuration and scale up during runtime:

1. Start with 1 publisher connection, 1 stream
2. Once running, increase publisher streams to 10
3. Add consumer connections at startup (1 connection, 5 streams)
4. Scale consumer streams up to 10 during runtime

## Requirements

- Go 1.25 or later
- Running Arke server
- Accessible message broker (RabbitMQ)

## Debugging

The tool automatically creates detailed logs in the `logs/` directory
with timestamps (e.g., `logs/anarkey-20240206-153045.log`). Logs include:

- Connection establishment and teardown
- Publisher/consumer lifecycle events
- Message sending/receiving errors
- Configuration changes and runtime scaling operations
- Detailed error messages with context

Check the logs for troubleshooting connection issues, understanding message
flow, or debugging unexpected behavior.

## Notes

- The tool automatically creates sources with declare-only mode before publishing
- Metrics update every second
- All gRPC streams are managed with proper context cancellation
- Connections are gracefully closed on exit
- Log files are created in the `logs/` directory with timestamps for each run
