# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [arke.proto](#arke-proto)
    - [Address](#arke-Address)
    - [ConnectResponse](#arke-ConnectResponse)
    - [ConnectionConfiguration](#arke-ConnectionConfiguration)
    - [Consume](#arke-Consume)
    - [ConsumeResponse](#arke-ConsumeResponse)
    - [Credentials](#arke-Credentials)
    - [DeclareOnlyResponse](#arke-DeclareOnlyResponse)
    - [Empty](#arke-Empty)
    - [Error](#arke-Error)
    - [Filter](#arke-Filter)
    - [Health](#arke-Health)
    - [HealthCheck](#arke-HealthCheck)
    - [HealthStatus](#arke-HealthStatus)
    - [Match](#arke-Match)
    - [Message](#arke-Message)
    - [Message.HeadersEntry](#arke-Message-HeadersEntry)
    - [MessageConsumed](#arke-MessageConsumed)
    - [MessageConsumedResponse](#arke-MessageConsumedResponse)
    - [MessageResponse](#arke-MessageResponse)
    - [Source](#arke-Source)
    - [Source.OptionsEntry](#arke-Source-OptionsEntry)
    - [SourceStats](#arke-SourceStats)
    - [SourceStatsCollection](#arke-SourceStatsCollection)
    - [Sources](#arke-Sources)
  
    - [Address.TargetType](#arke-Address-TargetType)
    - [Filter.MatchType](#arke-Filter-MatchType)
    - [HealthStatus.Code](#arke-HealthStatus-Code)
    - [Source.TargetType](#arke-Source-TargetType)
  
    - [Consumer](#arke-Consumer)
    - [Healthz](#arke-Healthz)
    - [Producer](#arke-Producer)
  
- [Scalar Value Types](#scalar-value-types)



<a name="arke-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## arke.proto
Arke message broker proxy messages.

This file outlines the gRPC interface for the Arke proxy.


<a name="arke-Address"></a>

### Address
Represents the publishing destination for a message.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The name of this destination address. |
| subjects | [string](#string) | repeated | The subjects of the address. Multiple subjects are allowed on Subscribe, but not on Publish. |
| type | [Address.TargetType](#arke-Address-TargetType) |  | Target type, default is TOPIC. |
| durable | [bool](#bool) |  | **Deprecated.**  |
| auto_delete | [bool](#bool) |  | Should the address automatically delete. |
| parent_address | [Address](#arke-Address) |  | A parent Address. Usage includes Address to Address binding. |






<a name="arke-ConnectResponse"></a>

### ConnectResponse
Represents the connection response


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Indicates whether the connection was successful. |
| error | [Error](#arke-Error) |  | Error if the connection failed. |






<a name="arke-ConnectionConfiguration"></a>

### ConnectionConfiguration
Represents the broker connection information. This is passed
in by the client allowing us to remain a proxy, but also
allowing us to support multiple providers at once such as
RabbitMQ and Kafka.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host | [string](#string) |  | Broker hostname or IP address. |
| port | [int32](#int32) |  | Broker port. |
| provider | [string](#string) |  | Provider type, currently only amqp091, and natsjs. |
| tenant | [string](#string) |  | Tenant name for this connection. Tenant is not required |
| credentials | [Credentials](#arke-Credentials) |  | Authentication credentials. |
| tls | [bool](#bool) |  | Should this provider connection use TLS. |
| client_name | [string](#string) |  | The name of the client connecting. |
| admin_port | [int32](#int32) |  | The administrative port for the provider (eg. RabbitMQ management port) for any actions needing to be performed by the provider (eg. modifying bindings for RabbitMQ) |






<a name="arke-Consume"></a>

### Consume
Sent on the Consume stream from the client to either start consuming messages or to ack/nack messages


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| src | [Source](#arke-Source) |  |  |
| ack | [MessageConsumed](#arke-MessageConsumed) |  |  |






<a name="arke-ConsumeResponse"></a>

### ConsumeResponse
Response to a Consume message. Either a Message or a MessageConsumedResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| msg | [Message](#arke-Message) |  |  |
| consumed_response | [MessageConsumedResponse](#arke-MessageConsumedResponse) |  |  |
| error | [Error](#arke-Error) |  |  |
| declare_only_response | [DeclareOnlyResponse](#arke-DeclareOnlyResponse) |  |  |






<a name="arke-Credentials"></a>

### Credentials
Represents the broker authentication information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| username | [string](#string) |  | Username for authenticating to broker. |
| password | [string](#string) |  | Password for authenticating to broker. |






<a name="arke-DeclareOnlyResponse"></a>

### DeclareOnlyResponse
Response to a Consume request where Source.DeclareOnly is true.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Was the source declaration successful. |
| error | [Error](#arke-Error) |  | Error if the source declaration failed. |






<a name="arke-Empty"></a>

### Empty
No parameters or return value.






<a name="arke-Error"></a>

### Error
Represents a generic error message.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  | The text error message. |
| code | [int32](#int32) |  | The error code. |
| is_fatal | [bool](#bool) |  | Indicator that a fatal error has occured, and a reconnect is required on this connection. |






<a name="arke-Filter"></a>

### Filter
Represents a filter for Message.headers. The Filter will prevent a consumer
from consuming all messages from a Source.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| matches | [Match](#arke-Match) | repeated | One or more filter Matches. |
| type | [Filter.MatchType](#arke-Filter-MatchType) |  | The MatchType for this filter. Default is ALL. |






<a name="arke-Health"></a>

### Health
Message used to communicate or request health to/from the server and client


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| check | [HealthCheck](#arke-HealthCheck) |  |  |
| status | [HealthStatus](#arke-HealthStatus) |  |  |






<a name="arke-HealthCheck"></a>

### HealthCheck
Message requesting the health of the other end of the stream. Essentially a ping.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| uuid | [string](#string) |  | UUID to identify the check and match with the response |






<a name="arke-HealthStatus"></a>

### HealthStatus
Message containing the response to a health check request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| uuid | [string](#string) |  | UUID of the HealthCheck message being responded to |
| time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| code | [HealthStatus.Code](#arke-HealthStatus-Code) |  |  |
| cpu_availability | [float](#float) |  | CPU availability as a ratio (0.0 to 1.0, -1 as unknown) |
| memory_availability | [float](#float) |  | Memory availability as a ratio (0.0 to 1.0, -1 as unknown) |






<a name="arke-Match"></a>

### Match
Represents the key/value match for Message.headers. Currently only an
exact match is supported.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The Message.headers key |
| value | [string](#string) |  | The Message.headers value |






<a name="arke-Message"></a>

### Message
Represents a message that is being produced or consumed. The
error property is set by the proxy when an error occurs during
the consumer process.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| uuid | [string](#string) |  | The proxy sets this UUID when the message is consumed. It should be returned when a message is Ack/Nack&#39;ed. |
| headers | [Message.HeadersEntry](#arke-Message-HeadersEntry) | repeated | The message headers. |
| body | [bytes](#bytes) |  | The message body. |
| address | [Address](#arke-Address) |  | The distination for a published message. |
| persistent | [bool](#bool) |  | Indicates whether to persist the message. |
| error | [Error](#arke-Error) |  | Error message if consuming failed. |
| confirm | [bool](#bool) |  | Enables guaranteed delivery to the broker. |
| publish_id | [int64](#int64) |  | Combined with the publisher_name to provide publishing deduplication. The publish_id is a strictly increasing sequence. |
| publisher_name | [string](#string) |  | The publisher_name is used in conjunction with publish_id to provide message deduplication on Streams ONLY. |






<a name="arke-Message-HeadersEntry"></a>

### Message.HeadersEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="arke-MessageConsumed"></a>

### MessageConsumed
Message used to ack or nack a message UUID


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| uuid | [string](#string) |  | arke.Message UUID returned from Consume. |
| nack | [bool](#bool) |  | Nack (true) or Ack (false) the message. By default all messages will be Ack&#39;d unless you set this to true. |
| requeue_delay | [int32](#int32) |  | Requeue delay in seconds for Nack messages. Delay of zero will result in Nack&#39;d messages getting dequeued. If delay is greater than zero, the message will be requeued after the delay. |






<a name="arke-MessageConsumedResponse"></a>

### MessageConsumedResponse
Response to a MessageConsumed message


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| uuid | [string](#string) |  | Message UUID of the arke.Message |
| success | [bool](#bool) |  | Was the ack or nack successful. |
| error | [Error](#arke-Error) |  | Error if the MessageConsumed failed |






<a name="arke-MessageResponse"></a>

### MessageResponse
Represents the response from publishing a message.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  | Indicates whether the message was published successfully. |
| error | [Error](#arke-Error) |  | Error if publishing failed. |






<a name="arke-Source"></a>

### Source
Represents the source for consumer subscriptions. The Stream TargetType
does not support exclusive or auto_delete. Streams also do not support the
following Options: Exclusive, DeadLetterAdress or DeadLetterSubject.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The name of this source. |
| address | [Address](#arke-Address) |  | The Address associated with this source. |
| durable | [bool](#bool) |  | **Deprecated.**  |
| auto_delete | [bool](#bool) |  | Should this Source automatically delete. |
| options | [Source.OptionsEntry](#arke-Source-OptionsEntry) | repeated | Additional options for this Source. Option keys include: MessageTTL, Expires, DeadLetterAddress, DeadLetterSubject, Offset(Valid values: first, continue, next, or a quoted integer), ConsumerGroup. |
| exclusive | [bool](#bool) |  | Should this source be exclusive to the subscriber. |
| prefetch_count | [int32](#int32) |  | Set the prefetch count for this subscriber. Must be greater than 0. Defaults to 1. |
| filters | [Filter](#arke-Filter) | repeated | Filters for this Source. |
| type | [Source.TargetType](#arke-Source-TargetType) |  | Target type, default is QUEUE. |
| declare_only | [bool](#bool) |  | Declare the source and any bindings but do not actually consume any messages. |
| single_active_consumer | [bool](#bool) |  | Only one consumer should be allowed to subscribe to this source. For queues, this applies to all consumers on the queue. For streams this option must be paired with the ConsumerGroup option. |






<a name="arke-Source-OptionsEntry"></a>

### Source.OptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="arke-SourceStats"></a>

### SourceStats
SourceStats includes information about the source from the broker


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [Error](#arke-Error) |  | Any error encountered while retrieving the SourceStats. |
| message_count | [int64](#int64) |  | Total number of messages in the queue. |
| consumer_count | [int32](#int32) |  | Number of consumers on the queue or stream. |
| last_offset | [int64](#int64) |  | Offset of the last message in the stream. |
| current_offset | [int64](#int64) |  | Current stream offset for the specified consumer. |
| name | [string](#string) |  | Name of the source. |
| publish_rate | [float](#float) |  | The rate of messages being published to this source in messages/second. |
| deliver_rate | [float](#float) |  | The rate of messages being delivered from this source in messages/second. |






<a name="arke-SourceStatsCollection"></a>

### SourceStatsCollection
Return a group of SourceStats for the specified Sources.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [Error](#arke-Error) |  | Any error encountered while retrieving the SourcesMetrics |
| stats | [SourceStats](#arke-SourceStats) | repeated | A list of SourceStats |






<a name="arke-Sources"></a>

### Sources
Represents a group of Sources


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sources | [Source](#arke-Source) | repeated | The sources we want metrics returned for. |





 


<a name="arke-Address-TargetType"></a>

### Address.TargetType


| Name | Number | Description |
| ---- | ------ | ----------- |
| TOPIC | 0 | The address is a message topic. |
| QUEUE | 1 | The address is a message queue. |
| FILTER | 2 | The address is a filtered queue. |
| STREAM | 3 | The address is a stream. |



<a name="arke-Filter-MatchType"></a>

### Filter.MatchType


| Name | Number | Description |
| ---- | ------ | ----------- |
| ALL | 0 | All matches must match for a successful filter. |
| ANY | 1 | Any match can match for a successful filter. |



<a name="arke-HealthStatus-Code"></a>

### HealthStatus.Code


| Name | Number | Description |
| ---- | ------ | ----------- |
| OK | 0 | Everything is fine. |
| UNHEALTHY | 1 | Everything is not fine (cpu/memory high) but we are operational. |
| GOAWAY | 2 | Please go away and come back. |



<a name="arke-Source-TargetType"></a>

### Source.TargetType


| Name | Number | Description |
| ---- | ------ | ----------- |
| QUEUE | 0 | The Source is a HA queue |
| TEMPORARY | 1 | The Source is a temporary queue |
| STREAM | 2 | The Source is a stream |


 

 


<a name="arke-Consumer"></a>

### Consumer
Service for consuming messages

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Connect | [ConnectionConfiguration](#arke-ConnectionConfiguration) | [ConnectResponse](#arke-ConnectResponse) | Connect to a message broker. Pass in a ConnectionConfiguration with broker specific connection information. |
| Consume | [Consume](#arke-Consume) stream | [ConsumeResponse](#arke-ConsumeResponse) stream | Subscribe to a message broker source and receive a stream of messages when they are available. |
| Disconnect | [Empty](#arke-Empty) | [Empty](#arke-Empty) | Disconnect from the proxy and the message broker. |
| SourceStats | [Source](#arke-Source) | [SourceStats](#arke-SourceStats) | Ask for and receive information about an arke.Source |
| SourceStatsGroup | [Sources](#arke-Sources) | [SourceStatsCollection](#arke-SourceStatsCollection) | Ask for and receive information about a group of arke.Sources |


<a name="arke-Healthz"></a>

### Healthz
Service for health checks and communication

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Check | [Health](#arke-Health) stream | [Health](#arke-Health) stream |  |


<a name="arke-Producer"></a>

### Producer
Service for producing messages

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Connect | [ConnectionConfiguration](#arke-ConnectionConfiguration) | [ConnectResponse](#arke-ConnectResponse) | Connect to a message broker. Pass in a ConnectionConfiguration with broker specific connection information. |
| Publish | [Message](#arke-Message) stream | [MessageResponse](#arke-MessageResponse) stream | Send a stream of messages to the message broker. |
| PublishOne | [Message](#arke-Message) | [MessageResponse](#arke-MessageResponse) | Send one message to the message broker. |
| Disconnect | [Empty](#arke-Empty) | [Empty](#arke-Empty) | Disconnect from the proxy and the message broker. |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

