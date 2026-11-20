# Metrics

The format registers its metrics on the metric group the connector hands it when a source or sink subtask opens, so every value is per subtask. Where that group sits in Flink's metric scope is the connector's decision: the Kafka source, for example, puts the format's metrics in a `deserializer` group under the source operator.

## Source

| Name | Type | Meaning |
| --- | --- | --- |
| `numDeserializeErrors` | Counter | Records that failed to deserialize, whatever happened to them next: counted with `on-deserialize-error` set to `fail` (the task then fails) and to `skip` (the record is dropped). A `null` record is not an error, and neither is a record whose writer schema could not be fetched because Schema Registry was unavailable: that fails the task and is counted in `schemaRegistryErrors` instead ([details](../how-to/dead-letter-topic.md#when-the-schema-registry-is-unavailable)). |
| `numDeadLetterRecords` | Counter | Failed records that were delivered to the [dead-letter topic](../how-to/dead-letter-topic.md). |
| `numDeadLetterErrors` | Counter | Failed records that could not be delivered to the dead-letter topic, whether the send failed at once or the broker reported the failure later. |
| `numCachedSchemas` | Gauge (integer) | Number of writer schemas the subtask has built a converter for: the size of the source's converter cache, which has one entry per schema ID and message seen in the records and is never evicted. A Confluent protobuf schema can define several messages and each record's message indexes pick one, so a schema ID whose records carry two different messages counts twice; when every record under a schema ID carries the same message, the value is the number of schema IDs seen. It grows by one the first time a record with a new schema ID, or a new message under a known schema ID, is converted and returns to zero on restart. It is not the size of the registry client's own schema cache. |
| `schemaRegistryRequests` | Counter | Requests sent to Schema Registry. See [Schema Registry metrics](#schema-registry-metrics). |
| `schemaRegistryErrors` | Counter | Requests to Schema Registry that failed. |
| `schemaRegistryLatencyMs` | Histogram (milliseconds) | Duration of the requests to Schema Registry. |

## Sink

| Name | Type | Meaning |
| --- | --- | --- |
| `numSerializeErrors` | Counter | Rows that failed to serialize, for any reason: a Schema Registry failure, a schema that cannot be registered, or a row that does not fit the schema. Each of them also fails the task, so the value is the number of failed attempts since the subtask last started. |
| `schemaRegistryRequests` | Counter | Requests sent to Schema Registry. See [Schema Registry metrics](#schema-registry-metrics). |
| `schemaRegistryErrors` | Counter | Requests to Schema Registry that failed. |
| `schemaRegistryLatencyMs` | Histogram (milliseconds) | Duration of the requests to Schema Registry. |

The sink has no `numCachedSchemas`: it keeps one schema per subject, not one per schema ID.

## Schema Registry metrics

The three `schemaRegistry*` metrics have the same meaning on the source and on the sink. They are measured at the registry client's HTTP layer, below its caches:

- **Only real requests count.** A lookup answered from a cache sends nothing and is not counted. On a source that is every record after the first one of each schema ID. On a sink it is every row for which the schema is still fresh (see `schema-cache.ttl` in the [configuration reference](configuration.md)) and the registry client still remembers the schema's ID.
- **Every kind of request counts**, not only lookups: on the source the fetch of a writer schema by ID, on the sink the lookup of the subject's latest schema, the lookup of a schema's ID and, with `auto-register-schemas`, the registration.
- **A request the client retries counts once.** The client retries some failed answers and fails over to the next URL when several are configured. That happens inside one request: it adds to the measured duration, not to the request count, and the request counts as an error only if the last attempt failed.
- **An error is a request that ended in an exception:** an I/O failure such as a refused connection or a timeout, or any answer that is not a success. That includes a 404. A sink writing to a subject that does not exist yet gets "subject not found" for its first lookup before it registers a schema derived from the table, so a few errors when a new sink starts are expected.
- **`schemaRegistryLatencyMs` is in whole milliseconds**, measured from before the request is sent until the answer is parsed or the request has failed, failed requests included. Its count is the number of requests since the subtask started; its minimum, maximum, mean, standard deviation and quantiles are computed over the most recent 1024 requests.

## Limitations

- **Without a metric group there are no metrics.** If a schema is opened with no initialization context (some tests and older call paths do this) the format works as usual and registers nothing.
- **Key and value formats share the names.** A table that uses `proto-confluent` for both `key.format` and `value.format` opens two format instances on the same metric group. Flink keeps the first metric registered under a name and logs a name-collision warning for the second, so the reported values then cover only one of the two.
