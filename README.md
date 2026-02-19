# flink-proto-confluent (improved)

A Confluent Schema Registry Protobuf format for Apache Flink Table API. This project is an improved, standalone derivative of [amstee/flink-proto-confluent](https://github.com/amstee/flink-proto-confluent), repackaged under `com.bbrownsound` with additional features and tests.

## Build

```bash
./gradlew build shadowJar
```

Or use the Makefile:

- `make` / `make all` — full build (check + shadow JAR)
- `make build` — `./gradlew build shadowJar`
- `make test` — run tests
- `make check` — tests + checkstyle
- `make checkstyle` — checkstyle only
- `make coverage` — unit tests + integration tests + JaCoCo report (HTML in `build/reports/jacoco/test/html/`). Full `make check` enforces ≥80% line coverage overall and for packages `serialize` and `deserialize` (`jacocoTestCoverageVerification`). Tests do not use Mockito.
- `make clean` — `./gradlew clean`

## Usage

Copy the built JAR into your Flink `lib/` directory:

```bash
cp build/libs/proto-confluent.jar /path/to/flink/lib/
```

Then in Flink SQL (e.g. `sql-client.sh`), use the format identifier `proto-confluent`:

```sql
'value.format' = 'proto-confluent',
'value.proto-confluent.url' = 'http://schema-registry:8081',
'value.proto-confluent.topic' = 'your-topic',
'value.proto-confluent.is_key' = 'false'
```

## Testing

Tests use JUnit 5 and real components (e.g. Testcontainers for Kafka/Schema Registry); **Mockito is not used**. Coverage includes both unit tests and integration tests.

## Package layout

All code uses the **com.bbrownsound** package. Test protos in `src/test/proto/com/bbrownsound/` are generated with buf (managed mode disabled; `option java_package` in each proto). Run `make -C src/test/proto generate` to regenerate (requires `buf generate . --path com/`).

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
