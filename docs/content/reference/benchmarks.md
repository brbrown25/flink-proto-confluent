# Benchmarks

The `jmh` source set (`src/jmh`) holds [JMH](https://github.com/openjdk/jmh) microbenchmarks of the format's per-record path in both directions. They give an order-of-magnitude figure for what the format costs per record, and a baseline that changes to the read or write path (projection pushdown, converter caching, header parsing) can be measured against.

## What is measured

| Benchmark | Measures |
| --- | --- |
| `DeserializeBenchmark.deserialize` | `ProtoRowDataDeserializationSchema.deserialize(byte[])`: one Confluent wire-format record (magic byte, schema ID, message indexes, payload) to the `RowData` the table sees. This is the call the Kafka source makes for every record. |
| `SerializeBenchmark.serialize` | `ProtoRowDataSerializationSchema.serialize(RowData)`: one row to Confluent wire-format bytes. This is the call the Kafka sink makes for every row. |

Each benchmark runs over the test messages the unit tests use, with one fully populated sample record each:

| `message` | Protobuf message | Shape |
| --- | --- | --- |
| `SIMPLE` | `SimpleMessage` (`test_simple.proto`) | Two short strings. |
| `BIG` | `BigPbMessage` (`test_big.proto`) | 28 scalar fields covering every protobuf scalar type. |
| `REPEATED_NESTED` | `RepeatedMessageTest` (`test_repeated_nested.proto`) | One nested message with two scalars. |
| `MAP` | `MapTest` (`test_map.proto`) | A `map<string, string>` and a `map<string, NestedMap>`, five entries each. The table pins `message-class`, because `MapTest` is not the first message in its file. |

The serialize benchmark writes the row the source produces for the same sample, so the two directions run over the same data. The deserialize benchmark also has a `shape` parameter for how the table reads the record: `FULL` (every field is a column) and `PROJECTED` (the query reads only the first two top-level columns and the planner pushed that projection into the format, see [projection pushdown](configuration.md#projection-pushdown)).

Schema Registry is kept out of the measurement. The schema is registered in Confluent's in-memory `MockSchemaRegistryClient` during setup, the format is handed that client through its client seam, and the first record is read or written during setup so the writer-schema lookup and the converter build are done before measuring starts. Repeated schema and ID lookups are answered from memory, as the production `CachedSchemaRegistryClient` answers them; the mock alone would normalize the schema on every serialize call, which costs tens of microseconds per record and is work the production client caches. Every other format option has its production default, and the schemas are opened without a Flink context, so no metrics are registered. No SLF4J provider is on the benchmark classpath, so log statements cost only their level check (JMH prints SLF4J warnings about this at the start of each fork).

## Running them

```bash
./gradlew jmh
```

This builds the benchmark JAR, runs every benchmark and writes:

- `build/results/jmh/results.json`: the machine-readable results (JMH JSON), for comparing runs or plotting with a tool such as [JMH Visualizer](https://jmh.morethan.io/).
- `build/results/jmh/human.txt`: the console output, ending with the summary table.

The defaults keep a full run at about two minutes: one fork, three 2-second warmup iterations and five 2-second measurement iterations per benchmark. Every setting can be changed with a project property:

| Property | Default | Meaning |
| --- | --- | --- |
| `-Pjmh.includes` | all | Benchmarks to run, as comma-separated regular expressions over the benchmark name, e.g. `Deserialize` or `SerializeBenchmark.serialize`. |
| `-Pjmh.fork` | `1` | JVM forks per benchmark. Use 2 or more when comparing two builds. |
| `-Pjmh.warmupIterations` | `3` | Warmup iterations per fork. |
| `-Pjmh.warmup` | `2s` | Length of a warmup iteration. |
| `-Pjmh.iterations` | `5` | Measurement iterations per fork. |
| `-Pjmh.timeOnIteration` | `2s` | Length of a measurement iteration. |
| `-Pjmh.profilers` | none | JMH profilers, e.g. `gc` for allocation per record or `stack` for a sampling profile. |

For example, a quick look at the read path only, with allocation figures:

```bash
./gradlew jmh -Pjmh.includes=Deserialize -Pjmh.profilers=gc
```

and a longer run for comparing two commits:

```bash
./gradlew jmh -Pjmh.fork=3 -Pjmh.iterations=10
```

`-PflinkVersion` works as for every other task, so `./gradlew jmh -PflinkVersion=2.2.1` benchmarks against the Flink 2 line.

The benchmarks are not part of `check`, `test` or the coverage report: `build` only compiles and lints the source set. The [Benchmarks workflow](https://github.com/brbrown25/flink-proto-confluent/actions/workflows/benchmarks.yml) runs them on demand (Actions → Benchmarks → Run workflow) on a GitHub-hosted runner and uploads `build/results/jmh/` as an artifact. Shared runners are noisy, so compare numbers from the same runner type, and treat differences of less than about 10% as noise.

## Baseline

Indicative numbers from one run of `./gradlew jmh -Pjmh.fork=2` (two forks × five 2-second iterations, so 10 samples per row) on an Apple M4 Pro (12 cores, 48 GB, macOS 26.7) with Temurin JDK 21.0.2, Flink 1.20.0 and Confluent 8.1.1. Average time per record, single thread, lower is better; the error column is JMH's 99.9% confidence interval. Treat them as an order of magnitude: absolute numbers move with the machine, the JDK and the load on it, so compare runs from the same machine.

| `message` | Deserialize (ns/record) | Serialize (ns/record) |
| --- | --- | --- |
| `SIMPLE` | 555 ± 12 | 691 ± 5 |
| `BIG` | 2,065 ± 411 | 3,320 ± 41 |
| `REPEATED_NESTED` | 610 ± 8 | 839 ± 30 |
| `MAP` | 4,005 ± 124 | 3,123 ± 140 |

In throughput terms, one core turns roughly 1.8 million small records (`SIMPLE`) or 250,000 records with two five-entry maps (`MAP`) per second into rows, which is well above what a single Kafka partition usually delivers; the format is rarely the bottleneck of a job, but its cost grows with the number of fields and map entries.

## Projection pushdown

`FULL` against `PROJECTED` from one run of `./gradlew jmh -Pjmh.includes=DeserializeBenchmark -Pjmh.fork=2`, same machine and settings as the baseline above (10 samples per row, ns/record, lower is better). `PROJECTED` keeps the first two top-level columns, so `SIMPLE` (two columns), `REPEATED_NESTED` (one column) and `MAP` (two map columns) read the same columns either way and only `BIG` (28 columns) shows the effect of the projection.

| `message` | `FULL` | `PROJECTED` | Change |
| --- | --- | --- | --- |
| `SIMPLE` | 550 ± 14 | 542 ± 5 | none (same columns) |
| `BIG` | 1,988 ± 117 | 1,463 ± 14 | -26% |
| `REPEATED_NESTED` | 601 ± 13 | 599 ± 6 | none (same columns) |
| `MAP` | 3,985 ± 114 | 3,875 ± 70 | none (same columns, within noise) |

Reading two of `BIG`'s 28 columns saves about a quarter of the per-record cost. The rest is the Protobuf parse of the whole payload into a `DynamicMessage` and the wire-header handling, which a projection does not skip: the converters only run for the projected columns, but the message is still decoded in full.

## Adding a benchmark

- A new message: add a constant to `BenchMessage` (in `src/jmh/java/.../bench`) that builds a populated sample, and add its name to the `message` parameter of both benchmark classes.
- A new way of reading the same record, such as a projected read: add a constant to `DeserializeBenchmark.ReadShape` that overrides `rowType` (and `create`, if the schema needs more than a row type), and add its name to the `shape` parameter. The results then show it next to `FULL` for every message.
