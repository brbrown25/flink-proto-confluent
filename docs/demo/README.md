# Demo stack and recorded casts

Kafka (KRaft) + Confluent Schema Registry + a Flink session cluster (Java 17) with the `proto-confluent` format and the Flink Kafka SQL connector on the classpath. Used to record the [asciinema](https://asciinema.org) casts shown on the docs site.

The stack runs **Flink 1.20** by default. Set `FLINK_VERSION=2.2` when running `setup.sh` (or `record.sh`) to run it on Flink 2.2 instead; the variable picks the Flink image, the Kafka SQL connector and the Flink version the format JAR is built against:

| `FLINK_VERSION` | Flink image | Format JAR in `lib/` | Kafka SQL connector in `lib/` |
| --- | --- | --- | --- |
| `1.20` (default) | `flink:1.20-java17` | `flink-proto-confluent-<version>-all.jar`, built with `-PflinkVersion=1.20.5` (the CI matrix version; the build's own default, from `gradle.properties`, is 1.20.0) | `flink-sql-connector-kafka-3.4.0-1.20.jar` |
| `2.2` | `flink:2.2-java17` | `flink-proto-confluent-flink2-<version>-all.jar`, built with `-PflinkVersion=2.2.1` | `flink-sql-connector-kafka-5.0.0-2.2.jar` |

`setup.sh` writes the chosen version to `.env`, so the `docker compose` and `bin/` commands that follow use the same image without repeating the variable. The recorded casts are captured on the default Flink 1.20 stack.

Requires Docker, `asciinema` (3.x) and a JDK to build the JAR.

```bash
./setup.sh                  # build shadow JAR + fetch Kafka SQL connector into ./lib (FLINK_VERSION=2.2 ./setup.sh for Flink 2.2)
docker compose up -d        # start the stack
bin/create-topic orders
echo '{"order_id":"o-1","customer":"acme","amount":42.5}' | bin/produce-proto orders
bin/flink-sql sql/quickstart.sql
docker compose down -v
```

`sql/quickstart.sql` declares the `orders` table with a hand-written `CREATE TABLE` and aggregates it. To generate the table from the registered subject instead (what the quickstart cast shows), run the format JAR's [`ddl` command](../content/how-to/generate-ddl.md) with `bin/generate-ddl` and pass the generated file before the query:

```bash
bin/generate-ddl --subject orders-value --bootstrap-servers kafka:9092 \
  --option scan.startup.mode=earliest-offset --option scan.bounded.mode=latest-offset >sql/orders.sql
bin/flink-sql sql/orders.sql sql/quickstart-generated.sql
```

`bin/generate-ddl` runs the `ddl` command inside the Flink container against the demo registry and prints the `CREATE TABLE` for a subject; `bin/flink-sql` runs one SQL file, or several concatenated in order, in the SQL client. `sql/quickstart-generated.sql` is the query half of `sql/quickstart.sql`.

The docs site includes SQL from `sql/` instead of copying it ([`pymdownx.snippets`](https://facelessuser.github.io/pymdown-extensions/extensions/snippets/)): a page names a file, or a section of it between `-- --8<-- [start:NAME]` and `-- --8<-- [end:NAME]` comment lines, e.g. `--8<-- "docs/demo/sql/quickstart.sql:create-table"`, and `mkdocs build --strict` fails if the file or section is missing. The markers are SQL comments, so the scripts run as before. Keep each marker directly above a statement, with no blank line between them: the SQL client echoes a comment that is followed by a blank line into `bin/flink-sql`'s output.

## Re-recording the casts

```bash
./record.sh                 # clean stack, records casts/*.sh into ../content/assets/casts/
./record.sh NAME...         # only the named casts, reusing a healthy stack (--reset for a clean one)
FLINK_VERSION=2.2 ./record.sh NAME...   # same against the Flink 2.2 stack; a stack on another version is always reset
```

Each `casts/<name>.sh` is a script of `say` (comment) and `run` (typed + executed) steps, so a recording is reproducible and diffable. `bin/` holds thin wrappers around the Docker containers; `sql/` holds the Flink SQL the casts run. Preview with `asciinema play ../content/assets/casts/quickstart.cast` or `mkdocs serve`.

Keep casts short (under ~45 seconds) and re-record when option names or output change.

`SCHEMA=... bin/check SUBJECT` asks the registry about the latest version only. Add `--all-versions` (`bin/check --all-versions SUBJECT`) to check every registered version, which is the transitive verdict. The casts have not been re-recorded to show it yet: `casts/compat-modes.sh` still uses the latest-only check (follow-up for #135).

## Verifying the demo

`bin/verify.sh` runs every cast (with `CAST_FAST=1`) and `sql/quickstart.sql` against a clean stack under its own compose project: it builds the JAR first, tears the stack down afterwards, and fails on a Flink SQL error or when a step's output lacks a line listed in `expected/<step>.txt`. CI runs it on pull requests touching `src/main/**` or `docs/**` (see [CI](../ci.md#docs-demo-check)).

```bash
bin/verify.sh                         # build, start, run and check everything, tear down
bin/verify.sh --no-build --keep quickstart write-back   # reuse ./lib, run two steps, leave the stack up
```

When a change to the output is intended, update the matching `expected/` file along with the re-recorded cast. A new cast needs a step in `STEPS` in `bin/verify.sh` and an `expected/<step>.txt`, and a new `sql/*.sql` file must be run by a cast or be a step itself; the script fails until they are.
