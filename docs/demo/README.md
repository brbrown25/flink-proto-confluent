# Demo stack and recorded casts

Kafka (KRaft) + Confluent Schema Registry + a Flink 1.20 (Java 17) session cluster with the `proto-confluent` format and the Flink Kafka SQL connector on the classpath. Used to record the [asciinema](https://asciinema.org) casts shown on the docs site.

Requires Docker, `asciinema` (3.x) and a JDK to build the JAR.

```bash
./setup.sh                  # build shadow JAR + fetch Kafka SQL connector into ./lib
docker compose up -d        # start the stack
bin/create-topic orders
echo '{"order_id":"o-1","customer":"acme","amount":42.5}' | bin/produce-proto orders
bin/flink-sql sql/quickstart.sql
docker compose down -v
```

## Re-recording the casts

```bash
./record.sh                 # clean stack, records casts/*.sh into ../content/assets/casts/
```

Each `casts/<name>.sh` is a script of `say` (comment) and `run` (typed + executed) steps, so a recording is reproducible and diffable. `bin/` holds thin wrappers around the Docker containers; `sql/` holds the Flink SQL the casts run. Preview with `asciinema play ../content/assets/casts/quickstart.cast` or `mkdocs serve`.

Keep casts short (under ~45 seconds) and re-record when option names or output change.
