#!/usr/bin/env bash
# Builds the shadow JAR and fetches the Kafka SQL connector into ./lib for the demo stack.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p lib
(cd ../.. && ./gradlew -q shadowJar)
rm -f lib/flink-proto-confluent-*.jar
# The shadow jar carries the "all" classifier; the unclassified jar is the thin library jar.
JAR=$(ls -t ../../build/libs/flink-proto-confluent-*-all.jar | head -1)
cp "$JAR" lib/
CONN=flink-sql-connector-kafka-3.4.0-1.20.jar
[ -f "lib/$CONN" ] || curl -fL -o "lib/$CONN" \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/$CONN"
ls -l lib
