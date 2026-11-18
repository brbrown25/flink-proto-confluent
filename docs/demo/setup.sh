#!/usr/bin/env bash
# Builds the shadow JAR and fetches the Kafka SQL connector into ./lib for the demo stack.
#
# FLINK_VERSION picks the Flink line the stack runs: 1.20 (default) or 2.2. It selects the Flink
# image, the Kafka SQL connector and the -PflinkVersion the format JAR is built against, and is
# written to .env so that later `docker compose` and bin/ calls use the same image.
set -euo pipefail
cd "$(dirname "$0")"
FLINK_VERSION=${FLINK_VERSION:-1.20}
case "$FLINK_VERSION" in
  1.20) BUILD_FLINK=1.20.0; ARTIFACT=flink-proto-confluent; KAFKA_CONNECTOR=3.4.0-1.20 ;;
  2.2) BUILD_FLINK=2.2.1; ARTIFACT=flink-proto-confluent-flink2; KAFKA_CONNECTOR=5.0.0-2.2 ;;
  *) echo "FLINK_VERSION must be 1.20 or 2.2, got '$FLINK_VERSION'" >&2; exit 2 ;;
esac
echo "FLINK_VERSION=$FLINK_VERSION" >.env

mkdir -p lib
(cd ../.. && ./gradlew -q shadowJar -PflinkVersion="$BUILD_FLINK")
# Everything in lib/ is copied into the Flink image, so clear the other line's JARs.
rm -f lib/flink-proto-confluent-*.jar lib/flink-sql-connector-kafka-*.jar
# The shadow jar carries the "all" classifier; the unclassified jar is the thin library jar.
# [0-9] keeps the Flink 1.20 glob from also matching flink-proto-confluent-flink2-*.
JAR=$(ls -t ../../build/libs/"$ARTIFACT"-[0-9]*-all.jar | head -1)
cp "$JAR" lib/
CONN=flink-sql-connector-kafka-$KAFKA_CONNECTOR.jar
mkdir -p .cache
[ -f ".cache/$CONN" ] || curl -fL -o ".cache/$CONN" \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/$KAFKA_CONNECTOR/$CONN"
cp ".cache/$CONN" lib/
echo "Flink $FLINK_VERSION stack (flink:$FLINK_VERSION-java17):"
ls -l lib
