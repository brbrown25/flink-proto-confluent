#!/usr/bin/env bash
# Builds the shadow JAR and fetches the Kafka SQL connector into ./lib for the demo stack.
#
# FLINK_VERSION picks the Flink line the stack runs: 1.20 (default) or 2.2. It selects the Flink
# image, the Kafka SQL connector and the -PflinkVersion the format JAR is built against, and is
# written to .env so that later `docker compose` and bin/ calls use the same image.
set -euo pipefail
cd "$(dirname "$0")"
FLINK_VERSION=${FLINK_VERSION:-1.20}
# BUILD_FLINK, ARTIFACT and KAFKA_CONNECTOR come from the line's flink<line>.* entries in
# gradle.properties, the versions CI builds, tests and releases (#249), so the docs-demo job reuses the
# Flink artifacts already in the shared Gradle cache.
case "$FLINK_VERSION" in
  1.20) LINE=1 ;;
  2.2) LINE=2 ;;
  *) echo "FLINK_VERSION must be 1.20 or 2.2, got '$FLINK_VERSION'" >&2; exit 2 ;;
esac
line_property() {
  sed -n "s/^flink$LINE\.$1=//p" ../../gradle.properties | tr -d '[:space:]'
}
BUILD_FLINK=$(line_property version)
ARTIFACT=$(line_property artifactId)
KAFKA_CONNECTOR=$(line_property kafkaConnectorVersion)
if [ -z "$BUILD_FLINK" ] || [ -z "$ARTIFACT" ] || [ -z "$KAFKA_CONNECTOR" ]; then
  echo "gradle.properties lacks flink$LINE.version, flink$LINE.artifactId or flink$LINE.kafkaConnectorVersion" >&2
  exit 2
fi
case "$BUILD_FLINK" in
  "$FLINK_VERSION".*) ;;
  *) echo "flink$LINE.version=$BUILD_FLINK in gradle.properties is not a Flink $FLINK_VERSION release" >&2; exit 2 ;;
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
