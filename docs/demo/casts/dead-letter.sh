#!/usr/bin/env bash
. "$(dirname "$0")/_lib.sh"
say "Poison records: skip them and keep the raw bytes on a dead-letter topic"
run create-topic events
run "echo '{\"order_id\":\"e-1\",\"customer\":\"acme\",\"amount\":5.0}' | produce-proto events"
say "Someone writes plain text to the topic"
run "echo 'this is not protobuf' | produce-raw events"
run "echo '{\"order_id\":\"e-2\",\"customer\":\"globex\",\"amount\":12.0}' | produce-proto events"
say "on-deserialize-error = skip, dead-letter-topic = events-dlq"
run "sed -n '14,21p' sql/dead-letter.sql"
say "The job survives: both valid rows come through"
run flink-sql sql/dead-letter.sql
say "The bad record is on the DLQ with error headers"
run consume-raw events-dlq
