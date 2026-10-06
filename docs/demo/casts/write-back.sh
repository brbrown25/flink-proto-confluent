#!/usr/bin/env bash
. "$(dirname "$0")/_lib.sh"
say "Write path: INSERT INTO a proto-confluent table, schema derived from the row type"
say "Sink table declares auto-register-schemas = true"
run "sed -n '19,40p' sql/write.sql"
run flink-sql sql/write.sql
say "Records land as Protobuf, readable by any Confluent consumer"
sleep 6
run consume-proto big-orders
say "...and the schema Flink registered from the table's row type"
run schema big-orders-value
