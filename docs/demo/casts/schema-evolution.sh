#!/usr/bin/env bash
. "$(dirname "$0")/_lib.sh"
say "Schema evolution: v1, then a compatible v2, then a breaking v3 (registry mode: BACKWARD)"
run create-topic orders
run "echo '{\"order_id\":\"o-1\",\"customer\":\"acme\",\"amount\":5.0}' | SCHEMA=\"\$V1\" produce-proto orders"
run schema orders-value
say "v2 adds 'string currency = 4'. Adding a field is compatible"
run "SCHEMA=\"\$V2\" check orders-value"
run "SCHEMA=\"\$V2\" register orders-value"
run "echo '{\"order_id\":\"o-2\",\"customer\":\"globex\",\"amount\":12.0,\"currency\":\"USD\"}' | SCHEMA=\"\$V2\" produce-proto orders"
say "A table declared against v2 reads both versions: the v1 row gets NULL"
run flink-sql sql/evolution-read.sql
say "v3 changes 'amount' from double to string. That breaks readers"
run "SCHEMA=\"\$V3\" check orders-value"
run "SCHEMA=\"\$V3\" register orders-value"
say "Rejected: the subject still has only v1 and v2"
run "docker compose exec -T schema-registry curl -s localhost:8081/subjects/orders-value/versions; echo"
