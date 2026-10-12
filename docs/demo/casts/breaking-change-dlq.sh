#!/usr/bin/env bash
. "$(dirname "$0")/_lib.sh"
say "Breaking change forced through (compatibility NONE): readers keep running, bad rows go to the DLQ"
run create-topic invoices
run "echo '{\"order_id\":\"i-1\",\"customer\":\"acme\",\"amount\":5.0}' | SCHEMA=\"\$V1\" produce-proto invoices"
say "A producer team turns compatibility off and ships amount as a string"
run set-compat invoices-value NONE
run "echo '{\"order_id\":\"i-2\",\"customer\":\"globex\",\"amount\":\"9.99\"}' | SCHEMA=\"\$V3\" produce-proto invoices"
run "echo '{\"order_id\":\"i-3\",\"customer\":\"initech\",\"amount\":7.5}' | SCHEMA=\"\$V1\" produce-proto invoices"
say "Our table still declares amount DOUBLE; on-deserialize-error = skip + a dead-letter-topic"
run "sed -n '18,21p' sql/breaking-dlq.sql"
say "The v1-shaped rows come through, the job survives"
run flink-sql sql/breaking-dlq.sql
say "The v3 record is on the DLQ with the type mismatch as its error"
run consume-raw invoices-dlq
