#!/usr/bin/env bash
. "$(dirname "$0")/_lib.sh"
say "Quickstart: query a Protobuf topic from Flink SQL with the proto-confluent format"
say "A topic of Protobuf Orders, framed in the Confluent wire format..."
run create-topic orders
run "printf '%s\n' '{\"order_id\":\"o-1001\",\"customer\":\"acme\",\"amount\":42.5}' '{\"order_id\":\"o-1002\",\"customer\":\"globex\",\"amount\":99.0}' '{\"order_id\":\"o-1003\",\"customer\":\"initech\",\"amount\":7.25}' | produce-proto orders"
say "...and the schema the producer registered"
run schema orders-value
say "Generate the Flink table from the subject with the format JAR's ddl command"
run "generate-ddl --subject orders-value --bootstrap-servers kafka:9092 --option properties.group.id=docs-demo --option scan.startup.mode=earliest-offset --option scan.bounded.mode=latest-offset | tee sql/orders.sql"
say "Declare the Flink table over it and aggregate with plain SQL"
run "grep -v '^--' sql/quickstart-generated.sql"
run flink-sql sql/orders.sql sql/quickstart-generated.sql
