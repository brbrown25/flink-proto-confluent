#!/usr/bin/env bash
. "$(dirname "$0")/_lib.sh"
say "Quickstart: query a Protobuf topic from Flink SQL with the proto-confluent format"
say "A topic of Protobuf Orders, framed in the Confluent wire format..."
run create-topic orders
run "printf '%s\n' '{\"order_id\":\"o-1001\",\"customer\":\"acme\",\"amount\":42.5}' '{\"order_id\":\"o-1002\",\"customer\":\"globex\",\"amount\":99.0}' '{\"order_id\":\"o-1003\",\"customer\":\"initech\",\"amount\":7.25}' | produce-proto orders"
say "...and the schema the producer registered"
run schema orders-value
say "Declare a Flink table over it and aggregate"
run "sed -n '4,22p' sql/quickstart.sql"
run flink-sql sql/quickstart.sql
