#!/usr/bin/env bash
. "$(dirname "$0")/_lib.sh"
say "Compatibility modes: the same breaking change (double -> string) under each mode"
run create-topic ledger
run "echo '{\"order_id\":\"l-1\",\"customer\":\"acme\",\"amount\":5.0}' | SCHEMA=\"\$V1\" produce-proto ledger"
for mode in BACKWARD FORWARD FULL NONE; do
  run "set-compat ledger-value $mode && SCHEMA=\"\$V3\" check ledger-value"
done
say "Transitive modes check every earlier version, not just the latest"
run "set-compat ledger-value NONE && SCHEMA=\"\$V3\" register ledger-value"
say "v4 only adds a field to v3, but v1 had amount as a double"
run "set-compat ledger-value BACKWARD_TRANSITIVE"
say "The registry's latest-only check says v4 is compatible..."
run "SCHEMA=\"\$V4\" check ledger-value"
say "...but checking every version shows v1 conflicts, and registration enforces that"
run "SCHEMA=\"\$V4\" check --all-versions ledger-value"
run "SCHEMA=\"\$V4\" register ledger-value"
say "BACKWARD compares with the latest version only, so v4 is accepted"
run "set-compat ledger-value BACKWARD && SCHEMA=\"\$V4\" register ledger-value"
