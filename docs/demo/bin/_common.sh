# Shared by the demo helper scripts. Runs from docs/demo regardless of caller cwd.
cd "$(dirname "${BASH_SOURCE[0]}")/.."
SR_URL=http://schema-registry:8081
BOOTSTRAP=kafka:9092
ORDER_PROTO='syntax = "proto3"; message Order { string order_id = 1; string customer = 2; double amount = 3; }'
