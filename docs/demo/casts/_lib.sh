# Typing simulation for the recorded casts. Set CAST_FAST=1 to skip the delays (dry runs).
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export PATH="$PWD/bin:$PATH"
BOLD=$'\e[1m'; DIM=$'\e[2m'; GREEN=$'\e[32m'; RESET=$'\e[0m'
nap() { [ -n "${CAST_FAST:-}" ] || sleep "$1"; }
say() { printf '%s# %s%s\n' "$DIM" "$*" "$RESET"; nap 1.2; }
run() {
  printf '%s$ %s' "$GREEN" "$RESET"
  local s="$*" i
  for ((i = 0; i < ${#s}; i++)); do printf '%s' "${s:$i:1}"; nap 0.03; done
  printf '\n'; nap 0.4
  eval "$s"
  nap 1.8
}

# Order schema versions used by the schema-evolution casts.
V1='syntax = "proto3"; message Order { string order_id = 1; string customer = 2; double amount = 3; }'
V2='syntax = "proto3"; message Order { string order_id = 1; string customer = 2; double amount = 3; string currency = 4; }'
V3='syntax = "proto3"; message Order { string order_id = 1; string customer = 2; string amount = 3; string currency = 4; }'
V4='syntax = "proto3"; message Order { string order_id = 1; string customer = 2; string amount = 3; string currency = 4; string region = 5; }'
