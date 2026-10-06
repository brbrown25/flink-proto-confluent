# Typing simulation for the recorded casts.
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export PATH="$PWD/bin:$PATH"
BOLD=$'\e[1m'; DIM=$'\e[2m'; GREEN=$'\e[32m'; RESET=$'\e[0m'
say() { printf '%s# %s%s\n' "$DIM" "$*" "$RESET"; sleep 1.2; }
run() {
  printf '%s$ %s' "$GREEN" "$RESET"
  local s="$*" i
  for ((i = 0; i < ${#s}; i++)); do printf '%s' "${s:$i:1}"; sleep 0.03; done
  printf '\n'; sleep 0.4
  eval "$s"
  sleep 1.8
}
