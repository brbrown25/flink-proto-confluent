#!/usr/bin/env bash
# Re-records casts into ../content/assets/casts. Needs: docker, asciinema.
#   ./record.sh                  rebuild the JAR, reset the stack, record every cast
#   ./record.sh NAME...          record only the named casts, reusing the stack if it is already healthy
#   ./record.sh --reset NAME...  same, but always rebuild and reset the stack first
# CASTS="a b" ./record.sh is still accepted. Reusing a stack keeps state from earlier casts (topics,
# schema ids), so do a full or --reset run before committing recordings.
# FLINK_VERSION=2.2 ./record.sh ... records against the Flink 2.2 stack instead of the default 1.20
# (see setup.sh). The committed casts are recorded on the default Flink 1.20 stack.
set -euo pipefail
cd "$(dirname "$0")"
OUT=../content/assets/casts
ALL="quickstart write-back dead-letter schema-evolution breaking-change-dlq compat-modes"

reset=""
names=()
for arg in "$@"; do
  case "$arg" in
    --reset) reset=1 ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    -*) echo "unknown option: $arg" >&2; exit 2 ;;
    *) names+=("$arg") ;;
  esac
done
[ ${#names[@]} -gt 0 ] || read -ra names <<<"${CASTS:-$ALL}"
for name in "${names[@]}"; do
  [ -x "casts/$name.sh" ] || { echo "no such cast: casts/$name.sh" >&2; exit 2; }
done

stack_healthy() {
  docker compose exec -T schema-registry curl -sf localhost:8081/subjects >/dev/null 2>&1 &&
    docker compose exec -T jobmanager curl -sf localhost:8081/taskmanagers 2>/dev/null | grep -q '"id"'
}
wait_for_stack() {
  echo "waiting for Schema Registry..."
  until docker compose exec -T schema-registry curl -sf localhost:8081/subjects >/dev/null 2>&1; do sleep 2; done
  until docker compose exec -T jobmanager curl -sf localhost:8081/taskmanagers 2>/dev/null | grep -q '"id"'; do sleep 2; done
}

# A full run (no names given) always starts clean; a partial run reuses a healthy stack, unless
# that stack runs a different Flink version than the one asked for.
partial=""; [ $# -gt 0 ] && [ ${#names[@]} -gt 0 ] && partial=1
running=$(sed -n 's/^FLINK_VERSION=//p' .env 2>/dev/null || true)
[ "$running" = "${FLINK_VERSION:-1.20}" ] || reset=1
if [ -n "$reset" ] || [ -z "$partial" ] || ! stack_healthy; then
  ./setup.sh >/dev/null
  docker compose down -v >/dev/null 2>&1
  docker compose up -d >/dev/null 2>&1
  wait_for_stack
else
  echo "reusing healthy Flink $running stack (pass --reset for a clean one)"
fi

mkdir -p "$OUT"
for name in "${names[@]}"; do
  asciinema rec --overwrite --quiet --cols 110 --rows 32 --idle-time-limit 2 \
    --command "./casts/$name.sh" "$OUT/$name.cast"
done
echo "recorded:"; ls -l "$OUT"
