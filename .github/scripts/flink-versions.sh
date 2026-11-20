#!/usr/bin/env bash
# Reads the Flink version of each supported line from gradle.properties (`flink<line>.version`),
# the single place they are set (#249), so every workflow builds, tests and publishes the same ones.
#
# Usage: flink-versions.sh [path/to/gradle.properties]
#
# Writes to $GITHUB_OUTPUT when set (stdout otherwise), ordered by line:
#   matrix=["1.20.5","2.2.1"]   JSON array for strategy.matrix (fromJSON)
#   list=1.20.5 2.2.1           space-separated, for shell loops
#   primary=1.20.5              the Flink 1 line: the build's default and the coverage leg
#   flink1=1.20.5 / flink2=...  one output per line
set -euo pipefail

props=${1:-gradle.properties}
pairs=$(sed -nE 's/^[[:space:]]*flink([0-9]+)\.version[[:space:]]*=[[:space:]]*([^[:space:]]+)[[:space:]]*$/\1 \2/p' "$props" | sort -n)
if [ -z "$pairs" ]; then
  echo "::error file=$props::no flink<line>.version entries in $props" >&2
  exit 1
fi
primary=$(printf '%s\n' "$pairs" | awk '$1 == 1 { print $2 }')
if [ -z "$primary" ]; then
  echo "::error file=$props::no flink1.version in $props" >&2
  exit 1
fi
versions=$(printf '%s\n' "$pairs" | cut -d' ' -f2)

out=${GITHUB_OUTPUT:-/dev/stdout}
{
  echo "matrix=$(printf '%s\n' "$versions" | jq -Rnc '[inputs]')"
  echo "list=$(printf '%s\n' "$versions" | paste -sd' ' -)"
  echo "primary=$primary"
  printf '%s\n' "$pairs" | while read -r line version; do echo "flink$line=$version"; done
} >>"$out"
