#!/usr/bin/env bash
# Tests for breaking-change-declared.sh. Run: .github/scripts/breaking-change-declared.test.sh
set -uo pipefail

script="$(cd "$(dirname "$0")" && pwd)/breaking-change-declared.sh"
rs=$'\x1e'
failures=0

# expect <expected exit code> <description> <title> <commit messages, records ending in 0x1e>
expect() {
  local want=$1 description=$2 title=$3 commits=$4 got
  printf '%s' "$commits" | "$script" "$title" > /dev/null
  got=$?
  if [ "$got" -eq "$want" ]; then
    echo "ok   - $description"
  else
    echo "FAIL - $description: exit $got, want $want"
    failures=$((failures + 1))
  fi
}

plain_commit="feat(build): add a check. #220

Closes #220
${rs}"

expect 1 "no marker anywhere" \
  "feat(build): add a check. #220" "$plain_commit"
expect 0 "! after the scope in the title" \
  "feat(api)!: remove a method. #220" "$plain_commit"
expect 0 "! without a scope in the title" \
  "chore!: require Flink 2. #220" "$plain_commit"
expect 0 "! in a commit subject" \
  "feat(api): remove a method. #220" "bug(api)!: remove a method. #220
${rs}${plain_commit}"
expect 0 "BREAKING CHANGE footer in a commit" \
  "feat(api): remove a method. #220" "feat(api): remove a method. #220

Callers must use the new overload.

BREAKING CHANGE: ProtoToTableDDL.printTableSchemaFromProto is gone.
Closes #220
${rs}"
expect 0 "BREAKING-CHANGE footer in a commit" \
  "feat(api): remove a method. #220" "feat(api): remove a method. #220

BREAKING-CHANGE: gone.
${rs}"
expect 0 "marker in a later commit of several" \
  "feat(api): two commits. #220" "${plain_commit}chore(api)!: drop it. #220
${rs}"
expect 1 "! elsewhere in the title is not a marker" \
  "feat(api): stop failing! #220" "$plain_commit"
expect 1 "! after the colon is not a marker" \
  "feat(api): !important. #220" "$plain_commit"
expect 1 "! in a commit body line is not a marker" \
  "feat(api): remove a method. #220" "feat(api): remove a method. #220

Unlike feat(x)!: this is not breaking.
${rs}"
expect 1 "BREAKING CHANGE not at the start of a line is not a footer" \
  "feat(api): remove a method. #220" "feat(api): remove a method. #220

This is not a BREAKING CHANGE: really.
${rs}"
expect 1 "lowercase breaking change is not a footer" \
  "feat(api): remove a method. #220" "feat(api): x. #220

breaking change: no.
${rs}"
expect 1 "no commits at all" \
  "feat(api): remove a method. #220" ""
expect 1 "an empty title is not a marker" \
  "" "$plain_commit"

# The usage error needs no title argument at all, which expect() cannot express.
"$script" < /dev/null > /dev/null 2>&1
if [ $? -eq 2 ]; then
  echo "ok   - no argument is a usage error"
else
  echo "FAIL - no argument is a usage error"
  failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
  echo "$failures test(s) failed"
  exit 1
fi
echo "all tests passed"
