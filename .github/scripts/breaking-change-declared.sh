#!/usr/bin/env bash
# Decides whether a pull request declares a breaking change, the marker that lets the `japicmp`
# API compatibility check pass with incompatible changes (issue #220).
#
# Usage: breaking-change-declared.sh <pull request title> < commit messages
#
# stdin holds the full messages of the pull request's commits, each one terminated by an ASCII
# record separator (0x1e), as `git log --format='%B%x1e' <base>..<head>` prints them.
#
# A breaking change is declared, as in Conventional Commits, by either:
#   - `!` right before the colon of the type/scope, in the title or in a commit subject:
#     `feat(api)!: drop the deprecated option. #123`, `chore!: require Flink 2. #124`
#   - a `BREAKING CHANGE:` (or `BREAKING-CHANGE:`) footer line in a commit message.
#
# Exits 0 when a breaking change is declared, 1 when it is not, 2 on a usage error, and prints the
# reason. When only the title carries the marker it also prints a GitHub Actions warning:
# release-please reads the commits that land on main, so a rebase merge would ship the break
# without the major version bump.
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <pull request title> < commit messages" >&2
  exit 2
fi
title=$1

# type, optional (scope), then `!` and the colon.
subject_marker='^[A-Za-z]+(\([^)]*\))?!:'
footer_marker='^BREAKING[ -]CHANGE:'

title_marked=false
if printf '%s\n' "$title" | grep -qE "$subject_marker"; then
  title_marked=true
fi

# For each record: the first non-blank line is the subject; a footer may be on any line.
# The patterns go through the environment: awk -v would interpret their backslashes.
commit_reason=$(SUBJECT="$subject_marker" FOOTER="$footer_marker" awk -v RS=$'\x1e' '
  BEGIN { subject = ENVIRON["SUBJECT"]; footer = ENVIRON["FOOTER"] }
  {
    n = split($0, lines, "\n")
    first = ""
    for (i = 1; i <= n; i++) {
      if (first == "" && lines[i] ~ /[^[:space:]]/) {
        first = lines[i]
        if (first ~ subject) { print "commit subject: " first; exit }
      }
      if (lines[i] ~ footer) { print "commit footer: " lines[i]; exit }
    }
  }
')

if [ -n "$commit_reason" ]; then
  echo "Breaking change declared by $commit_reason"
  exit 0
elif [ "$title_marked" = true ]; then
  echo "Breaking change declared by the pull request title: $title"
  echo "::warning title=Breaking change marker only in the title::release-please reads the commits that land on main. Squash-merge this pull request (the title becomes the commit), or add the marker to a commit; a rebase merge would release the break without a major version bump."
  exit 0
else
  echo "No breaking-change marker in the pull request title or its commits."
  exit 1
fi
