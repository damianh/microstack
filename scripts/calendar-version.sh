#!/bin/sh
# Prints the next release version in the form YYYY.MMDD.N (UTC).
#
# MMDD is the number month * 100 + day, so 1 November is 1101 and 5 January is 105.
# N starts at 1 for each release day and increments for every further release that day.
# Existing releases are read from tags named vYYYY.MMDD.N; legacy tags such as v0.4.0 are ignored.
#
# Usage: calendar-version.sh [--date YYYY-MM-DD] [--tags-file PATH]
#   --date       Release date (defaults to today in UTC).
#   --tags-file  File of tag names or `git ls-remote` output (defaults to querying origin).
set -eu

fail() {
  echo "calendar-version: $*" >&2
  exit 1
}

release_date=""
tags_file=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --date) [ "$#" -ge 2 ] || fail "--date requires a value"; release_date="$2"; shift 2 ;;
    --tags-file) [ "$#" -ge 2 ] || fail "--tags-file requires a value"; tags_file="$2"; shift 2 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[ -n "$release_date" ] || release_date="$(date -u +%Y-%m-%d)"

if ! printf '%s\n' "$release_date" | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'; then
  fail "invalid date '$release_date'; expected YYYY-MM-DD"
fi
year="${release_date%%-*}"
rest="${release_date#*-}"
month="${rest%%-*}"
day="${rest#*-}"
year=$(expr "$year" + 0)
month=$(expr "$month" + 0)
day=$(expr "$day" + 0)
# Reject impossible dates such as 2026-02-30 by round-tripping through date(1).
if [ "$(date -u -d "$release_date" +%Y-%m-%d 2>/dev/null || true)" != "$release_date" ]; then
  fail "invalid date '$release_date'"
fi
monthday=$((month * 100 + day))

if [ -n "$tags_file" ]; then
  [ -r "$tags_file" ] || fail "cannot read tags file '$tags_file'"
  tags="$(cat "$tags_file")"
else
  tags="$(git ls-remote --tags --refs origin)" || fail "failed to list tags from origin"
fi

# Normalise to bare tag names and keep only canonical calendar tags (no leading zeros, year >= 2000).
calendar_tags="$(printf '%s\n' "$tags" \
  | sed -e 's/^.*refs\/tags\///' -e 's/\^{}$//' \
  | grep -E '^v[1-9][0-9]{3}\.[1-9][0-9]{2,3}\.[1-9][0-9]*$' \
  | sed 's/^v//' \
  | sort -u -t. -k1,1n -k2,2n -k3,3n || true)"

counter=1
if [ -n "$calendar_tags" ]; then
  latest="$(printf '%s\n' "$calendar_tags" | tail -n 1)"
  latest_year="${latest%%.*}"
  latest_rest="${latest#*.}"
  latest_monthday="${latest_rest%%.*}"
  if [ "$year" -lt "$latest_year" ] || { [ "$year" -eq "$latest_year" ] && [ "$monthday" -lt "$latest_monthday" ]; }; then
    fail "release date $release_date is earlier than the latest release v$latest"
  fi

  last_counter="$(printf '%s\n' "$calendar_tags" \
    | awk -F. -v y="$year" -v md="$monthday" '$1 == y && $2 == md { n = $3 } END { print n + 0 }')"
  counter=$((last_counter + 1))
fi

# .NET version components (assembly/file versions) must not exceed 65534.
[ "$year" -le 65534 ] || fail "year $year exceeds the .NET version component limit"
[ "$counter" -le 65534 ] || fail "release counter $counter for $release_date exceeds the .NET version component limit"

echo "$year.$monthday.$counter"
