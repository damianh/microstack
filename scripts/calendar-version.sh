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

LC_ALL=C
export LC_ALL

fail() {
  echo "calendar-version: $*" >&2
  exit 1
}

release_date=""
tags_file=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --date) [ "$#" -ge 2 ] && [ -n "$2" ] || fail "--date requires a value"; release_date="$2"; shift 2 ;;
    --tags-file) [ "$#" -ge 2 ] && [ -n "$2" ] || fail "--tags-file requires a value"; tags_file="$2"; shift 2 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[ -n "$release_date" ] || release_date="$(date -u +%Y-%m-%d)"

if ! printf '%s\n' "$release_date" | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'; then
  fail "invalid date '$release_date'; expected YYYY-MM-DD"
fi
# Reject impossible dates such as 2026-02-30 by round-tripping through date(1).
if [ "$(date -u -d "$release_date" +%Y-%m-%d 2>/dev/null || true)" != "$release_date" ]; then
  fail "invalid date '$release_date'"
fi
year="${release_date%%-*}"
rest="${release_date#*-}"
month="${rest%%-*}"
day="${rest#*-}"
case "$year" in 0*) fail "release year must be a four-digit version component" ;; esac
month="${month#0}"
day="${day#0}"
monthday=$((month * 100 + day))

if [ -n "$tags_file" ]; then
  [ -r "$tags_file" ] || fail "cannot read tags file '$tags_file'"
  tags="$(cat < "$tags_file")" || fail "cannot read tags file '$tags_file'"
else
  tags="$(git ls-remote --tags --refs origin)" || fail "failed to list tags from origin"
fi

# Keep only canonical calendar tags, whether given as bare names or remote refs.
calendar_tags="$(printf '%s\n' "$tags" | awk '
  function valid_date(year, monthday, month, day, max_days) {
    month = int(monthday / 100)
    day = monthday % 100
    if (month < 1 || month > 12 || day < 1) return 0
    max_days = 31
    if (month == 4 || month == 6 || month == 9 || month == 11) max_days = 30
    if (month == 2) max_days = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 29 : 28
    return day <= max_days
  }
  {
    sub(/^[^\t]*\t/, "")
    sub(/^refs\/tags\//, "")
    sub(/\^\{\}$/, "")
    if ($0 ~ /^v[1-9][0-9][0-9][0-9]\.[1-9][0-9][0-9][0-9]?\.[1-9][0-9]*$/) {
      split(substr($0, 2), parts, "[.]")
      if (!valid_date(parts[1], parts[2])) {
        print "calendar-version: invalid date in release tag " $0 > "/dev/stderr"
        exit 1
      }
      print substr($0, 2)
    }
  }
')" || fail "failed to filter release tags"
calendar_tags="$(printf '%s\n' "$calendar_tags" | sort -u -t. -k1,1n -k2,2n -k3,3n)" \
  || fail "failed to sort release tags"

counter=1
if [ -n "$calendar_tags" ]; then
  latest="$(printf '%s\n' "$calendar_tags" | tail -n 1)"
  latest_year="${latest%%.*}"
  latest_rest="${latest#*.}"
  latest_monthday="${latest_rest%%.*}"
  if [ "$year" -lt "$latest_year" ] || { [ "$year" -eq "$latest_year" ] && [ "$monthday" -lt "$latest_monthday" ]; }; then
    fail "release date $release_date is earlier than the latest release v$latest"
  fi

  if [ "$year" -eq "$latest_year" ] && [ "$monthday" -eq "$latest_monthday" ]; then
    last_counter="${latest_rest#*.}"
    # Check the string length before arithmetic, which could wrap on an oversized tag.
    if [ "${#last_counter}" -gt 5 ] || { [ "${#last_counter}" -eq 5 ] && [ "$last_counter" -ge 65534 ]; }; then
      fail "release counter for $release_date exceeds the .NET version component limit"
    fi
    counter=$((last_counter + 1))
  fi
fi

echo "$year.$monthday.$counter"
