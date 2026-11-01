#!/bin/sh
# Deterministic tests for calendar-version.sh. Usage: sh scripts/calendar-version.test.sh
set -eu

script="$(dirname "$0")/calendar-version.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
failures=0

tags() {
  printf '%s\n' "$@" > "$work/tags"
}

expect() {
  name="$1"; date="$2"; expected="$3"
  if actual="$(sh "$script" --date "$date" --tags-file "$work/tags" 2>"$work/err")" && [ "$actual" = "$expected" ]; then
    echo "ok   - $name"
  else
    echo "FAIL - $name: expected '$expected', got '${actual:-}' $(cat "$work/err")"
    failures=$((failures + 1))
  fi
}

expect_failure() {
  name="$1"; date="$2"; message="$3"
  if sh "$script" --date "$date" --tags-file "$work/tags" >/dev/null 2>"$work/err"; then
    echo "FAIL - $name: expected failure"
    failures=$((failures + 1))
  elif grep -q "$message" "$work/err"; then
    echo "ok   - $name"
  else
    echo "FAIL - $name: unexpected error: $(cat "$work/err")"
    failures=$((failures + 1))
  fi
}

tags
expect "first release with no tags" 2026-11-01 2026.1101.1

tags v0.1.0 v0.4.0 v0.5.0-beta.1
expect "legacy tags are ignored" 2026-11-01 2026.1101.1

tags v0.4.0 v2026.1101.1
expect "second release on the same day" 2026-11-01 2026.1101.2

tags v2026.1101.1 v2026.1101.2 v2026.1101.10 v2026.1101.9
expect "counter uses numeric ordering" 2026-11-01 2026.1101.11

tags v2026.1101.3
expect "counter resets on a new day" 2026-11-02 2026.1102.1

tags v2026.1231.2
expect "year rollover with January date" 2027-01-05 2027.105.1

tags v2027.105.1 v2027.1231.1
expect "January after December of the previous year" 2028-01-01 2028.101.1

tags v2027.105.4
expect "January counter" 2027-01-05 2027.105.5

tags v2027.105.3 v2027.1001.1
expect "month-day uses numeric rather than lexical ordering" 2027-10-01 2027.1001.2
expect_failure "month-day ordering prevents backdating" 2027-09-30 "earlier than the latest release"

tags
expect "leap day" 2028-02-29 2028.229.1

tags v2028.229.1
expect "leap-day release tag is recognized" 2028-02-29 2028.229.2

tags "abc123	refs/tags/v2026.1101.1" "def456	refs/tags/v2026.1101.1^{}" "fed789	refs/tags/v0.4.0"
expect "git ls-remote output" 2026-11-01 2026.1101.2

tags v2026.1101.1-beta.1 v2026.01101.5 v2026.1101.01
expect "non-canonical and prerelease tags are ignored" 2026-11-01 2026.1101.1

tags "abc123	refs/tags/archive/refs/tags/v2026.1101.99"
expect "nested refs do not masquerade as release tags" 2026-11-01 2026.1101.1

tags refs/tags/v2026.1101.2
expect "bare refs are recognized" 2026-11-01 2026.1101.3

tags v2026.1131.1
expect_failure "invalid tag dates are rejected" 2026-11-30 "invalid date in release tag"

tags v2027.229.1
expect_failure "non-leap-year tag dates are rejected" 2027-03-01 "invalid date in release tag"

tags v2100.229.1
expect_failure "non-leap century tag dates are rejected" 2100-03-01 "invalid date in release tag"

tags v2026.1102.1
expect_failure "backdated release is rejected" 2026-11-01 "earlier than the latest release"

tags v2027.105.1
expect_failure "backdated release across years is rejected" 2026-12-31 "earlier than the latest release"

tags v2026.1101.65534
expect_failure "counter overflow is rejected" 2026-11-01 "exceeds the .NET version component limit"

tags v2026.1101.65533
expect "largest supported counter" 2026-11-01 2026.1101.65534

tags v2026.1101.65533 v2026.1101.18446744073709551615
expect_failure "oversized counter cannot wrap around" 2026-11-01 "exceeds the .NET version component limit"

tags v2026.1101.184467440737095516160000000000000000000
expect_failure "arbitrarily large counter is rejected" 2026-11-01 "exceeds the .NET version component limit"

tags
expect_failure "invalid date is rejected" 2027-02-29 "invalid date"
expect_failure "malformed date is rejected" 2026-1-1 "invalid date"
expect_failure "zero month is rejected explicitly" 2026-00-01 "invalid date"
expect_failure "zero day is rejected explicitly" 2026-11-00 "invalid date"
expect_failure "year must remain a four-digit component" 0001-01-01 "four-digit version component"

if sh "$script" --date 2026-11-01 --tags-file "$work/missing" >/dev/null 2>&1; then
  echo "FAIL - missing tags file should fail"
  failures=$((failures + 1))
else
  echo "ok   - missing tags file fails"
fi

if sh "$script" --date "" --tags-file "$work/tags" >"$work/out" 2>"$work/err"; then
  echo "FAIL - empty date should fail"
  failures=$((failures + 1))
elif grep -q -- "--date requires a value" "$work/err"; then
  echo "ok   - empty date fails explicitly"
else
  echo "FAIL - empty date: unexpected error: $(cat "$work/err")"
  failures=$((failures + 1))
fi

if sh "$script" --tags-file "" >"$work/out" 2>"$work/err"; then
  echo "FAIL - empty tags file should fail"
  failures=$((failures + 1))
elif grep -q -- "--tags-file requires a value" "$work/err"; then
  echo "ok   - empty tags file fails explicitly"
else
  echo "FAIL - empty tags file: unexpected error: $(cat "$work/err")"
  failures=$((failures + 1))
fi

mkdir "$work/bin"
cat > "$work/bin/git" <<'EOF'
#!/bin/sh
if [ "${CALENDAR_TEST_REMOTE_FAILURE:-}" = "1" ]; then
  echo "simulated remote failure" >&2
  exit 128
fi
printf 'abc123\trefs/tags/v2026.1101.4\n'
EOF
chmod +x "$work/bin/git"

if actual="$(PATH="$work/bin:$PATH" sh "$script" --date 2026-11-01 2>"$work/err")" \
  && [ "$actual" = "2026.1101.5" ]; then
  echo "ok   - reads tags from the remote"
else
  echo "FAIL - reads tags from the remote: got '${actual:-}' $(cat "$work/err")"
  failures=$((failures + 1))
fi

if PATH="$work/bin:$PATH" CALENDAR_TEST_REMOTE_FAILURE=1 sh "$script" --date 2026-11-01 >"$work/out" 2>"$work/err"; then
  echo "FAIL - remote lookup failure should fail"
  failures=$((failures + 1))
elif grep -q "failed to list tags from origin" "$work/err"; then
  echo "ok   - remote lookup failure is reported"
else
  echo "FAIL - remote lookup failure: unexpected error: $(cat "$work/err")"
  failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
  echo "$failures test(s) failed" >&2
  exit 1
fi
echo "All calendar-version tests passed."
