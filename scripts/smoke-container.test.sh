#!/bin/sh
set -eu

script="$(dirname "$0")/smoke-container.sh"
work="$(mktemp -d)"
cleanup() {
  rm -f "$work/bin/docker" "$work/bin/curl" "$work/out" "$work/err"
  rmdir "$work/bin" "$work"
}
trap cleanup EXIT
mkdir "$work/bin"

cat > "$work/bin/docker" <<'EOF'
#!/bin/sh
case "$1" in
  run) echo "test-container" ;;
  rm|logs) exit 0 ;;
  *) echo "Unexpected docker command: $*" >&2; exit 1 ;;
esac
EOF

cat > "$work/bin/curl" <<'EOF'
#!/bin/sh
for url do :; done
case "$url" in
  */_microstack/health) printf '%s\n' "$PRIMARY_HEALTH" ;;
  */_localstack/health) printf '%s\n' "$LOCALSTACK_HEALTH" ;;
  */health) printf '%s\n' "$SHORT_HEALTH" ;;
  */_microstack/admin/v1/services) echo '{}' ;;
  */ui/) echo '<script src="_framework/app.js"></script>' ;;
  */ui/services/s3) echo '<html></html>' ;;
  */ui/icons/s3.svg) echo '<svg></svg>' ;;
  */ui/_framework/app.js) echo 'application/javascript' ;;
  */) printf 'HTTP/1.1 302 Found\r\nLocation: /ui/\r\n\r\n' ;;
  *) echo "Unexpected URL: $url" >&2; exit 1 ;;
esac
EOF
chmod +x "$work/bin/docker" "$work/bin/curl"

PATH="$work/bin:$PATH"
MICROSTACK_EXPECTED_VERSION=2026.1101.7
PRIMARY_HEALTH='{"edition":"light","version":"2026.1101.7"}'
LOCALSTACK_HEALTH="$PRIMARY_HEALTH"
SHORT_HEALTH="$PRIMARY_HEALTH"
export PATH MICROSTACK_EXPECTED_VERSION PRIMARY_HEALTH LOCALSTACK_HEALTH SHORT_HEALTH

if sh "$script" test-image >"$work/out" 2>"$work/err"; then
  echo "ok   - matching versions pass on all health aliases"
else
  echo "FAIL - matching versions: $(cat "$work/err")" >&2
  exit 1
fi

PRIMARY_HEALTH='{"services":{"version":"2026.1101.7"},"version":"2026X1101X7"}'
export PRIMARY_HEALTH
if sh "$script" test-image >"$work/out" 2>"$work/err"; then
  echo "FAIL - mismatched top-level version was accepted" >&2
  exit 1
elif ! grep -Fq '/_microstack/health reported an unexpected version' "$work/err"; then
  echo "FAIL - unexpected error: $(cat "$work/err")" >&2
  exit 1
fi
echo "ok   - nested or regex-like versions cannot mask a mismatch"

PRIMARY_HEALTH='{"version":"2026.1101.7"}'
LOCALSTACK_HEALTH='{"version":"2026.1101.8"}'
export PRIMARY_HEALTH LOCALSTACK_HEALTH
if sh "$script" test-image >"$work/out" 2>"$work/err"; then
  echo "FAIL - mismatched alias version was accepted" >&2
  exit 1
elif ! grep -Fq '/_localstack/health reported an unexpected version' "$work/err"; then
  echo "FAIL - unexpected error: $(cat "$work/err")" >&2
  exit 1
fi
echo "ok   - mismatched health alias is rejected"
