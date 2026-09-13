#!/bin/sh
set -eu

image="${1:?Usage: smoke-container.sh IMAGE}"
api_port="${MICROSTACK_SMOKE_API_PORT:-4566}"
container_id="$(docker run --rm -d \
  -p "127.0.0.1:$api_port:4566" \
  "$image")"
cleanup() {
  status=$?
  trap - EXIT
  if [ "$status" -ne 0 ]; then
    docker logs "$container_id" >&2 || true
  fi
  docker rm -f "$container_id" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

api="http://localhost:$api_port"
# Docker can accept and reset connections before the application starts listening.
curl --fail --silent --show-error --retry 20 --retry-all-errors --retry-delay 1 --retry-max-time 30 --max-time 5 "$api/_microstack/health" >/dev/null
curl --fail --silent --show-error --retry 20 --retry-connrefused --retry-delay 1 "$api/ui/" >/dev/null
curl --fail --silent --show-error "$api/_microstack/admin/v1/services" >/dev/null
curl --fail --silent --show-error "$api/ui/services/s3" | grep -qi '<html'
curl --fail --silent --show-error "$api/ui/icons/s3.svg" | grep -q '<svg'
framework_path="$(curl --fail --silent --show-error "$api/ui/" | sed -n 's/.*src="\(_framework\/[^"]*\)".*/\1/p' | head -n 1)"
test -n "$framework_path"
framework_type="$(curl --fail --silent --show-error -o /dev/null -w '%{content_type}' "$api/ui/$framework_path")"
case "$framework_type" in
  *javascript*) ;;
  *) echo "Framework script returned $framework_type, not JavaScript" >&2; exit 1 ;;
esac
redirect="$(curl --silent --show-error -D - -o /dev/null -H "Accept: text/html" "$api/")"
printf '%s' "$redirect" | grep -qi '^HTTP/.* 302'
printf '%s' "$redirect" | grep -qi '^location: /ui/'
echo "API, same-port UI, deep links, framework, icons, and browser redirect passed."
