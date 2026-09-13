#!/bin/sh
set -eu

image="${1:?Usage: smoke-container.sh IMAGE}"
api_port="${MICROSTACK_SMOKE_API_PORT:-4566}"
ui_port="${MICROSTACK_SMOKE_UI_PORT:-4567}"
container_id="$(docker run --rm -d \
  -e "MICROSTACK_API_URL=http://localhost:$api_port" \
  -e "MICROSTACK_UI_PORT=$ui_port" \
  -p "127.0.0.1:$api_port:4566" \
  -p "127.0.0.1:$ui_port:$ui_port" \
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
ui="http://localhost:$ui_port"
curl --fail --silent --show-error --retry 20 --retry-connrefused --retry-delay 1 "$api/_microstack/health" >/dev/null
curl --fail --silent --show-error --retry 20 --retry-connrefused --retry-delay 1 "$ui/" >/dev/null
curl --fail --silent --show-error "$api/_microstack/admin/v1/services" >/dev/null
curl --fail --silent --show-error "$ui/_microstack/ui-config" | grep -q '"apiBaseUrl"'
curl --fail --silent --show-error "$ui/services/s3" | grep -qi '<html'
curl --fail --silent --show-error "$ui/icons/s3.svg" | grep -q '<svg'
framework_path="$(curl --fail --silent --show-error "$ui/" | sed -n 's/.*src="\(_framework\/[^"]*\)".*/\1/p' | head -n 1)"
test -n "$framework_path"
framework_type="$(curl --fail --silent --show-error -o /dev/null -w '%{content_type}' "$ui/$framework_path")"
case "$framework_type" in
  *javascript*) ;;
  *) echo "Framework script returned $framework_type, not JavaScript" >&2; exit 1 ;;
esac
curl --fail --silent --show-error -D - -o /dev/null -H "Origin: $ui" "$api/_microstack/admin/v1/context" \
  | grep -qi "access-control-allow-origin: $ui"
echo "API, UI, deep links, framework, icons, runtime configuration, and CORS passed."
