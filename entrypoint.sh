#!/bin/sh
set -eu

(cd /app && exec ./MicroStack) &
microstack_pid="$!"
(cd /app/ui && exec ./MicroStack.UI) &
ui_pid="$!"

cleanup() {
  trap - INT TERM
  kill "$microstack_pid" 2>/dev/null || true
  kill "$ui_pid" 2>/dev/null || true
  wait "$microstack_pid" 2>/dev/null || true
  wait "$ui_pid" 2>/dev/null || true
}

trap 'cleanup; exit 143' TERM
trap 'cleanup; exit 130' INT

while kill -0 "$microstack_pid" 2>/dev/null && kill -0 "$ui_pid" 2>/dev/null; do
  sleep 1
done

status=0
if ! kill -0 "$microstack_pid" 2>/dev/null; then
  wait "$microstack_pid" || status=$?
else
  wait "$ui_pid" || status=$?
fi
cleanup
exit "$status"
