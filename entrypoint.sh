#!/bin/sh
set -eu

/app/MicroStack &
microstack_pid="$!"

cleanup() {
  kill "$microstack_pid" 2>/dev/null || true
}

trap cleanup INT TERM

/app/MicroStack.UI
