#!/usr/bin/env bash
# Rustc wrapper that bounds how many rustc processes run at once on a host,
# across every cargo invocation, worktree and CI job that shares one slot
# directory. Linking happens inside rustc, so this also bounds link storms.
#
#   ~/.cargo/config.toml:  [build] rustc-wrapper = "/path/to/rustc-slot.sh"
#   PIPESTREAM_RUSTC_SLOTS     slots per host (default 12)
#   PIPESTREAM_RUSTC_SLOT_DIR  lock directory (default /tmp/rustc-slots)
#
# Cargo calls it as `rustc-slot.sh <rustc> <args...>`. The slot lock is an
# flock on an inherited descriptor, so it is released when rustc and every
# linker it spawned have exited, including on a crash or kill.
set -euo pipefail

for arg in "$@"; do
  # Version and target probes are instant and must never queue.
  case $arg in
    -vV | -V | --version | --print | --print=*) exec "$@" ;;
  esac
done

slots=${PIPESTREAM_RUSTC_SLOTS:-12}
dir=${PIPESTREAM_RUSTC_SLOT_DIR:-/tmp/rustc-slots}
mkdir -p "$dir"
chmod 1777 "$dir" 2>/dev/null || true

open_slot() {
  local path="$dir/slot-$1"
  exec {slot_fd}>>"$path"
  chmod 666 "$path" 2>/dev/null || true
}

# Take the first free slot; when every slot is busy, poll until one frees.
while :; do
  for ((i = 1; i <= slots; i++)); do
    open_slot "$i"
    if flock -n "$slot_fd"; then
      exec "$@"
    fi
    exec {slot_fd}>&-
  done
  sleep 0.$((RANDOM % 5 + 2))
done
