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
# linker it spawned have exited, including on a crash or kill. The cap is
# best effort: if a slot cannot be opened, rustc runs uncapped rather than
# failing the build.

for arg in "$@"; do
  # Version and target probes are instant and must never queue.
  case $arg in
    -vV | -V | --version | --print | --print=*) exec "$@" ;;
  esac
done

slots=${PIPESTREAM_RUSTC_SLOTS:-12}
dir=${PIPESTREAM_RUSTC_SLOT_DIR:-/tmp/rustc-slots}
mkdir -p "$dir" 2>/dev/null && chmod 1777 "$dir" 2>/dev/null

# Slot files are shared between users (CI containers run as root, host
# builds do not) in a sticky world-writable directory, where the kernel's
# protected_regular refuses an O_CREAT open of another user's file. So a
# missing slot is created once, and every slot is then opened read-only,
# which flock accepts and protected_regular does not apply to.
open_slot() {
  local path="$dir/slot-$1"
  if [[ ! -e $path ]]; then
    (umask 000 && : >>"$path") 2>/dev/null
  fi
  exec {slot_fd}<"$path"
} 2>/dev/null

opened=0
# Take the first free slot; when every slot is busy, poll until one frees.
while :; do
  for ((i = 1; i <= slots; i++)); do
    open_slot "$i" || continue
    opened=1
    if flock -n "$slot_fd"; then
      exec "$@"
    fi
    exec {slot_fd}<&-
  done
  ((opened)) || exec "$@"
  sleep 0.$((RANDOM % 5 + 2))
done
