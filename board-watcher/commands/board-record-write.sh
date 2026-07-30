#!/bin/sh
# gc board-record-write — record an own coordination-board write in the
# board-watcher hash ledger.
#
# The write-side half of the watcher's own-write filter: run this after
# every write your city makes to the coordination board, and the watcher
# will settle that change silently instead of nudging your own mayor
# about your own post. A city with a custom write path (e.g. a board-post
# helper that scp's the doc to the peer) can append the same JSONL shape
# itself — see ../schema/own_writes.schema.json and README.md
# "Own-write ledger".
#
# Usage:
#   gc board-record-write <file> [--state-dir <dir>] [--sha256 <hex>]
#   gc board-record-write <name> --deleted [--state-dir <dir>]
#
# <file> is the doc just written (its basename is what the ledger keys
# on). With --deleted, records a tombstone so an own removal of <name>
# doesn't nudge either; the file need not exist. Record the write
# IMMEDIATELY after it lands on the board — the watcher's debounce window
# is the grace period.
set -eu

die() {
  echo "gc board-record-write: $1" >&2
  exit "${2:-1}"
}

usage() {
  cat <<'EOF'
gc board-record-write — record an own coordination-board write in the
board-watcher hash ledger, so the watcher settles your own change
silently instead of nudging your own mayor about it.

Usage:
  gc board-record-write <file> [--state-dir <dir>] [--sha256 <hex>]
  gc board-record-write <name> --deleted [--state-dir <dir>]

<file> is the doc just written (its basename keys the ledger entry).
--deleted records a tombstone so an own removal doesn't nudge either.
Run it IMMEDIATELY after the write lands on the board; the watcher's
debounce window is the grace period. Ledger contract:
schema/own_writes.schema.json.
EOF
  exit 0
}

STATE_DIR="${BOARD_WATCHER_STATE_DIR:-}"
DELETED=0
SHA=""
FILE=""
while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage ;;
    --deleted) DELETED=1 ;;
    --sha256)
      [ $# -ge 2 ] || die "--sha256 needs a value"
      SHA="$2"; shift ;;
    --state-dir)
      [ $# -ge 2 ] || die "--state-dir needs a value"
      STATE_DIR="$2"; shift ;;
    -*) die "unknown flag: $1" ;;
    *)
      [ -z "$FILE" ] || die "exactly one file argument, got '$FILE' and '$1'"
      FILE="$1" ;;
  esac
  shift
done
[ -n "$FILE" ] || die "usage: gc board-record-write <file> [--deleted] [--sha256 <hex>] [--state-dir <dir>]"

if [ -z "$STATE_DIR" ]; then
  [ -n "${GC_CITY_PATH:-}" ] || die "GC_CITY_PATH is not set; pass --state-dir or set BOARD_WATCHER_STATE_DIR"
  STATE_DIR="$GC_CITY_PATH/.gc/board-watcher"
fi

BASE="$(basename "$FILE")"
# The basename is interpolated into a JSON line with printf, so hold it to
# the ledger schema's charset (leading alphanumeric, then [A-Za-z0-9._-]).
case "$BASE" in
  [A-Za-z0-9]*) ;;
  *) die "doc name must start with an alphanumeric: '$BASE'" ;;
esac
case "$BASE" in
  *[!A-Za-z0-9._-]*) die "doc name contains characters outside [A-Za-z0-9._-]: '$BASE'" ;;
esac

if [ "$DELETED" -eq 1 ]; then
  [ -z "$SHA" ] || die "--deleted and --sha256 are mutually exclusive"
  SHA="-"
elif [ -z "$SHA" ]; then
  [ -f "$FILE" ] || die "no such file: $FILE (use --sha256 to record a hash directly)"
  if command -v shasum >/dev/null 2>&1; then
    SHA="$(shasum -a 256 "$FILE" | awk '{print $1}')"
  elif command -v sha256sum >/dev/null 2>&1; then
    SHA="$(sha256sum "$FILE" | awk '{print $1}')"
  else
    die "need shasum or sha256sum on PATH"
  fi
fi

case "$SHA" in
  -) ;;
  *[!0-9a-f]*) die "sha256 must be lowercase hex: '$SHA'" ;;
  ????????????????????????????????????????????????????????????????) ;;
  *) die "sha256 must be 64 hex characters: '$SHA'" ;;
esac

TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
mkdir -p "$STATE_DIR"
printf '{"schema":1,"file":"%s","sha256":"%s","ts":"%s"}\n' "$BASE" "$SHA" "$TS" >> "$STATE_DIR/own-writes.jsonl"
echo "recorded $BASE $SHA"
