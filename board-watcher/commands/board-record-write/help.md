# gc board-record-write

Record an own coordination-board write in the board-watcher hash ledger,
so the watcher settles your own change silently instead of nudging your
own mayor about it.

## Usage

```
gc board-record-write <file> [--state-dir <dir>] [--sha256 <hex>]
gc board-record-write <name> --deleted [--state-dir <dir>]
```

## Arguments

- `<file>` — the board doc just written. Its **basename** keys the ledger
  entry; its content is hashed with sha256 unless `--sha256` supplies the
  hash directly (e.g. when the local copy has already been removed).
- `--deleted` — record a deletion tombstone for `<name>` instead of a
  content hash, so an own removal doesn't nudge either. The file need not
  exist.
- `--state-dir <dir>` — override the ledger directory (default
  `$GC_CITY_PATH/.gc/board-watcher`, or `$BOARD_WATCHER_STATE_DIR`).
- `--sha256 <hex>` — record this hash instead of hashing `<file>`.

## When to run it

Immediately after your write lands on the board — for a local board, right
after writing the file; for a remote board, right after the scp/rsync
succeeds (hash the local source copy). The watcher's debounce window
(default 60s) is the grace period between the write landing and the
suppression check.

The ledger is `own-writes.jsonl` in the state dir, one JSON object per
line; the shape is the interop contract in
`schema/own_writes.schema.json`, so custom write paths (a board-post
helper, a CI step) may append entries themselves instead of calling this
command.
