# Changelog

## 0.1.0 — 2026-07-30

Initial release of the shared coordination-board change watcher
(converged citadel/boomtown design; beads ci-92u6 / hq-h8tk8, built under
gp-gn5).

- Snapshot-diff poll loop with pluggable listing backend:
  `local-path` (local directory scan) and `remote-poll` (ssh mtime/size
  sweep, for a city with no local board copy). Persisted snapshot makes
  catch-up after sleep/downtime automatic; first run baselines silently.
- Per-file debounce (default 60s quiet window, configurable).
- Own-write filtering via the hash-recording write path: append-only
  `own-writes.jsonl` ledger, documented as an interop contract
  (schema/own_writes.schema.json) with deletion tombstones; reference
  write hook shipped as `gc board-record-write`.
- Touch/re-copy suppression: a settled change whose content hash matches
  the snapshot settles silently.
- Delivery via local `gc mail send <mayor> --notify` only (at-least-once:
  failed sends retry next poll). Subject `board: <file>
  added|updated|removed`, enriched with the doc's `From:` header when
  present.
- gc-supervised `proxy_process` service with UDS `/healthz`.
