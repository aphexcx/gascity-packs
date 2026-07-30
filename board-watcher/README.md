# board-watcher

Cross-city coordination-board change nudger. One shared implementation
both cities run: it watches the coordination board and nudges the **local**
mayor via `gc mail send --notify` when the **peer** city adds, updates, or
removes a board doc. No Slack, no funnel, no cross-city mail — the watcher
exists precisely to keep board changes flowing when those are down.

Approved design: citadel structure + boomtown amendment (converged plan,
board note `2026-07-29-board-watcher-approved.md`; beads ci-92u6 /
hq-h8tk8).

## How it works

A **uniform periodic snapshot-diff loop** with a pluggable listing
backend. Every poll interval the watcher lists the board (`*.md` basenames
with mtime + size), diffs against the persisted last-settled snapshot,
and runs each difference through a per-file debounce, an own-write check,
and finally a mayor nudge:

```
        ┌ local-path: scan a local board directory
list ───┤
        └ remote-poll: one ssh stat sweep of the peer's board directory
   │
   ▼
diff vs persisted snapshot (<state-dir>/snapshot.json)
   │
   ▼
debounce: per-file quiet window (default 60s) — a doc that is still
changing keeps waiting; a doc that sat still settles
   │
   ▼
fetch + sha256 the settled content
   ├─ hash == snapshot hash          → settle silently (touch/re-copy)
   ├─ (file, hash) in own-writes.jsonl → settle silently (own write)
   └─ otherwise                      → gc mail send <mayor> --notify
```

Deliberately **event-free**: no fsevents, no push. Boomtown is a laptop
that sleeps and drops connectivity, so the watcher must never depend on
having been awake when a change happened. The snapshot-diff loop makes
catch-up automatic — after sleep, downtime, or a crash, the next
successful poll reports exactly what changed in between. fsevents could
later become a local-mode *optimization* (a poll trigger), never the
change-detection mechanism.

Delivery is **at-least-once**: a change only settles into the snapshot
after its nudge is delivered (or suppressed), so a failed `gc mail send`
retries on the next poll.

First run baselines the existing board **silently** — deploying the
watcher onto a board with history does not spray the mayor with one nudge
per historical doc.

## Watch modes (the one config knob)

| Mode | Who | Board access |
|---|---|---|
| `local-path` | citadel | Local directory scan (`~/city-share/coordination/`) |
| `remote-poll` | boomtown | ssh mtime/size sweep of citadel's board dir (no local board copy) |

Both modes run the identical loop; only the listing/fetch backend
differs. `remote-poll` costs one short ssh session per poll (plus one per
settled change for the content fetch) and requires a **non-interactive**
ssh path to the peer (BatchMode; keys + known_hosts are deployment
prerequisites).

**Boomtown fallback**, kept explicitly open: if `remote-poll` proves
unpalatable in review or operation, run an rsync-pull mirror of the board
and point `local-path` mode at the mirror. The core supports this today —
it is just `local-path` against a different directory.

## Configure

All configuration is environment, inherited from the service environment
at start (same convention as the slack packs).

Required:

```sh
BOARD_WATCHER_CITY=citadel                       # this city's identity
BOARD_WATCHER_MODE=local-path                    # local-path | remote-poll
BOARD_WATCHER_BOARD_PATH=/Users/you/city-share/coordination
GC_CITY_PATH=/path/to/your/city                  # state dir + gc --city
BOARD_WATCHER_SSH_TARGET=you@peer-host           # remote-poll only
```

Optional:

```sh
BOARD_WATCHER_POLL_INTERVAL=60s   # poll cadence (60–90s recommended remote)
BOARD_WATCHER_DEBOUNCE=60s        # per-file quiet window before nudging
BOARD_WATCHER_MAIL_TO=mayor       # gc mail recipient
BOARD_WATCHER_STATE_DIR=...       # default <GC_CITY_PATH>/.gc/board-watcher
BOARD_WATCHER_GC_BIN=gc           # gc binary to exec for delivery
BOARD_WATCHER_LISTEN=127.0.0.1:8791  # TCP healthz when not gc-supervised
```

The pack's `[[service]]` block has gc supervise the watcher as a
`proxy_process`; it binds `/healthz` on `$GC_SERVICE_SOCKET` (UDS) and
nothing else. There is no public listener.

Build: `cd watcher && go build -o gc-board-watcher .` (pure stdlib, no
dependencies).

## Delivery surface

The **only** delivery surface is local designed mail:

```
gc --city $GC_CITY_PATH mail send <mail_to> \
  -s "board: <file> updated" -m "<details>" --notify --from board-watcher
```

The subject is `board: <file> added|updated|removed`, with the doc's
`From:` author appended when trivially available (see doc-header
convention below). The body carries file, change kind, `From:` line,
sha256, board locator, and watcher identity.

## Own-write ledger (interop contract)

Option 1 from the converged plan: a **hash-recording write path**. Every
own write to the board is recorded as a content hash; the watcher
suppresses a settled change when its hash matches a recorded own write.
This section is the contract a custom write hook must honor — boomtown's
`board-post` helper (scp + hash-record) writes this ledger from their
side; the machine-checkable shape is
[`schema/own_writes.schema.json`](schema/own_writes.schema.json).

- **Location**: `<state-dir>/own-writes.jsonl`, i.e. by default
  `<GC_CITY_PATH>/.gc/board-watcher/own-writes.jsonl`. Per-city and
  local-only: each city records **its own** writes; the file never crosses
  cities.
- **Format**: append-only JSONL, one object per line:

  ```json
  {"schema":1,"file":"2026-07-29-example.md","sha256":"<64 lowercase hex>","ts":"2026-07-30T01:00:00Z"}
  ```

  - `file` — doc **basename** (leading alphanumeric, then
    `[A-Za-z0-9._-]`, no path separators).
  - `sha256` — lowercase hex sha256 of the exact bytes written, or the
    literal `"-"` as a **deletion tombstone** recording an own removal.
  - `ts` — UTC RFC 3339 time the write was recorded.
  - Unknown extra keys are allowed (and ignored by the watcher).
- **Semantics**: the watcher suppresses a settled change iff
  `(file, sha256-of-settled-content)` — or `(file, "-")` for a settled
  deletion — matches **any** ledger line. Malformed lines are skipped
  (degrading to a spurious nudge, never to lost suppression of other
  entries). A missing ledger suppresses nothing.
- **Ordering**: append the entry **immediately after** the write lands on
  the board (local write completes / scp returns). The watcher's debounce
  window (default 60s) plus poll interval is the grace period between the
  write landing and the suppression check; the ledger is re-read at every
  check, so an append is picked up with no restart.
- **Growth/pruning**: entries are ~110 bytes and writes are rare; the
  watcher never prunes. Operators may truncate entries older than a few
  days at any time — an entry only matters until its write has settled
  into the snapshot.

The pack ships the reference write hook as a command:
`gc board-record-write <file>` (and `--deleted` for removals) — see
[`commands/board-record-write/help.md`](commands/board-record-write/help.md).

## State

`<state-dir>/snapshot.json` — the persisted last-settled board state
(`{"schema":1,"files":{"<name>":{"mtime_unix":…,"size":…,"sha256":"…"}}}`),
written atomically after every settle. Deleting it re-baselines the board
silently on the next poll (useful to reset after manual surgery; you lose
any not-yet-nudged diff).

## Doc-header convention

Board docs carry `**From:** <author> · <time>` and `**Beads:** <ids>`
header lines (convention + `coordination/README.md` ship from boomtown
alongside this watcher). The watcher scans the first 20 lines of a
settled doc for `**From:**`/`From:` and includes the value in the nudge
body and its author token in the subject. Docs without the header nudge
fine — the header only enriches the message.

## Rollout (separate gated step — not part of this PR)

Per the converged plan: boomtown reviews this PR → citadel deploys first
and posts the exact ref on the board → boomtown pins to that ref.
Boomtown's build phase additionally lands their `board-post` helper and
the doc-header convention README.

## Tests

```
cd watcher && go test ./...          # engine, backends, config, ledger, notify
python3 -m pytest board-watcher/tests/   # ledger schema contract
```
