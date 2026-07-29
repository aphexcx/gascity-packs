# gc slack company-status

One view of a company-room flow end to end. Joins **Python-owned reads**
— delegation records grouped by their synthesis group (5-tuple:
team, channel, thread root, requester bot user, requester session), each
with its S10-normalized synthesis snapshot, plus stale `posting`
delegation intents (age past their retry deadline) — with the adapter's
**receipt state** read over the internal listener
(`GET /internal/company/receipts`): parked receipts, per-target delivery
state, recovery/backoff fields, and ack state.

This verb is read-only. The receipt store has a single writer (the
adapter); `company-status` never mutates it.

## Usage

```bash
gc slack company-status [--receipt <id>] \
                        [--origin <team>:<channel>:<ts>] \
                        [--root <team>:<channel>:<root_ts>] \
                        [--status <status>]
```

## Flags

- `--receipt <id>` — restrict the receipt listing to one receipt id.
- `--origin <team>:<channel>:<ts>` — scope to one message origin.
- `--root <team>:<channel>:<root_ts>` — scope to one human root thread
  (groups, stale intents, and receipts sharing that root).
- `--status <status>` — filter receipts by status.

## Output

A JSON document with `groups` (each carrying its delegations and their
normalized `synthesis` snapshots), `ungrouped_delegations`,
`stale_posting_intents`, and `receipts`. A not-ready synthesis snapshot
alongside a `correlation_pending` / `ambiguous_pending_delegations`
receipt park is exactly the wedged-flow shape this verb surfaces.

## Connection

The adapter's internal listener is reached over `GC_SERVICE_SOCKET`
(a Unix domain socket, gc proxy_process mode) when set, else TCP to
`LISTEN_INTERNAL` (default `127.0.0.1:8766`). `GC_SERVICE_SOCKET` wins.

Routes to: scripts/slack_company_admin.py company-status
