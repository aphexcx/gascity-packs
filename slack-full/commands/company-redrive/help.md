# gc slack company-redrive

Operator redrive of failed or parked company-room receipt deliveries,
without reposting to Slack. The adapter resets the selected targets and
re-triggers delivery **reusing the same recorded idempotency keys**, so a
redrive is safe with respect to gc's session-message dedup.

Two legs, chosen by the receipt on the adapter side:

- Receipts **with frozen targets** — the selected targets (default: every
  `failed` target; `--target` filters) are reset to `pending`
  (`attempts: 0`, detail `operator_redrive`), the receipt returns to
  `routing`, recovery backoff is cleared, and delivery re-triggers.
- Receipts **with no recorded targets** (a `correlation_recovery_exhausted`
  or parked receipt — this verb is the designated recovery path) — the
  receipt resets to `received`, its reason and recovery fields clear, and
  first-routing re-runs from correlation.

## Usage

```bash
gc slack company-redrive (--receipt <id> | --origin <team>:<channel>:<ts>) \
                         [--target <session>]... [--include-failed]
```

## Flags

- `--receipt <id>` — the receipt to redrive. Mutually exclusive with
  `--origin`.
- `--origin <team>:<channel>:<ts>` — select the receipt by message origin.
- `--target <session>` — restrict to this target session; repeatable.
  Default is every `failed` target.
- `--include-failed` — **required** to touch a target whose detail begins
  `attempts_exhausted`.

## `attempts_exhausted` caveat

An `attempts_exhausted` target's earlier timeouts had **unknown**
outcomes, and gc's idempotency cache is best-effort (in-memory, bounded
TTL) — the message may already have been delivered. Check the target
session transcript before `--include-failed`; the reused idempotency key
suppresses a *cached* duplicate but is not a durable exactly-once
guarantee until the Phase 5 ledger lands.

## Errors

- `404` — the receipt is terminal and already swept past the 7-day
  retention horizon; there is nothing left to redrive.
- `409` — the receipt's single-flight is held elsewhere (a concurrent
  delivery or redrive); the verb retries a few times, then asks you to
  re-run.

## Connection

The adapter's internal listener is reached over `GC_SERVICE_SOCKET`
(a Unix domain socket, gc proxy_process mode) when set, else TCP to
`LISTEN_INTERNAL` (default `127.0.0.1:8766`). `GC_SERVICE_SOCKET` wins.

Routes to: scripts/slack_company_admin.py company-redrive
