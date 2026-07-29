# gc slack company-redact

Operator redaction of a single company-room receipt's raw stored body. The
adapter truncates the receipt's body sidecar
(`bodies/<receipt_id>.body.json`) to a fixed tombstone
(`{"redacted": true, "event_digest": ...}`) **atomically**, in place. The
receipt itself — its status, targets, and the immutable `event_digest` —
is untouched, so late-redelivery dedup semantics are unchanged.

This is a **manual operator hook only**. It is not wired to any automatic
trigger in this phase; the eventual automatic trigger is the ledger's
`core_bound` fence (raw payload retained only until the qualified core
receipt reference is durable, then redacted).

Redaction is fenced to receipts that are safe to strip:

- **Terminal only.** A non-terminal (`received`/`routing`) receipt is
  refused (`409`). Delivery re-reads the body on every attempt, so
  truncating a still-in-flight receipt would recompute routing from an
  empty message (terminalizing it under a misleading reason) or re-render a
  redrive with empty text under the same `Idempotency-Key`.
- **Past the reconciliation horizon.** A receipt younger than the outbound
  reconciliation window (the `INTENT_TTL_SECONDS` horizon, 24h) is refused
  (`409`): a stuck `posting` intent may still reconcile against this
  receipt's body nonce, and erasing it early would wedge that intent.

After redaction, readers degrade gracefully: hydration reads the body as
`context_unavailable` and reconciliation treats it as a no-match. A redrive
of a redacted receipt still runs — its route, idempotency keys, and frozen
`thread_root_ts` are on the receipt, not the body — but it **re-renders the
reminder WITHOUT the original message text** (the body is gone). Redact only
once the raw text is no longer needed for delivery.

## Usage

```bash
gc slack company-redact (--receipt <id> | --origin <team>:<channel>:<ts>)
```

## Flags

- `--receipt <id>` — the receipt whose body to redact. Mutually exclusive
  with `--origin`.
- `--origin <team>:<channel>:<ts>` — select the receipt by message origin.

## Errors

- `404` — the receipt is terminal and already swept past the 7-day
  retention horizon (including a sweep that raced this call); there is
  nothing left to redact.
- `409` — one of: the receipt's single-flight is held elsewhere (a
  concurrent delivery or redrive; the verb retries a few times); the
  receipt is a legacy embedded receipt with no separable body; the receipt
  is not terminal (redaction is fenced to `core_bound`-equivalent terminal
  state); or the receipt is younger than the reconciliation horizon. Only
  the held-single-flight case is retried — the others are surfaced with the
  endpoint's machine-readable reason and stop.

## Connection

The adapter's internal listener is reached over `GC_SERVICE_SOCKET`
(a Unix domain socket, gc proxy_process mode) when set, else TCP to
`LISTEN_INTERNAL` (default `127.0.0.1:8766`). `GC_SERVICE_SOCKET` wins.

Routes to: scripts/slack_company_admin.py company-redact
