# Phase 5 — Ledger Integration: Readiness Assessment and Scoped Increment

Status: ACTIVE. Written 2026-07-19 against the uncommitted
durable-request-ledger v0 drafts (`engdocs/proposals/` in the core
checkout at `/data/projects/gascity-durable-request-ledger`, branch
`design/durable-request-ledger-v0`) and their Slack companion docs.
Full contract digest archived with the migration artifacts
(ledger-v0-contract-digest, 2026-07-19). Both drafts are `Proposed`;
their Slice 0 reserves re-pinning of state and error vocabularies.

## Gate ledger — what blocks what

| Gate | State (2026-07-19) | Blocks |
| --- | --- | --- |
| Ledger v0 implementation | Design drafts only; zero code | Any live integration |
| Slice 0 vocabulary pin | Not run | Pinning state/error names in code |
| Slice 20 OpenAPI | Not generated | Writing ANY request/response DTOs — field spellings do not exist yet; generate the client from that artifact, never hand-write |
| Beads create-only + CAS (upstream #4682) | Not landed | End-to-end activation |
| Core multi-target delivery rework (Slack plan Task 1.2) | Unimplemented | Room (multi-wake) activation; the delivery read model will move |
| extmsg `DedupKey` consumer in core | Still a no-op (types.go only) | Migrating wakes to `extmsg/inbound` (pre-existing Phase 5 gate, unchanged) |
| Slack projector wake mechanism | UNSPECIFIED (UDS callback contract is Discord/Cherry-specific) | Outbound projector client design |

Consequence: this phase ships NO wire client and NO mock server yet.
The one implementable increment is the body-store split (below), which
every draft revision pins invariantly.

## Conformance: our shipped pack vs the Slack companion's adapter obligations

The companion is a core-side thin-transport redesign, not a port of
this pack — but its adapter-side obligations are largely what Phases
1–4 already built:

| Companion obligation | Our pack | Verdict |
| --- | --- | --- |
| 2xx to Slack only after durable spool row | Receipt O_EXCL claim-and-content before HTTP 200 | CONFORMANT |
| One shared city-owned gateway; no per-app forwarders | Single adapter; per-agent apps are identity+DM-subscription only | CONFORMANT |
| Personas subscribe `message.im` only; switchboard `message.channels`/`groups` | Exactly our Phase 2a/4 manifest split | CONFORMANT |
| Native mentions exclusive, suppress ambient | Phase 1 routing table | CONFORMANT |
| DM target fixed by verified `api_app_id`; in-DM mentions cannot fan out | Phase 4 app-bound HMAC + owner-app routing | CONFORMANT |
| Durable outbound intent before every non-idempotent post | Phase 2 intents/outbox, receipt-scan reconciliation | CONFORMANT |
| `event_id` must not be receipt origin | Our origin is `(team_id, channel_id, ts)` | CONFORMANT |
| Transport-event dedup key `(workspace_id, api_app_id, event_id)` | We dedup by origin key only; no event_id inbox row | DIVERGENT (minor): adopt at client build time; our origin dedup subsumes redelivery in practice |
| Receipts never carry raw bodies | Receipts EMBED the inner event today | GAP → closed by this increment |
| Raw payload retained only until `core_bound`, then redacted | No redaction fence exists | PREPARED by this increment (fence hook; activation with the ledger) |
| OpenBao-only credential access | File custody (pilot posture); vault convergence in progress with infra | DEFERRED (org-manifest phase, unchanged) |

## Corrected state mapping (supersedes the plan's three-state note)

The plan's `received→spooled`, `routing→admitting`, `terminal→terminal`
is too coarse and names a state (`terminal`) the drafts do not have.
The adapter journal must carry at least the Slack companion's six:
`spooled`, `admitting`, `core_bound`, `body_redacted`, `rejected`,
`quarantined` — with `core_bound` as the load-bearing fence: only
after the qualified core receipt reference + digest are durable may
the raw payload be redacted. Local mapping at client build time:

| Local (Phases 1–4) | Ledger-mode journal |
| --- | --- |
| receipt created (pre-route) | `spooled` |
| routing | `admitting` |
| delivered/no_delivery/failed + core ref durable | `core_bound` → `body_redacted` |
| signature/policy rejection | `rejected` |
| same-key different-digest replay | `quarantined` |

## Increment shipped this phase: body-store split

Motivation (invariant across every draft revision): receipts must not
carry raw bodies; the payload lives beside the spool and is
redactable independently. Also standalone value: smaller receipts,
cheaper scans, a single redaction point for retention.

- New `bodies/` sidecar under the ingress store: one file per receipt
  (`<receipt_id>.body.json`, 0600), holding the raw inner event
  exactly as previously embedded. Created atomically BEFORE the
  receipt in the admission sequence. A body without a receipt is
  garbage collected by the janitor — but ONLY on affirmative receipt
  absence (the receipt path and any quarantine sibling are lstat'd
  before removal), never for a receipt merely unreadable this pass,
  and never within a grace window (an in-flight cross-process
  admission must be able to finish linking its receipt). Orphan
  adoption is digest-checked: a divergent crash-orphan body (no
  receipt claims it yet) is replaced with the admitted bytes rather
  than birthing a permanent digest mismatch; first-writer-wins holds
  whenever a valid receipt already owns the body. A receipt whose
  body is missing or digest-mismatched is a recoverable integrity
  error: delivery PARKS it (non-terminal, sweep-retried, counted on
  /healthz) so an orphan-adoption repair or operator action can
  restore it — it is never terminalized under a routing reason. A
  corrupt receipt quarantined during a scan takes its intact body
  aside with it (`<id>.body-<token>.corrupt`), preserving the payload
  for forensics (the split-store analogue of the pre-split embedded
  copy).
- Receipt `event` field becomes a reference: `body_ref` (receipt id)
  + the immutable `event_digest` (sha256 of the stored bytes) for
  integrity. A `schema_version` bump on receipts; the reader accepts
  BOTH shapes forever (old embedded receipts remain valid until
  retention ages them out — no migration rewrite of live receipts).
- All readers go through one accessor per language (Go
  `receiptBody(rec)`, Python `receipt_body(rec)`): routing snapshot
  building, hydration reminder rendering, reconciliation
  (`_scan_receipt_for_nonce` — reads text/metadata through the
  accessor), admin/redrive display. The accessor is the ONLY place the
  body digest is verified; the periodic sweep classifies bodies by
  existence and a bounded tombstone probe only (no per-body SHA under
  the store mutex the admission hot path contends for), and a
  read-path counter (`company_body_digest_mismatch`) keeps digest
  integrity observable.
- Every root-keyed derivation (the dgser serialization lock name, the
  replay-chain root, the rendered `thread_root_ts`, the failure-reply
  root) reads a `thread_root_ts` frozen onto the receipt at admission,
  so a redacted or lost body can no longer collapse a threaded
  receipt's root to its origin ts and diverge a redrive's lock name or
  rendered root from the pre-redaction value. Legacy receipts fall
  back to body-derivation.
- Redaction hook: `redactReceiptBody(receiptID)` truncates the body
  file to a fixed tombstone `{"redacted": true, "event_digest": ...}`
  — NOT wired to any trigger in this phase (the trigger is the
  future `core_bound` fence); exposed as an operator admin verb for
  manual use only. It refuses (409) a non-terminal receipt and a
  receipt younger than the outbound reconciliation horizon
  (`INTENT_TTL_SECONDS`, so a stuck posting intent can still reconcile
  against the body nonce), re-verifies the receipt still exists under
  the store mutex (a swept receipt → 404, never a resurrected
  tombstone), and redacted receipts stay visible in root-filtered
  admin listings with a `body_state` marker rather than silently
  dropping out.
- Retention: body files delete with their receipt (janitor pairs
  them); the digest survives in the receipt for late-redelivery
  dedup semantics exactly as today.

Out of scope (explicitly): wire client, mock server, DTOs, projector
client, extmsg-inbound migration, event_id inbox rows, OpenBao
credential resolution.

## Asks for the ledger owners (Slice 0 inputs from the Slack adapter)

1. Specify the Slack projector wake mechanism (same
   `gc.request-ledger.projector-callback.v1` UDS contract, or an
   HTTP/funnel variant) — riskiest unspecified seam for us.
2. Expose per-target delivery state to adapters (the
   `request-delivery` records are HTTP-invisible; multi-target rooms
   need at least a read model for redrive tooling parity).
3. Reconcile the companion's `triage=not_applicable` and `WakeReason`
   vocabulary with the neutral design's axes before Slice 0 pins.
4. Confirm no If-Match protocol is intended (ETag response-only) so
   our client design does not assume preconditions.
5. Publish the Slice 20 OpenAPI artifact to a stable path we can
   vendor as the DTO source.
6. Our shipped pack already satisfies the companion's adapter
   obligations except the two rows marked above — the companion can
   cite this pack as its reference adapter rather than respecify.
