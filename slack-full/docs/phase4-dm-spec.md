# Phase 4 — Per-Agent DMs: Implementation Spec (v2)

Status: revised after four-lens adversarial review (19 confirmed
findings folded in; review archive: wf_8eac616e-b18). Normative parent:
`company-rooms.md` (§Per-Agent DMs, acceptance rule 12) and
`company-rooms-implementation-plan.md` (§Phase 4). Where this spec and
the parent conflict, the parent wins and this spec must be corrected.

## Goals

A human workspace member DMs an agent's app; the agent's singleton
DM-bound session wakes with the message, replies through
`gc slack reply-current`, and the reply appears in the DM as the agent's
own identity. Bot-authored DMs deliver nothing. All of rule 12 holds:
replays absorbed, cross-app signature spoofing rejected, exactly the
DM-bound session woken.

## Resolved design decisions

### D-DM1: DM session binding is pack-local, not extmsg-fabric

`dm_bindings.json` in the company state dir, sibling of
`company_bindings.json`, `schema_version: 1`:

```json
{"schema_version": 1,
 "dm_bindings": [{"agent": "ollie", "session": "ollie", "city": ""}]}
```

Exactly one binding per agent (singleton); `city` empty or absent means
the adapter's own city, mirroring room bindings. Imported and inspected
through the same `slack_company_directory.py` city-scoped
import/inspect path that owns `company_bindings.json`, extended with a
dm-bindings input (room bindings have no separate "import flow" —
dm_bindings ride the identical read-validate-write mechanism and
`(session, city)` guard).

Rationale (corrected per review): the fabric's
`ConversationRef{Kind: dm}` remains the right long-term home. What
blocks adoption NOW is not the DedupKey gate itself (that gates the
inbound *delivery* migration) but the split it would create: fabric-owned
binding state with pack-owned wake delivery over
`session/{id}/messages` gives two systems with different restart and
ownership semantics for one conversation, and the fabric's Phase-1
single-writer rule means the pack could not reconcile that split
unilaterally. Phase 5 migrates bindings AND delivery to the
fabric/ledger together, when DedupKey consumption is real. Revisit
marker: `PHASE5-DM-FABRIC` at the binding-registry seam.

### D-DM2: Allowed-human policy = workspace humans, optional allowlist

A DM author is allowed when the author classifier (shared with rooms)
returns `human` and the event's `team_id` matches the bound workspace.
Allowlist semantics (fail-closed, corrected per review): if the
directory document carries a top-level `dm_allowed_humans` key AT ALL —
including an empty array — the allowlist governs: only listed Slack
user IDs are allowed, and an empty list allows nobody. If the key is
absent, all workspace humans are allowed. Denied authors' events are
admitted and terminally resolved `dm_author_not_allowed` (visible in
receipts; policy denials must not be silent drops). Directory-wide,
not per-agent, in v0.

## Wire and manifest changes

`manifest/agent-app.json` template delta (single update, one reinstall):

- `oauth_config.scopes.bot`: add `im:history` AND `reactions:write`
  (the DM ack actor is the agent app — see Acks; rooms' ack actor is
  the switchboard, which already has it).
- `settings.event_subscriptions`: `request_url` (same public funnel
  endpoint as the switchboard) + `bot_events: ["message.im"]`.
- Messages tab settings already correct from Phase 2a.

Scope additions require per-app reinstall. **Delayed Events must be
enabled on every agent app** (18 apps + explicitly in the pilot step) —
the degraded-mode recovery below depends on Slack's redelivery horizon
exactly as rooms do; without it the 503-redrive model silently loses
DMs after the ~immediate/+1m/+5m retry ladder. Switchboard manifest:
unchanged.

## App registry and app-bound signature verification

### Registry: new Phase 4 work, not reuse

The existing `import-app` flow and `apps.json` records have NO
`agent`, `bot_user_id`, or `signing_secret` fields, and import-app's
manifest validation would reject agent-app manifests — the v1 spec was
wrong to claim otherwise. Phase 4 adds:

- A new CLI verb `gc slack register-agent-app` (both CLI ports) writing
  records `{team_id, api_app_id, signing_secret}` into a NEW
  `agent_apps.json` registry (schema_version'd, 0600, loaded at startup
  and on SIGHUP like the other read-only registries). It does not touch
  the legacy `apps.json` or import-app's validation profile.
- Owner-agent identity is NOT stored there: it derives by joining the
  envelope's `api_app_id` against `company_directory.json`
  `agents[].app_id` (the directory already binds
  name↔app_id↔bot_user_id and is the canonical identity source). A
  registered secret whose api_app_id has no directory agent is a
  startup/reload warning and admits nothing.
- The switchboard's own identity needs no record: the adapter already
  has `SLACK_APP_ID` and `SLACK_SIGNING_SECRET` in env; event_callbacks
  claiming `api_app_id == SLACK_APP_ID` verify against the env secret
  (the true "existing path, unchanged").

### Verification order (event POSTs)

1. `type == "url_verification"`: no `api_app_id` in the handshake;
   trial-HMAC across env secret + all registered agent secrets; echo
   challenge on any match. Side-effect-free, so trial is acceptable
   here and ONLY here.
2. `type == "event_callback"`, `api_app_id == SLACK_APP_ID`: verify
   against env secret only (rooms path, unchanged).
3. `type == "event_callback"`, `api_app_id` has a registered agent
   record: verify against exactly that record's secret. Mismatch →
   HTTP 401, counter `company_dm_sig_reject`. A signature that matches
   a DIFFERENT registered app's secret is still 401 (rule 12 spoof) —
   the bind check is authoritative, no fallback.
4. `type == "event_callback"`, unknown `api_app_id`: legacy trial-HMAC
   fallback (parent's carve-out for legacy single-app installs,
   restored per review) — but a trial match against a REGISTERED agent
   secret is rejected, not accepted: registration opts an app into
   strict binding permanently.

Timestamp-skew rules unchanged.

## Admission

DM events: `event_callback` with inner `channel_type == "im"` from a
registered agent app (verification rule 3).

Gate:
1. Signature bound to the delivering app (above), and the app joins to
   a directory agent.
2. `channel_type == "im"`; subtype allowlist IDENTICAL to rooms'
   `AdmissibleSubtype` set in `company_routing.go` (no separate DM
   table; the v1 "edit/delete shapes" language was wrong).
3. ALL admissible DM events are admitted — human AND bot-authored.
   In a 1:1 `im` the only possible bot author is the owner app itself
   (self-echo of its own outbound posts). Self-echoes admit with an
   immediately-terminal status `no_delivery / dm_self_echo`, routed to
   nobody. This keeps the receipt store the dedup memory AND the
   reconciliation source for DM outbound posts — the reconciler is
   receipt-scan-only by design (`_scan_receipt_for_nonce` never calls
   the Slack API), and rooms only work because bot echoes become
   receipts; the v1 no-receipt rule would have wedged every timed-out
   DM reply permanently (review blocker C2/C9).

Receipt key `(team_id, dm_channel_id, ts)`, same store, plus
`kind: "dm"` and `owner_app_id`. All Phase 1 admission mechanics apply
verbatim (O_EXCL claim-and-content, dedup-before-200,
503-without-`x-slack-no-retry`, redelivery absorption, company-scoped
startup barrier, 7-day retention).

Degraded mode: same 503 rule per app. Auto-disable budgets are per
agent app; runbook: an adapter outage now risks 19 subscriptions, the
re-enable step is per app, and messages sent during a disable window
were never admitted — humans re-send; there is NO history backfill on
re-enable in v0 (explicitly out of scope).

## Routing

For an admitted DM receipt:

- Owner agent = directory agent joined from `owner_app_id`. Exactly one
  admission owner per DM (the app only receives its own `message.im`).
- Self-echo → terminal `dm_self_echo` (above), nobody woken.
- Allowed human → single target `dm_bindings[agent]`, wake kind `dm`.
  No mention semantics, no ambient set, no delegation authority (the
  human-root gate refuses delegation verbs from a `dm`-kind root).
- Not-allowed author → terminal `dm_author_not_allowed`.
- Unbound agent (no dm_bindings row) → the ROOMS rule, not a park
  (v1's park diverged from precedent): a recorded FAILED target
  (`failed_dm_unbound`, definitive), recoverable via the existing
  `company-redrive` re-resolution machinery after a binding is
  imported. No retention exemption, no unbounded accumulation, and the
  operator drains it with the tool built for exactly this in Phase 3.
- Delivery reuses `TargetDelivery` verbatim (city-qualified resolution,
  idempotency keys, sweep, claims, generation counters).

## Session-existence guard (shared hardening, revised)

`SLACK_COMPANY_VERIFY_SESSIONS=1` (default off): before the first
delivery attempt per `(city, session)`, GET
`/v0/city/{city}/session/{session}`. Results are advisory, never
terminal (v1 terminalized — wrong: today's fleet showed sessions
404-then-materialize and aliases go 409-ambiguous transiently):

- 200 → proceed; cache positive for 10 minutes.
- 404/409 → leave the target PENDING with detail
  `session_missing`/`session_ambiguous`; do NOT post; the 60s sweep
  re-checks. Negative results are not cached. The existing
  per-target attempt cap bounds the loop; targets that exhaust it fail
  with the detail preserved and are company-redrive-recoverable.
- Guard-check network errors → proceed as if unchecked (the guard must
  never reduce availability below flag-off behavior).

Applies to room and DM deliveries equally.

## Owner-token custody in the Go gateway (new Phase 4 work)

DM hydration, DM acks, and the DM failure-reply need the OWNER AGENT's
bot token; the Go gateway today holds only the switchboard token,
wired at construction into `hydrate`/`reactHook`/`replyHook`. Phase 4
amends the Phase 2 ownership split: the Go adapter gains a per-agent
token loader over the company secrets dir (`bot-token-<agent>.txt`,
same refusals as the Python loader: file not 0600 → refuse, dir not
0700 → refuse, symlink → refuse), selected per receipt: room receipts
keep the switchboard token unconditionally; `kind: "dm"` receipts
resolve owner agent → token. A DM receipt whose owner token is missing
delivers with `context_unavailable` hydration and degraded acks
(counter `company_dm_token_missing`) rather than trying the
switchboard token, which must never be used on a DM channel. Config:
the secrets dir is the existing `SLACK_COMPANY_SECRETS_DIR` (shared
with the Python side).

## Hydration, pointer, reply

- Hydration: frozen-snapshot discipline as rooms, reading
  `conversations.history` on the DM channel with the owner token
  (`im:history`), same bounded window and `include_all_metadata`.
- Current-turn pointer: DM turns get their OWN pointer file in a
  dedicated subdirectory `company-current-turn/dm/<session>.json`
  (created 0700 on demand) — a shared singleton would let a room wake
  clobber a DM turn and misdirect a private reply into a room (review
  C6). A flat `<session>.dm.json` would still collide with the room
  pointer of a session literally named `<session>.dm` (the shared
  sanitizer passes interior dots through), so the DM pointer is
  namespaced under its own subdirectory rather than by suffix, keeping
  the DM and room namespaces disjoint for every session name. Pointer
  contract delta is explicit cross-language
  work: add `"dm"` to `_TURN_KINDS` and the Go/Python kind sets; `room`
  field empty and permitted-empty for kind `dm` (validator relaxation
  in both languages); new `owner_app_id` field; golden + interop
  fixtures for the DM pointer and `dm_bindings.json`.
- `gc slack reply-current`: resolves the pointer by kind — with both a
  room and a DM pointer live, the NEWEST by delivered-at wins, and
  explicit `--kind room|dm` overrides; origin-ts pinning is honored as
  today. Posting uses the owner token to the DM channel via the SAME
  durable-intent machinery (create intent → CAS attempts →
  Retry-After → receipt-scan reconciliation, which works because
  self-echoes are admitted). Root derivation unchanged (`thread_ts`
  else `ts`), but DM POSTING is flat: a reply whose root equals the
  turn's own `ts` (a top-level DM message) posts in-channel with no
  `thread_ts`; threading applies only when the human themselves
  replied inside a thread. (Rooms thread to protect channel
  timelines; a 1:1 DM has none, and a threaded reply hides behind a
  thread indicator the human easily misses — observed live on the
  first pilot DM.)
- Spoof guard: extended, not reused — the room guard is keyed against
  `company_bindings.json`; kind-`dm` replies validate through
  `dm_bindings` (the session must be the one the DM pointer's owner
  agent binds to), with the same dot/dunder session aliasing.
- Visible acks (same config flag): ack actor in a DM is the owner
  token (`reactions:write` added above). 👀 admission, ✅ delivered,
  ⚠️ terminal-with-failures; AckState cursor and sweep-healing
  unchanged. Missing owner token → acks silently degrade (counted).

## Observability

`/healthz` additions (v1 cited a surface it never defined):
`company_dm_receipts` (by status), `company_dm_sig_reject`,
`company_dm_token_missing`, `registered_agent_apps` (count +
directory-join warnings). Existing stale-intent and park counters
apply to DM receipts unchanged.

## Out of scope (unchanged deferrals)

Bot→bot DMs, DM-rooted delegation, `mpim` group DMs (not subscribed;
`channel_type` gate), token rotation automation, history backfill
after auto-disable, extmsg fabric adoption (`PHASE5-DM-FABRIC`).

## Config summary

| Item | Meaning |
| --- | --- |
| `dm_bindings.json` | agent → singleton (city, session); schema v1 |
| `agent_apps.json` + `register-agent-app` | (team_id, api_app_id, signing_secret) |
| `dm_allowed_humans` (directory, optional) | present ⇒ allowlist mode (empty = nobody) |
| `SLACK_COMPANY_VERIFY_SESSIONS` | 0/1 advisory session-existence guard |
| `SLACK_COMPANY_SECRETS_DIR` | now also read by the Go adapter (DM tokens) |

## Test plan (maps to acceptance rule 12)

1. App-bound HMAC: event claiming app A signed with app B's secret →
   401, no receipt; unknown api_app_id + valid legacy secret → legacy
   trial path admits (carve-out); unknown api_app_id + registered
   agent secret → 401 (registration is strict).
2. `url_verification`: any registered secret → challenge; none → 401.
3. Human DM admitted once; redelivery (`x-slack-retry-num` ≥ 1) → 200,
   no second receipt.
4. Self-echo DM → admitted, terminal `dm_self_echo`, nobody woken; a
   stuck posting intent reconciles against that receipt's nonce.
5. `dm_allowed_humans` present+empty → every human terminal
   `dm_author_not_allowed`; absent → workspace humans allowed.
6. Unbound agent → `failed_dm_unbound` target; import binding +
   company-redrive → delivered.
7. DM pointer written to `company-current-turn/dm/<session>.json`;
   concurrent room turn (including one for a session named `<x>.dm`)
   does not clobber it; reply-current picks newest, `--kind` overrides;
   owner-token post; unbound session reply → spoof-guard refusal.
8. Delegation verb from a dm-kind root → refused.
9. Guard on: 404 session → target stays pending, sweep retries,
   attempt-cap exhaustion preserves detail; guard network error →
   delivery proceeds.
10. DM receipt-store write failure → 503 without `x-slack-no-retry`;
    retry admits once.
11. Missing owner token → delivery with `context_unavailable`,
    degraded acks, counter increments; switchboard token never used
    (assert no API call with it on the DM channel).
12. Golden/interop fixtures: `dm_bindings.json`, DM pointer, captured
    real `message.im` (wire-shape pin: envelope `api_app_id` presence,
    exact `channel_type`, self-echo shape with metadata).

## Operator runbook (delta, ordered)

0. Deploy the Phase 4 adapter + CLI build FIRST (register-agent-app
   and the verification order ship together; on manual installs the
   switchboard path needs `SLACK_APP_ID` already in env — it is).
1. Per agent app (pilot: OLLIE only, then the other 17): harvest the
   signing secret from Basic Info → `gc slack register-agent-app` →
   confirm `/healthz` `registered_agent_apps` increments with no
   directory-join warning.
2. Update the app manifest: `im:history` + `reactions:write` +
   `message.im` event subscription with the shared funnel Request URL.
   The handshake fires on save and verifies via the trial path —
   registration MUST precede the Request URL, else 401.
3. Reinstall the app to the workspace (scope change). Reinstall does
   not rotate the signing secret; the bot token rotates only if
   explicitly reissued — if it was, re-drop `bot-token-<agent>.txt`.
4. Enable Delayed Events on the app.
5. Import `dm_bindings.json` (start: ollie → its session), restart or
   SIGHUP the adapter, DM the app as a human: 👀→reply→✅ in the DM.
6. Replay + spoof fixtures against the live endpoint before rolling
   the fleet.
