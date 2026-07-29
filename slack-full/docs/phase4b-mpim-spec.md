# Phase 4b — Group DMs (mpim): Implementation Spec (v2)

Status: revised after adversarial review (18 confirmed findings, 5
blockers; archive wf_7c0b3105-2ea), then clarified in the Phase 4b
pre-commit review (provenance freeze + human-only downgrade + root-leg
downgrade, missing-token probe block, owner-join-loss degrades ack only,
vanished-agent pointer skip). Extends Phase 4
(`phase4-dm-spec.md`); rule-12 analogues apply. Motivating
requirement (2026-07-19): group DMs with agents woke nobody.

DIVERGENCE NOTE for the ledger owners: the core Slack companion pins
personas to `message.im` only; `message.mpim` is a deliberate
pack-side extension (Slice-0 ask #7). SECURITY DELTA acknowledged: in
1:1 DMs one compromised app secret exposes only that app's DMs; with
mpim any registered agent secret can forge events for group channels
— mitigated by the membership probe below.

## Semantics

- Wake policy: MENTION-ONLY. A human message natively mentioning
  member agents wakes exactly the mentioned, homed agents (mention
  extraction identical to rooms). Unmentioned human messages →
  terminal `no_delivery / mpim_no_mention`. Bot-authored → terminal
  `no_delivery / mpim_bot_author` (reconciliation memory). No
  ambient set exists. No delegation from mpim roots.
- The directory is NOT extended: no mpim rosters; dm_bindings is the
  only wake registry.
- `dm_allowed_humans` (when present): a non-allowed ROOT author →
  terminal `dm_author_not_allowed` exactly as dm. Additionally,
  because group hydration can excerpt non-allowed members' text, the
  reminder's provenance line downgrades to `human_root_unlisted`
  (never "verified") whenever any unlisted HUMAN author appears in the
  frozen hydration — either the verified thread-root author or any
  excerpt line. Only humans participate: an excerpt line authored by a
  directory agent (its `bot_user_id`) or a classic bot (empty user id)
  is NOT an "unlisted human" and never downgrades, so an agent's own
  reply in the recent window cannot poison the signal. The verdict is
  computed ONCE when the hydration snapshot freezes and persisted in
  the blob, so every retry renders byte-identical reminder bytes (the
  frozen-hydration discipline — never recomputed from the live
  directory). Agents are instructed to treat unlisted-author content
  as untrusted context. Data-at-rest note: mpim admission makes
  human-to-human group text a durable receipt-store class for the
  7-day retention window; the Phase 5 body-store split is the
  redaction point and its admin verb applies.

## Admission

- Each member agent app subscribes to `message.mpim` (scope
  `mpim:history`).
- Admission accepts `channel_type == "mpim"` from a REGISTERED agent
  app (verification order unchanged). The switchboard ALREADY
  subscribes to `message.mpim` (existing manifest): switchboard-signed
  mpim events are acked 200 with NO receipt and NO legacy dispatch —
  they must not double-deliver nor fall through (the switchboard has
  no business in agent group DMs; documented, not manifest-changed).
- Origin key `(team_id, mpim_channel_id, ts)` dedups multi-app
  observation: the first member app's event admits; the rest absorb
  as replays. Receipt kind `mpim`; receipt-level `owner_app_id` =
  admission winner = ACK ACTOR ONLY (never a wake-target input). If the
  winner's directory join is lost after admission (offboarded mid-
  flight), that ONLY degrades the visible ack (counted, like a missing
  owner token); it never parks the receipt — the other mentioned
  agents' targets are independent of the owner and must still wake.
  Delivery parks only when routing itself cannot proceed (a registry-
  unavailable author answer, or a wholly empty/unavailable directory
  snapshot that cannot resolve any mention).
- Self-echo N-plication is absorbed the same way; first copy admits
  as `mpim_bot_author`.
- Membership probe (blast-radius mitigation): before the FIRST
  delivery attempt for each woken agent, the delivery worker probes
  the mpim with that agent's own token (`conversations.info`);
  `not_in_channel`/`channel_not_found` → recorded failed target
  `failed_mpim_not_member` (redrive-recoverable), never a wake. A
  MISSING/unloadable woken-agent token also BLOCKS the wake as
  `failed_mpim_not_member` (durable local state, not a transient error —
  redrive-recoverable once the token is dropped): advisory-proceed is
  reserved ONLY for genuine probe transport errors (network / non-
  membership Slack errors), like the session guard.

## Kind-dispatch inventory (normative — review C4/C10)

A shared predicate `isDMFamilyKind(kind)` covers `dm` and `mpim`.
Every existing `Kind == "dm"` seam changes as follows:

| Seam | mpim behavior |
| --- | --- |
| `deliverReceipt` dispatch | dm-family → the DM worker (extended, below); never the room path (an mpim receipt in the room path parks forever on `RoomByChannel`) |
| `ackActorToken` / `noteDMAckDegraded` | dm-family: owner-app join → owner token; degradation counters shared |
| `applyRedrive` unbound re-resolution | dm-family: re-resolve via dm_bindings by agent (extends the dm-only gate) |
| receipt status gauges | dm-family folded into the existing single-scan tally, reported as `company_dm_receipts` plus a `company_mpim_receipts` breakdown |
| `writeCurrentTurnPointer` subdir | kind dm → `dm/`; kind mpim → `mpim/` (NEW subdirectory, 0700 on demand) |
| pointer validators (both languages) | `"mpim"` added to kind sets; `room` empty-permitted; `owner_app_id` REQUIRED non-empty (see Pointer) |
| delegation human-root gate | dm-family refused |
| session-existence guard | dm-family and rooms alike (unchanged) |

## Routing and delivery (corrected seam — review C2/C13)

mpim extends the DM DELIVERY WORKER (`deliverDMReceipt`/
`deliverDMTargets`), which is already multi-target-capable via the
per-receipt `TargetDelivery` map:

1. Author policy (dm rules) → terminal reasons above when no wake.
2. Mention set = `ExtractMentionIDs` × directory `bot_user_id`s (no
   room membership/eligibility analog — Slack membership is the
   roster, enforced by the probe).
3. Route freeze: one target per mentioned agent via
   `dm_bindings.BindingFor(agent)`; unhomed → `failed_dm_unbound`
   (redrive-recoverable via the extended gate).
4. Per-target delivery loop, idempotency keys, sweep, generation
   counters, `computeReceiptStatus` — reused verbatim from the DM
   worker.
5. Reminder: a dedicated mpim renderer (group framing, member
   context, provenance line per the allowlist rule).

## Hydration (pinned — review C5/C14)

ONE frozen hydration blob per receipt (content is token-independent:
same channel history). Token choice: the ADMISSION OWNER's token
first; on missing token file, deterministic fallback over the woken
agents' tokens (sorted by agent name); degrade to
`context_unavailable` only when none load. Replies still use each
woken agent's OWN token.

## Pointer and reply (corrected — review C1/C3/C6/C8/C11/C12)

- mpim turns get their OWN pointer namespace:
  `company-current-turn/mpim/<session>.json`. All three kinds are
  distinct live files; `resolve_reply_pointer_source` becomes
  three-way newest-wins; `--kind` accepts `room|dm|mpim`. (A shared
  dm/ file would be last-writer-DESTROYS: an mpim wake would
  irrecoverably clobber an unanswered 1:1 DM turn — the exact
  C6-class misdirection Phase 4's subdirectory exists to prevent.)
- Pointer `owner_app_id` for kind mpim = the WOKEN AGENT's own
  directory app_id (per-target pointer, per-session file — matches
  the dm validator's owner-join check so every woken agent's reply
  passes its own guard). The RECEIPT-level owner_app_id (admission
  winner / ack actor) is a different field with different semantics;
  the spec's tests pin both. If the woken agent vanished from the
  directory between route-freeze and the pointer write, its app_id
  resolves empty — the worker MUST NOT write that pointer (an empty
  `owner_app_id` is refused at parse time and would brick reply-current
  for EVERY kind on that session, including its live room/dm turns). It
  instead fails the target recoverably (`failed_mpim_agent_unknown`,
  redrive-recoverable) with no pointer and no wake. Defense in depth:
  `resolve_reply_pointer_source` treats a pointer that fails parsing as
  absent for auto-resolution (so one corrupt file cannot disable the
  session's other reply surfaces), raising only when that kind is the
  explicit `--kind`.
- Reply: the woken agent's token, flat unless the human threaded
  (Phase 4 rule), through the durable-intent machinery with intent
  op `dm` (unchanged — reconciliation matches the admitted
  `mpim_bot_author` echo receipts by nonce exactly as dm; a distinct
  op would break `_scan_receipt_for_nonce`'s gates).
- Spoof guard: kind-mpim replies validate through dm_bindings for
  the pointer's agent, same aliasing.

## Out of scope

Ambient in mpim (by design), agent-initiated mpim creation, mpim in
the directory, switchboard manifest changes, the ledger companion's
stance (ask #7).

## Config

None new.

## Test plan

1. Mention ollie only → exactly ollie wakes; olivia (member,
   unmentioned) does not.
2. Mention both → two targets on one receipt, both deliver via the
   DM worker; one unhomed → failed_dm_unbound; `company-redrive`
   after binding import delivers it (redrive gate extension pinned).
3. Unmentioned human → terminal mpim_no_mention; bot-authored →
   terminal mpim_bot_author; both recorded, nobody woken.
4. Multi-app duplicate admission → one receipt; ack actor = winner;
   switchboard-signed mpim copy → 200, no receipt, no legacy
   dispatch.
5. Pointer isolation: dm turn then mpim turn for the same session →
   BOTH live; newest wins; `--kind dm` recovers the older 1:1 turn;
   each pointer's owner_app_id = that turn's woken agent.
6. Reply: each woken agent posts flat with its own token; the
   non-winner agent's reply passes the guard (C11 regression); a
   session not bound to the pointer agent is refused.
7. Membership probe: forged event for a group the agent is not in →
   failed_mpim_not_member, no wake; probe network error → delivery
   proceeds.
8. Allowlist: non-allowed root author → dm_author_not_allowed;
   allowed author + unlisted excerpted member → reminder provenance
   `human_root_unlisted`.
9. Replay/spoof: retry-num absorbed; cross-app signature 401 (rule
   12); hydration token fallback order; ack degradation counters.
10. Wire fixture: captured real `message.mpim` event (envelope
    api_app_id, channel_type, mention rich_text synthesis in mpim —
    MUST be captured live before the mention extractor is trusted,
    since rich_text synthesis is only pinned for channels).
11. Delegation from mpim root → refused.

## Rollout (ordered — review C17)

0. Implement + deploy the adapter's mpim branch FIRST. Interim
   truth: until deployed, member-app `message.mpim` events fall into
   the LEGACY dispatch path N-plicated (once per member app) — the
   bulk manifest update MUST NOT precede the adapter deploy.
1. Capture the live mpim wire fixture (one app, one test group).
2. Bulk manifest update via the config token: add `mpim:history` +
   `message.mpim` to all 18 agent apps; reinstalls activate the
   scope.
3. Pilot: the user's existing group DM (ollie + olivia): mention
   each in turn (exclusive wakes), both (two targets), unmentioned
   message (no wake), reply guard for the non-winner.
4. Fleet-wide.
