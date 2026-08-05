# Slack Company Rooms — Implementation Plan

Companion to `slack-full/docs/company-rooms.md` (v2). Reference
implementation: the Discord company-rooms branch
(`feat/discord-company-rooms`), whose behavioral contract this pack
reproduces on Slack.

Verification bar for every phase (matches CI; each line runs from the repo
root):

```sh
(cd slack-full/adapter && gofmt -l . && go vet ./... && go test -race ./...)
(cd slack-full/cli && go test -race ./...)
python3 -m pytest slack-full/tests -q
```

## Phase overview

| Phase | Scope | Acceptance rules |
| --- | --- | --- |
| 1 | Durable admission, company directory + bindings, deterministic human routing | 1, 2, 4 (non-delegation legs), 6, 8, 9, 10, 11 |
| 2a | Agent identity app provisioning (manifest template, install, harvest, secrets) | pilot checklist |
| 2 | Per-agent tokens, delegation, results, trust envelope, hydration | 3, 5, 7, remaining legs of 4 |
| 3 | Synthesis snapshots, ordered replay, correlation backoff, receipt redrive, visible acks | Phase 3 acceptance proofs 1–8 |
| 4 | Per-agent DMs | 12 |
| 5 | Durable-request-ledger `/v0` integration | blocked on gascity drafts landing |

Company-room behavior stays additive throughout: nothing changes until
`gc slack import-company-directory` has produced a valid directory registry,
and non-company rooms keep legacy behavior in every phase.

---

## Phase 1 — durable admission + directory + human routing

Sub-phases 1a–1c are independent new files (parallelizable). 1d wires them
into the adapter (sequential, after 1a–1c). 1e is the Python/CLI surface
(parallel with 1d).

Delivery guarantee in this phase (stated honestly): at-least-once with
receipt-side suppression. Each session submission carries
`Idempotency-Key: ingress:<id>:target:<session>` as an HTTP header, but
gc's idempotency cache is in-memory with a bounded TTL, so the durable
receipt is the authority; a crash between gc submission and the receipt's
delivery record may duplicate that one target. Phase 5 upgrades this.

### Normalized registries (shared contract for 1a and 1e)

Two CLI-written JSON files, resolved exactly like the existing registries
(same directory as `apps.json`), each with an env-var path override
following the existing per-registry override convention (add
`SLACK_COMPANY_DIRECTORY_PATH`, `SLACK_COMPANY_BINDINGS_PATH`, and
`SLACK_COMPANY_INGRESS_DIR`, mirroring the style of the existing overrides
around `main.go:497-507`; the Python CLI resolves the identical paths).
Both are staged/committed **outside** the six-registry atomic reload with
their own last-known-good retention, so an invalid company file never
blocks the other registries' SIGHUP.

`company_directory.json` — written by `import-company-directory`; wildcards
expanded at import time; the Go loader re-validates and fails closed:

```json
{
  "schema_version": 1,
  "source_sha256": "<hex of the imported TOML>",
  "imported_at": "<RFC3339>",
  "agents": [
    {"name": "ollie", "app_id": "A0AAAAAA1", "bot_user_id": "U0AAAAAA1"}
  ],
  "rooms": [
    {
      "name": "orchestrator-team",
      "team_id": "T0AAAAAAA",
      "channel_id": "C0AAAAAAA",
      "members": ["ollie", "riley"],
      "ambient_wake": ["ollie"],
      "mention_wake": ["ollie", "riley"]
    }
  ]
}
```

`company_bindings.json` — written by `bind-company-agent`; singleton by
construction:

```json
{
  "schema_version": 1,
  "bindings": [
    {"room": "orchestrator-team", "agent": "ollie", "session": "ollie-main"},
    {"room": "orchestrator-team", "agent": "riley", "session": "teams__it",
     "city": "platform"}
  ]
}
```

`city` is optional (city-qualified binding): the live org runs one team
per gc city, each with its own supervisor API, while the switchboard
stays the single admission owner. A city-qualified target is delivered
to that city's API base, resolved from the adapter env
`SLACK_COMPANY_CITY_APIS`
(`"platform=http://127.0.0.1:8377,orchestration=http://127.0.0.1:8374"`);
an unmapped city is a definitive per-target configuration failure
(operator fixes the map and redrives). City values are URL-interpolated
and validated fail-closed against URL-significant/whitespace bytes on
both sides. The frozen `TargetDelivery` and the current-turn pointer
carry the city so redrives and verbs stay city-stable. Cross-city
sessions additionally need the slack pack imported into their city and
the `SLACK_COMPANY_*` state paths pointed at the adapter's dirs (shared
filesystem) for the reply verbs to function.

Validation (both sides, fail closed): lowercase slug names; unique names,
`app_id`s, `bot_user_id`s, room names, `(team_id, channel_id)` pairs;
`ambient_wake ⊆ members`, `mention_wake ⊆ members`; wildcard permitted in
TOML `members`/`mention_wake` only (never `ambient_wake`), absent from the
normalized JSON; unknown agent references invalid; at most one binding per
(room, agent); binding room/agent must exist in the directory at load time
(a binding referencing a missing room/agent is dropped with a warning, not
fatal); empty `agents`/`rooms`/`bindings` valid.

### 1a — `adapter/company_directory.go`, `adapter/company_bindings.go` (+ `_test.go`)

```go
type CompanyAgent struct {
    Name      string `json:"name"`
    AppID     string `json:"app_id"`
    BotUserID string `json:"bot_user_id"`
}

type CompanyRoom struct {
    Name        string   `json:"name"`
    TeamID      string   `json:"team_id"`
    ChannelID   string   `json:"channel_id"`
    Members     []string `json:"members"`
    AmbientWake []string `json:"ambient_wake"`
    MentionWake []string `json:"mention_wake"`
}

type CompanyDirectory struct { /* parsed + normalized indexes */ }

func ParseCompanyDirectory(data []byte) (*CompanyDirectory, error)

func (d *CompanyDirectory) RoomByChannel(teamID, channelID string) (*CompanyRoom, bool)
func (d *CompanyDirectory) AgentByBotUserID(id string) (*CompanyAgent, bool)
func (d *CompanyDirectory) AgentByName(name string) (*CompanyAgent, bool)
func (d *CompanyDirectory) IsMember(room *CompanyRoom, agent string) bool
func (d *CompanyDirectory) IsMentionEligible(room *CompanyRoom, agent string) bool

type CompanyBindings struct { /* (room, agent) -> session index */ }

func ParseCompanyBindings(data []byte, dir *CompanyDirectory) (*CompanyBindings, warnings []string, err error)
func (b *CompanyBindings) SessionFor(room, agent string) (string, bool)
```

Snapshot holders (`companyDirectoryStore`, `companyBindingsStore`) with
`Load(path)` / `Snapshot()` / `StageReload(path)` semantics. **Deliberate
divergence from the other registries**: `Load` at startup is never fatal —
a corrupt or invalid file logs the validation error and installs a nil
snapshot (company routing disabled) while the adapter keeps serving legacy
traffic. Reload failure keeps last-known-good. Nil snapshot = no company
routing.

Tests: every validation rule (table-driven); duplicate/unknown references
fail; corrupt file at construction → nil snapshot + surfaced error,
process viable; last-known-good retention on bad reload; binding referencing
removed room/agent dropped with warning; empty registries inert.

### 1b — `adapter/ingress_receipts.go` (+ `_test.go`)

Durable receipt store rooted at `<GC_CITY_PATH>/.gc/slack/chat-ingress/`
(fallback `/tmp/gc-slack-adapter/chat-ingress`, mirroring
`thread_sessions.json` path resolution; `SLACK_COMPANY_INGRESS_DIR`
override), files 0600 in 0700 dirs.

```go
type ReceiptOrigin struct{ TeamID, ChannelID, TS string }

type TargetDelivery struct {
    Session        string    `json:"session"`
    Kind           string    `json:"kind"`   // "ambient" | "targeted"
    Status         string    `json:"status"` // "pending" | "delivered" | "failed"
    IdempotencyKey string    `json:"idempotency_key"` // ingress:<id>:target:<session>
    Attempts       int       `json:"attempts"`
    UpdatedAt      time.Time `json:"updated_at"`
    Detail         string    `json:"detail,omitempty"`
}

type IngressReceipt struct {
    ID          string        `json:"id"` // "in-" + sanitized origin
    Generation  int64         `json:"generation"`
    Origin      ReceiptOrigin `json:"origin"`
    EventID     string        `json:"event_id"`
    APIAppID    string        `json:"api_app_id"`
    RetryNum    int           `json:"retry_num"`
    RetryReason string        `json:"retry_reason,omitempty"`
    ReceivedAt  time.Time     `json:"received_at"`
    Status      string        `json:"status"` // "received" | "routing" | "delivered" | "no_delivery" | "failed"
    // Event is the COMPLETE inner Slack event object as received —
    // routing (text/blocks/thread_ts) and crash replay depend on it.
    Event   json.RawMessage           `json:"event"`
    Targets map[string]TargetDelivery `json:"targets,omitempty"`
    Reason  string                    `json:"reason,omitempty"` // parked/no_delivery/failed detail
}

func NewIngressReceiptStore(dir string) (*IngressReceiptStore, error)

// Admit is the linearization point, claim-and-content atomic: the full
// receipt is written to a temp file in the same directory, fsynced, then
// hard-linked (os.Link) to the final origin-keyed name; EEXIST = duplicate;
// the directory is fsynced and the temp name removed. An existing but
// unparseable receipt file is quarantined (*.corrupt) and the link retried
// once; persistent failure returns err (caller answers 503). There is no
// state in which a claim exists without content.
func (s *IngressReceiptStore) Admit(r *IngressReceipt) (created bool, existing *IngressReceipt, err error)

// Update performs a generation-checked atomic rewrite (temp+rename+fsync
// of file and directory — same durability as Admit). If the on-disk
// generation differs from r.Generation, ErrStale is returned; the caller
// re-reads and merges. Update increments the generation it writes.
func (s *IngressReceiptStore) Update(r *IngressReceipt) error

func (s *IngressReceiptStore) Get(origin ReceiptOrigin) (*IngressReceipt, error)

// Pending returns non-terminal receipts ordered by (ReceivedAt, Origin.TS).
func (s *IngressReceiptStore) Pending() ([]*IngressReceipt, error)

// Sweep removes terminal receipts older than retention. Retention is
// pinned at 7 days (Discord parity); values below 24h are rejected —
// terminal receipts are the dedup memory for Slack's Delayed Events
// redelivery horizon. Non-terminal receipts are never swept.
func (s *IngressReceiptStore) Sweep(retention time.Duration) (removed int, err error)

// WriteFailures exposes a monotonic counter of failed Admit/Update
// persistence attempts for the gateway status payload / healthz detail.
func (s *IngressReceiptStore) WriteFailures() uint64
```

Filenames derive from the origin via a `safe_storage_id`-style sanitizer
(hash long/hostile components). Terminal states: `delivered`,
`no_delivery`, `failed` (per-target detail preserved). Parked receipts are
non-terminal (`Status: "received"`, `Reason: "parked_no_directory_room"`).

Tests: concurrent Admit first-writer-wins (`-race`); duplicate returns
existing; **crash between temp write and link** (simulate by leaving temp
file) admits cleanly on retry; corrupt existing receipt quarantined then
claimed; Update generation conflict returns ErrStale; un-fsynced-rename
crash simulation (temp present, final intact); Pending ordering; Sweep
respects retention floor, never sweeps non-terminal, quarantines corrupt
scan entries.

### 1c — `adapter/company_routing.go` (+ `_test.go`)

Pure functions, no I/O.

```go
type AuthorClass int

const (
    AuthorHuman AuthorClass = iota
    AuthorCompanyBot // resolution to a single registered agent happens in Phase 2
    AuthorBot        // any bot/webhook/integration author (no delivery in Phase 1)
    AuthorSelf
)

// AdmissibleSubtype reports whether a message subtype is admissible:
// "" (absent), "file_share", "thread_broadcast", "bot_message".
func AdmissibleSubtype(subtype string) bool

type CompanyMessage struct {
    TeamID, ChannelID, TS, ThreadTS string
    UserID, BotID                   string
    Subtype                         string
    Text                            string
    Blocks                          json.RawMessage
}

type WakeTarget struct {
    Agent CompanyAgent
    Kind  string // "ambient" | "targeted"
}

type RouteDecision struct {
    Room   *CompanyRoom
    Author AuthorClass
    Wakes  []WakeTarget
    Reason string // machine-readable no-delivery reason
}

// ExtractMentionIDs returns the UNION of user IDs found as rich_text
// "user" elements in blocks and as canonical <@U…> / <@U…|label> tokens in
// text. Neither source alone is sufficient: Slack guarantees rich_text
// only for end-user client messages, and bot-composed messages may carry
// mentions only as text tokens.
func ExtractMentionIDs(blocks json.RawMessage, text string) []string

func ComputeWakeSet(dir *CompanyDirectory, msg CompanyMessage, selfBotUserID string) RouteDecision
```

Rules implemented exactly (design doc routing table): mention exclusivity;
ambient set for unmentioned human messages; the subtype allowlist gate
(non-allowlisted subtypes are handled before this function, but
`ComputeWakeSet` re-checks and returns no wakes with reason
`subtype_not_admissible` as defense in depth); bot-authored messages
(`bot_message` subtype or `BotID != ""`) return **no wakes in Phase 1**
with reasons distinguishing `company_bot_phase2` / `unknown_bot` /
`company_self`; non-company channels return `Room == nil` (caller falls
through to legacy); mentioned-but-ineligible or non-member IDs add no wake
(recorded in Reason). Wake set deduplicated.

Tests: table-driven port of acceptance rules 1, 2, 4, 6, plus: mention
extraction from rich_text alone, text tokens alone (`<@U…>` and
`<@U…|label>` forms), union of both, literal `@riley` never matches,
`channel_join`/`channel_topic` subtypes produce no wakes, self messages
produce no wakes.

### 1d — adapter wiring (`main.go`, `company_ingress_test.go`)

- Load both company registries at startup (never fatal — nil snapshot on
  error) and stage/commit them on SIGHUP **outside** the six-registry
  atomic set, with their own last-known-good log lines.
- `handleSlackEvents` changes, inserted between the `url_verification`
  return (~main.go:1917-1921) and the current unconditional
  `w.WriteHeader(StatusOK)` (~main.go:1924):
  - Synchronously decode the inner event when the envelope is an
    `event_callback` (the current code defers this to the async goroutine;
    the company path needs channel/ts/subtype before acking). Extend the
    event struct with `Blocks json.RawMessage`.
  - If `(team_id, channel)` matches a directory room: apply the
    admissibility gate (allowlist; non-admissible → 200, no receipt;
    unkeyable → log + 200); capture `X-Slack-Retry-Num`/`-Reason` from
    `r.Header`; `Admit`; duplicate → 200; created → trigger delivery and
    200; store failure → 503 **without** `x-slack-no-retry`.
  - Non-company events keep today's path byte-for-byte.
  - Admission never depends on `acquireDispatchSlot`: the semaphore gates
    only the async delivery trigger; if no slot is free the receipt simply
    stays pending for the sweep (backpressure, not drop).
- Company delivery worker: in-process single-flight per receipt ID;
  claims the receipt (`Status: routing` via generation-checked Update);
  computes the wake set (1c) only when the receipt has no recorded
  targets — recorded targets are a frozen route that redrives drive to
  terminal states, never recompute (`no_delivery` is legal only with no
  recorded targets and an empty freshly-computed wake set); resolves each
  wake through the company-bindings snapshot (missing binding → target
  `failed` with reason, no legacy fallback); delivers each target via a
  new `deliverToCompanySession` helper — POST
  `/v0/city/{city}/session/{session}/messages` with header
  `Idempotency-Key: ingress:<id>:target:<session>`, an explicit
  per-request timeout, and the system-reminder-style envelope used by
  alias dispatch (markup-neutralized); marks a target `delivered` only on
  gc's acknowledged 2xx; leaves it `pending` (attempts++) on
  timeout/5xx/408/429/connection error for sweep retry with the same
  key; marks it `failed` on any other 4xx or when the bounded attempts
  cap is exhausted; sets the terminal receipt status when all targets
  resolve. `app_mention` events for company-room channels are owned by
  the company path (200, no receipt, no legacy dispatch).
- Parking: a pending receipt whose channel matches no *current* directory
  room (including nil directory) is left pending with
  `Reason: parked_no_directory_room` — never terminally resolved, never
  legacy-delivered; the sweep retries it after every directory change.
- Startup recovery barrier, company-scoped: legacy routes, `/healthz`,
  interactions, and the internal listener serve immediately; company-room
  admissible events get 503 (retryable) until one synchronous
  `Pending()` scan completes and its receipts are enqueued. (Posting
  intents precede pending ingress in the barrier ordering from Phase 2
  on.) `/healthz` detail is the sole adapter status surface: barrier
  state, receipt-store health and `WriteFailures()`, delivery-failure
  count, and directory/bindings snapshot state. Membership warnings
  surface only via the Python `import-company-directory`/`peers` path in
  Phase 1. A receipt-store construction failure at startup puts the
  gateway in degraded mode — company-admissible events 503 (never
  legacy), `/healthz` reports the store error, and construction is
  retried — it is never a silent fallthrough to legacy.
- Periodic sweep (60s) starts after one successful barrier pass, waits one
  interval first, respects the routing-claim stale-reclaim window (skip
  receipts whose `UpdatedAt` is fresher than 5 minutes unless terminal).
- Company rooms bypass legacy generic fanout and alias dispatch
  deterministically.

Integration tests (`company_ingress_test.go`, httptest): acceptance 8
(admit, crash before delivery → restart delivers exactly once; crash
after submit before record → second delivery attempt carries the same
Idempotency-Key), 9 (retry-num redelivery → 200, no second delivery), 10
(store failure → 503 without no-retry header + counter increment;
subsequent retry admits), 11 (parked receipt: directory removed → stays
pending; restored → delivers), ambient and mention routing end-to-end,
saturation backpressure (no slot → receipt pending, sweep delivers),
legacy path untouched for non-company channels.

### 1e — CLI surface (Python)

New script `slack-full/scripts/slack_company_directory.py`: TOML parse
(`tomllib`), full validation, wildcard expansion, normalized JSON emission
(atomic tmp+rename) for both registries, `source_sha256`, membership
verification via `conversations.info`/`conversations.members` (warnings,
not failures, when the token lacks scopes or the bot is not a member),
`peers` listing (rooms, members, wake policy, bindings, membership
warnings). Command wrappers follow the shipped convention —
`commands/<verb>.sh` at the top level plus `commands/<verb>/command.toml`
(with `run = "../<verb>.sh"`) and `commands/<verb>/help.md` — for three
verbs: `import-company-directory`, `bind-company-agent`, `peers`. No
`pack.toml` registration exists or is needed (verb discovery is
directory-driven). Tests (`tests/test_slack_company_directory.py`):
validation table mirrored from 1a, TOML→JSON golden file, idempotent
re-import, invalid file leaves the existing registry untouched, bind
create/replace, peers output including membership warnings (Slack API
mocked).

---

## Phase 2a — agent identity app provisioning

Manifest template for agent identity apps (`manifest/agent-app.json` +
README section): bot user, `chat:write`, `app_home.messages_tab_enabled:
true`, `messages_tab_read_only_enabled: false`, interactivity off, **no
event subscriptions** (the DM phase adds `message.im` + `im:history`).
Switchboard manifest (`manifest/app.json`) gains `channels:read`,
`groups:read` (membership checks) and `users:read` (`bots.info` author
resolution) — scope changes require reinstall, called out in the runbook.
Operator steps documented: create/install per agent, harvest `app_id` +
`bot_user_id` into the directory TOML, register each signing secret with
the adapter (existing `import-app` flow), drop each bot token into the
company secrets dir (the token loader refuses files not 0600, dirs not
0700, and symlinks — validation, not trust), invite each member bot to
its rooms. These apps are internal, never Marketplace-distributed
(distribution would demote `conversations.*` rate tiers).

## Phase 2 — identities, delegation, results, hydration

Ownership split (mirrors the Discord reference): **Python owns company
outbound** — intents, per-agent token files, `chat.postMessage`,
delegation-record creation, lazy recovery, pruning — exactly as
`discord_chat_delegate.py` / `discord_intake_common.py` do; **Go owns
ingress** — author resolution, trust checklist, result-claim and expiry
transitions on delegation records, peer envelopes, hydration, the
per-session current-turn pointer. Both sides write shared on-disk state
under the adapter state root; every schema below is the cross-language
contract, validated fail-closed on both sides, serialized by the lock
contract below.

### Shared state contract (pins 2b and 2c)

Paths (env override > `<GC_CITY_PATH>/.gc/slack/<leaf>` >
`/tmp/gc-slack-adapter/<leaf>`, matching Phase 1 conventions):
- `SLACK_COMPANY_SECRETS_DIR` > `secrets/` — token files
  `bot-token-<agent>.txt`, 0600 in 0700 dir, one per directory agent.
- `SLACK_COMPANY_INTENTS_DIR` > `company-delegation-intents/` —
  `<nonce>.json`, written by Python only.
- `SLACK_COMPANY_DELEGATIONS_DIR` > `company-delegations/` — created
  once by Python at publish (O_EXCL/link create-once; `EEXIST` = adopt
  the existing record, never overwrite); status transitions
  (claim/expiry) written by Go, expiry also by Python's lazy pruner.
- `SLACK_COMPANY_TURNS_DIR` > `company-current-turn/` —
  `<session>.json`, written by Go only (the delivery worker), read by
  the Python verbs.
- `SLACK_COMPANY_LOCKS_DIR` > `locks/` — advisory lock files (below).

**Lock contract.** All cross-process critical sections use an advisory
`flock(LOCK_EX)` (`fcntl.flock` in Python, `syscall.Flock` in Go) on
`locks/<label>-<sha256hex(NUL-joined key fields)[:16]>.lock`. Two locks
exist in Phase 2: the **delegation-tuple lock** (label `dtuple`, key =
team, channel, thread_root_ts, responder_bot_user_id,
requester_bot_user_id), held by `delegate` across its
scan → intent-create → post → record-materialize section, by `--cancel`,
by Go's claim transition, and by any expiry rewrite; and the
**intent lock** (label `intent`, key = nonce) for attempts-CAS updates.
Generation checks remain on top of the locks (defense in depth), but
the locks are what make cross-process read-modify-write sound.

**Filename sanitizer (byte-for-byte cross-language spec).** For each
component: bytes outside `[A-Za-z0-9._-]`, a leading `.`, the string
`..`, or length > 64 cause the component to be replaced by
`h<sha256hex(component)[:16]>`; otherwise the component passes through.
Delegation filename: `dg-<san(team)>-<san(channel)>-<san(ts)>-`
`<sha256hex(team NUL channel NUL ts)[:12]>.json` (digest suffix always
present, mirroring Go `receiptID`). 2d ships golden filename fixtures
both suites must reproduce exactly.

**Root derivation (normative).** From the triggering receipt's stored
event: `human_root_ts := Event.thread_ts if non-empty else Event.ts`.
`thread_ts` on `chat.postMessage` must always be a parent — Slack
documents replying to a reply as invalid. 2c's root verifier grants
`root_provenance: human_root_verified` only when the fetched root is a
parent (`thread_ts` absent or equal to `ts`) AND its author is a non-bot
human; otherwise `root_unverified`.

`company-delegation-intents/<nonce>.json` — created BEFORE any provider
POST, for delegations AND for results/synthesis (an `op` field:
`delegation | result | synthesis`; every company post goes through the
intent path so no post can double on a timeout). Nonce = `gcs-` + first
20 hex of sha256 over the canonical anticipated record: (source app_id,
source bot_user_id, target agent, target bot_user_id, team, channel,
human_root_ts, body_sha256, retry_seq) — `retry_seq` is monotonic:
`max(retry_seq of existing intents for the tuple) + 1` when minting
fresh (never a file count, so pruning cannot re-mint a used nonce), and
the pruner always retains the highest-seq intent per tuple. Crash
retries of one logical delegation resume the existing intent; successive
logical delegations mint fresh nonces:

```json
{
  "schema_version": 1,
  "nonce": "gcs-<20hex>",
  "retry_seq": 0,
  "status": "prepared",
  "attempts": 0, "max_attempts": 3,
  "created_at": "<RFC3339>", "updated_at": "<RFC3339>",
  "retry_deadline": "<RFC3339: first attempt + 120s>",
  "ttl_seconds": 86400,
  "source_agent": "ollie", "source_app_id": "A…", "source_bot_user_id": "U…",
  "target_agent": "riley", "target_bot_user_id": "U…",
  "team_id": "T…", "channel_id": "C…", "room": "orchestrator-team",
  "human_root_ts": "1700000000.000100",
  "requester_session": "ollie-main",
  "body_sha256": "<hex>",
  "posted_ts": ""
}
```

Statuses: `prepared → posting → published | failed | expired`.
Attempts updates under the intent lock. **Recovery is lazy and
Python-owned** (accepted deviation from the Phase 1 barrier-ordering
sentence, which is amended): every company verb invocation first runs a
bounded reconciliation pass over `posting` intents. Reconciliation does
NOT call the Slack API: the delegation post, arriving back through the
switchboard, is itself admitted as a bot-message receipt whose stored
raw event embeds the posted metadata — reconciliation scans the
receipts dir (read-only) for a bot-authored receipt in (team, channel)
whose `metadata.event_payload.nonce` equals the intent nonce; found →
adopt its origin `ts` as `posted_ts`, mark `published`, materialize the
delegation record if absent; not found and past `retry_deadline` → the
intent stays parked (`posting`) — never repost on ambiguity
(`chat.postMessage` is not idempotent). The Go sweep surfaces a count of
stale `posting` intents (age > retry_deadline) on `/healthz` as the
operator signal. Existing-nonce handling at `delegate` time: `prepared`
→ adopt and resume (nothing was ever posted; safe to proceed to
posting); `posting` → resume reconciliation; `published` with its
delegation record still `pending` → the one-pending error below;
anything terminal → mint a fresh nonce at the next `retry_seq`.

`company-delegations/<key>.json` (key per the sanitizer spec) —
materialized before the CLI reports success:

```json
{
  "schema_version": 1,
  "generation": 1,
  "nonce": "gcs-<20hex>",
  "room": "orchestrator-team",
  "team_id": "T…", "channel_id": "C…", "ts": "<posted_ts>",
  "thread_root_ts": "<human_root_ts>",
  "requester_agent": "ollie", "requester_bot_user_id": "U…",
  "requester_session": "ollie-main",
  "expected_responder_agent": "riley",
  "expected_responder_bot_user_id": "U…",
  "created_at": "<RFC3339>", "ttl_seconds": 86400,
  "status": "pending",
  "result_ts": "", "result_claimed_at": ""
}
```

**Result correlation (normative; Slack deviation from Discord).** Slack
has no nested replies — delegation and result share the human root's
thread. A claim requires ALL of: (1) the message passes the five-part
peer trust checklist; (2) it is in thread `thread_root_ts`, authored by
`expected_responder_bot_user_id`; (3) its native mention set includes
`requester_bot_user_id`; (4) its `metadata.event_type ==
"gc_delegation_result"` with `event_payload.nonce == record.nonce` and
`event_payload.delegation_ts == record.ts` (the breadcrumb is
load-bearing for claim ADMISSION — responder chatter, clarifying
questions, and hand-typed posts deliver as ordinary peer input without
consuming the claim; authorship remains the Slack-authoritative trust
anchor, so metadata alone can never claim); (5) route window
`-300s <= now - created_at <= ttl`. `delegate` enforces **at most one
pending delegation per (team, channel, thread_root_ts, responder,
requester)** under the tuple lock; TTL-expired `pending` records count
as not-pending (and are rewritten `expired` under the lock). Go's claim
(`pending → result_claimed`, under the tuple lock, generation-checked)
is replay-idempotent; >1 pending record matching one claim key parks
the receipt fail-closed (`ambiguous_pending_delegations`). A wedged
flow is recoverable without the TTL: `gc slack delegate --cancel --to
<agent>` transitions the caller's own pending record to `expired` under
the lock.

**Pruning (lazy, Python-owned).** Every company verb invocation prunes:
terminal intents and terminal/expired delegation records older than 7
days. The prune retention must be `>=` the maximum record
`ttl_seconds` plus a one-hour clock-skew margin (the pruner clamps to
that floor), so a record can never be S2-compatible on Go's clock
while already prunable on Python's; snapshot scans tolerate concurrent
deletion by skipping vanished files. The pruner always retains the
highest-`retry_seq` intent per tuple (the monotonicity watermark);
pruned files cannot collide because intent creation is O_EXCL on a
fresh nonce.

`company-current-turn/<session>.json` — written atomically by the Go
delivery worker on EVERY company wake, before the gc session POST; the
deterministic context source for the Python verbs (no receipt scanning):

```json
{
  "schema_version": 1,
  "session": "ollie-main",
  "receipt_id": "in-…",
  "team_id": "T…", "channel_id": "C…", "ts": "<origin ts>",
  "room": "orchestrator-team",
  "kind": "ambient | targeted | peer_input | peer_delegation | peer_result",
  "thread_root_ts": "<derived root>",
  "agent": "ollie",
  "delegation_key": "<delegations filename; present iff kind is peer_delegation or peer_result>",
  "delivered_at": "<RFC3339>"
}
```

The delivered reminder text also displays the origin `ts`, and the verbs
accept `--origin-ts` to pin a specific turn when a newer wake has
overwritten the pointer (mismatch without the flag is a hard error
telling the agent to pass it).

Message metadata: delegations post `metadata: {event_type:
"gc_delegation", event_payload: {v: 1, nonce, root_ts, requester,
target}}`; results post `metadata: {event_type: "gc_delegation_result",
event_payload: {v: 1, nonce, delegation_ts}}`. Metadata is embedded in
the switchboard's `message.*` events (no extra scope) and is
workspace-visible and mutable — breadcrumbs plus claim-admission gate;
the durable record stays authoritative. Pilot wire capture must confirm
metadata presence on a real agent-app post event.

Composition contract (both verbs): top-level `text` only — no `blocks`,
no `link_names`, `reply_broadcast` never set, default `parse` (bare URLs
may auto-link; harmless, no notification) — body entity-escaped (`&`,
`<`, `>`); the service-constructed mention is the only live entity.
Delegation text: `<@target_bot_user_id> <escaped body>` with
`thread_ts=human_root_ts`. Result text: `<@requester_bot_user_id>
<escaped body>` with `thread_ts=thread_root_ts`. Synthesis: escaped
body, no live mentions, `thread_ts=thread_root_ts`. All company posts
are therefore visible in the human root's thread (not the channel
timeline). Retry policy on `chat.postMessage`: explicit timeout; 429
honors `Retry-After` within `max_attempts`; definitive 4xx → intent
`failed`; timeout/5xx → reconcile-before-repost.

Identity plumbing: the session namespace is the bound session NAME,
obtained from `GC_SESSION_NAME` (hard error if unset); the anti-spoof
check compares it to the pointer file's `session` and the (room, agent)
binding. `bind-company-agent` rejects binding a session already bound
to a DIFFERENT agent in the same room, so `delegate`'s reverse
resolution session → (room, agent) is unambiguous.

### 2b — Python outbound (`scripts/slack_company_outbound.py` + verbs)

New module `slack_company_outbound.py` (intents store with lock/CAS,
token loader with permission/symlink refusal, sanitizer, escaping/
composition, postMessage with metadata, bounded retries, receipt-based
reconciliation, lazy pruner) + `gc slack delegate` verb (3-file
wrapper, including `--cancel`) + `reply-current` gaining
company-context awareness: it reads `company-current-turn/<session>`;
`kind: peer_delegation` → post the result (acting agent's own token,
metadata gate attached) and report `posted_ts` only on success; `kind:
peer_result` → post the synthesis to the human root with no live
mentions; ambient/targeted → unchanged legacy behavior for non-company
context, company rooms answer into the root thread. Tests: hermetic
(mocked Slack API), covering intent lifecycle incl. receipt-based
reconciliation (nonce receipt found → adopt + materialize; absent →
parked, never reposted), retry_seq nonce freshness after terminal
intents, one-pending-per-tuple under two concurrent delegates (real
flock race test), `--cancel`, composition/escaping (`@channel @here
#general <!channel>` inert; only the target mention live), root
derivation from threaded and unthreaded triggers, token
selection/permission refusal, Retry-After honored, definitive 4xx →
failed, record create-once (EEXIST adopts), pointer-file consumption +
`--origin-ts` mismatch error, pruning.

### 2c — Go ingress (author resolution, trust, correlation, envelopes)

- `company_authors.go`: cached `bot_id → bots.info → user_id` resolver
  (switchboard token, `users:read`), TTL cache + singleflight.
  Outcomes are split: definitive `bot_not_found`/`deleted` → unknown
  bot (terminal for that message); transient (ratelimited — honor
  `Retry-After` — timeout, 5xx, network) → park the receipt
  non-terminally (`author_resolution_pending`) for sweep retry, never
  a terminal `no_delivery`. Corroboration: the event's own
  `app_id`/`bot_profile` fields pre-check, `bots.info`'s `app_id` must
  match the directory agent's `app_id`, `event.user` must match when
  present; any mismatch → unknown bot.
- `company_routing.go`: `CompanyMessage` gains `ResolvedBotUserID
  string` (populated by the delivery worker; keeps ComputeWakeSet
  pure) and `Metadata json.RawMessage`; `slackMessageEvent` gains
  `Metadata json.RawMessage`. The bot-authored mention-wake leg turns
  on: AuthorCompanyBot (resolved, room member) with a native mention of
  an eligible member routes per the table. Reason-code taxonomy
  replacing `company_bot_phase2`: `company_bot_no_mention`,
  `company_bot_not_member`, `unknown_bot`, `company_self` (existing),
  plus routing-stage `mentioned_not_eligible` reasons — Phase 1 tests
  pinning `company_bot_phase2` are updated in the same change.
- `company_peer.go`: five-condition trust checklist with
  machine-readable failure reasons; result-claim per the shared
  contract (tuple lock + generation, metadata gate, requester-mention
  check, replay-idempotent, `ambiguous_pending_delegations` parking);
  plausible in-flight tuple (a `posting` intent exists) → park
  `correlation_pending`; unmatched identifiable replies to the
  switchboard rejected; everything else = ordinary peer delegation
  processing.
- `company_delivery.go`: writes `company-current-turn/<session>.json`
  before each gc POST; envelope stays a single reminder string (gc API
  unchanged) rendered from a pinned template with sections for kind,
  `peer_authority: peer_only`, `root_provenance`, verified human root,
  and the bounded excerpt — every interpolated value (root JSON fields,
  each excerpt line) passes `neutralizeMarkupBoundaries`.
  `TargetDelivery.Kind` grows `peer_delegation`/`peer_result` (Phase 1
  fixtures updated). **Hydration is frozen**: the verified root and the
  excerpt (`conversations.history`/`replies`: max 8 messages, 12KiB
  total, 1024 chars each; failure → `context_status:
  context_unavailable`, never broader routing) are fetched once at
  first delivery and stored in a new receipt field `Hydration
  json.RawMessage`, so redrives re-render identical bytes under the
  same Idempotency-Key. Sweep gains the stale-`posting`-intent count
  for `/healthz` (read-only scan).
- Tests: acceptance 3 (trusted Ollie `@Riley` wakes Riley exactly once
  through the full checklist), 5 (Riley's metadata-gated threaded
  result wakes only Ollie and claims the record; a clarifying question
  without result metadata delivers as peer input and claims nothing;
  replay claims nothing twice), 7 (dormant mentioned agent gets
  current message + verified root + bounded excerpt, no duplicate;
  threaded human trigger derives the parent root), remaining rule-4
  legs (unknown bot, non-member author, self, unresolvable
  `bot_message`, mention of non-member, expired/claimed/ambiguous
  results fall to peer input or park), transient `bots.info` failure
  parks then delivers exactly once, hydration-failure marker, excerpt
  bounds, frozen-hydration byte-identity across redrives,
  pointer-file write ordering.

### 2d — cross-language wire tests

`slack-full/tests/fixtures/company/` golden files: intent record,
delegation record, current-turn pointer, and sanitizer filename
fixtures (hostile/long/dotted components). The Go and Python suites
each parse, validate, and re-derive the same bytes. Interop is
fixture-mediated across the two independent suites:
`fixtures/company/interop/` holds records generated through the REAL
Python code paths (with pinned clocks); the Python suite proves
byte-stable regeneration, and the Go suite claims/consumes those exact
bytes through its real claim and pointer paths. Lock-filename parity is
pinned by identical derivations in both suites (verified:
`dtuple-3a4b34ac4caada68.lock` from both languages for the same key).

Pilot step: capture one real agent-app post event and assert the
mention extractor, author classifier, AND embedded metadata match the
wire shape the contract assumes.

## Phase 3 — synthesis snapshots, ordered replay, redrive, visible acks

Reference citations in this section: `CM` =
`discord/scripts/discord_intake_common.py`, `GW` =
`discord/scripts/discord_gateway_service.py`, `DOC` =
`discord/docs/company-rooms.md`, all on `feat/discord-company-rooms`.

Ownership split continues the Phase 2 pattern: **Go owns every synthesis
write** — the claim-time snapshot freeze (it already owns the claim
transition), group/serialization locks on the ingress side, replay
ordering, correlation backoff, reactions, and the receipt-redrive
surface; **Python owns the agent-facing reads** — the snapshot validator
behind `reply-current`'s synthesis gate, the one-hop gate in `delegate`,
and the new operator verbs (thin clients of the adapter's internal
listener; the receipt store keeps exactly one writer). Every schema and
lock below is cross-language contract, validated fail-closed on both
sides.

### Shared state contract additions (pins 3a–3c)

**Synthesis group (normative).** The canonical durable root shared by
sibling delegations is the 5-tuple, in this pinned field order:

```
(team_id, channel_id, thread_root_ts, requester_bot_user_id, requester_session)
```

A delegation record belongs to a group iff all five fields are non-empty
(Phase 2 record validation already guarantees this for parseable
records; an unparseable record has no group and is never claimable).
**Deviation D1 (normative):** Discord's group is an 8-tuple including
`root_ingress_receipt_id`, `source_app`, and the gc session *ID*
alongside the name (CM:2250-2291). On Slack the human-root receipt ID
is a pure function of `(team_id, channel_id, thread_root_ts)` and the
requester's app is 1:1 with `requester_bot_user_id` under directory
uniqueness — those dimensions are derivable. The session-incarnation
dimension is NOT derivable: Phase 2 records carry only the session
name (`GC_SESSION_NAME`), so a requester session destroyed and
recreated under the same name mid-flight joins the dead incarnation's
group, and the stale incarnation's pending delegations freeze new
claims not-ready until their 24h TTL or `gc slack delegate --cancel`.
This is an accepted, declared consequence (failure direction is safe:
not-ready + operator-resolvable), revisited if gc ever exposes a
stable session incarnation ID to the verbs. Two implementations
disagreeing on the group tuple is a correctness bug; 3c pins it with a
fixture.

**Locks.** Two new labels under the existing Phase 2 lock contract
(`locks/<label>-<sha256hex(NUL-joined key fields)[:16]>.lock`,
`flock(LOCK_EX)`):

- **Group lock** — label `dgroup`, key = the 5 group fields in pinned
  order. Serializes result claims for one group (DOC:155-157;
  CM:2510-2521). When a claim's preflight scan finds no matching record
  (the correlation-pending leg), the group is unknown; the lock key is
  then the 6 fields `("unavailable", team_id, channel_id,
  thread_root_ts, responder_bot_user_id, requester_bot_user_id)` —
  mirroring Discord's `("unavailable", delegation_id)` fallback
  (CM:2511-2515).
- **Root serialization lock** — label `dgser`, key = `(team_id,
  channel_id, thread_root_ts)`. The Slack port of Discord's coarse
  per-(app, bot, guild, room) referenced-bot-message lock
  (DOC:166-171; GW:2739-2767): held by the delivery worker for a
  result-bearing message from before correlation through local delivery
  and finalize, so snapshot order at the requester equals delivery
  order. **Deviation D4 (normative):** the Slack key is finer than
  Discord's room-wide key because the thread root is derivable from the
  stored event *before* correlation (`thread_ts`), which Discord's
  reply reference is not (it requires a fetch). Sibling results always
  share `thread_root_ts`, so the ordering guarantee is preserved; the
  pilot caveats transfer verbatim: the lock is coarse, queues every
  result for the same root behind a slow delivery, is not
  cancellation-aware, and production must replace it with a durable
  ordered per-root delivery queue before increasing concurrency
  (DOC:168-171).

**Lock ordering (deadlock freedom, normative):** `dgser` → `dgroup` →
`dtuple` → `intent`. No code path acquires a higher-rank lock while
holding a lower-rank one. Python never takes `dgser` or `dgroup`
(delegate/cancel stay `dtuple`-only, matching Discord where delegate
takes no group lock — snapshot readiness "neither predicts future
delegations" DOC:161-163, so sibling materialization needs no group
serialization).

**Synthesis snapshot fields (additive, on the delegation record).**
Frozen by Go in the *same atomic rewrite* as the
`pending → result_claimed` transition (rule 11; CM:2479-2581 persists
claim + snapshot in one `save_company_delegation`):

```json
{
  "...existing Phase 2 fields...": "...",
  "status": "result_claimed",
  "result_ts": "1700000000.000300",
  "result_claimed_at": "<RFC3339>",
  "synthesis_state_version": 1,
  "synthesis_state_available": true,
  "compatible_delegation_count": 2,
  "responded_delegation_count": 1,
  "pending_delegation_count": 1,
  "pending_delegations": [
    {
      "delegation_ts": "1700000000.000200",
      "delegation_key": "dg-<san(team)>-<san(channel)>-<san(ts)>-<12hex>.json",
      "expected_responder_agent": "seth",
      "expected_responder_bot_user_id": "U…"
    }
  ],
  "synthesis_ready": false,
  "synthesis_snapshot_at": "<RFC3339, == result_claimed_at>"
}
```

Decision and justification: the snapshot lives **on the delegation
record**, not a separate group record — exactly Discord's placement
(CM:2389-2398). One record, one write gives claim+snapshot atomicity
under the existing generation counter and tuple lock with no
two-file-commit protocol; the replay orderer reads it from the records
it already loads; and a separate group record would need its own
generation/lock/pruning lifecycle for no additional invariant. Both
parsers treat the fields as additive: Go's struct-decode ignores
unknowns and `companyRewriteDelegation` preserves them
(company_peer.go:222-235); Python's `parse_delegation` must accept and
pass them through. Pending-entry identity uses the Slack record's field
names (`expected_responder_agent`, not Discord's
`expected_responder_name`) plus both the posted `ts` and the derived
`delegation_key`, so readers never re-derive filenames.

**Receipt store additions (`ingress_receipts.go`, all
`json:"…,omitempty"`, Phase 1 fixtures unaffected):**

```go
// Synthesis is the frozen snapshot bytes for a peer_result receipt,
// copied from the claimed record in the same routing commit that
// freezes targets, so redrives re-render byte-identical reminder
// synthesis fields even if the record is later pruned (the same
// frozen-bytes discipline as Hydration).
Synthesis json.RawMessage `json:"synthesis,omitempty"`
// Correlation-park recovery backoff (rule 17 parity). NOTE omitzero,
// not omitempty: encoding/json's omitempty never elides a struct, and
// a bogus 0001-01-01 timestamp on every receipt would pollute the
// cross-language shape (go.mod is go >= 1.24, omitzero available).
RecoveryAttempts int       `json:"recovery_attempts,omitempty"`
RecoveryNextAt   time.Time `json:"recovery_next_at,omitzero"`
RecoveryReason   string    `json:"recovery_reason,omitempty"`
// AckState is the visible-ack cursor: "" | "eyes" | "done" | "degraded".
AckState string `json:"ack_state,omitempty"`
```

### Normative rules ported from Discord

- **S1 — Atomic claim + frozen snapshot (rule 11; CM:2479-2581,
  2321-2398).** Under `dgroup` then `dtuple`, the claim computes and
  persists the snapshot with the `pending → result_claimed` transition
  in one atomic write. Preflight/group-changed handling pins the
  CM:2510-2526 loop explicitly: scan without locks; derive the group
  from the matched pending record (or the fallback key on no match);
  acquire the locks; re-scan; if the tuple's record set or derived
  group changed, **release both locks, re-derive from the fresh scan,
  re-acquire, and re-scan** — at most 3 iterations, then park the
  receipt `correlation_error` (every-sweep cadence, no attempt
  budget). Never demote a mismatched claim to a frozen `peer_input`:
  targets freeze at the first routing commit, and a result consumed
  as peer_input can never claim later.
- **S2 — Compatible set (rule 12; CM:2268-2318).** A record is
  *compatible* with a claim's group iff it parses, its group equals the
  claim's group, its status is `pending` or `result_claimed`, and its
  age satisfies `-300s <= now - created_at <= ttl_seconds`. The record
  being claimed is always included (CM:2348-2358). *Responded* =
  status `result_claimed`. **Deviation D3 (normative, structural
  equivalence):** Discord's responded statuses are
  `{result_processing, completed, result_recovery_failed}`
  (CM:2268-2272) because Discord tracks local result delivery on the
  delegation record. On Slack, local delivery lives on the result's
  ingress receipt (Phase 1/2 target states), so `result_claimed` is
  the single responded status and no
  `result_processing`/`completed`/`result_recovery_failed` statuses
  exist. This preserves the reference semantics exactly because
  readiness explicitly does not assert local delivery success
  (DOC:161-163; GW:1195-1198).
- **S3 — Snapshot content (CM:2360-2398).** `compatible_delegation_count`,
  `responded_delegation_count`, `pending_delegation_count`,
  `pending_delegations` (the still-`pending` compatible records, sorted
  by `(created_at, ts)`), `synthesis_ready := compatible > 0 &&
  responded == compatible`, `synthesis_snapshot_at := result_claimed_at`.
  The first of two serialized sibling claims is therefore frozen
  not-ready with one pending sibling; the second is frozen ready
  (DOC:158-161).
- **S4 — Frozen snapshots are immutable (rule 11; CM:2528-2566).** A
  claim replay — same `result_ts` and same nonce against an
  already-claimed record — returns the stored record and rewrites
  nothing. A *different* result against a claimed record claims
  nothing (Phase 2 behavior, now extended: it is peer input, never a
  second claim). Replay acceptance is time-bounded: the idempotent hit
  is honored only while `-300s <= now - result_claimed_at <= 7 days`
  (the receipt-retention constant; `companyPeerEnv` gains a
  `retention` field plumbed from the gateway's value; CM:2548-2565);
  outside the window — or when `result_claimed_at` is unparseable (S9
  fail-closed reads) — the message is ordinary peer input. This
  amends the unbounded replay match Phase 2c shipped
  (company_peer.go:469-474). An already-claimed tuple bypasses the
  `postingIntentExists` check entirely: a lagging `posting` intent for
  a claimed tuple must never park the message `correlation_pending`.
- **S5 — Monotonic per-group replay ordering (rule 16; GW:2031-2097;
  DOC:181-187).** Startup recovery and every sweep pass order replay
  within a group: receipts whose claimed record has no valid snapshot
  (legacy/malformed/pruned) first, by `(ReceivedAt, origin ts, receipt
  id)` — the receipt does not store `result_claimed_at`/result ts, so
  the received time is the normative stand-in (a deterministic total
  order over the legacy bucket); then snapshot-bearing receipts by
  ascending
  `(responded_delegation_count, synthesis_snapshot_at, result ts,
  receipt id)`. Slack `ts` ordering is numeric on the two
  `.`-separated integer components; a malformed ts sorts after all
  well-formed ts by raw string (a deliberate simplification of GW's
  `(1, created_at, raw_id)` malformed fallback — both are
  deterministic total orders). Receipts outside any group keep the
  store's `(ReceivedAt, Origin.TS)` order (GW:2080-2093 permutes only
  sibling positions). Chains are partitioned by root triple, coarser
  than Discord's per-group permutation: multiple groups sharing one
  root serialize together, which is safe (sorting a superset by a
  total order preserves each group's relative order) and merely adds
  cross-group serialization Discord did not impose; pruned-record
  receipts land in the chain's legacy-first bucket (Discord left them
  in store order — accepted divergence). Group members are delivered
  **sequentially by one worker chain** — receipt k must reach a
  terminal state or a pre-claim park before k+1 starts, per the
  deliverOutcome contract in 3b — because ordered trigger issuance
  alone proves nothing under concurrent delivery goroutines.
- **S6 — Live ordering (DOC:166-171; GW:2739-2767).** The delivery
  worker takes `dgser` before correlation and holds it through
  finalize for every *result-bearing* message: bot-authored AND
  `metadata.event_type == "gc_delegation_result"`. (Discord serializes
  every referenced bot message because it cannot classify before a
  fetch; Slack's metadata gate is load-bearing for claim admission —
  Phase 2 — so only metadata-bearing messages can claim, and gating
  the lock on it is sound.)
- **S7 — Parked correlation references with bounded backoff (rule 17;
  GW:44-46, 1603-1695, 2961-2998).** A receipt parked
  `correlation_pending` (a result whose tuple has a plausibly
  in-flight `posting` intent) follows the reference schedule exactly:
  the park is immediately eligible on the next pass (no initial
  delay, GW:1603-1613); a redrive that re-ran `resolveResultWake` and
  found the posting intent STILL in flight counts one attempt and
  schedules the next at `min(60s * 2^(n-1), 15min)` where n is the
  failure count — five waits (60/120/240/480/900) — and the receipt
  goes **terminal** when the attempt count reaches 6: `Status:
  failed`, `Reason: "correlation_recovery_exhausted"` (Discord's
  `recovery_failed`), counted in `deliveryFailures`. Correlation-layer
  I/O or scan ERRORS never consume the budget: they park under the
  distinct reason `correlation_error` with plain every-sweep cadence.
  `recoverPending` applies the same `RecoveryNextAt` eligibility as
  `sweepEligible` (an adapter restart neither bypasses the backoff nor
  bumps attempts). `parkWithReason`'s idempotent early-return must
  still commit the attempts++/next-at update for the recovery reasons
  (explicit edit). A redrive that finds the record materialized
  (Python's lazy reconciliation ran) claims and delivers normally and
  clears the recovery fields.
  **Deviation D5 (normative):** the Slack-born
  `ambiguous_pending_delegations` park (no Discord analog — Discord's
  reply reference is unique) uses the same backoff schedule but
  **never goes terminal**: after 6 attempts it keeps retrying at the
  15-minute cap. Ambiguity means the one-pending-per-tuple invariant
  was breached; terminalizing would silently drop a trusted result,
  while the non-terminal park stays visible on `/healthz` and is
  operator-resolvable (`delegate --cancel`, or `company-redrive` after
  repair). The retention sweep never deletes non-terminal receipts
  (Phase 1b contract), so correlation/ambiguity parks are exempt from
  the 7-day retention by construction; the resulting
  unbounded-but-healthz-visible accumulation for never-resolved
  ambiguity is the accepted tradeoff (noted in Deferred).
  `author_resolution_pending` and `parked_no_directory_room` keep
  their Phase 1/2 every-sweep cadence (no backoff), as on Discord.
- **S8 — One-hop enforcement (rule 9; GW:1174; DOC:195-196;
  discord delegate help.md:10-12; discord_chat_delegate.py:83-84).**
  Strict reference parity: `run_delegate` proceeds only when the
  resolved turn's kind is `ambient` or `targeted` (the human-rooted
  kinds — Discord requires `discord_human_message` context) and
  hard-errors on `peer_delegation`, `peer_input`, and `peer_result`
  alike. A delegated recipient may `reply-current` a result but may
  not redelegate; a requester issues every intended sibling delegation
  from its human-rooted turn(s) *before* waiting — a frozen ready
  snapshot cannot account for a delegation created later
  (discord-v0.template.md:68-72), and delegating from a result turn
  would enable a second synthesis to the same root. Bot-rooted
  delegation chains (delegating from a `peer_input` turn whose derived
  root is a bot message) are likewise excluded. The rendered reminder
  for every peer kind gains the line `peer_redelegation: forbidden`,
  now consistent with the verb-level gate.
- **S9 — Tri-state correlation reads (rule 18; CM:2147-2160;
  GW:2919-2959).** Already the Phase 2 posture (parse-failure =
  not-live, never claimable, never rewritten); Phase 3 extends it to
  snapshot reads: an unparseable or invalid stored snapshot is
  *unavailable* (S10), never an error that blocks delivery, and never
  rewritten by replay.
- **S10 — Snapshot validation (CM:2401-2476).** Both languages ship
  the same normalizer: a stored snapshot is *available* iff
  `synthesis_state_version == 1`, `synthesis_state_available == true`,
  all three counts are non-negative ints with `responded + pending ==
  compatible` and `compatible > 0`, the pending list length equals
  `pending_delegation_count` with unique non-empty `delegation_ts`,
  `synthesis_snapshot_at` is non-empty, and the stored
  `synthesis_ready` equals the recomputed `compatible > 0 && responded
  == compatible && pending == 0`. Anything else normalizes to the
  unavailable shape (version 0, zero counts, empty list, ready false).
  Normalization is STRICT and identical across languages: no whitespace
  trimming anywhere, and `synthesis_state_version` plus all three
  counts must be exact JSON integers — bool, float (including `1.0`),
  and numeric-string forms are invalid on both sides (Python note:
  `bool` is an `int` subclass and must be explicitly excluded). The
  envelope and the Python gate consume only the normalized form.

### Synthesis envelope and the reply-current gate

`renderCompanyReminder` (company_hydration.go:185-231) gains, for
`kind == peer_result` only, the Discord synthesis block rendered from
the receipt's frozen `Synthesis` bytes (GW:1177-1200):

```
synthesis_state_version: 1
synthesis_state_available: true
compatible_delegation_count: 2
responded_delegation_count: 2
pending_delegation_count: 0
pending_delegations_json: []
synthesis_ready: true
synthesis_ready_meaning: all_currently_materialized_compatible_delegations_have_durably_claimed_slack_results
synthesis_ready_is_local_delivery_success: false
```

Every interpolated value passes `neutralizeMarkupBoundaries`; the
snapshot is normalized (S10) before rendering, so a malformed frozen
blob renders the unavailable shape rather than failing delivery.

`post_peer_synthesis` (slack_company_outbound.py:1648) gains the gate:
it loads the record at `turn["delegation_key"]`, normalizes its stored
snapshot (S10), and

- *available and ready* → proceed;
- *available and not ready* → hard error (exit 1) listing each pending
  delegation (`expected_responder_agent`, `delegation_ts`) and the two
  remedies — wait for the remaining sibling result wake, or
  `gc slack delegate --cancel --to <agent>` for a dead sibling (which
  expires it out of the compatible set, so the *next* claim freezes
  ready) — unless the new flag `--allow-partial` is passed, which
  proceeds and records `"allow_partial": true` in the report;
- *unavailable* (legacy/malformed/pruned record) → proceed with a
  stderr warning (never wedge on cosmetic-state corruption).

**Deviation D2 (normative):** Discord's "synthesize only on
`synthesis_ready: true`" is prompt-level instruction in the envelope
template (context-map rule 11, TPL:68-75). Slack enforces it at the
verb because Python has local read access to the frozen record —
Discord's CLI did not re-read the snapshot at reply time. The
`--allow-partial` escape preserves agent agency for deliberately
partial synthesis. Note the composed Phase 2 behavior: the pointer file
is overwritten by each newer wake, so an agent acting on a stale
`--origin-ts` pinned turn sees that claim's frozen (not-ready)
snapshot and is correctly refused; the final sibling's wake carries the
ready snapshot.

### Redrive parity (`retry-peer-fanout` → `company-redrive`)

What Discord's `retry-peer-fanout` provides (commands/retry-peer-fanout/
help.md; CM:6399-6491): operator redrive of failed peer targets from a
saved publish record, without reposting to the provider, reusing the
same deterministic idempotency keys, with target selection. On Slack
the *automatic* half already exists structurally — frozen targets,
same-key sweep retries, bounded attempts (Phase 1d/2c) — so Phase 3
adds only the operator half, receipt-native:

```text
gc slack company-status [--receipt <id>] [--origin <team>:<channel>:<ts>] [--root <team>:<channel>:<root_ts>]
gc slack company-redrive (--receipt <id> | --origin <team>:<channel>:<ts>) [--target <session>]... [--include-failed]
```

The legacy `gc slack retry-peer-fanout` (extmsg audit-event path) is
untouched; overloading its name onto receipt semantics would conflate
two delivery systems, hence the new verb names.

Ownership: the receipt store has exactly one writer (Go, generation
CAS), so Python must not rewrite receipts. The adapter's internal
listener (main.go internal mux / `LISTEN_INTERNAL` / `serviceSocket`
UDS) gains two company endpoints, and the Python verbs are thin
clients resolving the same env vars — the same pattern as Discord's
CLI operating on state its own process family owned:

- `GET /internal/company/receipts?origin=…|root=…|status=…` — bounded
  JSON listing: receipt id/origin/status/reason/recovery fields,
  per-target state (session, kind, status, attempts, detail,
  delegation_key), ack state.
- `POST /internal/company/redrive` body `{"origin": {…}, "targets":
  ["s1", …], "include_failed": true|false}` — under the receipt's
  single-flight + generation commit (single-flight held elsewhere →
  409, the verb retries). The `receipt` id, when supplied, is
  pattern-validated before any path use (400 otherwise). Two legs:
  (1) receipts **with frozen targets**: selected targets (default:
  all **bound** `failed` targets; `--target` filters;
  `--include-failed` is required to touch a target whose Detail
  begins `attempts_exhausted`) are reset to `pending` with
  `Attempts: 0`, `Detail: "operator_redrive"`, the **same recorded
  IdempotencyKey** (never re-derived), receipt status back to
  `routing`, recovery backoff fields cleared, then `triggerDelivery`.
  A `failed` target frozen **unbound** (`Session == ""`) is
  re-resolved at redrive time from its recorded `Agent` name against
  the CURRENT bindings snapshot — the binding-repair recovery path;
  on success it gains its session and a normally-derived
  IdempotencyKey; still-unbound targets are reported as unresolvable
  in the response. An empty reset that leaves
  recoverable-but-unbound targets is an explicit 422
  (`reason: "unresolved_targets"`) — never a success-shaped empty
  reset hiding lost data; benign empties (all delivered, `--target`
  miss, `attempts_exhausted` without `--include-failed`) remain 200
  with an empty reset so status checks do not error.
  (2) receipts **with no recorded targets** (the
  `correlation_recovery_exhausted` / parked states this verb is the
  designated recovery for): reset Status to `received`, clear Reason
  and the recovery fields, then `triggerDelivery` so first-routing
  resolution re-runs from correlation. Terminal-and-swept receipts
  (past 7-day retention) are gone; the endpoint returns 404 and the
  verb says so.

`company-status` additionally assembles Python-owned reads (delegation
records grouped by synthesis group with normalized snapshots, stale
`posting` intents) so one command shows a wedged flow end to end.
Help text carries the Discord caveat forward: an
`attempts_exhausted` target's earlier timeouts had unknown outcomes and
gc's idempotency cache is best-effort — check the target session
transcript before `--include-failed` (Discord help.md:9-16).

### Visible-ack reactions (config-gated)

Actor: the **switchboard** token — it owns admission and the receipt
lifecycle, `reactions:write` is already in its manifest
(manifest/app.json:34), and per-agent tokens must not react to
messages their agent never saw. Gate: env var
`SLACK_COMPANY_VISIBLE_ACKS` (unset/empty/`0` = off, default; anything
else = on), resolved in `main.go` alongside the other company env
overrides. All ack traffic is best-effort and asynchronous — it runs
inside the delivery worker, never the HTTP handler (the 3-second ack
budget is for admission), never changes receipt status, and never
counts as delivery failure.

Lifecycle hooks in `deliverReceipt`, keyed off the durable `AckState`
cursor so redrives are idempotent:

1. First delivery attempt for a receipt with `AckState == ""` →
   `reactions.add eyes` on the origin message; success or
   `already_reacted` → `AckState: "eyes"`.
2. Terminal `delivered` → `reactions.remove eyes` (best-effort) +
   `reactions.add white_check_mark`; → `AckState: "done"`.
3. Terminal `failed` → the concise switchboard reply into the
   message's thread root posts **exactly once** (`thread_ts =
   deriveHumanRootTS`, entity-escaped, no live mentions, body exactly
   `delivery failed for receipt <id>`), committed as the durable
   intermediate cursor `AckState: "warned"` in the same receipt
   update; then `reactions.remove eyes` (best-effort) +
   `reactions.add warning` advance to `AckState: "done"`. Sweep-heal
   of a `"warned"` receipt retries ONLY the reactions — a
   rate-limited ⚠️ can never re-post the reply.
4. Terminal `no_delivery` → `reactions.remove eyes` only (a green
   check on a message that woke nobody would misreport); →
   `AckState: "done"`.

Error taxonomy (normative): `already_reacted` (and `no_reaction` on
remove) = success; `too_many_emoji`, `too_many_reactions` =
**silent degradation** — `AckState: "degraded"`, one log line, no
further ack calls for this receipt; `ratelimited` and transient
HTTP/network errors leave `AckState` unchanged; any other definitive
error degrades. Because terminal receipts are never redriven, stranded
terminal acks are **sweep-healed**: each sweep's retention scan runs
`applyTerminalAck` for terminal receipts whose `AckState` is `"eyes"`
or `"warned"` (bounded, within retention; the `"warned"` cursor makes
the failure reply once-only) — `reactions.remove` is Tier 2 (20+/min),
tighter than add's Tier 3, so rate-limited terminal acks are the
expected case, not a corner. A crash between the finalize commit and
the ack commit still leaves an at-most-once window (👀 may persist
briefly until the next sweep) — acceptable under the cosmetic
contract. Ack commits go through the normal generation-bumping receipt
Update and therefore refresh `UpdatedAt` (they extend the stale-claim
window `sweepEligible` reads — documented, accepted). Implementation
reuses the existing reactions client behind the internal `/react`
endpoint rather than adding a second Slack reactions POST path. The
emoji are pinned: `eyes`, `white_check_mark`, `warning`. The durable
receipt, not the emoji, stays authoritative; no `/healthz` surface is
added for acks (they are cosmetic by contract).

### `/healthz` additions

One line: `company_correlation_parked=<n>` — receipts currently parked
with reason `correlation_pending` or `ambiguous_pending_delegations`
(computed in the existing sweep scan). The Phase 1 test pinning the
`/healthz` shape is updated in the same change.

### Sub-phase decomposition

3a and the ack half of 3b are independently implementable after Phase
2; 3b's ordering work builds on 3a's snapshot reads; 3c's verbs need
3b's endpoints.

#### 3a — Go synthesis core (`adapter/company_synthesis.go` + `_test.go`; edits to `company_peer.go`, `company_delivery.go`, `company_hydration.go`, `ingress_receipts.go`)

```go
// company_synthesis.go
type companySynthesisGroup struct {
    TeamID, ChannelID, ThreadRootTS, RequesterBotUserID, RequesterSession string
}
func synthesisGroupOf(r *companyDelegationRecord) (companySynthesisGroup, bool)
func (gk companySynthesisGroup) lockName() string            // "dgroup", 5 pinned fields
func synthesisFallbackLockName(t companyDelegationTuple) string // "dgroup", "unavailable" + tuple
func rootSerialLockName(teamID, channelID, threadRootTS string) string // "dgser"

type companyPendingDelegation struct {
    DelegationTS               string `json:"delegation_ts"`
    DelegationKey              string `json:"delegation_key"`
    ExpectedResponderAgent     string `json:"expected_responder_agent"`
    ExpectedResponderBotUserID string `json:"expected_responder_bot_user_id"`
}
type companySynthesisSnapshot struct {
    Version    int                        `json:"synthesis_state_version"`
    Available  bool                       `json:"synthesis_state_available"`
    Compatible int                        `json:"compatible_delegation_count"`
    Responded  int                        `json:"responded_delegation_count"`
    Pending    int                        `json:"pending_delegation_count"`
    PendingIDs []companyPendingDelegation `json:"pending_delegations"`
    Ready      bool                       `json:"synthesis_ready"`
    SnapshotAt string                     `json:"synthesis_snapshot_at"`
}
// computeSynthesisSnapshot scans delegationsDir for the group's compatible
// records (S2) with the current record substituted in, and freezes S3.
func (env companyPeerEnv) computeSynthesisSnapshot(current *companyDelegationRecord, filename, snapshotAt string) companySynthesisSnapshot
// normalizeSynthesisState is the S10 validator over raw record JSON.
func normalizeSynthesisState(raw map[string]json.RawMessage) companySynthesisSnapshot
```

Edits: `resolveResultWake` acquires `dgroup` (or the fallback name)
before `dtuple` with the preflight/re-check dance (S1) and applies the
S4 replay window; `claimRecord` grows a snapshot parameter merged into
the one atomic rewrite; `peerWake`/`frozenWake` carry `Snapshot
*companySynthesisSnapshot` (non-nil iff peer_result); `deliverReceipt`
stores the marshaled snapshot into `IngressReceipt.Synthesis` in the
same routing commit that freezes targets, takes `dgser` around the
whole result-bearing path (S6), and `renderCompanyReminder` renders the
synthesis block for peer_result from the frozen bytes.

Tests (3a): two serialized sibling claims freeze not-ready(1 pending) /
ready in order and a replayed first claim returns its stored snapshot
unchanged (S1/S3/S4, mirroring DOC:158-163); compatible-set membership
table — expired-out, window-out (−300s/TTL), different group, corrupt
record, claimed sibling counts responded (S2); snapshot validator
table ported case-for-case from CM:2401-2476 (bad version, count
mismatch, duplicate pending ids, stored_ready contradiction, non-int
counts); replay-window expiry demotes a stale replay to peer_input;
frozen `Synthesis` bytes identical across redrives; envelope renders
ready/not-ready/unavailable shapes; lock-order assertion test (no
path holds `dtuple` while acquiring `dgroup`); `--race` claim storm on
one group.

#### 3b — Go ordering, backoff, redrive surface, acks (`adapter/company_replay.go`, `company_acks.go`, `company_admin.go` + `_test.go` each; edits to `company_delivery.go`, `main.go`)

```go
// company_replay.go
type replayChain struct{ Origins []ReceiptOrigin }
// orderPendingForReplay partitions pending receipts into per-root chains
// (result-bearing receipts and correlation parks, keyed by
// (team, channel, thread_root_ts)) sorted per S5; all other receipts pass
// through in store order.
func (g *companyGateway) orderPendingForReplay(pending []*IngressReceipt) (chains []replayChain, rest []*IngressReceipt)
func compareSlackTS(a, b string) int // numeric two-part compare, malformed after well-formed
// deliverOutcome is the typed result the chain sequencer needs — the
// bare void deliverReceipt cannot distinguish "safe to advance" from
// "must abort the chain remainder".
type deliverOutcome int
const (
    deliverTerminal deliverOutcome = iota // terminal status committed
    deliverParkedPreclaim                 // parked before any claim — safe to advance
    deliverPending                        // transient failure/pending target — abort chain
    deliverBusy                           // single-flight held elsewhere — abort chain
    deliverError                          // commit/pointer/hydration error — abort chain
)
func (g *companyGateway) deliverReceiptOutcome(origin ReceiptOrigin) deliverOutcome

// deliverChain delivers a chain strictly sequentially and SYNCHRONOUSLY
// (never via triggerDelivery): it advances past deliverTerminal and
// deliverParkedPreclaim, and aborts the chain remainder on
// pending/busy/error (the next sweep pass rebuilds and resumes).
// A per-root in-process ownership registry (map keyed by
// (team, channel, thread_root_ts)) guarantees one active chain per
// root: a sweep pass or live trigger for an owned root enqueues the
// origin into the active chain instead of racing it, and live
// result-bearing triggers route through the owner when one exists.
func (g *companyGateway) deliverChain(c replayChain)

// backoff (S7)
const (
    companyPeerRecoveryMaxAttempts = 6
    companyPeerRecoveryBaseDelay   = 60 * time.Second
    companyPeerRecoveryMaxDelay    = 15 * time.Minute
    companyReasonRecoveryExhausted = "correlation_recovery_exhausted"
)
func recoveryDue(r *IngressReceipt, now time.Time) bool
func nextRecoveryDelay(attempts int) time.Duration

// company_acks.go
func (g *companyGateway) applyAdmissionAck(r *IngressReceipt)  // ""→eyes
func (g *companyGateway) applyTerminalAck(r *IngressReceipt)   // eyes→check/warn/remove
func slackReact(client *http.Client, token, method, channel, ts, name string) ackOutcome

// company_admin.go — internal mux only
func (g *companyGateway) handleCompanyReceipts(w http.ResponseWriter, r *http.Request)
func (g *companyGateway) handleCompanyRedrive(w http.ResponseWriter, r *http.Request)
```

Edits: `recoverPending` and `sweepOnce` route through
`orderPendingForReplay` (chains sequential, rest concurrent as today);
`parkWithReason` increments/schedules recovery fields for the two
correlation reasons and applies the S7 terminal transition;
`sweepEligible` respects `RecoveryNextAt`; `deliverReceipt` invokes the
ack hooks under the config gate; `main.go` wires
`SLACK_COMPANY_VISIBLE_ACKS`, the two internal routes, and the
`/healthz` line.

Tests (3b): shuffled-discovery restart replays one group
legacy-first then by (responded, snapshot_at) while an unrelated root
keeps store order (S5, mirroring GW:2070-2097); chain sequentiality
(sibling B's POST observed only after A terminal); live two-result race
under `dgser` delivers in claim order; correlation park backoff
schedule per S7 — immediate first eligibility, five waits
60/120/240/480/900, terminal `correlation_recovery_exhausted` at
attempt 6 (the 15-min cap continues indefinitely only for D5's
ambiguous park); attempts are scoped per recovery reason — a reason
transition resets the counter, and `correlation_error` never counts;
park resolved on attempt 3
claims and delivers exactly once and clears recovery fields; ambiguous
park never terminal, capped cadence (D5); redrive endpoint resets only
selected targets, preserves recorded IdempotencyKey byte-for-byte,
`attempts_exhausted` requires `include_failed`, 404 past retention;
ack lifecycle: gate off = zero Slack calls; 👀→✅, 👀→⚠️+threaded
receipt-id reply, no_delivery removes only; `already_reacted` =
success; `too_many_emoji` degrades silently and permanently; ack
failure never blocks or fails delivery; `compareSlackTS` table.

#### 3c — Python gates, verbs, interop + acceptance proofs (`scripts/slack_company_outbound.py`, new `scripts/slack_company_admin.py`, `scripts/slack_chat_reply_current.py`; command wrappers `company-status`, `company-redrive` (3-file convention); `tests/…`; fixtures)

```python
# slack_company_outbound.py
def synthesis_group(record: dict) -> tuple[str, ...] | None      # 5-tuple or None
def synthesis_state(record: dict) -> dict                        # S10 normalizer
def dgroup_lock_name(*fields: str) -> str                        # parity only, never taken by Python
# post_peer_synthesis(...) gains allow_partial: bool = False and the D2 gate
# run_delegate(...) gains the S8 one-hop error

# slack_company_admin.py
# The internal listener is a UDS (GC_SERVICE_SOCKET, primary
# proxy_process mode — LISTEN_INTERNAL is ignored when it is set) or a
# loopback TCP address. A base-URL string cannot express the UDS case,
# so the client is a connection factory: an http.client.HTTPConnection
# subclass dialing GC_SERVICE_SOCKET when set, else TCP to
# LISTEN_INTERNAL (default 127.0.0.1:8766). GC_SERVICE_SOCKET wins.
def internal_connection() -> http.client.HTTPConnection
def cmd_company_status(argv) -> int
def cmd_company_redrive(argv) -> int
```

`reply-current` passes `--allow-partial` through to
`post_peer_synthesis`. Fixture additions under
`tests/fixtures/company/`: a claimed delegation record with frozen
snapshot (golden bytes both suites re-derive), a not-ready snapshot, an
invalid-snapshot record normalizing to unavailable, and lock-filename
parity pins for `dgroup` (5-field and fallback keys) and `dgser` —
both languages must derive identical `.lock` names, verified like
Phase 2d's `dtuple-3a4b34ac4caada68.lock`. Interop: the Go suite
claims (freezing a snapshot) over records generated by the real Python
paths; the Python suite validates and gates on those exact bytes.

Tests (3c): gate table — ready posts, not-ready exits 1 listing
pending siblings, `--allow-partial` posts with the report flag,
unavailable warns and posts; cancel-then-ready flow (cancel a dead
sibling, next claim freezes ready — proven against Go-claimed bytes in
interop); one-hop matrix per S8: delegate errors on `peer_delegation`,
`peer_input`, AND `peer_result` turns alike, proceeds only on
`ambient`/`targeted`;
`company-status` renders groups/snapshots/parks/stale intents from
mixed fixtures; `company-redrive` client (HTTP mocked) passes
targets/include_failed and surfaces 404; `parse_delegation`
passes snapshot fields through unmodified (additive-field tolerance).

**Acceptance proofs (automated; the Phase 3 acceptance gate,
mirroring DOC:239-257).** The phase is done only when these pass:

1. Sibling freeze: requester delegates to two responders; concurrent
   result claims serialize under `dgroup`; first snapshot not-ready
   with one pending sibling, second ready; replay of either rewrites
   neither (DOC:158-163).
2. Restart ordering: kill the adapter mid-flight with both results
   claimed and undelivered, shuffle delegation-file discovery order;
   the requester observes the two peer_result wakes in snapshot order,
   and durable ingress idempotency absorbs a repeated replay
   (DOC:181-187).
3. Live ordering: two results arriving concurrently deliver to the
   requester in claim order under `dgser` (DOC:166-171).
4. Correlation park: a result racing its delegation's `posting` intent
   parks `correlation_pending`, claims exactly once after
   reconciliation, and an unresolvable park terminalizes after 6
   backed-off attempts (rule 17).
5. Synthesis gate: reply-current's synthesis leg refuses on not-ready
   listing the pending sibling; after `delegate --cancel` of that
   sibling the next claim is ready and synthesis posts to the human
   root with no live mentions.
6. One-hop: `delegate` is rejected from `peer_delegation`,
   `peer_input`, and `peer_result` turns alike and proceeds from
   `ambient`/`targeted`; every peer reminder carries
   `peer_redelegation: forbidden`.
7. Acks: everything above runs identically with acks off (default);
   with acks on, 👀/✅/⚠️ follow the lifecycle, `already_reacted` is
   success, `too_many_emoji` degrades silently, and no ack outcome
   changes any receipt or delegation state.
8. Redrive: an `attempts_exhausted` target redriven via
   `company-redrive --include-failed` delivers once with the original
   Idempotency-Key, and a target-less `correlation_recovery_exhausted`
   receipt redriven via the same verb re-enters correlation and
   delivers.
9. Replay window: a claim replay inside the 7-day window returns the
   stored claim unchanged; the same replay past the window delivers as
   ordinary peer input and rewrites nothing (the S4 amendment of
   Phase 2 behavior, proven at the gate so it cannot be silently
   dropped).

The live pilot proof is Discord's, on Slack: one human request, two
visible sibling delegations, two visible results, one synthesis to the
human root — every wake exactly once, restart in the middle of the
result phase, and no credential, GitHub, infrastructure, or mail
mutation (DOC:253-257).

### Deferred from Phase 3

- A durable ordered per-root delivery queue replacing the `dgser`
  advisory lock and replay chains (required before raising concurrency
  or delivery latency — DOC:168-171 carries the same production
  boundary).
- Cross-root ordering guarantees (unrelated roots stay
  discovery-ordered, as on Discord).
- Ack re-assertion after a human removes the switchboard's reaction,
  per-agent ack identities, and any ack observability surface.
- Synthesis groups spanning multiple rooms or requester sessions
  (out of contract by construction of the group tuple).
- Automatic un-terminalizing of `correlation_recovery_exhausted`
  receipts (operator `company-redrive` is the recovery path).
- Bounding the accumulation of never-resolved
  `ambiguous_pending_delegations` parks (exempt from retention by the
  non-terminal rule; visible on `/healthz`; operator-resolvable — an
  automatic bound would reintroduce the silent drop D5 exists to
  prevent).
- The ledger-backed exactly-once upgrade (Phase 5) and per-agent DMs
  (Phase 4) are unchanged by this phase.

## Phase 4 — per-agent DMs

`message.im` + `im:history` per agent identity app against the shared
events endpoint. Signature verification binds the claimed app: resolve the
app record by `(team_id, api_app_id)` and verify the HMAC against that
record's secret, failing closed on mismatch (trial-HMAC fallback only for
legacy single-app installs). DM admission into the same receipt store with
`(team_id, dm_channel_id, ts)` keys; allowed-human policy; DM-bound
singleton sessions; bot-authored DMs deliver nothing; acceptance rule 12.

DM session binding should ride the extmsg fabric's `dm` conversation
primitives rather than a pack-local registry: a `ConversationRef{Kind:
dm}` bound 1:1 to the agent's session is an exact fit for the fabric's
one-active-binding-per-conversation model and gives durable,
restart-surviving bindings via the controller-owned bead store
(`engdocs/design/external-messaging-fabric.md` in gascity core; the
fabric's Phase-1 single-writer rule means the pack drives this through
the typed API only). Decide at Phase 4 design time; the room-side
deterministic wake set stays pack-owned regardless — the fabric's
member-fanout and group/launcher routing must NOT be adopted for rooms
(all-members fanout with mention annotation is the prompt-trusted
suppression model this design replaced).

## Phase 5 — ledger integration

When the gascity durable-request-ledger lands: swap local admission for
`POST /v0/city/{city}/extmsg/request-receipts` + receipt-lookup recovery,
implement the projector-callback UDS contract and publish-intent outbox
against the ledger API, adopt the cross-repo wire fixture. This is an
explicit state-mapping exercise — the adapter journal adopts the Slack
companion's six states (`spooled`, `admitting`, `core_bound`,
`body_redacted`, `rejected`, `quarantined`; `core_bound` is the
redaction fence — see phase5-ledger-readiness.md, which supersedes the
earlier three-state note and records the 15-route surface, the
no-If-Match header protocol, and the rule that DTOs are generated from
the Slice 20 OpenAPI artifact, never hand-written) — and the ledger's
receipts never carry raw bodies, so the inner-event payload moves to a
spool-side body store (shipped ahead of the ledger as the Phase 5
readiness increment). The Phase 1 origin-key discipline and
ordering are chosen so the swap does not change pack-observable routing
semantics; true end-to-end exactly-once delivery arrives here.

Transport note (researched 2026-07-17, gascity core): the supervisor's
typed control plane is HTTP+SSE only — its two websocket surfaces
(t3bridge runtime client; opaque `/svc/*` proxy passthrough) reach
neither orders nor conversations, and orders are named trigger→action
definitions, the wrong altitude for per-wake delivery. The one
pre-ledger hardening candidate is migrating company wakes from
`POST /v0/city/{city}/session/{id}/messages` (202-queued, 30-min
in-memory idempotency) to `POST /v0/city/{city}/extmsg/inbound`
(durable bead-backed transcript persisted before session read, typed
receipt with transcript entry id, principled 4xx-permanent /
5xx-retryable contract). GATE: extmsg's inbound `DedupKey` is
currently a no-op in core (type-defined, never consumed —
`internal/extmsg/types.go`), so gc-side redelivery dedup is not real
yet and a retried POST would duplicate the turn; do not migrate until
a DedupKey consumer lands in core or the ledger supplies it. Also
worth borrowing at ledger time: the orders tracking-bead pattern
(synchronous durable marker before dispatch, label-keyed dedup) as
the request-ledger row template.
