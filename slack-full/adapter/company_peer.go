package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// company_peer.go — the Go ingress half of the delegation/result contract
// (Phase 2c). Python owns creation of intents and delegation records; Go owns
// the five-condition trust checklist, result correlation, and the
// pending -> result_claimed transition. Every cross-process critical section
// takes the delegation-tuple advisory lock (label "dtuple") and re-checks the
// record generation underneath it.

// Delegation record status values (shared with Python).
const (
	companyDelegationPending  = "pending"
	companyDelegationClaimed  = "result_claimed"
	companyDelegationExpired  = "expired"
	companyDelegationSchemaV  = 1
	companyResultEventType    = "gc_delegation_result"
	companyDelegateEventType  = "gc_delegation"
	companyRouteWindowBack    = 300 * time.Second // -300s lower bound on now-created_at
	companyIntentStatusPosted = "posting"
)

// Peer-path park reasons (fail-closed; surfaced on the parked receipt).
const (
	peerParkCorrelationPending = "correlation_pending"
	peerParkAmbiguousPending   = "ambiguous_pending_delegations"
	peerParkResolutionPending  = "author_resolution_pending"
	// peerParkCorrelationError is the S1 fail-closed park when the synthesis
	// group cannot be pinned across the bounded re-lock loop (or on a
	// correlation-layer I/O error): every-sweep cadence, never demoted to a
	// frozen peer_input.
	peerParkCorrelationError = "correlation_error"
)

// peerWake is a refined bot-authored wake: the agent to wake, the frozen
// kind, and the correlated delegation-record filename. peer_delegation and
// peer_result ALWAYS carry a non-empty DelegationKey; peer_input (uncorrelated
// peer chatter, an unmatched reply, or a gate-failed/clarifying post) carries
// an empty DelegationKey and correlates to no record.
type peerWake struct {
	Agent         CompanyAgent
	Kind          string
	DelegationKey string
	// Snapshot is the frozen synthesis snapshot, non-nil iff Kind is
	// peer_result (a fresh claim carries the just-computed snapshot; a replay
	// hit carries the record's stored, S10-normalized snapshot). It rides up to
	// deliverReceipt, which freezes it onto the receipt in the routing commit.
	Snapshot *companySynthesisSnapshot
}

// Five-condition peer-trust checklist reasons (design "Trust and Authority").
// An empty reason means the (author, receiver) pair passes.
const (
	peerTrustOK                   = ""
	peerTrustNotCompanyRoom       = "peer_not_company_room"
	peerTrustAuthorNotCompanyBot  = "peer_author_not_company_bot"
	peerTrustAuthorIsReceiver     = "peer_author_is_receiver"
	peerTrustReceiverNotMentioned = "peer_receiver_not_mentioned"
	peerTrustReceiverNotMember    = "peer_receiver_not_member"
	peerTrustReceiverNotEligible  = "peer_receiver_not_eligible"
	peerTrustReceiverUnbound      = "peer_receiver_unbound"
)

// evaluatePeerTrust applies the five-condition checklist for one
// (author, receiver) peer pair and returns a machine-readable failure reason
// (peerTrustOK on success). It is defense-in-depth over ComputeWakeSet's
// routing filter and is the single place the checklist is spelled out:
//
//  1. team+channel is an imported company room (room != nil).
//  2. the author resolves to a registered company agent (author != nil).
//  3. the author is not the receiver.
//  4. the receiver's bot_user_id is in the message's native mention set.
//  5. both are room members, the receiver is mention-eligible, and the
//     (room, receiver) pair has exactly one company binding.
func evaluatePeerTrust(dir *CompanyDirectory, bindings *CompanyBindings, room *CompanyRoom, author, receiver *CompanyAgent, mentionSet map[string]bool) string {
	if room == nil {
		return peerTrustNotCompanyRoom
	}
	if author == nil {
		return peerTrustAuthorNotCompanyBot
	}
	if receiver == nil || receiver.BotUserID == author.BotUserID {
		return peerTrustAuthorIsReceiver
	}
	if !mentionSet[receiver.BotUserID] {
		return peerTrustReceiverNotMentioned
	}
	if !dir.IsMember(room, author.Name) || !dir.IsMember(room, receiver.Name) {
		return peerTrustReceiverNotMember
	}
	if !dir.IsMentionEligible(room, receiver.Name) {
		return peerTrustReceiverNotEligible
	}
	if _, ok := bindings.SessionFor(room.Name, receiver.Name); !ok {
		return peerTrustReceiverUnbound
	}
	return peerTrustOK
}

// companyDelegationRecord mirrors company-delegations/<key>.json. Field order
// matches the golden fixture so a parse+re-marshal round-trips byte-for-byte.
type companyDelegationRecord struct {
	SchemaVersion              int    `json:"schema_version"`
	Generation                 int64  `json:"generation"`
	Nonce                      string `json:"nonce"`
	Room                       string `json:"room"`
	TeamID                     string `json:"team_id"`
	ChannelID                  string `json:"channel_id"`
	TS                         string `json:"ts"`
	ThreadRootTS               string `json:"thread_root_ts"`
	RequesterAgent             string `json:"requester_agent"`
	RequesterBotUserID         string `json:"requester_bot_user_id"`
	RequesterSession           string `json:"requester_session"`
	ExpectedResponderAgent     string `json:"expected_responder_agent"`
	ExpectedResponderBotUserID string `json:"expected_responder_bot_user_id"`
	CreatedAt                  string `json:"created_at"`
	TTLSeconds                 int64  `json:"ttl_seconds"`
	Status                     string `json:"status"`
	ResultTS                   string `json:"result_ts"`
	ResultClaimedAt            string `json:"result_claimed_at"`
}

// companyIntentRecord mirrors company-delegation-intents/<nonce>.json. Go
// only reads intents (Python owns writes); the type exists for correlation
// and the /healthz stale-intent count, and its field order matches the golden
// fixture for parity.
type companyIntentRecord struct {
	SchemaVersion    int    `json:"schema_version"`
	Nonce            string `json:"nonce"`
	RetrySeq         int    `json:"retry_seq"`
	Status           string `json:"status"`
	Attempts         int    `json:"attempts"`
	MaxAttempts      int    `json:"max_attempts"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
	RetryDeadline    string `json:"retry_deadline"`
	TTLSeconds       int64  `json:"ttl_seconds"`
	SourceAgent      string `json:"source_agent"`
	SourceAppID      string `json:"source_app_id"`
	SourceBotUserID  string `json:"source_bot_user_id"`
	TargetAgent      string `json:"target_agent"`
	TargetBotUserID  string `json:"target_bot_user_id"`
	TeamID           string `json:"team_id"`
	ChannelID        string `json:"channel_id"`
	Room             string `json:"room"`
	HumanRootTS      string `json:"human_root_ts"`
	RequesterSession string `json:"requester_session"`
	BodySHA256       string `json:"body_sha256"`
	PostedTS         string `json:"posted_ts"`
}

// slackMessageMetadata is the Slack message metadata envelope carried on
// delegation / result posts.
type slackMessageMetadata struct {
	EventType    string          `json:"event_type"`
	EventPayload json.RawMessage `json:"event_payload"`
}

// gcResultPayload is the event_payload of a gc_delegation_result message.
type gcResultPayload struct {
	V            int    `json:"v"`
	Nonce        string `json:"nonce"`
	DelegationTS string `json:"delegation_ts"`
}

// companyParseDelegation decodes and fail-closed-validates a delegation
// record, mirroring Python's parse_delegation so both sides reject the same
// corrupt records: schema_version, generation >= 1, every required tuple/
// identity string non-empty, a known status, and a non-negative ttl. A record
// that fails validation is treated as not-live (never claimable, never
// rewritten), same as Python raising OutboundError.
func companyParseDelegation(data []byte) (*companyDelegationRecord, error) {
	var r companyDelegationRecord
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("decode delegation record: %w", err)
	}
	if r.SchemaVersion != companyDelegationSchemaV {
		return nil, fmt.Errorf("delegation record: unsupported schema_version %d", r.SchemaVersion)
	}
	if r.Generation < 1 {
		return nil, fmt.Errorf("delegation record: generation must be >= 1, got %d", r.Generation)
	}
	for _, f := range []struct{ name, val string }{
		{"nonce", r.Nonce},
		{"room", r.Room},
		{"team_id", r.TeamID},
		{"channel_id", r.ChannelID},
		{"ts", r.TS},
		{"thread_root_ts", r.ThreadRootTS},
		{"requester_agent", r.RequesterAgent},
		{"requester_bot_user_id", r.RequesterBotUserID},
		{"requester_session", r.RequesterSession},
		{"expected_responder_agent", r.ExpectedResponderAgent},
		{"expected_responder_bot_user_id", r.ExpectedResponderBotUserID},
		{"created_at", r.CreatedAt},
		{"status", r.Status},
	} {
		if f.val == "" {
			return nil, fmt.Errorf("delegation record: missing required field %q", f.name)
		}
	}
	switch r.Status {
	case companyDelegationPending, companyDelegationClaimed, companyDelegationExpired:
	default:
		return nil, fmt.Errorf("delegation record: invalid status %q", r.Status)
	}
	if r.TTLSeconds < 0 {
		return nil, fmt.Errorf("delegation record: ttl_seconds must be >= 0, got %d", r.TTLSeconds)
	}
	return &r, nil
}

func companyMarshalDelegation(r *companyDelegationRecord) ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// companyRewriteDelegation overlays the field mutations in mut onto the raw
// on-disk record, preserving every key present on disk — including fields this
// Go version does not model — so a claim/expiry rewrite never silently drops an
// additive field a newer (e.g. Python) writer added. This matches Python's
// dict-passthrough rewrite semantics; a fixed-struct re-marshal would erase
// unknown keys.
func companyRewriteDelegation(raw []byte, mut map[string]any) ([]byte, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	for k, v := range mut {
		enc, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		obj[k] = enc
	}
	return json.MarshalIndent(obj, "", "  ")
}

func companyParseIntent(data []byte) (*companyIntentRecord, error) {
	var r companyIntentRecord
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("decode intent record: %w", err)
	}
	return &r, nil
}

func companyMarshalIntent(r *companyIntentRecord) ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// companyDelegationTuple is the (team, channel, thread_root_ts, responder,
// requester) key shared by the tuple lock and record correlation.
type companyDelegationTuple struct {
	TeamID             string
	ChannelID          string
	ThreadRootTS       string
	ResponderBotUserID string
	RequesterBotUserID string
}

func (t companyDelegationTuple) lockName() string {
	return companyLockFilename("dtuple",
		t.TeamID, t.ChannelID, t.ThreadRootTS, t.ResponderBotUserID, t.RequesterBotUserID)
}

func (t companyDelegationTuple) matchesRecord(r *companyDelegationRecord) bool {
	return r.TeamID == t.TeamID &&
		r.ChannelID == t.ChannelID &&
		r.ThreadRootTS == t.ThreadRootTS &&
		r.ExpectedResponderBotUserID == t.ResponderBotUserID &&
		r.RequesterBotUserID == t.RequesterBotUserID
}

// companyPeerEnv bundles the on-disk state directories the peer path reads and
// writes, plus the injectable clock.
type companyPeerEnv struct {
	delegationsDir string
	intentsDir     string
	locksDir       string
	// retention bounds S4 replay acceptance: an idempotent claim hit is honored
	// only while -300s <= now - result_claimed_at <= retention (the receipt-
	// retention constant, plumbed from the gateway's value).
	retention time.Duration
	now       func() time.Time
}

// delegationMatch pairs a parsed record with its on-disk filename.
type delegationMatch struct {
	record   *companyDelegationRecord
	filename string
}

// scanDelegations returns every delegation record whose tuple fields match t,
// regardless of status. A corrupt file is skipped, not fatal.
func (env companyPeerEnv) scanDelegations(t companyDelegationTuple) ([]delegationMatch, error) {
	entries, err := os.ReadDir(env.delegationsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []delegationMatch
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, rerr := os.ReadFile(filepath.Join(env.delegationsDir, e.Name()))
		if rerr != nil {
			continue
		}
		rec, perr := companyParseDelegation(data)
		if perr != nil {
			continue
		}
		if t.matchesRecord(rec) {
			out = append(out, delegationMatch{record: rec, filename: e.Name()})
		}
	}
	return out, nil
}

// ttlExpired reports whether a record's route TTL has elapsed relative to now.
func (env companyPeerEnv) ttlExpired(r *companyDelegationRecord) bool {
	created, err := time.Parse(time.RFC3339, r.CreatedAt)
	if err != nil {
		return false // unparseable timestamps are treated as live, fail-open on staleness only
	}
	age := env.now().Sub(created)
	return age > time.Duration(r.TTLSeconds)*time.Second
}

// withinRouteWindow reports whether now is within [-300s, ttl] of created_at.
func (env companyPeerEnv) withinRouteWindow(r *companyDelegationRecord) bool {
	created, err := time.Parse(time.RFC3339, r.CreatedAt)
	if err != nil {
		return false
	}
	delta := env.now().Sub(created)
	return delta >= -companyRouteWindowBack && delta <= time.Duration(r.TTLSeconds)*time.Second
}

// postingIntentExists reports whether a plausibly in-flight posting intent
// exists for the tuple: the requester (source) delegating to the responder
// (target) on this human root. Read-only; a corrupt file is skipped.
func (env companyPeerEnv) postingIntentExists(t companyDelegationTuple) (bool, error) {
	entries, err := os.ReadDir(env.intentsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, rerr := os.ReadFile(filepath.Join(env.intentsDir, e.Name()))
		if rerr != nil {
			continue
		}
		rec, perr := companyParseIntent(data)
		if perr != nil {
			continue
		}
		if rec.Status == companyIntentStatusPosted &&
			rec.TeamID == t.TeamID && rec.ChannelID == t.ChannelID &&
			rec.HumanRootTS == t.ThreadRootTS &&
			rec.SourceBotUserID == t.RequesterBotUserID &&
			rec.TargetBotUserID == t.ResponderBotUserID {
			return true, nil
		}
	}
	return false, nil
}

// countStalePostingIntents returns the number of intents stuck in "posting"
// past their retry_deadline — the operator signal surfaced on /healthz.
func (env companyPeerEnv) countStalePostingIntents() int {
	entries, err := os.ReadDir(env.intentsDir)
	if err != nil {
		return 0
	}
	now := env.now()
	n := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, rerr := os.ReadFile(filepath.Join(env.intentsDir, e.Name()))
		if rerr != nil {
			continue
		}
		rec, perr := companyParseIntent(data)
		if perr != nil || rec.Status != companyIntentStatusPosted {
			continue
		}
		deadline, derr := time.Parse(time.RFC3339, rec.RetryDeadline)
		if derr != nil {
			continue
		}
		if now.After(deadline) {
			n++
		}
	}
	return n
}

// resolvePeerWakes refines the router's bot-authored wakes: a gc_delegation
// post wakes as peer_delegation (keyed to its own record); on a metadata-gated
// result it claims the matching delegation record and wakes the requester as
// peer_result; on a plausibly in-flight or ambiguous correlation it returns a
// fail-closed park reason (wakes nil). Everything uncorrelated — plain peer
// chatter, an unmatched reply, or a gate-failed/clarifying post — delivers as
// keyless peer_input.
func (env companyPeerEnv) resolvePeerWakes(msg CompanyMessage, author *CompanyAgent, wakes []WakeTarget) (out []peerWake, parkReason string, err error) {
	meta := parseMessageMetadata(msg.Metadata)
	derivedRoot := deriveHumanRootTS(msg)
	isResult := meta.EventType == companyResultEventType

	for _, wt := range wakes {
		requester := wt.Agent
		if !isResult {
			// A gc_delegation post correlates to its own record (keyed by its
			// posted ts) and wakes as peer_delegation; plain peer chatter
			// without gc metadata is uncorrelated peer_input (no key).
			if meta.EventType == companyDelegateEventType {
				out = append(out, peerWake{
					Agent:         requester,
					Kind:          wakeKindPeerDelegation,
					DelegationKey: companyDelegationFilename(msg.TeamID, msg.ChannelID, msg.TS),
				})
			} else {
				out = append(out, peerWake{Agent: requester, Kind: wakeKindPeerInput})
			}
			continue
		}

		tuple := companyDelegationTuple{
			TeamID:             msg.TeamID,
			ChannelID:          msg.ChannelID,
			ThreadRootTS:       derivedRoot,
			ResponderBotUserID: author.BotUserID,
			RequesterBotUserID: requester.BotUserID,
		}
		pw, park, rerr := env.resolveResultWake(msg, meta, tuple, requester)
		if rerr != nil {
			return nil, "", rerr
		}
		if park != "" {
			return nil, park, nil
		}
		out = append(out, pw)
	}
	return out, "", nil
}

// resolveResultWake handles one requester candidate for a gc_delegation_result
// message. It runs the S1 preflight/re-check dance: scan without locks to
// derive the synthesis group, acquire dgroup THEN dtuple (the pinned lock
// order — no path holds a lower-rank lock while acquiring a higher one),
// re-scan, and if the derived group changed between the two scans release both
// locks, re-derive, and retry (at most companySynthesisRelockAttempts). On
// exhaustion the receipt parks correlation_error — never demoted to a frozen
// peer_input, since a result consumed as peer_input can never claim later.
func (env companyPeerEnv) resolveResultWake(msg CompanyMessage, meta slackMessageMetadata, tuple companyDelegationTuple, requester CompanyAgent) (peerWake, string, error) {
	payload := parseResultPayload(meta.EventPayload)

	for attempt := 0; attempt < companySynthesisRelockAttempts; attempt++ {
		pre, err := env.scanDelegations(tuple)
		if err != nil {
			return peerWake{}, "", err
		}
		groupLock := resultGroupLockName(pre, payload, tuple)

		gl, err := acquireCompanyLock(env.locksDir, groupLock)
		if err != nil {
			return peerWake{}, "", err
		}
		tl, err := acquireCompanyLock(env.locksDir, tuple.lockName())
		if err != nil {
			gl.release()
			return peerWake{}, "", err
		}

		matches, err := env.scanDelegations(tuple)
		if err != nil {
			tl.release()
			gl.release()
			return peerWake{}, "", err
		}
		if resultGroupLockName(matches, payload, tuple) != groupLock {
			// A sibling materialized/vanished between the unlocked preflight and
			// the locked re-scan, changing the derived group: release both,
			// re-derive from the fresh scan, re-acquire, re-scan.
			tl.release()
			gl.release()
			continue
		}

		pw, park, rerr := env.resolveResultLocked(msg, payload, tuple, requester, matches)
		tl.release()
		gl.release()
		return pw, park, rerr
	}
	return peerWake{}, peerParkCorrelationError, nil
}

// resolveResultLocked is the correlation decision for one result under the held
// dgroup+dtuple locks: the S4 replay window, TTL pruning of stale pendings,
// and the metadata-gated claim that freezes the synthesis snapshot.
func (env companyPeerEnv) resolveResultLocked(msg CompanyMessage, payload gcResultPayload, tuple companyDelegationTuple, requester CompanyAgent, matches []delegationMatch) (peerWake, string, error) {
	// Replay (S4): a record already claimed by THIS result ts+nonce is an
	// idempotent hit, honored only while within the bounded replay window.
	for _, m := range matches {
		if m.record.Status == companyDelegationClaimed &&
			m.record.ResultTS == msg.TS &&
			m.record.Nonce == payload.Nonce {
			if !env.replayWithinWindow(m.record) {
				// Outside the window, or an unparseable result_claimed_at (S9
				// fail-closed read): ordinary peer input, never a second claim.
				return peerWake{Agent: requester, Kind: wakeKindPeerInput}, "", nil
			}
			snap := env.storedSnapshot(m.filename)
			return peerWake{Agent: requester, Kind: wakeKindPeerResult, DelegationKey: m.filename, Snapshot: &snap}, "", nil
		}
	}

	// Live pending matches (TTL-valid). Expired pendings are rewritten under
	// the lock and excluded. A claimed record for the tuple is remembered so a
	// second, different result bypasses the correlation-pending park (S4).
	var pending []delegationMatch
	claimedTuple := false
	for _, m := range matches {
		if m.record.Status == companyDelegationClaimed {
			claimedTuple = true
		}
		if m.record.Status != companyDelegationPending {
			continue
		}
		if env.ttlExpired(m.record) {
			env.expireRecord(m) // best-effort; lock held
			continue
		}
		pending = append(pending, m)
	}

	switch len(pending) {
	case 0:
		// An already-claimed tuple bypasses the posting-intent check entirely
		// (S4): a lagging posting intent must never park a claimed tuple
		// correlation_pending. Deliver the extra result as ordinary peer input.
		if claimedTuple {
			return peerWake{Agent: requester, Kind: wakeKindPeerInput}, "", nil
		}
		exists, ierr := env.postingIntentExists(tuple)
		if ierr != nil {
			return peerWake{}, "", ierr
		}
		if exists {
			return peerWake{}, peerParkCorrelationPending, nil
		}
		// Unmatched identifiable reply: ordinary peer input, claims nothing.
		return peerWake{Agent: requester, Kind: wakeKindPeerInput}, "", nil
	case 1:
		m := pending[0]
		// Metadata gate + route window are load-bearing for claim admission.
		gated := payload.Nonce == m.record.Nonce && payload.DelegationTS == m.record.TS
		if gated && env.withinRouteWindow(m.record) {
			// Freeze the snapshot over the group with this record substituted in
			// as claimed (S3), sharing snapshotAt with result_claimed_at, then
			// persist both in the one atomic rewrite (S1).
			snapshotAt := env.now().UTC().Format(time.RFC3339)
			claimedView := *m.record
			claimedView.Status = companyDelegationClaimed
			claimedView.ResultTS = msg.TS
			claimedView.ResultClaimedAt = snapshotAt
			snap := env.computeSynthesisSnapshot(&claimedView, m.filename, snapshotAt)
			if cerr := env.claimRecord(m, msg.TS, snapshotAt, snap); cerr != nil {
				return peerWake{}, "", cerr
			}
			return peerWake{Agent: requester, Kind: wakeKindPeerResult, DelegationKey: m.filename, Snapshot: &snap}, "", nil
		}
		// Clarifying question / hand-typed / out-of-window: peer input only.
		return peerWake{Agent: requester, Kind: wakeKindPeerInput}, "", nil
	default:
		return peerWake{}, peerParkAmbiguousPending, nil
	}
}

// replayWithinWindow reports whether an already-claimed record's replay is
// still honorable: -300s <= now - result_claimed_at <= retention (S4). An
// unparseable result_claimed_at fails closed (S9) — the replay is treated as
// ordinary peer input, never rewritten.
func (env companyPeerEnv) replayWithinWindow(r *companyDelegationRecord) bool {
	claimed, err := time.Parse(time.RFC3339, r.ResultClaimedAt)
	if err != nil {
		return false
	}
	delta := env.now().Sub(claimed)
	return delta >= -companyRouteWindowBack && delta <= env.retention
}

// claimRecord transitions a pending record to result_claimed under the held
// tuple lock, generation-checked, freezing the synthesis snapshot into the SAME
// atomic rewrite (S1). Re-reads on disk to defend against a lost race even
// though the lock already serializes writers. claimedAt must equal the
// snapshot's synthesis_snapshot_at (S3).
func (env companyPeerEnv) claimRecord(m delegationMatch, resultTS, claimedAt string, snap companySynthesisSnapshot) error {
	path := filepath.Join(env.delegationsDir, m.filename)
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	onDisk, err := companyParseDelegation(data)
	if err != nil {
		return err
	}
	if onDisk.Status == companyDelegationClaimed && onDisk.ResultTS == resultTS {
		return nil // already our claim (replay)
	}
	if onDisk.Status != companyDelegationPending || onDisk.Generation != m.record.Generation {
		return fmt.Errorf("delegation %s not claimable (status=%s gen=%d)", m.filename, onDisk.Status, onDisk.Generation)
	}
	out, err := companyRewriteDelegation(data, map[string]any{
		"status":                      companyDelegationClaimed,
		"result_ts":                   resultTS,
		"result_claimed_at":           claimedAt,
		"generation":                  onDisk.Generation + 1,
		"synthesis_state_version":     snap.Version,
		"synthesis_state_available":   snap.Available,
		"compatible_delegation_count": snap.Compatible,
		"responded_delegation_count":  snap.Responded,
		"pending_delegation_count":    snap.Pending,
		"pending_delegations":         snap.PendingIDs,
		"synthesis_ready":             snap.Ready,
		"synthesis_snapshot_at":       snap.SnapshotAt,
	})
	if err != nil {
		return err
	}
	return companyWriteFileAtomic(path, out)
}

// expireRecord rewrites a TTL-expired pending record to "expired" under the
// held tuple lock. Best-effort: a failure only means the sweep / pruner will
// retry, so errors are swallowed here.
func (env companyPeerEnv) expireRecord(m delegationMatch) {
	path := filepath.Join(env.delegationsDir, m.filename)
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	onDisk, err := companyParseDelegation(data)
	if err != nil || onDisk.Status != companyDelegationPending || onDisk.Generation != m.record.Generation {
		return
	}
	out, err := companyRewriteDelegation(data, map[string]any{
		"status":     companyDelegationExpired,
		"generation": onDisk.Generation + 1,
	})
	if err != nil {
		return
	}
	_ = companyWriteFileAtomic(path, out)
}

// parseMessageMetadata tolerantly decodes the Slack message metadata object.
func parseMessageMetadata(raw json.RawMessage) slackMessageMetadata {
	var m slackMessageMetadata
	if len(raw) == 0 {
		return m
	}
	_ = json.Unmarshal(raw, &m)
	return m
}

func parseResultPayload(raw json.RawMessage) gcResultPayload {
	var p gcResultPayload
	if len(raw) == 0 {
		return p
	}
	_ = json.Unmarshal(raw, &p)
	return p
}

// deriveHumanRootTS applies the normative root-derivation rule:
// thread_ts when present, else ts.
func deriveHumanRootTS(msg CompanyMessage) string {
	if msg.ThreadTS != "" {
		return msg.ThreadTS
	}
	return msg.TS
}

// receiptRootTS returns the receipt's frozen human root, derived ONCE at admission
// (ThreadRootTS). Every root-keyed derivation goes through here so a redacted or
// missing body — which loses thread_ts and collapses deriveHumanRootTS to origin.TS
// — can no longer diverge the dgser lock name, replay-chain root, rendered
// thread_root_ts, or failure-reply root from their pre-redaction values (C7). A
// legacy receipt admitted before the field existed falls back to body-derivation
// (both shapes forever).
func receiptRootTS(r *IngressReceipt, msg CompanyMessage) string {
	if r != nil && r.ThreadRootTS != "" {
		return r.ThreadRootTS
	}
	return deriveHumanRootTS(msg)
}

// companyWriteFileAtomic writes data to path via temp + rename with an fsync
// of the file and the parent directory, matching the receipt store's
// durability discipline.
func companyWriteFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".company-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return fsyncDir(dir)
}
