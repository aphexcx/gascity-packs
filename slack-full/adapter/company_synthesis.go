package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// company_synthesis.go — the Go synthesis core (Slack company-rooms Phase 3a).
// A result claim (company_peer.go) freezes a durable snapshot of its synthesis
// group in the SAME atomic rewrite as the pending -> result_claimed transition
// (S1). The snapshot answers "are all currently-materialized compatible sibling
// delegations already answered?" for the requester's reply-current gate. Every
// schema and lock label here is a cross-language contract with the Python
// outbound side (3c); the Discord reference is discord_intake_common.py
// (CM:2250-2581).

// companySynthesisStateVersion is the wire version stamped on a computed
// snapshot and required by the S10 validator for a stored snapshot to be
// considered available.
const companySynthesisStateVersion = 1

// companySynthesisRelockAttempts bounds the S1 preflight/re-check dance: a
// claim scans without locks to derive the group, acquires dgroup+dtuple, then
// re-scans; if the derived group changed it releases both and retries, at most
// this many times before parking correlation_error.
const companySynthesisRelockAttempts = 3

// companySynthesisGroup is the canonical durable root shared by sibling
// delegations, in the pinned 5-field order
// (team_id, channel_id, thread_root_ts, requester_bot_user_id,
// requester_session). Deviation D1: Discord's group is an 8-tuple; on Slack the
// receipt id and source app are derivable and only the session NAME (not a
// stable incarnation id) is carried, an accepted, declared consequence.
type companySynthesisGroup struct {
	TeamID             string
	ChannelID          string
	ThreadRootTS       string
	RequesterBotUserID string
	RequesterSession   string
}

// synthesisGroupOf returns the group of a delegation record and whether it is
// well-formed: a record belongs to a group iff all five group fields are
// non-empty. A parseable Phase 2 record always satisfies this (the parser
// requires every field), so ok is false only defensively.
func synthesisGroupOf(r *companyDelegationRecord) (companySynthesisGroup, bool) {
	if r == nil {
		return companySynthesisGroup{}, false
	}
	// STRICT (canonical S10/group rule): no whitespace trimming anywhere — the
	// group fields are compared and hashed as-is, byte-for-byte with Python's
	// synthesis_group (which does no strip), so both sides derive identical
	// dgroup lock names for the same on-disk bytes.
	g := companySynthesisGroup{
		TeamID:             r.TeamID,
		ChannelID:          r.ChannelID,
		ThreadRootTS:       r.ThreadRootTS,
		RequesterBotUserID: r.RequesterBotUserID,
		RequesterSession:   r.RequesterSession,
	}
	if g.TeamID == "" || g.ChannelID == "" || g.ThreadRootTS == "" ||
		g.RequesterBotUserID == "" || g.RequesterSession == "" {
		return companySynthesisGroup{}, false
	}
	return g, true
}

// lockName is the dgroup advisory-lock filename over the five group fields in
// pinned order. All sibling claims for one group serialize on it.
func (gk companySynthesisGroup) lockName() string {
	return companyLockFilename("dgroup",
		gk.TeamID, gk.ChannelID, gk.ThreadRootTS, gk.RequesterBotUserID, gk.RequesterSession)
}

// synthesisFallbackLockName is the dgroup lock used on the correlation-pending
// leg, when a claim's preflight scan finds no matching record and the group is
// therefore unknown. Key = ("unavailable", team, channel, thread_root_ts,
// responder, requester), mirroring Discord's ("unavailable", delegation_id)
// fallback (CM:2511-2515).
func synthesisFallbackLockName(t companyDelegationTuple) string {
	return companyLockFilename("dgroup",
		"unavailable", t.TeamID, t.ChannelID, t.ThreadRootTS, t.ResponderBotUserID, t.RequesterBotUserID)
}

// rootSerialLockName is the dgser advisory-lock filename over
// (team_id, channel_id, thread_root_ts) — the Slack port of Discord's coarse
// per-root referenced-bot-message lock (Deviation D4). Defined here for the 3b
// live-ordering path; 3a does not hold it.
func rootSerialLockName(teamID, channelID, threadRootTS string) string {
	return companyLockFilename("dgser", teamID, channelID, threadRootTS)
}

// companyPendingDelegation is one still-pending compatible sibling recorded in
// a frozen snapshot. Identity is the posted delegation ts plus the derived
// delegation_key so readers never re-derive filenames.
type companyPendingDelegation struct {
	DelegationTS               string `json:"delegation_ts"`
	DelegationKey              string `json:"delegation_key"`
	ExpectedResponderAgent     string `json:"expected_responder_agent"`
	ExpectedResponderBotUserID string `json:"expected_responder_bot_user_id"`
}

// companySynthesisSnapshot is the additive block frozen onto a delegation
// record at claim time and copied onto the peer_result receipt. The JSON tags
// are the cross-language wire contract.
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

// resultGroupLockName derives the dgroup lock name for a result claim: the
// group of the metadata-identified record (same ts+nonce the claim gate uses),
// or the fallback key when no such record is present. Because team/channel/
// root/requester are all pinned by the tuple, the only group dimension that can
// vary across records for one tuple is requester_session, so this name is
// stable exactly while the metadata-identified record's group is — the S1
// re-check compares it across the unlocked and locked scans.
func resultGroupLockName(matches []delegationMatch, payload gcResultPayload, tuple companyDelegationTuple) string {
	for _, m := range matches {
		if m.record.TS == payload.DelegationTS && m.record.Nonce == payload.Nonce {
			if g, ok := synthesisGroupOf(m.record); ok {
				return g.lockName()
			}
		}
	}
	return synthesisFallbackLockName(tuple)
}

// computeSynthesisSnapshot scans delegationsDir for the group's compatible
// records (S2) with the current record substituted in as claimed, and freezes
// the S3 counts. current must already carry its claimed status/result fields so
// it counts as responded. snapshotAt becomes synthesis_snapshot_at
// (== result_claimed_at, S3). A vanished file during the scan is skipped
// (tolerating the concurrent Python pruner).
func (env companyPeerEnv) computeSynthesisSnapshot(current *companyDelegationRecord, filename, snapshotAt string) companySynthesisSnapshot {
	group, ok := synthesisGroupOf(current)
	if !ok {
		// A claimed record with no derivable group is unavailable-but-stamped
		// (compute path, version present), mirroring CM:2328-2338.
		return companySynthesisSnapshot{
			Version:    companySynthesisStateVersion,
			Available:  false,
			PendingIDs: []companyPendingDelegation{},
			SnapshotAt: snapshotAt,
		}
	}

	// Candidate set keyed by on-disk filename (unique per record), with the
	// current record substituted in so its in-memory claimed status wins over
	// the still-pending bytes on disk.
	type candidate struct {
		filename string
		record   *companyDelegationRecord
	}
	byFilename := map[string]candidate{}
	if entries, err := os.ReadDir(env.delegationsDir); err == nil {
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
			byFilename[e.Name()] = candidate{filename: e.Name(), record: rec}
		}
	}
	byFilename[filename] = candidate{filename: filename, record: current}

	now := env.now()
	var compatible []candidate
	for _, c := range byFilename {
		if c.filename == filename || env.recordInGroup(c.record, group, now) {
			compatible = append(compatible, c)
		}
	}
	sort.Slice(compatible, func(i, j int) bool {
		if compatible[i].record.CreatedAt != compatible[j].record.CreatedAt {
			return compatible[i].record.CreatedAt < compatible[j].record.CreatedAt
		}
		return compatible[i].record.TS < compatible[j].record.TS
	})

	responded := 0
	pending := make([]companyPendingDelegation, 0, len(compatible))
	for _, c := range compatible {
		switch c.record.Status {
		case companyDelegationClaimed:
			responded++
		case companyDelegationPending:
			pending = append(pending, companyPendingDelegation{
				DelegationTS:               c.record.TS,
				DelegationKey:              c.filename,
				ExpectedResponderAgent:     c.record.ExpectedResponderAgent,
				ExpectedResponderBotUserID: c.record.ExpectedResponderBotUserID,
			})
		}
	}
	return companySynthesisSnapshot{
		Version:    companySynthesisStateVersion,
		Available:  true,
		Compatible: len(compatible),
		Responded:  responded,
		Pending:    len(pending),
		PendingIDs: pending,
		Ready:      len(compatible) > 0 && responded == len(compatible),
		SnapshotAt: snapshotAt,
	}
}

// recordInGroup applies the S2 compatible-membership test to a non-current
// record: it parses, shares the claim's group, is pending or result_claimed
// (Deviation D3: result_claimed is Slack's single responded status), and its
// age is within [-300s, ttl_seconds] of created_at.
func (env companyPeerEnv) recordInGroup(rec *companyDelegationRecord, group companySynthesisGroup, now time.Time) bool {
	g, ok := synthesisGroupOf(rec)
	if !ok || g != group {
		return false
	}
	if rec.Status != companyDelegationPending && rec.Status != companyDelegationClaimed {
		return false
	}
	created, err := time.Parse(time.RFC3339, rec.CreatedAt)
	if err != nil {
		return false
	}
	age := now.Sub(created)
	return age >= -companyRouteWindowBack && age <= time.Duration(rec.TTLSeconds)*time.Second
}

// storedSnapshot reads a delegation record's frozen snapshot from disk and
// normalizes it (S10). A vanished or malformed file yields the unavailable
// shape rather than an error.
func (env companyPeerEnv) storedSnapshot(filename string) companySynthesisSnapshot {
	data, err := os.ReadFile(filepath.Join(env.delegationsDir, filename))
	if err != nil {
		return normalizeSynthesisState(nil)
	}
	return normalizeSynthesisBytes(data)
}

// normalizeSynthesisBytes normalizes a raw JSON object (a record or a frozen
// snapshot blob) through the S10 validator. Empty or non-object input yields
// the unavailable shape.
func normalizeSynthesisBytes(raw json.RawMessage) companySynthesisSnapshot {
	if len(raw) == 0 {
		return normalizeSynthesisState(nil)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return normalizeSynthesisState(nil)
	}
	return normalizeSynthesisState(obj)
}

// normalizeSynthesisState is the S10 validator (CM:2401-2476), the byte-for-
// byte cross-language normalizer both sides ship. A stored snapshot is
// available iff synthesis_state_version == 1, synthesis_state_available ==
// true, all three counts are non-negative ints with responded + pending ==
// compatible and compatible > 0, the pending list length equals
// pending_delegation_count with unique non-empty delegation_ts,
// synthesis_snapshot_at is non-empty, and the stored synthesis_ready equals the
// recomputed value. Anything else normalizes to the unavailable shape (version
// 0, zero counts, empty list, ready false).
func normalizeSynthesisState(raw map[string]json.RawMessage) companySynthesisSnapshot {
	unavailable := companySynthesisSnapshot{PendingIDs: []companyPendingDelegation{}}
	if raw == nil {
		return unavailable
	}

	// Pending list: it must be a JSON array; each object item with a non-empty
	// delegation_ts is normalized, non-object / empty-id items are skipped (and
	// therefore break the length equality below, exactly like Discord).
	var rawList []json.RawMessage
	isList := false
	if pl, ok := raw["pending_delegations"]; ok {
		if err := json.Unmarshal(pl, &rawList); err == nil {
			isList = true
		}
	}
	normalized := make([]companyPendingDelegation, 0, len(rawList))
	for _, item := range rawList {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(item, &obj); err != nil {
			continue
		}
		// STRICT (canonical S10 rule): no whitespace trimming — delegation_ts is
		// tested for emptiness and uniqueness on its raw string value, matching
		// Python's normalizer exactly (a whitespace-padded id is a distinct id,
		// not a duplicate). A truthy-whitespace id therefore stays available.
		ts := jsonString(obj["delegation_ts"])
		if ts == "" {
			continue
		}
		normalized = append(normalized, companyPendingDelegation{
			DelegationTS:               ts,
			DelegationKey:              jsonString(obj["delegation_key"]),
			ExpectedResponderAgent:     jsonString(obj["expected_responder_agent"]),
			ExpectedResponderBotUserID: jsonString(obj["expected_responder_bot_user_id"]),
		})
	}

	version, versionOK := jsonNonNegInt(raw["synthesis_state_version"])
	compatible, cOK := jsonNonNegInt(raw["compatible_delegation_count"])
	responded, rOK := jsonNonNegInt(raw["responded_delegation_count"])
	pendingCount, pOK := jsonNonNegInt(raw["pending_delegation_count"])
	countsValid := cOK && rOK && pOK
	stateAvailable := jsonStrictTrue(raw["synthesis_state_available"])
	storedReady, storedReadyOK := jsonStrictBool(raw["synthesis_ready"])
	// STRICT: raw snapshot_at value, no trim — a truthy-whitespace snapshot_at is
	// non-empty (matches Python's `not snapshot_at` falsiness on "").
	snapshotAt := jsonString(raw["synthesis_snapshot_at"])

	ids := make(map[string]struct{}, len(normalized))
	for _, p := range normalized {
		ids[p.DelegationTS] = struct{}{}
	}
	uniqueIDs := len(ids) == len(normalized)

	computedReady := compatible > 0 && responded == compatible && pendingCount == 0

	available := stateAvailable &&
		versionOK && version == companySynthesisStateVersion &&
		countsValid &&
		compatible > 0 &&
		responded+pendingCount == compatible &&
		isList &&
		len(rawList) == len(normalized) &&
		pendingCount == len(normalized) &&
		uniqueIDs &&
		snapshotAt != "" &&
		storedReadyOK &&
		storedReady == computedReady

	if !available {
		return unavailable
	}
	return companySynthesisSnapshot{
		Version:    companySynthesisStateVersion,
		Available:  true,
		Compatible: compatible,
		Responded:  responded,
		Pending:    pendingCount,
		PendingIDs: normalized,
		Ready:      storedReady && computedReady,
		SnapshotAt: snapshotAt,
	}
}

// jsonNonNegInt reports whether raw is a JSON integer >= 0, matching Discord's
// `type(value) is int and value >= 0`. It validates the raw token directly (a
// bare integer literal) rather than through json.Number, which leniently
// accepts a quoted number like "2" and would misread a string count as an int.
func jsonNonNegInt(raw json.RawMessage) (int, bool) {
	s := strings.TrimSpace(string(raw))
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			continue
		}
		if i == 0 && s[i] == '-' {
			continue // parsed, then rejected by the >= 0 check below
		}
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// jsonStrictBool decodes a JSON boolean literal, reporting ok only for an exact
// true/false token (Discord's `type(value) is bool`).
func jsonStrictBool(raw json.RawMessage) (bool, bool) {
	switch strings.TrimSpace(string(raw)) {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

// jsonStrictTrue reports whether raw is exactly the JSON literal true.
func jsonStrictTrue(raw json.RawMessage) bool {
	v, ok := jsonStrictBool(raw)
	return ok && v
}

// jsonString returns the string value of a JSON string token, or "" for any
// non-string / absent value.
func jsonString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}
