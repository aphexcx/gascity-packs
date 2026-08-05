package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// company_synthesis_test.go — Phase 3a synthesis core coverage: sibling freeze
// order + replay immutability (S1/S3/S4), the compatible-set membership table
// (S2), the S10 validator table ported from CM:2401-2476, replay-window and
// unparseable-claimed-at demotion (S4/S9), frozen Synthesis byte-identity
// across redrives, the peer_result envelope shapes, the dgroup-before-dtuple
// lock-order assertion, and a -race claim storm on one group.

const (
	botSeth         = "U0AAAAAA3"
	siblingTS       = "1700000000.000600"
	siblingNonce    = "gcs-abcdef0123456789abcd"
	siblingResultTS = "1700000000.000950"
)

// resultTuple builds the claim tuple for a responder answering ollie on the
// shared human root.
func resultTuple(responderBot string) companyDelegationTuple {
	return companyDelegationTuple{
		TeamID:             testTeam,
		ChannelID:          testChannel,
		ThreadRootTS:       humanRootTS,
		ResponderBotUserID: responderBot,
		RequesterBotUserID: botOllie,
	}
}

// doClaim drives one gc_delegation_result through resolveResultWake (the S1
// lock dance) for the ollie requester.
func doClaim(env companyPeerEnv, responderBot, nonce, delegTS, resTS string) (peerWake, string, error) {
	meta := slackMessageMetadata{
		EventType:    companyResultEventType,
		EventPayload: json.RawMessage(`{"v":1,"nonce":"` + nonce + `","delegation_ts":"` + delegTS + `"}`),
	}
	msg := CompanyMessage{TeamID: testTeam, ChannelID: testChannel, TS: resTS, ThreadTS: humanRootTS}
	requester := CompanyAgent{Name: "ollie", BotUserID: botOllie}
	return env.resolveResultWake(msg, meta, resultTuple(responderBot), requester)
}

// groupSibling builds a pending delegation in ollie's synthesis group with a
// custom responder / nonce / created_at.
func groupSibling(ts, nonce, responderAgent, responderBot, createdAt string) *companyDelegationRecord {
	r := pendingRecord(ts)
	r.Nonce = nonce
	r.ExpectedResponderAgent = responderAgent
	r.ExpectedResponderBotUserID = responderBot
	r.CreatedAt = createdAt
	return r
}

// TestSynthesisSiblingClaimsFreezeInOrder — two serialized sibling claims
// freeze not-ready(1 pending) then ready; the first claim's frozen snapshot is
// immutable when the second sibling later claims, and a replay of the first
// returns the stored snapshot without rewriting (S1/S3/S4; DOC:158-163).
func TestSynthesisSiblingClaimsFreezeInOrder(t *testing.T) {
	env := peerTestEnv(t)
	fnA := writeRecord(t, env, groupSibling(delegationTS, fixtureNonce, "riley", botRiley, "2026-07-17T12:00:05Z"))
	fnB := writeRecord(t, env, groupSibling(siblingTS, siblingNonce, "seth", botSeth, "2026-07-17T12:00:06Z"))

	// First sibling: frozen not-ready with one pending (B).
	pwA, park, err := doClaim(env, botRiley, fixtureNonce, delegationTS, resultTS)
	if err != nil || park != "" || pwA.Kind != wakeKindPeerResult {
		t.Fatalf("claim A: kind=%q park=%q err=%v", pwA.Kind, park, err)
	}
	if pwA.Snapshot == nil {
		t.Fatal("claim A wake carried no snapshot")
	}
	snapA := env.storedSnapshot(fnA)
	if !snapA.Available || snapA.Compatible != 2 || snapA.Responded != 1 || snapA.Pending != 1 || snapA.Ready {
		t.Fatalf("A snapshot = %+v, want compatible2/responded1/pending1/ready=false", snapA)
	}
	if len(snapA.PendingIDs) != 1 || snapA.PendingIDs[0].DelegationTS != siblingTS ||
		snapA.PendingIDs[0].DelegationKey != fnB || snapA.PendingIDs[0].ExpectedResponderAgent != "seth" {
		t.Fatalf("A pending list = %+v, want [B(seth)]", snapA.PendingIDs)
	}

	// Second sibling: frozen ready, no pending.
	pwB, park, err := doClaim(env, botSeth, siblingNonce, siblingTS, siblingResultTS)
	if err != nil || park != "" || pwB.Kind != wakeKindPeerResult {
		t.Fatalf("claim B: kind=%q park=%q err=%v", pwB.Kind, park, err)
	}
	snapB := env.storedSnapshot(fnB)
	if !snapB.Available || snapB.Compatible != 2 || snapB.Responded != 2 || snapB.Pending != 0 || !snapB.Ready {
		t.Fatalf("B snapshot = %+v, want compatible2/responded2/pending0/ready=true", snapB)
	}
	if len(snapB.PendingIDs) != 0 {
		t.Fatalf("B pending list = %+v, want empty", snapB.PendingIDs)
	}

	// A's frozen snapshot is immutable: B claiming did not recompute it.
	if again := env.storedSnapshot(fnA); again.Responded != 1 || again.Pending != 1 || again.Ready {
		t.Fatalf("A snapshot mutated after B claimed: %+v", again)
	}

	// Replay of A: returns the stored snapshot, rewrites nothing.
	rawBefore := readRaw(t, env, fnA)
	genBefore := readRecord(t, env, fnA).Generation
	pwA2, park, err := doClaim(env, botRiley, fixtureNonce, delegationTS, resultTS)
	if err != nil || park != "" || pwA2.Kind != wakeKindPeerResult {
		t.Fatalf("replay A: kind=%q park=%q err=%v", pwA2.Kind, park, err)
	}
	if !bytes.Equal(rawBefore, readRaw(t, env, fnA)) {
		t.Error("replay rewrote the record bytes")
	}
	if readRecord(t, env, fnA).Generation != genBefore {
		t.Errorf("replay bumped generation from %d", genBefore)
	}
	if pwA2.Snapshot == nil || pwA2.Snapshot.Responded != 1 || pwA2.Snapshot.Ready {
		t.Errorf("replay snapshot = %+v, want the stored not-ready snapshot", pwA2.Snapshot)
	}
}

// TestComputeSynthesisCompatibleSet — the S2 membership table: the current
// record is always included, a compatible pending sibling counts, a claimed
// sibling counts responded, and expired-out / window-out(−300s) / different-
// group / corrupt records are all excluded.
func TestComputeSynthesisCompatibleSet(t *testing.T) {
	env := peerTestEnv(t)

	cur := pendingRecord(delegationTS) // responder riley, group G
	fnCur := writeRecord(t, env, cur)

	writeRecord(t, env, groupSibling("1700000000.000600", "gcs-pending000000000001", "seth", botSeth, "2026-07-17T12:00:05Z"))

	claimedSib := groupSibling("1700000000.000601", "gcs-claimed000000000001", "cara", "U0AAAAAA4", "2026-07-17T12:00:05Z")
	claimedSib.Status = companyDelegationClaimed
	claimedSib.ResultTS = "1700000000.000970"
	claimedSib.ResultClaimedAt = fixedNow
	claimedSib.Generation = 2
	writeRecord(t, env, claimedSib)

	// Expired-out (age > ttl, the TTL boundary).
	writeRecord(t, env, groupSibling("1700000000.000602", "gcs-expired000000000001", "dan", "U0AAAAAA5", "2026-07-16T00:00:00Z"))
	// Window-out on the −300s side (created 400s in the future).
	writeRecord(t, env, groupSibling("1700000000.000603", "gcs-future000000000001", "eve", "U0AAAAAA6", "2026-07-17T12:11:40Z"))
	// Different group (a distinct requester_session incarnation).
	diffGroup := groupSibling("1700000000.000604", "gcs-diffgrp000000000001", "fay", "U0AAAAAA7", "2026-07-17T12:00:05Z")
	diffGroup.RequesterSession = "ollie-other"
	writeRecord(t, env, diffGroup)
	// Corrupt record (unparseable): skipped, never fatal.
	if err := os.WriteFile(filepath.Join(env.delegationsDir, "dg-corrupt-000000000000.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}

	claimedView := *cur
	claimedView.Status = companyDelegationClaimed
	claimedView.ResultTS = resultTS
	claimedView.ResultClaimedAt = fixedNow
	snap := env.computeSynthesisSnapshot(&claimedView, fnCur, fixedNow)

	if snap.Compatible != 3 {
		t.Errorf("compatible = %d, want 3 (current + pending sibling + claimed sibling)", snap.Compatible)
	}
	if snap.Responded != 2 {
		t.Errorf("responded = %d, want 2 (current + claimed sibling)", snap.Responded)
	}
	if snap.Pending != 1 || len(snap.PendingIDs) != 1 || snap.PendingIDs[0].DelegationTS != "1700000000.000600" {
		t.Errorf("pending = %d %+v, want 1 [the compatible pending sibling]", snap.Pending, snap.PendingIDs)
	}
	if snap.Ready {
		t.Errorf("ready = true, want false (responded 2 != compatible 3)")
	}
}

// TestNormalizeSynthesisStateTable — the S10 validator ported case-for-case
// from CM:2401-2476.
func TestNormalizeSynthesisStateTable(t *testing.T) {
	base := func() map[string]json.RawMessage {
		return map[string]json.RawMessage{
			"synthesis_state_version":     json.RawMessage(`1`),
			"synthesis_state_available":   json.RawMessage(`true`),
			"compatible_delegation_count": json.RawMessage(`2`),
			"responded_delegation_count":  json.RawMessage(`2`),
			"pending_delegation_count":    json.RawMessage(`0`),
			"pending_delegations":         json.RawMessage(`[]`),
			"synthesis_ready":             json.RawMessage(`true`),
			"synthesis_snapshot_at":       json.RawMessage(`"2026-07-17T12:05:00Z"`),
		}
	}
	tests := []struct {
		name          string
		mutate        func(m map[string]json.RawMessage)
		wantAvailable bool
	}{
		{"valid ready", func(m map[string]json.RawMessage) {}, true},
		{"valid not ready", func(m map[string]json.RawMessage) {
			m["compatible_delegation_count"] = json.RawMessage(`2`)
			m["responded_delegation_count"] = json.RawMessage(`1`)
			m["pending_delegation_count"] = json.RawMessage(`1`)
			m["pending_delegations"] = json.RawMessage(`[{"delegation_ts":"1700000000.000600","expected_responder_agent":"seth"}]`)
			m["synthesis_ready"] = json.RawMessage(`false`)
		}, true},
		{"bad version", func(m map[string]json.RawMessage) {
			m["synthesis_state_version"] = json.RawMessage(`2`)
		}, false},
		{"state not available", func(m map[string]json.RawMessage) {
			m["synthesis_state_available"] = json.RawMessage(`false`)
		}, false},
		{"count mismatch", func(m map[string]json.RawMessage) {
			m["responded_delegation_count"] = json.RawMessage(`1`) // 1 + 0 != 2
		}, false},
		{"duplicate pending ids", func(m map[string]json.RawMessage) {
			m["compatible_delegation_count"] = json.RawMessage(`2`)
			m["responded_delegation_count"] = json.RawMessage(`0`)
			m["pending_delegation_count"] = json.RawMessage(`2`)
			m["pending_delegations"] = json.RawMessage(`[{"delegation_ts":"a"},{"delegation_ts":"a"}]`)
			m["synthesis_ready"] = json.RawMessage(`false`)
		}, false},
		{"stored_ready contradiction", func(m map[string]json.RawMessage) {
			m["synthesis_ready"] = json.RawMessage(`false`) // computed true (2==2, pending 0)
		}, false},
		{"non-int count (string)", func(m map[string]json.RawMessage) {
			m["compatible_delegation_count"] = json.RawMessage(`"2"`)
		}, false},
		{"non-int count (float)", func(m map[string]json.RawMessage) {
			m["compatible_delegation_count"] = json.RawMessage(`2.0`)
		}, false},
		{"len mismatch (skipped item)", func(m map[string]json.RawMessage) {
			m["compatible_delegation_count"] = json.RawMessage(`2`)
			m["responded_delegation_count"] = json.RawMessage(`1`)
			m["pending_delegation_count"] = json.RawMessage(`1`)
			// One valid + one empty-id item: normalized length (1) != raw length (2).
			m["pending_delegations"] = json.RawMessage(`[{"delegation_ts":"x"},{"delegation_ts":""}]`)
			m["synthesis_ready"] = json.RawMessage(`false`)
		}, false},
		{"empty snapshot_at", func(m map[string]json.RawMessage) {
			m["synthesis_snapshot_at"] = json.RawMessage(`""`)
		}, false},
		{"pending not a list", func(m map[string]json.RawMessage) {
			m["pending_delegations"] = json.RawMessage(`{}`)
		}, false},
		// STRICT canonical-rule cases (F8): version must be an exact JSON integer
		// (bool/float rejected), and NO whitespace trimming anywhere — a truthy-
		// whitespace snapshot_at is non-empty and whitespace-padded ids are
		// distinct, matching the Python normalizer byte-for-byte.
		{"version bool true (strict reject)", func(m map[string]json.RawMessage) {
			m["synthesis_state_version"] = json.RawMessage(`true`)
		}, false},
		{"version float 1.0 (strict reject)", func(m map[string]json.RawMessage) {
			m["synthesis_state_version"] = json.RawMessage(`1.0`)
		}, false},
		{"version string (strict reject)", func(m map[string]json.RawMessage) {
			m["synthesis_state_version"] = json.RawMessage(`"1"`)
		}, false},
		{"whitespace-only snapshot_at is non-empty (strict no-trim)", func(m map[string]json.RawMessage) {
			m["synthesis_snapshot_at"] = json.RawMessage(`"   "`)
		}, true},
		{"whitespace-padded pending ids are distinct (strict no-trim)", func(m map[string]json.RawMessage) {
			m["compatible_delegation_count"] = json.RawMessage(`2`)
			m["responded_delegation_count"] = json.RawMessage(`0`)
			m["pending_delegation_count"] = json.RawMessage(`2`)
			m["pending_delegations"] = json.RawMessage(`[{"delegation_ts":"1700000000.000200"},{"delegation_ts":" 1700000000.000200"}]`)
			m["synthesis_ready"] = json.RawMessage(`false`)
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := base()
			tt.mutate(m)
			got := normalizeSynthesisState(m)
			if got.Available != tt.wantAvailable {
				t.Fatalf("available = %v, want %v (snapshot %+v)", got.Available, tt.wantAvailable, got)
			}
			if !got.Available {
				// The unavailable shape is version 0, zero counts, empty list, ready false.
				if got.Version != 0 || got.Compatible != 0 || got.Responded != 0 ||
					got.Pending != 0 || len(got.PendingIDs) != 0 || got.Ready || got.SnapshotAt != "" {
					t.Fatalf("unavailable shape not normalized: %+v", got)
				}
			}
		})
	}
}

// TestReplayWindowExpiryDemotes — a claim replay older than the retention
// window is ordinary peer input, never a re-claim, and the record is not
// rewritten (S4).
func TestReplayWindowExpiryDemotes(t *testing.T) {
	env := peerTestEnv(t)
	rec := pendingRecord(delegationTS)
	rec.Status = companyDelegationClaimed
	rec.ResultTS = resultTS
	rec.ResultClaimedAt = "2026-07-09T12:05:00Z" // 8 days before fixedNow, past the 7-day window
	rec.Generation = 2
	fn := writeRecord(t, env, rec)
	rawBefore := readRaw(t, env, fn)

	pw, park, err := doClaim(env, botRiley, fixtureNonce, delegationTS, resultTS)
	if err != nil || park != "" {
		t.Fatalf("err=%v park=%q", err, park)
	}
	if pw.Kind != wakeKindPeerInput || pw.DelegationKey != "" {
		t.Fatalf("wake = %+v, want keyless peer_input", pw)
	}
	if !bytes.Equal(rawBefore, readRaw(t, env, fn)) {
		t.Error("stale replay rewrote the record")
	}
}

// TestReplayUnparseableClaimedAtDemotes — an unparseable result_claimed_at
// fails closed (S9): the replay is ordinary peer input, never rewritten.
func TestReplayUnparseableClaimedAtDemotes(t *testing.T) {
	env := peerTestEnv(t)
	rec := pendingRecord(delegationTS)
	rec.Status = companyDelegationClaimed
	rec.ResultTS = resultTS
	rec.ResultClaimedAt = "not-a-timestamp"
	rec.Generation = 2
	fn := writeRecord(t, env, rec)
	rawBefore := readRaw(t, env, fn)

	pw, park, err := doClaim(env, botRiley, fixtureNonce, delegationTS, resultTS)
	if err != nil || park != "" {
		t.Fatalf("err=%v park=%q", err, park)
	}
	if pw.Kind != wakeKindPeerInput {
		t.Fatalf("wake kind = %q, want peer_input", pw.Kind)
	}
	if !bytes.Equal(rawBefore, readRaw(t, env, fn)) {
		t.Error("fail-closed replay rewrote the record")
	}
}

// TestSynthesisEnvelopeRenders — the peer_result envelope renders the
// ready / not-ready / unavailable shapes, adds peer_redelegation: forbidden,
// and omits the synthesis block for non-peer_result kinds.
func TestSynthesisEnvelopeRenders(t *testing.T) {
	dir := testDirectory(t)
	room, _ := dir.RoomByChannel(testTeam, testChannel)
	hy := companyHydration{RootProvenance: companyRootProvenanceUnverified, ContextStatus: companyContextUnavailable}

	render := func(kind string, snap json.RawMessage) string {
		return renderCompanyReminder(room, "company_bot", kind, "body", "1700000000.000900", humanRootTS, hy, snap, nil)
	}
	marshalSnap := func(s companySynthesisSnapshot) json.RawMessage {
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("marshal snapshot: %v", err)
		}
		return b
	}

	ready := render(wakeKindPeerResult, marshalSnap(companySynthesisSnapshot{
		Version: 1, Available: true, Compatible: 2, Responded: 2, Pending: 0,
		PendingIDs: []companyPendingDelegation{}, Ready: true, SnapshotAt: "2026-07-17T12:05:00Z",
	}))
	for _, want := range []string{
		"peer_redelegation: forbidden",
		"synthesis_state_version: 1",
		"synthesis_state_available: true",
		"compatible_delegation_count: 2",
		"responded_delegation_count: 2",
		"pending_delegation_count: 0",
		"pending_delegations_json: []",
		"synthesis_ready: true",
		"synthesis_ready_meaning: all_currently_materialized_compatible_delegations_have_durably_claimed_slack_results",
		"synthesis_ready_is_local_delivery_success: false",
	} {
		if !strings.Contains(ready, want) {
			t.Errorf("ready envelope missing %q\n%s", want, ready)
		}
	}

	notReady := render(wakeKindPeerResult, marshalSnap(companySynthesisSnapshot{
		Version: 1, Available: true, Compatible: 2, Responded: 1, Pending: 1,
		PendingIDs: []companyPendingDelegation{{DelegationTS: siblingTS, DelegationKey: "dg-x.json", ExpectedResponderAgent: "seth", ExpectedResponderBotUserID: botSeth}},
		Ready:      false, SnapshotAt: "2026-07-17T12:05:00Z",
	}))
	if !strings.Contains(notReady, "synthesis_ready: false") || !strings.Contains(notReady, "pending_delegation_count: 1") {
		t.Errorf("not-ready envelope wrong:\n%s", notReady)
	}
	if !strings.Contains(notReady, "seth") || !strings.Contains(notReady, siblingTS) {
		t.Errorf("not-ready pending_delegations_json missing the pending sibling:\n%s", notReady)
	}

	// A malformed / legacy blob renders the unavailable shape rather than failing.
	unavailable := render(wakeKindPeerResult, json.RawMessage(`{"synthesis_state_version":9}`))
	for _, want := range []string{
		"synthesis_state_version: 0",
		"synthesis_state_available: false",
		"compatible_delegation_count: 0",
		"pending_delegations_json: []",
		"synthesis_ready: false",
	} {
		if !strings.Contains(unavailable, want) {
			t.Errorf("unavailable envelope missing %q\n%s", want, unavailable)
		}
	}

	// A non-peer_result peer kind carries peer_redelegation but no synthesis block.
	deleg := render(wakeKindPeerDelegation, nil)
	if !strings.Contains(deleg, "peer_redelegation: forbidden") {
		t.Errorf("peer_delegation envelope missing peer_redelegation line:\n%s", deleg)
	}
	if strings.Contains(deleg, "synthesis_state_version") {
		t.Errorf("peer_delegation envelope leaked a synthesis block:\n%s", deleg)
	}
}

// TestFrozenSynthesisByteIdentityAcrossRedrives — the receipt's Synthesis bytes
// are frozen at the routing commit, so a redrive re-renders byte-identical
// synthesis fields (never recomputed) even after a retryable first attempt.
func TestFrozenSynthesisByteIdentityAcrossRedrives(t *testing.T) {
	gc := newFakeGC(t)
	gc.respStatus = func(n int) int {
		if n == 0 {
			return http.StatusInternalServerError // retryable: target stays pending
		}
		return http.StatusOK
	}
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.authors = fakeAuthorResolver{byBot: map[string]companyBotInfo{
		"B0RILEY": {UserID: botRiley, AppID: "A0AAAAAA2"},
	}}
	writeHarnessRecord(t, h, pendingRecord(delegationTS))
	h.openBarrier()

	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: resultTS}
	ev := botEvent("B0RILEY", botRiley, "A0AAAAAA2", resultTS, humanRootTS,
		"<@"+botOllie+"> done", resultMetadata(fixtureNonce, delegationTS))
	if _, handled := h.admitViaHandler(t, ev, 0); !handled {
		t.Fatal("admit result")
	}
	h.wait()

	r, err := h.gw.store().Get(origin)
	if err != nil || r == nil {
		t.Fatalf("get receipt: %v", err)
	}
	frozen := append(json.RawMessage(nil), r.Synthesis...)
	if len(frozen) == 0 {
		t.Fatal("routing commit did not freeze a synthesis snapshot")
	}

	// Redrive the still-pending target (skips freezeWakes; uses frozen bytes).
	h.gw.deliverReceipt(origin)
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 2 {
		t.Fatalf("delivery attempts = %d, want 2 (5xx then 2xx)", len(calls))
	}
	if calls[0].body != calls[1].body {
		t.Errorf("redrive body differs:\n1: %q\n2: %q", calls[0].body, calls[1].body)
	}
	if !strings.Contains(calls[0].body, "synthesis_ready: true") {
		t.Errorf("body missing rendered synthesis block:\n%s", calls[0].body)
	}
	after, _ := h.gw.store().Get(origin)
	if !bytes.Equal(frozen, after.Synthesis) {
		t.Errorf("Synthesis bytes changed across redrive:\n before: %s\n after: %s", frozen, after.Synthesis)
	}
}

// acquiredWithin reports whether an exclusive lock on name can be taken within
// d — used to prove a lock is NOT held elsewhere.
func acquiredWithin(locksDir, name string, d time.Duration) bool {
	got := make(chan *companyFileLock, 1)
	go func() {
		l, err := acquireCompanyLock(locksDir, name)
		if err != nil {
			got <- nil
			return
		}
		got <- l
	}()
	select {
	case l := <-got:
		if l != nil {
			l.release()
			return true
		}
		return false
	case <-time.After(d):
		return false
	}
}

// TestSynthesisLockOrderDgroupBeforeDtuple — a claim blocked on dgroup has NOT
// acquired dtuple: the pinned order is dgroup → dtuple and no path holds the
// lower-rank tuple lock while acquiring the higher-rank group lock.
func TestSynthesisLockOrderDgroupBeforeDtuple(t *testing.T) {
	env := peerTestEnv(t)
	rec := pendingRecord(delegationTS)
	writeRecord(t, env, rec)
	group, ok := synthesisGroupOf(rec)
	if !ok {
		t.Fatal("record has no derivable group")
	}

	// Pre-hold dgroup so any claim must block there first.
	gl, err := acquireCompanyLock(env.locksDir, group.lockName())
	if err != nil {
		t.Fatalf("hold dgroup: %v", err)
	}

	done := make(chan peerWake, 1)
	go func() {
		pw, _, _ := doClaim(env, botRiley, fixtureNonce, delegationTS, resultTS)
		done <- pw
	}()

	// Give the claim time to reach — and block on — the dgroup acquire.
	time.Sleep(150 * time.Millisecond)

	// If the order were dtuple-first, the blocked claim would be holding dtuple
	// and this probe would time out.
	if !acquiredWithin(env.locksDir, resultTuple(botRiley).lockName(), 2*time.Second) {
		gl.release()
		t.Fatal("claim holds dtuple while blocked on dgroup (lock-order violation)")
	}

	// Release dgroup; the claim now proceeds to a normal peer_result claim.
	gl.release()
	select {
	case pw := <-done:
		if pw.Kind != wakeKindPeerResult {
			t.Fatalf("claim after dgroup release = %q, want peer_result", pw.Kind)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("claim did not complete after dgroup release")
	}
}

// TestSynthesisClaimStormOneGroup — concurrent claims of two siblings sharing
// one synthesis group serialize on dgroup: each record is claimed exactly once
// (generation bumped once) with a consistent compatible count. Run under -race.
func TestSynthesisClaimStormOneGroup(t *testing.T) {
	env := peerTestEnv(t)
	writeRecord(t, env, groupSibling(delegationTS, fixtureNonce, "riley", botRiley, "2026-07-17T12:00:05Z"))
	writeRecord(t, env, groupSibling(siblingTS, siblingNonce, "seth", botSeth, "2026-07-17T12:00:06Z"))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_, _, _ = doClaim(env, botRiley, fixtureNonce, delegationTS, resultTS)
			} else {
				_, _, _ = doClaim(env, botSeth, siblingNonce, siblingTS, siblingResultTS)
			}
		}(i)
	}
	wg.Wait()

	fnA := companyDelegationFilename(testTeam, testChannel, delegationTS)
	fnB := companyDelegationFilename(testTeam, testChannel, siblingTS)
	for _, fn := range []string{fnA, fnB} {
		rec := readRecord(t, env, fn)
		if rec.Status != companyDelegationClaimed {
			t.Errorf("%s status = %s, want result_claimed", fn, rec.Status)
		}
		if rec.Generation != 2 {
			t.Errorf("%s generation = %d, want 2 (claimed exactly once)", fn, rec.Generation)
		}
		if snap := env.storedSnapshot(fn); snap.Compatible != 2 {
			t.Errorf("%s compatible = %d, want 2", fn, snap.Compatible)
		}
	}
}
