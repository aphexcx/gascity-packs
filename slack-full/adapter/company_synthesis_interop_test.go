package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// company_synthesis_interop_test.go — Slack company-rooms Phase 3c cross-language
// pins consumed by the GO suite (the counterpart to the Python gate tests): the
// runtime-only dgroup/dgser lock-name derivations pinned against
// tests/fixtures/company/synthesis_locks.json, the S10 normalizer pinned against
// the golden claimed_delegation_*.json fixtures, and the cancel-then-ready flow
// (a sibling expired as Python's --cancel writes it -> the next claim freezes
// ready). Reordering any lock key field or normalizer rule now fails the GO
// suite, not just Python's.

func companyFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "tests", "fixtures", "company", name))
	if err != nil {
		t.Fatalf("read company fixture %s: %v", name, err)
	}
	return data
}

// TestSynthesisLockNameParityPins asserts the Go dgroup (5-field + 6-field
// fallback) and dgser lock-name derivations produce byte-identical filenames to
// the literals Python pins in synthesis_locks.json. These derivations
// (companySynthesisGroup.lockName / synthesisFallbackLockName / rootSerialLockName)
// are the ONLY ones taken at runtime; Python never acquires them, so without this
// pin a Go-side field reorder would silently diverge the cross-process lock names
// while every other test stayed green.
func TestSynthesisLockNameParityPins(t *testing.T) {
	var fx struct {
		Dgroup struct {
			GroupFields      []string `json:"group_fields"`
			ExpectedLockName string   `json:"expected_lock_name"`
		} `json:"dgroup"`
		DgroupFallback struct {
			FallbackFields   []string `json:"fallback_fields"`
			ExpectedLockName string   `json:"expected_lock_name"`
		} `json:"dgroup_fallback"`
		Dgser struct {
			RootFields       []string `json:"root_fields"`
			ExpectedLockName string   `json:"expected_lock_name"`
		} `json:"dgser"`
	}
	if err := json.Unmarshal(companyFixture(t, "synthesis_locks.json"), &fx); err != nil {
		t.Fatalf("decode synthesis_locks.json: %v", err)
	}

	if got := len(fx.Dgroup.GroupFields); got != 5 {
		t.Fatalf("dgroup.group_fields = %d, want 5", got)
	}
	group := companySynthesisGroup{
		TeamID:             fx.Dgroup.GroupFields[0],
		ChannelID:          fx.Dgroup.GroupFields[1],
		ThreadRootTS:       fx.Dgroup.GroupFields[2],
		RequesterBotUserID: fx.Dgroup.GroupFields[3],
		RequesterSession:   fx.Dgroup.GroupFields[4],
	}
	if got := group.lockName(); got != fx.Dgroup.ExpectedLockName {
		t.Errorf("dgroup lockName = %q, want pinned %q", got, fx.Dgroup.ExpectedLockName)
	}

	if got := len(fx.DgroupFallback.FallbackFields); got != 6 {
		t.Fatalf("dgroup_fallback.fallback_fields = %d, want 6", got)
	}
	if fx.DgroupFallback.FallbackFields[0] != "unavailable" {
		t.Errorf("fallback[0] = %q, want unavailable", fx.DgroupFallback.FallbackFields[0])
	}
	fallbackTuple := companyDelegationTuple{
		TeamID:             fx.DgroupFallback.FallbackFields[1],
		ChannelID:          fx.DgroupFallback.FallbackFields[2],
		ThreadRootTS:       fx.DgroupFallback.FallbackFields[3],
		ResponderBotUserID: fx.DgroupFallback.FallbackFields[4],
		RequesterBotUserID: fx.DgroupFallback.FallbackFields[5],
	}
	if got := synthesisFallbackLockName(fallbackTuple); got != fx.DgroupFallback.ExpectedLockName {
		t.Errorf("dgroup fallback lockName = %q, want pinned %q", got, fx.DgroupFallback.ExpectedLockName)
	}

	if got := len(fx.Dgser.RootFields); got != 3 {
		t.Fatalf("dgser.root_fields = %d, want 3", got)
	}
	if got := rootSerialLockName(fx.Dgser.RootFields[0], fx.Dgser.RootFields[1], fx.Dgser.RootFields[2]); got != fx.Dgser.ExpectedLockName {
		t.Errorf("dgser lockName = %q, want pinned %q", got, fx.Dgser.ExpectedLockName)
	}
}

// TestNormalizeGoldenClaimedFixtures runs the Go S10 normalizer over the golden
// claimed-record fixtures and pins the exact normalized outcomes: ready available,
// not-ready available with the frozen pending sibling, and the arithmetically
// inconsistent snapshot degraded to unavailable.
func TestNormalizeGoldenClaimedFixtures(t *testing.T) {
	ready := normalizeSynthesisBytes(companyFixture(t, "claimed_delegation_ready.json"))
	if !ready.Available || ready.Version != companySynthesisStateVersion ||
		ready.Compatible != 1 || ready.Responded != 1 || ready.Pending != 0 ||
		!ready.Ready || len(ready.PendingIDs) != 0 || ready.SnapshotAt != "2026-07-17T12:00:07Z" {
		t.Fatalf("ready fixture normalized = %+v, want available ready compatible1/responded1/pending0", ready)
	}

	notReady := normalizeSynthesisBytes(companyFixture(t, "claimed_delegation_not_ready.json"))
	if !notReady.Available || notReady.Compatible != 2 || notReady.Responded != 1 ||
		notReady.Pending != 1 || notReady.Ready || len(notReady.PendingIDs) != 1 {
		t.Fatalf("not_ready fixture normalized = %+v, want available not-ready compatible2/responded1/pending1", notReady)
	}
	pd := notReady.PendingIDs[0]
	if pd.DelegationTS != "1700000000.000200" || pd.ExpectedResponderAgent != "seth" ||
		pd.ExpectedResponderBotUserID != botSeth ||
		pd.DelegationKey != "dg-T0AAAAAAA-C0AAAAAAA-1700000000.000200-63c6bb5b3717.json" {
		t.Fatalf("not_ready pending id = %+v, want the frozen seth sibling", pd)
	}

	// responded(2)+pending(1) != compatible(2): arithmetically inconsistent ->
	// unavailable (version 0, zero counts, empty list, ready false).
	invalid := normalizeSynthesisBytes(companyFixture(t, "claimed_delegation_invalid_snapshot.json"))
	if invalid.Available || invalid.Version != 0 || invalid.Compatible != 0 ||
		invalid.Responded != 0 || invalid.Pending != 0 || len(invalid.PendingIDs) != 0 ||
		invalid.Ready || invalid.SnapshotAt != "" {
		t.Fatalf("invalid_snapshot fixture normalized = %+v, want the unavailable shape", invalid)
	}
}

// TestCancelThenReadyFreezesReady — Phase 3c acceptance proof 5 (Go half): a
// sibling delegation expired exactly as Python's --cancel writes it (status
// "expired", generation bumped) is excluded from the synthesis group, so the next
// (and now sole) claim freezes a READY snapshot. Composes the S2 expired-exclusion
// with a live claim, which no prior test did.
func TestCancelThenReadyFreezesReady(t *testing.T) {
	env := peerTestEnv(t)

	// The surviving sibling (riley) is pending; the cancelled sibling (seth) is
	// expired, as run_cancel would have written it.
	fnA := writeRecord(t, env, groupSibling(delegationTS, fixtureNonce, "riley", botRiley, "2026-07-17T12:00:05Z"))
	cancelled := groupSibling(siblingTS, siblingNonce, "seth", botSeth, "2026-07-17T12:00:05Z")
	cancelled.Status = companyDelegationExpired
	cancelled.Generation = 2
	writeRecord(t, env, cancelled)

	pw, park, err := doClaim(env, botRiley, fixtureNonce, delegationTS, resultTS)
	if err != nil || park != "" || pw.Kind != wakeKindPeerResult {
		t.Fatalf("claim after cancel: kind=%q park=%q err=%v", pw.Kind, park, err)
	}
	if pw.Snapshot == nil {
		t.Fatal("claim carried no snapshot")
	}
	if !pw.Snapshot.Ready || pw.Snapshot.Compatible != 1 || pw.Snapshot.Responded != 1 || pw.Snapshot.Pending != 0 {
		t.Fatalf("post-cancel snapshot = %+v, want ready compatible1/responded1/pending0 (expired sibling excluded)", pw.Snapshot)
	}
	// The stored snapshot on disk agrees (frozen ready).
	if stored := env.storedSnapshot(fnA); !stored.Ready || stored.Compatible != 1 {
		t.Errorf("stored snapshot = %+v, want frozen ready", stored)
	}
}
