package main

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// company_replay_test.go — Slack company-rooms Phase 3b coverage for the Go
// ordering half: compareSlackTS's total order, S5 per-root replay ordering, the
// S7 correlation backoff schedule (terminal correlation vs never-terminal
// ambiguous, correlation_error never consuming the budget, restart not bumping
// before RecoveryNextAt), synchronous chain sequentiality, the S6 dgser hold,
// and live-trigger routing into an active chain.

// ---- compareSlackTS --------------------------------------------------------

func TestCompareSlackTS(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"1700000000.000100", "1700000000.000200", -1}, // fraction asc
		{"1700000000.000200", "1700000000.000100", 1},
		{"1700000000.000100", "1700000000.000100", 0}, // equal
		{"1700000001.000000", "1700000000.999999", 1}, // seconds dominate
		{"170.1", "1700000000.1", -1},                 // numeric magnitude, not lexicographic
		{"1700000000.5", "1700000000.10", -1},         // fraction is numeric: 5 < 10 (lexicographic would say >)
		{"1700000000.010", "1700000000.10", 0},        // leading zeros are magnitude-equal
		{"badts", "1700000000.000100", 1},             // malformed sorts after well-formed
		{"1700000000.000100", "badts", -1},
		{"1700000000", "1700000000.1", 1}, // no '.' is malformed -> after well-formed
		{"zzz", "aaa", 1},                 // both malformed: raw-string order
		{"aaa", "zzz", -1},
		{"1700000000.abc", "1700000000.000100", 1}, // non-digit fraction is malformed
	}
	for _, c := range cases {
		if got := signOf(compareSlackTS(c.a, c.b)); got != c.want {
			t.Errorf("compareSlackTS(%q,%q) = %d, want %d", c.a, c.b, got, c.want)
		}
		// Antisymmetry: swapping arguments negates the sign.
		if got := signOf(compareSlackTS(c.b, c.a)); got != -c.want {
			t.Errorf("compareSlackTS(%q,%q) not antisymmetric: %d vs %d", c.b, c.a, got, -c.want)
		}
	}
}

func signOf(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

// ---- backoff tunables ------------------------------------------------------

func TestNextRecoveryDelaySchedule(t *testing.T) {
	sec := time.Second
	want := []time.Duration{60 * sec, 120 * sec, 240 * sec, 480 * sec, 900 * sec, 900 * sec, 900 * sec}
	for i, w := range want {
		if got := nextRecoveryDelay(i + 1); got != w {
			t.Errorf("nextRecoveryDelay(%d) = %s, want %s", i+1, got, w)
		}
	}
	if got := nextRecoveryDelay(0); got != 60*sec {
		t.Errorf("nextRecoveryDelay(0) = %s, want 60s (clamped)", got)
	}
}

func TestRecoveryDue(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	if !recoveryDue(&IngressReceipt{}, now) {
		t.Error("zero RecoveryNextAt should be immediately due (S7)")
	}
	if recoveryDue(&IngressReceipt{RecoveryNextAt: now.Add(time.Minute)}, now) {
		t.Error("future RecoveryNextAt should not be due")
	}
	if !recoveryDue(&IngressReceipt{RecoveryNextAt: now.Add(-time.Second)}, now) {
		t.Error("past RecoveryNextAt should be due")
	}
}

// ---- S5 replay ordering ----------------------------------------------------

func snapshotBytes(t *testing.T, responded int, snapshotAt string) json.RawMessage {
	t.Helper()
	snap := companySynthesisSnapshot{
		Version:    companySynthesisStateVersion,
		Available:  true,
		Compatible: responded, // pending == 0, so compatible == responded
		Responded:  responded,
		Pending:    0,
		PendingIDs: []companyPendingDelegation{},
		Ready:      true,
		SnapshotAt: snapshotAt,
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if got := normalizeSynthesisBytes(data); !got.Available || got.Responded != responded {
		t.Fatalf("snapshotBytes(%d) did not normalize available: %+v", responded, got)
	}
	return data
}

func replayReceipt(ts, threadTS string, receivedAt time.Time, synthesis json.RawMessage, resultTarget bool) *IngressReceipt {
	ev := slackMessageEvent{Type: "message", Channel: testChannelID, TS: ts, ThreadTS: threadTS}
	raw, _ := json.Marshal(ev)
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts}
	r := &IngressReceipt{
		ID:         receiptID(origin),
		Origin:     origin,
		Status:     ingressStatusRouting,
		Event:      raw,
		ReceivedAt: receivedAt,
		Synthesis:  synthesis,
	}
	if resultTarget {
		r.Targets = map[string]TargetDelivery{
			companyBoundTargetKeyPrefix + "sess": {Session: "sess", Kind: wakeKindPeerResult, Status: companyTargetPending},
		}
	}
	return r
}

func plainReceipt(ts, threadTS string) *IngressReceipt {
	ev := slackMessageEvent{Type: "message", Channel: testChannelID, TS: ts, ThreadTS: threadTS, User: "Uhuman"}
	raw, _ := json.Marshal(ev)
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts}
	return &IngressReceipt{ID: receiptID(origin), Origin: origin, Status: ingressStatusReceived, Event: raw}
}

// TestOrderPendingForReplayS5 — one root's chain orders legacy (no-snapshot)
// receipts first by (ReceivedAt, ts, id), then snapshot-bearing receipts by
// ascending responded_delegation_count; non-chain receipts pass through in
// store order (S5, mirroring GW:2070-2097).
func TestOrderPendingForReplayS5(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	base, _ := time.Parse(time.RFC3339, "2026-07-17T12:00:00Z")
	l1 := replayReceipt("1700000000.000201", humanRootTS, base.Add(time.Second), nil, true) // later ReceivedAt
	l2 := replayReceipt("1700000000.000202", humanRootTS, base, nil, true)                  // earlier -> first
	s1 := replayReceipt("1700000000.000301", humanRootTS, base, snapshotBytes(t, 1, "2026-07-17T12:00:06Z"), false)
	s2 := replayReceipt("1700000000.000302", humanRootTS, base, snapshotBytes(t, 2, "2026-07-17T12:00:05Z"), false)
	n1 := plainReceipt("1700000000.000401", humanRootTS)
	n2 := plainReceipt("1700000000.000402", humanRootTS)

	// Shuffled discovery order.
	input := []*IngressReceipt{s2, n1, l1, s1, l2, n2}
	chains, rest := h.gw.orderPendingForReplay(input)

	if len(chains) != 1 {
		t.Fatalf("chains = %d, want 1", len(chains))
	}
	wantOrder := []string{l2.Origin.TS, l1.Origin.TS, s1.Origin.TS, s2.Origin.TS}
	if len(chains[0].Origins) != len(wantOrder) {
		t.Fatalf("chain length = %d, want %d", len(chains[0].Origins), len(wantOrder))
	}
	for i, want := range wantOrder {
		if chains[0].Origins[i].TS != want {
			t.Errorf("chain[%d] = %s, want %s (legacy-first then responded asc)", i, chains[0].Origins[i].TS, want)
		}
	}
	if len(rest) != 2 || rest[0].Origin.TS != n1.Origin.TS || rest[1].Origin.TS != n2.Origin.TS {
		t.Errorf("rest = %+v, want [n1 n2] in store order", rest)
	}
}

// TestReplayChainIncludesClaimedButUnroutedResult — F5: a result receipt whose
// delegation record was already claimed but which crashed BEFORE the routing
// commit has the shape (Status received, no reason, no targets, no synthesis)
// that the recorded-state classifier misses. Its result-bearing MESSAGE (S6) must
// still pull it into its root chain so a restart cannot deliver a later sibling's
// wake before it.
func TestReplayChainIncludesClaimedButUnroutedResult(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	ev := botEvent("B0RILEY", botRiley, "A0AAAAAA2", "1700000000.000850", humanRootTS,
		"<@"+botOllie+"> done", resultMetadata(fixtureNonce, delegationTS))
	raw, _ := json.Marshal(ev)
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.000850"}
	unrouted := &IngressReceipt{ID: receiptID(origin), Origin: origin, Status: ingressStatusReceived, Event: raw}

	// Precondition: the recorded-state classifier does NOT catch this shape.
	if isReplayChainReceipt(unrouted) {
		t.Fatal("precondition: crash-window result should not match isReplayChainReceipt by recorded state")
	}
	human := plainReceipt("1700000000.000851", humanRootTS)

	chains, rest := h.gw.orderPendingForReplay([]*IngressReceipt{unrouted, human})
	if len(chains) != 1 || len(chains[0].Origins) != 1 || chains[0].Origins[0].TS != unrouted.Origin.TS {
		t.Fatalf("chains = %+v, want the unrouted result in its root chain (S6 message classification)", chains)
	}
	if len(rest) != 1 || rest[0].Origin.TS != human.Origin.TS {
		t.Errorf("rest = %+v, want only the human receipt", rest)
	}
}

// ---- S7 backoff schedule (integration through parkWithReason) --------------

func admitReceived(t *testing.T, h *companyHarness, ts string) *IngressReceipt {
	t.Helper()
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts}
	ev := botEvent("B0RILEY", botRiley, "A0AAAAAA2", ts, humanRootTS, "done", resultMetadata(fixtureNonce, delegationTS))
	raw, _ := json.Marshal(ev)
	r := &IngressReceipt{Origin: origin, Event: raw, Status: ingressStatusReceived}
	if created, _, err := h.receipts.Admit(r); err != nil || !created {
		t.Fatalf("admit received: created=%v err=%v", created, err)
	}
	return r
}

func TestCorrelationParkBackoffTerminal(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	now, _ := time.Parse(time.RFC3339, fixedNow)
	h.gw.now = func() time.Time { return now }

	r := admitReceived(t, h, "1700000000.000700")

	// First park: immediately eligible, attempts unchanged, next-at zero.
	h.gw.parkWithReason(r, peerParkCorrelationPending)
	if r.RecoveryAttempts != 0 || !r.RecoveryNextAt.IsZero() {
		t.Fatalf("first park attempts=%d next=%v, want 0/zero", r.RecoveryAttempts, r.RecoveryNextAt)
	}
	if r.Status != ingressStatusReceived || r.Reason != peerParkCorrelationPending {
		t.Fatalf("first park status=%s reason=%s", r.Status, r.Reason)
	}

	sec := time.Second
	for i, d := range []time.Duration{60 * sec, 120 * sec, 240 * sec, 480 * sec, 900 * sec} {
		h.gw.parkWithReason(r, peerParkCorrelationPending)
		if r.RecoveryAttempts != i+1 {
			t.Fatalf("repark %d attempts=%d, want %d", i+1, r.RecoveryAttempts, i+1)
		}
		if !r.RecoveryNextAt.Equal(now.Add(d)) {
			t.Errorf("repark %d next=%v, want %v", i+1, r.RecoveryNextAt, now.Add(d))
		}
		if r.Status != ingressStatusReceived {
			t.Fatalf("repark %d status=%s, want received (not yet terminal)", i+1, r.Status)
		}
	}

	before := h.gw.deliveryFailures.Load()
	h.gw.parkWithReason(r, peerParkCorrelationPending) // 6th counted attempt -> terminal
	if r.Status != ingressStatusFailed || r.Reason != companyReasonRecoveryExhausted {
		t.Fatalf("terminal park status=%s reason=%s, want failed/%s", r.Status, r.Reason, companyReasonRecoveryExhausted)
	}
	if r.RecoveryAttempts != companyPeerRecoveryMaxAttempts {
		t.Errorf("terminal attempts=%d, want %d", r.RecoveryAttempts, companyPeerRecoveryMaxAttempts)
	}
	if got := h.gw.deliveryFailures.Load(); got != before+1 {
		t.Errorf("deliveryFailures = %d, want %d (exhaustion counted)", got, before+1)
	}
}

func TestAmbiguousParkNeverTerminal(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	now, _ := time.Parse(time.RFC3339, fixedNow)
	h.gw.now = func() time.Time { return now }

	r := admitReceived(t, h, "1700000000.000710")
	h.gw.parkWithReason(r, peerParkAmbiguousPending) // first
	before := h.gw.deliveryFailures.Load()

	// Nine reparks: the backoff caps at 900s and the receipt NEVER terminalizes
	// (D5). Attempts keep climbing well past the correlation max.
	for i := 0; i < 9; i++ {
		h.gw.parkWithReason(r, peerParkAmbiguousPending)
		if r.Status != ingressStatusReceived || r.Reason != peerParkAmbiguousPending {
			t.Fatalf("ambiguous repark %d went terminal: status=%s reason=%s", i+1, r.Status, r.Reason)
		}
	}
	if r.RecoveryAttempts <= companyPeerRecoveryMaxAttempts {
		t.Errorf("ambiguous attempts=%d, want > %d (never terminal)", r.RecoveryAttempts, companyPeerRecoveryMaxAttempts)
	}
	if !r.RecoveryNextAt.Equal(now.Add(companyPeerRecoveryMaxDelay)) {
		t.Errorf("ambiguous next=%v, want capped at %v", r.RecoveryNextAt, now.Add(companyPeerRecoveryMaxDelay))
	}
	if got := h.gw.deliveryFailures.Load(); got != before {
		t.Errorf("deliveryFailures = %d, want %d (ambiguous never counts)", got, before)
	}
}

func TestCorrelationErrorNeverConsumesBudget(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	now, _ := time.Parse(time.RFC3339, fixedNow)
	h.gw.now = func() time.Time { return now }

	r := admitReceived(t, h, "1700000000.000720")
	for i := 0; i < 3; i++ {
		h.gw.parkWithReason(r, peerParkCorrelationError)
		if r.Status != ingressStatusReceived || r.Reason != peerParkCorrelationError {
			t.Fatalf("correlation_error park %d status=%s reason=%s", i+1, r.Status, r.Reason)
		}
		if r.RecoveryAttempts != 0 || r.RecoveryReason != "" || !r.RecoveryNextAt.IsZero() {
			t.Fatalf("correlation_error consumed budget: attempts=%d reason=%q next=%v",
				r.RecoveryAttempts, r.RecoveryReason, r.RecoveryNextAt)
		}
	}
}

// TestFreezeWakesErrorParksCorrelationError — F1: a correlation-layer I/O error
// (here scanDelegations' ReadDir failing because the delegations path is not a
// directory) parks the receipt under the non-counting correlation_error reason,
// NOT the budget-consuming correlation_pending — so a degraded-infra window can
// never terminalize a trusted, claimable result. Attempts/budget stay untouched.
func TestFreezeWakesErrorParksCorrelationError(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.authors = fakeAuthorResolver{byBot: map[string]companyBotInfo{
		"B0RILEY": {UserID: botRiley, AppID: "A0AAAAAA2"},
	}}
	// Inject a correlation-layer I/O error: the delegations "dir" is a regular
	// file, so scanDelegations' os.ReadDir fails with ENOTDIR (not ErrNotExist).
	if err := os.WriteFile(h.delegationsDir, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write blocking file: %v", err)
	}
	h.openBarrier()

	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: resultTS}
	ev := botEvent("B0RILEY", botRiley, "A0AAAAAA2", resultTS, humanRootTS,
		"<@"+botOllie+"> done", resultMetadata(fixtureNonce, delegationTS))
	before := h.gw.deliveryFailures.Load()
	if _, handled := h.admitViaHandler(t, ev, 0); !handled {
		t.Fatal("admit result")
	}
	h.wait()

	r, _ := h.gw.store().Get(origin)
	if r == nil || r.Status != ingressStatusReceived || r.Reason != peerParkCorrelationError {
		t.Fatalf("receipt = %+v, want parked received/correlation_error", r)
	}
	if r.RecoveryAttempts != 0 || r.RecoveryReason != "" || !r.RecoveryNextAt.IsZero() {
		t.Errorf("correlation_error consumed the S7 budget: attempts=%d reason=%q next=%v",
			r.RecoveryAttempts, r.RecoveryReason, r.RecoveryNextAt)
	}
	if got := h.gw.deliveryFailures.Load(); got != before {
		t.Errorf("deliveryFailures = %d, want %d (correlation_error never counts)", got, before)
	}
	if n := len(gc.sessionCalls()); n != 0 {
		t.Errorf("delivered %d, want 0 (parked, not dropped)", n)
	}
}

// TestRecoveryAttemptsResetOnReasonTransition — F1 (related MINOR): the S7 attempt
// budget is scoped per reason. A long-lived ambiguous park (D5, unbounded
// attempts) that transitions to a genuine correlation_pending must NOT inherit
// the ambiguous streak — otherwise the first correlation_pending re-park would
// near-instantly terminalize a recoverable result. The transition resets the
// attempt count so the full S7 schedule applies from the start.
func TestRecoveryAttemptsResetOnReasonTransition(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	now, _ := time.Parse(time.RFC3339, fixedNow)
	h.gw.now = func() time.Time { return now }

	r := admitReceived(t, h, "1700000000.000740")

	// Accumulate ambiguous-park attempts well past the correlation max.
	h.gw.parkWithReason(r, peerParkAmbiguousPending) // first park
	for i := 0; i < 8; i++ {
		h.gw.parkWithReason(r, peerParkAmbiguousPending)
	}
	if r.RecoveryAttempts <= companyPeerRecoveryMaxAttempts {
		t.Fatalf("precondition: ambiguous attempts=%d, want > %d", r.RecoveryAttempts, companyPeerRecoveryMaxAttempts)
	}

	// Transition to correlation_pending (ambiguity repaired, posting intent in
	// flight): a fresh streak, NOT an instant terminalization.
	h.gw.parkWithReason(r, peerParkCorrelationPending)
	if r.Status != ingressStatusReceived || r.Reason != peerParkCorrelationPending {
		t.Fatalf("transition park status=%s reason=%s, want received/correlation_pending", r.Status, r.Reason)
	}
	if r.RecoveryAttempts != 0 || r.RecoveryReason != peerParkCorrelationPending || !r.RecoveryNextAt.IsZero() {
		t.Fatalf("attempts not reset on reason change: attempts=%d reason=%q next=%v",
			r.RecoveryAttempts, r.RecoveryReason, r.RecoveryNextAt)
	}

	// The next re-park is attempt 1 on the full schedule, not terminal.
	h.gw.parkWithReason(r, peerParkCorrelationPending)
	if r.Status != ingressStatusReceived || r.RecoveryAttempts != 1 {
		t.Fatalf("second correlation park status=%s attempts=%d, want received/1", r.Status, r.RecoveryAttempts)
	}
	if !r.RecoveryNextAt.Equal(now.Add(60 * time.Second)) {
		t.Errorf("second correlation park next=%v, want +60s (schedule from the start)", r.RecoveryNextAt)
	}
}

// TestRestartDoesNotBumpAttemptsBeforeNextAt — recoverPending (the restart scan)
// applies the same RecoveryNextAt eligibility as the sweep, so a park scheduled
// for the future is neither delivered nor attempt-bumped by a restart (S7).
func TestRestartDoesNotBumpAttemptsBeforeNextAt(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	now, _ := time.Parse(time.RFC3339, fixedNow)
	h.gw.now = func() time.Time { return now }

	r := admitReceived(t, h, "1700000000.000730")
	if err := h.gw.commitReceipt(r, func(cur *IngressReceipt) {
		cur.Status = ingressStatusReceived
		cur.Reason = peerParkCorrelationPending
		cur.RecoveryReason = peerParkCorrelationPending
		cur.RecoveryAttempts = 2
		cur.RecoveryNextAt = now.Add(120 * time.Second)
	}); err != nil {
		t.Fatalf("seed park: %v", err)
	}

	if sweepEligible(r, now, h.gw.staleWindow) {
		t.Error("sweepEligible true before RecoveryNextAt, want false")
	}
	if err := h.gw.recoverPending(); err != nil {
		t.Fatalf("recoverPending: %v", err)
	}
	h.wait()

	after, _ := h.gw.store().Get(r.Origin)
	if after.RecoveryAttempts != 2 {
		t.Errorf("restart bumped attempts to %d, want 2 (unchanged)", after.RecoveryAttempts)
	}
	if n := len(gc.sessionCalls()); n != 0 {
		t.Errorf("restart delivered %d, want 0 (not yet due)", n)
	}
	if !sweepEligible(r, now.Add(200*time.Second), h.gw.staleWindow) {
		t.Error("sweepEligible false after RecoveryNextAt, want true")
	}
}

// ---- chain sequentiality + S6 dgser ----------------------------------------

func claimedResultReceipt(t *testing.T, h *companyHarness, ts string, synthesis json.RawMessage) ReceiptOrigin {
	t.Helper()
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts}
	ev := botEvent("B0RILEY", botRiley, "A0AAAAAA2", ts, humanRootTS, "<@"+botOllie+"> done", resultMetadata(fixtureNonce, delegationTS))
	raw, _ := json.Marshal(ev)
	hy, _ := json.Marshal(companyHydration{RootProvenance: companyRootProvenanceUnverified, ContextStatus: companyContextUnavailable})
	r := &IngressReceipt{
		Origin:    origin,
		Status:    ingressStatusRouting,
		Event:     raw,
		Synthesis: synthesis,
		Hydration: hy,
		Targets: map[string]TargetDelivery{
			companyBoundTargetKeyPrefix + "ollie-main": {
				Session:        "ollie-main",
				Kind:           wakeKindPeerResult,
				Status:         companyTargetPending,
				IdempotencyKey: companyIdempotencyKey(receiptID(origin), "ollie-main"),
				Agent:          "ollie",
				DelegationKey:  "dg-x.json",
			},
		},
	}
	if created, _, err := h.receipts.Admit(r); err != nil || !created {
		t.Fatalf("admit claimed result: created=%v err=%v", created, err)
	}
	return origin
}

// TestDeliverChainSequential — the chain sequencer delivers strictly in order:
// sibling B's POST is observed only after sibling A has reached a terminal
// state (deliverOutcome contract, S5).
func TestDeliverChainSequential(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	h.openBarrier()

	aOrigin := claimedResultReceipt(t, h, "1700000000.000801", snapshotBytes(t, 1, "2026-07-17T12:00:06Z"))
	bOrigin := claimedResultReceipt(t, h, "1700000000.000802", snapshotBytes(t, 2, "2026-07-17T12:00:07Z"))

	gc.hook = func(reqNum int) {
		if reqNum == 1 { // B's POST: A must already be terminal
			ra, _ := h.gw.store().Get(aOrigin)
			if ra == nil || !isTerminalStatus(ra.Status) {
				t.Errorf("sibling B delivered before A terminal (A status=%v)", ra.Status)
			}
		}
	}

	h.gw.deliverChain(replayChain{Origins: []ReceiptOrigin{aOrigin, bOrigin}})
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 2 {
		t.Fatalf("session calls = %d, want 2", len(calls))
	}
	aKey := companyIdempotencyKey(receiptID(aOrigin), "ollie-main")
	bKey := companyIdempotencyKey(receiptID(bOrigin), "ollie-main")
	if calls[0].idemKey != aKey || calls[1].idemKey != bKey {
		t.Errorf("delivery order = [%s, %s], want [A, B]", calls[0].idemKey, calls[1].idemKey)
	}
	for _, o := range []ReceiptOrigin{aOrigin, bOrigin} {
		if r, _ := h.gw.store().Get(o); r == nil || r.Status != ingressStatusDelivered {
			t.Errorf("receipt %s not delivered", o.TS)
		}
	}
}

// TestLiveTwoResultRaceDeliversInClaimOrder — acceptance proof 3: two result-
// bearing deliveries for the SAME root, run concurrently, are serialized by dgser
// so the requester (ollie) observes the wakes in CLAIM order end-to-end. The
// first gc POST carries the responded_delegation_count of the first claim (1),
// the second carries the second claim's count (2) — regardless of which sibling
// physically wins the lock. This exercises the real live claim path (not a pre-
// frozen chain), closing the gap the dgser hold-probe alone cannot catch.
func TestLiveTwoResultRaceDeliversInClaimOrder(t *testing.T) {
	gc := newFakeGC(t)
	// Two responders (riley, seth) answering the same requester (ollie) on one
	// root: two sibling delegation records in ollie's synthesis group.
	df := baseDirectoryFile()
	df.Agents = append(df.Agents, CompanyAgent{Name: "seth", AppID: "A0AAAAAA3", BotUserID: botSeth})
	df.Rooms[0].Members = append(df.Rooms[0].Members, "seth")
	df.Rooms[0].MentionWake = append(df.Rooms[0].MentionWake, "seth")
	bf := baseBindingsFile()
	bf.Bindings = append(bf.Bindings, CompanyBinding{Room: "orchestrator-team", Agent: "seth", Session: "seth-main"})
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.authors = fakeAuthorResolver{byBot: map[string]companyBotInfo{
		"B0RILEY": {UserID: botRiley, AppID: "A0AAAAAA2"},
		"B0SETH":  {UserID: botSeth, AppID: "A0AAAAAA3"},
	}}
	writeHarnessRecord(t, h, groupSibling(delegationTS, fixtureNonce, "riley", botRiley, "2026-07-17T12:00:05Z"))
	writeHarnessRecord(t, h, groupSibling(siblingTS, siblingNonce, "seth", botSeth, "2026-07-17T12:00:05Z"))
	h.openBarrier()

	admit := func(ts string, ev slackMessageEvent) ReceiptOrigin {
		raw, _ := json.Marshal(ev)
		origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts}
		if created, _, err := h.receipts.Admit(&IngressReceipt{Origin: origin, Event: raw, Status: ingressStatusReceived}); err != nil || !created {
			t.Fatalf("admit %s: created=%v err=%v", ts, created, err)
		}
		return origin
	}
	originA := admit(resultTS, botEvent("B0RILEY", botRiley, "A0AAAAAA2", resultTS, humanRootTS,
		"<@"+botOllie+"> done", resultMetadata(fixtureNonce, delegationTS)))
	originB := admit(siblingResultTS, botEvent("B0SETH", botSeth, "A0AAAAAA3", siblingResultTS, humanRootTS,
		"<@"+botOllie+"> done", resultMetadata(siblingNonce, siblingTS)))

	// Two concurrent live delivery workers for the same root.
	var wg sync.WaitGroup
	for _, o := range []ReceiptOrigin{originA, originB} {
		wg.Add(1)
		go func(o ReceiptOrigin) { defer wg.Done(); h.gw.deliverReceipt(o) }(o)
	}
	wg.Wait()
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 2 {
		t.Fatalf("session POSTs = %d, want 2 (both results woke ollie)", len(calls))
	}
	respByKey := map[string]int{}
	for _, o := range []ReceiptOrigin{originA, originB} {
		r, _ := h.gw.store().Get(o)
		if r == nil || r.Status != ingressStatusDelivered {
			t.Fatalf("receipt %s not delivered: %+v", o.TS, r)
		}
		if !strings.Contains(string(r.Synthesis), "synthesis_state_version") {
			t.Fatalf("receipt %s has no frozen synthesis: %s", o.TS, r.Synthesis)
		}
		var snap companySynthesisSnapshot
		if err := json.Unmarshal(r.Synthesis, &snap); err != nil {
			t.Fatalf("decode synthesis %s: %v", o.TS, err)
		}
		respByKey[companyIdempotencyKey(receiptID(o), "ollie-main")] = snap.Responded
	}
	for _, c := range calls {
		if !strings.Contains(c.path, "/session/ollie-main/") {
			t.Fatalf("delivered to %q, want ollie-main", c.path)
		}
	}
	if got := respByKey[calls[0].idemKey]; got != 1 {
		t.Errorf("first delivered wake responded=%d, want 1 (claim order == delivery order under dgser)", got)
	}
	if got := respByKey[calls[1].idemKey]; got != 2 {
		t.Errorf("second delivered wake responded=%d, want 2 (claim order == delivery order under dgser)", got)
	}
}

// TestResultBearingHoldsDgser — a result-bearing delivery holds the dgser root
// serialization lock across the whole path (S6), so a concurrent acquire blocks.
func TestResultBearingHoldsDgser(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	h.openBarrier()

	origin := claimedResultReceipt(t, h, "1700000000.000810", snapshotBytes(t, 1, "2026-07-17T12:00:06Z"))
	lockName := rootSerialLockName(testTeamID, testChannelID, humanRootTS)

	checked := false
	gc.hook = func(int) {
		checked = true
		if acquiredWithin(h.locksDir, lockName, 50*time.Millisecond) {
			t.Error("dgser not held during result-bearing delivery")
		}
	}
	h.gw.deliverReceipt(origin)
	if !checked {
		t.Fatal("delivery never POSTed; dgser hold not exercised")
	}
	if r, _ := h.gw.store().Get(origin); r == nil || r.Status != ingressStatusDelivered {
		t.Errorf("result-bearing receipt not delivered under dgser (status=%v)", r.Status)
	}
}

// TestLiveResultTriggerRoutesIntoActiveChain — a live result-bearing trigger for
// a root with an active chain is enqueued into that chain instead of racing it;
// a non-result receipt (or an unowned root) is not (S6).
func TestLiveResultTriggerRoutesIntoActiveChain(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	root := rootTriple{TeamID: testTeamID, ChannelID: testChannelID, ThreadRootTS: humanRootTS}
	seed := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.000820"}
	oc, owner := h.gw.chains.acquire(root, []ReceiptOrigin{seed})
	if !owner {
		t.Fatal("acquire did not grant ownership")
	}

	resultOrigin := claimedResultReceipt(t, h, "1700000000.000821", snapshotBytes(t, 1, "2026-07-17T12:00:06Z"))
	if !h.gw.enqueueForRoot(resultOrigin) {
		t.Fatal("live result trigger not routed into the active chain")
	}
	if len(oc.queue) != 2 {
		t.Errorf("owned queue = %d, want 2 (seed + routed result)", len(oc.queue))
	}

	// A non-result receipt is never routed into the chain.
	human := plainReceipt("1700000000.000822", humanRootTS)
	if _, _, err := h.receipts.Admit(human); err != nil {
		t.Fatalf("admit human: %v", err)
	}
	if h.gw.enqueueForRoot(human.Origin) {
		t.Error("non-result receipt routed into chain, want passthrough")
	}
}

// TestCorrelationParkResolvedDeliversOnceClearsRecovery — a result racing its
// delegation's posting intent parks correlation_pending, backs off across
// several passes, then (after the record materializes) claims and delivers
// exactly once and clears the recovery fields (S7 / acceptance proof 4).
func TestCorrelationParkResolvedDeliversOnceClearsRecovery(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.gw.authors = fakeAuthorResolver{byBot: map[string]companyBotInfo{
		"B0RILEY": {UserID: botRiley, AppID: "A0AAAAAA2"},
	}}

	var clk atomic.Int64
	t0, _ := time.Parse(time.RFC3339, fixedNow)
	clk.Store(t0.UnixNano())
	h.gw.now = func() time.Time { return time.Unix(0, clk.Load()).UTC() }

	// Posting intent in flight, no delegation record yet -> correlation_pending.
	if err := os.MkdirAll(h.intentsDir, 0o700); err != nil {
		t.Fatalf("mkdir intents: %v", err)
	}
	writePostingIntent(t, h.gw.peerEnv(), false)
	h.openBarrier()

	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: resultTS}
	ev := botEvent("B0RILEY", botRiley, "A0AAAAAA2", resultTS, humanRootTS,
		"<@"+botOllie+"> done", resultMetadata(fixtureNonce, delegationTS))
	if _, handled := h.admitViaHandler(t, ev, 0); !handled {
		t.Fatal("admit result")
	}
	h.wait()

	if r, _ := h.gw.store().Get(origin); r == nil || r.Reason != peerParkCorrelationPending {
		t.Fatalf("first attempt should park correlation_pending, got %+v", r)
	}

	// Two backed-off sweeps advance the attempt count without delivering.
	for i := 0; i < 2; i++ {
		clk.Add(int64(20 * time.Minute)) // well past the 15-minute cap
		h.gw.sweepOnce()
		h.wait()
	}
	parked, _ := h.gw.store().Get(origin)
	if parked.Reason != peerParkCorrelationPending || parked.RecoveryAttempts < 1 {
		t.Fatalf("expected still-parked with attempts>=1, got reason=%s attempts=%d", parked.Reason, parked.RecoveryAttempts)
	}
	if n := len(gc.sessionCalls()); n != 0 {
		t.Fatalf("parked receipt delivered %d times, want 0", n)
	}

	// The record materializes (Python's lazy reconciliation ran); the next
	// eligible sweep claims and delivers.
	writeHarnessRecord(t, h, pendingRecord(delegationTS))
	clk.Add(int64(20 * time.Minute))
	h.gw.sweepOnce()
	h.wait()

	if n := len(gc.sessionCalls()); n != 1 {
		t.Fatalf("resolved receipt delivered %d times, want exactly 1", n)
	}
	final, _ := h.gw.store().Get(origin)
	if final.Status != ingressStatusDelivered {
		t.Errorf("final status = %s, want delivered", final.Status)
	}
	if final.RecoveryAttempts != 0 || final.RecoveryReason != "" || !final.RecoveryNextAt.IsZero() || final.Reason != "" {
		t.Errorf("recovery fields not cleared: attempts=%d reason=%q next=%v park=%q",
			final.RecoveryAttempts, final.RecoveryReason, final.RecoveryNextAt, final.Reason)
	}
}
