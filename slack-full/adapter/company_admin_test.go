package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// company_admin_test.go — Slack company-rooms Phase 3b operator-surface
// coverage: the two-leg redrive (frozen-target reset preserving the recorded
// IdempotencyKey byte-for-byte; attempts_exhausted requiring --include-failed;
// target-less reset-to-received re-entering correlation), 409 on a held
// single-flight, 404 past retention, and the filtered receipts listing.

func doRedrive(t *testing.T, h *companyHarness, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/internal/company/redrive", bytes.NewReader(raw))
	w := httptest.NewRecorder()
	h.gw.handleCompanyRedrive(w, req)
	var out map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &out)
	}
	return w.Code, out
}

func doReceipts(t *testing.T, h *companyHarness, query string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/internal/company/receipts"+query, nil)
	w := httptest.NewRecorder()
	h.gw.handleCompanyReceipts(w, req)
	var out map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &out)
	}
	return w.Code, out
}

func resetTargetSet(out map[string]any) map[string]bool {
	set := map[string]bool{}
	if list, ok := out["reset_targets"].([]any); ok {
		for _, v := range list {
			if s, ok := v.(string); ok {
				set[s] = true
			}
		}
	}
	return set
}

func admitTargetsReceipt(t *testing.T, h *companyHarness, ts string, targets map[string]TargetDelivery) *IngressReceipt {
	t.Helper()
	ev := slackMessageEvent{Type: "message", User: "Uhuman", Channel: testChannelID, TS: ts, Text: "x"}
	raw, _ := json.Marshal(ev)
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts}
	r := &IngressReceipt{Origin: origin, Event: raw, Status: ingressStatusRouting, Targets: targets}
	if created, _, err := h.receipts.Admit(r); err != nil || !created {
		t.Fatalf("admit targets receipt: created=%v err=%v", created, err)
	}
	return r
}

// TestRedriveResetsSelectedTargetsPreservesIdempotencyKey — leg 1: a failed
// bound target is reset to pending and re-delivered under the SAME recorded
// IdempotencyKey (never re-derived); a delivered sibling target is untouched.
func TestRedriveResetsSelectedTargetsPreservesIdempotencyKey(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)

	const frozenKey = "ingress:frozen:custom-key"
	r := admitTargetsReceipt(t, h, "1700000000.001000", map[string]TargetDelivery{
		companyBoundTargetKeyPrefix + "ollie-main": {
			Session: "ollie-main", Kind: "ambient", Status: companyTargetFailed,
			Detail: "message_failed: queued=false", Attempts: 3, IdempotencyKey: frozenKey, Agent: "ollie",
			RequestID: "req-prior-failure", EventCursor: "123",
		},
		companyBoundTargetKeyPrefix + "riley-main": {
			Session: "riley-main", Kind: "ambient", Status: companyTargetDelivered,
			IdempotencyKey: "ingress:done:riley", Agent: "riley",
		},
	})

	code, out := doRedrive(t, h, map[string]any{"receipt": r.ID, "targets": []string{}, "include_failed": false})
	if code != http.StatusOK {
		t.Fatalf("redrive HTTP %d, body=%+v", code, out)
	}
	if out["leg"] != "targets" {
		t.Errorf("leg = %v, want targets", out["leg"])
	}
	if set := resetTargetSet(out); !set["ollie-main"] || set["riley-main"] {
		t.Errorf("reset_targets = %v, want only ollie-main", out["reset_targets"])
	}
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 1 {
		t.Fatalf("delivery calls = %d, want 1 (only the reset target)", len(calls))
	}
	if calls[0].idemKey != frozenKey {
		t.Errorf("Idempotency-Key = %q, want preserved %q", calls[0].idemKey, frozenKey)
	}
	final, _ := h.gw.store().Get(r.Origin)
	if final.Status != ingressStatusDelivered {
		t.Errorf("final status = %s, want delivered", final.Status)
	}
	if td := final.Targets[companyBoundTargetKeyPrefix+"ollie-main"]; td.IdempotencyKey != frozenKey {
		t.Errorf("reset target key mutated to %q, want %q", td.IdempotencyKey, frozenKey)
	} else if td.RequestID != "" || td.EventCursor != "" {
		t.Errorf("redriven target retained old async correlation: request=%q cursor=%q", td.RequestID, td.EventCursor)
	}
}

func TestRedriveAttemptsExhaustedRequiresIncludeFailed(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)

	r := admitTargetsReceipt(t, h, "1700000000.001010", map[string]TargetDelivery{
		companyBoundTargetKeyPrefix + "ollie-main": {
			Session: "ollie-main", Kind: "ambient", Status: companyTargetFailed,
			Detail:         companyReasonAttemptsExhausted + " after 12 attempts: gc 500",
			Attempts:       12,
			IdempotencyKey: "ingress:exhausted:ollie", Agent: "ollie",
		},
	})

	// Without --include-failed the exhausted target is left alone.
	code, out := doRedrive(t, h, map[string]any{"receipt": r.ID, "include_failed": false})
	if code != http.StatusOK {
		t.Fatalf("redrive HTTP %d", code)
	}
	if set := resetTargetSet(out); len(set) != 0 {
		t.Errorf("reset_targets = %v, want empty without include_failed", out["reset_targets"])
	}
	h.wait()
	if n := len(gc.sessionCalls()); n != 0 {
		t.Fatalf("delivery = %d, want 0 (exhausted not touched)", n)
	}
	if got, _ := h.gw.store().Get(r.Origin); got.Targets[companyBoundTargetKeyPrefix+"ollie-main"].Status != companyTargetFailed {
		t.Error("exhausted target changed without include_failed")
	}

	// With --include-failed it is reset and re-delivered.
	code, out = doRedrive(t, h, map[string]any{"receipt": r.ID, "include_failed": true})
	if code != http.StatusOK {
		t.Fatalf("redrive(include_failed) HTTP %d", code)
	}
	if set := resetTargetSet(out); !set["ollie-main"] {
		t.Errorf("reset_targets = %v, want ollie-main with include_failed", out["reset_targets"])
	}
	h.wait()
	if n := len(gc.sessionCalls()); n != 1 {
		t.Errorf("delivery = %d, want 1 after include_failed", n)
	}
}

// TestRedriveReresolvesUnboundTargetAfterBindingRepair — leg 1, F2: a failed-
// unbound target (frozen with Session=="" when its binding was stale at route
// time) is re-resolved from its recorded Agent name against the CURRENT bindings
// (now repaired), bound to the resolved session with a freshly-derived
// idempotency key, reset to pending, and delivered.
func TestRedriveReresolvesUnboundTargetAfterBindingRepair(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile() // current bindings resolve ollie -> ollie-main (repaired)
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)

	r := admitTargetsReceipt(t, h, "1700000000.001030", map[string]TargetDelivery{
		companyUnboundTargetKeyPrefix + "ollie": {
			Kind:   "ambient",
			Status: companyTargetFailed,
			Detail: "no company binding for (room=orchestrator-team, agent=ollie)",
			Agent:  "ollie",
			// No Session and no IdempotencyKey — this is the unbound freeze shape.
		},
	})

	code, out := doRedrive(t, h, map[string]any{"receipt": r.ID})
	if code != http.StatusOK {
		t.Fatalf("redrive HTTP %d, body=%+v", code, out)
	}
	if set := resetTargetSet(out); !set["ollie-main"] {
		t.Errorf("reset_targets = %v, want ollie-main (re-resolved)", out["reset_targets"])
	}
	h.wait()

	wantKey := companyIdempotencyKey(r.ID, "ollie-main")
	calls := gc.sessionCalls()
	if len(calls) != 1 {
		t.Fatalf("delivery calls = %d, want 1 (re-resolved target delivered)", len(calls))
	}
	if calls[0].idemKey != wantKey {
		t.Errorf("Idempotency-Key = %q, want derived %q", calls[0].idemKey, wantKey)
	}
	final, _ := h.gw.store().Get(r.Origin)
	if final.Status != ingressStatusDelivered {
		t.Errorf("final status = %s, want delivered", final.Status)
	}
	td, ok := final.Targets[companyBoundTargetKeyPrefix+"ollie-main"]
	if !ok || td.Session != "ollie-main" || td.IdempotencyKey != wantKey {
		t.Errorf("re-resolved target = %+v (ok=%v), want bound ollie-main with derived key", td, ok)
	}
	if _, stillUnbound := final.Targets[companyUnboundTargetKeyPrefix+"ollie"]; stillUnbound {
		t.Error("unbound target key was not relocated after re-resolution")
	}
}

// TestRedriveStillUnboundReturns422 — leg 1, F2: an unbound failed target whose
// agent still does not resolve to a session is NOT a success-shaped empty reset;
// the endpoint returns 422 with a machine-readable reason and the operator has an
// explicit signal to repair the binding.
func TestRedriveStillUnboundReturns422(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile() // "ghost" has no binding
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)

	r := admitTargetsReceipt(t, h, "1700000000.001031", map[string]TargetDelivery{
		companyUnboundTargetKeyPrefix + "ghost": {
			Kind:   "ambient",
			Status: companyTargetFailed,
			Detail: "no company binding for (room=orchestrator-team, agent=ghost)",
			Agent:  "ghost",
		},
	})

	code, out := doRedrive(t, h, map[string]any{"receipt": r.ID})
	if code != http.StatusUnprocessableEntity {
		t.Fatalf("still-unbound redrive HTTP %d, want 422 (body=%+v)", code, out)
	}
	if out["reason"] != "unresolved_targets" {
		t.Errorf("reason = %v, want unresolved_targets", out["reason"])
	}
	unres := map[string]bool{}
	if list, ok := out["unresolvable"].([]any); ok {
		for _, v := range list {
			if s, ok := v.(string); ok {
				unres[s] = true
			}
		}
	}
	if !unres["ghost"] {
		t.Errorf("unresolvable = %v, want ghost", out["unresolvable"])
	}
	h.wait()
	if n := len(gc.sessionCalls()); n != 0 {
		t.Errorf("delivery = %d, want 0 (nothing deliverable)", n)
	}
	// The receipt must be untouched (no generation churn, still failed).
	if got, _ := h.gw.store().Get(r.Origin); got.Status != ingressStatusRouting {
		// admitted as routing; a 422 must not flip it. (No commit occurred.)
		t.Errorf("status = %s, want routing (unchanged)", got.Status)
	}
}

// TestRedriveRejectsMalformedReceiptID — F4: a traversal- or NUL-shaped receipt
// id is rejected with 400 before any filesystem path is derived from it.
func TestRedriveRejectsMalformedReceiptID(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	for _, bad := range []string{
		"../../../../etc/hostname",
		"in-../../etc/passwd",
		"in-ok/../../escape",
		"not-a-receipt",
		"in-",
		"in-bad\x00null",
	} {
		code, out := doRedrive(t, h, map[string]any{"receipt": bad})
		if code != http.StatusBadRequest {
			t.Errorf("redrive receipt=%q HTTP %d, want 400 (body=%+v)", bad, code, out)
		}
	}
}

// TestRedriveTargetlessResetToReceived — leg 2: a target-less
// correlation_recovery_exhausted receipt is reset to received and re-enters
// correlation; with the record now materialized it claims and delivers.
func TestRedriveTargetlessResetToReceived(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.authors = fakeAuthorResolver{byBot: map[string]companyBotInfo{
		"B0RILEY": {UserID: botRiley, AppID: "A0AAAAAA2"},
	}}
	writeHarnessRecord(t, h, pendingRecord(delegationTS))

	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: resultTS}
	ev := botEvent("B0RILEY", botRiley, "A0AAAAAA2", resultTS, humanRootTS,
		"<@"+botOllie+"> done", resultMetadata(fixtureNonce, delegationTS))
	raw, _ := json.Marshal(ev)
	if created, _, err := h.receipts.Admit(&IngressReceipt{
		Origin: origin, Event: raw, Status: ingressStatusFailed,
		Reason: companyReasonRecoveryExhausted, RecoveryReason: peerParkCorrelationPending,
		RecoveryAttempts: companyPeerRecoveryMaxAttempts,
	}); err != nil || !created {
		t.Fatalf("admit exhausted receipt: %v", err)
	}

	code, out := doRedrive(t, h, map[string]any{
		"origin": map[string]string{"team_id": testTeamID, "channel_id": testChannelID, "ts": resultTS},
	})
	if code != http.StatusOK {
		t.Fatalf("redrive HTTP %d, body=%+v", code, out)
	}
	if out["leg"] != "correlation" {
		t.Errorf("leg = %v, want correlation", out["leg"])
	}
	h.wait()

	if n := len(gc.sessionCalls()); n != 1 {
		t.Fatalf("delivery = %d, want 1 (re-entered correlation and delivered)", n)
	}
	final, _ := h.gw.store().Get(origin)
	if final.Status != ingressStatusDelivered {
		t.Errorf("final status = %s, want delivered", final.Status)
	}
	if final.RecoveryAttempts != 0 || final.RecoveryReason != "" || final.Reason != "" {
		t.Errorf("recovery not cleared: attempts=%d reason=%q park=%q",
			final.RecoveryAttempts, final.RecoveryReason, final.Reason)
	}
}

func TestRedrive404PastRetention(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	code, out := doRedrive(t, h, map[string]any{
		"origin": map[string]string{"team_id": testTeamID, "channel_id": testChannelID, "ts": "1700000000.009999"},
	})
	if code != http.StatusNotFound {
		t.Fatalf("redrive of absent receipt HTTP %d, want 404 (body=%+v)", code, out)
	}
}

func TestRedrive409SingleFlightHeld(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	r := admitTargetsReceipt(t, h, "1700000000.001020", map[string]TargetDelivery{
		companyBoundTargetKeyPrefix + "ollie-main": {
			Session: "ollie-main", Kind: "ambient", Status: companyTargetFailed, IdempotencyKey: "k", Agent: "ollie",
		},
	})
	// Hold the single-flight as a concurrent delivery would.
	if !h.gw.acquireSingleFlight(r.ID) {
		t.Fatal("could not seed single-flight")
	}
	defer h.gw.releaseSingleFlight(r.ID)

	code, _ := doRedrive(t, h, map[string]any{"receipt": r.ID})
	if code != http.StatusConflict {
		t.Fatalf("redrive with held single-flight HTTP %d, want 409", code)
	}
}

// TestCompanyReceiptsListingFilters — the receipts endpoint returns the
// operator view and honors the origin / root / status filters.
func TestCompanyReceiptsListingFilters(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	rootA := "1700000000.000001"
	rootB := "1700000000.000002"
	mk := func(ts, threadTS, status string, mutate func(*IngressReceipt)) *IngressReceipt {
		ev := slackMessageEvent{Type: "message", User: "Uhuman", Channel: testChannelID, TS: ts, ThreadTS: threadTS, Text: "x"}
		raw, _ := json.Marshal(ev)
		r := &IngressReceipt{Origin: ReceiptOrigin{testTeamID, testChannelID, ts}, Event: raw, Status: status}
		if mutate != nil {
			mutate(r)
		}
		if created, _, err := h.receipts.Admit(r); err != nil || !created {
			t.Fatalf("admit %s: %v", ts, err)
		}
		return r
	}
	r1 := mk("1700000000.001100", rootA, ingressStatusRouting, nil)
	_ = mk("1700000000.001101", rootA, ingressStatusReceived, func(r *IngressReceipt) {
		r.Reason = peerParkCorrelationPending
		r.RecoveryReason = peerParkCorrelationPending
		r.RecoveryAttempts = 2
		r.RecoveryNextAt = time.Now().UTC().Add(time.Minute)
	})
	r3 := mk("1700000000.001102", rootB, ingressStatusDelivered, nil)

	// No filter: all three.
	code, out := doReceipts(t, h, "")
	if code != http.StatusOK {
		t.Fatalf("receipts HTTP %d", code)
	}
	if got := len(receiptList(out)); got != 3 {
		t.Fatalf("unfiltered receipts = %d, want 3", got)
	}

	// status filter.
	_, out = doReceipts(t, h, "?status=routing")
	if list := receiptList(out); len(list) != 1 || list[0]["id"] != r1.ID {
		t.Errorf("status=routing = %+v, want only r1", list)
	}

	// origin filter.
	_, out = doReceipts(t, h, "?origin="+testTeamID+":"+testChannelID+":"+r3.Origin.TS)
	if list := receiptList(out); len(list) != 1 || list[0]["id"] != r3.ID {
		t.Errorf("origin filter = %+v, want only r3", list)
	}

	// root filter (derived thread root).
	_, out = doReceipts(t, h, "?root="+testTeamID+":"+testChannelID+":"+rootA)
	if list := receiptList(out); len(list) != 2 {
		t.Errorf("root=A receipts = %d, want 2", len(list))
	}

	// Shape: parked receipt surfaces the recovery fields.
	_, out = doReceipts(t, h, "?status=received")
	list := receiptList(out)
	if len(list) != 1 {
		t.Fatalf("status=received = %d, want 1", len(list))
	}
	pv := list[0]
	if pv["reason"] != peerParkCorrelationPending {
		t.Errorf("view reason = %v, want correlation_pending", pv["reason"])
	}
	if got, _ := pv["recovery_attempts"].(float64); int(got) != 2 {
		t.Errorf("view recovery_attempts = %v, want 2", pv["recovery_attempts"])
	}
	if _, ok := pv["origin"].(map[string]any); !ok {
		t.Errorf("view missing origin object: %+v", pv)
	}
	if _, ok := pv["targets"].([]any); !ok {
		t.Errorf("view missing targets array: %+v", pv)
	}
}

func receiptList(out map[string]any) []map[string]any {
	var list []map[string]any
	if raw, ok := out["receipts"].([]any); ok {
		for _, v := range raw {
			if m, ok := v.(map[string]any); ok {
				list = append(list, m)
			}
		}
	}
	return list
}

// --- Phase 5 body redaction verb -------------------------------------------

func doRedact(t *testing.T, h *companyHarness, body map[string]any) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/internal/company/redact", bytes.NewReader(raw))
	w := httptest.NewRecorder()
	h.gw.handleCompanyRedact(w, req)
	var out map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &out)
	}
	return w.Code, out
}

// TestRedactVerbTruncatesBodyAndDegradesReads — the operator redact verb
// truncates a body-split receipt's sidecar to the tombstone; a later read
// through the accessor degrades to a JSON null (context_unavailable / no-match).
func TestRedactVerbTruncatesBodyAndDegradesReads(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	r := admitTargetsReceipt(t, h, "1700000000.002000", map[string]TargetDelivery{
		companyBoundTargetKeyPrefix + "ollie-main": {
			Session: "ollie-main", Kind: "ambient", Status: companyTargetDelivered, IdempotencyKey: "k", Agent: "ollie",
		},
	})
	// Redaction is guarded to terminal receipts past the reconciliation horizon
	// (C4/C6): drive this one terminal (delivered) and backdate ReceivedAt beyond
	// the horizon so the verb is allowed to redact.
	r.Status = ingressStatusDelivered
	r.ReceivedAt = time.Now().Add(-48 * time.Hour).UTC()
	if err := h.receipts.Update(r); err != nil {
		t.Fatalf("make receipt terminal+aged: %v", err)
	}
	code, out := doRedact(t, h, map[string]any{"receipt": r.ID})
	if code != http.StatusOK {
		t.Fatalf("redact HTTP %d, want 200 (body=%+v)", code, out)
	}
	if out["redacted"] != true {
		t.Errorf("redact response = %+v; want redacted true", out)
	}
	got, _ := h.receipts.GetByID(r.ID)
	if _, st := h.receipts.loadBody(got); st != bodyRedacted {
		t.Fatalf("post-redact loadBody = %v; want bodyRedacted", st)
	}
	if string(h.receipts.receiptBody(got)) != "null" {
		t.Errorf("post-redact receiptBody = %q; want null", h.receipts.receiptBody(got))
	}
}

// TestRedact404PastRetention — redacting an absent receipt is a 404.
func TestRedact404PastRetention(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	code, _ := doRedact(t, h, map[string]any{
		"origin": map[string]string{"team_id": testTeamID, "channel_id": testChannelID, "ts": "1700000000.008888"},
	})
	if code != http.StatusNotFound {
		t.Fatalf("redact of absent receipt HTTP %d, want 404", code)
	}
}

// TestRedactLegacyEmbedded409 — a legacy embedded receipt has no separable body,
// so the verb refuses it with 409 (it ages out at retention instead).
func TestRedactLegacyEmbedded409(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.007777"}
	legacy := &IngressReceipt{
		ID: receiptID(origin), Generation: 1, Origin: origin,
		Status: ingressStatusRouting,
		Event:  json.RawMessage(`{"type":"message","text":"legacy"}`),
	}
	data, _ := json.MarshalIndent(legacy, "", "  ")
	if err := os.WriteFile(h.receipts.pathForID(legacy.ID), data, 0o600); err != nil {
		t.Fatalf("seed legacy receipt: %v", err)
	}
	code, out := doRedact(t, h, map[string]any{"receipt": legacy.ID})
	if code != http.StatusConflict {
		t.Fatalf("redact of legacy embedded receipt HTTP %d, want 409 (body=%+v)", code, out)
	}
}

// TestHealthzSurfacesBodyGauges — after a sweep, /healthz reports the body
// integrity gauges folded into the single SweepAndPending scan: a redacted
// receipt on company_bodies_redacted and a body-missing receipt on
// company_body_missing.
func TestHealthzSurfacesBodyGauges(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	redacted := admitTargetsReceipt(t, h, "1700000000.003000", map[string]TargetDelivery{})
	if err := h.receipts.redactReceiptBody(redacted); err != nil {
		t.Fatalf("redact: %v", err)
	}
	missing := admitTargetsReceipt(t, h, "1700000000.003001", map[string]TargetDelivery{})
	if err := os.Remove(h.receipts.bodyPathForID(missing.ID)); err != nil {
		t.Fatalf("remove body: %v", err)
	}

	h.gw.sweepOnce()

	detail := h.gw.healthzDetail()
	if !strings.Contains(detail, "company_bodies_redacted=1") {
		t.Errorf("healthz missing company_bodies_redacted=1: %q", detail)
	}
	if !strings.Contains(detail, "company_body_missing=1") {
		t.Errorf("healthz missing company_body_missing=1: %q", detail)
	}
}

// adminErrorText pulls the machine-readable error string from a verb response.
func adminErrorText(out map[string]any) string {
	if s, ok := out["error"].(string); ok {
		return s
	}
	return ""
}

// TestRedactRefusesNonTerminalReceipt pins C4/C6: redaction of a non-terminal
// (routing) receipt is refused with 409 (the core_bound fence), and the body is
// left intact — truncating it would recompute routing / re-render a redrive from a
// null body.
func TestRedactRefusesNonTerminalReceipt(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	r := admitTargetsReceipt(t, h, "1700000000.007000", map[string]TargetDelivery{
		companyBoundTargetKeyPrefix + "ollie-main": {
			Session: "ollie-main", Kind: "ambient", Status: companyTargetPending, IdempotencyKey: "k", Agent: "ollie",
		},
	}) // admitTargetsReceipt sets Status routing (non-terminal)

	code, out := doRedact(t, h, map[string]any{"receipt": r.ID})
	if code != http.StatusConflict {
		t.Fatalf("redact of routing receipt HTTP %d, want 409 (body=%+v)", code, out)
	}
	if !strings.Contains(adminErrorText(out), "not terminal") {
		t.Errorf("409 reason = %q; want a not-terminal explanation", adminErrorText(out))
	}
	got, _ := h.receipts.GetByID(r.ID)
	if _, st := h.receipts.loadBody(got); st == bodyRedacted {
		t.Errorf("non-terminal receipt was redacted despite the 409")
	}
}

// TestRedactRefusesWithinReconciliationHorizon pins C6: a terminal receipt younger
// than the reconciliation horizon is refused (409) so a stuck posting intent can
// still reconcile against its body's nonce — the self-echo wedge class.
func TestRedactRefusesWithinReconciliationHorizon(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	r := admitTargetsReceipt(t, h, "1700000000.007100", map[string]TargetDelivery{})
	// Terminal but FRESH (ReceivedAt ~ now, well inside the 24h horizon).
	r.Status = ingressStatusNoDelivery
	r.Reason = wakeReasonDMSelfEcho
	if err := h.receipts.Update(r); err != nil {
		t.Fatalf("make terminal: %v", err)
	}
	code, out := doRedact(t, h, map[string]any{"receipt": r.ID})
	if code != http.StatusConflict {
		t.Fatalf("redact within horizon HTTP %d, want 409 (body=%+v)", code, out)
	}
	if !strings.Contains(adminErrorText(out), "horizon") {
		t.Errorf("409 reason = %q; want a reconciliation-horizon explanation", adminErrorText(out))
	}
	got, _ := h.receipts.GetByID(r.ID)
	if _, st := h.receipts.loadBody(got); st == bodyRedacted {
		t.Errorf("within-horizon receipt was redacted despite the 409")
	}
}

// TestRedactThenRedriveUsesFrozenRoot pins C7: a redrive of a redacted receipt
// re-renders under the FROZEN thread_root_ts (derived once at admission), not the
// origin ts a null body would collapse to — and keeps the recorded Idempotency-Key
// byte-for-byte.
func TestRedactThenRedriveUsesFrozenRoot(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	ts := "1700000000.005000"
	rootTS := "1700000000.004000" // thread_ts != origin ts
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts}
	ev := slackMessageEvent{Type: "message", User: "Uhuman", Channel: testChannelID, TS: ts, ThreadTS: rootTS, Text: "threaded hi"}
	raw, _ := json.Marshal(ev)
	const frozenKey = "ingress:frozen:c7-key"
	r := &IngressReceipt{
		Origin:       origin,
		Event:        raw,
		Status:       ingressStatusFailed,
		Reason:       "1 target(s) failed delivery",
		ReceivedAt:   time.Now().Add(-48 * time.Hour).UTC(),
		ThreadRootTS: deriveHumanRootTS(decodeCompanyMessage(origin, raw)),
		Targets: map[string]TargetDelivery{
			companyBoundTargetKeyPrefix + "ollie-main": {
				Session: "ollie-main", Kind: "ambient", Status: companyTargetFailed,
				Detail: "gc 500", Attempts: 1, IdempotencyKey: frozenKey, Agent: "ollie",
			},
		},
	}
	if created, _, err := h.receipts.Admit(r); err != nil || !created {
		t.Fatalf("admit: created=%v err=%v", created, err)
	}
	// Redact (terminal + aged): allowed.
	if code, out := doRedact(t, h, map[string]any{"receipt": r.ID}); code != http.StatusOK {
		t.Fatalf("redact HTTP %d, want 200 (body=%+v)", code, out)
	}
	// Redrive the failed target: re-renders from the frozen route.
	if code, out := doRedrive(t, h, map[string]any{"receipt": r.ID, "targets": []string{}, "include_failed": true}); code != http.StatusOK {
		t.Fatalf("redrive HTTP %d, want 200 (body=%+v)", code, out)
	}
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 1 {
		t.Fatalf("deliveries = %d, want 1", len(calls))
	}
	if !strings.Contains(calls[0].body, "thread_root_ts: "+rootTS) {
		t.Errorf("redrive lost the frozen root; body=%q", calls[0].body)
	}
	if strings.Contains(calls[0].body, "thread_root_ts: "+ts) {
		t.Errorf("redrive collapsed root to origin ts (C7 regression); body=%q", calls[0].body)
	}
	if calls[0].idemKey != frozenKey {
		t.Errorf("Idempotency-Key = %q, want preserved %q", calls[0].idemKey, frozenKey)
	}
}

// TestReceiptsListingShowsRedactedMarkerAndMatchesRoot pins m9: a redacted thread
// reply stays in the root-filtered admin listing (matched by its frozen root, not
// the null body) with a visible body_state marker — never silently dropped.
func TestReceiptsListingShowsRedactedMarkerAndMatchesRoot(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	ts := "1700000000.006000"
	rootTS := "1700000000.005500"
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts}
	ev := slackMessageEvent{Type: "message", User: "Uhuman", Channel: testChannelID, TS: ts, ThreadTS: rootTS, Text: "x"}
	raw, _ := json.Marshal(ev)
	r := &IngressReceipt{
		Origin: origin, Event: raw, Status: ingressStatusDelivered,
		ReceivedAt:   time.Now().Add(-48 * time.Hour).UTC(),
		ThreadRootTS: deriveHumanRootTS(decodeCompanyMessage(origin, raw)),
	}
	if created, _, err := h.receipts.Admit(r); err != nil || !created {
		t.Fatalf("admit: created=%v err=%v", created, err)
	}
	if err := h.receipts.redactReceiptBody(r); err != nil {
		t.Fatalf("redact: %v", err)
	}

	_, out := doReceipts(t, h, "?root="+testTeamID+":"+testChannelID+":"+rootTS)
	list := receiptList(out)
	if len(list) != 1 {
		t.Fatalf("root-filtered listing = %d, want 1 (a redacted reply must not vanish)", len(list))
	}
	if list[0]["id"] != r.ID {
		t.Errorf("listing id = %v, want %s", list[0]["id"], r.ID)
	}
	if list[0]["body_state"] != "redacted" {
		t.Errorf("body_state = %v, want redacted marker", list[0]["body_state"])
	}
}
