package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// company_delivery_test.go — Phase 2c acceptance coverage: trusted peer
// delegation (rule 3), metadata-gated result claim + clarifying-question +
// replay (rule 5), dormant-agent hydration + threaded root (rule 7), remaining
// rule-4 legs, transient bots.info park-then-deliver-once, frozen-hydration
// byte-identity, pointer-file write ordering.

// botEvent builds a bot-authored company message event.
func botEvent(botID, user, appID, ts, threadTS, text string, metadata json.RawMessage) slackMessageEvent {
	return slackMessageEvent{
		Subtype:  "bot_message",
		BotID:    botID,
		User:     user,
		AppID:    appID,
		Channel:  testChannelID,
		TS:       ts,
		ThreadTS: threadTS,
		Text:     text,
		Metadata: metadata,
	}
}

func delegationMetadata(nonce, rootTS string) json.RawMessage {
	return json.RawMessage(`{"event_type":"gc_delegation","event_payload":{"v":1,"nonce":"` + nonce + `","root_ts":"` + rootTS + `"}}`)
}

func resultMetadata(nonce, delegTS string) json.RawMessage {
	return json.RawMessage(`{"event_type":"gc_delegation_result","event_payload":{"v":1,"nonce":"` + nonce + `","delegation_ts":"` + delegTS + `"}}`)
}

func setFixedClock(h *companyHarness) {
	now, _ := time.Parse(time.RFC3339, fixedNow)
	h.gw.now = func() time.Time { return now }
}

func readPointer(t *testing.T, h *companyHarness, session string) companyCurrentTurn {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(h.turnsDir, session+".json"))
	if err != nil {
		t.Fatalf("read pointer %s: %v", session, err)
	}
	var p companyCurrentTurn
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("decode pointer %s: %v", session, err)
	}
	return p
}

func writeHarnessRecord(t *testing.T, h *companyHarness, rec *companyDelegationRecord) string {
	t.Helper()
	if err := os.MkdirAll(h.delegationsDir, 0o700); err != nil {
		t.Fatalf("mkdir delegations: %v", err)
	}
	name := companyDelegationFilename(rec.TeamID, rec.ChannelID, rec.TS)
	data, err := companyMarshalDelegation(rec)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	if err := os.WriteFile(filepath.Join(h.delegationsDir, name), data, 0o600); err != nil {
		t.Fatalf("write record: %v", err)
	}
	return name
}

// TestAcceptance3TrustedDelegationWakesResponderOnce — a trusted Ollie post
// mentioning @Riley (with delegation metadata) wakes Riley exactly once as a
// peer_delegation, and writes the current-turn pointer before delivery.
func TestAcceptance3TrustedDelegationWakesResponderOnce(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.authors = fakeAuthorResolver{byBot: map[string]companyBotInfo{
		"B0OLLIE": {UserID: botOllie, AppID: "A0AAAAAA1"},
	}}
	// Assert the pointer is durable BEFORE the gc POST (write-ordering).
	gc.hook = func(int) {
		if _, err := os.Stat(filepath.Join(h.turnsDir, "riley-main.json")); err != nil {
			t.Errorf("pointer not written before gc POST: %v", err)
		}
	}
	h.openBarrier()

	ts := "1700000000.000500"
	ev := botEvent("B0OLLIE", botOllie, "A0AAAAAA1", ts, humanRootTS,
		"<@"+botRiley+"> please review", delegationMetadata(fixtureNonce, humanRootTS))
	w, handled := h.admitViaHandler(t, ev, 0)
	if !handled || w.Result().StatusCode != http.StatusOK {
		t.Fatalf("admit: handled=%v status=%d", handled, w.Result().StatusCode)
	}
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 1 {
		t.Fatalf("session POSTs = %d, want exactly 1", len(calls))
	}
	if !strings.Contains(calls[0].path, "/session/riley-main/") {
		t.Errorf("delivered to %q, want riley-main", calls[0].path)
	}
	if !strings.Contains(calls[0].body, "peer_delegation delivery") || !strings.Contains(calls[0].body, "peer_authority: peer_only") {
		t.Errorf("body missing peer markers: %q", calls[0].body)
	}
	ptr := readPointer(t, h, "riley-main")
	wantKey := companyDelegationFilename(testTeamID, testChannelID, ts)
	if ptr.Kind != wakeKindPeerDelegation || ptr.Agent != "riley" || ptr.DelegationKey != wantKey {
		t.Errorf("pointer = %+v, want kind peer_delegation agent riley key %s", ptr, wantKey)
	}
	if ptr.ThreadRootTS != humanRootTS {
		t.Errorf("pointer thread_root_ts = %q, want %q", ptr.ThreadRootTS, humanRootTS)
	}
	if !validCompanyTurnReference(ptr.TurnRef) {
		t.Fatalf("pointer turn_ref = %q, want a valid immutable reference", ptr.TurnRef)
	}
	if _, err := os.Stat(filepath.Join(h.turnsDir, "by-ref", ptr.TurnRef+".json")); err != nil {
		t.Errorf("immutable turn record was not durable before delivery: %v", err)
	}
	for _, want := range []string{
		"turn_ref: " + ptr.TurnRef,
		"channel_id: " + testChannelID,
		"thread_root_ts: " + humanRootTS,
		"gc slack reply-current --turn-ref " + ptr.TurnRef + " --body-file <file>",
	} {
		if !strings.Contains(calls[0].body, want) {
			t.Errorf("delivered reminder missing %q: %s", want, calls[0].body)
		}
	}
}

// TestAcceptance5ResultClaimAndClarifyAndReplay — Riley's metadata-gated result
// wakes only Ollie and claims the record; a clarifying question claims nothing;
// a replay claims nothing twice.
func TestAcceptance5ResultClaimAndClarifyAndReplay(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.authors = fakeAuthorResolver{byBot: map[string]companyBotInfo{
		"B0RILEY": {UserID: botRiley, AppID: "A0AAAAAA2"},
	}}
	name := writeHarnessRecord(t, h, pendingRecord(delegationTS))
	h.openBarrier()

	// Result: Riley -> @Ollie in the human-root thread with result metadata.
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: resultTS}
	ev := botEvent("B0RILEY", botRiley, "A0AAAAAA2", resultTS, humanRootTS,
		"<@"+botOllie+"> done", resultMetadata(fixtureNonce, delegationTS))
	if w, handled := h.admitViaHandler(t, ev, 0); !handled || w.Result().StatusCode != http.StatusOK {
		t.Fatalf("admit result: handled=%v", handled)
	}
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 1 || !strings.Contains(calls[0].path, "/session/ollie-main/") {
		t.Fatalf("result woke %d sessions, want exactly ollie-main: %+v", len(calls), calls)
	}
	if !strings.Contains(calls[0].body, "peer_result delivery") {
		t.Errorf("result body missing peer_result kind: %q", calls[0].body)
	}
	rec := readRecord(t, companyPeerEnv{delegationsDir: h.delegationsDir}, name)
	if rec.Status != companyDelegationClaimed || rec.ResultTS != resultTS {
		t.Errorf("record = %+v, want result_claimed ts=%s", rec, resultTS)
	}
	ptr := readPointer(t, h, "ollie-main")
	if ptr.Kind != wakeKindPeerResult || ptr.DelegationKey != name {
		t.Errorf("pointer = %+v, want peer_result key %s", ptr, name)
	}

	// Clarifying question (no metadata) on a fresh record: claims nothing.
	gc2 := newFakeGC(t)
	h2 := newCompanyHarness(t, gc2.server.URL, &df, &bf, 4)
	setFixedClock(h2)
	h2.gw.authors = h.gw.authors
	name2 := writeHarnessRecord(t, h2, pendingRecord(delegationTS))
	h2.openBarrier()
	clar := botEvent("B0RILEY", botRiley, "A0AAAAAA2", "1700000000.000901", humanRootTS, "<@"+botOllie+"> quick question", nil)
	if _, handled := h2.admitViaHandler(t, clar, 0); !handled {
		t.Fatal("admit clarifying")
	}
	h2.wait()
	if c := gc2.sessionCalls(); len(c) != 1 || !strings.Contains(c[0].body, "peer_input delivery") {
		t.Errorf("clarifying delivered %+v, want one peer_input", c)
	}
	// The clarifying wake is uncorrelated: kind peer_input with NO delegation_key
	// (the schema round-trip Python's parser accepts without a key).
	clarPtr := readPointer(t, h2, "ollie-main")
	if clarPtr.Kind != wakeKindPeerInput || clarPtr.DelegationKey != "" {
		t.Errorf("clarifying pointer = %+v, want kind peer_input and no delegation_key", clarPtr)
	}
	if rec := readRecord(t, companyPeerEnv{delegationsDir: h2.delegationsDir}, name2); rec.Status != companyDelegationPending {
		t.Errorf("clarifying claimed the record: %s", rec.Status)
	}

	// Replay of the original claimed result: no second gc delivery, no re-claim.
	genBefore := rec.Generation
	h.gw.deliverReceipt(origin)
	h.wait()
	if c := gc.sessionCalls(); len(c) != 1 {
		t.Errorf("replay produced %d session POSTs, want still 1", len(c))
	}
	if again := readRecord(t, companyPeerEnv{delegationsDir: h.delegationsDir}, name); again.Generation != genBefore {
		t.Errorf("replay re-claimed record (gen %d -> %d)", genBefore, again.Generation)
	}
}

// TestAcceptance7DormantAgentHydration — a human mention of a dormant agent
// delivers the current message + verified human root + bounded excerpt exactly
// once, and derives the parent root for a threaded trigger.
func TestAcceptance7DormantAgentHydration(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	var hydrateCalls int
	h.gw.hydrate = func(msg CompanyMessage) companyHydration {
		hydrateCalls++
		return companyHydration{
			RootProvenance: companyRootProvenanceVerified,
			Root:           &companyHydrationRoot{TS: humanRootTS, User: "Uhuman", Text: "kick off the review"},
			ContextStatus:  companyContextAvailable,
			Excerpt:        []companyExcerptLine{{TS: "1700000000.000101", User: "Uhuman", Text: "earlier note"}},
		}
	}
	h.openBarrier()

	// Threaded human trigger mentioning dormant Riley (ollie is ambient).
	ev := humanMessage("1700000000.000700", "<@"+botRiley+"> take a look")
	ev.ThreadTS = humanRootTS
	if w, handled := h.admitViaHandler(t, ev, 0); !handled || w.Result().StatusCode != http.StatusOK {
		t.Fatalf("admit: handled=%v", handled)
	}
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 1 || !strings.Contains(calls[0].path, "/session/riley-main/") {
		t.Fatalf("woke %d, want exactly riley-main: %+v", len(calls), calls)
	}
	body := calls[0].body
	for _, want := range []string{"human_root_verified", "kick off the review", "context_available", "earlier note", "targeted delivery"} {
		if !strings.Contains(body, want) {
			t.Errorf("hydrated body missing %q: %q", want, body)
		}
	}
	ptr := readPointer(t, h, "riley-main")
	if ptr.ThreadRootTS != humanRootTS {
		t.Errorf("threaded trigger derived root %q, want %q", ptr.ThreadRootTS, humanRootTS)
	}

	// Redrive: hydration is frozen (fetched once), body re-renders identically.
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.000700"}
	h.gw.deliverReceipt(origin)
	h.wait()
	if hydrateCalls != 1 {
		t.Errorf("hydrate called %d times, want exactly 1 (frozen)", hydrateCalls)
	}
}

// TestRule4UnknownBotNoDelivery — a bot the resolver cannot resolve delivers
// nothing (unknown_bot).
func TestRule4UnknownBotNoDelivery(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.authors = fakeAuthorResolver{} // resolves nothing -> unknown
	h.openBarrier()

	ev := botEvent("B999", "U0OUTSIDER", "", "1700000000.000800", "", "<@"+botRiley+"> hi", nil)
	if _, handled := h.admitViaHandler(t, ev, 0); !handled {
		t.Fatal("admit")
	}
	h.wait()
	r, _ := h.receipts.Get(ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.000800"})
	if r == nil || r.Status != ingressStatusNoDelivery || r.Reason != wakeReasonUnknownBot {
		t.Fatalf("status=%v reason=%q, want no_delivery/unknown_bot", statusOf(r), reasonOf(r))
	}
	if len(gc.sessionCalls()) != 0 {
		t.Errorf("unknown bot delivered %d", len(gc.sessionCalls()))
	}
}

// TestTransientAuthorResolutionParksThenDeliversOnce — a transient bots.info
// failure parks the receipt non-terminally; a later redrive that resolves
// delivers exactly once.
func TestTransientAuthorResolutionParksThenDeliversOnce(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	flip := &flipResolver{transientFirst: true, info: companyBotInfo{UserID: botOllie, AppID: "A0AAAAAA1"}}
	h.gw.authors = flip
	h.openBarrier()

	ts := "1700000000.000500"
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts}
	ev := botEvent("B0OLLIE", botOllie, "A0AAAAAA1", ts, humanRootTS,
		"<@"+botRiley+"> please review", delegationMetadata(fixtureNonce, humanRootTS))
	if _, handled := h.admitViaHandler(t, ev, 0); !handled {
		t.Fatal("admit")
	}
	h.wait()

	r, _ := h.receipts.Get(origin)
	if r == nil || r.Status != ingressStatusReceived || r.Reason != peerParkResolutionPending {
		t.Fatalf("first pass status=%v reason=%q, want parked author_resolution_pending", statusOf(r), reasonOf(r))
	}
	if len(gc.sessionCalls()) != 0 {
		t.Fatalf("delivered during transient park: %d", len(gc.sessionCalls()))
	}

	// Redrive: the resolver now resolves; delivers exactly once.
	h.gw.deliverReceipt(origin)
	h.wait()
	if c := gc.sessionCalls(); len(c) != 1 {
		t.Errorf("post-resolution delivered %d, want exactly 1", len(c))
	}
}

// TestBodyMissingPendingReceiptParks pins C5/m8: a live room receipt whose body
// sidecar is missing does NOT route an empty message to a terminal no_delivery —
// it PARKS (non-terminal, counted, sweep-retried), and delivers once the body is
// restored (an orphan-adoption repair or operator action).
func TestBodyMissingPendingReceiptParks(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	h.openBarrier()

	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.004000"}
	ev := humanMessage(origin.TS, "hello team")
	rawEv, _ := json.Marshal(ev)
	created, _, err := h.receipts.Admit(&IngressReceipt{
		Origin: origin, Event: rawEv, Status: ingressStatusReceived,
		ThreadRootTS: deriveHumanRootTS(decodeCompanyMessage(origin, rawEv)),
	})
	if err != nil || !created {
		t.Fatalf("admit: created=%v err=%v", created, err)
	}
	// Body sidecar goes missing before delivery (crash orphan, or a lost pair).
	if err := os.Remove(h.receipts.bodyPathForID(receiptID(origin))); err != nil {
		t.Fatalf("remove body: %v", err)
	}

	if out := h.gw.deliverReceipt(origin); out != deliverParkedPreclaim {
		t.Fatalf("deliverReceipt outcome = %v, want parked", out)
	}
	r, _ := h.receipts.Get(origin)
	if r == nil || isTerminalStatus(r.Status) {
		t.Fatalf("body-missing receipt terminalized: status=%q reason=%q", statusOf(r), reasonOf(r))
	}
	if r.Status != ingressStatusReceived || r.Reason != companyReasonBodyIntegrity {
		t.Fatalf("status/reason = %q/%q, want received/parked_body_integrity", statusOf(r), reasonOf(r))
	}
	if len(gc.sessionCalls()) != 0 {
		t.Fatalf("body-missing receipt woke %d sessions, want 0", len(gc.sessionCalls()))
	}

	// Restore the body → redrive delivers exactly once.
	if err := h.receipts.writeBodyOnce(receiptID(origin), rawEv); err != nil {
		t.Fatalf("restore body: %v", err)
	}
	h.gw.deliverReceipt(origin)
	h.wait()
	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("post-restore deliveries = %d, want 1", got)
	}
	if final, _ := h.receipts.Get(origin); final == nil || final.Status != ingressStatusDelivered {
		t.Errorf("post-restore status = %q, want delivered", statusOf(final))
	}
}

// TestFrozenHydrationByteIdentityAcrossRedrives — two delivery attempts of the
// same target render byte-identical bodies (5xx then 2xx), proving the frozen
// hydration + deterministic envelope.
func TestFrozenHydrationByteIdentityAcrossRedrives(t *testing.T) {
	gc := newFakeGC(t)
	gc.respStatus = func(n int) int {
		if n == 0 {
			return http.StatusInternalServerError // retryable -> stays pending
		}
		return http.StatusOK
	}
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	var hydrateCalls int
	h.gw.hydrate = func(CompanyMessage) companyHydration {
		hydrateCalls++
		return companyHydration{
			RootProvenance: companyRootProvenanceVerified,
			Root:           &companyHydrationRoot{TS: humanRootTS, User: "Uhuman", Text: "root text"},
			ContextStatus:  companyContextAvailable,
			Excerpt:        []companyExcerptLine{{TS: "1700000000.000101", User: "Uhuman", Text: "prior"}},
		}
	}
	h.openBarrier()

	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.000700"}
	ev := humanMessage(origin.TS, "hello ambient")
	if _, handled := h.admitViaHandler(t, ev, 0); !handled {
		t.Fatal("admit")
	}
	h.wait()
	// Redrive the still-pending target.
	h.gw.deliverReceipt(origin)
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 2 {
		t.Fatalf("delivery attempts = %d, want 2 (5xx then 2xx)", len(calls))
	}
	if calls[0].body != calls[1].body {
		t.Errorf("redrive body differs from first:\n1: %q\n2: %q", calls[0].body, calls[1].body)
	}
	if hydrateCalls != 1 {
		t.Errorf("hydrate called %d times, want 1 (frozen across redrives)", hydrateCalls)
	}
}

// TestPeerUnboundReceiverIsRecordedFailure — a peer delegation to an unbound
// receiver is a recorded delivery failure (never a silent drop / empty
// delivered receipt, and never a legacy fallback).
func TestPeerUnboundReceiverIsRecordedFailure(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	// riley is deliberately unbound.
	bf := companyBindingsFile{
		SchemaVersion: 1,
		Bindings:      []CompanyBinding{{Room: "orchestrator-team", Agent: "ollie", Session: "ollie-main"}},
	}
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.authors = fakeAuthorResolver{byBot: map[string]companyBotInfo{
		"B0OLLIE": {UserID: botOllie, AppID: "A0AAAAAA1"},
	}}
	h.openBarrier()

	ts := "1700000000.000500"
	ev := botEvent("B0OLLIE", botOllie, "A0AAAAAA1", ts, humanRootTS,
		"<@"+botRiley+"> please review", delegationMetadata(fixtureNonce, humanRootTS))
	if _, handled := h.admitViaHandler(t, ev, 0); !handled {
		t.Fatal("admit")
	}
	h.wait()

	r, _ := h.receipts.Get(ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts})
	if r == nil || r.Status != ingressStatusFailed {
		t.Fatalf("status = %v, want failed (unbound peer receiver)", statusOf(r))
	}
	var failedUnbound bool
	for _, td := range r.Targets {
		if td.Status == companyTargetFailed && td.Session == "" && strings.Contains(td.Detail, "riley") {
			failedUnbound = true
		}
	}
	if !failedUnbound {
		t.Errorf("no failed-unbound peer target recorded: %+v", r.Targets)
	}
	if len(gc.sessionCalls()) != 0 || gc.inboundCalls() != 0 {
		t.Errorf("unbound peer receiver delivered somewhere: sessions=%d inbound=%d", len(gc.sessionCalls()), gc.inboundCalls())
	}
}

// flipResolver returns a transient outcome on its first call, then resolves.
type flipResolver struct {
	transientFirst bool
	called         bool
	info           companyBotInfo
}

func (f *flipResolver) Resolve(botID string) (companyBotInfo, botResolveOutcome) {
	if f.transientFirst && !f.called {
		f.called = true
		return companyBotInfo{}, botResolveTransient
	}
	return f.info, botResolveOK
}

func reasonOf(r *IngressReceipt) string {
	if r == nil {
		return "<nil>"
	}
	return r.Reason
}

// TestHydrationFreezeWriteFailureParksNoDelivery — a failure persisting the
// frozen hydration must NOT deliver with unpersisted bytes (G3): the receipt
// stays non-terminal with no hydration on disk, nothing is POSTed, and the next
// attempt refetches and delivers exactly once.
func TestHydrationFreezeWriteFailureParksNoDelivery(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	var hydrateCalls int
	h.gw.hydrate = func(CompanyMessage) companyHydration {
		hydrateCalls++
		if hydrateCalls == 1 {
			// Routing was already committed; make the store read-only so the
			// hydration-freeze Update fails before any POST.
			_ = os.Chmod(h.ingressDir, 0o500)
		}
		return companyHydration{
			RootProvenance: companyRootProvenanceVerified,
			Root:           &companyHydrationRoot{TS: humanRootTS, User: "Uhuman", Text: "root text"},
			ContextStatus:  companyContextAvailable,
			Excerpt:        []companyExcerptLine{{TS: "1700000000.000101", User: "Uhuman", Text: "prior"}},
		}
	}
	h.openBarrier()

	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.000700"}
	ev := humanMessage(origin.TS, "hello ambient")
	if _, handled := h.admitViaHandler(t, ev, 0); !handled {
		t.Fatal("admit")
	}
	h.wait()

	if c := gc.sessionCalls(); len(c) != 0 {
		t.Fatalf("delivered with unpersisted hydration: %d POSTs", len(c))
	}
	if err := os.Chmod(h.ingressDir, 0o700); err != nil {
		t.Fatalf("chmod restore: %v", err)
	}
	r, _ := h.receipts.Get(origin)
	if r == nil || isTerminalStatus(r.Status) {
		t.Fatalf("receipt terminal after hydration-persist failure: %v", statusOf(r))
	}
	if len(r.Hydration) != 0 {
		t.Fatalf("hydration persisted despite write failure: %s", r.Hydration)
	}

	// Redrive: refetches hydration, persists, delivers exactly once.
	h.gw.deliverReceipt(origin)
	h.wait()
	if c := gc.sessionCalls(); len(c) != 1 {
		t.Errorf("post-recovery deliveries = %d, want exactly 1", len(c))
	}
	if hydrateCalls != 2 {
		t.Errorf("hydrate called %d times, want 2 (refetch after failed persist)", hydrateCalls)
	}
}

// TestAcceptance5ClaimReplayThroughWorker — the documented crash window (G9):
// the delegation record is already result_claimed (claim persisted) but the
// routing receipt has no frozen targets yet (crash before ensureTargets).
// Redriving the same result event THROUGH the delivery worker delivers the
// peer_result exactly once via the idempotent-claim branch, re-claims nothing
// (generation unchanged), and freezes a peer_result pointer — then a second
// redrive of the now-terminal receipt produces no further wake.
func TestAcceptance5ClaimReplayThroughWorker(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.authors = fakeAuthorResolver{byBot: map[string]companyBotInfo{
		"B0RILEY": {UserID: botRiley, AppID: "A0AAAAAA2"},
	}}

	// Seed a record already claimed by this result ts (claim persisted before
	// the crash): the exact claim-then-crash state.
	claimed := pendingRecord(delegationTS)
	claimed.Status = companyDelegationClaimed
	claimed.ResultTS = resultTS
	claimed.ResultClaimedAt = fixedNow
	claimed.Generation = 2
	name := writeHarnessRecord(t, h, claimed)
	h.openBarrier()

	// Admit the result event as a fresh receipt: routing has not yet frozen
	// targets (crash before ensureTargets committed).
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: resultTS}
	ev := botEvent("B0RILEY", botRiley, "A0AAAAAA2", resultTS, humanRootTS,
		"<@"+botOllie+"> done", resultMetadata(fixtureNonce, delegationTS))
	if w, handled := h.admitViaHandler(t, ev, 0); !handled || w.Result().StatusCode != http.StatusOK {
		t.Fatalf("admit result: handled=%v", handled)
	}
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 1 || !strings.Contains(calls[0].path, "/session/ollie-main/") {
		t.Fatalf("claim replay woke %d sessions, want exactly ollie-main: %+v", len(calls), calls)
	}
	if !strings.Contains(calls[0].body, "peer_result delivery") {
		t.Errorf("body missing peer_result kind: %q", calls[0].body)
	}
	// The claim replay re-claimed nothing: record unchanged.
	rec := readRecord(t, companyPeerEnv{delegationsDir: h.delegationsDir}, name)
	if rec.Generation != claimed.Generation || rec.ResultTS != resultTS || rec.Status != companyDelegationClaimed {
		t.Errorf("record mutated by replay: %+v, want gen %d result_claimed ts %s", rec, claimed.Generation, resultTS)
	}
	ptr := readPointer(t, h, "ollie-main")
	if ptr.Kind != wakeKindPeerResult || ptr.DelegationKey != name {
		t.Errorf("pointer = %+v, want peer_result key %s", ptr, name)
	}

	// Redrive the SAME result event: the receipt is now terminal, so no second
	// POST and the record is still unchanged.
	h.gw.deliverReceipt(origin)
	h.wait()
	if c := gc.sessionCalls(); len(c) != 1 {
		t.Errorf("second redrive produced %d POSTs, want still 1", len(c))
	}
	if again := readRecord(t, companyPeerEnv{delegationsDir: h.delegationsDir}, name); again.Generation != claimed.Generation {
		t.Errorf("second redrive re-claimed record (gen %d -> %d)", claimed.Generation, again.Generation)
	}
}
