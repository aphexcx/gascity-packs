package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// company_acks_test.go — Slack company-rooms Phase 3b visible-ack coverage: the
// AckState-cursor lifecycle (👀→✅ / 👀→⚠️+threaded reply / no_delivery
// removes-only), the error taxonomy (already_reacted success, too_many_* silent
// permanent degradation, ratelimited unchanged), the config gate (off = zero
// Slack calls), ack failure never blocking delivery, and terminal-ack
// sweep-healing.

type ackCall struct{ method, channel, ts, name string }

type ackSpy struct {
	mu      sync.Mutex
	calls   []ackCall
	replies []replyCall
	// outcome maps (method, name) -> ackOutcome; nil defaults to success.
	outcome func(method, name string) ackOutcome
	// replyFail, when true, makes reply report a failed post (the durable
	// "warned" cursor must not advance) — the reply is still recorded so tests
	// can assert the attempt count.
	replyFail bool
}

type replyCall struct{ channel, threadTS, text string }

func (s *ackSpy) react(method, channel, ts, name string) ackOutcome {
	s.mu.Lock()
	s.calls = append(s.calls, ackCall{method, channel, ts, name})
	f := s.outcome
	s.mu.Unlock()
	if f != nil {
		return f(method, name)
	}
	return ackSuccess
}

func (s *ackSpy) reply(channel, threadTS, text string) bool {
	s.mu.Lock()
	s.replies = append(s.replies, replyCall{channel, threadTS, text})
	ok := !s.replyFail
	s.mu.Unlock()
	return ok
}

func (s *ackSpy) snapshot() ([]ackCall, []replyCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ackCall(nil), s.calls...), append([]replyCall(nil), s.replies...)
}

func wireAckSpy(h *companyHarness, spy *ackSpy) {
	h.gw.visibleAcks = true
	h.gw.reactHook = spy.react
	h.gw.replyHook = spy.reply
}

func admitAckReceipt(t *testing.T, h *companyHarness, ts, threadTS, status, ackState string) *IngressReceipt {
	t.Helper()
	ev := slackMessageEvent{Type: "message", User: "Uhuman", Channel: testChannelID, TS: ts, ThreadTS: threadTS, Text: "hi"}
	raw, _ := json.Marshal(ev)
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts}
	r := &IngressReceipt{Origin: origin, Event: raw, Status: status, AckState: ackState}
	if created, _, err := h.receipts.Admit(r); err != nil || !created {
		t.Fatalf("admit ack receipt: created=%v err=%v", created, err)
	}
	return r
}

func ackHarness(t *testing.T) (*companyHarness, *fakeGC) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	setFixedClock(h)
	return h, gc
}

// ---- unit lifecycle over the hooks -----------------------------------------

func TestAckAdmissionEyes(t *testing.T) {
	h, _ := ackHarness(t)
	spy := &ackSpy{}
	wireAckSpy(h, spy)
	r := admitAckReceipt(t, h, "1700000000.000900", "", ingressStatusReceived, ackStateNone)

	h.gw.applyAdmissionAck(r)

	calls, _ := spy.snapshot()
	if len(calls) != 1 || calls[0] != (ackCall{"reactions.add", testChannelID, r.Origin.TS, ackEmojiEyes}) {
		t.Fatalf("admission calls = %+v, want one add eyes", calls)
	}
	if r.AckState != ackStateEyes {
		t.Errorf("AckState = %q, want eyes", r.AckState)
	}
	if got, _ := h.gw.store().Get(r.Origin); got.AckState != ackStateEyes {
		t.Errorf("persisted AckState = %q, want eyes", got.AckState)
	}
}

func TestAckAdmissionRatelimitedLeavesEmpty(t *testing.T) {
	h, _ := ackHarness(t)
	spy := &ackSpy{outcome: func(string, string) ackOutcome { return ackUnchanged }}
	wireAckSpy(h, spy)
	r := admitAckReceipt(t, h, "1700000000.000901", "", ingressStatusReceived, ackStateNone)

	h.gw.applyAdmissionAck(r)
	if r.AckState != ackStateNone {
		t.Errorf("AckState = %q, want empty (ratelimited retries next attempt)", r.AckState)
	}
}

func TestAckTerminalDeliveredToCheck(t *testing.T) {
	h, _ := ackHarness(t)
	spy := &ackSpy{}
	wireAckSpy(h, spy)
	r := admitAckReceipt(t, h, "1700000000.000902", "", ingressStatusDelivered, ackStateEyes)

	h.gw.applyTerminalAck(r)

	calls, _ := spy.snapshot()
	want := []ackCall{
		{"reactions.remove", testChannelID, r.Origin.TS, ackEmojiEyes},
		{"reactions.add", testChannelID, r.Origin.TS, ackEmojiCheck},
	}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("delivered ack calls = %+v, want %+v", calls, want)
	}
	if r.AckState != ackStateDone {
		t.Errorf("AckState = %q, want done", r.AckState)
	}
}

func TestAckTerminalFailedWarnPlusThreadedReply(t *testing.T) {
	h, _ := ackHarness(t)
	spy := &ackSpy{}
	wireAckSpy(h, spy)
	r := admitAckReceipt(t, h, "1700000000.000903", humanRootTS, ingressStatusFailed, ackStateEyes)

	h.gw.applyTerminalAck(r)

	calls, replies := spy.snapshot()
	want := []ackCall{
		{"reactions.remove", testChannelID, r.Origin.TS, ackEmojiEyes},
		{"reactions.add", testChannelID, r.Origin.TS, ackEmojiWarning},
	}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("failed ack calls = %+v, want %+v", calls, want)
	}
	wantReply := replyCall{testChannelID, humanRootTS, "delivery failed for receipt " + r.ID}
	if len(replies) != 1 || replies[0] != wantReply {
		t.Fatalf("failure replies = %+v, want %+v", replies, wantReply)
	}
	if r.AckState != ackStateDone {
		t.Errorf("AckState = %q, want done", r.AckState)
	}
}

func TestAckTerminalNoDeliveryRemovesOnly(t *testing.T) {
	h, _ := ackHarness(t)
	spy := &ackSpy{}
	wireAckSpy(h, spy)
	r := admitAckReceipt(t, h, "1700000000.000904", "", ingressStatusNoDelivery, ackStateEyes)

	h.gw.applyTerminalAck(r)

	calls, _ := spy.snapshot()
	if len(calls) != 1 || calls[0] != (ackCall{"reactions.remove", testChannelID, r.Origin.TS, ackEmojiEyes}) {
		t.Fatalf("no_delivery ack calls = %+v, want a single remove eyes", calls)
	}
	if r.AckState != ackStateDone {
		t.Errorf("AckState = %q, want done", r.AckState)
	}
}

func TestAckTerminalRatelimitLeavesEyesForSweep(t *testing.T) {
	h, _ := ackHarness(t)
	spy := &ackSpy{outcome: func(method, name string) ackOutcome {
		if method == "reactions.add" {
			return ackUnchanged // the check add is rate-limited
		}
		return ackSuccess
	}}
	wireAckSpy(h, spy)
	r := admitAckReceipt(t, h, "1700000000.000905", "", ingressStatusDelivered, ackStateEyes)

	h.gw.applyTerminalAck(r)
	if r.AckState != ackStateEyes {
		t.Errorf("AckState = %q, want eyes (rate-limited terminal ack heals on sweep)", r.AckState)
	}
}

func TestAckTooManyEmojiDegradesPermanently(t *testing.T) {
	h, _ := ackHarness(t)
	spy := &ackSpy{outcome: func(string, string) ackOutcome { return ackDegrade }}
	wireAckSpy(h, spy)
	r := admitAckReceipt(t, h, "1700000000.000906", "", ingressStatusReceived, ackStateNone)

	h.gw.applyAdmissionAck(r)
	if r.AckState != ackStateDegraded {
		t.Fatalf("AckState = %q, want degraded", r.AckState)
	}
	countAfterAdmission := len(spy.calls)

	// Terminal transition: a degraded receipt makes no further ack calls.
	r.Status = ingressStatusDelivered
	h.gw.applyTerminalAck(r)
	if len(spy.calls) != countAfterAdmission {
		t.Errorf("degraded receipt made further ack calls: %+v", spy.calls)
	}
	if r.AckState != ackStateDegraded {
		t.Errorf("AckState = %q, want degraded (permanent)", r.AckState)
	}
}

// ---- slackReact taxonomy (over a fake Slack endpoint) ----------------------

func TestSlackReactTaxonomy(t *testing.T) {
	var respBody string
	var closeConn bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if closeConn {
			hj, _ := w.(http.Hijacker)
			c, _, _ := hj.Hijack()
			_ = c.Close()
			return
		}
		_, _ = w.Write([]byte(respBody))
	}))
	defer srv.Close()
	prev := slackAPIBase
	slackAPIBase = srv.URL
	defer func() { slackAPIBase = prev }()

	cases := []struct {
		name string
		body string
		conn bool
		want ackOutcome
	}{
		{"ok", `{"ok":true}`, false, ackSuccess},
		{"already_reacted", `{"ok":false,"error":"already_reacted"}`, false, ackSuccess},
		{"no_reaction", `{"ok":false,"error":"no_reaction"}`, false, ackSuccess},
		{"too_many_emoji", `{"ok":false,"error":"too_many_emoji"}`, false, ackDegrade},
		{"too_many_reactions", `{"ok":false,"error":"too_many_reactions"}`, false, ackDegrade},
		{"ratelimited", `{"ok":false,"error":"ratelimited"}`, false, ackUnchanged},
		{"other_error", `{"ok":false,"error":"message_not_found"}`, false, ackDegrade},
		{"network_error", ``, true, ackUnchanged},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			respBody = c.body
			closeConn = c.conn
			got := slackReact(http.DefaultClient, "xoxb-test", "reactions.add", testChannelID, "1700000000.1", ackEmojiEyes)
			if got != c.want {
				t.Errorf("slackReact(%s) = %d, want %d", c.name, got, c.want)
			}
		})
	}
}

// ---- integration: gate + delivery independence ------------------------------

func deliverAckHumanReceipt(t *testing.T, h *companyHarness, ts string) ReceiptOrigin {
	t.Helper()
	ev := slackMessageEvent{Type: "message", User: "Uhuman", Channel: testChannelID, TS: ts, Text: "hello team"}
	raw, _ := json.Marshal(ev)
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts}
	if created, _, err := h.receipts.Admit(&IngressReceipt{Origin: origin, Event: raw, Status: ingressStatusReceived}); err != nil || !created {
		t.Fatalf("admit human: created=%v err=%v", created, err)
	}
	return origin
}

func TestAckGateOffZeroCalls(t *testing.T) {
	h, gc := ackHarness(t)
	spy := &ackSpy{}
	// Gate OFF (default): wire the hooks but leave visibleAcks false.
	h.gw.reactHook = spy.react
	h.gw.replyHook = spy.reply
	origin := deliverAckHumanReceipt(t, h, "1700000000.000910")

	h.gw.deliverReceipt(origin)
	h.wait()

	if n := len(gc.sessionCalls()); n != 1 {
		t.Fatalf("delivery = %d, want 1 (delivery must run with acks off)", n)
	}
	if calls, replies := spy.snapshot(); len(calls) != 0 || len(replies) != 0 {
		t.Errorf("acks off but Slack ack traffic occurred: calls=%+v replies=%+v", calls, replies)
	}
	if r, _ := h.gw.store().Get(origin); r.AckState != ackStateNone {
		t.Errorf("AckState = %q, want empty with acks off", r.AckState)
	}
}

func TestAckLifecycleDuringDelivery(t *testing.T) {
	h, _ := ackHarness(t)
	spy := &ackSpy{}
	wireAckSpy(h, spy)
	origin := deliverAckHumanReceipt(t, h, "1700000000.000911")

	h.gw.deliverReceipt(origin)
	h.wait()

	calls, _ := spy.snapshot()
	want := []ackCall{
		{"reactions.add", testChannelID, origin.TS, ackEmojiEyes},
		{"reactions.remove", testChannelID, origin.TS, ackEmojiEyes},
		{"reactions.add", testChannelID, origin.TS, ackEmojiCheck},
	}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("lifecycle calls = %+v, want %+v", calls, want)
	}
	if r, _ := h.gw.store().Get(origin); r.AckState != ackStateDone {
		t.Errorf("AckState = %q, want done", r.AckState)
	}
}

func TestAckFailureNeverBlocksDelivery(t *testing.T) {
	h, gc := ackHarness(t)
	// Every ack call reports a transient/unknown outcome; delivery must be
	// unaffected.
	spy := &ackSpy{outcome: func(string, string) ackOutcome { return ackUnchanged }}
	wireAckSpy(h, spy)
	origin := deliverAckHumanReceipt(t, h, "1700000000.000912")

	h.gw.deliverReceipt(origin)
	h.wait()

	if n := len(gc.sessionCalls()); n != 1 {
		t.Errorf("delivery = %d, want 1 (ack failure must not block delivery)", n)
	}
	if r, _ := h.gw.store().Get(origin); r.Status != ingressStatusDelivered {
		t.Errorf("status = %v, want delivered", r.Status)
	}
}

func TestAckTerminalFailedRatelimitPostsReplyOnce(t *testing.T) {
	h, _ := ackHarness(t)
	// The ⚠️ reaction stays rate-limited; the reply post and the 👀 remove
	// succeed. This is the spec's "expected case" for a terminal failed ack.
	ratelimited := true
	spy := &ackSpy{outcome: func(method, name string) ackOutcome {
		if method == "reactions.add" && name == ackEmojiWarning && ratelimited {
			return ackUnchanged
		}
		return ackSuccess
	}}
	wireAckSpy(h, spy)
	r := admitAckReceipt(t, h, "1700000000.000914", humanRootTS, ingressStatusFailed, ackStateEyes)

	// Three sweep heals while the ⚠️ add is rate-limited: the reply is posted
	// exactly once (first heal), then the durable "warned" cursor keeps every
	// subsequent heal to the reaction only — never a second thread reply.
	for i := 0; i < 3; i++ {
		h.gw.sweepOnce()
		h.wait()
	}
	if _, replies := spy.snapshot(); len(replies) != 1 {
		t.Fatalf("failure replies after 3 rate-limited sweeps = %d, want exactly 1", len(replies))
	}
	if got, _ := h.gw.store().Get(r.Origin); got.AckState != ackStateWarned {
		t.Fatalf("AckState = %q, want warned (reply posted, reaction still pending)", got.AckState)
	}

	// The ⚠️ add finally lands: the cursor advances to done and STILL no second
	// reply is posted.
	ratelimited = false
	h.gw.sweepOnce()
	h.wait()
	if _, replies := spy.snapshot(); len(replies) != 1 {
		t.Fatalf("failure replies after the reaction landed = %d, want exactly 1", len(replies))
	}
	if got, _ := h.gw.store().Get(r.Origin); got.AckState != ackStateDone {
		t.Fatalf("AckState = %q, want done", got.AckState)
	}
}

func TestAckTerminalFailedReplyPostFailureLeavesEyes(t *testing.T) {
	h, _ := ackHarness(t)
	spy := &ackSpy{replyFail: true}
	wireAckSpy(h, spy)
	r := admitAckReceipt(t, h, "1700000000.000915", humanRootTS, ingressStatusFailed, ackStateEyes)

	// The reply POST fails: the cursor stays on "eyes" (never "warned") and the
	// ⚠️ reaction is NOT applied — a receipt with no confirmed reply never
	// advances, so the crash/failure window can only ever re-attempt the reply,
	// never strand a warning with no reply.
	h.gw.applyTerminalAck(r)
	calls, replies := spy.snapshot()
	if len(replies) != 1 {
		t.Fatalf("reply attempts = %d, want 1", len(replies))
	}
	for _, c := range calls {
		if c.method == "reactions.add" {
			t.Fatalf("⚠️ reaction applied before a confirmed reply: %+v", calls)
		}
	}
	if r.AckState != ackStateEyes {
		t.Fatalf("AckState = %q, want eyes (reply unconfirmed)", r.AckState)
	}

	// Once the reply confirms, the next heal posts it and advances warned→done.
	spy.mu.Lock()
	spy.replyFail = false
	spy.mu.Unlock()
	h.gw.applyTerminalAck(r)
	if r.AckState != ackStateDone {
		t.Fatalf("AckState = %q, want done after reply confirmed", r.AckState)
	}
	if _, replies := spy.snapshot(); len(replies) != 2 {
		t.Fatalf("reply attempts = %d, want 2 (one failed, one confirmed)", len(replies))
	}
}

func TestSweepHealsStrandedEyes(t *testing.T) {
	h, _ := ackHarness(t)
	spy := &ackSpy{}
	wireAckSpy(h, spy)
	// A terminal (delivered) receipt whose terminal ack never landed: stranded
	// on AckState=="eyes" within retention.
	r := admitAckReceipt(t, h, "1700000000.000913", "", ingressStatusDelivered, ackStateEyes)

	h.gw.sweepOnce()
	h.wait()

	calls, _ := spy.snapshot()
	want := []ackCall{
		{"reactions.remove", testChannelID, r.Origin.TS, ackEmojiEyes},
		{"reactions.add", testChannelID, r.Origin.TS, ackEmojiCheck},
	}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("sweep-heal calls = %+v, want %+v", calls, want)
	}
	if got, _ := h.gw.store().Get(r.Origin); got.AckState != ackStateDone {
		t.Errorf("healed AckState = %q, want done", got.AckState)
	}
}
