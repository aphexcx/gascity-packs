package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// company_mpim_test.go — Slack company-rooms Phase 4b (group-DM / mpim) coverage,
// mapping to the spec's 11-item test plan: mention-only wake, multi-target route
// with one unhomed → failed_dm_unbound + redrive, the mpim_no_mention /
// mpim_bot_author terminals, multi-app admission dedup + switchboard absorption,
// the mpim pointer namespace + per-target owner_app_id, the membership probe
// (not_in_channel fails; network error proceeds), the allowlist deny +
// provenance downgrade, replay/spoof (redelivery, cross-app 401, hydration token
// fallback, ack degradation counters), and the LIVE-captured wire fixture.

const testMpimChannel = "G0GROUPDM01"

type mpimProbeSpy struct {
	mu       sync.Mutex
	tokens   []string
	channels []string
	result   mpimProbeOutcome
}

func (s *mpimProbeSpy) probeTokens() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.tokens...)
}

// setupMpim reuses the DM harness (ollie + riley registered agent apps, an
// ollie→ollie DM binding with riley intentionally unbound, ollie's owner token
// present) and adds a membership-probe spy that defaults to "member" so tests
// never touch the real conversations.info endpoint.
func setupMpim(t *testing.T) (*companyHarness, *fakeGC, *dmSpy, *mpimProbeSpy) {
	h, gc, spy := setupDM(t)
	ps := &mpimProbeSpy{result: mpimProbeMember}
	h.gw.mpimMemberProbe = func(token, channel string) mpimProbeOutcome {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		ps.tokens = append(ps.tokens, token)
		ps.channels = append(ps.channels, channel)
		return ps.result
	}
	return h, gc, spy, ps
}

func mpimMessage(user, ts, text string) slackMessageEvent {
	return slackMessageEvent{User: user, Channel: testMpimChannel, TS: ts, Text: text, ChannelType: "mpim"}
}

func admitMpimViaHandler(t *testing.T, h *companyHarness, ev slackMessageEvent, apiAppID string, retryNum int) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	ev.Type = "message"
	ev.ChannelType = "mpim"
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal mpim event: %v", err)
	}
	env := slackEventEnvelope{Type: "event_callback", TeamID: testTeamID, APIAppID: apiAppID, EventID: "Ev-" + apiAppID + "-" + ev.TS, Event: raw}
	req := httptest.NewRequest(http.MethodPost, "/slack/events", nil)
	if retryNum > 0 {
		req.Header.Set("X-Slack-Retry-Num", strconv.Itoa(retryNum))
		req.Header.Set("X-Slack-Retry-Reason", "http_timeout")
	}
	w := httptest.NewRecorder()
	handled := h.gw.tryHandleEvent(w, req, env, h.gw.agentAppsSnapshot())
	return w, handled
}

func mpimOrigin(ts string) ReceiptOrigin {
	return ReceiptOrigin{TeamID: testTeamID, ChannelID: testMpimChannel, TS: ts}
}

func getMpimReceipt(t *testing.T, h *companyHarness, ts string) *IngressReceipt {
	t.Helper()
	r, err := h.gw.store().Get(mpimOrigin(ts))
	if err != nil {
		t.Fatalf("get mpim receipt: %v", err)
	}
	return r
}

func readMpimPointer(t *testing.T, h *companyHarness, session string) companyCurrentTurn {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(h.turnsDir, "mpim", session+".json"))
	if err != nil {
		t.Fatalf("read mpim pointer %s: %v", session, err)
	}
	var p companyCurrentTurn
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("decode mpim pointer: %v", err)
	}
	return p
}

// ---- item 1: mention-only wakes exactly the mentioned homed agent ----------

func TestMpimMentionOnlyWakesMentioned(t *testing.T) {
	h, gc, spy, ps := setupMpim(t)
	ts := "1700000000.001000"
	// olivia (== riley here) is a member of the group but UNMENTIONED; only
	// ollie is mentioned, so only ollie wakes.
	ev := mpimMessage("U0HUMAN01", ts, "hey <@U0AAAAAA1> check the deploy")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("mpim not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r == nil || r.Kind != receiptKindMpim || r.OwnerAppID != ollieAppID {
		t.Fatalf("receipt = %+v, want mpim/owner=ollie", r)
	}
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered", r.Status)
	}
	calls := gc.sessionCalls()
	if len(calls) != 1 || !strings.Contains(calls[0].path, "/session/ollie/messages") {
		t.Fatalf("session calls = %+v, want exactly one to ollie", calls)
	}
	if !strings.Contains(calls[0].body, "group direct message to agent \"ollie\"") {
		t.Errorf("reminder missing mpim framing: %q", calls[0].body)
	}
	// No riley target at all (unmentioned, not merely failed).
	for k, td := range r.Targets {
		if td.Agent == "riley" {
			t.Errorf("unmentioned riley got a target %q=%+v", k, td)
		}
	}
	// Pointer lands in the mpim/ subdir with kind mpim + owner_app_id = the WOKEN
	// agent's own app id (ollie), NOT the receipt-level admission owner as a raw
	// field would give (here they coincide; the multi-target test separates them).
	p := readMpimPointer(t, h, "ollie")
	if p.Kind != "mpim" || p.OwnerAppID != ollieAppID || p.Room != "" {
		t.Errorf("mpim pointer = %+v, want kind mpim / owner=ollie / empty room", p)
	}
	// The membership probe ran with ollie's OWN token.
	if toks := ps.probeTokens(); len(toks) != 1 || toks[0] != "xoxb-ollie" {
		t.Errorf("probe tokens = %v, want [xoxb-ollie]", toks)
	}
	// Hydration used a DM-family token, never the switchboard sentinel.
	hydrate, _, _ := spy.tokens()
	for _, tok := range hydrate {
		if tok == "xoxb-SWITCHBOARD" {
			t.Fatal("switchboard token used on an mpim channel")
		}
	}
}

// ---- item 2: two targets on one receipt; one unhomed → redrive delivers -----

func TestMpimMentionBothOneUnhomedThenRedrive(t *testing.T) {
	h, gc, _, ps := setupMpim(t)
	ts := "1700000000.001010"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> and <@U0AAAAAA2> please sync")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("mpim not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusFailed {
		t.Fatalf("status = %q, want failed (ollie delivered, riley unbound)", r.Status)
	}
	var ollieDelivered, rileyUnbound bool
	for _, td := range r.Targets {
		if td.Agent == "ollie" && td.Status == companyTargetDelivered {
			ollieDelivered = true
		}
		if td.Agent == "riley" && td.Status == companyTargetFailed && td.Detail == companyReasonFailedDMUnbound {
			rileyUnbound = true
		}
	}
	if !ollieDelivered || !rileyUnbound {
		t.Fatalf("targets = %+v, want ollie delivered + riley failed_dm_unbound", r.Targets)
	}
	if got := len(gc.sessionCalls()); got != 1 {
		t.Fatalf("session calls = %d, want 1 (only ollie)", got)
	}

	// Import a riley dm binding + token, then company-redrive re-resolves the
	// unbound target via dm_bindings (the dm-family redrive gate) and delivers.
	h.gw.dmBindStore = loadDMBindingsStore(t, h, dmBindingsFile{
		SchemaVersion: 1,
		DMBindings:    []DMBinding{{Agent: "ollie", Session: "ollie"}, {Agent: "riley", Session: "riley-dm"}},
	})
	if err := os.WriteFile(filepath.Join(h.gw.secretsDir, "bot-token-riley.txt"), []byte("xoxb-riley"), 0o600); err != nil {
		t.Fatalf("write riley token: %v", err)
	}
	ps.result = mpimProbeMember
	origin := mpimOrigin(ts)
	if _, cerr := h.gw.applyRedrive(r, companyRedriveRequest{Origin: &origin}); cerr != nil {
		t.Fatalf("applyRedrive: %v", cerr)
	}
	h.gw.triggerDelivery(origin)
	h.wait()

	r = getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("post-redrive status = %q, want delivered", r.Status)
	}
	found := false
	for _, c := range gc.sessionCalls() {
		if strings.Contains(c.path, "/session/riley-dm/messages") {
			found = true
		}
	}
	if !found {
		t.Errorf("no delivery to riley-dm after redrive: %+v", gc.sessionCalls())
	}
	// riley's pointer carries riley's OWN app id, distinct from the receipt-level
	// admission owner (ollie's app) — the per-target owner_app_id contract.
	p := readMpimPointer(t, h, "riley-dm")
	if p.OwnerAppID != rileyAppID {
		t.Errorf("riley mpim pointer owner_app_id = %q, want %q", p.OwnerAppID, rileyAppID)
	}
}

// ---- item 3: unmentioned + bot-authored terminals --------------------------

func TestMpimUnmentionedTerminalNoMention(t *testing.T) {
	h, gc, _, _ := setupMpim(t)
	ts := "1700000000.001020"
	// A human message mentioning no homed agent (a plain @-less line).
	ev := mpimMessage("U0HUMAN01", ts, "morning everyone")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusNoDelivery || r.Reason != wakeReasonMpimNoMention {
		t.Fatalf("status/reason = %q/%q, want no_delivery/mpim_no_mention", r.Status, r.Reason)
	}
	if len(r.Targets) != 0 || len(gc.sessionCalls()) != 0 {
		t.Errorf("no-mention woke someone: targets=%+v calls=%d", r.Targets, len(gc.sessionCalls()))
	}
}

func TestMpimBotAuthoredTerminal(t *testing.T) {
	h, gc, _, _ := setupMpim(t)
	ts := "1700000000.001030"
	// A bot-authored mpim (a member app's own echo) that even mentions an agent:
	// bot authorship is terminal before the mention set is consulted.
	ev := mpimMessage("U0AAAAAA1", ts, "<@U0AAAAAA2> done")
	ev.BotID = "B0AAAAAA1"
	ev.AppID = ollieAppID
	ev.Metadata = json.RawMessage(`{"event_type":"gc_dm_reply","event_payload":{"v":1,"nonce":"gcs-mpim1"}}`)

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r == nil {
		t.Fatal("bot-authored mpim produced no receipt (needed as reconciliation memory)")
	}
	if r.Status != ingressStatusNoDelivery || r.Reason != wakeReasonMpimBotAuthor {
		t.Fatalf("status/reason = %q/%q, want no_delivery/mpim_bot_author", r.Status, r.Reason)
	}
	if len(gc.sessionCalls()) != 0 {
		t.Errorf("bot-authored mpim woke %d sessions, want 0", len(gc.sessionCalls()))
	}
	// The stored event keeps its metadata so the Python reconciler can match the
	// op=dm intent's nonce against this mpim self-echo.
	if !strings.Contains(string(h.gw.store().receiptBody(r)), "gc_dm_reply") {
		t.Error("mpim self-echo dropped its metadata")
	}
}

// ---- item 4: multi-app dedup + switchboard absorption ----------------------

func TestMpimMultiAppDedupAndSwitchboardAbsorbed(t *testing.T) {
	h, gc, _, _ := setupMpim(t)
	ts := "1700000000.001040"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> ping")

	// riley's app observes the same event first and admits it: the receipt's
	// owner (ack actor) is the winner, ollie's later copy of the SAME origin is
	// absorbed as a replay (no second receipt, no second delivery).
	if _, handled := admitMpimViaHandler(t, h, ev, rileyAppID, 0); !handled {
		t.Fatal("first (riley) copy not handled")
	}
	h.wait()
	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("second (ollie) copy not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.OwnerAppID != rileyAppID {
		t.Errorf("ack actor = %q, want the admission winner riley (%q)", r.OwnerAppID, rileyAppID)
	}
	// Exactly one delivery to ollie despite two admitting apps.
	if got := len(gc.sessionCalls()); got != 1 {
		t.Fatalf("session calls = %d, want 1 (deduped)", got)
	}

	// A switchboard-signed mpim (unregistered app id): 200, NO receipt, and the
	// gateway OWNS it (handled=true) so it never falls through to legacy dispatch.
	sbTS := "1700000000.001041"
	sbEv := mpimMessage("U0HUMAN01", sbTS, "<@U0AAAAAA1> from the switchboard")
	w, handled := admitMpimViaHandler(t, h, sbEv, switchAppID, 0)
	if !handled || w.Code != http.StatusOK {
		t.Fatalf("switchboard mpim: handled=%v code=%d, want handled/200", handled, w.Code)
	}
	if r := getMpimReceipt(t, h, sbTS); r != nil {
		t.Errorf("switchboard mpim created a receipt %+v, want none", r)
	}
}

// ---- item 7: membership probe ----------------------------------------------

func TestMpimProbeNotMemberFailsNoWake(t *testing.T) {
	h, gc, _, ps := setupMpim(t)
	ps.result = mpimProbeNotMember // forged event for a group ollie is not in
	ts := "1700000000.001050"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> are you here?")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusFailed {
		t.Fatalf("status = %q, want failed", r.Status)
	}
	var probed bool
	for _, td := range r.Targets {
		if td.Agent == "ollie" && td.Status == companyTargetFailed && td.Detail == companyReasonFailedMpimNotMember {
			probed = true
		}
	}
	if !probed {
		t.Fatalf("targets = %+v, want ollie failed_mpim_not_member", r.Targets)
	}
	if got := len(gc.sessionCalls()); got != 0 {
		t.Errorf("probe-failed target still woke %d sessions, want 0", got)
	}
}

func TestMpimProbeNetworkErrorProceeds(t *testing.T) {
	h, gc, _, ps := setupMpim(t)
	ps.result = mpimProbeError // advisory: proceed as if unchecked
	ts := "1700000000.001060"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> deploy status?")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered (probe error proceeds)", r.Status)
	}
	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("session calls = %d, want 1", got)
	}
}

// ---- item 8: allowlist deny + provenance downgrade -------------------------

func mpimAllowlistDir(allowed ...string) companyDirectoryFile {
	df := baseDirectoryFile()
	a := append([]string(nil), allowed...)
	df.DMAllowedHumans = &a
	return df
}

// setupMpimDir rebuilds the mpim harness over a custom directory file (for the
// allowlist tests), keeping the same registered apps, ollie binding, ollie token,
// and probe/hydration/ack spies as setupMpim.
func setupMpimDir(t *testing.T, df companyDirectoryFile) (*companyHarness, *fakeGC, *dmSpy, *mpimProbeSpy) {
	gc := newFakeGC(t)
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 8)
	setFixedClock(h)
	h.gw.slackToken = "xoxb-SWITCHBOARD"
	h.gw.agentApps = loadAgentAppsStore(t, h, baseAgentAppsFile())
	h.gw.dmBindStore = loadDMBindingsStore(t, h, dmBindingsFile{
		SchemaVersion: 1,
		DMBindings:    []DMBinding{{Agent: "ollie", Session: "ollie"}},
	})
	h.gw.secretsDir = writeTokenFile(t, "ollie", "xoxb-ollie", 0o700, 0o600)
	spy := &dmSpy{}
	h.gw.hydrateDM = func(token string, msg CompanyMessage) companyHydration {
		spy.mu.Lock()
		spy.hydrateTokens = append(spy.hydrateTokens, token)
		spy.mu.Unlock()
		return companyHydration{RootProvenance: companyRootProvenanceVerified, ContextStatus: companyContextAvailable}
	}
	ps := &mpimProbeSpy{result: mpimProbeMember}
	h.gw.mpimMemberProbe = func(token, channel string) mpimProbeOutcome {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		ps.tokens = append(ps.tokens, token)
		return ps.result
	}
	h.openBarrier()
	return h, gc, spy, ps
}

func TestMpimAllowlistDenyRootAuthor(t *testing.T) {
	// Allowlist present but names a DIFFERENT user: the root author is denied.
	h, gc, _, _ := setupMpimDir(t, mpimAllowlistDir("U0SOMEONEELSE"))
	ts := "1700000000.001070"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> hi")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusNoDelivery || r.Reason != wakeReasonDMAuthorNotAllowed {
		t.Fatalf("status/reason = %q/%q, want no_delivery/dm_author_not_allowed", r.Status, r.Reason)
	}
	if len(gc.sessionCalls()) != 0 {
		t.Errorf("denied author still woke %d sessions", len(gc.sessionCalls()))
	}
}

func TestMpimProvenanceDowngradeUnlistedExcerpt(t *testing.T) {
	// Root author allowed, but an excerpted group member is NOT on the allowlist:
	// the reminder's provenance line downgrades to human_root_unlisted.
	h, gc, _, _ := setupMpimDir(t, mpimAllowlistDir("U0HUMAN01"))
	// Override hydration to carry an excerpt authored by an unlisted member.
	h.gw.hydrateDM = func(token string, msg CompanyMessage) companyHydration {
		return companyHydration{
			RootProvenance: companyRootProvenanceVerified,
			ContextStatus:  companyContextAvailable,
			Excerpt:        []companyExcerptLine{{TS: "1700000000.000500", User: "U0OUTSIDER", Text: "hello"}},
		}
	}
	ts := "1700000000.001080"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> ship it")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered", r.Status)
	}
	calls := gc.sessionCalls()
	if len(calls) != 1 {
		t.Fatalf("session calls = %d, want 1", len(calls))
	}
	if !strings.Contains(calls[0].body, "root_provenance: "+companyRootProvenanceUnlisted) {
		t.Errorf("reminder provenance not downgraded: %q", calls[0].body)
	}
}

// ---- item 9: replay / spoof / fallback / ack degradation -------------------

func TestMpimRedeliveryAbsorbed(t *testing.T) {
	h, gc, _, _ := setupMpim(t)
	ts := "1700000000.001090"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> hey")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("first admit not handled")
	}
	h.wait()
	if got := len(gc.sessionCalls()); got != 1 {
		t.Fatalf("first delivery calls = %d, want 1", got)
	}
	// x-slack-retry redelivery of the same origin: 200, no second receipt/delivery.
	w, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 2)
	if !handled || w.Code != http.StatusOK {
		t.Fatalf("redelivery: handled=%v code=%d", handled, w.Code)
	}
	h.wait()
	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("session calls after redelivery = %d, want still 1", got)
	}
}

func TestMpimCrossAppSignatureRejected(t *testing.T) {
	// rule 12 spoof: an mpim event claiming ollie's app but signed with riley's
	// secret is a strict-bind reject (401), counted on company_dm_sig_reject.
	cfg := dmVerifyCfg(t)
	before := cfg.companyGateway.dmSigRejects.Load()
	env := slackEventEnvelope{
		Type:     "event_callback",
		TeamID:   testTeamID,
		APIAppID: ollieAppID,
		Event:    json.RawMessage(`{"type":"message","channel_type":"mpim","channel":"G0X","ts":"1.0","user":"U0HUMAN01","text":"<@U0AAAAAA1> hi"}`),
	}
	body, ts, sig := signedBody(t, "riley-secret", env)
	if verifyInboundEvent(cfg, cfg.companyGateway.agentAppsSnapshot(), parseEventHead(body), body, ts, sig) {
		t.Fatal("cross-app-signed mpim verified, want reject")
	}
	if got := cfg.companyGateway.dmSigRejects.Load() - before; got != 1 {
		t.Errorf("dmSigRejects delta = %d, want 1", got)
	}
}

func TestMpimHydrationTokenFallbackOrder(t *testing.T) {
	// Admission owner is riley (the ack actor), whose token is ABSENT. Hydration
	// falls back over the woken agents' tokens sorted by name — here ollie, whose
	// token is present — so the frozen blob is fetched with ollie's token.
	h, _, spy, _ := setupMpim(t)
	// riley token is absent in setupMpim's secrets dir (only ollie's is written).
	ts := "1700000000.001100"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> please look")

	if _, handled := admitMpimViaHandler(t, h, ev, rileyAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.OwnerAppID != rileyAppID {
		t.Fatalf("owner = %q, want riley", r.OwnerAppID)
	}
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered", r.Status)
	}
	hydrate, _, _ := spy.tokens()
	if len(hydrate) != 1 || hydrate[0] != "xoxb-ollie" {
		t.Fatalf("hydration tokens = %v, want [xoxb-ollie] (owner riley token absent → fallback)", hydrate)
	}
}

func TestMpimMissingOwnerTokenAckDegradesCounted(t *testing.T) {
	// A bot-authored mpim (no pending target) whose ack-actor owner token is
	// missing degrades its visible ack — counted once on company_dm_token_missing
	// via the shared dm-family degradation counter.
	h, _, spy, _ := setupMpim(t)
	h.gw.visibleAcks = true
	if err := os.Remove(filepath.Join(h.gw.secretsDir, "bot-token-ollie.txt")); err != nil {
		t.Fatalf("remove token: %v", err)
	}
	before := h.gw.dmTokenMissing.Load()

	ts := "1700000000.001110"
	ev := mpimMessage("U0AAAAAA1", ts, "<@U0AAAAAA2> done")
	ev.BotID = "B0AAAAAA1"
	ev.AppID = ollieAppID
	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusNoDelivery || r.Reason != wakeReasonMpimBotAuthor {
		t.Fatalf("status/reason = %q/%q, want no_delivery/mpim_bot_author", r.Status, r.Reason)
	}
	if got := h.gw.dmTokenMissing.Load() - before; got != 1 {
		t.Fatalf("dmTokenMissing delta = %d, want exactly 1", got)
	}
	if r.AckState != ackStateDegraded {
		t.Errorf("AckState = %q, want degraded", r.AckState)
	}
	_, react, reply := spy.tokens()
	if len(react) != 0 || len(reply) != 0 {
		t.Errorf("degraded ack still called react/reply: react=%v reply=%v", react, reply)
	}
}

// ---- item 10: LIVE wire fixture (captured 2026-07-19, workspace T0ARJCFV8QL) --

// TestMpimWireFixtureParse pins the shape of the LIVE message.mpim capture
// (workspace T0ARJCFV8QL, 2026-07-19, spec test-plan item 10): it proves Slack
// synthesizes rich_text mention elements in mpim — the assumption the mention
// extractor rests on — using the real event's app and user ids.
func TestMpimWireFixtureParse(t *testing.T) {
	f := baseDirectoryFile()
	f.Agents = []CompanyAgent{
		{Name: "ollie", AppID: "A0BHQ812PL7", BotUserID: "U0BJ2AA5GMT"},
		{Name: "olivia", AppID: "A0BJ5N5FNBU", BotUserID: "U0BHZEU1VQT"},
	}
	f.Rooms[0].Members = []string{"ollie", "olivia"}
	f.Rooms[0].AmbientWake = []string{"ollie"}
	f.Rooms[0].MentionWake = []string{"ollie", "olivia"}
	dir, err := ParseCompanyDirectory(marshalDirectory(t, f))
	if err != nil {
		t.Fatalf("build live-id directory: %v", err)
	}
	env := readEnvFixture(t, "message_mpim_human.json")
	if env.APIAppID != "A0BHQ812PL7" {
		t.Errorf("fixture api_app_id = %q, want live ollie app", env.APIAppID)
	}
	var ev slackMessageEvent
	if err := json.Unmarshal(env.Event, &ev); err != nil {
		t.Fatalf("decode inner: %v", err)
	}
	if ev.ChannelType != "mpim" || ev.BotID != "" || ev.User == "" {
		t.Errorf("inner shape = %+v, want human mpim", ev)
	}
	msg := decodeCompanyMessage(ReceiptOrigin{TeamID: env.TeamID, ChannelID: ev.Channel, TS: ev.TS}, env.Event)
	woken := mpimWokenAgents(dir, msg)
	names := make([]string, 0, len(woken))
	for _, a := range woken {
		names = append(names, a.Name)
	}
	if len(names) != 2 || names[0] != "ollie" || names[1] != "olivia" {
		t.Errorf("woken agents = %v, want [ollie olivia] (mention order)", names)
	}
}

// ---- C1/C7: unlisted verified-root author downgrades provenance ------------

func TestMpimProvenanceDowngradeUnlistedRootAuthor(t *testing.T) {
	// Threaded mpim: the mentioning message author is allowed, but the hydrated
	// thread-root author is UNLISTED and its text is rendered as the "verified
	// human root". The root leg must downgrade to human_root_unlisted even with an
	// empty/all-listed excerpt — the previous code scanned only the excerpt and
	// labeled unlisted-author root text as verified.
	h, gc, _, _ := setupMpimDir(t, mpimAllowlistDir("U0HUMAN01"))
	h.gw.hydrateDM = func(token string, msg CompanyMessage) companyHydration {
		return companyHydration{
			RootProvenance: companyRootProvenanceVerified,
			ContextStatus:  companyContextAvailable,
			Root:           &companyHydrationRoot{TS: "1700000000.000400", User: "U0UNLISTED", Text: "root text"},
		}
	}
	ts := "1700000000.001220"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> see above")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered", r.Status)
	}
	calls := gc.sessionCalls()
	if len(calls) != 1 {
		t.Fatalf("session calls = %d, want 1", len(calls))
	}
	if !strings.Contains(calls[0].body, "root_provenance: "+companyRootProvenanceUnlisted) {
		t.Errorf("unlisted root author not downgraded: %q", calls[0].body)
	}
}

// ---- C5/m5: bot/agent-authored excerpt lines never downgrade ---------------

func TestMpimProvenanceIgnoresBotExcerptAuthors(t *testing.T) {
	// Allowlist mode; the recent excerpt MIXES an agent's own reply (carrying its
	// bot_user_id, never in dm_allowed_humans) with a listed human. Only human
	// authors participate in the unlisted check, so neither the agent line nor an
	// empty-user classic bot line may downgrade a genuinely verified human root.
	h, gc, _, _ := setupMpimDir(t, mpimAllowlistDir("U0HUMAN01"))
	h.gw.hydrateDM = func(token string, msg CompanyMessage) companyHydration {
		return companyHydration{
			RootProvenance: companyRootProvenanceVerified,
			ContextStatus:  companyContextAvailable,
			Excerpt: []companyExcerptLine{
				{TS: "1700000000.000400", User: "U0AAAAAA1", Text: "on it"},   // ollie's own reply
				{TS: "1700000000.000450", User: "", Text: "classic bot post"}, // empty-user bot line
				{TS: "1700000000.000500", User: "U0HUMAN01", Text: "thanks"},  // listed human
			},
		}
	}
	ts := "1700000000.001210"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> status?")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered", r.Status)
	}
	calls := gc.sessionCalls()
	if len(calls) != 1 {
		t.Fatalf("session calls = %d, want 1", len(calls))
	}
	if !strings.Contains(calls[0].body, "root_provenance: "+companyRootProvenanceVerified) {
		t.Errorf("agent/bot excerpt lines wrongly downgraded provenance: %q", calls[0].body)
	}
}

// ---- C4/m2: provenance frozen — retries render byte-identical bodies --------

func TestMpimProvenanceFrozenAcrossAllowlistChange(t *testing.T) {
	// The first delivery POST is retryable (gc 500) so the target stays pending;
	// the DM allowlist is then WIDENED to include the previously-unlisted excerpt
	// author before the retry. A per-attempt recomputation from the live directory
	// would flip the retry's provenance line to verified — divergent bytes under
	// the same Idempotency-Key. The frozen verdict must keep both bodies identical.
	h, gc, _, _ := setupMpimDir(t, mpimAllowlistDir("U0HUMAN01"))
	h.gw.hydrateDM = func(token string, msg CompanyMessage) companyHydration {
		return companyHydration{
			RootProvenance: companyRootProvenanceVerified,
			ContextStatus:  companyContextAvailable,
			Excerpt:        []companyExcerptLine{{TS: "1700000000.000500", User: "U0OUTSIDER", Text: "hello"}},
		}
	}
	gc.mu.Lock()
	gc.respStatus = func(reqNum int) int {
		if reqNum == 0 {
			return http.StatusInternalServerError // retryable → target stays pending
		}
		return http.StatusOK
	}
	gc.mu.Unlock()

	ts := "1700000000.001200"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> ship it")
	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()
	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusRouting {
		t.Fatalf("status = %q, want routing (first POST retryable, still pending)", r.Status)
	}

	// Widen the allowlist to include U0OUTSIDER, then redrive the pending target.
	widened := mpimAllowlistDir("U0HUMAN01", "U0OUTSIDER")
	if err := os.WriteFile(h.dirPath, marshalDirectory(t, widened), 0o600); err != nil {
		t.Fatalf("rewrite directory: %v", err)
	}
	if err := h.dirStore.StageReload(h.dirPath); err != nil {
		t.Fatalf("StageReload: %v", err)
	}
	h.gw.triggerDelivery(mpimOrigin(ts))
	h.wait()

	r = getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("post-retry status = %q, want delivered", r.Status)
	}
	calls := gc.sessionCalls()
	if len(calls) != 2 {
		t.Fatalf("session POSTs = %d, want 2 (initial 500 + retry)", len(calls))
	}
	if calls[0].body != calls[1].body {
		t.Errorf("reminder bytes diverged across allowlist change:\n#1 %q\n#2 %q", calls[0].body, calls[1].body)
	}
	if !strings.Contains(calls[0].body, "root_provenance: "+companyRootProvenanceUnlisted) {
		t.Errorf("frozen provenance not the downgraded value: %q", calls[0].body)
	}
}

// ---- C2/C6: missing woken-agent token BLOCKS the wake ----------------------

func TestMpimProbeMissingTokenBlocksNoWake(t *testing.T) {
	// ollie is mentioned + bound, but its own bot token is absent so the membership
	// probe cannot run. A missing token is durable local state, not a transient
	// probe error, so the wake is BLOCKED (failed_mpim_not_member), never proceeds
	// — otherwise a forged event could wake a bound-but-tokenless agent.
	h, gc, _, _ := setupMpim(t)
	if err := os.Remove(filepath.Join(h.gw.secretsDir, "bot-token-ollie.txt")); err != nil {
		t.Fatalf("remove token: %v", err)
	}
	ts := "1700000000.001230"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> are you here?")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusFailed {
		t.Fatalf("status = %q, want failed", r.Status)
	}
	var blocked bool
	for _, td := range r.Targets {
		if td.Agent == "ollie" && td.Status == companyTargetFailed && td.Detail == companyReasonFailedMpimNotMember {
			blocked = true
		}
	}
	if !blocked {
		t.Fatalf("targets = %+v, want ollie failed_mpim_not_member", r.Targets)
	}
	if got := len(gc.sessionCalls()); got != 0 {
		t.Errorf("missing-token target still woke %d sessions, want 0", got)
	}
}

// ---- C3/m1: admission-owner join loss must not strand other agents ---------

func TestMpimAdmissionOwnerVanishedStillWakesOthers(t *testing.T) {
	// Directory WITHOUT the admission-owner app (riley): models the winner being
	// offboarded from the directory after admission. The mentioned agent (ollie) is
	// still present + bound, so it must still wake; the owner's lost join only
	// degrades the visible ack (counted), it never parks/strands the receipt.
	df := baseDirectoryFile()
	df.Agents = []CompanyAgent{{Name: "ollie", AppID: ollieAppID, BotUserID: "U0AAAAAA1"}}
	df.Rooms = nil
	h, gc, _, _ := setupMpimDir(t, df)
	h.gw.visibleAcks = true
	before := h.gw.dmTokenMissing.Load()

	ts := "1700000000.001120"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> please look")
	ev.Type = "message"
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	origin := mpimOrigin(ts)
	receipt := &IngressReceipt{
		Origin: origin, EventID: "Ev-" + ts, APIAppID: rileyAppID,
		Kind: receiptKindMpim, OwnerAppID: rileyAppID, // owner app NOT in the directory
		Status: ingressStatusReceived, Event: raw,
		ThreadRootTS: deriveHumanRootTS(decodeCompanyMessage(origin, raw)),
	}
	if created, _, aerr := h.gw.store().Admit(receipt); aerr != nil || !created {
		t.Fatalf("admit: created=%v err=%v", created, aerr)
	}

	h.gw.deliverReceipt(origin)

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered (owner join loss must not park)", r.Status)
	}
	calls := gc.sessionCalls()
	if len(calls) != 1 || !strings.Contains(calls[0].path, "/session/ollie/messages") {
		t.Fatalf("session calls = %+v, want exactly one to ollie", calls)
	}
	if r.AckState != ackStateDegraded {
		t.Errorf("AckState = %q, want degraded (owner join lost)", r.AckState)
	}
	if got := h.gw.dmTokenMissing.Load() - before; got != 1 {
		t.Errorf("dmTokenMissing delta = %d, want 1 (ack degraded once)", got)
	}
}

// ---- m4: a vanished woken agent must not write a poison pointer ------------

func TestMpimVanishedTargetSkipsPoisonPointer(t *testing.T) {
	// A receipt with two frozen bound targets: ollie is still in the directory,
	// "ghost" has vanished. Attempts=1 skips the membership probe so both reach the
	// pointer-write step (models a retry pass after the agent left). ghost's app_id
	// would resolve empty; writing an empty-owner_app_id pointer would brick EVERY
	// reply for that session, so the target fails recoverably with no pointer/wake,
	// while ollie's pointer + wake still land.
	h, gc, _, _ := setupMpim(t)
	ts := "1700000000.001280"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> and someone")
	ev.Type = "message"
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	origin := mpimOrigin(ts)
	receipt := &IngressReceipt{
		Origin: origin, EventID: "Ev-" + ts, APIAppID: ollieAppID,
		Kind: receiptKindMpim, OwnerAppID: ollieAppID,
		Status: ingressStatusRouting, Event: raw,
		ThreadRootTS: deriveHumanRootTS(decodeCompanyMessage(origin, raw)),
		Targets: map[string]TargetDelivery{
			companyBoundTargetKeyPrefix + "ollie": {
				Session: "ollie", Kind: wakeKindMpim, Status: companyTargetPending,
				Attempts: 1, IdempotencyKey: "ingress:test:ollie", Agent: "ollie",
			},
			companyBoundTargetKeyPrefix + "ghost-sess": {
				Session: "ghost-sess", Kind: wakeKindMpim, Status: companyTargetPending,
				Attempts: 1, IdempotencyKey: "ingress:test:ghost", Agent: "ghost",
			},
		},
	}
	if created, _, aerr := h.gw.store().Admit(receipt); aerr != nil || !created {
		t.Fatalf("admit: created=%v err=%v", created, aerr)
	}

	h.gw.deliverReceipt(origin)

	r := getMpimReceipt(t, h, ts)
	var ollieDelivered, ghostFailed bool
	for _, td := range r.Targets {
		if td.Agent == "ollie" && td.Status == companyTargetDelivered {
			ollieDelivered = true
		}
		if td.Agent == "ghost" && td.Status == companyTargetFailed && td.Detail == companyReasonFailedMpimAgentUnknown {
			ghostFailed = true
		}
	}
	if !ollieDelivered || !ghostFailed {
		t.Fatalf("targets = %+v, want ollie delivered + ghost failed_mpim_agent_unknown", r.Targets)
	}
	// ollie's pointer exists (readMpimPointer fatals if absent); the poison ghost
	// pointer was NEVER written.
	_ = readMpimPointer(t, h, "ollie")
	if _, statErr := os.Stat(filepath.Join(h.turnsDir, "mpim", "ghost-sess.json")); !os.IsNotExist(statErr) {
		t.Errorf("poison ghost pointer written (err=%v), want absent", statErr)
	}
	calls := gc.sessionCalls()
	if len(calls) != 1 || !strings.Contains(calls[0].path, "/session/ollie/messages") {
		t.Fatalf("session calls = %+v, want exactly one to ollie", calls)
	}
}

// ---- m3: previously-unpinned spec behaviors --------------------------------

func TestMpimRedriveReprobesStillNotMember(t *testing.T) {
	// The probe fires only on Attempts==0 and redrive resets Attempts=0 — the sole
	// thing keeping a redriven failed_mpim_not_member target from waking while the
	// agent is STILL not a member. Redrive without inviting the agent must re-probe
	// and re-fail, never wake.
	h, gc, _, ps := setupMpim(t)
	ps.result = mpimProbeNotMember
	ts := "1700000000.001240"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> here?")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()
	if r := getMpimReceipt(t, h, ts); r.Status != ingressStatusFailed {
		t.Fatalf("first status = %q, want failed", r.Status)
	}

	origin := mpimOrigin(ts)
	r := getMpimReceipt(t, h, ts)
	if _, cerr := h.gw.applyRedrive(r, companyRedriveRequest{Origin: &origin}); cerr != nil {
		t.Fatalf("applyRedrive: %v", cerr)
	}
	h.gw.triggerDelivery(origin)
	h.wait()

	r = getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusFailed {
		t.Fatalf("post-redrive status = %q, want failed (still not a member)", r.Status)
	}
	var stillFailed bool
	for _, td := range r.Targets {
		if td.Agent == "ollie" && td.Status == companyTargetFailed && td.Detail == companyReasonFailedMpimNotMember {
			stillFailed = true
		}
	}
	if !stillFailed {
		t.Fatalf("targets = %+v, want ollie still failed_mpim_not_member", r.Targets)
	}
	if got := len(gc.sessionCalls()); got != 0 {
		t.Errorf("re-probed not-member woke %d sessions, want 0", got)
	}
	if toks := ps.probeTokens(); len(toks) != 2 {
		t.Errorf("probe tokens = %v, want 2 (initial + redrive re-probe)", toks)
	}
}

func TestMpimSessionGuardHoldsThenProceeds(t *testing.T) {
	// The advisory session-existence guard applies to mpim targets (dm-family):
	// a 404 session leaves the target pending/materializing without a delivery
	// POST, and the target delivers once the session materializes.
	h, _, _, _ := setupMpim(t)
	h.gw.verifySessions = true
	var missing atomic.Bool
	missing.Store(true)
	var deliveryPosts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && !strings.HasSuffix(r.URL.Path, "/messages") {
			if missing.Load() {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/messages") {
			deliveryPosts.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.001250"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> ping")
	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusRouting {
		t.Fatalf("status = %q, want routing (guard held, not terminal)", r.Status)
	}
	if deliveryPosts.Load() != 0 {
		t.Fatalf("guard did not prevent the delivery POST: posts=%d", deliveryPosts.Load())
	}
	var detail string
	for _, td := range r.Targets {
		detail = td.Detail
	}
	if detail != companyDetailMaterializing {
		t.Fatalf("target detail = %q, want materializing", detail)
	}

	missing.Store(false)
	h.gw.triggerDelivery(mpimOrigin(ts))
	h.wait()
	r = getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status after materialize = %q, want delivered", r.Status)
	}
	if deliveryPosts.Load() != 1 {
		t.Errorf("delivery posts = %d, want 1", deliveryPosts.Load())
	}
}

func TestMpimHealthzGauge(t *testing.T) {
	// The company_mpim_receipts_* /healthz breakdown is folded into the single
	// SweepAndPending scan: a delivered mpim receipt shows on the mpim gauge.
	h, _, _, _ := setupMpim(t)
	ts := "1700000000.001260"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> hi")
	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()
	if r := getMpimReceipt(t, h, ts); r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered", r.Status)
	}

	h.gw.sweepOnce()
	detail := h.gw.healthzDetail()
	if !strings.Contains(detail, "company_mpim_receipts_delivered=1") {
		t.Errorf("healthz missing company_mpim_receipts_delivered=1: %q", detail)
	}
}

func TestMpimNonAgentMentionTerminalNoMention(t *testing.T) {
	// A human message that DOES mention someone, but only a non-agent user id (no
	// directory bot_user_id match) → terminal mpim_no_mention, nobody woken. Pins
	// the mpimWokenAgents filter beyond the mention-free case.
	h, gc, _, _ := setupMpim(t)
	ts := "1700000000.001270"
	ev := mpimMessage("U0HUMAN01", ts, "hey <@U0NOTANAGENT> take a look")

	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusNoDelivery || r.Reason != wakeReasonMpimNoMention {
		t.Fatalf("status/reason = %q/%q, want no_delivery/mpim_no_mention", r.Status, r.Reason)
	}
	if len(r.Targets) != 0 || len(gc.sessionCalls()) != 0 {
		t.Errorf("non-agent mention woke someone: targets=%+v calls=%d", r.Targets, len(gc.sessionCalls()))
	}
}
