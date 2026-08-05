package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// company_ingress_test.go — integration coverage for the Slack
// company-rooms Phase 1d adapter wiring: durable admission, the delivery
// worker, parking, the startup barrier, saturation backpressure, and the
// legacy-path-untouched guarantee. Fake gc is an httptest.Server used as
// gcAPIBase; the receipt store, directory, and bindings are file-backed so
// restart / reload behavior is exercised faithfully.

const (
	testTeamID    = "T0AAAAAAA"
	testChannelID = "C0AAAAAAA"
)

// ---- fake gc ---------------------------------------------------------------

type gcDelivery struct {
	path    string
	idemKey string
	body    string
}

type fakeGC struct {
	server *httptest.Server
	mu     sync.Mutex
	calls  []gcDelivery
	// hook runs (outside the lock) after a request is recorded, receiving
	// its zero-based index. Tests use it to inject a mid-delivery crash.
	hook func(reqNum int)
	// respStatus, when set, returns the HTTP status the handler should
	// write for the reqNum-th recorded request (0 => 200). Tests use it to
	// simulate gc rejecting or throttling a delivery. Set before sending.
	respStatus func(reqNum int) int
}

func newFakeGC(t *testing.T) *fakeGC {
	t.Helper()
	f := &fakeGC{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var m gcSessionMessageRequest
		_ = json.Unmarshal(raw, &m) // errors (e.g. inbound object body) leave Message empty
		f.mu.Lock()
		f.calls = append(f.calls, gcDelivery{
			path:    r.URL.Path,
			idemKey: r.Header.Get("Idempotency-Key"),
			body:    m.Message,
		})
		reqNum := len(f.calls) - 1
		hook := f.hook
		respStatus := f.respStatus
		f.mu.Unlock()
		if hook != nil {
			hook(reqNum)
		}
		status := http.StatusOK
		if respStatus != nil {
			if s := respStatus(reqNum); s != 0 {
				status = s
			}
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeGC) sessionCalls() []gcDelivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []gcDelivery
	for _, c := range f.calls {
		if strings.Contains(c.path, "/session/") && strings.HasSuffix(c.path, "/messages") {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeGC) inboundCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if strings.HasSuffix(c.path, "/extmsg/inbound") {
			n++
		}
	}
	return n
}

// fakeAuthorResolver is a deterministic companyAuthorResolver for tests: it
// resolves known bot ids to a fixed identity and reports every other bot id as
// a definitive unknown, so bot-leg tests never touch the Slack API.
type fakeAuthorResolver struct {
	byBot     map[string]companyBotInfo
	transient map[string]bool // bot ids that report a transient failure
}

func (f fakeAuthorResolver) Resolve(botID string) (companyBotInfo, botResolveOutcome) {
	if f.transient[botID] {
		return companyBotInfo{}, botResolveTransient
	}
	if info, ok := f.byBot[botID]; ok {
		return info, botResolveOK
	}
	return companyBotInfo{}, botResolveUnknown
}

// ---- harness ---------------------------------------------------------------

type companyHarness struct {
	gw         *companyGateway
	dirStore   *companyDirectoryStore
	bindStore  *companyBindingsStore
	receipts   *IngressReceiptStore
	ingressDir string
	dirPath    string
	bindPath   string
	// Phase 2 shared-state directories.
	intentsDir     string
	delegationsDir string
	turnsDir       string
	locksDir       string
}

// newCompanyHarness builds a file-backed harness. df/bf nil means the
// corresponding registry file is absent (routing / bindings disabled).
func newCompanyHarness(t *testing.T, gcURL string, df *companyDirectoryFile, bf *companyBindingsFile, semCap int) *companyHarness {
	t.Helper()
	root := t.TempDir()
	ingressDir := filepath.Join(root, "chat-ingress")
	dirPath := filepath.Join(root, "company_directory.json")
	bindPath := filepath.Join(root, "company_bindings.json")
	if df != nil {
		if err := os.WriteFile(dirPath, marshalDirectory(t, *df), 0o600); err != nil {
			t.Fatalf("write directory: %v", err)
		}
	}
	if bf != nil {
		if err := os.WriteFile(bindPath, marshalBindings(t, *bf), 0o600); err != nil {
			t.Fatalf("write bindings: %v", err)
		}
	}
	h := &companyHarness{
		ingressDir:     ingressDir,
		dirPath:        dirPath,
		bindPath:       bindPath,
		intentsDir:     filepath.Join(root, "company-delegation-intents"),
		delegationsDir: filepath.Join(root, "company-delegations"),
		turnsDir:       filepath.Join(root, "company-current-turn"),
		locksDir:       filepath.Join(root, "locks"),
	}
	h.reopen(t, gcURL, semCap)
	// Ensure t.TempDir cleanup can remove a dir a test left read-only.
	t.Cleanup(func() { _ = os.Chmod(ingressDir, 0o700) })
	return h
}

// reopen rebuilds the stores + gateway over the same on-disk paths,
// modeling an adapter restart. The barrier starts closed.
func (h *companyHarness) reopen(t *testing.T, gcURL string, semCap int) {
	t.Helper()
	receipts, err := NewIngressReceiptStore(h.ingressDir)
	if err != nil {
		t.Fatalf("NewIngressReceiptStore: %v", err)
	}
	dirStore := &companyDirectoryStore{}
	if err := dirStore.Load(h.dirPath); err != nil {
		t.Fatalf("directory load: %v", err)
	}
	bindStore := &companyBindingsStore{}
	if err := bindStore.Load(h.bindPath, dirStore.Snapshot()); err != nil {
		t.Fatalf("bindings load: %v", err)
	}
	cfg := config{
		gcAPIBase:             gcURL,
		cityName:              "test-city",
		provider:              "slack",
		accountID:             testTeamID,
		dispatchSem:           make(chan struct{}, semCap),
		companyIntentsDir:     h.intentsDir,
		companyDelegationsDir: h.delegationsDir,
		companyTurnsDir:       h.turnsDir,
		companyLocksDir:       h.locksDir,
	}
	h.receipts = receipts
	h.dirStore = dirStore
	h.bindStore = bindStore
	h.gw = newCompanyGateway(cfg, dirStore, bindStore, receipts)
	// Default: hydration returns a deterministic "unavailable" bundle so the
	// common integration tests never touch Slack. Bot/peer/hydration tests
	// override h.gw.hydrate / h.gw.authors explicitly.
	h.gw.hydrate = func(CompanyMessage) companyHydration {
		return companyHydration{RootProvenance: companyRootProvenanceUnverified, ContextStatus: companyContextUnavailable}
	}
}

func (h *companyHarness) openBarrier() { h.gw.barrier.Store(true) }

func (h *companyHarness) wait() { h.gw.deliverWG.Wait() }

func baseOrigin() ReceiptOrigin {
	return ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.000100"}
}

// admitViaHandler drives tryHandleEvent directly (past HMAC verification),
// returning the recorder and whether the gateway owned the event.
func (h *companyHarness) admitViaHandler(t *testing.T, ev slackMessageEvent, retryNum int) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	env := companyEnvelope(t, ev)
	req := httptest.NewRequest(http.MethodPost, "/slack/events", nil)
	if retryNum > 0 {
		req.Header.Set("X-Slack-Retry-Num", strconv.Itoa(retryNum))
		req.Header.Set("X-Slack-Retry-Reason", "http_timeout")
	}
	w := httptest.NewRecorder()
	handled := h.gw.tryHandleEvent(w, req, env, h.gw.agentAppsSnapshot())
	return w, handled
}

func companyEnvelope(t *testing.T, ev slackMessageEvent) slackEventEnvelope {
	t.Helper()
	ev.Type = "message"
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return slackEventEnvelope{
		Type:     "event_callback",
		TeamID:   testTeamID,
		APIAppID: "A0SWITCH",
		EventID:  "Ev-" + ev.TS,
		Event:    raw,
	}
}

func humanMessage(ts, text string) slackMessageEvent {
	return slackMessageEvent{User: "Uhuman", Channel: testChannelID, TS: ts, Text: text}
}

// ---- acceptance 8: crash before delivery -> restart delivers exactly once ---

func TestCompanyCrashBeforeDeliveryDeliversExactlyOnce(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	// Admit directly, then "crash" before any delivery ran.
	origin := baseOrigin()
	ev := humanMessage(origin.TS, "hello team")
	rawEv, _ := json.Marshal(ev)
	created, _, err := h.receipts.Admit(&IngressReceipt{Origin: origin, Event: rawEv, Status: ingressStatusReceived})
	if err != nil || !created {
		t.Fatalf("admit: created=%v err=%v", created, err)
	}

	// Restart: fresh stores + worker over the same directory; run the
	// synchronous recovery pass.
	h.reopen(t, gc.server.URL, 4)
	if err := h.gw.recoverPending(); err != nil {
		t.Fatalf("recoverPending: %v", err)
	}
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 1 {
		t.Fatalf("session deliveries = %d, want exactly 1: %+v", len(calls), calls)
	}
	if !strings.HasSuffix(calls[0].path, "/session/ollie-main/messages") {
		t.Errorf("delivery path = %q, want ambient session ollie-main", calls[0].path)
	}
	wantKey := companyIdempotencyKey(receiptID(origin), "ollie-main")
	if calls[0].idemKey != wantKey {
		t.Errorf("idempotency key = %q, want %q", calls[0].idemKey, wantKey)
	}
	if r, _ := h.receipts.Get(origin); r == nil || r.Status != ingressStatusDelivered {
		t.Errorf("receipt status = %v, want delivered", statusOf(r))
	}
}

// ---- acceptance 8: crash after submit before record -> same idempotency key -

func TestCompanyCrashAfterSubmitBeforeRecordSameKey(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	// On the first delivery, gc records the key then the receipts dir is
	// made read-only so the finalize Update (the delivery record) fails —
	// modeling a crash after submit, before record.
	gc.hook = func(reqNum int) {
		if reqNum == 0 {
			_ = os.Chmod(h.ingressDir, 0o500)
		}
	}

	ev := humanMessage(baseOrigin().TS, "hello team")
	w, handled := h.admitViaHandler(t, ev, 0)
	if !handled || w.Result().StatusCode != http.StatusOK {
		t.Fatalf("admit: handled=%v status=%d", handled, w.Result().StatusCode)
	}
	h.wait()

	if got := len(gc.sessionCalls()); got != 1 {
		t.Fatalf("first pass deliveries = %d, want 1", got)
	}

	// "Restart" with a writable store and re-drive the still-pending
	// receipt directly (the sweep would skip a fresh routing claim).
	if err := os.Chmod(h.ingressDir, 0o700); err != nil {
		t.Fatalf("chmod restore: %v", err)
	}
	gc.hook = nil
	h.reopen(t, gc.server.URL, 4)
	h.gw.deliverReceipt(baseOrigin())
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 2 {
		t.Fatalf("total deliveries = %d, want 2 (redelivery after crash)", len(calls))
	}
	if calls[0].idemKey == "" || calls[0].idemKey != calls[1].idemKey {
		t.Errorf("idempotency keys differ across crash: %q vs %q", calls[0].idemKey, calls[1].idemKey)
	}
	wantKey := companyIdempotencyKey(receiptID(baseOrigin()), "ollie-main")
	if calls[1].idemKey != wantKey {
		t.Errorf("idempotency key = %q, want %q", calls[1].idemKey, wantKey)
	}
}

// ---- acceptance 9: retry-num redelivery -> 200, no second delivery ----------

func TestCompanyRetryRedeliveryNoSecondDelivery(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	ev := humanMessage(baseOrigin().TS, "hello team")
	w1, _ := h.admitViaHandler(t, ev, 0)
	if w1.Result().StatusCode != http.StatusOK {
		t.Fatalf("first admit status = %d, want 200", w1.Result().StatusCode)
	}
	h.wait()
	if got := len(gc.sessionCalls()); got != 1 {
		t.Fatalf("after admit deliveries = %d, want 1", got)
	}

	// Slack redelivery of the same origin (X-Slack-Retry-Num=1).
	w2, handled := h.admitViaHandler(t, ev, 1)
	if !handled || w2.Result().StatusCode != http.StatusOK {
		t.Fatalf("redelivery: handled=%v status=%d, want handled 200", handled, w2.Result().StatusCode)
	}
	h.wait()
	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("after redelivery deliveries = %d, want still 1", got)
	}
}

// ---- acceptance 10: store failure -> 503 without no-retry, then admits ------

func TestCompanyStoreFailure503ThenRetryAdmits(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	// Make the receipts dir read-only so Admit's temp write fails.
	if err := os.Chmod(h.ingressDir, 0o500); err != nil {
		t.Fatalf("chmod ro: %v", err)
	}
	before := h.receipts.WriteFailures()

	ev := humanMessage(baseOrigin().TS, "hello team")
	w, handled := h.admitViaHandler(t, ev, 0)
	if !handled {
		t.Fatal("store-failure event not handled by gateway")
	}
	if w.Result().StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Result().StatusCode)
	}
	if v := w.Header().Get("X-Slack-No-Retry"); v != "" {
		t.Errorf("X-Slack-No-Retry = %q, want unset (Slack must retry)", v)
	}
	if h.receipts.WriteFailures() <= before {
		t.Errorf("WriteFailures not incremented: before=%d after=%d", before, h.receipts.WriteFailures())
	}
	if got := len(gc.sessionCalls()); got != 0 {
		t.Errorf("deliveries = %d, want 0 (nothing admitted)", got)
	}

	// Restore writability; the retry admits and delivers once.
	if err := os.Chmod(h.ingressDir, 0o700); err != nil {
		t.Fatalf("chmod rw: %v", err)
	}
	w2, _ := h.admitViaHandler(t, ev, 1)
	if w2.Result().StatusCode != http.StatusOK {
		t.Fatalf("retry status = %d, want 200", w2.Result().StatusCode)
	}
	h.wait()
	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("after retry deliveries = %d, want 1", got)
	}
}

// ---- acceptance 11: parked (no directory room) -> restored -> sweep delivers -

func TestCompanyParkedThenSweepDeliversAfterRestore(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	// Admit directly, then remove the directory (channel matches no room).
	origin := baseOrigin()
	ev := humanMessage(origin.TS, "hello team")
	rawEv, _ := json.Marshal(ev)
	if created, _, err := h.receipts.Admit(&IngressReceipt{Origin: origin, Event: rawEv, Status: ingressStatusReceived}); err != nil || !created {
		t.Fatalf("admit: created=%v err=%v", created, err)
	}
	if err := os.Remove(h.dirPath); err != nil {
		t.Fatalf("remove directory: %v", err)
	}
	if err := h.dirStore.StageReload(h.dirPath); err != nil {
		t.Fatalf("reload to nil directory: %v", err)
	}

	// Delivery with no directory room parks the receipt.
	h.gw.deliverReceipt(origin)
	parked, _ := h.receipts.Get(origin)
	if parked == nil || parked.Status != ingressStatusReceived || parked.Reason != companyReasonParked {
		t.Fatalf("parked receipt = %+v, want status received reason %q", parked, companyReasonParked)
	}
	if got := len(gc.sessionCalls()); got != 0 {
		t.Fatalf("parked delivered %d times, want 0", got)
	}

	// Restore the directory; the sweep delivers.
	if err := os.WriteFile(h.dirPath, marshalDirectory(t, df), 0o600); err != nil {
		t.Fatalf("restore directory: %v", err)
	}
	if err := h.dirStore.StageReload(h.dirPath); err != nil {
		t.Fatalf("reload restored directory: %v", err)
	}
	h.gw.sweepOnce()
	h.wait()

	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("after restore deliveries = %d, want 1", got)
	}
	if r, _ := h.receipts.Get(origin); r == nil || r.Status != ingressStatusDelivered {
		t.Errorf("receipt status = %v, want delivered", statusOf(r))
	}
}

// ---- ambient routing end to end -------------------------------------------

func TestCompanyAmbientRoutingEndToEnd(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile() // ambient_wake = [ollie]
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	w, _ := h.admitViaHandler(t, humanMessage(baseOrigin().TS, "morning all"), 0)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Result().StatusCode)
	}
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 1 {
		t.Fatalf("ambient deliveries = %d, want exactly 1 (ollie only): %+v", len(calls), calls)
	}
	if !strings.HasSuffix(calls[0].path, "/session/ollie-main/messages") {
		t.Errorf("ambient delivery path = %q, want ollie-main", calls[0].path)
	}
	if !strings.Contains(calls[0].body, "ambient delivery") {
		t.Errorf("reminder body missing ambient kind: %q", calls[0].body)
	}
	if !strings.Contains(calls[0].body, "orchestrator-team") {
		t.Errorf("reminder body missing room name: %q", calls[0].body)
	}
	if !strings.Contains(calls[0].body, "UNTRUSTED") {
		t.Errorf("reminder body not labeled untrusted: %q", calls[0].body)
	}
}

// ---- mention routing wakes only the mentioned agent ------------------------

func TestCompanyMentionRoutingWakesOnlyMentioned(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile() // ambient ollie; mention_wake ollie+riley
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	// Human mentions riley (bot_user_id U0AAAAAA2) as a canonical token.
	w, _ := h.admitViaHandler(t, humanMessage(baseOrigin().TS, "please look <@U0AAAAAA2>"), 0)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Result().StatusCode)
	}
	h.wait()

	calls := gc.sessionCalls()
	if len(calls) != 1 {
		t.Fatalf("mention deliveries = %d, want exactly 1 (riley only): %+v", len(calls), calls)
	}
	if !strings.HasSuffix(calls[0].path, "/session/riley-main/messages") {
		t.Errorf("mention delivery path = %q, want riley-main (ambient ollie suppressed)", calls[0].path)
	}
	if !strings.Contains(calls[0].body, "targeted delivery") {
		t.Errorf("reminder body missing targeted kind: %q", calls[0].body)
	}
}

// ---- saturation backpressure: no slot -> pending -> sweep delivers ---------

func TestCompanySaturationBackpressureSweepDelivers(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 1)
	h.openBarrier()

	// Hold the only dispatch slot so the admit-time delivery trigger finds
	// no slot and leaves the receipt pending.
	release, _, ok := h.gw.cfg.acquireDispatchSlot()
	if !ok {
		t.Fatal("failed to take the sole dispatch slot")
	}

	origin := baseOrigin()
	w, _ := h.admitViaHandler(t, humanMessage(origin.TS, "hello"), 0)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (admitted despite saturation)", w.Result().StatusCode)
	}
	h.wait() // no goroutine was started; returns immediately
	if got := len(gc.sessionCalls()); got != 0 {
		t.Fatalf("deliveries under saturation = %d, want 0 (pending)", got)
	}
	if r, _ := h.receipts.Get(origin); r == nil || r.Status != ingressStatusReceived {
		t.Fatalf("receipt status = %v, want received (pending)", statusOf(r))
	}

	// Free the slot; the sweep redrives the pending receipt.
	release()
	h.gw.sweepOnce()
	h.wait()
	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("after sweep deliveries = %d, want 1", got)
	}
}

// ---- legacy path untouched for non-company channels ------------------------

func TestCompanyLegacyPathUntouchedForNonCompanyChannel(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile() // company room is C0AAAAAAA
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 8)
	h.openBarrier()

	const secret = "test-signing-secret"
	cfg := config{
		gcAPIBase:       gc.server.URL,
		cityName:        "test-city",
		provider:        "slack",
		accountID:       testTeamID,
		slackSigningKey: secret,
		dispatchSem:     make(chan struct{}, 8),
		companyGateway:  h.gw,
	}
	// The gateway shares cfg.dispatchSem with the handler so both paths
	// draw from the same bound (mirrors production wiring).
	h.gw.cfg.dispatchSem = cfg.dispatchSem
	aliasReg := newTestHandleAliasRegistry(t)
	handler := handleSlackEvents(cfg, aliasReg, nil, nil, nil, nil)

	// A human message in a NON-company channel flows through the legacy
	// path: 200, a POST to /extmsg/inbound, and no company receipt.
	legacyEv := slackMessageEvent{Type: "message", User: "Uhuman", Channel: "C_OTHER", TS: "1700000000.900001", Text: "legacy hi"}
	rawLegacy, _ := json.Marshal(legacyEv)
	envLegacy, _ := json.Marshal(slackEventEnvelope{Type: "event_callback", TeamID: testTeamID, Event: rawLegacy})
	req := signedSlackEventRequest(t, secret, envLegacy)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("legacy event status = %d, want 200", w.Result().StatusCode)
	}
	waitUntil(t, func() bool { return gc.inboundCalls() >= 1 })
	if got := len(gc.sessionCalls()); got != 0 {
		t.Errorf("non-company channel produced %d company deliveries, want 0", got)
	}
	// No company receipt should exist for the non-company origin.
	if r, _ := h.receipts.Get(ReceiptOrigin{TeamID: testTeamID, ChannelID: "C_OTHER", TS: legacyEv.TS}); r != nil {
		t.Errorf("non-company event created a company receipt: %+v", r)
	}

	// A human message IN the company channel takes the company path: a
	// receipt is created and delivered, and NO legacy /extmsg/inbound POST
	// fires for it.
	inboundBefore := gc.inboundCalls()
	companyEv := slackMessageEvent{Type: "message", User: "Uhuman", Channel: testChannelID, TS: "1700000000.900002", Text: "company hi"}
	rawCompany, _ := json.Marshal(companyEv)
	envCompany, _ := json.Marshal(slackEventEnvelope{Type: "event_callback", TeamID: testTeamID, Event: rawCompany})
	req2 := signedSlackEventRequest(t, secret, envCompany)
	w2 := httptest.NewRecorder()
	handler(w2, req2)
	if w2.Result().StatusCode != http.StatusOK {
		t.Fatalf("company event status = %d, want 200", w2.Result().StatusCode)
	}
	h.wait()
	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("company channel deliveries = %d, want 1", got)
	}
	if gc.inboundCalls() != inboundBefore {
		t.Errorf("company channel event triggered a legacy /extmsg/inbound POST (before=%d after=%d)", inboundBefore, gc.inboundCalls())
	}
}

// ---- F1: degraded store -> 503, never legacy, /healthz store_error --------

func TestCompanyDegradedStore503NeverLegacyThenRecovers(t *testing.T) {
	gc := newFakeGC(t)
	root := t.TempDir()
	// A regular file where the ingress dir's parent should be, so
	// NewIngressReceiptStore's MkdirAll fails: the gateway starts DEGRADED.
	blocker := filepath.Join(root, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("seed blocker: %v", err)
	}
	badIngress := filepath.Join(blocker, "chat-ingress")

	dirPath := filepath.Join(root, "company_directory.json")
	bindPath := filepath.Join(root, "company_bindings.json")
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	if err := os.WriteFile(dirPath, marshalDirectory(t, df), 0o600); err != nil {
		t.Fatalf("write dir: %v", err)
	}
	if err := os.WriteFile(bindPath, marshalBindings(t, bf), 0o600); err != nil {
		t.Fatalf("write bindings: %v", err)
	}
	dirStore := &companyDirectoryStore{}
	if err := dirStore.Load(dirPath); err != nil {
		t.Fatalf("dir load: %v", err)
	}
	bindStore := &companyBindingsStore{}
	if err := bindStore.Load(bindPath, dirStore.Snapshot()); err != nil {
		t.Fatalf("bind load: %v", err)
	}
	cfg := config{
		gcAPIBase:         gc.server.URL,
		cityName:          "test-city",
		provider:          "slack",
		accountID:         testTeamID,
		dispatchSem:       make(chan struct{}, 4),
		companyIngressDir: badIngress,
		companyTurnsDir:   filepath.Join(root, "company-current-turn"),
	}
	receipts, rerr := NewIngressReceiptStore(badIngress)
	if rerr == nil {
		t.Fatal("expected store construction to fail on blocked ingress dir")
	}
	gw := newCompanyGateway(cfg, dirStore, bindStore, receipts) // receipts nil -> degraded
	gw.setStoreError(rerr)

	// A company-room admissible event: owned (not legacy), 503 without the
	// no-retry header, no receipt.
	env := companyEnvelope(t, humanMessage(baseOrigin().TS, "hi"))
	req := httptest.NewRequest(http.MethodPost, "/slack/events", nil)
	w := httptest.NewRecorder()
	if handled := gw.tryHandleEvent(w, req, env, gw.agentAppsSnapshot()); !handled {
		t.Fatal("degraded gateway did not own the company event (would fall to legacy)")
	}
	if w.Result().StatusCode != http.StatusServiceUnavailable {
		t.Errorf("degraded status = %d, want 503", w.Result().StatusCode)
	}
	if v := w.Header().Get("X-Slack-No-Retry"); v != "" {
		t.Errorf("X-Slack-No-Retry = %q, want unset (Slack must retry)", v)
	}
	if got := len(gc.sessionCalls()); got != 0 {
		t.Errorf("degraded gateway delivered %d, want 0", got)
	}
	if gc.inboundCalls() != 0 {
		t.Errorf("degraded gateway leaked to legacy /extmsg/inbound")
	}

	// /healthz reports the degraded store (paging hook).
	detail := gw.healthzDetail()
	if !strings.Contains(detail, "company_store_ready=false") {
		t.Errorf("healthz missing company_store_ready=false: %q", detail)
	}
	if strings.Contains(detail, `company_store_error=""`) || !strings.Contains(detail, "company_store_error=") {
		t.Errorf("healthz store_error not populated: %q", detail)
	}

	// Recover: clear the blocker so construction can succeed. startRecovery
	// (production path) constructs the store, opens the barrier, and admits.
	if err := os.Remove(blocker); err != nil {
		t.Fatalf("remove blocker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gw.cfg.dispatchSem = cfg.dispatchSem
	gw.startRecovery(ctx)
	waitUntil(t, func() bool { return gw.barrier.Load() })

	env2 := companyEnvelope(t, humanMessage("1700000000.000250", "hello again"))
	req2 := httptest.NewRequest(http.MethodPost, "/slack/events", nil)
	w2 := httptest.NewRecorder()
	if handled := gw.tryHandleEvent(w2, req2, env2, gw.agentAppsSnapshot()); !handled {
		t.Fatal("recovered gateway did not own the company event")
	}
	if w2.Result().StatusCode != http.StatusOK {
		t.Errorf("post-recovery status = %d, want 200", w2.Result().StatusCode)
	}
	gw.deliverWG.Wait()
	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("post-recovery deliveries = %d, want 1", got)
	}
	if !strings.Contains(gw.healthzDetail(), "company_store_ready=true") {
		t.Errorf("healthz still degraded after recovery: %q", gw.healthzDetail())
	}
}

// ---- F(minor): closed barrier -> 503 (no no-retry), open -> 200; retry hdrs -

func TestCompanyBarrierClosed503ThenOpenAdmitsWithRetryHeaders(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	// Barrier is closed after reopen.
	ev := humanMessage(baseOrigin().TS, "hello team")
	w, handled := h.admitViaHandler(t, ev, 0)
	if !handled {
		t.Fatal("closed-barrier company event not owned by gateway (would fall to legacy)")
	}
	if w.Result().StatusCode != http.StatusServiceUnavailable {
		t.Errorf("closed-barrier status = %d, want 503", w.Result().StatusCode)
	}
	if v := w.Header().Get("X-Slack-No-Retry"); v != "" {
		t.Errorf("X-Slack-No-Retry = %q, want unset on barrier 503", v)
	}
	if r, _ := h.receipts.Get(baseOrigin()); r != nil {
		t.Errorf("receipt created while barrier closed: %+v", r)
	}

	// Open the barrier; the Slack retry (retryNum=1) admits and delivers once.
	h.openBarrier()
	w2, _ := h.admitViaHandler(t, ev, 1)
	if w2.Result().StatusCode != http.StatusOK {
		t.Fatalf("post-open status = %d, want 200", w2.Result().StatusCode)
	}
	h.wait()
	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("after open deliveries = %d, want 1", got)
	}
	// The admitted receipt durably carries the Slack redelivery evidence.
	r, _ := h.receipts.Get(baseOrigin())
	if r == nil || r.RetryNum != 1 || r.RetryReason != "http_timeout" {
		t.Errorf("retry headers not captured on receipt: %+v", r)
	}
}

// ---- F4: app_mention in a company room is owned; no receipt, no legacy ------

func TestCompanyAppMentionOwnedNoReceiptNoLegacy(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 8)
	h.openBarrier()

	// Unit: an app_mention in a NON-company channel is NOT owned (legacy).
	amOther := companyEnvelope(t, slackMessageEvent{})
	amOther.TeamID = testTeamID
	{
		ev := slackMessageEvent{Type: "app_mention", User: "Uhuman", Channel: "C_OTHER", TS: "1700000000.560000", Text: "hi"}
		raw, _ := json.Marshal(ev)
		amOther.Event = raw
		w := httptest.NewRecorder()
		if handled := h.gw.tryHandleEvent(w, httptest.NewRequest(http.MethodPost, "/slack/events", nil), amOther, h.gw.agentAppsSnapshot()); handled {
			t.Error("app_mention in non-company channel was owned by gateway; want legacy fallthrough")
		}
	}

	const secret = "test-signing-secret"
	cfg := config{
		gcAPIBase:       gc.server.URL,
		cityName:        "test-city",
		provider:        "slack",
		accountID:       testTeamID,
		slackSigningKey: secret,
		dispatchSem:     make(chan struct{}, 8),
		companyGateway:  h.gw,
	}
	h.gw.cfg.dispatchSem = cfg.dispatchSem
	aliasReg := newTestHandleAliasRegistry(t)
	handler := handleSlackEvents(cfg, aliasReg, nil, nil, nil, nil)

	// app_mention IN the company channel: owned -> 200, no receipt, and it
	// must never reach the legacy /extmsg/inbound path.
	amEv := slackMessageEvent{Type: "app_mention", User: "Uhuman", Channel: testChannelID, TS: "1700000000.560001", Text: "<@U0SWITCH> status?"}
	rawAM, _ := json.Marshal(amEv)
	envAM, _ := json.Marshal(slackEventEnvelope{Type: "event_callback", TeamID: testTeamID, Event: rawAM})
	wAM := httptest.NewRecorder()
	handler(wAM, signedSlackEventRequest(t, secret, envAM))
	if wAM.Result().StatusCode != http.StatusOK {
		t.Fatalf("company app_mention status = %d, want 200", wAM.Result().StatusCode)
	}
	if r, _ := h.receipts.Get(ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: amEv.TS}); r != nil {
		t.Errorf("company app_mention created a receipt: %+v", r)
	}

	// A plain human message in a NON-company channel DOES reach legacy: its
	// inbound POST proves the handler/legacy path are wired and would have
	// fired for the app_mention too if it were not owned.
	legacyEv := slackMessageEvent{Type: "message", User: "Uhuman", Channel: "C_OTHER", TS: "1700000000.560002", Text: "legacy hi"}
	rawL, _ := json.Marshal(legacyEv)
	envL, _ := json.Marshal(slackEventEnvelope{Type: "event_callback", TeamID: testTeamID, Event: rawL})
	handler(httptest.NewRecorder(), signedSlackEventRequest(t, secret, envL))
	waitUntil(t, func() bool { return gc.inboundCalls() >= 1 })

	if got := gc.inboundCalls(); got != 1 {
		t.Errorf("legacy inbound calls = %d, want exactly 1 (the app_mention must have added none)", got)
	}
	if got := len(gc.sessionCalls()); got != 0 {
		t.Errorf("app_mention produced %d company deliveries, want 0", got)
	}
}

// ---- F(test): bot author -> receipt admitted, terminal no_delivery, no wake -

func TestCompanyBotAuthorTerminalNoDelivery(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	// A resolved company bot with no native company mention wakes nobody
	// (Phase 2c company_bot_no_mention). The resolver is faked so the test
	// does not touch Slack.
	h.gw.authors = fakeAuthorResolver{byBot: map[string]companyBotInfo{
		"B0RILEY": {UserID: "U0AAAAAA2", AppID: "A0AAAAAA2"},
	}}
	h.openBarrier()

	// bot_message from riley's registered bot_user_id: admissible subtype,
	// but a bot author with no company mention wakes nobody.
	ev := slackMessageEvent{Subtype: "bot_message", BotID: "B0RILEY", User: "U0AAAAAA2", Channel: testChannelID, TS: baseOrigin().TS, Text: "status update"}
	w, handled := h.admitViaHandler(t, ev, 0)
	if !handled || w.Result().StatusCode != http.StatusOK {
		t.Fatalf("bot message admit: handled=%v status=%d", handled, w.Result().StatusCode)
	}
	h.wait()
	r, _ := h.receipts.Get(baseOrigin())
	if r == nil || r.Status != ingressStatusNoDelivery {
		t.Fatalf("status = %v, want no_delivery", statusOf(r))
	}
	if r.Reason != wakeReasonCompanyBotNoMention {
		t.Errorf("reason = %q, want %q", r.Reason, wakeReasonCompanyBotNoMention)
	}
	if len(r.Targets) != 0 {
		t.Errorf("bot author recorded targets: %+v", r.Targets)
	}
	if got := len(gc.sessionCalls()); got != 0 {
		t.Errorf("bot author delivered %d, want 0", got)
	}
}

// ---- F(test): unbound woken agent -> target failed, no legacy fallback ------

func TestCompanyUnboundTargetFailedNoLegacy(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	// riley is deliberately unbound (only ollie is bound).
	bf := companyBindingsFile{
		SchemaVersion: 1,
		Bindings:      []CompanyBinding{{Room: "orchestrator-team", Agent: "ollie", Session: "ollie-main"}},
	}
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	// Human mentions riley (member + mention-eligible, but unbound).
	w, _ := h.admitViaHandler(t, humanMessage(baseOrigin().TS, "please look <@U0AAAAAA2>"), 0)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Result().StatusCode)
	}
	h.wait()
	r, _ := h.receipts.Get(baseOrigin())
	if r == nil || r.Status != ingressStatusFailed {
		t.Fatalf("status = %v, want failed", statusOf(r))
	}
	var found bool
	for _, td := range r.Targets {
		if td.Status == companyTargetFailed && td.Session == "" &&
			strings.Contains(td.Detail, "no company binding") && strings.Contains(td.Detail, "riley") {
			found = true
		}
	}
	if !found {
		t.Errorf("no failed unbound target recorded: %+v", r.Targets)
	}
	if got := len(gc.sessionCalls()); got != 0 {
		t.Errorf("unbound target produced %d gc session POSTs, want 0", got)
	}
	if gc.inboundCalls() != 0 {
		t.Errorf("unbound target fell back to legacy /extmsg/inbound")
	}
}

// ---- F2: definitive 4xx -> target failed (no retry); 429 stays pending ------

func TestCompanyDefinitive4xxMarksTargetFailed(t *testing.T) {
	gc := newFakeGC(t)
	gc.respStatus = func(int) int { return http.StatusBadRequest } // 400
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	w, _ := h.admitViaHandler(t, humanMessage(baseOrigin().TS, "hi"), 0)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("admit status = %d, want 200", w.Result().StatusCode)
	}
	h.wait()
	r, _ := h.receipts.Get(baseOrigin())
	if r == nil || r.Status != ingressStatusFailed {
		t.Fatalf("status = %v, want failed (definitive 4xx)", statusOf(r))
	}
	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("session POSTs = %d, want exactly 1 (no retry on definitive 4xx)", got)
	}
	if h.gw.deliveryFailures.Load() == 0 {
		t.Errorf("deliveryFailures not counted for a definitive 4xx")
	}
}

func TestCompany429StaysPending(t *testing.T) {
	gc := newFakeGC(t)
	gc.respStatus = func(int) int { return http.StatusTooManyRequests } // 429
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	w, _ := h.admitViaHandler(t, humanMessage(baseOrigin().TS, "hi"), 0)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("admit status = %d, want 200", w.Result().StatusCode)
	}
	h.wait()
	r, _ := h.receipts.Get(baseOrigin())
	if r == nil || r.Status != ingressStatusRouting {
		t.Fatalf("status = %v, want routing (429 stays pending)", statusOf(r))
	}
	var pending bool
	for _, td := range r.Targets {
		if td.Status == companyTargetPending {
			pending = true
		}
	}
	if !pending {
		t.Errorf("429 did not leave the target pending: %+v", r.Targets)
	}
}

// ---- F2: bounded attempts cap -> target failed attempts_exhausted ----------

func TestCompanyAttemptsCapExhausted(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	// Admit, then hand-craft a routing receipt whose recorded target has
	// already hit the attempts cap (a target prior sweeps retried to
	// exhaustion). The next delivery must terminalize it, not POST again.
	origin := baseOrigin()
	rawEv, _ := json.Marshal(humanMessage(origin.TS, "hi"))
	if created, _, err := h.receipts.Admit(&IngressReceipt{Origin: origin, Event: rawEv, Status: ingressStatusReceived}); err != nil || !created {
		t.Fatalf("admit: created=%v err=%v", created, err)
	}
	r, _ := h.receipts.Get(origin)
	r.Status = ingressStatusRouting
	key := companyBoundTargetKeyPrefix + "ollie-main"
	r.Targets = map[string]TargetDelivery{
		key: {
			Session:        "ollie-main",
			Kind:           wakeKindAmbient,
			Status:         companyTargetPending,
			IdempotencyKey: companyIdempotencyKey(r.ID, "ollie-main"),
			Attempts:       companyMaxDeliveryAttempts,
		},
	}
	if err := h.receipts.Update(r); err != nil {
		t.Fatalf("seed routing receipt: %v", err)
	}

	h.gw.deliverReceipt(origin)
	h.wait()
	got, _ := h.receipts.Get(origin)
	if got == nil || got.Status != ingressStatusFailed {
		t.Fatalf("status = %v, want failed (attempts exhausted)", statusOf(got))
	}
	var exhausted bool
	for _, td := range got.Targets {
		if td.Status == companyTargetFailed && strings.Contains(td.Detail, companyReasonAttemptsExhausted) {
			exhausted = true
		}
	}
	if !exhausted {
		t.Errorf("target not marked attempts_exhausted: %+v", got.Targets)
	}
	if got := len(gc.sessionCalls()); got != 0 {
		t.Errorf("exhausted target still POSTed %d times, want 0", got)
	}
}

// ---- F3: route freeze -> recorded targets driven, never re-terminalized ----

func TestCompanyRouteFreezeDrivesRecordedTargetsAfterDirectoryShrink(t *testing.T) {
	gc := newFakeGC(t)
	// First delivery attempt 5xx (retryable -> target stays pending); the
	// redrive attempt succeeds.
	gc.respStatus = func(reqNum int) int {
		if reqNum == 0 {
			return http.StatusBadGateway // 502
		}
		return http.StatusOK
	}
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	origin := baseOrigin()
	w, _ := h.admitViaHandler(t, humanMessage(origin.TS, "hi"), 0)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("admit status = %d, want 200", w.Result().StatusCode)
	}
	h.wait()
	r, _ := h.receipts.Get(origin)
	if r == nil || r.Status != ingressStatusRouting {
		t.Fatalf("after first delivery status = %v, want routing (target pending)", statusOf(r))
	}
	if len(r.Targets) != 1 {
		t.Fatalf("recorded targets = %+v, want exactly 1 (frozen route)", r.Targets)
	}

	// Shrink the directory: ambient_wake now empty. If the redrive recomputed
	// the wake set it would flip to terminal no_delivery and drop the pending
	// ollie target. The frozen route must instead drive the recorded target.
	shrunk := baseDirectoryFile()
	shrunk.Rooms[0].AmbientWake = nil
	if err := os.WriteFile(h.dirPath, marshalDirectory(t, shrunk), 0o600); err != nil {
		t.Fatalf("shrink directory: %v", err)
	}
	if err := h.dirStore.StageReload(h.dirPath); err != nil {
		t.Fatalf("reload shrunk directory: %v", err)
	}

	h.gw.deliverReceipt(origin)
	h.wait()
	got, _ := h.receipts.Get(origin)
	if got == nil || got.Status != ingressStatusDelivered {
		t.Fatalf("status = %v, want delivered (frozen route driven, not no_delivery)", statusOf(got))
	}
	if n := len(gc.sessionCalls()); n != 2 {
		t.Errorf("session POSTs = %d, want 2 (initial 502 + successful redrive)", n)
	}
}

// ---- F8/sweep: stale-reclaim boundary via the production sweep -------------

func TestSweepEligibleStaleReclaimBoundary(t *testing.T) {
	now := time.Now()
	window := companyStaleReclaimWindow
	if !sweepEligible(&IngressReceipt{Status: ingressStatusReceived}, now, window) {
		t.Error("received receipt not sweep-eligible")
	}
	if !sweepEligible(&IngressReceipt{Status: ingressStatusReceived, Reason: companyReasonParked}, now, window) {
		t.Error("parked receipt not sweep-eligible")
	}
	for _, s := range []string{ingressStatusDelivered, ingressStatusNoDelivery, ingressStatusFailed} {
		if sweepEligible(&IngressReceipt{Status: s}, now, window) {
			t.Errorf("terminal receipt %q is sweep-eligible", s)
		}
	}
	if !sweepEligible(&IngressReceipt{Status: ingressStatusRouting}, now, window) {
		t.Error("routing receipt with zero UpdatedAt not reclaimed")
	}
	if sweepEligible(&IngressReceipt{Status: ingressStatusRouting, UpdatedAt: now}, now, window) {
		t.Error("fresh routing claim reclaimed (would steal live work)")
	}
	if !sweepEligible(&IngressReceipt{Status: ingressStatusRouting, UpdatedAt: now.Add(-window)}, now, window) {
		t.Error("routing claim exactly window-old not reclaimed")
	}
	if sweepEligible(&IngressReceipt{Status: ingressStatusRouting, UpdatedAt: now.Add(-window + time.Second)}, now, window) {
		t.Error("sub-window routing claim reclaimed")
	}
	// A session-guard hold sits in routing with a fresh UpdatedAt but must be
	// eligible on the 60s sweep, NOT deferred behind the 5m stale window (m6 /
	// spec §Session-existence guard: "the 60s sweep re-checks").
	for _, detail := range []string{companyDetailSessionMissing, companyDetailSessionAmbiguous} {
		held := &IngressReceipt{
			Status:    ingressStatusRouting,
			UpdatedAt: now,
			Targets: map[string]TargetDelivery{
				"b-ollie": {Status: companyTargetPending, Detail: detail},
			},
		}
		if !sweepEligible(held, now, window) {
			t.Errorf("guard-held (%s) routing receipt not sweep-eligible within the stale window", detail)
		}
	}
	// A delivered/failed target carrying a stale guard detail is NOT a live hold
	// and must not force the fresh routing claim to be reclaimed early.
	notHeld := &IngressReceipt{
		Status:    ingressStatusRouting,
		UpdatedAt: now,
		Targets: map[string]TargetDelivery{
			"b-ollie": {Status: companyTargetDelivered, Detail: companyDetailSessionMissing},
			"b-riley": {Status: companyTargetPending, Detail: "current-turn pointer write: boom"},
		},
	}
	if sweepEligible(notHeld, now, window) {
		t.Error("non-guard pending target treated as guard-held (would steal live work)")
	}
	// An accepted async message whose stream disconnected is also not a live
	// mutating claim: redrive only reopens the correlated event stream and can
	// never POST again. Let the next 60s sweep recover it instead of waiting the
	// full stale-claim window.
	asyncPending := &IngressReceipt{
		Status:    ingressStatusRouting,
		UpdatedAt: now,
		Targets: map[string]TargetDelivery{
			"b-ollie": {Status: companyTargetPending, RequestID: "req-pending", EventCursor: "42"},
		},
	}
	if !sweepEligible(asyncPending, now, window) {
		t.Error("accepted async result wait not sweep-eligible within the stale window")
	}
}

func TestCompanySweepReclaimsOnlyStaleRoutingClaim(t *testing.T) {
	gc := newFakeGC(t)
	// The first delivery 5xx leaves a routing claim with a pending target
	// (a crashed worker mid-flight); redrive succeeds.
	gc.respStatus = func(reqNum int) int {
		if reqNum == 0 {
			return http.StatusServiceUnavailable // 503
		}
		return http.StatusOK
	}
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
	h.openBarrier()

	origin := baseOrigin()
	w, _ := h.admitViaHandler(t, humanMessage(origin.TS, "hi"), 0)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("admit status = %d, want 200", w.Result().StatusCode)
	}
	h.wait()
	if r, _ := h.receipts.Get(origin); r == nil || r.Status != ingressStatusRouting {
		t.Fatalf("want routing claim after 5xx, got %v", statusOf(r))
	}

	// A sweep at the current time must NOT reclaim a fresh routing claim.
	h.gw.now = func() time.Time { return time.Now() }
	h.gw.sweepOnce()
	h.wait()
	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("fresh routing claim reclaimed by sweep: calls=%d, want 1", got)
	}

	// Advance the injected clock past the stale window: the sweep reclaims it.
	h.gw.now = func() time.Time { return time.Now().Add(companyStaleReclaimWindow + time.Minute) }
	h.gw.sweepOnce()
	h.wait()
	if got := len(gc.sessionCalls()); got != 2 {
		t.Errorf("stale routing claim not reclaimed: calls=%d, want 2", got)
	}
	if got, _ := h.receipts.Get(origin); got == nil || got.Status != ingressStatusDelivered {
		t.Errorf("status = %v, want delivered after reclaim", statusOf(got))
	}
}

// ---- acceptance 8 via the PRODUCTION startRecovery path --------------------

func TestCompanyStartRecoveryDeliversPendingAndOpensBarrier(t *testing.T) {
	gc := newFakeGC(t)
	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)

	// Admit before the "restart".
	origin := baseOrigin()
	rawEv, _ := json.Marshal(humanMessage(origin.TS, "hello team"))
	if created, _, err := h.receipts.Admit(&IngressReceipt{Origin: origin, Event: rawEv, Status: ingressStatusReceived}); err != nil || !created {
		t.Fatalf("admit: created=%v err=%v", created, err)
	}

	// Restart, then drive the PRODUCTION startRecovery goroutine (barrier +
	// recovery scan + sweep) rather than a hand-rolled recoverPending.
	h.reopen(t, gc.server.URL, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.gw.startRecovery(ctx)
	waitUntil(t, func() bool { return h.gw.barrier.Load() })
	h.wait()

	if got := len(gc.sessionCalls()); got != 1 {
		t.Errorf("startRecovery deliveries = %d, want 1", got)
	}
	if r, _ := h.receipts.Get(origin); r == nil || r.Status != ingressStatusDelivered {
		t.Errorf("receipt status = %v, want delivered", statusOf(r))
	}
}

// ---- human file_share / thread_broadcast admitted and routed end to end ----

func TestCompanyHumanFileShareAndThreadBroadcastRouted(t *testing.T) {
	for _, subtype := range []string{"file_share", "thread_broadcast"} {
		t.Run(subtype, func(t *testing.T) {
			gc := newFakeGC(t)
			df := baseDirectoryFile()
			bf := baseBindingsFile()
			h := newCompanyHarness(t, gc.server.URL, &df, &bf, 4)
			h.openBarrier()

			ev := slackMessageEvent{Subtype: subtype, User: "Uhuman", Channel: testChannelID, TS: baseOrigin().TS, Text: "morning all"}
			w, handled := h.admitViaHandler(t, ev, 0)
			if !handled || w.Result().StatusCode != http.StatusOK {
				t.Fatalf("%s admit: handled=%v status=%d", subtype, handled, w.Result().StatusCode)
			}
			h.wait()
			calls := gc.sessionCalls()
			if len(calls) != 1 {
				t.Fatalf("%s deliveries = %d, want 1 (ambient ollie): %+v", subtype, len(calls), calls)
			}
			if !strings.HasSuffix(calls[0].path, "/session/ollie-main/messages") {
				t.Errorf("%s delivery path = %q, want ollie-main", subtype, calls[0].path)
			}
			if r, _ := h.receipts.Get(baseOrigin()); r == nil || r.Status != ingressStatusDelivered {
				t.Errorf("%s receipt status = %v, want delivered", subtype, statusOf(r))
			}
		})
	}
}

// ---- F9: bound and unbound target keys can never collide -------------------

func TestCompanyEnsureTargetsDisjointKeys(t *testing.T) {
	dir := testDirectory(t)
	room, ok := dir.RoomByChannel(testTeamID, testChannelID)
	if !ok {
		t.Fatal("room not resolved")
	}
	// ollie is bound to a session literally named after the OTHER agent,
	// "riley"; riley itself is left unbound. Pre-fix, an "unbound:riley" key
	// and a bound session "riley" could collide.
	binds, _, err := ParseCompanyBindings(marshalBindings(t, companyBindingsFile{
		SchemaVersion: 1,
		Bindings:      []CompanyBinding{{Room: "orchestrator-team", Agent: "ollie", Session: "riley"}},
	}), dir)
	if err != nil {
		t.Fatalf("ParseCompanyBindings: %v", err)
	}
	oa, _ := dir.AgentByName("ollie")
	ra, _ := dir.AgentByName("riley")
	wakes := []frozenWake{
		{Agent: *oa, Kind: wakeKindAmbient},  // bound -> session "riley"
		{Agent: *ra, Kind: wakeKindTargeted}, // unbound
	}
	g := &companyGateway{}
	r := &IngressReceipt{ID: "in-x"}
	g.ensureTargets(r, room, wakes, binds, time.Now())
	if len(r.Targets) != 2 {
		t.Fatalf("targets = %d, want 2 distinct records (bound session 'riley' + unbound agent 'riley'): %+v", len(r.Targets), r.Targets)
	}
	var bound, unbound int
	for _, td := range r.Targets {
		switch {
		case td.Session == "riley" && td.Status == companyTargetPending:
			bound++
		case td.Session == "" && td.Status == companyTargetFailed:
			unbound++
		}
	}
	if bound != 1 || unbound != 1 {
		t.Errorf("bound=%d unbound=%d, want 1/1: %+v", bound, unbound, r.Targets)
	}
	// Structural guarantee: the two key namespaces are disjoint by prefix.
	if strings.HasPrefix(companyBoundTargetKeyPrefix, companyUnboundTargetKeyPrefix) ||
		strings.HasPrefix(companyUnboundTargetKeyPrefix, companyBoundTargetKeyPrefix) {
		t.Error("bound/unbound key prefixes are not disjoint")
	}
}

// ---- helpers ---------------------------------------------------------------

func statusOf(r *IngressReceipt) string {
	if r == nil {
		return "<nil>"
	}
	return r.Status
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 2s")
}
