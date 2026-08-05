package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// company_selfheal_test.go — P0 delivery self-healing coverage: stale-frozen
// target auto-re-resolution (the petra outage, end-to-end), cold-pool
// auto-materialization, unbound targets staying failed, redrive re-resolution of
// a stale bound target, failure-notice suppression while recoverable, and the
// one-materialization-per-sweep-interval throttle.

// selfHealClock is a mutable, race-safe clock for driving the materialization
// throttle across sweep intervals.
type selfHealClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *selfHealClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *selfHealClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// reloadRoomBindings rewrites and stage-reloads the room bindings, modeling an
// operator `gc slack bind-company-agent` that re-points a binding mid-flight.
func reloadRoomBindings(t *testing.T, h *companyHarness, bf companyBindingsFile) {
	t.Helper()
	if err := os.WriteFile(h.bindPath, marshalBindings(t, bf), 0o600); err != nil {
		t.Fatalf("write bindings: %v", err)
	}
	if err := h.gw.bindStore.StageReload(h.bindPath, h.dirStore.Snapshot()); err != nil {
		t.Fatalf("reload bindings: %v", err)
	}
}

func roomBinding(session string) companyBindingsFile {
	return companyBindingsFile{
		SchemaVersion: 1,
		Bindings:      []CompanyBinding{{Room: "orchestrator-team", Agent: "ollie", Session: session}},
	}
}

func roomOrigin(ts string) ReceiptOrigin {
	return ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: ts}
}

// isGuardGET reports a session-existence guard probe (GET /session/{s}, not the
// /messages POST).
func isGuardGET(r *http.Request) bool {
	return r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/session/") && !strings.HasSuffix(r.URL.Path, "/messages")
}

// ---- item: stale-frozen-target re-resolution (petra, end-to-end) -----------

// TestCompanyStaleFrozenTargetReResolvesAndDelivers reproduces the petra outage:
// a receipt frozen to a now-dead session name, the binding since fixed to a live
// session. On the session-not-found retry the adapter re-resolves the frozen
// target to the current binding and delivers — no operator receipt edit.
func TestCompanyStaleFrozenTargetReResolvesAndDelivers(t *testing.T) {
	var mu sync.Mutex
	var deliverPaths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case isGuardGET(r):
			if strings.Contains(p, "ollie-live") {
				w.WriteHeader(http.StatusOK) // the live rebind exists
			} else {
				w.WriteHeader(http.StatusNotFound) // teams__pm is dead
			}
		case r.Method == http.MethodPost && strings.HasSuffix(p, "/sessions"):
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPost && strings.HasSuffix(p, "/messages"):
			mu.Lock()
			deliverPaths = append(deliverPaths, p)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	df := baseDirectoryFile()
	bf := roomBinding("teams__pm")
	h := newCompanyHarness(t, srv.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.verifySessions = true
	h.openBarrier()

	ts := "1700000000.010001"
	origin := roomOrigin(ts)
	if _, handled := h.admitViaHandler(t, humanMessage(ts, "morning"), 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()
	// Pass 1: target frozen to teams__pm, guard 404, binding still names it → held.
	r, _ := h.gw.store().Get(origin)
	if r.Status != ingressStatusRouting {
		t.Fatalf("pass 1 status = %q, want routing (held, not failed)", r.Status)
	}

	// Operator re-points the binding to a live session (no receipt edit).
	reloadRoomBindings(t, h, roomBinding("ollie-live"))

	// Pass 2: guard 404 on the stale teams__pm → re-resolve to the live binding.
	h.gw.triggerDelivery(origin)
	h.wait()
	r, _ = h.gw.store().Get(origin)
	if got := h.gw.targetReresolved.Load(); got != 1 {
		t.Fatalf("company_target_reresolved = %d, want 1", got)
	}
	foundLive := false
	for _, td := range r.Targets {
		if td.Session == "teams__pm" {
			t.Fatalf("stale teams__pm target survived re-resolution: %+v", r.Targets)
		}
		if td.Session == "ollie-live" {
			foundLive = true
		}
	}
	if !foundLive {
		t.Fatalf("no ollie-live target after re-resolution: %+v", r.Targets)
	}
	if r.Status != ingressStatusRouting {
		t.Fatalf("pass 2 status = %q, want routing (re-resolved, still probing)", r.Status)
	}

	// Pass 3: guard 200 on the live session → deliver.
	h.gw.triggerDelivery(origin)
	h.wait()
	r, _ = h.gw.store().Get(origin)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("final status = %q, want delivered", r.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(deliverPaths) != 1 || !strings.Contains(deliverPaths[0], "/session/ollie-live/messages") {
		t.Fatalf("delivery paths = %v, want a single ollie-live delivery", deliverPaths)
	}
}

// ---- item: cold-pool materialization (404 → materialize 202 → deliver) ------

// TestCompanyColdPoolMaterializesThenDelivers proves a bound target whose pool
// session is cold is materialized (one POST /sessions) and delivered on the next
// sweep — the operator never has to warm the session by hand.
func TestCompanyColdPoolMaterializesThenDelivers(t *testing.T) {
	h, _, _ := setupDM(t)
	h.gw.verifySessions = true
	h.gw.dmBindStore = loadDMBindingsStore(t, h, dmBindingsFile{
		SchemaVersion: 1,
		DMBindings:    []DMBinding{{Agent: "ollie", Session: "teams.pm"}},
	})

	var mu sync.Mutex
	var live bool
	var materializeReqs []companySessionCreateRequest
	var materializeIdem []string
	var deliverPaths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case isGuardGET(r):
			mu.Lock()
			l := live
			mu.Unlock()
			if l {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case r.Method == http.MethodPost && strings.HasSuffix(p, "/sessions"):
			raw, _ := io.ReadAll(r.Body)
			var req companySessionCreateRequest
			_ = json.Unmarshal(raw, &req)
			mu.Lock()
			materializeReqs = append(materializeReqs, req)
			materializeIdem = append(materializeIdem, r.Header.Get("Idempotency-Key"))
			live = true // the materialize call warms the session
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPost && strings.HasSuffix(p, "/messages"):
			mu.Lock()
			deliverPaths = append(deliverPaths, p)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.010100"
	if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()
	// Pass 1: guard 404 on the cold pool session → one materialize POST; held.
	r := getReceipt(t, h, ts)
	if r.Status != ingressStatusRouting {
		t.Fatalf("pass 1 status = %q, want routing (materializing)", r.Status)
	}
	if got := h.gw.materializeRequests.Load(); got != 1 {
		t.Fatalf("company_materialize_requests = %d, want 1", got)
	}

	// Pass 2: the session is warm → deliver.
	h.gw.triggerDelivery(dmOrigin(ts))
	h.wait()
	r = getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("pass 2 status = %q, want delivered", r.Status)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(materializeReqs) != 1 {
		t.Fatalf("materialize POSTs = %d, want exactly 1", len(materializeReqs))
	}
	if materializeReqs[0].Name != "teams.pm" || materializeReqs[0].Kind != "agent" {
		t.Fatalf("materialize body = %+v, want {name=teams.pm kind=agent}", materializeReqs[0])
	}
	if want := "materialize:" + r.ID + ":teams.pm"; materializeIdem[0] != want {
		t.Fatalf("materialize Idempotency-Key = %q, want %q", materializeIdem[0], want)
	}
	if len(deliverPaths) != 1 || !strings.Contains(deliverPaths[0], "/session/teams.pm/messages") {
		t.Fatalf("delivery paths = %v, want a single teams.pm delivery", deliverPaths)
	}
}

// TestCompanyColdNamedSessionWakesThenDelivers proves a cold configured named
// session uses the supervisor's safe wake/materialization endpoint. Unlike a
// POST /sessions template create, POST /session/{name}/wake can materialize the
// canonical named-session identity without minting an unrelated adhoc worker.
func TestCompanyColdNamedSessionWakesThenDelivers(t *testing.T) {
	h, _, _ := setupDM(t)
	h.gw.verifySessions = true

	var mu sync.Mutex
	var live bool
	var wakePaths, wakeIdem, deliverPaths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case isGuardGET(r):
			mu.Lock()
			l := live
			mu.Unlock()
			if l {
				writeSessionState(w, "active")
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case isWakePost(r):
			mu.Lock()
			wakePaths = append(wakePaths, p)
			wakeIdem = append(wakeIdem, r.Header.Get("Idempotency-Key"))
			live = true
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && strings.HasSuffix(p, "/messages"):
			mu.Lock()
			deliverPaths = append(deliverPaths, p)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.010150"
	if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()
	r := getReceipt(t, h, ts)
	if r.Status != ingressStatusRouting {
		t.Fatalf("pass 1 status = %q, want routing (waking named session)", r.Status)
	}

	h.gw.triggerDelivery(dmOrigin(ts))
	h.wait()
	r = getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("pass 2 status = %q, want delivered", r.Status)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(wakePaths) != 1 || !strings.HasSuffix(wakePaths[0], "/session/ollie/wake") {
		t.Fatalf("wake paths = %v, want one canonical named-session wake", wakePaths)
	}
	if want := "materialize:" + r.ID + ":ollie"; len(wakeIdem) != 1 || wakeIdem[0] != want {
		t.Fatalf("wake idempotency keys = %v, want [%s]", wakeIdem, want)
	}
	if len(deliverPaths) != 1 || !strings.Contains(deliverPaths[0], "/session/ollie/messages") {
		t.Fatalf("delivery paths = %v, want a single ollie delivery", deliverPaths)
	}
	if got := h.gw.materializeRequests.Load(); got != 1 {
		t.Fatalf("company_materialize_requests = %d, want 1", got)
	}
}

// TestCompanyExactSessionMissingUsesSafeWakeWithoutTemplateOrphan reproduces
// the Tessa outage: an exact session_name binding has vanished. Self-heal may
// probe the safe named-session wake endpoint (which returns 404 here), but it
// must never POST the former template to /sessions and create an orphan.
func TestCompanyExactSessionMissingUsesSafeWakeWithoutTemplateOrphan(t *testing.T) {
	h, _, _ := setupDM(t)

	var mu sync.Mutex
	wakeCount, sessionCreateCount := 0, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isWakePost(r):
			mu.Lock()
			wakeCount++
			mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/sessions"):
			mu.Lock()
			sessionCreateCount++
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	r := &IngressReceipt{ID: "in-exact-session"}
	td := TargetDelivery{Session: "s-tr-wisp-q9j91"}
	if !h.gw.tryMaterialize(r, td) {
		t.Fatal("tryMaterialize did not probe the safe named-session wake endpoint")
	}

	mu.Lock()
	defer mu.Unlock()
	if wakeCount != 1 {
		t.Errorf("named-session wake POSTs = %d, want 1", wakeCount)
	}
	if sessionCreateCount != 0 {
		t.Errorf("session-create POSTs = %d, want 0 (would create an orphan)", sessionCreateCount)
	}
	if got := h.gw.materializeRequests.Load(); got != 1 {
		t.Errorf("company_materialize_requests = %d, want 1", got)
	}
}

// ---- item: unbound target stays failed (no self-heal) ----------------------

// TestCompanyUnboundTargetStaysFailedNoSelfHeal proves an unbound receiver is a
// definitive failed_dm_unbound target that the self-heal path never re-resolves
// or materializes — unbound semantics are preserved.
func TestCompanyUnboundTargetStaysFailedNoSelfHeal(t *testing.T) {
	h, gc, _ := setupDM(t)
	h.gw.verifySessions = true // riley has NO dm binding

	ts := "1700000000.010200"
	ev := dmMessage("U0HUMAN01", ts, "riley?")
	ev.Channel = "D0RILEYDM"
	if _, handled := admitDMViaHandler(t, h, ev, rileyAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: "D0RILEYDM", TS: ts}
	r, _ := h.gw.store().Get(origin)
	if r.Status != ingressStatusFailed {
		t.Fatalf("status = %q, want failed", r.Status)
	}
	var detail string
	for _, td := range r.Targets {
		if td.Agent == "riley" {
			detail = td.Detail
		}
	}
	if detail != companyReasonFailedDMUnbound {
		t.Fatalf("unbound target detail = %q, want failed_dm_unbound", detail)
	}
	if rr, mr := h.gw.targetReresolved.Load(), h.gw.materializeRequests.Load(); rr != 0 || mr != 0 {
		t.Fatalf("self-heal fired for an unbound target: reresolved=%d materialize=%d", rr, mr)
	}
	if n := len(gc.sessionCalls()); n != 0 {
		t.Fatalf("unbound target woke %d sessions, want 0", n)
	}
}

// ---- item: redrive re-resolves a stale bound target ------------------------

// TestCompanyRedriveReResolvesStaleBoundTarget proves applyRedrive re-resolves a
// FAILED bound target's session from the current bindings (not just unbound
// ones), so an operator redrive after a rebind drives the live session.
func TestCompanyRedriveReResolvesStaleBoundTarget(t *testing.T) {
	var mu sync.Mutex
	var deliverPaths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if r.Method == http.MethodPost && strings.HasSuffix(p, "/messages") {
			mu.Lock()
			deliverPaths = append(deliverPaths, p)
			mu.Unlock()
			if strings.Contains(p, "/session/deadsession/messages") {
				w.WriteHeader(http.StatusBadRequest) // definitive failure, not a 404
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	df := baseDirectoryFile()
	bf := roomBinding("deadsession")
	h := newCompanyHarness(t, srv.URL, &df, &bf, 4) // guard off (verifySessions default false)
	setFixedClock(h)
	h.openBarrier()

	ts := "1700000000.010300"
	origin := roomOrigin(ts)
	if _, handled := h.admitViaHandler(t, humanMessage(ts, "hi"), 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()
	r, _ := h.gw.store().Get(origin)
	if r.Status != ingressStatusFailed {
		t.Fatalf("status = %q, want failed", r.Status)
	}

	// Operator rebinds ollie to a live session, then redrives.
	reloadRoomBindings(t, h, roomBinding("ollie-live"))
	if _, cerr := h.gw.applyRedrive(r, companyRedriveRequest{Origin: &origin}); cerr != nil {
		t.Fatalf("applyRedrive: %v", cerr)
	}
	h.gw.triggerDelivery(origin)
	h.wait()

	r, _ = h.gw.store().Get(origin)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("post-redrive status = %q, want delivered", r.Status)
	}
	for _, td := range r.Targets {
		if td.Session == "deadsession" {
			t.Fatalf("stale deadsession target survived redrive: %+v", r.Targets)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(deliverPaths) < 2 {
		t.Fatalf("delivery paths = %v, want dead attempt + live redrive", deliverPaths)
	}
	last := deliverPaths[len(deliverPaths)-1]
	if !strings.Contains(last, "/session/ollie-live/messages") {
		t.Fatalf("last delivery = %q, want ollie-live", last)
	}
}

// ---- item: notice suppressed while pending, fires on genuine exhaustion -----

// TestCompanyNoticeSuppressedWhilePendingThenFiresOnExhaustion proves a
// session_missing/materializing pending target does not trigger the user-visible
// failure reply or ⚠️ while the attempts budget remains, but the notice DOES fire
// once the budget is genuinely exhausted.
func TestCompanyNoticeSuppressedWhilePendingThenFiresOnExhaustion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isGuardGET(r) {
			w.WriteHeader(http.StatusNotFound) // never materializes
			return
		}
		w.WriteHeader(http.StatusOK) // materialize POST 200, but the session stays cold
	}))
	defer srv.Close()

	df := baseDirectoryFile()
	bf := roomBinding("teams__pm")
	h := newCompanyHarness(t, srv.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.verifySessions = true
	spy := &ackSpy{}
	wireAckSpy(h, spy)
	h.openBarrier()

	ts := "1700000000.010400"
	origin := roomOrigin(ts)
	if _, handled := h.admitViaHandler(t, humanMessage(ts, "hi"), 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()
	r, _ := h.gw.store().Get(origin)
	if r.Status != ingressStatusRouting {
		t.Fatalf("status = %q, want routing (held pending)", r.Status)
	}
	calls, replies := spy.snapshot()
	if len(replies) != 0 {
		t.Fatalf("failure reply fired while recoverable-pending: %+v", replies)
	}
	for _, c := range calls {
		if c.name == ackEmojiWarning {
			t.Fatalf("⚠️ fired while recoverable-pending: %+v", calls)
		}
	}

	// Drive to genuine exhaustion.
	for i := 0; i < 20 && r.Status != ingressStatusFailed; i++ {
		h.gw.triggerDelivery(origin)
		h.wait()
		r, _ = h.gw.store().Get(origin)
	}
	if r.Status != ingressStatusFailed {
		t.Fatalf("did not reach exhaustion: status = %q", r.Status)
	}
	calls, replies = spy.snapshot()
	if len(replies) != 1 {
		t.Fatalf("failure reply count = %d, want exactly 1 on exhaustion", len(replies))
	}
	warned := 0
	for _, c := range calls {
		if c.name == ackEmojiWarning {
			warned++
		}
	}
	if warned == 0 {
		t.Fatalf("no ⚠️ on genuine exhaustion: %+v", calls)
	}
}

// ---- item: one materialization per (city, session) per sweep interval -------

// TestCompanyMaterializeThrottledPerSweepInterval proves two delivery passes
// within one sweep interval fire at most one materialize POST, and a pass after
// the interval fires another.
func TestCompanyMaterializeThrottledPerSweepInterval(t *testing.T) {
	h, _, _ := setupDM(t)
	h.gw.verifySessions = true
	h.gw.dmBindStore = loadDMBindingsStore(t, h, dmBindingsFile{
		SchemaVersion: 1,
		DMBindings:    []DMBinding{{Agent: "ollie", Session: "teams.pm"}},
	})
	t0, _ := time.Parse(time.RFC3339, fixedNow)
	clk := &selfHealClock{t: t0}
	h.gw.now = clk.now

	var mu sync.Mutex
	materializeCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isGuardGET(r):
			w.WriteHeader(http.StatusNotFound) // never materializes; keeps re-driving
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/sessions"):
			mu.Lock()
			materializeCount++
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.010500"
	if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()                           // pass 1 at t0: fires
	h.gw.triggerDelivery(dmOrigin(ts)) // pass 2 at t0: throttled
	h.wait()
	mu.Lock()
	c1 := materializeCount
	mu.Unlock()
	if c1 != 1 {
		t.Fatalf("materialize POSTs after two same-interval passes = %d, want 1 (throttled)", c1)
	}
	if got := h.gw.materializeRequests.Load(); got != 1 {
		t.Fatalf("company_materialize_requests = %d, want 1", got)
	}

	// Advance past the sweep interval → the throttle releases.
	clk.advance(h.gw.sweepInterval + time.Second)
	h.gw.triggerDelivery(dmOrigin(ts)) // pass 3: fires again
	h.wait()
	mu.Lock()
	c2 := materializeCount
	mu.Unlock()
	if c2 != 2 {
		t.Fatalf("materialize POSTs after interval advance = %d, want 2", c2)
	}
	if got := h.gw.materializeRequests.Load(); got != 2 {
		t.Fatalf("company_materialize_requests = %d, want 2", got)
	}
}
