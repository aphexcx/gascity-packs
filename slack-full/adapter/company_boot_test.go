package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// company_boot_test.go — delivered-asleep boot escalation (Feature 2): when the
// post-delivery re-check still shows the session asleep (the wake POST cleared
// the drain flag without starting a runtime), the adapter fires the
// materialization lever — POST /v0/city/{city}/sessions — to provably boot a
// runtime, throttled per (city,session), counted as company_boot_requests, with
// an Idempotency-Key of boot:<receipt>:<session>. An exact session_name binding
// is skipped (materializing would orphan the asleep instance); a boot POST
// failure leaves the target delivered (advisory).

// isSessionsPost reports the session-create materialization POST.
func isSessionsPost(r *http.Request) bool {
	return r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/sessions")
}

// bindOllieDM re-points ollie's DM binding to session for a boot test.
func bindOllieDM(t *testing.T, h *companyHarness, session string) {
	t.Helper()
	h.gw.dmBindStore = loadDMBindingsStore(t, h, dmBindingsFile{
		SchemaVersion: 1,
		DMBindings:    []DMBinding{{Agent: "ollie", Session: session}},
	})
}

// TestBootEscalationTemplateBindingFires — a still-asleep session bound to a
// template-shaped name fires exactly one boot POST (name = the alias, kind
// agent, Idempotency-Key boot:<receipt>:<session>, counter incremented).
func TestBootEscalationTemplateBindingFires(t *testing.T) {
	h, _, _ := setupDM(t)
	h.gw.verifySessions = true
	bindOllieDM(t, h, "teams.pm") // template-shaped alias

	var mu sync.Mutex
	var bootBodies []companySessionCreateRequest
	var bootIdem []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isGuardGET(r):
			writeSessionState(w, "drained") // never clears — wake does not start a runtime
		case isWakePost(r):
			w.WriteHeader(http.StatusOK)
		case isSessionsPost(r):
			raw, _ := io.ReadAll(r.Body)
			var req companySessionCreateRequest
			_ = json.Unmarshal(raw, &req)
			mu.Lock()
			bootBodies = append(bootBodies, req)
			bootIdem = append(bootIdem, r.Header.Get("Idempotency-Key"))
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		case isDeliverPost(r):
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.020010"
	if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()
	r := getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered", r.Status)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bootBodies) != 1 {
		t.Fatalf("boot POSTs = %d, want exactly 1", len(bootBodies))
	}
	if bootBodies[0].Name != "teams.pm" || bootBodies[0].Kind != "agent" {
		t.Errorf("boot body = %+v, want {name=teams.pm kind=agent}", bootBodies[0])
	}
	if want := "boot:" + r.ID + ":teams.pm"; bootIdem[0] != want {
		t.Errorf("boot Idempotency-Key = %q, want %q", bootIdem[0], want)
	}
	if got := h.gw.bootRequests.Load(); got != 1 {
		t.Errorf("company_boot_requests = %d, want 1", got)
	}
	if got := h.gw.deliveredAsleep.Load(); got != 1 {
		t.Errorf("company_delivered_asleep = %d, want 1", got)
	}
}

// TestBootThrottlePerSweepInterval drives tryBoot directly (bypassing the
// positive session-existence cache that would otherwise stop a second receipt
// from re-probing) to prove the (city,session) throttle: two calls in one sweep
// interval fire one POST; a call after the interval fires another.
func TestBootThrottlePerSweepInterval(t *testing.T) {
	h, _, _ := setupDM(t)
	t0, _ := time.Parse(time.RFC3339, fixedNow)
	clk := &selfHealClock{t: t0}
	h.gw.now = clk.now

	var mu sync.Mutex
	bootCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isSessionsPost(r) {
			mu.Lock()
			bootCount++
			mu.Unlock()
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	r := &IngressReceipt{ID: "in-boot"}
	td := TargetDelivery{Session: "teams.pm"}

	if !h.gw.tryBoot(r, td) {
		t.Fatal("first tryBoot should fire")
	}
	if h.gw.tryBoot(r, td) {
		t.Fatal("second tryBoot in the same interval should be throttled")
	}
	mu.Lock()
	c1 := bootCount
	mu.Unlock()
	if c1 != 1 {
		t.Fatalf("boot POSTs after two same-interval calls = %d, want 1", c1)
	}

	clk.advance(h.gw.sweepInterval + time.Second)
	if !h.gw.tryBoot(r, td) {
		t.Fatal("tryBoot after the interval should fire again")
	}
	mu.Lock()
	c2 := bootCount
	mu.Unlock()
	if c2 != 2 {
		t.Fatalf("boot POSTs after interval advance = %d, want 2", c2)
	}
	if got := h.gw.bootRequests.Load(); got != 2 {
		t.Errorf("company_boot_requests = %d, want 2", got)
	}
}

// TestBootThrottleSharedWithMaterialize proves the boot escalation and the 404
// self-heal share ONE per-(city,session) throttle gate: a materialize this
// interval suppresses a boot for the same session (and vice versa), so a
// session is never double-POSTed within a sweep.
func TestBootThrottleSharedWithMaterialize(t *testing.T) {
	h, _, _ := setupDM(t)
	t0, _ := time.Parse(time.RFC3339, fixedNow)
	clk := &selfHealClock{t: t0}
	h.gw.now = clk.now

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	r := &IngressReceipt{ID: "in-shared"}
	td := TargetDelivery{Session: "teams.pm"}
	if !h.gw.tryMaterialize(r, td) {
		t.Fatal("materialize should fire first")
	}
	if h.gw.tryBoot(r, td) {
		t.Fatal("boot should be throttled by the same-interval materialize (shared gate)")
	}
}

// TestBootEscalationExactNameBindingUsesNamedWake proves a still-asleep exact
// named-session target receives a second, boot-keyed wake request instead of a
// template create. The first wake is the ordinary pre-delivery wake; the second
// is the post-delivery escalation after the session still reports drained.
func TestBootEscalationExactNameBindingUsesNamedWake(t *testing.T) {
	h, _, _ := setupDM(t)
	h.gw.verifySessions = true
	// setupDM's default binding is ollie → "ollie" (exact session_name).

	var mu sync.Mutex
	var wakeIdem []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isGuardGET(r):
			writeSessionState(w, "drained")
		case isWakePost(r):
			mu.Lock()
			wakeIdem = append(wakeIdem, r.Header.Get("Idempotency-Key"))
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.020210"
	if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered", r.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(wakeIdem) != 2 {
		t.Fatalf("wake POSTs = %d, want 2 (pre-delivery wake + boot escalation): %v", len(wakeIdem), wakeIdem)
	}
	if want := "wake:" + r.ID + ":ollie"; wakeIdem[0] != want {
		t.Errorf("pre-delivery wake idempotency key = %q, want %q", wakeIdem[0], want)
	}
	if want := "boot:" + r.ID + ":ollie"; wakeIdem[1] != want {
		t.Errorf("boot wake idempotency key = %q, want %q", wakeIdem[1], want)
	}
	if got := h.gw.bootRequests.Load(); got != 1 {
		t.Errorf("company_boot_requests = %d, want 1", got)
	}
	if got := h.gw.deliveredAsleep.Load(); got != 1 {
		t.Errorf("company_delivered_asleep = %d, want 1 (still-asleep recorded)", got)
	}
}

// TestBootEscalationPOSTFailureLeavesDelivered — a boot POST that fails (5xx) is
// advisory: the target stays delivered and the counter still records the
// attempt.
func TestBootEscalationPOSTFailureLeavesDelivered(t *testing.T) {
	h, _, _ := setupDM(t)
	h.gw.verifySessions = true
	bindOllieDM(t, h, "teams.pm")

	var mu sync.Mutex
	bootCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isGuardGET(r):
			writeSessionState(w, "drained")
		case isSessionsPost(r):
			mu.Lock()
			bootCount++
			mu.Unlock()
			w.WriteHeader(http.StatusInternalServerError) // boot POST fails
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.020310"
	if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered (boot failure is advisory)", r.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if bootCount != 1 {
		t.Errorf("boot POSTs = %d, want 1 (attempted despite failure)", bootCount)
	}
	if got := h.gw.bootRequests.Load(); got != 1 {
		t.Errorf("company_boot_requests = %d, want 1", got)
	}
}

// TestBootEscalationRoomPathFires proves the shared boot wiring on the ROOM
// delivery loop: a still-asleep template-bound room session fires one boot POST.
func TestBootEscalationRoomPathFires(t *testing.T) {
	var mu sync.Mutex
	var bootBodies []companySessionCreateRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isGuardGET(r):
			writeSessionState(w, "drained") // never clears
		case isWakePost(r):
			w.WriteHeader(http.StatusOK)
		case isSessionsPost(r):
			raw, _ := io.ReadAll(r.Body)
			var req companySessionCreateRequest
			_ = json.Unmarshal(raw, &req)
			mu.Lock()
			bootBodies = append(bootBodies, req)
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	df := baseDirectoryFile()
	bf := roomBinding("teams__pm") // template-shaped alias (dunder)
	h := newCompanyHarness(t, srv.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.verifySessions = true
	h.openBarrier()

	ts := "1700000000.020410"
	origin := roomOrigin(ts)
	if _, handled := h.admitViaHandler(t, humanMessage(ts, "morning"), 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r, _ := h.gw.store().Get(origin)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered", r.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bootBodies) != 1 || bootBodies[0].Name != "teams__pm" {
		t.Fatalf("boot bodies = %+v, want a single teams__pm boot", bootBodies)
	}
	if got := h.gw.bootRequests.Load(); got != 1 {
		t.Errorf("company_boot_requests = %d, want 1", got)
	}
}
