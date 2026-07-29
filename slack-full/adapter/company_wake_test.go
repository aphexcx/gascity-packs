package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// company_wake_test.go — pre-delivery wake coverage: an asleep/drained configured
// session must be woken (POST /session/{id}/wake) before the message POST so the
// delivered message is actually processed rather than silently queued. The wake
// is advisory (a failed wake still delivers); a session that is STILL asleep on
// the post-delivery re-check is left delivered but counted (company_delivered_asleep).
// Guard-off must issue no GET and no wake at all (byte-stable prior behavior).

// writeSessionState replies 200 with a supervisor session record carrying state.
func writeSessionState(w http.ResponseWriter, state string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"state":"` + state + `"}`))
}

// isWakePost reports the wake mutation (POST /session/{id}/wake).
func isWakePost(r *http.Request) bool {
	return r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/wake")
}

// isDeliverPost reports the message delivery (POST /session/{id}/messages).
func isDeliverPost(r *http.Request) bool {
	return r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/messages")
}

// TestDMDeliverAsleepSessionWakesThenDelivers: the guard GET shows the target
// asleep → a wake POST fires (with the tag + wake:<receipt>:<session> key) BEFORE
// the message POST; the wake clears the drain so the post-delivery re-check shows
// awake and the delivered_asleep counter stays zero.
func TestDMDeliverAsleepSessionWakesThenDelivers(t *testing.T) {
	h, _, _ := setupDM(t)
	h.gw.verifySessions = true

	var mu sync.Mutex
	var getCount, wakeCount, deliverCount int
	var woken bool
	var wakeIdem, wakeTag string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isGuardGET(r):
			mu.Lock()
			getCount++
			awake := woken
			mu.Unlock()
			if awake {
				writeSessionState(w, "active")
			} else {
				writeSessionState(w, "asleep")
			}
		case isWakePost(r):
			mu.Lock()
			wakeCount++
			woken = true
			wakeIdem = r.Header.Get("Idempotency-Key")
			wakeTag = r.Header.Get("X-GC-Request")
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case isDeliverPost(r):
			mu.Lock()
			deliverCount++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.010010"
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
	if wakeCount != 1 {
		t.Errorf("wake POSTs = %d, want 1", wakeCount)
	}
	if deliverCount != 1 {
		t.Errorf("delivery POSTs = %d, want 1", deliverCount)
	}
	if getCount != 2 {
		t.Errorf("session GETs = %d, want 2 (guard + post-delivery re-check)", getCount)
	}
	if got := h.gw.deliveredAsleep.Load(); got != 0 {
		t.Errorf("company_delivered_asleep = %d, want 0 (re-check showed awake)", got)
	}
	if wakeTag != companyDeliverRequestTag {
		t.Errorf("wake X-GC-Request = %q, want %q", wakeTag, companyDeliverRequestTag)
	}
	if !strings.HasPrefix(wakeIdem, "wake:") || !strings.HasSuffix(wakeIdem, ":ollie") {
		t.Errorf("wake Idempotency-Key = %q, want wake:<receipt>:ollie", wakeIdem)
	}
}

// TestDMDeliverWakeFailureStillDelivers: named-session wake POSTs that fail
// (non-2xx) are advisory — the message POST proceeds and the target still
// delivers. A still-asleep re-check triggers the separate boot escalation.
func TestDMDeliverWakeFailureStillDelivers(t *testing.T) {
	h, _, _ := setupDM(t)
	h.gw.verifySessions = true

	var mu sync.Mutex
	var wakeIdem []string
	var deliverCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isGuardGET(r):
			writeSessionState(w, "asleep")
		case isWakePost(r):
			mu.Lock()
			wakeIdem = append(wakeIdem, r.Header.Get("Idempotency-Key"))
			mu.Unlock()
			w.WriteHeader(http.StatusInternalServerError) // wake fails
		case isDeliverPost(r):
			mu.Lock()
			deliverCount++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.010020"
	if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered (wake failure must not block delivery)", r.Status)
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
	if deliverCount != 1 {
		t.Errorf("delivery POSTs = %d, want 1", deliverCount)
	}
}

// TestDMDeliverActiveSessionNoWake: an active session trips no wake and no
// post-delivery re-check — only the single guard GET plus the message POST.
func TestDMDeliverActiveSessionNoWake(t *testing.T) {
	h, _, _ := setupDM(t)
	h.gw.verifySessions = true

	var mu sync.Mutex
	var getCount, wakeCount, deliverCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isGuardGET(r):
			mu.Lock()
			getCount++
			mu.Unlock()
			writeSessionState(w, "active")
		case isWakePost(r):
			mu.Lock()
			wakeCount++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case isDeliverPost(r):
			mu.Lock()
			deliverCount++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.010030"
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
	if wakeCount != 0 {
		t.Errorf("wake POSTs = %d, want 0 (active session)", wakeCount)
	}
	if getCount != 1 {
		t.Errorf("session GETs = %d, want 1 (guard only, no re-check)", getCount)
	}
	if deliverCount != 1 {
		t.Errorf("delivery POSTs = %d, want 1", deliverCount)
	}
	if got := h.gw.deliveredAsleep.Load(); got != 0 {
		t.Errorf("company_delivered_asleep = %d, want 0", got)
	}
}

// TestDMDeliverStillAsleepIncrementsCounter: the wake POST HTTP-succeeds but the
// drain persists (the post-delivery re-check still shows the session asleep). The
// target stays delivered because this legacy synchronous stub confirmed the
// delivery; company_delivered_asleep separately records the stale sleep state.
func TestDMDeliverStillAsleepIncrementsCounter(t *testing.T) {
	h, _, _ := setupDM(t)
	h.gw.verifySessions = true

	var mu sync.Mutex
	var getCount, deliverCount int
	var wakeIdem []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isGuardGET(r):
			mu.Lock()
			getCount++
			mu.Unlock()
			writeSessionState(w, "drained") // never clears — wake did not take
		case isWakePost(r):
			mu.Lock()
			wakeIdem = append(wakeIdem, r.Header.Get("Idempotency-Key"))
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case isDeliverPost(r):
			mu.Lock()
			deliverCount++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.010040"
	if _, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hi"), ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered (still-asleep never fails the target)", r.Status)
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
	if deliverCount != 1 {
		t.Errorf("delivery POSTs = %d, want 1", deliverCount)
	}
	if getCount != 2 {
		t.Errorf("session GETs = %d, want 2 (guard + post-delivery re-check)", getCount)
	}
	if got := h.gw.deliveredAsleep.Load(); got != 1 {
		t.Errorf("company_delivered_asleep = %d, want 1", got)
	}
}

// TestDMDeliverGuardOffNoWakeNoGET: with the guard flag unset the delivery path
// must issue neither a session GET nor a wake — only the message POST — so prior
// (flag-off) behavior is byte-stable.
func TestDMDeliverGuardOffNoWakeNoGET(t *testing.T) {
	h, _, _ := setupDM(t)
	// verifySessions stays false (default).

	var mu sync.Mutex
	var getCount, wakeCount, deliverCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isGuardGET(r):
			mu.Lock()
			getCount++
			mu.Unlock()
			writeSessionState(w, "asleep")
		case isWakePost(r):
			mu.Lock()
			wakeCount++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case isDeliverPost(r):
			mu.Lock()
			deliverCount++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.010050"
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
	if getCount != 0 {
		t.Errorf("session GETs = %d, want 0 (guard off)", getCount)
	}
	if wakeCount != 0 {
		t.Errorf("wake POSTs = %d, want 0 (guard off)", wakeCount)
	}
	if deliverCount != 1 {
		t.Errorf("delivery POSTs = %d, want 1 (only the message POST)", deliverCount)
	}
}

// TestCompanyRoomDeliverAsleepSessionWakes proves the shared wake wiring on the
// ROOM delivery loop: an asleep room-bound session is woken before delivery.
func TestCompanyRoomDeliverAsleepSessionWakes(t *testing.T) {
	var mu sync.Mutex
	var wakeCount, deliverCount int
	var woken bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isGuardGET(r):
			mu.Lock()
			awake := woken
			mu.Unlock()
			if awake {
				writeSessionState(w, "active")
			} else {
				writeSessionState(w, "asleep")
			}
		case isWakePost(r):
			mu.Lock()
			wakeCount++
			woken = true
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case isDeliverPost(r):
			mu.Lock()
			deliverCount++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	df := baseDirectoryFile()
	bf := roomBinding("ollie-live")
	h := newCompanyHarness(t, srv.URL, &df, &bf, 4)
	setFixedClock(h)
	h.gw.verifySessions = true
	h.openBarrier()

	ts := "1700000000.010060"
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
	if wakeCount != 1 {
		t.Errorf("wake POSTs = %d, want 1", wakeCount)
	}
	if deliverCount != 1 {
		t.Errorf("delivery POSTs = %d, want 1", deliverCount)
	}
	if got := h.gw.deliveredAsleep.Load(); got != 0 {
		t.Errorf("company_delivered_asleep = %d, want 0 (re-check showed awake)", got)
	}
}

// TestMpimDeliverAsleepSessionWakes proves the shared wake wiring on the MPIM
// delivery loop (after the membership probe): an asleep target is woken before
// delivery.
func TestMpimDeliverAsleepSessionWakes(t *testing.T) {
	h, _, _, _ := setupMpim(t)
	h.gw.verifySessions = true

	var mu sync.Mutex
	var wakeCount, deliverCount int
	var woken bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case isGuardGET(r):
			mu.Lock()
			awake := woken
			mu.Unlock()
			if awake {
				writeSessionState(w, "active")
			} else {
				writeSessionState(w, "asleep")
			}
		case isWakePost(r):
			mu.Lock()
			wakeCount++
			woken = true
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case isDeliverPost(r):
			mu.Lock()
			deliverCount++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h.gw.cfg.gcAPIBase = srv.URL

	ts := "1700000000.010070"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> ping")
	if _, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled {
		t.Fatal("not handled")
	}
	h.wait()

	r := getMpimReceipt(t, h, ts)
	if r.Status != ingressStatusDelivered {
		t.Fatalf("status = %q, want delivered", r.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if wakeCount != 1 {
		t.Errorf("wake POSTs = %d, want 1", wakeCount)
	}
	if deliverCount != 1 {
		t.Errorf("delivery POSTs = %d, want 1", deliverCount)
	}
}
