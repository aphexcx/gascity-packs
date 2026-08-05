package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// withSlackStub points slackAPIBase at a test server for the duration of the
// test.
func withSlackStub(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	prev := slackAPIBase
	slackAPIBase = srv.URL
	t.Cleanup(func() {
		slackAPIBase = prev
		srv.Close()
	})
	return srv
}

func TestBotInfoResolverDefinitiveAndTransient(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		want    botResolveOutcome
		userID  string
		appID   string
		retryAt string
	}{
		{"ok", 200, `{"ok":true,"bot":{"id":"B1","app_id":"A0AAAAAA2","user_id":"U0AAAAAA2","deleted":false}}`, botResolveOK, "U0AAAAAA2", "A0AAAAAA2", ""},
		{"not_found", 200, `{"ok":false,"error":"bot_not_found"}`, botResolveUnknown, "", "", ""},
		{"deleted", 200, `{"ok":true,"bot":{"id":"B1","app_id":"A0AAAAAA2","user_id":"U0AAAAAA2","deleted":true}}`, botResolveUnknown, "", "", ""},
		{"ratelimited_json", 200, `{"ok":false,"error":"ratelimited"}`, botResolveTransient, "", "", ""},
		{"http_429", 429, `{"ok":false,"error":"rate_limited"}`, botResolveTransient, "", "", "5"},
		{"http_500", 500, ``, botResolveTransient, "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withSlackStub(t, func(w http.ResponseWriter, r *http.Request) {
				if tt.retryAt != "" {
					w.Header().Set("Retry-After", tt.retryAt)
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			})
			res := newBotInfoResolver("xoxb-test")
			info, outcome := res.Resolve("B1")
			if outcome != tt.want {
				t.Fatalf("outcome = %v, want %v", outcome, tt.want)
			}
			if outcome == botResolveOK {
				if info.UserID != tt.userID || info.AppID != tt.appID {
					t.Errorf("info = %+v, want user=%s app=%s", info, tt.userID, tt.appID)
				}
			}
		})
	}
}

// TestBotInfoResolverCaches — a definitive outcome is cached; a transient one
// is not (so a parked receipt re-drives against Slack).
func TestBotInfoResolverCaches(t *testing.T) {
	var calls atomic.Int64
	withSlackStub(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"ok":true,"bot":{"id":"B1","app_id":"A1","user_id":"U1"}}`))
	})
	res := newBotInfoResolver("xoxb-test")
	for i := 0; i < 3; i++ {
		if _, o := res.Resolve("B1"); o != botResolveOK {
			t.Fatalf("resolve %d outcome %v", i, o)
		}
	}
	if calls.Load() != 1 {
		t.Errorf("bots.info calls = %d, want 1 (cached)", calls.Load())
	}
}

func TestBotInfoResolverTransientNotCached(t *testing.T) {
	var calls atomic.Int64
	withSlackStub(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	res := newBotInfoResolver("xoxb-test")
	for i := 0; i < 3; i++ {
		if _, o := res.Resolve("B1"); o != botResolveTransient {
			t.Fatalf("resolve %d outcome %v", i, o)
		}
	}
	if calls.Load() != 3 {
		t.Errorf("bots.info calls = %d, want 3 (transient uncached)", calls.Load())
	}
}

// TestBotInfoResolverSingleflight — a burst of concurrent lookups for one bot
// collapses to a single bots.info call.
func TestBotInfoResolverSingleflight(t *testing.T) {
	var calls atomic.Int64
	release := make(chan struct{})
	withSlackStub(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release // hold the first request open so waiters pile up
		_, _ = w.Write([]byte(`{"ok":true,"bot":{"id":"B1","app_id":"A1","user_id":"U1"}}`))
	})
	res := newBotInfoResolver("xoxb-test")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = res.Resolve("B1")
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if calls.Load() != 1 {
		t.Errorf("bots.info calls = %d, want 1 (singleflight collapsed the burst)", calls.Load())
	}
}

// TestBotInfoResolverNoToken — with no switchboard token bots.info cannot be
// called, so the outcome is transient (park + retry), never a false unknown.
func TestBotInfoResolverNoToken(t *testing.T) {
	res := newBotInfoResolver("")
	if _, o := res.Resolve("B1"); o != botResolveTransient {
		t.Errorf("outcome = %v, want transient with empty token", o)
	}
}

// TestResolveCompanyAuthorCorroboration exercises the fail-closed corroboration
// checklist against a fake resolver (no network).
func TestResolveCompanyAuthorCorroboration(t *testing.T) {
	dir := testDirectory(t)
	riley := companyBotInfo{UserID: "U0AAAAAA2", AppID: "A0AAAAAA2"}
	base := CompanyMessage{TeamID: testTeam, ChannelID: testChannel, Subtype: "bot_message", BotID: "B0RILEY"}

	tests := []struct {
		name      string
		resolver  companyAuthorResolver
		msg       CompanyMessage
		want      botResolveOutcome
		wantAgent string
	}{
		{
			name:      "happy path",
			resolver:  fakeAuthorResolver{byBot: map[string]companyBotInfo{"B0RILEY": riley}},
			msg:       func() CompanyMessage { m := base; m.UserID = "U0AAAAAA2"; m.AppID = "A0AAAAAA2"; return m }(),
			want:      botResolveOK,
			wantAgent: "riley",
		},
		{
			name:     "app_id mismatch fails closed",
			resolver: fakeAuthorResolver{byBot: map[string]companyBotInfo{"B0RILEY": {UserID: "U0AAAAAA2", AppID: "A_WRONG"}}},
			msg:      base,
			want:     botResolveUnknown,
		},
		{
			name:     "event user mismatch fails closed",
			resolver: fakeAuthorResolver{byBot: map[string]companyBotInfo{"B0RILEY": riley}},
			msg:      func() CompanyMessage { m := base; m.UserID = "U0DIFFERENT"; return m }(),
			want:     botResolveUnknown,
		},
		{
			name:     "foreign event app_id pre-check skips resolver",
			resolver: fakeAuthorResolver{transient: map[string]bool{"B0RILEY": true}}, // would be transient if called
			msg:      func() CompanyMessage { m := base; m.AppID = "A_FOREIGN"; return m }(),
			want:     botResolveUnknown,
		},
		{
			name:     "transient bubbles up",
			resolver: fakeAuthorResolver{transient: map[string]bool{"B0RILEY": true}},
			msg:      base,
			want:     botResolveTransient,
		},
		{
			name:     "unregistered bot user id",
			resolver: fakeAuthorResolver{byBot: map[string]companyBotInfo{"B0RILEY": {UserID: "U0OUTSIDER", AppID: "A0OUT"}}},
			msg:      base,
			want:     botResolveUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := resolveCompanyAuthor(tt.resolver, dir, tt.msg)
			if res.Outcome != tt.want {
				t.Fatalf("outcome = %v, want %v", res.Outcome, tt.want)
			}
			if tt.wantAgent != "" {
				if res.Agent == nil || res.Agent.Name != tt.wantAgent {
					t.Errorf("agent = %v, want %s", res.Agent, tt.wantAgent)
				}
			}
		})
	}
}
