package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// withUsersInfoStub points slackAPIBase at a users.info stub for the test's
// duration and returns a call counter.
func withUsersInfoStub(t *testing.T, handler http.HandlerFunc) *atomic.Int64 {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	prev := slackAPIBase
	slackAPIBase = srv.URL
	t.Cleanup(func() { slackAPIBase = prev })
	return &calls
}

func TestResolveUserDisplayNameCachesSuccesses(t *testing.T) {
	calls := withUsersInfoStub(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/users.info" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("user"); got != "U123" {
			t.Errorf("user param = %q", got)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer xoxb-test" {
			t.Errorf("auth = %q", auth)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"user":{"name":"afik","real_name":"Afik Cohen","profile":{"display_name":"Afik","real_name":"Afik Cohen"}}}`))
	})

	cfg := config{slackBotToken: "xoxb-test", userNames: newUserNameCache()}
	if got := resolveUserDisplayName(cfg, "U123"); got != "Afik" {
		t.Fatalf("resolved = %q, want Afik (profile.display_name preferred)", got)
	}
	if got := resolveUserDisplayName(cfg, "U123"); got != "Afik" {
		t.Fatalf("second resolve = %q", got)
	}
	if calls.Load() != 1 {
		t.Errorf("users.info calls = %d, want 1 (cached)", calls.Load())
	}
}

func TestResolveUserDisplayNameFallsBackToRawIDAndNegativeCaches(t *testing.T) {
	calls := withUsersInfoStub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":false,"error":"missing_scope"}`))
	})

	cfg := config{slackBotToken: "xoxb-test", userNames: newUserNameCache()}
	if got := resolveUserDisplayName(cfg, "U456"); got != "U456" {
		t.Fatalf("resolved = %q, want raw id fallback", got)
	}
	if got := resolveUserDisplayName(cfg, "U456"); got != "U456" {
		t.Fatalf("second resolve = %q", got)
	}
	if calls.Load() != 1 {
		t.Errorf("users.info calls = %d, want 1 (failure negative-cached)", calls.Load())
	}
}

func TestResolveUserDisplayNameInertWithoutCacheOrToken(t *testing.T) {
	calls := withUsersInfoStub(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// nil cache (a directly-constructed test config) must not call Slack.
	if got := resolveUserDisplayName(config{slackBotToken: "xoxb-test"}, "U1"); got != "U1" {
		t.Fatalf("nil-cache resolve = %q, want raw id", got)
	}
	// Empty bot token likewise.
	if got := resolveUserDisplayName(config{userNames: newUserNameCache()}, "U1"); got != "U1" {
		t.Fatalf("no-token resolve = %q, want raw id", got)
	}
	if calls.Load() != 0 {
		t.Errorf("users.info calls = %d, want 0", calls.Load())
	}
}

func TestUserNameCacheExpiry(t *testing.T) {
	c := newUserNameCache()
	base := time.Now()
	c.put("U1", "Afik", base, time.Minute)
	if name, ok := c.get("U1", base.Add(30*time.Second)); !ok || name != "Afik" {
		t.Fatalf("fresh entry: got %q ok=%v", name, ok)
	}
	if _, ok := c.get("U1", base.Add(2*time.Minute)); ok {
		t.Fatal("expired entry still served")
	}
}

func TestRewriteSlackUserMentions(t *testing.T) {
	calls := withUsersInfoStub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("user") {
		case "U111":
			_, _ = w.Write([]byte(`{"ok":true,"user":{"profile":{"display_name":"Afik"}}}`))
		case "W222":
			_, _ = w.Write([]byte(`{"ok":true,"user":{"profile":{"display_name":"Taylor"}}}`))
		default:
			_, _ = w.Write([]byte(`{"ok":false,"error":"user_not_found"}`))
		}
	})

	cfg := config{slackBotToken: "xoxb-test", userNames: newUserNameCache()}
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"resolved mention", "ping <@U111> about the deploy", "ping @Afik about the deploy"},
		{"enterprise W id", "cc <@W222>", "cc @Taylor"},
		{"multiple mentions", "<@U111> and <@W222>", "@Afik and @Taylor"},
		{"labeled fallback on failure", "ask <@U999|bob> later", "ask @bob later"},
		{"unresolved kept verbatim", "ask <@U999> later", "ask <@U999> later"},
		{"resolution beats label", "<@U111|stale-label> hi", "@Afik hi"},
		{"non-user tokens untouched", "<#C123|general> <!subteam^S1|@ops> <@U111>", "<#C123|general> <!subteam^S1|@ops> @Afik"},
		{"no mentions", "plain text", "plain text"},
	}
	for _, tc := range cases {
		if got := rewriteSlackUserMentions(cfg, tc.in); got != tc.want {
			t.Errorf("%s: rewrite(%q) = %q, want %q", tc.name, tc.in, got, tc.want)
		}
	}
	// U111 and W222 resolve once each; U999 fails once then negative-caches.
	if calls.Load() != 3 {
		t.Errorf("users.info calls = %d, want 3 (cache shared across rewrites)", calls.Load())
	}

	// nil cache: text passes through untouched, no network.
	before := calls.Load()
	if got := rewriteSlackUserMentions(config{slackBotToken: "xoxb-test"}, "hi <@U111>"); got != "hi <@U111>" {
		t.Errorf("nil-cache rewrite = %q, want unchanged", got)
	}
	if calls.Load() != before {
		t.Error("nil-cache rewrite hit the network")
	}
}

func TestPreambleAuthorsResolve(t *testing.T) {
	withUsersInfoStub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("user") == "U111" {
			_, _ = w.Write([]byte(`{"ok":true,"user":{"profile":{"display_name":"Afik"}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":false,"error":"user_not_found"}`))
	})

	cfg := config{slackBotToken: "xoxb-test", userNames: newUserNameCache()}
	resolveName := func(id string) string { return resolveUserDisplayName(cfg, id) }
	replies := []slackThreadMessage{
		{TS: "1.0", User: "U111", Text: "kicking off"},
		{TS: "2.0", User: "U999", Text: "ack"},
	}
	got := formatThreadContextPreamble(replies, "3.0", "", resolveName)
	want := "Thread context (2 earlier messages):\n@Afik: kicking off\n@U999: ack\n\n---\n\n"
	if got != want {
		t.Errorf("preamble:\ngot:  %q\nwant: %q", got, want)
	}
}
