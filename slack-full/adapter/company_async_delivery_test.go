package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPostCompanyMessageRejectsInvalidAsyncCorrelation(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "wrong status", body: `{"status":"done","request_id":"req-valid","event_cursor":"1"}`},
		{name: "control in request id", body: `{"status":"accepted","request_id":"req-bad\nlog","event_cursor":"1"}`},
		{name: "nonnumeric cursor", body: `{"status":"accepted","request_id":"req-valid","event_cursor":"not-a-sequence"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			g := &companyGateway{
				cfg:           config{gcAPIBase: srv.URL, cityName: "test-city"},
				deliverClient: srv.Client(),
			}
			result := g.postCompanyMessage(TargetDelivery{Session: "ollie"}, "hello")
			if result.disposition != postDefinitive {
				t.Fatalf("disposition = %v, want definitive rejection", result.disposition)
			}
		})
	}
}

func TestDecodeCompanyMessageEventBoundsFailureDetail(t *testing.T) {
	const maxDetailBytes = 4096
	payload, err := json.Marshal(companyAsyncEnvelope{
		Type: companyAsyncFailureEvent,
		Payload: json.RawMessage(fmt.Sprintf(
			`{"request_id":"req-bounded","operation":"session.message","error_code":"message_failed","error_message":%q}`,
			strings.Repeat("x", maxDetailBytes*2),
		)),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, matched := decodeCompanyMessageEvent(string(payload), "req-bounded")
	if !matched || result.disposition != postDefinitive {
		t.Fatalf("decoded result = %+v matched=%v", result, matched)
	}
	if len(result.detail) > maxDetailBytes {
		t.Fatalf("failure detail bytes = %d, want <= %d", len(result.detail), maxDetailBytes)
	}
}

func TestMergeCompanyTargetResultNeverLosesCorrelationOrRegressesTerminal(t *testing.T) {
	const key = "s\x00ollie"
	targets := map[string]TargetDelivery{
		key: {Status: companyTargetPending, RequestID: "req-winner", EventCursor: "42"},
	}
	mergeCompanyTargetResult(targets, key, TargetDelivery{Status: companyTargetPending, Detail: "late POST timeout"})
	if got := targets[key]; got.RequestID != "req-winner" || got.EventCursor != "42" {
		t.Fatalf("late uncorrelated result erased durable correlation: %+v", got)
	}

	mergeCompanyTargetResult(targets, key, TargetDelivery{Status: companyTargetDelivered, RequestID: "req-winner", EventCursor: "42"})
	if got := targets[key]; got.Status != companyTargetDelivered {
		t.Fatalf("matching terminal result did not settle target: %+v", got)
	}

	mergeCompanyTargetResult(targets, key, TargetDelivery{Status: companyTargetPending, RequestID: "req-winner", EventCursor: "42"})
	if got := targets[key]; got.Status != companyTargetDelivered {
		t.Fatalf("late pending result regressed terminal target: %+v", got)
	}
}

// TestCompanyAsyncMessageFailureDoesNotEmitDeliveredAck reproduces the Olivia
// incident: gc accepts the /messages request, then reports the real delivery
// failure asynchronously. A 202 is not a completed delivery and must never
// advance the receipt to delivered or emit a checkmark.
func TestCompanyAsyncMessageFailureDoesNotEmitDeliveredAck(t *testing.T) {
	const (
		requestID   = "req-olivia-failed"
		eventCursor = "42"
	)

	var mu sync.Mutex
	postCount := 0
	streamCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && isDeliverPost(r):
			mu.Lock()
			postCount++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, `{"status":"accepted","request_id":%q,"event_cursor":%q}`, requestID, eventCursor)
		case r.Method == http.MethodGet && r.URL.Path == "/v0/city/test-city/events/stream":
			mu.Lock()
			streamCount++
			mu.Unlock()
			if got := r.URL.Query().Get("after_seq"); got != eventCursor {
				t.Errorf("after_seq = %q, want %q", got, eventCursor)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, "event: event\nid: 43\ndata: {\"type\":\"request.failed\",\"payload\":{\"request_id\":%q,\"operation\":\"session.message\",\"error_code\":\"message_failed\",\"error_message\":\"queued=false\"}}\n\n", requestID)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, srv.URL, &df, &bf, 4)
	spy := &ackSpy{}
	wireAckSpy(h, spy)
	h.openBarrier()

	origin := baseOrigin()
	if w, handled := h.admitViaHandler(t, humanMessage(origin.TS, "Olivia, can you follow up?"), 0); !handled || w.Code != http.StatusOK {
		t.Fatalf("admit: handled=%v status=%d", handled, w.Code)
	}
	h.wait()

	receipt, err := h.receipts.Get(origin)
	if err != nil {
		t.Fatalf("get receipt: %v", err)
	}
	if receipt == nil {
		t.Fatal("receipt missing")
	}
	if receipt.Status != ingressStatusFailed {
		t.Fatalf("receipt status = %q, want failed", receipt.Status)
	}
	calls, replies := spy.snapshot()
	warned := false
	for _, call := range calls {
		if call.method == "reactions.add" && call.name == ackEmojiCheck {
			t.Fatalf("emitted delivered checkmark after asynchronous failure: %+v", call)
		}
		if call.method == "reactions.add" && call.name == ackEmojiWarning {
			warned = true
		}
	}
	if !warned || len(replies) != 1 {
		t.Fatalf("failure acknowledgement = calls %+v replies %+v, want warning and one reply", calls, replies)
	}
	mu.Lock()
	defer mu.Unlock()
	if postCount != 1 || streamCount != 1 {
		t.Fatalf("gc calls: POST=%d stream=%d, want 1 each", postCount, streamCount)
	}
}

func TestCompanyAsyncMessageSuccessCompletesAfterEvent(t *testing.T) {
	const requestID = "req-room-success"
	var mu sync.Mutex
	postCount, streamCount := 0, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && isDeliverPost(r):
			mu.Lock()
			postCount++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, `{"status":"accepted","request_id":%q,"event_cursor":"100"}`, requestID)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/events/stream"):
			mu.Lock()
			streamCount++
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"request.result.session.message\",\"payload\":{\"request_id\":%q,\"session_id\":\"ollie-main\"}}\n\n", requestID)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, srv.URL, &df, &bf, 4)
	spy := &ackSpy{}
	wireAckSpy(h, spy)
	h.openBarrier()
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.000101"}
	if w, handled := h.admitViaHandler(t, humanMessage(origin.TS, "hello"), 0); !handled || w.Code != http.StatusOK {
		t.Fatalf("admit: handled=%v status=%d", handled, w.Code)
	}
	h.wait()

	receipt, err := h.receipts.Get(origin)
	if err != nil || receipt == nil {
		t.Fatalf("receipt = %+v, err=%v", receipt, err)
	}
	if receipt.Status != ingressStatusDelivered {
		t.Fatalf("receipt status = %q, want delivered", receipt.Status)
	}
	for _, td := range receipt.Targets {
		if td.RequestID != requestID || td.EventCursor != "100" {
			t.Fatalf("persisted correlation = request %q cursor %q", td.RequestID, td.EventCursor)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if postCount != 1 || streamCount != 1 {
		t.Fatalf("gc calls: POST=%d stream=%d, want 1 each", postCount, streamCount)
	}
}

func TestCompanyAsyncPostsAllTargetsBeforeWaiting(t *testing.T) {
	var mu sync.Mutex
	postCount := 0
	postsAtFirstStream := -1
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && isDeliverPost(r):
			requestID, cursor := "req-riley", "20"
			if strings.Contains(r.URL.Path, "/session/ollie-main/") {
				requestID, cursor = "req-ollie", "10"
			}
			mu.Lock()
			postCount++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, `{"status":"accepted","request_id":%q,"event_cursor":%q}`, requestID, cursor)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/events/stream"):
			mu.Lock()
			if postsAtFirstStream < 0 {
				postsAtFirstStream = postCount
			}
			mu.Unlock()
			requestID := "req-riley"
			if r.URL.Query().Get("after_seq") == "10" {
				requestID = "req-ollie"
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"request.result.session.message\",\"payload\":{\"request_id\":%q,\"session_id\":\"ok\"}}\n\n", requestID)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, srv.URL, &df, &bf, 4)
	h.openBarrier()
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.000104"}
	text := "<@U0AAAAAA1> and <@U0AAAAAA2> please investigate"
	if w, handled := h.admitViaHandler(t, humanMessage(origin.TS, text), 0); !handled || w.Code != http.StatusOK {
		t.Fatalf("admit: handled=%v status=%d", handled, w.Code)
	}
	h.wait()
	receipt, err := h.receipts.Get(origin)
	if err != nil || receipt == nil || receipt.Status != ingressStatusDelivered {
		t.Fatalf("receipt = %+v, err=%v; want delivered", receipt, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if postCount != 2 {
		t.Fatalf("message POSTs = %d, want 2", postCount)
	}
	if postsAtFirstStream != 2 {
		t.Fatalf("POSTs completed before first stream wait = %d, want 2", postsAtFirstStream)
	}
}

func TestCompanyAsyncStreamDisconnectRecoversWithoutRepost(t *testing.T) {
	const requestID = "req-room-recovery"
	var mu sync.Mutex
	postCount, streamCount := 0, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && isDeliverPost(r):
			mu.Lock()
			postCount++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, `{"status":"accepted","request_id":%q,"event_cursor":"200"}`, requestID)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/events/stream"):
			mu.Lock()
			streamCount++
			attempt := streamCount
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			if attempt == 1 {
				return // connection drops before the result arrives
			}
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"request.result.session.message\",\"payload\":{\"request_id\":%q,\"session_id\":\"ollie-main\"}}\n\n", requestID)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, srv.URL, &df, &bf, 4)
	spy := &ackSpy{}
	wireAckSpy(h, spy)
	h.openBarrier()
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.000102"}
	if w, handled := h.admitViaHandler(t, humanMessage(origin.TS, "hello"), 0); !handled || w.Code != http.StatusOK {
		t.Fatalf("admit: handled=%v status=%d", handled, w.Code)
	}
	h.wait()

	pending, err := h.receipts.Get(origin)
	if err != nil || pending == nil {
		t.Fatalf("pending receipt = %+v, err=%v", pending, err)
	}
	if pending.Status != ingressStatusRouting {
		t.Fatalf("status after stream disconnect = %q, want routing", pending.Status)
	}
	if pending.AckState != ackStateEyes {
		t.Fatalf("ack state after 202 + disconnect = %q, want eyes", pending.AckState)
	}
	calls, _ := spy.snapshot()
	for _, call := range calls {
		if call.method == "reactions.add" && call.name == ackEmojiCheck {
			t.Fatalf("emitted delivered checkmark while async result was pending: %+v", call)
		}
	}
	for _, td := range pending.Targets {
		if td.Status != companyTargetPending || td.RequestID != requestID || td.EventCursor != "200" {
			t.Fatalf("pending target lost correlation: %+v", td)
		}
	}

	// A fresh gateway over the same receipt store models adapter restart. Startup
	// recovery must reconnect from the cursor and must not POST the message again.
	h.reopen(t, srv.URL, 4)
	if err := h.gw.recoverPending(); err != nil {
		t.Fatalf("recoverPending: %v", err)
	}
	h.wait()
	completed, err := h.receipts.Get(origin)
	if err != nil || completed == nil {
		t.Fatalf("completed receipt = %+v, err=%v", completed, err)
	}
	if completed.Status != ingressStatusDelivered {
		t.Fatalf("recovered status = %q, want delivered", completed.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if postCount != 1 || streamCount != 2 {
		t.Fatalf("gc calls after restart: POST=%d stream=%d, want POST=1 stream=2", postCount, streamCount)
	}
}

func TestCompanyAsyncResultTimeoutStaysPending(t *testing.T) {
	const requestID = "req-room-timeout"
	var mu sync.Mutex
	postCount, streamCount := 0, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && isDeliverPost(r):
			mu.Lock()
			postCount++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, `{"status":"accepted","request_id":%q,"event_cursor":"250"}`, requestID)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/events/stream"):
			mu.Lock()
			streamCount++
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	df := baseDirectoryFile()
	bf := baseBindingsFile()
	h := newCompanyHarness(t, srv.URL, &df, &bf, 4)
	h.gw.asyncResultTimeout = 25 * time.Millisecond
	h.openBarrier()
	origin := ReceiptOrigin{TeamID: testTeamID, ChannelID: testChannelID, TS: "1700000000.000103"}
	if w, handled := h.admitViaHandler(t, humanMessage(origin.TS, "hello"), 0); !handled || w.Code != http.StatusOK {
		t.Fatalf("admit: handled=%v status=%d", handled, w.Code)
	}
	h.wait()

	receipt, err := h.receipts.Get(origin)
	if err != nil || receipt == nil {
		t.Fatalf("receipt = %+v, err=%v", receipt, err)
	}
	if receipt.Status != ingressStatusRouting {
		t.Fatalf("receipt status after timeout = %q, want routing", receipt.Status)
	}
	for _, td := range receipt.Targets {
		if td.Status != companyTargetPending || td.RequestID != requestID || td.EventCursor != "250" {
			t.Fatalf("timed-out target lost correlation: %+v", td)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if postCount != 1 || streamCount != 1 {
		t.Fatalf("gc calls after timeout: POST=%d stream=%d, want 1 each", postCount, streamCount)
	}
}

func TestDMAsyncMessageSuccessWaitsForTerminalEvent(t *testing.T) {
	const requestID = "req-dm-success"
	var mu sync.Mutex
	postCount, streamCount := 0, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && isDeliverPost(r):
			mu.Lock()
			postCount++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, `{"status":"accepted","request_id":%q,"event_cursor":"300"}`, requestID)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/events/stream"):
			mu.Lock()
			streamCount++
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"request.result.session.message\",\"payload\":{\"request_id\":%q,\"session_id\":\"ollie\"}}\n\n", requestID)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	h, _, _ := setupDM(t)
	h.gw.cfg.gcAPIBase = srv.URL
	ts := "1700000000.010050"
	if w, handled := admitDMViaHandler(t, h, dmMessage("U0HUMAN01", ts, "hello"), ollieAppID, 0); !handled || w.Code != http.StatusOK {
		t.Fatalf("admit: handled=%v status=%d", handled, w.Code)
	}
	h.wait()
	if receipt := getReceipt(t, h, ts); receipt.Status != ingressStatusDelivered {
		t.Fatalf("DM receipt status = %q, want delivered", receipt.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if postCount != 1 || streamCount != 1 {
		t.Fatalf("DM gc calls: POST=%d stream=%d, want 1 each", postCount, streamCount)
	}
}

func TestMpimAsyncMessageSuccessWaitsForTerminalEvent(t *testing.T) {
	const requestID = "req-mpim-success"
	var mu sync.Mutex
	postCount, streamCount := 0, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && isDeliverPost(r):
			mu.Lock()
			postCount++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = fmt.Fprintf(w, `{"status":"accepted","request_id":%q,"event_cursor":"400"}`, requestID)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/events/stream"):
			mu.Lock()
			streamCount++
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"request.result.session.message\",\"payload\":{\"request_id\":%q,\"session_id\":\"ollie\"}}\n\n", requestID)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	h, _, _, _ := setupMpim(t)
	h.gw.cfg.gcAPIBase = srv.URL
	ts := "1700000000.001050"
	ev := mpimMessage("U0HUMAN01", ts, "<@U0AAAAAA1> hello")
	if w, handled := admitMpimViaHandler(t, h, ev, ollieAppID, 0); !handled || w.Code != http.StatusOK {
		t.Fatalf("admit: handled=%v status=%d", handled, w.Code)
	}
	h.wait()
	if receipt := getMpimReceipt(t, h, ts); receipt.Status != ingressStatusDelivered {
		t.Fatalf("mpim receipt status = %q, want delivered", receipt.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if postCount != 1 || streamCount != 1 {
		t.Fatalf("mpim gc calls: POST=%d stream=%d, want 1 each", postCount, streamCount)
	}
}
