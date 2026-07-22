package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests for the Events API redelivery seen-set (hw-94w5k finding #4):
// a Slack retry re-delivers the same envelope with the same event_id,
// and the adapter must forward it into gc exactly once.

// postSignedEvent signs and POSTs one events-API envelope through
// handleSlackEvents, returning the recorder.
func postSignedEvent(t *testing.T, cfg config, envBody []byte) *httptest.ResponseRecorder {
	t.Helper()
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	sig := signFor(cfg.slackSigningKey, ts, envBody)
	req := httptest.NewRequest(http.MethodPost, "/slack/events", bytes.NewReader(envBody))
	req.Header.Set("X-Slack-Request-Timestamp", ts)
	req.Header.Set("X-Slack-Signature", sig)
	w := httptest.NewRecorder()
	handleSlackEvents(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil)(w, req)
	return w
}

// eventEnvelopeBody marshals a plain channel-message event_callback
// with the given event_id (empty means "omit").
func eventEnvelopeBody(t *testing.T, eventID, ts, text string) []byte {
	t.Helper()
	rawMsg, err := json.Marshal(slackMessageEvent{
		Type: "message", Channel: "C1", User: "U1", TS: ts, Text: text,
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	envBody, err := json.Marshal(slackEventEnvelope{
		Type: "event_callback", EventID: eventID, Event: rawMsg,
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return envBody
}

// awaitInboundHits polls until the gc stub has seen want inbound POSTs
// or the deadline passes, then asserts the count holds (no extras).
func awaitInboundHits(t *testing.T, hits *int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(hits) >= want {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Settle window: catch a duplicate forward racing in late.
	time.Sleep(150 * time.Millisecond)
	if got := atomic.LoadInt32(hits); got != want {
		t.Errorf("inbound POSTs = %d, want %d", got, want)
	}
}

func dedupTestConfig(t *testing.T, gcURL string) config {
	t.Helper()
	return config{
		gcAPIBase:       gcURL,
		cityName:        "test-city",
		provider:        "slack",
		accountID:       "T1",
		slackSigningKey: "secret",
		dispatchSem:     make(chan struct{}, 4),
		eventDedup:      newEventDedupCache(eventDedupTTL),
	}
}

func countingGCStub(t *testing.T) (*httptest.Server, *int32) {
	t.Helper()
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// A retried delivery — same envelope, same event_id — forwards to gc
// exactly once; the retry is dropped with a dedup log line and still
// sees a 200 ack.
func TestHandleSlackEventsDedupsRetriedEventID(t *testing.T) {
	gcStub, hits := countingGCStub(t)
	cfg := dedupTestConfig(t, gcStub.URL)

	read, cleanup := captureLog(t)
	t.Cleanup(cleanup)

	envBody := eventEnvelopeBody(t, "Ev0001", "1.0", "hi")
	if w := postSignedEvent(t, cfg, envBody); w.Result().StatusCode != http.StatusOK {
		t.Fatalf("first delivery status = %d, want 200", w.Result().StatusCode)
	}
	awaitInboundHits(t, hits, 1)

	if w := postSignedEvent(t, cfg, envBody); w.Result().StatusCode != http.StatusOK {
		t.Fatalf("retry delivery status = %d, want 200 (retries must still be acked)", w.Result().StatusCode)
	}
	awaitInboundHits(t, hits, 1)
	if !strings.Contains(read(), "slack event dedup") {
		t.Errorf("log missing 'slack event dedup' marker:\n%s", read())
	}
}

// Distinct event ids are independent — both forward.
func TestHandleSlackEventsDistinctEventIDsBothForward(t *testing.T) {
	gcStub, hits := countingGCStub(t)
	cfg := dedupTestConfig(t, gcStub.URL)

	postSignedEvent(t, cfg, eventEnvelopeBody(t, "Ev0001", "1.0", "hi"))
	postSignedEvent(t, cfg, eventEnvelopeBody(t, "Ev0002", "2.0", "hello"))
	awaitInboundHits(t, hits, 2)
}

// An envelope with no event_id is never deduped: two deliveries both
// forward. (Only event_callback envelopes carry event_id; nothing
// else must get caught in the seen-set.)
func TestHandleSlackEventsNoEventIDNoDedup(t *testing.T) {
	gcStub, hits := countingGCStub(t)
	cfg := dedupTestConfig(t, gcStub.URL)

	envBody := eventEnvelopeBody(t, "", "1.0", "hi")
	postSignedEvent(t, cfg, envBody)
	postSignedEvent(t, cfg, envBody)
	awaitInboundHits(t, hits, 2)
}

// A delivery dropped at the queue-full boundary must NOT record its
// event_id: Slack's retry is the only recovery for that event, and it
// has to pass the seen-set when capacity is back.
func TestHandleSlackEventsQueueFullDropDoesNotRecordEventID(t *testing.T) {
	gcStub, hits := countingGCStub(t)
	cfg := dedupTestConfig(t, gcStub.URL)
	cfg.dispatchSem = make(chan struct{}, 1)

	holdRelease, _, ok := cfg.acquireDispatchSlot()
	if !ok {
		t.Fatal("acquireDispatchSlot: failed to take initial slot in fresh sem")
	}
	envBody := eventEnvelopeBody(t, "Ev0001", "1.0", "hi")
	postSignedEvent(t, cfg, envBody) // dropped: queue full
	awaitInboundHits(t, hits, 0)

	holdRelease()
	postSignedEvent(t, cfg, envBody) // Slack retry after capacity freed
	awaitInboundHits(t, hits, 1)
}

// A nil cache (mis-wired test config) never dedupes and never panics.
func TestHandleSlackEventsNilDedupCacheForwardsEverything(t *testing.T) {
	gcStub, hits := countingGCStub(t)
	cfg := dedupTestConfig(t, gcStub.URL)
	cfg.eventDedup = nil

	envBody := eventEnvelopeBody(t, "Ev0001", "1.0", "hi")
	postSignedEvent(t, cfg, envBody)
	postSignedEvent(t, cfg, envBody)
	awaitInboundHits(t, hits, 2)
}

// Cache semantics: first seen is false, repeat within TTL is true,
// repeat after TTL is false again; empty ids are never recorded.
func TestEventDedupCacheSeenAndTTL(t *testing.T) {
	var mu sync.Mutex
	current := time.Now()
	c := newEventDedupCache(eventDedupTTL)
	c.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return current
	}

	if c.seen("Ev1") {
		t.Error("first seen(Ev1) = true, want false")
	}
	if !c.seen("Ev1") {
		t.Error("second seen(Ev1) = false, want true (within TTL)")
	}
	mu.Lock()
	current = current.Add(eventDedupTTL + time.Minute)
	mu.Unlock()
	if c.seen("Ev1") {
		t.Error("seen(Ev1) after TTL = true, want false (entry expired)")
	}
	if c.seen("") {
		t.Error("seen(\"\") = true, want false (empty ids never dedupe)")
	}
	if n := c.size(); n != 1 {
		t.Errorf("size = %d, want 1 (only the re-recorded Ev1; empty id not stored)", n)
	}
}

// Cache semantics: the size cap evicts the oldest id, keeping the map
// bounded under an event flood.
func TestEventDedupCacheCapEvictsOldest(t *testing.T) {
	var mu sync.Mutex
	current := time.Now()
	c := newEventDedupCache(eventDedupTTL)
	c.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return current
	}

	for i := 0; i <= eventDedupMaxEntries; i++ {
		mu.Lock()
		current = current.Add(time.Millisecond)
		mu.Unlock()
		c.seen(fmt.Sprintf("Ev%d", i))
	}
	if n := c.size(); n != eventDedupMaxEntries {
		t.Errorf("size = %d, want cap %d", n, eventDedupMaxEntries)
	}
	if c.seen("Ev0") {
		t.Error("oldest id survived cap eviction; want evicted (seen = false)")
	}
	last := fmt.Sprintf("Ev%d", eventDedupMaxEntries)
	if !c.seen(last) {
		t.Error("newest id missing after cap eviction; want kept (seen = true)")
	}
}

// A nil cache is inert on every method.
func TestEventDedupCacheNilSafe(t *testing.T) {
	var c *eventDedupCache
	if c.seen("Ev1") {
		t.Error("seen on nil cache = true, want false")
	}
	if n := c.size(); n != 0 {
		t.Errorf("size on nil cache = %d, want 0", n)
	}
}
