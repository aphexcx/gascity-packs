package main

import (
	"sync"
	"time"
)

// Events API redelivery dedup (hw-94w5k finding #4).
//
// Slack retries any /slack/events delivery it considers unacknowledged
// — the initial POST timing out, a dropped connection, or a non-2xx —
// up to three times (immediately, ~1 minute, ~5 minutes), and every
// retry carries the same event_id. handleSlackEvents acks with a 200
// before processing, but an ack lost in transit still produces a
// retry, and without a seen-set that retry re-forwards the same
// message into the bound session as a duplicate notification
// (observed as byte-identical inbound log pairs). The cache remembers
// recently dispatched event_ids just long enough to cover Slack's
// retry ladder.

// eventDedupTTL bounds how long a dispatched event_id is remembered.
// Slack's last retry fires ~5 minutes after the original delivery;
// 10 minutes covers the ladder with slack for clock skew and queue
// delay while keeping the map small.
const eventDedupTTL = 10 * time.Minute

// eventDedupMaxEntries hard-caps the seen-set so a pathological event
// flood cannot grow it without bound. At the cap, the oldest entry is
// evicted — that event's redeliveries just stop being deduplicable,
// which is the pre-cache behavior. 4096 ids at ~10 minutes of traffic
// is far beyond any realistic workspace's event rate.
const eventDedupMaxEntries = 4096

// eventDedupCache is a TTL seen-set over Slack event ids. Safe for
// concurrent callers. A nil *eventDedupCache never dedupes, so tests
// (and a misordered main) degrade to "no dedup" rather than panicking.
type eventDedupCache struct {
	mu      sync.Mutex
	entries map[string]time.Time
	ttl     time.Duration
	// now is the clock; nil means time.Now. Injectable so tests can
	// drive TTL expiry without sleeping.
	now func() time.Time
}

func newEventDedupCache(ttl time.Duration) *eventDedupCache {
	return &eventDedupCache{entries: make(map[string]time.Time), ttl: ttl}
}

func (c *eventDedupCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// seen reports whether id was recorded within the TTL, recording it as
// a side effect either way. The check-and-record is atomic under the
// mutex so two concurrent deliveries of the same event dedupe to one.
// Expired entries are swept opportunistically on each call; if the map
// is still over cap after the sweep, the oldest entry is evicted.
func (c *eventDedupCache) seen(id string) bool {
	if c == nil || id == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	for k, at := range c.entries {
		if now.Sub(at) > c.ttl {
			delete(c.entries, k)
		}
	}
	_, present := c.entries[id]
	c.entries[id] = now
	if !present && len(c.entries) > eventDedupMaxEntries {
		c.evictOldestLocked()
	}
	return present
}

// size reports the number of remembered ids. Test helper.
func (c *eventDedupCache) size() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// evictOldestLocked drops the single oldest entry. Called with c.mu
// held, only on the insert that pushed the map past the cap (expired
// entries were already swept by seen).
func (c *eventDedupCache) evictOldestLocked() {
	var oldestID string
	var oldestAt time.Time
	first := true
	for k, at := range c.entries {
		if first || at.Before(oldestAt) {
			oldestID, oldestAt, first = k, at, false
		}
	}
	if !first {
		delete(c.entries, oldestID)
	}
}
