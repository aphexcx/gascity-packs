package main

import (
	"sync"
	"time"
)

// Busy-reaction lifecycle (hq-xizo).
//
// Multi-party channel threads are the primary surface for talking to
// agents through this adapter, and Slack Assistant mode — whose
// assistant.threads.setStatus would normally render a "working on it"
// status — is deliberately not used. The channel-native
// replacement: when a targeted inbound is dispatched,
// processSlackEvent adds a busy reaction (BUSY_REACTION, default
// "hourglass") to the inbound Slack message and records the pending
// mark here; when the agent's reply is published back into the same
// conversation/thread, handlePublish looks the mark up and removes
// the reaction via reactions.remove — add on dispatch, remove on
// reply.
//
// Entries are keyed by (conversation id, thread key), where the
// thread key is the inbound's thread_ts when it was a thread reply
// and its own ts when it was a channel-root message. A reply publish
// carries reply_to_message_id equal to exactly that thread key in
// both shapes (replies to a root message thread under the root's own
// ts), so a single lookup form covers both.
//
// The registry is memory-only and best-effort by design: a mark whose
// reply never arrives expires after busyReactionTTL (the entry is
// dropped and the reaction simply stops being removable), and an
// adapter restart forgets pending marks. Nothing here may block or
// fail the dispatch or publish paths.

// busyReactionDefault is the emoji added when BUSY_REACTION is unset.
const busyReactionDefault = "hourglass"

// busyReactionTTL bounds how long a pending busy mark stays
// removable. A reply landing later than this is either a very slow
// agent or a session that died mid-task; in both cases silently
// keeping the map entry forever is worse than leaving a stale
// hourglass on one old message.
const busyReactionTTL = 30 * time.Minute

// busyReactionMaxEntries hard-caps the registry so a pathological
// event stream (many distinct targeted inbounds, no replies) cannot
// grow it without bound. Mirrors dmGateMaxEntries: on overflow,
// expired entries are already swept and the oldest surviving mark is
// evicted — that mark's reaction just stops being removable.
const busyReactionMaxEntries = 4096

// busyReactionAddWait bounds how long the remove side waits for the
// corresponding reactions.add call to finish before issuing
// reactions.remove anyway. The add normally completes in well under a
// second; the bound only matters when the add hangs (no client
// timeout on the Slack HTTP call), where an unbounded wait would leak
// the remove goroutine.
const busyReactionAddWait = 10 * time.Second

// busyReactionKey identifies one conversation/thread with a pending
// busy mark.
type busyReactionKey struct {
	channel   string
	threadKey string
}

// busyReactionMark is one pending busy reaction: the ts of the Slack
// message the reaction was added to, when it was added (for TTL), and
// the channel the add goroutine closes once its reactions.add call has
// returned. The remove side waits on addDone (bounded by
// busyReactionAddWait) before firing reactions.remove, so a reply that
// lands while the add is still in flight cannot have its remove
// overtaken by the delayed add — which would leave a permanent busy
// emoji on the message.
type busyReactionMark struct {
	messageTS string
	addedAt   time.Time
	addDone   chan struct{}
}

// busyThreadKey derives the registry thread key for an inbound
// message: its thread_ts when it is a thread reply, its own ts when
// it is a channel-root message (a reply to it will thread under that
// same ts).
func busyThreadKey(threadTS, messageTS string) string {
	if threadTS != "" {
		return threadTS
	}
	return messageTS
}

// busyReactionRegistry tracks pending busy marks. Safe for concurrent
// callers; the mutex guards the map only — Slack API calls never run
// under it.
//
// A nil *busyReactionRegistry is inert: mark is a no-op and take
// reports no pending mark, so tests (and a misordered main) degrade
// to "no lifecycle" rather than panicking.
type busyReactionRegistry struct {
	mu      sync.Mutex
	entries map[busyReactionKey]busyReactionMark
	// now is the clock; nil means time.Now. Injectable so tests can
	// drive TTL expiry without sleeping.
	now func() time.Time
}

func newBusyReactionRegistry() *busyReactionRegistry {
	return &busyReactionRegistry{entries: make(map[busyReactionKey]busyReactionMark)}
}

func (r *busyReactionRegistry) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

// mark records a pending busy reaction on messageTS under
// (channel, threadKey), sweeping expired entries opportunistically
// and evicting the oldest surviving mark if the map is still over
// cap. A re-mark of the same key (human re-tags the agent in the same
// thread before the first reply lands) overwrites — the reaction sits
// on the newest targeted message and the reply clears that one.
//
// The returned channel is the mark's addDone: the caller's add
// goroutine MUST close it once its reactions.add call has returned so
// the remove side can order remove-after-add. Always non-nil — a
// nil/invalid-args no-op still returns a fresh channel so the caller
// can close it unconditionally.
func (r *busyReactionRegistry) mark(channel, threadKey, messageTS string) chan struct{} {
	addDone := make(chan struct{})
	if r == nil || channel == "" || threadKey == "" || messageTS == "" {
		return addDone
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	r.sweepLocked(now)
	r.entries[busyReactionKey{channel: channel, threadKey: threadKey}] =
		busyReactionMark{messageTS: messageTS, addedAt: now, addDone: addDone}
	if len(r.entries) > busyReactionMaxEntries {
		r.evictOldestLocked()
	}
	return addDone
}

// take removes and returns the pending mark for (channel, threadKey),
// along with its addDone channel (see mark). An expired entry is
// deleted but NOT returned — the caller must not fire a
// reactions.remove for a mark past its TTL.
func (r *busyReactionRegistry) take(channel, threadKey string) (messageTS string, addDone chan struct{}, ok bool) {
	if r == nil {
		return "", nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := busyReactionKey{channel: channel, threadKey: threadKey}
	m, present := r.entries[key]
	if !present {
		return "", nil, false
	}
	delete(r.entries, key)
	if r.clock().Sub(m.addedAt) > busyReactionTTL {
		return "", nil, false
	}
	return m.messageTS, m.addDone, true
}

// pending reports the recorded message ts for (channel, threadKey)
// without consuming or TTL-checking the entry. Test/observability
// helper.
func (r *busyReactionRegistry) pending(channel, threadKey string) (messageTS string, ok bool) {
	if r == nil {
		return "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m, present := r.entries[busyReactionKey{channel: channel, threadKey: threadKey}]
	if !present {
		return "", false
	}
	return m.messageTS, true
}

// size reports the number of pending marks. Test helper.
func (r *busyReactionRegistry) size() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// sweepLocked drops expired marks. Called with r.mu held.
func (r *busyReactionRegistry) sweepLocked(now time.Time) {
	for k, m := range r.entries {
		if now.Sub(m.addedAt) > busyReactionTTL {
			delete(r.entries, k)
		}
	}
}

// evictOldestLocked drops the single oldest mark. Called with r.mu
// held, only on the insert that pushed the map past the cap (expired
// entries were already swept by mark).
func (r *busyReactionRegistry) evictOldestLocked() {
	var oldestKey busyReactionKey
	var oldestAt time.Time
	first := true
	for k, m := range r.entries {
		if first || m.addedAt.Before(oldestAt) {
			oldestKey, oldestAt, first = k, m.addedAt, false
		}
	}
	if !first {
		delete(r.entries, oldestKey)
	}
}
