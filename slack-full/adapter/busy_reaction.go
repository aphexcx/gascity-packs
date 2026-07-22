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
// reactions.remove anyway. It MUST exceed slackAPIClient's timeout
// (30s): the add is bounded by that client, so waiting past it means
// addDone has provably closed and remove-after-add ordering holds; a
// shorter bound would reopen the very race this wait exists to
// prevent (codex r6). The timeout branch is therefore effectively
// unreachable and exists only as a leak backstop.
const busyReactionAddWait = 45 * time.Second

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
//
// stale carries orphaned predecessor reactions that still ride under
// this key (codex r6): when a re-target's forward fails while an even
// newer mark owns the key, the failed attempt's restore list merges
// here instead of being lost, so every reaction added under the key
// is eventually removed when the key's current mark concludes.
type busyReactionMark struct {
	messageTS string
	addedAt   time.Time
	addDone   chan struct{}
	stale     []busyTaken
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
// busyTaken is one consumed mark: the ts the reaction sits on and the
// channel its add goroutine closes on completion (remove-after-add
// ordering).
type busyTaken struct {
	messageTS string
	addDone   chan struct{}
}

// busyDisplaced is a mark displaced by a re-target, together with the
// registry key it was displaced from — enough to restore it if the
// displacing inbound's forward then fails (codex r5).
type busyDisplaced struct {
	threadKey string
	mark      busyTaken
}

// The returned channel is the mark's addDone: the caller's add
// goroutine MUST close it once its reactions.add call has returned so
// the remove side can order remove-after-add. Always non-nil — a
// nil/invalid-args no-op still returns a fresh channel so the caller
// can close it unconditionally. Any mark this overwrites is silently
// discarded — production code uses markBoth, which surfaces
// superseded marks so their reactions can be cleaned up.
func (r *busyReactionRegistry) mark(channel, threadKey, messageTS string) chan struct{} {
	addDone := make(chan struct{})
	r.markWithDone(channel, threadKey, messageTS, addDone)
	return addDone
}

// markBoth records the pending mark under EVERY thread key a reply may
// carry (codex r2). The canonical key is the thread root (thread_ts
// for a thread-reply inbound, own ts for a channel-root one) — but
// reply-current and the alias-dispatch instructions thread replies
// under the inbound's OWN ts (Slack normalizes either form into the
// same thread), so a thread-reply inbound is additionally marked under
// its own ts. Both entries share one addDone; consuming one leaves the
// sibling to expire by TTL, whose eventual redundant reactions.remove
// is benign ("no_reaction" counts as delivered).
//
// superseded returns the marks these writes displaced (a human
// re-targeting the same thread before the first reply lands, codex
// r3): those messages already carry a busy reaction that no registry
// entry points at anymore. Once the displacing inbound's forward
// SUCCEEDS the caller must remove those reactions (TTL expiry only
// deletes metadata, never the Slack-side emoji); if the forward
// fails, the caller restores them via cancelBoth instead (codex r5).
// Deduplicated by message ts and never includes messageTS itself.
func (r *busyReactionRegistry) markBoth(channel, threadTS, messageTS string) (addDone chan struct{}, superseded []busyDisplaced) {
	addDone = make(chan struct{})
	seen := map[string]bool{messageTS: true}
	collect := func(key string, old busyTaken, ok bool) {
		if ok && !seen[old.messageTS] {
			seen[old.messageTS] = true
			superseded = append(superseded, busyDisplaced{threadKey: key, mark: old})
		}
	}
	rootKey := busyThreadKey(threadTS, messageTS)
	for _, old := range r.markWithDone(channel, rootKey, messageTS, addDone) {
		collect(rootKey, old, true)
	}
	if threadTS != "" && threadTS != messageTS {
		for _, old := range r.markWithDone(channel, messageTS, messageTS, addDone) {
			collect(messageTS, old, true)
		}
	}
	return addDone, superseded
}

// markWithDone is the mark implementation with a caller-supplied
// addDone, letting markBoth share one channel across its two entries.
// Returns every displaced pending reaction — the previous mark (when
// its reaction sits on a DIFFERENT message) plus any stale ancestors
// riding on it (codex r6). A re-mark of the SAME message (a retaken
// Slack redelivery re-marking after a failed dispatch, codex r4)
// merges completion channels instead of overwriting — the earlier
// reactions.add may still be in flight and could land after a remove
// that only waited for the newer add — and keeps the previous entry's
// stale ancestors.
func (r *busyReactionRegistry) markWithDone(channel, threadKey, messageTS string, addDone chan struct{}) (displaced []busyTaken) {
	if r == nil || channel == "" || threadKey == "" || messageTS == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	r.sweepLocked(now)
	key := busyReactionKey{channel: channel, threadKey: threadKey}
	storeDone := addDone
	var keepStale []busyTaken
	if prev, present := r.entries[key]; present {
		switch {
		case prev.messageTS != messageTS:
			displaced = append([]busyTaken{{messageTS: prev.messageTS, addDone: prev.addDone}}, prev.stale...)
		default:
			keepStale = prev.stale
			if prev.addDone != nil && prev.addDone != addDone {
				prevDone := prev.addDone
				merged := make(chan struct{})
				go func() {
					<-prevDone
					<-addDone
					close(merged)
				}()
				storeDone = merged
			}
		}
	}
	r.entries[key] = busyReactionMark{messageTS: messageTS, addedAt: now, addDone: storeDone, stale: keepStale}
	if len(r.entries) > busyReactionMaxEntries {
		r.evictOldestLocked()
	}
	return displaced
}

// cancelBoth removes the entries markBoth created for (channel,
// threadTS, messageTS) — the inbound never reached gc, so no reply
// will ever come to clear them — restores any marks that inbound had
// displaced (their agents may still be working and their reactions
// were deliberately NOT removed yet, codex r5), and closes addDone so
// any waiter that already consumed a mark proceeds to its benign
// no-op remove (the reactions.add for a cancelled mark never fires).
// Entries are deleted only while they still point at messageTS, and a
// restore never clobbers a key a racing retry has already re-marked.
// Callers must run this BEFORE releasing the event's dedup claim, so
// a woken redelivery cannot re-mark the timestamp while the old
// attempt's cancellation is still in flight.
func (r *busyReactionRegistry) cancelBoth(channel, threadTS, messageTS string, addDone chan struct{}, restore []busyDisplaced) {
	if r != nil && channel != "" && messageTS != "" {
		r.mu.Lock()
		now := r.clock()
		keys := []string{busyThreadKey(threadTS, messageTS)}
		if threadTS != "" && threadTS != messageTS {
			keys = append(keys, messageTS)
		}
		// Restore pool: the marks this failed inbound displaced, plus
		// any stale ancestors that were riding on the cancelled
		// entries themselves (merged there by an even earlier failed
		// re-target, codex r6). Grouped per key.
		perKey := map[string][]busyTaken{}
		for _, d := range restore {
			perKey[d.threadKey] = append(perKey[d.threadKey], d.mark)
		}
		for _, k := range keys {
			key := busyReactionKey{channel: channel, threadKey: k}
			if m, ok := r.entries[key]; ok && m.messageTS == messageTS {
				delete(r.entries, key)
				perKey[k] = append(perKey[k], m.stale...)
			}
		}
		for k, marks := range perKey {
			if len(marks) == 0 {
				continue
			}
			key := busyReactionKey{channel: channel, threadKey: k}
			if cur, taken := r.entries[key]; taken {
				// A racing retry (or a newer re-target) owns the key:
				// never clobber it — merge the pool into its stale
				// ancestry so its conclusion still clears them.
				cur.stale = append(cur.stale, marks...)
				r.entries[key] = cur
				continue
			}
			r.entries[key] = busyReactionMark{
				messageTS: marks[0].messageTS,
				addedAt:   now,
				addDone:   marks[0].addDone,
				stale:     marks[1:],
			}
		}
		r.mu.Unlock()
	}
	if addDone != nil {
		close(addDone)
	}
}

// takeConversation removes and returns every pending mark in channel,
// deduplicated by message ts (dual-key entries share one message).
// Backs the unthreaded-reply path (codex r3): the documented default
// `gc slack reply-current` posts at channel root with no thread ts,
// so a delivered root publish clears every busy affordance pending in
// that conversation rather than leaving hourglasses stuck forever.
// Expired entries are dropped, not returned.
func (r *busyReactionRegistry) takeConversation(channel string) []busyTaken {
	if r == nil || channel == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	seen := map[string]bool{}
	var taken []busyTaken
	for k, m := range r.entries {
		if k.channel != channel {
			continue
		}
		delete(r.entries, k)
		if now.Sub(m.addedAt) > busyReactionTTL {
			continue
		}
		for _, t := range append([]busyTaken{{messageTS: m.messageTS, addDone: m.addDone}}, m.stale...) {
			if !seen[t.messageTS] {
				seen[t.messageTS] = true
				taken = append(taken, t)
			}
		}
	}
	return taken
}

// takeMessage removes the entries markBoth created for (channel,
// threadTS, messageTS) while they still point at messageTS, returning
// that message's pending reaction for removal. Backs the
// alias-delivery-failure path (codex r6): the reactions.add already
// launched but no reply is coming (the addressed session never got
// the message and the bound session stays silent), so the emoji must
// come off now. Stale ancestors riding on a consumed entry are NOT
// removed — their messages' fate is independent — they are re-parked
// under the key so a later conclusion still clears them.
func (r *busyReactionRegistry) takeMessage(channel, threadTS, messageTS string) []busyTaken {
	if r == nil || channel == "" || messageTS == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	keys := []string{busyThreadKey(threadTS, messageTS)}
	if threadTS != "" && threadTS != messageTS {
		keys = append(keys, messageTS)
	}
	var taken []busyTaken
	seen := map[string]bool{}
	for _, k := range keys {
		key := busyReactionKey{channel: channel, threadKey: k}
		m, ok := r.entries[key]
		if !ok || m.messageTS != messageTS {
			continue
		}
		delete(r.entries, key)
		expired := now.Sub(m.addedAt) > busyReactionTTL
		if !expired && !seen[m.messageTS] {
			seen[m.messageTS] = true
			taken = append(taken, busyTaken{messageTS: m.messageTS, addDone: m.addDone})
		}
		if len(m.stale) > 0 && !expired {
			// Re-park the ancestors: first becomes the key's mark,
			// the rest stay stale on it.
			r.entries[key] = busyReactionMark{
				messageTS: m.stale[0].messageTS,
				addedAt:   now,
				addDone:   m.stale[0].addDone,
				stale:     m.stale[1:],
			}
		}
	}
	return taken
}

// take removes and returns every pending reaction for (channel,
// threadKey) — the current mark plus any stale ancestors riding on it
// (codex r6). An expired entry is deleted but NOT returned — the
// caller must not fire a reactions.remove for a mark past its TTL
// (its stale ancestors expire with it).
func (r *busyReactionRegistry) take(channel, threadKey string) []busyTaken {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := busyReactionKey{channel: channel, threadKey: threadKey}
	m, present := r.entries[key]
	if !present {
		return nil
	}
	delete(r.entries, key)
	if r.clock().Sub(m.addedAt) > busyReactionTTL {
		return nil
	}
	return append([]busyTaken{{messageTS: m.messageTS, addDone: m.addDone}}, m.stale...)
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
