package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// snapshotEntry is the settled state of one board doc: the stat the last
// settle observed plus the hash of the content it settled on. The hash is
// what lets a pure mtime bump (touch, rsync re-copy) settle silently.
type snapshotEntry struct {
	MTimeUnix int64  `json:"mtime_unix"`
	Size      int64  `json:"size"`
	SHA256    string `json:"sha256,omitempty"`
}

// snapshot is the persisted last-seen board state. Persisting it is what
// makes catch-up automatic: a watcher that was down (laptop asleep, city
// stopped) diffs the live board against the snapshot on its next poll and
// reports everything that happened in between.
type snapshot struct {
	Schema int                      `json:"schema"`
	Files  map[string]snapshotEntry `json:"files"`
}

// pendingChange tracks a doc that differs from the snapshot but has not
// yet sat still for the debounce window.
type pendingChange struct {
	stat       fileStat
	deleted    bool
	lastChange time.Time
}

// pollStatus is what /healthz reports about the loop's recent life.
type pollStatus struct {
	mu      sync.Mutex
	lastOK  time.Time
	lastErr string
}

func (s *pollStatus) ok(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastOK = t
	s.lastErr = ""
}

func (s *pollStatus) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastErr = err.Error()
}

func (s *pollStatus) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	last := "never"
	if !s.lastOK.IsZero() {
		last = s.lastOK.UTC().Format(time.RFC3339)
	}
	out := "last_poll_ok=" + last
	if s.lastErr != "" {
		out += " last_poll_err=" + s.lastErr
	}
	return out
}

// engine runs the snapshot-diff loop: list the board, diff against the
// settled snapshot, debounce per file, filter own writes, notify, persist.
// The loop is deliberately event-free — no fsevents, no push — so laptop
// sleep, network loss, and restarts all degrade to "the next successful
// poll reports the difference".
type engine struct {
	cfg    config
	be     backend
	notify notifier
	ledger *ledger
	now    func() time.Time
	status *pollStatus

	snap    *snapshot
	pending map[string]*pendingChange
}

func newEngine(cfg config, be backend, n notifier) *engine {
	return &engine{
		cfg:     cfg,
		be:      be,
		notify:  n,
		ledger:  &ledger{path: cfg.ledgerPath()},
		now:     time.Now,
		status:  &pollStatus{},
		pending: map[string]*pendingChange{},
	}
}

// run polls until ctx is cancelled. The first tick fires immediately so a
// restart catches up without waiting a full interval.
func (e *engine) run(ctx context.Context) {
	t := time.NewTicker(e.cfg.pollInterval)
	defer t.Stop()
	e.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.tick(ctx)
		}
	}
}

func (e *engine) tick(ctx context.Context) {
	// Bound each cycle so a hung ssh cannot stall the loop past its next
	// scheduled poll.
	ctx, cancel := context.WithTimeout(ctx, e.cfg.pollInterval)
	defer cancel()

	if e.snap == nil {
		if err := e.loadOrBaseline(ctx); err != nil {
			log.Printf("baseline: %v (will retry next poll)", err)
			e.status.fail(err)
			return
		}
	}
	if err := e.cycle(ctx); err != nil {
		log.Printf("poll: %v (skipping cycle)", err)
		e.status.fail(err)
		return
	}
	e.status.ok(e.now())
}

// loadOrBaseline restores the persisted snapshot, or on the very first run
// records the current board state WITHOUT notifying: a freshly deployed
// watcher baselining an existing board must not spray the mayor with one
// nudge per historical doc.
func (e *engine) loadOrBaseline(ctx context.Context) error {
	snap, err := loadSnapshot(e.cfg.snapshotPath())
	if err != nil {
		return err
	}
	if snap != nil {
		e.snap = snap
		return nil
	}
	lst, err := e.be.List(ctx)
	if err != nil {
		return err
	}
	snap = &snapshot{Schema: 1, Files: map[string]snapshotEntry{}}
	for name, st := range lst {
		entry := snapshotEntry{MTimeUnix: st.MTimeUnix, Size: st.Size}
		// Hash at baseline so later touch-only changes settle silently. A
		// fetch failure leaves the hash empty, which just forfeits that
		// optimization for the doc until its next real change.
		if content, err := e.be.Fetch(ctx, name); err == nil {
			entry.SHA256 = sha256hex(content)
		} else {
			log.Printf("baseline hash %s: %v", name, err)
		}
		snap.Files[name] = entry
	}
	e.snap = snap
	log.Printf("baseline recorded: %d docs (no notifications)", len(snap.Files))
	return e.persist()
}

func (e *engine) cycle(ctx context.Context) error {
	lst, err := e.be.List(ctx)
	if err != nil {
		return err
	}
	now := e.now()

	// Track additions and updates against the settled snapshot.
	for name, st := range lst {
		prev, settled := e.snap.Files[name]
		if settled && prev.MTimeUnix == st.MTimeUnix && prev.Size == st.Size {
			// Back to the settled state — drop any half-tracked change.
			delete(e.pending, name)
			continue
		}
		e.track(name, st, false, now)
	}
	// Track deletions: settled docs missing from the listing.
	for name := range e.snap.Files {
		if _, ok := lst[name]; !ok {
			e.track(name, fileStat{}, true, now)
		}
	}
	// Drop pendings for docs that appeared and vanished before settling.
	for name := range e.pending {
		_, inLst := lst[name]
		_, inSnap := e.snap.Files[name]
		if !inLst && !inSnap {
			delete(e.pending, name)
		}
	}
	// Settle everything that has sat still for the debounce window.
	for name, p := range e.pending {
		if now.Sub(p.lastChange) >= e.cfg.debounce {
			e.settle(ctx, name, p)
		}
	}
	return nil
}

// track records or refreshes a pending change. Any further movement of the
// doc restarts its quiet window — a doc being actively written (or scp'd in
// chunks) waits until it stops moving.
func (e *engine) track(name string, st fileStat, deleted bool, now time.Time) {
	p, ok := e.pending[name]
	if !ok {
		e.pending[name] = &pendingChange{stat: st, deleted: deleted, lastChange: now}
		return
	}
	if p.stat != st || p.deleted != deleted {
		p.stat, p.deleted, p.lastChange = st, deleted, now
	}
}

// settle finalizes one debounced change: hash it, decide whether it was our
// own write, notify if not, and fold it into the snapshot. Notification
// failures keep the change pending so the next cycle retries — delivery is
// at-least-once, with the hash equality check making retries idempotent
// enough in practice.
func (e *engine) settle(ctx context.Context, name string, p *pendingChange) {
	if p.deleted {
		if !e.ledger.matches(name, ledgerTombstone) {
			if err := e.notify.Notify(ctx, change{Name: name, Verb: "removed"}); err != nil {
				log.Printf("notify %s removed: %v (will retry)", name, err)
				return
			}
			log.Printf("nudged %s: %s removed", e.cfg.mailTo, name)
		} else {
			log.Printf("own delete of %s settled silently", name)
		}
		delete(e.snap.Files, name)
		delete(e.pending, name)
		if err := e.persist(); err != nil {
			log.Printf("persist snapshot: %v", err)
		}
		return
	}

	content, err := e.be.Fetch(ctx, name)
	if err != nil {
		log.Printf("fetch %s: %v (will retry)", name, err)
		return
	}
	sum := sha256hex(content)
	prev, existed := e.snap.Files[name]
	verb := "added"
	if existed {
		verb = "updated"
	}

	switch {
	case existed && prev.SHA256 != "" && prev.SHA256 == sum:
		// mtime moved but content didn't (touch, re-copy): settle silently.
		log.Printf("%s restat settled silently (content unchanged)", name)
	case e.ledger.matches(name, sum):
		log.Printf("own write of %s settled silently", name)
	default:
		ch := change{Name: name, Verb: verb, SHA256: sum, From: extractFrom(content)}
		if err := e.notify.Notify(ctx, ch); err != nil {
			log.Printf("notify %s %s: %v (will retry)", name, verb, err)
			return
		}
		log.Printf("nudged %s: %s %s", e.cfg.mailTo, name, verb)
	}

	e.snap.Files[name] = snapshotEntry{MTimeUnix: p.stat.MTimeUnix, Size: p.stat.Size, SHA256: sum}
	delete(e.pending, name)
	if err := e.persist(); err != nil {
		log.Printf("persist snapshot: %v", err)
	}
}

func (e *engine) persist() error {
	return persistSnapshot(e.cfg.snapshotPath(), e.snap)
}

func loadSnapshot(path string) (*snapshot, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("decode snapshot %s: %w", path, err)
	}
	if snap.Files == nil {
		snap.Files = map[string]snapshotEntry{}
	}
	return &snap, nil
}

// persistSnapshot writes atomically (tmp + rename) so a crash mid-write
// can't leave a truncated snapshot that would re-baseline the board.
func persistSnapshot(path string, snap *snapshot) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir state dir: %w", err)
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}
	return nil
}

func sha256hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
