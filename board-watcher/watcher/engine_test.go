package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// fakeBackend serves a mutable in-memory board.
type fakeBackend struct {
	lst      listing
	content  map[string][]byte
	listErr  error
	fetchErr map[string]error
}

func (f *fakeBackend) List(_ context.Context) (listing, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := listing{}
	for k, v := range f.lst {
		out[k] = v
	}
	return out, nil
}

func (f *fakeBackend) Fetch(_ context.Context, name string) ([]byte, error) {
	if err := f.fetchErr[name]; err != nil {
		return nil, err
	}
	c, ok := f.content[name]
	if !ok {
		return nil, fmt.Errorf("fetch %s: not found", name)
	}
	return c, nil
}

// set writes a doc into the fake board with the given mtime.
func (f *fakeBackend) set(name string, mtime int64, content string) {
	f.lst[name] = fileStat{MTimeUnix: mtime, Size: int64(len(content))}
	f.content[name] = []byte(content)
}

func (f *fakeBackend) remove(name string) {
	delete(f.lst, name)
	delete(f.content, name)
}

// fakeNotifier records deliveries; failFirst makes the first N calls error.
type fakeNotifier struct {
	calls     []change
	failFirst int
	attempts  int
}

func (f *fakeNotifier) Notify(_ context.Context, ch change) error {
	f.attempts++
	if f.failFirst > 0 {
		f.failFirst--
		return fmt.Errorf("mail down")
	}
	f.calls = append(f.calls, ch)
	return nil
}

// testRig wires an engine to a fake board and a controllable clock, with
// state in a temp dir.
type testRig struct {
	eng   *engine
	be    *fakeBackend
	not   *fakeNotifier
	clock time.Time
}

func newTestRig(t *testing.T) *testRig {
	t.Helper()
	env := baseEnv()
	env["BOARD_WATCHER_STATE_DIR"] = t.TempDir()
	cfg, err := loadConfigFromEnv(fakeEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	be := &fakeBackend{lst: listing{}, content: map[string][]byte{}, fetchErr: map[string]error{}}
	not := &fakeNotifier{}
	rig := &testRig{
		be:    be,
		not:   not,
		clock: time.Date(2026, 7, 30, 1, 0, 0, 0, time.UTC),
	}
	rig.eng = newEngine(cfg, be, not)
	rig.eng.now = func() time.Time { return rig.clock }
	return rig
}

func (r *testRig) tick() { r.eng.tick(context.Background()) }

func (r *testRig) advance(d time.Duration) { r.clock = r.clock.Add(d) }

// pastDebounce runs the tick sequence that lets a just-made change settle:
// one tick to observe it, an advance past the quiet window, one to settle.
func (r *testRig) pastDebounce() {
	r.tick()
	r.advance(r.eng.cfg.debounce + time.Second)
	r.tick()
}

func (r *testRig) recordOwnWrite(t *testing.T, name, sha string) {
	t.Helper()
	line := fmt.Sprintf("{\"schema\":1,\"file\":%q,\"sha256\":%q,\"ts\":\"2026-07-30T00:00:00Z\"}\n", name, sha)
	f, err := os.OpenFile(r.eng.cfg.ledgerPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(line); err != nil {
		t.Fatal(err)
	}
}

func TestBaselineIsSilent(t *testing.T) {
	r := newTestRig(t)
	r.be.set("old-1.md", 1000, "# doc one\n")
	r.be.set("old-2.md", 2000, "# doc two\n")
	r.tick()
	if len(r.not.calls) != 0 {
		t.Fatalf("baseline notified: %v", r.not.calls)
	}
	snap, err := loadSnapshot(r.eng.cfg.snapshotPath())
	if err != nil || snap == nil {
		t.Fatalf("snapshot not persisted: %v", err)
	}
	if len(snap.Files) != 2 {
		t.Fatalf("snapshot has %d files, want 2", len(snap.Files))
	}
	if snap.Files["old-1.md"].SHA256 != sha256hex([]byte("# doc one\n")) {
		t.Error("baseline did not hash content")
	}
}

func TestPeerAddNotifiesAfterDebounce(t *testing.T) {
	r := newTestRig(t)
	r.tick() // empty baseline

	r.be.set("new.md", 3000, "# hi\n\n**From:** boomtown-mayor · ~23:55Z\n")
	r.tick()
	if len(r.not.calls) != 0 {
		t.Fatal("notified before debounce elapsed")
	}
	r.advance(61 * time.Second)
	r.tick()
	if len(r.not.calls) != 1 {
		t.Fatalf("want 1 notification, got %d", len(r.not.calls))
	}
	ch := r.not.calls[0]
	if ch.Name != "new.md" || ch.Verb != "added" || ch.From != "boomtown-mayor · ~23:55Z" {
		t.Errorf("notification = %+v", ch)
	}
	// Settled: further quiet polls stay silent.
	r.advance(5 * time.Minute)
	r.tick()
	if len(r.not.calls) != 1 {
		t.Errorf("settled change re-notified")
	}
}

func TestActiveWritingDefersUntilQuiet(t *testing.T) {
	r := newTestRig(t)
	r.be.set("doc.md", 1000, "v1")
	r.tick() // baseline

	for i := int64(1); i <= 3; i++ {
		r.be.set("doc.md", 1000+i, fmt.Sprintf("v1 rev %d", i))
		r.advance(30 * time.Second)
		r.tick()
	}
	if len(r.not.calls) != 0 {
		t.Fatal("notified while the doc was still moving")
	}
	r.advance(61 * time.Second)
	r.tick()
	if len(r.not.calls) != 1 || r.not.calls[0].Verb != "updated" {
		t.Fatalf("want 1 'updated' notification, got %v", r.not.calls)
	}
}

func TestOwnWriteIsSuppressed(t *testing.T) {
	r := newTestRig(t)
	r.tick() // empty baseline

	content := "# our own post\n"
	r.recordOwnWrite(t, "ours.md", sha256hex([]byte(content)))
	r.be.set("ours.md", 4000, content)
	r.pastDebounce()
	if len(r.not.calls) != 0 {
		t.Fatalf("own write notified: %v", r.not.calls)
	}
	// Still settled into the snapshot so a later peer edit diffs cleanly.
	snap, _ := loadSnapshot(r.eng.cfg.snapshotPath())
	if _, ok := snap.Files["ours.md"]; !ok {
		t.Fatal("own write not settled into snapshot")
	}

	// A peer edit of the same doc afterwards still notifies.
	r.be.set("ours.md", 5000, content+"\npeer amendment\n")
	r.pastDebounce()
	if len(r.not.calls) != 1 || r.not.calls[0].Verb != "updated" {
		t.Fatalf("peer edit after own write: %v", r.not.calls)
	}
}

func TestTouchWithoutContentChangeIsSilent(t *testing.T) {
	r := newTestRig(t)
	r.be.set("doc.md", 1000, "same content")
	r.tick() // baseline hashes it

	r.be.lst["doc.md"] = fileStat{MTimeUnix: 9999, Size: int64(len("same content"))}
	r.pastDebounce()
	if len(r.not.calls) != 0 {
		t.Fatalf("touch notified: %v", r.not.calls)
	}
}

func TestPeerDeleteNotifies(t *testing.T) {
	r := newTestRig(t)
	r.be.set("doomed.md", 1000, "bye")
	r.tick() // baseline

	r.be.remove("doomed.md")
	r.pastDebounce()
	if len(r.not.calls) != 1 || r.not.calls[0].Verb != "removed" || r.not.calls[0].Name != "doomed.md" {
		t.Fatalf("want removed notification, got %v", r.not.calls)
	}
	snap, _ := loadSnapshot(r.eng.cfg.snapshotPath())
	if _, ok := snap.Files["doomed.md"]; ok {
		t.Error("deleted doc still in snapshot")
	}
}

func TestOwnDeleteTombstoneIsSuppressed(t *testing.T) {
	r := newTestRig(t)
	r.be.set("ours.md", 1000, "ours")
	r.tick() // baseline

	r.recordOwnWrite(t, "ours.md", ledgerTombstone)
	r.be.remove("ours.md")
	r.pastDebounce()
	if len(r.not.calls) != 0 {
		t.Fatalf("own delete notified: %v", r.not.calls)
	}
	snap, _ := loadSnapshot(r.eng.cfg.snapshotPath())
	if _, ok := snap.Files["ours.md"]; ok {
		t.Error("own-deleted doc still in snapshot")
	}
}

func TestNotifyFailureRetriesNextCycle(t *testing.T) {
	r := newTestRig(t)
	r.tick() // empty baseline
	r.not.failFirst = 1

	r.be.set("new.md", 3000, "content")
	r.pastDebounce() // first delivery attempt fails
	if len(r.not.calls) != 0 {
		t.Fatal("failed notify recorded a delivery")
	}
	// Change must NOT have settled — the nudge would be lost.
	snap, _ := loadSnapshot(r.eng.cfg.snapshotPath())
	if _, ok := snap.Files["new.md"]; ok {
		t.Fatal("change settled despite notify failure")
	}
	r.advance(61 * time.Second)
	r.tick() // retry succeeds
	if len(r.not.calls) != 1 {
		t.Fatalf("want 1 delivered notification after retry, got %d", len(r.not.calls))
	}
	if r.not.attempts != 2 {
		t.Errorf("want 2 attempts, got %d", r.not.attempts)
	}
}

func TestRestartCatchesUpFromPersistedSnapshot(t *testing.T) {
	r := newTestRig(t)
	r.be.set("doc.md", 1000, "v1")
	r.tick() // baseline persists

	// New engine, same state dir: simulates a restart after downtime
	// during which the peer updated the doc.
	r.be.set("doc.md", 2000, "v2 written while we slept")
	r2 := &testRig{be: r.be, not: &fakeNotifier{}, clock: r.clock.Add(time.Hour)}
	r2.eng = newEngine(r.eng.cfg, r.be, r2.not)
	r2.eng.now = func() time.Time { return r2.clock }

	r2.pastDebounce()
	if len(r2.not.calls) != 1 || r2.not.calls[0].Verb != "updated" {
		t.Fatalf("restart catch-up: %v", r2.not.calls)
	}
}

func TestListErrorSkipsCycleWithoutForgetting(t *testing.T) {
	r := newTestRig(t)
	r.be.set("doc.md", 1000, "v1")
	r.tick() // baseline

	r.be.set("doc.md", 2000, "v2")
	r.tick() // change observed, pending

	r.be.listErr = fmt.Errorf("ssh: connect timeout")
	r.advance(61 * time.Second)
	r.tick() // failed poll: no settle, no notify, no crash
	if len(r.not.calls) != 0 {
		t.Fatal("notified during failed poll")
	}

	r.be.listErr = nil
	r.tick() // recovered poll settles the still-pending change
	if len(r.not.calls) != 1 {
		t.Fatalf("want 1 notification after recovery, got %d", len(r.not.calls))
	}
}

func TestFlickerAddRemoveBeforeSettleIsDropped(t *testing.T) {
	r := newTestRig(t)
	r.tick() // empty baseline

	r.be.set("blip.md", 1000, "here and gone")
	r.tick()
	r.be.remove("blip.md")
	r.advance(61 * time.Second)
	r.tick()
	if len(r.not.calls) != 0 {
		t.Fatalf("flicker notified: %v", r.not.calls)
	}
	if len(r.eng.pending) != 0 {
		t.Errorf("flicker left pending state: %v", r.eng.pending)
	}
}
