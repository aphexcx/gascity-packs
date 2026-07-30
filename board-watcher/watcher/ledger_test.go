package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeLedger(t *testing.T, lines ...string) *ledger {
	t.Helper()
	path := filepath.Join(t.TempDir(), "own-writes.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return &ledger{path: path}
}

func TestLedgerMatches(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	l := writeLedger(t,
		`{"schema":1,"file":"note.md","sha256":"`+sha+`","ts":"2026-07-29T23:55:00Z"}`,
		`{"schema":1,"file":"gone.md","sha256":"-","ts":"2026-07-29T23:56:00Z"}`,
	)
	if !l.matches("note.md", sha) {
		t.Error("recorded own write did not match")
	}
	if !l.matches("gone.md", ledgerTombstone) {
		t.Error("recorded tombstone did not match")
	}
	if l.matches("note.md", strings.Repeat("cd", 32)) {
		t.Error("different hash matched")
	}
	if l.matches("other.md", sha) {
		t.Error("same hash under a different file matched")
	}
}

func TestLedgerToleratesGarbageLines(t *testing.T) {
	sha := strings.Repeat("ef", 32)
	l := writeLedger(t,
		"not json at all",
		"",
		`{"schema":1,"file":"ok.md","sha256":"`+sha+`","ts":"2026-07-29T23:55:00Z"}`,
	)
	if !l.matches("ok.md", sha) {
		t.Error("valid line after garbage did not match")
	}
}

func TestLedgerMissingFile(t *testing.T) {
	l := &ledger{path: filepath.Join(t.TempDir(), "absent.jsonl")}
	if l.matches("any.md", strings.Repeat("00", 32)) {
		t.Error("missing ledger matched something")
	}
}
