package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
)

// ledgerTombstone is the sha256 value that records an own DELETION of a
// board doc, so a city's own removals don't nudge its mayor.
const ledgerTombstone = "-"

// ledgerEntry is one line of own-writes.jsonl — the interop contract with
// each city's write path (citadel's board-record-write command, boomtown's
// board-post helper). See the pack README "Own-write ledger" section; the
// canonical shape is schema/own_writes.schema.json.
type ledgerEntry struct {
	Schema int    `json:"schema"`
	File   string `json:"file"`
	SHA256 string `json:"sha256"`
	TS     string `json:"ts"`
}

// ledger answers "did we write this ourselves?" against own-writes.jsonl.
// It re-reads the file on every check: checks happen only when a change
// settles (rare), the file is tiny, and re-reading picks up hashes the
// write helper appended moments ago without any coordination.
type ledger struct {
	path string
}

// matches reports whether the ledger records (file, sha) as an own write.
// A missing ledger file suppresses nothing. Malformed lines are skipped:
// a corrupt ledger line should degrade to a spurious nudge, not kill the
// suppression of every other entry.
func (l *ledger) matches(file, sha string) bool {
	f, err := os.Open(l.path)
	if err != nil {
		return false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e ledgerEntry
		if json.Unmarshal(line, &e) != nil {
			continue
		}
		if e.File == file && e.SHA256 == sha {
			return true
		}
	}
	return false
}
