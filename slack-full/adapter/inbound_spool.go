// Inbound persist-and-retry spool (hq-xizo).
//
// handleSlackEvents 200-acks Slack BEFORE processSlackEvent forwards the
// event to gc's extmsg/inbound endpoint, so a failed forward used to
// lose the message silently — Slack never redelivers after a 200. Each
// decoded inbound is now:
//
//   - spooled to <spoolDir>/<unixnano>-<dedupkey>.json before the first
//     forward attempt (atomic tmp + fsync + rename, 0o600 file / 0o700
//     dir via writeFile0600WithSync; best-effort — a spool failure logs
//     and falls back to in-memory retries only)
//   - retried per inboundRetryDelays when the forward fails
//   - dead-lettered to <spoolDir>/dead on exhaustion; the final log
//     line keeps the "inbound POST failed" substring that external
//     log-watchers key on
//   - replayed at startup when a previous run crashed mid-retry
//
// Redelivery can duplicate an event gc already accepted; the message
// DedupKey ("slack-"+ts) makes that safe — gc's extmsg layer dedups
// on it.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// inboundRetryDelays spaces the forward retries after a failed inbound
// POST to gc: 5 attempts total over ~2 minutes, then dead-letter
// (hq-xizo). Package-level so tests can compress the schedule. Reads go
// through snapshotInboundRetryDelays and test-time swaps hold
// inboundRetryDelaysMu: delivery goroutines can outlive the test that
// spawned them (their gc stub closes at t.Cleanup, pushing them onto
// the retry path), so an unguarded swap would be a data race.
var (
	inboundRetryDelaysMu sync.RWMutex
	inboundRetryDelays   = []time.Duration{
		5 * time.Second,
		15 * time.Second,
		30 * time.Second,
		60 * time.Second,
	}
)

// snapshotInboundRetryDelays returns the current retry schedule. The
// slice contents are never mutated, only the package var is swapped
// (tests), so a snapshot of the header is safe to iterate lock-free.
func snapshotInboundRetryDelays() []time.Duration {
	inboundRetryDelaysMu.RLock()
	defer inboundRetryDelaysMu.RUnlock()
	return inboundRetryDelays
}

// spoolInbound persists a decoded inbound event before the first
// forward attempt, so a crash mid-retry cannot silently lose a
// Slack-acked message. Returns the spool file path, or "" when spooling
// is disabled (empty spoolDir) or the write fails — persistence is
// best-effort and never blocks the forward. The write routes through
// writeFile0600WithSync (tmp file in the same dir + fsync + close +
// rename, tmp removed on every error path) so a torn write can never
// leave a half-entry for startup replay to choke on.
func spoolInbound(spoolDir string, msg externalInboundMessage) string {
	if spoolDir == "" {
		return ""
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("spool: marshal: %v", err)
		return ""
	}
	name := fmt.Sprintf("%d-%s.json", time.Now().UnixNano(), sanitizeSpoolName(msg.DedupKey))
	path := filepath.Join(spoolDir, name)
	if err := writeFile0600WithSync(path, data); err != nil {
		log.Printf("spool: write %s: %v", path, err)
		return ""
	}
	return path
}

// spoolNameRE matches characters unsafe in a spool filename.
var spoolNameRE = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

func sanitizeSpoolName(s string) string {
	return spoolNameRE.ReplaceAllString(s, "_")
}

// deliverInbound forwards one inbound message to gc, retrying failures
// per inboundRetryDelays. On first success the spool entry is removed
// and the canonical "inbound:" line is logged once; when every attempt
// fails the entry moves to the dead-letter directory and deliverInbound
// returns false so processSlackEvent skips the post-forward work (eyes
// reaction, alias dispatch). The final failure line deliberately
// contains "inbound POST failed" — external log-watchers key on that
// exact substring.
func deliverInbound(cfg config, msg externalInboundMessage, spoolPath string) bool {
	delays := snapshotInboundRetryDelays()
	attempts := len(delays) + 1
	var lastErr error
	for i := range attempts {
		if i > 0 {
			time.Sleep(delays[i-1])
		}
		err := postInbound(cfg, msg)
		if err == nil {
			if spoolPath != "" {
				_ = os.Remove(spoolPath)
			}
			log.Printf("inbound: chan=%s user=%s ts=%s thread=%s target=%q files=%d text=%dch",
				msg.Conversation.ConversationID, msg.Actor.ID, msg.ProviderMessageID,
				msg.ReplyToMessageID, msg.ExplicitTarget, len(msg.Attachments), len(msg.Text))
			return true
		}
		lastErr = err
		if i < attempts-1 {
			log.Printf("inbound forward attempt %d/%d failed (retry in %s): %v",
				i+1, attempts, delays[i], err)
		}
	}
	log.Printf("inbound POST failed after %d attempts (dead-letter=%s) chan=%s ts=%s: %v",
		attempts, moveToDeadLetter(spoolPath), msg.Conversation.ConversationID,
		msg.ProviderMessageID, lastErr)
	return false
}

// moveToDeadLetter quarantines an exhausted spool entry in the sibling
// "dead" directory so it can be replayed by hand; returns the new path,
// or "none" when spooling was disabled for this message. If the move
// fails the entry stays in the spool, where startup replay will retry
// it on the next adapter restart.
func moveToDeadLetter(spoolPath string) string {
	if spoolPath == "" {
		return "none"
	}
	deadDir := filepath.Join(filepath.Dir(spoolPath), "dead")
	if err := os.MkdirAll(deadDir, 0o700); err != nil {
		log.Printf("dead-letter: mkdir: %v", err)
		return spoolPath
	}
	dest := filepath.Join(deadDir, filepath.Base(spoolPath))
	if err := os.Rename(spoolPath, dest); err != nil {
		log.Printf("dead-letter: rename: %v", err)
		return spoolPath
	}
	return dest
}

// replaySpool re-delivers inbound events a previous run persisted but
// never confirmed forwarded (crash mid-retry). main() calls it before
// the listeners start serving. Redelivery may duplicate an event gc
// already accepted — the message DedupKey makes that safe. Undecodable
// entries are quarantined to the dead-letter dir instead of
// crash-looping replay on every restart; dead-lettered entries are NOT
// replayed automatically — they stay under <spoolDir>/dead for manual
// replay. A missing spool dir is a no-op.
func replaySpool(cfg config) {
	if cfg.inboundSpoolDir == "" {
		return
	}
	entries, err := os.ReadDir(cfg.inboundSpoolDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("spool replay: %v", err)
		}
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join(cfg.inboundSpoolDir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			log.Printf("spool replay: read %s: %v", path, err)
			continue
		}
		var msg externalInboundMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("spool replay: decode %s: %v (dead-letter=%s)", path, err, moveToDeadLetter(path))
			continue
		}
		log.Printf("spool replay: re-delivering %s (chan=%s ts=%s)",
			e.Name(), msg.Conversation.ConversationID, msg.ProviderMessageID)
		go deliverInbound(cfg, msg, path)
	}
}
