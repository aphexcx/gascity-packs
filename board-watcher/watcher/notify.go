package main

import (
	"context"
	"fmt"
	"strings"
)

// change is one settled, non-own board change headed for the mayor.
type change struct {
	Name   string // doc basename
	Verb   string // added | updated | removed
	SHA256 string // settled content hash ("" for removals)
	From   string // value of the doc's From: header, if present
}

// notifier delivers a settled change. The only production implementation
// mails the local mayor; tests substitute a recorder.
type notifier interface {
	Notify(ctx context.Context, ch change) error
}

// mailNotifier delivers nudges via the designed surface ONLY: local
// `gc mail send <recipient> --notify`. No cross-city mail, no Slack — the
// whole point of the watcher is to keep working when Slack or the funnel
// is down.
type mailNotifier struct {
	cfg config
	run cmdRunner
}

func (m *mailNotifier) Notify(ctx context.Context, ch change) error {
	args := []string{}
	if m.cfg.cityPath != "" {
		// The watcher runs as a supervised service, not from inside the
		// city tree; aim gc at the city explicitly instead of relying on
		// cwd discovery.
		args = append(args, "--city", m.cfg.cityPath)
	}
	args = append(args,
		"mail", "send", m.cfg.mailTo,
		"-s", subjectLine(ch),
		"-m", bodyText(m.cfg, ch),
		"--notify",
		"--from", "board-watcher",
	)
	if _, err := m.run(ctx, m.cfg.gcBin, args...); err != nil {
		return fmt.Errorf("gc mail send: %w", err)
	}
	return nil
}

// subjectLine renders "board: <file> <verb>", with the doc's From: author
// appended when trivially available (the doc-header convention boomtown
// ships alongside this watcher).
func subjectLine(ch change) string {
	s := fmt.Sprintf("board: %s %s", ch.Name, ch.Verb)
	if author := fromAuthor(ch.From); author != "" {
		s += fmt.Sprintf(" (from %s)", author)
	}
	return s
}

func bodyText(cfg config, ch change) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Peer change on the coordination board.\n\n")
	fmt.Fprintf(&b, "file: %s\n", ch.Name)
	fmt.Fprintf(&b, "change: %s\n", ch.Verb)
	if ch.From != "" {
		fmt.Fprintf(&b, "from: %s\n", ch.From)
	}
	if ch.SHA256 != "" {
		fmt.Fprintf(&b, "sha256: %s\n", ch.SHA256)
	}
	fmt.Fprintf(&b, "board: %s\n", cfg.boardDescr())
	fmt.Fprintf(&b, "watcher: %s (%s)\n", cfg.city, cfg.mode)
	return b.String()
}

// extractFrom pulls the value of a doc's From: header. The board doc
// convention writes "**From:** boomtown-mayor · ~23:55Z" near the top;
// plain "From:" is accepted too. Only the doc's head is scanned so a
// quoted mail transcript later in a doc can't spoof the author line.
func extractFrom(content []byte) string {
	lines := strings.Split(string(content), "\n")
	if len(lines) > 20 {
		lines = lines[:20]
	}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		for _, prefix := range []string{"**From:**", "From:"} {
			if v, ok := strings.CutPrefix(line, prefix); ok {
				return strings.TrimSpace(v)
			}
		}
	}
	return ""
}

// fromAuthor reduces a full From: value to the author token for the mail
// subject: everything before the first "·" separator.
func fromAuthor(from string) string {
	author, _, _ := strings.Cut(from, "·")
	return strings.TrimSpace(author)
}
