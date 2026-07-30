package main

import (
	"context"
	"strings"
	"testing"
)

func TestExtractFrom(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{"board convention", "# 2026-07-29: Title\n\n**From:** boomtown-mayor · ~23:55Z\n**Beads:** hq-1\n", "boomtown-mayor · ~23:55Z"},
		{"plain prefix", "From: citadel-mayor\n\nbody\n", "citadel-mayor"},
		{"absent", "# Title\n\nno header here\n", ""},
		{"beyond head is ignored", "# Title\n" + strings.Repeat("line\n", 25) + "**From:** late-author\n", ""},
	}
	for _, tc := range cases {
		if got := extractFrom([]byte(tc.content)); got != tc.want {
			t.Errorf("%s: extractFrom = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestSubjectLine(t *testing.T) {
	ch := change{Name: "note.md", Verb: "updated"}
	if got := subjectLine(ch); got != "board: note.md updated" {
		t.Errorf("subjectLine = %q", got)
	}
	ch.From = "boomtown-mayor · ~23:55Z"
	if got := subjectLine(ch); got != "board: note.md updated (from boomtown-mayor)" {
		t.Errorf("subjectLine with From = %q", got)
	}
}

func TestMailNotifierArgs(t *testing.T) {
	cfg, err := loadConfigFromEnv(fakeEnv(baseEnv()))
	if err != nil {
		t.Fatal(err)
	}
	r := &captureRunner{}
	n := &mailNotifier{cfg: cfg, run: r.run}
	ch := change{Name: "note.md", Verb: "added", SHA256: strings.Repeat("aa", 32), From: "boomtown-mayor · ~23:55Z"}
	if err := n.Notify(context.Background(), ch); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if len(r.calls) != 1 {
		t.Fatalf("runner called %d times", len(r.calls))
	}
	argv := r.calls[0]
	joined := strings.Join(argv, "\x00")
	for _, want := range []string{
		"gc", "--city", "/cities/citadel",
		"mail", "send", "mayor",
		"-s", "board: note.md added (from boomtown-mayor)",
		"--notify",
		"--from", "board-watcher",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("argv missing %q: %v", want, argv)
		}
	}
	// Body carries the detail lines.
	var gotBody string
	for i, a := range argv {
		if a == "-m" && i+1 < len(argv) {
			gotBody = argv[i+1]
		}
	}
	for _, want := range []string{"file: note.md", "change: added", "from: boomtown-mayor", "sha256: " + ch.SHA256, "board: /share/coordination", "watcher: citadel (local-path)"} {
		if !strings.Contains(gotBody, want) {
			t.Errorf("body missing %q:\n%s", want, gotBody)
		}
	}
}
