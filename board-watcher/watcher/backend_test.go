package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestValidateName(t *testing.T) {
	good := []string{"2026-07-29-board-watcher-approved.md", "a.md", "A_b-c.1.md"}
	for _, name := range good {
		if err := validateName(name); err != nil {
			t.Errorf("validateName(%q) = %v, want nil", name, err)
		}
	}
	bad := []string{
		"",
		"notes.txt",                      // not .md
		"../escape.md",                   // traversal
		"dir/na.md",                      // path separator
		".hidden.md",                     // leading dot
		"-oProxyCommand=x.md",            // option-looking
		"sp ace.md",                      // space (outside contract charset)
		"new\nline.md",                   // control char
		strings.Repeat("a", 300) + ".md", // too long
	}
	for _, name := range bad {
		if err := validateName(name); err == nil {
			t.Errorf("validateName(%q) = nil, want error", name)
		}
	}
}

func TestLocalBackendList(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("one.md", "hello")
	write("two.md", "world!")
	write("ignore.txt", "nope")
	if err := os.Mkdir(filepath.Join(dir, "sub.md"), 0o755); err != nil {
		t.Fatal(err)
	}

	be := &localBackend{dir: dir}
	lst, err := be.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(lst) != 2 {
		t.Fatalf("List returned %d entries, want 2: %v", len(lst), lst)
	}
	if lst["one.md"].Size != 5 || lst["two.md"].Size != 6 {
		t.Errorf("sizes wrong: %v", lst)
	}
	if lst["one.md"].MTimeUnix == 0 {
		t.Error("mtime not populated")
	}

	content, err := be.Fetch(context.Background(), "one.md")
	if err != nil || string(content) != "hello" {
		t.Errorf("Fetch = %q, %v", content, err)
	}
	if _, err := be.Fetch(context.Background(), "../escape.md"); err == nil {
		t.Error("Fetch accepted a traversal name")
	}
}

func TestLocalBackendListMissingDir(t *testing.T) {
	be := &localBackend{dir: filepath.Join(t.TempDir(), "nope")}
	if _, err := be.List(context.Background()); err == nil {
		t.Fatal("want error for missing board dir")
	}
}

// captureRunner records invocations and plays back canned responses.
type captureRunner struct {
	calls [][]string
	out   []byte
	err   error
}

func (c *captureRunner) run(_ context.Context, name string, args ...string) ([]byte, error) {
	c.calls = append(c.calls, append([]string{name}, args...))
	return c.out, c.err
}

func TestSSHBackendList(t *testing.T) {
	r := &captureRunner{out: []byte("1700000000 123 a.md\n1700000500 456 board-note.md\n")}
	be := &sshBackend{target: "afik@citadel", dir: "/share/coordination", run: r.run}
	lst, err := be.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := listing{
		"a.md":          {MTimeUnix: 1700000000, Size: 123},
		"board-note.md": {MTimeUnix: 1700000500, Size: 456},
	}
	if !reflect.DeepEqual(lst, want) {
		t.Errorf("List = %v, want %v", lst, want)
	}

	if len(r.calls) != 1 {
		t.Fatalf("runner called %d times", len(r.calls))
	}
	argv := r.calls[0]
	if argv[0] != "ssh" {
		t.Errorf("argv[0] = %q, want ssh", argv[0])
	}
	joined := strings.Join(argv, " ")
	if !strings.Contains(joined, "BatchMode=yes") {
		t.Errorf("ssh args missing BatchMode: %v", argv)
	}
	if argv[len(argv)-2] != "afik@citadel" {
		t.Errorf("target not penultimate arg: %v", argv)
	}
	script := argv[len(argv)-1]
	if !strings.Contains(script, "'/share/coordination'") {
		t.Errorf("script does not quote the board dir: %q", script)
	}
	if !strings.Contains(script, "stat -c") || !strings.Contains(script, "stat -f") {
		t.Errorf("script missing GNU/BSD stat fallback: %q", script)
	}
}

func TestSSHBackendListEmptyAndErrors(t *testing.T) {
	// Empty board dir → empty listing, no error.
	r := &captureRunner{out: []byte("")}
	be := &sshBackend{target: "peer", dir: "/b", run: r.run}
	lst, err := be.List(context.Background())
	if err != nil || len(lst) != 0 {
		t.Errorf("empty listing: got %v, %v", lst, err)
	}

	// Transport failure propagates.
	r = &captureRunner{err: fmt.Errorf("connection refused")}
	be = &sshBackend{target: "peer", dir: "/b", run: r.run}
	if _, err := be.List(context.Background()); err == nil {
		t.Error("want error on runner failure")
	}

	// Structurally broken stat output fails the whole listing rather than
	// half-applying.
	for _, out := range []string{
		"garbage\n",
		"1700000000 notanumber a.md\n",
	} {
		r = &captureRunner{out: []byte(out)}
		be = &sshBackend{target: "peer", dir: "/b", run: r.run}
		if _, err := be.List(context.Background()); err == nil {
			t.Errorf("List accepted malformed output %q", out)
		}
	}

	// Well-formed lines with out-of-contract names are skipped, not fatal:
	// one stray file on the board must not wedge polling.
	r = &captureRunner{out: []byte("1700000000 12 ../evil.md\n1700000000 12 sub/dir.md\n1700000000 5 ok.md\n")}
	be = &sshBackend{target: "peer", dir: "/b", run: r.run}
	lst, err = be.List(context.Background())
	if err != nil {
		t.Fatalf("List with skippable names: %v", err)
	}
	if len(lst) != 1 || lst["ok.md"].Size != 5 {
		t.Errorf("want only ok.md tracked, got %v", lst)
	}
}

func TestSSHBackendFetch(t *testing.T) {
	r := &captureRunner{out: []byte("# doc\n")}
	be := &sshBackend{target: "peer", dir: "/share/coordination", run: r.run}
	content, err := be.Fetch(context.Background(), "note.md")
	if err != nil || string(content) != "# doc\n" {
		t.Fatalf("Fetch = %q, %v", content, err)
	}
	remoteCmd := r.calls[0][len(r.calls[0])-1]
	if remoteCmd != "cat -- '/share/coordination/note.md'" {
		t.Errorf("remote cmd = %q", remoteCmd)
	}

	if _, err := be.Fetch(context.Background(), "../../etc/passwd.md"); err == nil {
		t.Error("Fetch accepted traversal name")
	}
	if len(r.calls) != 1 {
		t.Error("invalid name still reached the runner")
	}
}

func TestShellQuote(t *testing.T) {
	if got := shellQuote("/plain/path"); got != "'/plain/path'" {
		t.Errorf("shellQuote plain = %q", got)
	}
	if got := shellQuote("it's"); got != `'it'\''s'` {
		t.Errorf("shellQuote embedded quote = %q", got)
	}
}
