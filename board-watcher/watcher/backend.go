package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// fileStat is the cheap change signal a listing backend reports for one
// board doc. Content is only fetched (and hashed) once a change has sat
// through the debounce window, so a poll cycle stays one listing call.
type fileStat struct {
	MTimeUnix int64
	Size      int64
}

// listing maps doc basename → stat for one poll of the board directory.
type listing map[string]fileStat

// backend abstracts where the board lives. The engine runs the same
// snapshot-diff loop against either implementation; the watch-mode config
// knob only selects which backend is constructed.
type backend interface {
	// List returns the current *.md listing of the board directory.
	List(ctx context.Context) (listing, error)
	// Fetch returns the content of one board doc, by validated basename.
	Fetch(ctx context.Context, name string) ([]byte, error)
}

// validateName gates every doc name that crosses a trust boundary — names
// parsed out of remote listing output, and names interpolated into fetch
// paths. The charset matches the own-writes schema (`file` pattern): a
// leading alphanumeric blocks dotfiles and option-looking names, and the
// absence of '/' blocks traversal out of the board directory.
func validateName(name string) error {
	if !strings.HasSuffix(name, ".md") {
		return fmt.Errorf("doc name %q: not a .md file", name)
	}
	if len(name) > 255 {
		return fmt.Errorf("doc name too long (%d bytes)", len(name))
	}
	for i, r := range name {
		ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
			r == '.' || r == '_' || r == '-'
		if i == 0 {
			ok = r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
		}
		if !ok {
			return fmt.Errorf("doc name %q: character %q not allowed", name, r)
		}
	}
	return nil
}

// --- local-path backend ----------------------------------------------------

// localBackend lists a board directory on local disk. Deliberately a plain
// directory scan, not fsevents: the uniform snapshot-diff loop is what makes
// catch-up after downtime automatic, and at board scale (tens of small docs)
// a scan per poll is free.
type localBackend struct {
	dir string
}

func (b *localBackend) List(_ context.Context) (listing, error) {
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", b.dir, err)
	}
	out := listing{}
	for _, e := range entries {
		if e.IsDir() || validateName(e.Name()) != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			// Deleted between ReadDir and Info; the next poll settles it.
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		out[e.Name()] = fileStat{MTimeUnix: info.ModTime().Unix(), Size: info.Size()}
	}
	return out, nil
}

func (b *localBackend) Fetch(_ context.Context, name string) ([]byte, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	return os.ReadFile(filepath.Join(b.dir, name))
}

// --- remote-poll backend ---------------------------------------------------

// cmdRunner executes a command and returns its stdout. Injected so tests
// can capture argv and canned output without spawning ssh.
type cmdRunner func(ctx context.Context, name string, args ...string) ([]byte, error)

func execRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s: %w (stderr: %s)", name, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// sshBackend lists the peer city's board directory over ssh. One ssh
// invocation per poll for the listing, plus one per settled change for the
// content fetch — changes are rare, so the steady-state cost is a single
// short ssh session per poll interval.
//
// The connection must be non-interactive (BatchMode): keys and known_hosts
// are deployment prerequisites, not something the watcher negotiates.
type sshBackend struct {
	target string
	dir    string
	run    cmdRunner
}

// listScript stats every *.md in the board dir as "<mtime> <size> <name>"
// lines. GNU stat (-c) is tried first, BSD/macOS stat (-f) second; when the
// first succeeds the second never runs, and when both fail the non-zero
// exit makes the whole listing error out rather than parse partial output.
// Exit 9 distinguishes "board dir missing" from ssh/transport failures.
const listScript = `cd %s || exit 9
set -- *.md
[ -e "$1" ] || exit 0
stat -c '%%Y %%s %%n' -- "$@" 2>/dev/null || stat -f '%%m %%z %%N' -- "$@"`

func (b *sshBackend) sshArgs(remoteCmd string) []string {
	return []string{
		"-o", "BatchMode=yes",
		"-o", fmt.Sprintf("ConnectTimeout=%d", int(sshConnectTimeout.Seconds())),
		b.target,
		remoteCmd,
	}
}

func (b *sshBackend) List(ctx context.Context) (listing, error) {
	script := fmt.Sprintf(listScript, shellQuote(b.dir))
	out, err := b.run(ctx, "ssh", b.sshArgs(script)...)
	if err != nil {
		return nil, fmt.Errorf("remote list %s:%s: %w", b.target, b.dir, err)
	}
	return parseListing(out)
}

func (b *sshBackend) Fetch(ctx context.Context, name string) ([]byte, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	path := shellQuote(b.dir + "/" + name)
	out, err := b.run(ctx, "ssh", b.sshArgs("cat -- "+path)...)
	if err != nil {
		return nil, fmt.Errorf("remote fetch %s:%s/%s: %w", b.target, b.dir, name, err)
	}
	return out, nil
}

// parseListing decodes "<mtime> <size> <name>" lines. A structurally
// broken line (wrong field count, non-numeric stat) fails the whole
// listing — acting on half-parsed stat output risks phantom deletions,
// and skipping a cycle is cheap. A well-formed line whose NAME is outside
// the contract charset is skipped instead, mirroring the local backend:
// one stray oddly-named file on the board must not wedge polling forever.
func parseListing(out []byte) (listing, error) {
	lst := listing{}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("remote listing: malformed line %q", line)
		}
		mtime, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("remote listing: bad mtime in %q", line)
		}
		size, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("remote listing: bad size in %q", line)
		}
		name := parts[2]
		if validateName(name) != nil {
			continue
		}
		lst[name] = fileStat{MTimeUnix: mtime, Size: size}
	}
	return lst, nil
}

// shellQuote single-quotes s for the remote shell, closing and reopening
// the quotes around any embedded single quote.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
