package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// company_bot_tokens.go — the Phase 4 Go loader for a DM owner agent's bot
// token, a faithful port of slack_company_outbound.py load_bot_token. The Go
// gateway holds only the switchboard token; DM hydration, DM acks, and the DM
// failure-reply need the OWNER AGENT's token, selected per receipt. The
// switchboard token must NEVER be used on a DM channel — a DM receipt whose
// owner token is missing degrades (context_unavailable hydration, silent acks)
// rather than falling back to the switchboard token.
//
// The refusals match the Python loader exactly (validation, not trust: a lax
// token file is refused so a same-UID squatter cannot substitute one):
//   - the secrets dir must be a non-symlink directory, mode exactly 0700
//   - the token file bot-token-<agent>.txt must be a non-symlink regular file,
//     mode exactly 0600
//   - the file is opened O_NOFOLLOW and must be non-empty

// loadOwnerBotToken reads secrets/bot-token-<agent>.txt under secretsDir with
// the same permission/symlink refusals as the Python loader. It returns the
// trimmed token, or an error describing the refusal (absent file, lax mode,
// symlink, empty). Callers treat any error as "owner token unavailable".
func loadOwnerBotToken(secretsDir, agent string) (string, error) {
	if strings.TrimSpace(agent) == "" {
		return "", fmt.Errorf("company: token load requires an agent name")
	}
	// Defense in depth: the agent name becomes a path component. Owner agent
	// names always come from the directory (validated lowercase slugs), so a
	// well-formed call never trips this; a hostile name is rejected before any
	// path join rather than allowed to traverse.
	if !isCompanySlug(agent) {
		return "", fmt.Errorf("company: agent %q is not a directory slug; refusing token path", agent)
	}
	if strings.TrimSpace(secretsDir) == "" {
		return "", fmt.Errorf("company: secrets dir unset")
	}
	dinfo, err := os.Lstat(secretsDir)
	if err != nil {
		return "", fmt.Errorf("company: secrets dir %q is unavailable: %w", secretsDir, err)
	}
	if dinfo.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("company: secrets dir %q is a symlink; refusing", secretsDir)
	}
	if !dinfo.IsDir() {
		return "", fmt.Errorf("company: secrets dir %q is not a directory", secretsDir)
	}
	if !exactPerm(dinfo, 0o700) {
		return "", fmt.Errorf("company: secrets dir %q must be mode 0700, got %04o", secretsDir, permBits(dinfo))
	}

	// Open FIRST (O_NOFOLLOW refuses a symlink final component with ELOOP), then
	// fstat the very fd we hold and enforce the regular-file / 0600 refusals on
	// THAT inode. A pre-open Lstat would validate a different inode than the one
	// read if the path is swapped between the two syscalls (rotation TOCTOU); the
	// open-then-fstat order closes that race — the checks and the read see the
	// same file (m9).
	path := filepath.Join(secretsDir, "bot-token-"+agent+".txt")
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return "", fmt.Errorf("company: cannot open token file %q for agent %q: %w", path, agent, err)
	}
	f := os.NewFile(uintptr(fd), path)
	defer func() { _ = f.Close() }()
	finfo, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("company: cannot stat open token file %q: %w", path, err)
	}
	if !finfo.Mode().IsRegular() {
		return "", fmt.Errorf("company: token file %q is not a regular file", path)
	}
	if !exactPerm(finfo, 0o600) {
		return "", fmt.Errorf("company: token file %q must be mode 0600, got %04o", path, permBits(finfo))
	}
	data, err := io.ReadAll(io.LimitReader(f, 1<<16))
	if err != nil {
		return "", fmt.Errorf("company: cannot read token file %q: %w", path, err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("company: token file %q is empty", path)
	}
	return token, nil
}

// exactPerm reports whether info's permission bits equal want AND no
// setuid/setgid/sticky bit is set — the Go analog of Python's
// stat.S_IMODE(mode) == want over the low 12 bits.
func exactPerm(info os.FileInfo, want os.FileMode) bool {
	if info.Mode().Perm() != want {
		return false
	}
	return info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) == 0
}

// permBits renders the low 12 permission bits (including setuid/setgid/sticky)
// for the refusal message, matching the Python loader's %04o formatting.
func permBits(info os.FileInfo) os.FileMode {
	m := info.Mode().Perm()
	if info.Mode()&os.ModeSetuid != 0 {
		m |= 0o4000
	}
	if info.Mode()&os.ModeSetgid != 0 {
		m |= 0o2000
	}
	if info.Mode()&os.ModeSticky != 0 {
		m |= 0o1000
	}
	return m
}
