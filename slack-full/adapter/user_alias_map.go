package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// userAliasMap is the read-mostly adapter-side view of the
// slack-user-aliases.json file an operator curates. It maps a gc handle
// (the bare token after the address prefix, e.g. "mayor") to the
// fully-formed Slack mention that handlePublish substitutes when that
// handle appears as an `@handle` token in an outbound message body.
//
// This is the OUTBOUND inverse of subteamAliasMap: where subteamAliasMap
// resolves an inbound Slack User Group ID -> handle, userAliasMap
// resolves an outbound handle -> Slack mention, so a session writing
// "@mayor" produces a clickable, notifying mention (`<@U…>`) instead of
// the literal string "@mayor" that chat.postMessage renders today
// (gpk-uha7). The inbound side already resolves User Groups -> handles
// via subteamAliasMap (PR #19); this reuses the same operator-curated
// allowlist discipline for the reverse direction.
//
// The map is the ONLY gate on outbound rewriting: a handle absent from
// the map is left verbatim — fail-safe, so unmapped handles never ping a
// surprise target and today's behavior is preserved for them.
// Locked-down workspaces that lack users:read / usergroups:read are
// fully supported because the file is operator-populated off-band,
// exactly like subteam-aliases.json. No users.lookup / usergroups.list
// fetch is performed.
//
// Reload contract mirrors subteamAliasMap: CLI/operator-edited off-band
// (file edited directly, or by a future `gc slack user-alias` command)
// and re-read on SIGHUP via the same Stage/Commit pattern.
//
// Schema (slack-user-aliases.json): a flat object of bare handle -> raw
// Slack target ID. The user-vs-group mention syntax is derived from the
// ID prefix at parse time (see slackMentionFor); the stored value is the
// finished mention token.
//
//	{
//	  "mayor":       "U0123ABCD",
//	  "design-team": "S0456WXYZ"
//	}
type userAliasMap struct {
	mu sync.RWMutex
	// byHandle maps a bare handle to its finished Slack mention token,
	// e.g. "mayor" -> "<@U0123ABCD>" or "design-team" ->
	// "<!subteam^S0456WXYZ>". Precomputing the token at parse time keeps
	// the user-vs-group decision in one validated place and makes rewrite
	// a pure substitution.
	byHandle map[string]string
	diskPath string
}

// userAliasSnapshot is a parsed-but-not-yet-committed view of
// slack-user-aliases.json. nil = "file absent" sentinel — same SIGHUP
// semantics as subteamAliasSnapshot.
type userAliasSnapshot struct {
	byHandle map[string]string
}

// handleTokenPattern is the alphabet of a gc handle as it appears after
// the address prefix: letters, digits, '.', '_', '-'. Used both to scan
// outbound bodies for `@handle` tokens and to validate map keys at parse
// time, so a key that could never match the scanner is rejected loudly
// rather than sitting as dead config.
var handleTokenPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// mentionTokenPattern matches an `@handle` token whose `@` sits at a left
// word boundary — the start of the text, or any character that is not
// part of a handle. The boundary guard is what keeps the rewriter from
// touching the local part of an email-like "user@host" string, which
// would both corrupt the address and risk pinging an unrelated handle.
// Capture group 1 is the boundary char (empty at start of text); group 2
// is the bare handle.
var mentionTokenPattern = regexp.MustCompile(`(^|[^A-Za-z0-9._-])@([A-Za-z0-9._-]+)`)

// slackMentionFor converts a raw Slack target ID into the mention syntax
// chat.postMessage linkifies. The user-vs-group choice is a mechanical
// read of Slack's documented, stable ID-prefix convention — user IDs
// begin with U (or W for Enterprise Grid org users); User Group
// ("subteam") IDs begin with S — not a semantic judgment. Any other
// shape (a channel ID Cxxxx, a bot ID Bxxxx, lowercase, a typo) returns
// ok=false so parse can reject it before it ever becomes a broken
// mention on the wire.
func slackMentionFor(id string) (string, bool) {
	if len(id) < 2 {
		return "", false
	}
	for _, r := range id {
		if !((r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return "", false
		}
	}
	switch id[0] {
	case 'U', 'W':
		return "<@" + id + ">", true
	case 'S':
		return "<!subteam^" + id + ">", true
	default:
		return "", false
	}
}

// newUserAliasMap opens (or creates) the map at diskPath. A missing file
// yields an empty map — operators on locked-down apps without the file in
// place still get a fully-functional adapter; outbound `@handle` tokens
// simply stay literal, exactly as before gpk-uha7.
func newUserAliasMap(diskPath string) (*userAliasMap, error) {
	m := &userAliasMap{
		byHandle: make(map[string]string),
		diskPath: diskPath,
	}
	if err := m.load(); err != nil {
		return nil, fmt.Errorf("load user alias map from %s: %w", diskPath, err)
	}
	return m, nil
}

// rewrite returns text with every mapped `@handle` token replaced by its
// Slack mention. Unmapped handles are left verbatim. Rewriting is
// position-independent (leading and mid-text tokens both rewrite), unlike
// the inbound address parser which only honors a leading handle.
//
// v1 scope decision: rewriting is NOT suppressed inside inline-code spans
// (`@handle`) or triple-backtick preformatted blocks. Slack's own
// rendering of mentions inside code spans is client-inconsistent, so
// honoring them would add parsing surface for an ambiguous payoff. The
// limitation is documented here; revisit if operators report mentions
// leaking out of code samples.
func (m *userAliasMap) rewrite(text string) string {
	if m == nil || text == "" {
		return text
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.byHandle) == 0 {
		return text
	}
	return mentionTokenPattern.ReplaceAllStringFunc(text, func(match string) string {
		// match = boundary (0 or 1 char) + "@" + handle. Handle chars
		// never include '@', so the first '@' is the separator.
		at := strings.IndexByte(match, '@')
		boundary, handle := match[:at], match[at+1:]
		if mention, ok := m.byHandle[handle]; ok {
			return boundary + mention
		}
		return match
	})
}

// handleForUserID is the inbound inverse of the outbound rewrite: given a
// raw Slack user id, return the curated gc handle whose mention token
// targets it. Lets inbound rendering (hq-uxln9) show the operator's name
// for an identity without any Slack API call — locked-down workspaces
// missing users:read still get names for the identities the operator
// curated. When several handles map to the same id the lexicographically
// smallest wins, keeping the answer deterministic across reloads. The map
// is tiny (operator-curated), so a linear scan beats maintaining a second
// index under the same lock.
func (m *userAliasMap) handleForUserID(id string) (string, bool) {
	if m == nil || id == "" {
		return "", false
	}
	mention := "<@" + id + ">"
	m.mu.RLock()
	defer m.mu.RUnlock()
	best := ""
	for h, target := range m.byHandle {
		if target == mention && (best == "" || h < best) {
			best = h
		}
	}
	return best, best != ""
}

// Len returns the number of bindings currently loaded. Used by the
// startup / SIGHUP log lines so operators can confirm a reload picked up
// an edit.
func (m *userAliasMap) Len() int {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byHandle)
}

// All returns every loaded binding as `handle=mention` strings, sorted by
// handle for diff-stable test ordering. Tests only.
func (m *userAliasMap) All() []string {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	handles := make([]string, 0, len(m.byHandle))
	for h := range m.byHandle {
		handles = append(handles, h)
	}
	sort.Strings(handles)
	out := make([]string, 0, len(handles))
	for _, h := range handles {
		out = append(out, h+"="+m.byHandle[h])
	}
	return out
}

// maxUserAliasBytes caps the JSON file size. Handles and Slack IDs are
// short strings; even a workspace mapping thousands of handles is well
// under a few hundred KiB. 10 MiB matches subteamAliasMap's ceiling and
// is several orders of magnitude above any healthy install.
const maxUserAliasBytes = 10 << 20 // 10 MiB

// parseUserAliasMap reads diskPath into a ready-to-commit snapshot. nil +
// nil = "file absent" sentinel for SIGHUP semantics, matching
// parseSubteamAliasMap. Empty handles, handles outside the address
// alphabet, and unrecognized target IDs are all rejected at parse time so
// a corrupt or mis-shaped upstream write fails loudly instead of silently
// dropping (or mis-emitting) a mention.
func parseUserAliasMap(diskPath string) (*userAliasSnapshot, error) {
	if diskPath == "" {
		return nil, nil
	}
	f, err := openRegistryFile(diskPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxUserAliasBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", diskPath, err)
	}
	if int64(len(data)) > maxUserAliasBytes {
		return nil, fmt.Errorf("user alias file %s exceeds %d bytes", diskPath, maxUserAliasBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var stored map[string]string
	if err := dec.Decode(&stored); err != nil {
		return nil, fmt.Errorf("decode user alias map: %w", err)
	}
	byHandle := make(map[string]string, len(stored))
	for handle, target := range stored {
		if handle == "" {
			return nil, fmt.Errorf("user alias map: empty handle key")
		}
		if !handleTokenPattern.MatchString(handle) {
			return nil, fmt.Errorf("user alias map: handle %q contains characters outside the address alphabet [A-Za-z0-9._-] — use the bare handle (e.g. \"mayor\", not \"@mayor\")", handle)
		}
		mention, ok := slackMentionFor(target)
		if !ok {
			return nil, fmt.Errorf("user alias map: handle %q maps to %q, which is not a recognized Slack user (U…/W…) or user-group (S…) ID", handle, target)
		}
		byHandle[handle] = mention
	}
	return &userAliasSnapshot{byHandle: byHandle}, nil
}

// load is the constructor-time helper — called pre-publish, no lock needed.
func (m *userAliasMap) load() error {
	snap, err := parseUserAliasMap(m.diskPath)
	if err != nil {
		return err
	}
	if snap != nil {
		m.byHandle = snap.byHandle
	}
	return nil
}

// Stage parses the on-disk file into a snapshot ready for atomic Commit.
// nil snapshot + nil error = file absent, preserve live state.
func (m *userAliasMap) Stage() (*userAliasSnapshot, error) {
	return parseUserAliasMap(m.diskPath)
}

// Commit atomically swaps the in-memory snapshot under the write lock.
func (m *userAliasMap) Commit(snap *userAliasSnapshot) {
	if snap == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byHandle = snap.byHandle
}

// Reload combines Stage and Commit; per-registry test convenience.
func (m *userAliasMap) Reload() error {
	snap, err := m.Stage()
	if err != nil {
		return err
	}
	m.Commit(snap)
	return nil
}
