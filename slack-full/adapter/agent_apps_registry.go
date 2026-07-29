package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
)

// agent_apps_registry.go — the Phase 4 per-agent-app signing-secret registry
// (agent_apps.json). It is NEW work, disjoint from the legacy apps.json:
// `gc slack register-agent-app` writes records {team_id, api_app_id,
// signing_secret} that bind an agent identity app's DM events to exactly that
// app's secret. The adapter reads it read-only at startup and on SIGHUP,
// exactly like company_directory.json / company_bindings.json (never-fatal
// load, symlink-rejecting open, last-known-good on a bad reload).
//
// Owner-agent identity is NOT stored here: it derives by joining an
// api_app_id against company_directory.json agents[].app_id (the directory is
// the canonical name↔app_id↔bot_user_id source). A registered secret whose
// api_app_id has no directory agent is a load/reload warning and admits
// nothing.

// AgentApp is one registered agent identity app: its workspace, its Slack
// api_app_id (A0…), and the app's signing secret used to verify that app's
// inbound DM events. The JSON tags are the contract with the CLI writer.
type AgentApp struct {
	TeamID        string `json:"team_id"`
	APIAppID      string `json:"api_app_id"`
	SigningSecret string `json:"signing_secret"`
}

// agentAppsFile is the on-disk JSON envelope for agent_apps.json.
type agentAppsFile struct {
	SchemaVersion int        `json:"schema_version"`
	AgentApps     []AgentApp `json:"agent_apps"`
}

// agentAppsSchemaVersion is the only agent-apps schema this build understands.
const agentAppsSchemaVersion = 1

// AgentApps is a validated, normalized api_app_id → record index. Like
// CompanyDirectory it is immutable after parse and safe for concurrent reads;
// the store swaps whole values.
type AgentApps struct {
	byAppID map[string]*AgentApp
}

// ParseAgentApps decodes and validates agent_apps.json. Structural problems
// fail closed (unsupported schema, a record missing team_id/api_app_id, a
// duplicate api_app_id, an empty signing_secret). An empty document is valid
// (no apps registered). Directory joins are checked separately (JoinWarnings)
// because a registered secret is authoritative for signature verification
// regardless of whether the directory currently names an owner agent.
func ParseAgentApps(data []byte) (*AgentApps, error) {
	var file agentAppsFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("decode agent apps: %w", err)
	}
	if file.SchemaVersion != agentAppsSchemaVersion {
		return nil, fmt.Errorf("agent apps: unsupported schema_version %d (want %d)", file.SchemaVersion, agentAppsSchemaVersion)
	}
	out := &AgentApps{byAppID: make(map[string]*AgentApp, len(file.AgentApps))}
	for i := range file.AgentApps {
		rec := file.AgentApps[i]
		if rec.TeamID == "" || rec.APIAppID == "" {
			return nil, fmt.Errorf("agent apps: record[%d] missing team_id/api_app_id (team_id=%q api_app_id=%q)", i, rec.TeamID, rec.APIAppID)
		}
		if rec.SigningSecret == "" {
			return nil, fmt.Errorf("agent apps: record[%d] (api_app_id=%q) missing signing_secret", i, rec.APIAppID)
		}
		if _, dup := out.byAppID[rec.APIAppID]; dup {
			return nil, fmt.Errorf("agent apps: duplicate api_app_id %q", rec.APIAppID)
		}
		bound := rec
		out.byAppID[rec.APIAppID] = &bound
	}
	return out, nil
}

// Get resolves the registered record for an api_app_id, or (nil, false).
func (a *AgentApps) Get(appID string) (*AgentApp, bool) {
	if a == nil || appID == "" {
		return nil, false
	}
	rec, ok := a.byAppID[appID]
	return rec, ok
}

// SecretFor returns the registered signing secret bound to api_app_id, if any.
func (a *AgentApps) SecretFor(appID string) (string, bool) {
	rec, ok := a.Get(appID)
	if !ok {
		return "", false
	}
	return rec.SigningSecret, true
}

// SigningSecrets returns every registered signing secret, in api_app_id order
// (deterministic). Used by the url_verification trial and by the legacy
// trial-fallback's rejection check (a legacy trial that matches a REGISTERED
// agent secret must be rejected — registration is strict, permanently).
func (a *AgentApps) SigningSecrets() []string {
	if a == nil {
		return nil
	}
	ids := make([]string, 0, len(a.byAppID))
	for id := range a.byAppID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, a.byAppID[id].SigningSecret)
	}
	return out
}

// isRegisteredSecret reports whether secret is the signing secret of any
// registered agent app. Used by the legacy verification fallback to reject a
// trial match on a secret that opted into strict binding via registration.
func (a *AgentApps) isRegisteredSecret(secret string) bool {
	if a == nil || secret == "" {
		return false
	}
	for _, rec := range a.byAppID {
		if rec.SigningSecret == secret {
			return true
		}
	}
	return false
}

// Len returns the number of registered agent apps.
func (a *AgentApps) Len() int {
	if a == nil {
		return 0
	}
	return len(a.byAppID)
}

// JoinWarnings returns one warning per registered api_app_id that has no
// directory agent with that app_id — the app is registered (its signature
// verifies) but admits nothing because no owner agent joins. Deterministic
// order for stable logs / healthz. A nil directory makes every registered app
// unjoined.
func (a *AgentApps) JoinWarnings(dir *CompanyDirectory) []string {
	if a == nil {
		return nil
	}
	ids := make([]string, 0, len(a.byAppID))
	for id := range a.byAppID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var warnings []string
	for _, id := range ids {
		if _, ok := dir.AgentByAppID(id); !ok {
			warnings = append(warnings, fmt.Sprintf("registered agent app api_app_id=%q joins no directory agent; admits nothing", id))
		}
	}
	return warnings
}

// agentAppsStore is the in-memory snapshot holder for agent_apps.json. It
// shares the company directory's never-fatal load contract: an invalid file
// logs and installs a nil snapshot; a bad reload keeps the last-known-good.
type agentAppsStore struct {
	mu   sync.RWMutex
	snap *AgentApps
}

// Snapshot returns the current agent-apps snapshot, or nil when none is loaded.
func (s *agentAppsStore) Snapshot() *AgentApps {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap
}

func (s *agentAppsStore) set(a *AgentApps) {
	s.mu.Lock()
	s.snap = a
	s.mu.Unlock()
}

func (s *agentAppsStore) parse(path string) (*AgentApps, error) {
	data, exists, err := readSecretRegistryBytes(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return ParseAgentApps(data)
}

// readSecretRegistryBytes reads the SECRET-bearing agent_apps.json with the same
// symlink-refusing open and size cap as the non-secret registries, PLUS a
// secret-at-rest guard: a file whose mode carries any group/world bit (0o077) is
// refused, mirroring the Python register verb's 0o077 refusal
// (slack_register_agent_app.py) and the bot-token loader's exactPerm. The check
// is fstat-on-the-open-fd (not a pre-open Lstat) so it validates the inode
// actually read. A refusal is surfaced as a load error, which the never-fatal
// store contract turns into a nil snapshot + WARN — the write path (Python) and
// the read path (Go) now both treat a loosened signing-secret store as unsafe.
// An absent file returns exists=false with a nil error. Used ONLY for the
// secret registry, never the non-secret directory/bindings files.
func readSecretRegistryBytes(path string) (data []byte, exists bool, err error) {
	if path == "" {
		return nil, false, nil
	}
	f, err := openRegistryFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	finfo, err := f.Stat()
	if err != nil {
		return nil, false, fmt.Errorf("stat %s: %w", path, err)
	}
	if perm := finfo.Mode().Perm(); perm&0o077 != 0 {
		return nil, false, fmt.Errorf(
			"secret registry %s is group/world-accessible (mode %04o); it must be 0600 — "+
				"a possible signing-secret leak, fix the permissions and rotate the affected "+
				"secrets before reload", path, perm)
	}
	data, err = io.ReadAll(io.LimitReader(f, maxRegistryBytes+1))
	if err != nil {
		return nil, false, fmt.Errorf("read %s: %w", path, err)
	}
	if int64(len(data)) > maxRegistryBytes {
		return nil, false, fmt.Errorf("company registry file %s exceeds %d bytes", path, maxRegistryBytes)
	}
	return data, true, nil
}

// Load installs the agent-apps snapshot at startup. Never fatal: an invalid
// file logs the error, installs a nil snapshot, and returns the error for
// surfacing; an absent file installs a nil snapshot with no error. Directory-
// join warnings (computed against dir) are logged.
func (s *agentAppsStore) Load(path string, dir *CompanyDirectory) error {
	a, err := s.parse(path)
	if err != nil {
		log.Printf("WARN: agent apps: load from %q failed: %v; installing nil snapshot", path, err)
		s.set(nil)
		return err
	}
	for _, w := range a.JoinWarnings(dir) {
		log.Printf("WARN: agent apps: %s", w)
	}
	s.set(a)
	return nil
}

// StageReload re-reads agent_apps.json on SIGHUP. A malformed or invalid file
// retains the last-known-good snapshot; a valid file (including absent) is
// installed. Directory-join warnings are logged.
func (s *agentAppsStore) StageReload(path string, dir *CompanyDirectory) error {
	a, err := s.parse(path)
	if err != nil {
		log.Printf("WARN: agent apps: reload from %q failed: %v; retaining last-known-good snapshot", path, err)
		return err
	}
	for _, w := range a.JoinWarnings(dir) {
		log.Printf("WARN: agent apps: %s", w)
	}
	s.set(a)
	return nil
}
