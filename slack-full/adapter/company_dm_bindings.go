package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
)

// company_dm_bindings.go — the Phase 4 DM session registry (dm_bindings.json),
// a pack-local sibling of company_bindings.json. Each agent has at most one
// binding (singleton) mapping it to its DM-bound gc session. It rides the same
// read-validate-write mechanism and (session, city) shape as room bindings;
// the fabric's ConversationRef{Kind: dm} is the Phase 5 home (revisit marker:
// PHASE5-DM-FABRIC at this binding-registry seam).

// DMBinding is one singleton agent → session mapping (Phase 4). Exactly one
// per agent. City optionally targets a session in a DIFFERENT gc city than the
// adapter's own (mirroring room bindings); empty means the adapter's own city.
type DMBinding struct {
	Agent   string `json:"agent"`
	Session string `json:"session"`
	City    string `json:"city,omitempty"`
}

// dmBindingsFile is the on-disk JSON envelope for dm_bindings.json.
type dmBindingsFile struct {
	SchemaVersion int         `json:"schema_version"`
	DMBindings    []DMBinding `json:"dm_bindings"`
}

// DMBindings is a validated, normalized agent → binding index, immutable after
// parse and safe for concurrent reads.
type DMBindings struct {
	byAgent map[string]*DMBinding
}

// dmBindingsSchemaVersion is the only dm-bindings schema this build understands.
const dmBindingsSchemaVersion = 1

// ParseDMBindings decodes and validates dm_bindings.json against the current
// directory. Structural problems fail closed (an unsupported schema, a missing
// required field, a URL-significant city). Softer corruptions warn-and-drop so
// Go interprets the SAME bytes the Python reader does instead of going
// feature-dark (m10): a duplicate agent keeps the FIRST binding and warns (the
// singleton invariant — Python's _dm_session_for_agent likewise takes the first
// row); two DIFFERENT agents claiming the same running session (alias-equivalent
// by the gc dot/dunder rule) keep the FIRST claimant and warn (the writer guard
// prevents this, and the reader backstops a hand-edited file so a shared session
// can never make two agents reply as each other — m5); a binding referencing an
// agent absent from the directory is dropped with a warning (the directory may
// have shrunk). An empty document is valid. dir may be nil (routing disabled),
// in which case every binding drops as an unknown reference.
func ParseDMBindings(data []byte, dir *CompanyDirectory) (b *DMBindings, warnings []string, err error) {
	var file dmBindingsFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, nil, fmt.Errorf("decode dm bindings: %w", err)
	}
	if file.SchemaVersion != dmBindingsSchemaVersion {
		return nil, nil, fmt.Errorf("dm bindings: unsupported schema_version %d (want %d)", file.SchemaVersion, dmBindingsSchemaVersion)
	}
	out := &DMBindings{byAgent: make(map[string]*DMBinding, len(file.DMBindings))}
	seen := make(map[string]bool, len(file.DMBindings))
	sessionClaims := make(map[string]string, len(file.DMBindings)) // (city, canonical session) -> agent
	for i := range file.DMBindings {
		bd := file.DMBindings[i]
		if bd.Agent == "" || bd.Session == "" {
			return nil, nil, fmt.Errorf("dm bindings: binding[%d] missing agent/session (agent=%q session=%q)", i, bd.Agent, bd.Session)
		}
		// City is optional; when present it is interpolated into
		// /v0/city/{city}/... URLs, so URL-significant bytes fail closed
		// exactly like company bindings.
		if strings.ContainsAny(bd.City, "/?#% \t") {
			return nil, nil, fmt.Errorf("dm bindings: binding[%d] city %q contains URL-significant or whitespace characters", i, bd.City)
		}
		// Singleton per agent: keep the first binding, warn-and-drop the rest.
		if seen[bd.Agent] {
			warnings = append(warnings, fmt.Sprintf("dm binding: duplicate binding for agent %q; keeping the first, dropping the rest", bd.Agent))
			continue
		}
		seen[bd.Agent] = true
		if _, ok := dir.AgentByName(bd.Agent); !ok {
			warnings = append(warnings, fmt.Sprintf("dm binding for agent %q references unknown agent; dropped", bd.Agent))
			continue
		}
		// (session, city) singleton across agents, keyed on the gc-runtime
		// canonical session so alias-equivalent spellings collapse to one key.
		key := bd.City + "\x00" + canonicalSessionKey(bd.Session)
		if prior, taken := sessionClaims[key]; taken {
			warnings = append(warnings, fmt.Sprintf("dm binding for agent %q claims session %q already bound to agent %q; dropped", bd.Agent, bd.Session, prior))
			continue
		}
		sessionClaims[key] = bd.Agent
		bound := bd
		out.byAgent[bd.Agent] = &bound
	}
	return out, warnings, nil
}

// canonicalSessionKey collapses a session name to the form gc actually runs, so
// two alias-equivalent spellings compare equal. gc sanitizes a configured named
// session by replacing every dot with a double underscore (config "teams.it"
// runs as GC_SESSION_NAME "teams__it"), so a config-form "a.b" and a runtime-
// form "a__b" name the same session — the dot→dunder normalization is the shared
// cross-language collision key (Python cmd_bind_dm normalizes identically).
func canonicalSessionKey(session string) string {
	return strings.ReplaceAll(session, ".", "__")
}

// BindingFor returns the singleton DM binding for an agent, including its
// optional target city.
func (b *DMBindings) BindingFor(agent string) (*DMBinding, bool) {
	if b == nil {
		return nil, false
	}
	bd, ok := b.byAgent[agent]
	return bd, ok
}

// dmBindingsStore is the in-memory snapshot holder for the DM bindings,
// sharing the company bindings' never-fatal load contract (invalid file →
// nil snapshot; bad reload → last-known-good).
type dmBindingsStore struct {
	mu   sync.RWMutex
	snap *DMBindings
}

// Snapshot returns the current DM bindings snapshot, or nil when none is loaded.
func (s *dmBindingsStore) Snapshot() *DMBindings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap
}

func (s *dmBindingsStore) set(b *DMBindings) {
	s.mu.Lock()
	s.snap = b
	s.mu.Unlock()
}

func (s *dmBindingsStore) parse(path string, dir *CompanyDirectory) (*DMBindings, []string, error) {
	data, exists, err := readCompanyRegistryBytes(path)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, nil
	}
	return ParseDMBindings(data, dir)
}

// Load installs the DM bindings snapshot at startup. Never fatal: an invalid
// file logs the error, installs a nil snapshot, and returns the error for
// surfacing; an absent file installs a nil snapshot with no error. Drop
// warnings are logged.
func (s *dmBindingsStore) Load(path string, dir *CompanyDirectory) error {
	b, warnings, err := s.parse(path, dir)
	if err != nil {
		log.Printf("WARN: dm bindings: load from %q failed: %v; installing nil snapshot", path, err)
		s.set(nil)
		return err
	}
	for _, w := range warnings {
		log.Printf("WARN: dm bindings: %s", w)
	}
	s.set(b)
	return nil
}

// StageReload re-reads the DM bindings on SIGHUP. A malformed or invalid file
// retains the last-known-good snapshot; a valid file (including absent) is
// installed. Drop warnings are logged.
func (s *dmBindingsStore) StageReload(path string, dir *CompanyDirectory) error {
	b, warnings, err := s.parse(path, dir)
	if err != nil {
		log.Printf("WARN: dm bindings: reload from %q failed: %v; retaining last-known-good snapshot", path, err)
		return err
	}
	for _, w := range warnings {
		log.Printf("WARN: dm bindings: %s", w)
	}
	s.set(b)
	return nil
}
