package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
)

// CompanyBinding is one singleton (room, agent) → session mapping written
// by `gc slack bind-company-agent`. The company-bindings registry is the
// only thing that authorizes a woken agent to receive a session turn: a
// woken agent with no binding is a recorded delivery failure, never a
// legacy fallback.
type CompanyBinding struct {
	Room    string `json:"room"`
	Agent   string `json:"agent"`
	Session string `json:"session"`
	// City optionally targets a session in a DIFFERENT gc city than the
	// adapter's own (the live org runs one team per city; the switchboard
	// stays the single admission owner and delivers wakes cross-city).
	// Empty means the adapter's configured city.
	City string `json:"city,omitempty"`
}

// companyBindingsFile is the on-disk JSON envelope for
// company_bindings.json.
type companyBindingsFile struct {
	SchemaVersion int              `json:"schema_version"`
	Bindings      []CompanyBinding `json:"bindings"`
}

// CompanyBindings is a validated, normalized (room, agent) → session
// index. Like CompanyDirectory it is immutable after parse and safe for
// concurrent reads; the snapshot holder swaps whole values.
type CompanyBindings struct {
	byPair map[string]*CompanyBinding // companyBindingKey(room, agent) -> binding
}

// companyBindingsSchemaVersion is the only bindings schema this build
// understands.
const companyBindingsSchemaVersion = 1

func companyBindingKey(room, agent string) string {
	// NUL separates the fields so a room named "a" + agent "b:c" can never
	// collide with room "a:b" + agent "c".
	return room + "\x00" + agent
}

// ParseCompanyBindings decodes and validates company_bindings.json against
// the current directory. Structural problems fail closed (a duplicate
// (room, agent) pair, a missing required field, an unsupported schema); a
// binding that references a room or agent absent from the directory is
// dropped with a warning rather than failing the whole file, since the
// directory may have shrunk since the binding was written. An empty
// bindings document is valid. dir may be nil (routing disabled), in which
// case every binding drops as an unknown reference.
func ParseCompanyBindings(data []byte, dir *CompanyDirectory) (b *CompanyBindings, warnings []string, err error) {
	var file companyBindingsFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, nil, fmt.Errorf("decode company bindings: %w", err)
	}
	if file.SchemaVersion != companyBindingsSchemaVersion {
		return nil, nil, fmt.Errorf("company bindings: unsupported schema_version %d (want %d)", file.SchemaVersion, companyBindingsSchemaVersion)
	}

	out := &CompanyBindings{byPair: make(map[string]*CompanyBinding, len(file.Bindings))}
	seen := make(map[string]bool, len(file.Bindings))
	for i := range file.Bindings {
		bd := file.Bindings[i]
		if bd.Room == "" || bd.Agent == "" || bd.Session == "" {
			return nil, nil, fmt.Errorf("company bindings: binding[%d] missing room/agent/session (room=%q agent=%q session=%q)", i, bd.Room, bd.Agent, bd.Session)
		}
		// City is optional; when present it is interpolated into
		// /v0/city/{city}/... URLs, so URL-significant bytes fail closed
		// exactly like GC_CITY_NAME validation at startup.
		if strings.ContainsAny(bd.City, "/?#% \t") {
			return nil, nil, fmt.Errorf("company bindings: binding[%d] city %q contains URL-significant or whitespace characters", i, bd.City)
		}
		key := companyBindingKey(bd.Room, bd.Agent)
		// Singleton invariant: at most one binding per (room, agent). A
		// duplicate is a corrupt file and fails closed regardless of
		// whether the pair resolves in the directory.
		if seen[key] {
			return nil, nil, fmt.Errorf("company bindings: duplicate binding for (room=%q, agent=%q)", bd.Room, bd.Agent)
		}
		seen[key] = true
		if _, ok := dir.roomByName(bd.Room); !ok {
			warnings = append(warnings, fmt.Sprintf("binding (room=%q, agent=%q) references unknown room; dropped", bd.Room, bd.Agent))
			continue
		}
		if _, ok := dir.AgentByName(bd.Agent); !ok {
			warnings = append(warnings, fmt.Sprintf("binding (room=%q, agent=%q) references unknown agent; dropped", bd.Room, bd.Agent))
			continue
		}
		bound := bd
		out.byPair[key] = &bound
	}
	return out, warnings, nil
}

// SessionFor returns the singleton session bound to (room, agent).
func (b *CompanyBindings) SessionFor(room, agent string) (string, bool) {
	bd, ok := b.BindingFor(room, agent)
	if !ok {
		return "", false
	}
	return bd.Session, true
}

// BindingFor returns the full singleton binding for (room, agent),
// including its optional target city.
func (b *CompanyBindings) BindingFor(room, agent string) (*CompanyBinding, bool) {
	if b == nil {
		return nil, false
	}
	bd, ok := b.byPair[companyBindingKey(room, agent)]
	return bd, ok
}

// companyBindingsStore is the in-memory snapshot holder for the company
// bindings. It shares the company directory's never-fatal load contract:
// an invalid file logs and installs a nil snapshot; a bad reload keeps the
// last-known-good. Load/StageReload take the current directory because a
// binding's validity depends on it (a deliberate signature widening
// relative to the single-argument holders of the six atomic registries).
type companyBindingsStore struct {
	mu   sync.RWMutex
	snap *CompanyBindings
}

// Snapshot returns the current bindings snapshot, or nil when none is
// loaded.
func (s *companyBindingsStore) Snapshot() *CompanyBindings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap
}

func (s *companyBindingsStore) set(b *CompanyBindings) {
	s.mu.Lock()
	s.snap = b
	s.mu.Unlock()
}

func (s *companyBindingsStore) parse(path string, dir *CompanyDirectory) (*CompanyBindings, []string, error) {
	data, exists, err := readCompanyRegistryBytes(path)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, nil
	}
	return ParseCompanyBindings(data, dir)
}

// Load installs the bindings snapshot at startup. Never fatal: an invalid
// file logs the error, installs a nil snapshot, and returns the error for
// surfacing; an absent file installs a nil snapshot with no error. Drop
// warnings are logged.
func (s *companyBindingsStore) Load(path string, dir *CompanyDirectory) error {
	b, warnings, err := s.parse(path, dir)
	if err != nil {
		log.Printf("WARN: company bindings: load from %q failed: %v; installing nil snapshot", path, err)
		s.set(nil)
		return err
	}
	for _, w := range warnings {
		log.Printf("WARN: company bindings: %s", w)
	}
	s.set(b)
	return nil
}

// StageReload re-reads the bindings on SIGHUP, outside the six-registry
// atomic set. A malformed or invalid file retains the last-known-good
// snapshot; a valid file (including absent) is installed. Drop warnings are
// logged.
func (s *companyBindingsStore) StageReload(path string, dir *CompanyDirectory) error {
	b, warnings, err := s.parse(path, dir)
	if err != nil {
		log.Printf("WARN: company bindings: reload from %q failed: %v; retaining last-known-good snapshot", path, err)
		return err
	}
	for _, w := range warnings {
		log.Printf("WARN: company bindings: %s", w)
	}
	s.set(b)
	return nil
}
