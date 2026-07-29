package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func marshalBindings(t *testing.T, f companyBindingsFile) []byte {
	t.Helper()
	data, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal bindings: %v", err)
	}
	return data
}

func testDirectory(t *testing.T) *CompanyDirectory {
	t.Helper()
	dir, err := ParseCompanyDirectory(marshalDirectory(t, baseDirectoryFile()))
	if err != nil {
		t.Fatalf("build test directory: %v", err)
	}
	return dir
}

func baseBindingsFile() companyBindingsFile {
	return companyBindingsFile{
		SchemaVersion: 1,
		Bindings: []CompanyBinding{
			{Room: "orchestrator-team", Agent: "ollie", Session: "ollie-main"},
			{Room: "orchestrator-team", Agent: "riley", Session: "riley-main"},
		},
	}
}

func TestParseCompanyBindingsValid(t *testing.T) {
	dir := testDirectory(t)
	b, warnings, err := ParseCompanyBindings(marshalBindings(t, baseBindingsFile()), dir)
	if err != nil {
		t.Fatalf("ParseCompanyBindings: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("warnings = %v, want none", warnings)
	}
	if got, ok := b.SessionFor("orchestrator-team", "ollie"); !ok || got != "ollie-main" {
		t.Errorf("SessionFor(ollie) = (%q, %v), want ollie-main", got, ok)
	}
	if got, ok := b.SessionFor("orchestrator-team", "riley"); !ok || got != "riley-main" {
		t.Errorf("SessionFor(riley) = (%q, %v), want riley-main", got, ok)
	}
	if _, ok := b.SessionFor("orchestrator-team", "ghost"); ok {
		t.Error("SessionFor(ghost) ok=true, want false")
	}
}

func TestParseCompanyBindingsEmptyValid(t *testing.T) {
	dir := testDirectory(t)
	b, warnings, err := ParseCompanyBindings(marshalBindings(t, companyBindingsFile{SchemaVersion: 1}), dir)
	if err != nil {
		t.Fatalf("ParseCompanyBindings(empty): %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("warnings = %v, want none", warnings)
	}
	if _, ok := b.SessionFor("orchestrator-team", "ollie"); ok {
		t.Error("SessionFor on empty bindings ok=true")
	}
}

func TestParseCompanyBindingsValidation(t *testing.T) {
	dir := testDirectory(t)
	tests := []struct {
		name    string
		mutate  func(f *companyBindingsFile)
		wantErr bool
	}{
		{"unsupported schema_version", func(f *companyBindingsFile) {
			f.SchemaVersion = 2
		}, true},
		{"missing room", func(f *companyBindingsFile) {
			f.Bindings[0].Room = ""
		}, true},
		{"missing agent", func(f *companyBindingsFile) {
			f.Bindings[0].Agent = ""
		}, true},
		{"missing session", func(f *companyBindingsFile) {
			f.Bindings[0].Session = ""
		}, true},
		{"duplicate (room, agent) pair", func(f *companyBindingsFile) {
			f.Bindings = append(f.Bindings, CompanyBinding{Room: "orchestrator-team", Agent: "ollie", Session: "ollie-alt"})
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := baseBindingsFile()
			tt.mutate(&f)
			_, _, err := ParseCompanyBindings(marshalBindings(t, f), dir)
			if tt.wantErr && err == nil {
				t.Fatalf("ParseCompanyBindings: err=nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ParseCompanyBindings: err=%v, want nil", err)
			}
		})
	}
}

func TestParseCompanyBindingsCorruptJSON(t *testing.T) {
	dir := testDirectory(t)
	if _, _, err := ParseCompanyBindings([]byte("{not json"), dir); err == nil {
		t.Fatal("ParseCompanyBindings on corrupt JSON: err=nil, want error")
	}
}

// TestParseCompanyBindingsDropsUnknownRefs — a binding referencing a room
// or agent absent from the directory is dropped with a warning, not fatal.
func TestParseCompanyBindingsDropsUnknownRefs(t *testing.T) {
	dir := testDirectory(t)
	f := baseBindingsFile()
	f.Bindings = append(f.Bindings,
		CompanyBinding{Room: "ghost-room", Agent: "ollie", Session: "s1"},
		CompanyBinding{Room: "orchestrator-team", Agent: "ghost-agent", Session: "s2"},
	)
	b, warnings, err := ParseCompanyBindings(marshalBindings(t, f), dir)
	if err != nil {
		t.Fatalf("ParseCompanyBindings: %v", err)
	}
	if len(warnings) != 2 {
		t.Fatalf("warnings = %v, want 2", warnings)
	}
	// The two valid bindings survive.
	if _, ok := b.SessionFor("orchestrator-team", "ollie"); !ok {
		t.Error("valid binding dropped")
	}
	// The dropped ones are absent.
	if _, ok := b.SessionFor("ghost-room", "ollie"); ok {
		t.Error("unknown-room binding was kept")
	}
	if _, ok := b.SessionFor("orchestrator-team", "ghost-agent"); ok {
		t.Error("unknown-agent binding was kept")
	}
}

// TestParseCompanyBindingsNilDirectoryDropsAll — with no directory loaded
// every binding references a missing room and drops with a warning; the
// result is valid but empty.
func TestParseCompanyBindingsNilDirectoryDropsAll(t *testing.T) {
	b, warnings, err := ParseCompanyBindings(marshalBindings(t, baseBindingsFile()), nil)
	if err != nil {
		t.Fatalf("ParseCompanyBindings(nil dir): %v", err)
	}
	if len(warnings) != 2 {
		t.Fatalf("warnings = %v, want 2", warnings)
	}
	if _, ok := b.SessionFor("orchestrator-team", "ollie"); ok {
		t.Error("binding kept with nil directory")
	}
}

// TestParseCompanyBindingsDuplicateBeatsUnknown — the singleton invariant
// is structural: a duplicate (room, agent) fails closed even when the pair
// does not resolve in the directory.
func TestParseCompanyBindingsDuplicateBeatsUnknown(t *testing.T) {
	dir := testDirectory(t)
	f := companyBindingsFile{
		SchemaVersion: 1,
		Bindings: []CompanyBinding{
			{Room: "ghost-room", Agent: "ollie", Session: "s1"},
			{Room: "ghost-room", Agent: "ollie", Session: "s2"},
		},
	}
	if _, _, err := ParseCompanyBindings(marshalBindings(t, f), dir); err == nil {
		t.Fatal("duplicate unknown-room binding: err=nil, want error")
	}
}

func TestCompanyBindingsStoreLoadMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "company_bindings.json")
	dir := testDirectory(t)
	var store companyBindingsStore
	if err := store.Load(path, dir); err != nil {
		t.Fatalf("Load(missing) err = %v, want nil", err)
	}
	if store.Snapshot() != nil {
		t.Error("Snapshot non-nil for missing file")
	}
}

func TestCompanyBindingsStoreLoadCorruptNeverFatal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "company_bindings.json")
	dir := testDirectory(t)
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("seed corrupt: %v", err)
	}
	var store companyBindingsStore
	if err := store.Load(path, dir); err == nil {
		t.Fatal("Load(corrupt) err = nil, want surfaced error")
	}
	if store.Snapshot() != nil {
		t.Error("Snapshot non-nil after corrupt load")
	}
	if err := os.WriteFile(path, marshalBindings(t, baseBindingsFile()), 0o600); err != nil {
		t.Fatalf("seed valid: %v", err)
	}
	if err := store.StageReload(path, dir); err != nil {
		t.Fatalf("StageReload(valid): %v", err)
	}
	if store.Snapshot() == nil {
		t.Error("Snapshot nil after valid reload following corrupt load")
	}
}

func TestCompanyBindingsStoreReloadKeepsLastKnownGood(t *testing.T) {
	path := filepath.Join(t.TempDir(), "company_bindings.json")
	dir := testDirectory(t)
	if err := os.WriteFile(path, marshalBindings(t, baseBindingsFile()), 0o600); err != nil {
		t.Fatalf("seed valid: %v", err)
	}
	var store companyBindingsStore
	if err := store.Load(path, dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	good := store.Snapshot()
	if good == nil {
		t.Fatal("Snapshot nil after valid load")
	}
	if err := os.WriteFile(path, []byte("{bad"), 0o600); err != nil {
		t.Fatalf("seed corrupt: %v", err)
	}
	if err := store.StageReload(path, dir); err == nil {
		t.Fatal("StageReload(corrupt) err = nil, want error")
	}
	if store.Snapshot() != good {
		t.Error("Snapshot changed on bad reload; want last-known-good retained")
	}
	if _, ok := store.Snapshot().SessionFor("orchestrator-team", "ollie"); !ok {
		t.Error("last-known-good bindings lost after bad reload")
	}
}
