package main

import (
	"encoding/json"
	"testing"
)

func marshalDMBindings(t *testing.T, f dmBindingsFile) []byte {
	t.Helper()
	data, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal dm bindings: %v", err)
	}
	return data
}

func baseDMBindingsFile() dmBindingsFile {
	return dmBindingsFile{
		SchemaVersion: 1,
		DMBindings: []DMBinding{
			{Agent: "ollie", Session: "ollie"},
			{Agent: "riley", Session: "riley-main", City: "riley-city"},
		},
	}
}

func TestParseDMBindingsValid(t *testing.T) {
	dir := testDirectory(t)
	b, warnings, err := ParseDMBindings(marshalDMBindings(t, baseDMBindingsFile()), dir)
	if err != nil {
		t.Fatalf("ParseDMBindings: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("warnings = %v, want none", warnings)
	}
	bd, ok := b.BindingFor("ollie")
	if !ok || bd.Session != "ollie" || bd.City != "" {
		t.Errorf("BindingFor(ollie) = (%+v, %v)", bd, ok)
	}
	bd, ok = b.BindingFor("riley")
	if !ok || bd.Session != "riley-main" || bd.City != "riley-city" {
		t.Errorf("BindingFor(riley) = (%+v, %v)", bd, ok)
	}
	if _, ok := b.BindingFor("ghost"); ok {
		t.Error("BindingFor(ghost) ok=true, want false")
	}
}

func TestParseDMBindingsValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(f *dmBindingsFile)
		wantErr bool
	}{
		{"valid", func(f *dmBindingsFile) {}, false},
		{"empty valid", func(f *dmBindingsFile) { f.DMBindings = nil }, false},
		{"bad schema", func(f *dmBindingsFile) { f.SchemaVersion = 9 }, true},
		{"missing agent", func(f *dmBindingsFile) { f.DMBindings[0].Agent = "" }, true},
		{"missing session", func(f *dmBindingsFile) { f.DMBindings[0].Session = "" }, true},
		// A duplicate agent now warns-and-drops (keep first) rather than failing
		// closed, so Go and Python interpret the same bytes identically (m10).
		{"duplicate agent singleton", func(f *dmBindingsFile) { f.DMBindings[1].Agent = "ollie" }, false},
		{"url-significant city", func(f *dmBindingsFile) { f.DMBindings[0].City = "a/b" }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := baseDMBindingsFile()
			tc.mutate(&f)
			_, _, err := ParseDMBindings(marshalDMBindings(t, f), testDirectory(t))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

// TestParseDMBindingsDuplicateAgentKeepsFirst pins the m10 symmetry: a duplicate
// agent keeps the FIRST binding and warns (no error, no feature-dark nil
// snapshot), matching the Python reader's first-row-wins behavior.
func TestParseDMBindingsDuplicateAgentKeepsFirst(t *testing.T) {
	dir := testDirectory(t)
	f := dmBindingsFile{
		SchemaVersion: 1,
		DMBindings: []DMBinding{
			{Agent: "ollie", Session: "ollie-first"},
			{Agent: "ollie", Session: "ollie-second"},
		},
	}
	b, warnings, err := ParseDMBindings(marshalDMBindings(t, f), dir)
	if err != nil {
		t.Fatalf("ParseDMBindings: %v", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one (duplicate dropped)", warnings)
	}
	if bd, ok := b.BindingFor("ollie"); !ok || bd.Session != "ollie-first" {
		t.Errorf("BindingFor(ollie) = (%+v, %v), want first binding kept", bd, ok)
	}
}

// TestParseDMBindingsSharedSessionKeepsFirst pins m5: two DIFFERENT agents bound
// to the same running session (alias-equivalent by the dot/dunder rule) keep the
// first claimant and warn — the reader backstops a hand-edited file so a shared
// session can never let two agents reply as each other.
func TestParseDMBindingsSharedSessionKeepsFirst(t *testing.T) {
	dir := testDirectory(t)
	f := dmBindingsFile{
		SchemaVersion: 1,
		DMBindings: []DMBinding{
			{Agent: "ollie", Session: "a.b"},  // config form
			{Agent: "riley", Session: "a__b"}, // gc-runtime form of the SAME session
		},
	}
	b, warnings, err := ParseDMBindings(marshalDMBindings(t, f), dir)
	if err != nil {
		t.Fatalf("ParseDMBindings: %v", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one (shared-session claimant dropped)", warnings)
	}
	if _, ok := b.BindingFor("ollie"); !ok {
		t.Error("first claimant ollie dropped")
	}
	if _, ok := b.BindingFor("riley"); ok {
		t.Error("second agent riley bound to a session ollie already claims")
	}
}

func TestParseDMBindingsDropsUnknownAgent(t *testing.T) {
	dir := testDirectory(t)
	f := dmBindingsFile{
		SchemaVersion: 1,
		DMBindings: []DMBinding{
			{Agent: "ollie", Session: "ollie"},
			{Agent: "ghost", Session: "ghost-session"},
		},
	}
	b, warnings, err := ParseDMBindings(marshalDMBindings(t, f), dir)
	if err != nil {
		t.Fatalf("ParseDMBindings: %v", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one (ghost dropped)", warnings)
	}
	if _, ok := b.BindingFor("ghost"); ok {
		t.Error("ghost binding retained despite unknown agent")
	}
	if _, ok := b.BindingFor("ollie"); !ok {
		t.Error("ollie binding dropped")
	}
}

func TestDMBindingsNilSafe(t *testing.T) {
	var b *DMBindings
	if _, ok := b.BindingFor("ollie"); ok {
		t.Error("nil BindingFor ok=true")
	}
}
