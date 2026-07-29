package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func marshalAgentApps(t *testing.T, f agentAppsFile) []byte {
	t.Helper()
	data, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal agent apps: %v", err)
	}
	return data
}

func baseAgentAppsFile() agentAppsFile {
	return agentAppsFile{
		SchemaVersion: 1,
		AgentApps: []AgentApp{
			{TeamID: "T0AAAAAAA", APIAppID: "A0AAAAAA1", SigningSecret: "ollie-secret"},
			{TeamID: "T0AAAAAAA", APIAppID: "A0AAAAAA2", SigningSecret: "riley-secret"},
		},
	}
}

func TestParseAgentAppsValid(t *testing.T) {
	a, err := ParseAgentApps(marshalAgentApps(t, baseAgentAppsFile()))
	if err != nil {
		t.Fatalf("ParseAgentApps: %v", err)
	}
	if a.Len() != 2 {
		t.Fatalf("Len = %d, want 2", a.Len())
	}
	rec, ok := a.Get("A0AAAAAA1")
	if !ok || rec.SigningSecret != "ollie-secret" || rec.TeamID != "T0AAAAAAA" {
		t.Errorf("Get(A0AAAAAA1) = (%+v, %v)", rec, ok)
	}
	if sec, ok := a.SecretFor("A0AAAAAA2"); !ok || sec != "riley-secret" {
		t.Errorf("SecretFor(A0AAAAAA2) = (%q, %v)", sec, ok)
	}
	if _, ok := a.Get("A0NOPE"); ok {
		t.Error("Get(A0NOPE) ok=true, want false")
	}
	if !a.isRegisteredSecret("ollie-secret") || a.isRegisteredSecret("bogus") {
		t.Error("isRegisteredSecret misclassified")
	}
	got := a.SigningSecrets()
	if len(got) != 2 || got[0] != "ollie-secret" || got[1] != "riley-secret" {
		t.Errorf("SigningSecrets = %v (want sorted by api_app_id)", got)
	}
}

func TestParseAgentAppsValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(f *agentAppsFile)
		wantErr bool
	}{
		{"valid", func(f *agentAppsFile) {}, false},
		{"empty valid", func(f *agentAppsFile) { f.AgentApps = nil }, false},
		{"bad schema", func(f *agentAppsFile) { f.SchemaVersion = 2 }, true},
		{"missing schema", func(f *agentAppsFile) { f.SchemaVersion = 0 }, true},
		{"missing team_id", func(f *agentAppsFile) { f.AgentApps[0].TeamID = "" }, true},
		{"missing api_app_id", func(f *agentAppsFile) { f.AgentApps[0].APIAppID = "" }, true},
		{"missing signing_secret", func(f *agentAppsFile) { f.AgentApps[0].SigningSecret = "" }, true},
		{"duplicate api_app_id", func(f *agentAppsFile) { f.AgentApps[1].APIAppID = "A0AAAAAA1" }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := baseAgentAppsFile()
			tc.mutate(&f)
			_, err := ParseAgentApps(marshalAgentApps(t, f))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestAgentAppsJoinWarnings(t *testing.T) {
	dir := testDirectory(t) // agents ollie (A0AAAAAA1), riley (A0AAAAAA2)
	// One registered app joins a directory agent; one does not.
	f := agentAppsFile{
		SchemaVersion: 1,
		AgentApps: []AgentApp{
			{TeamID: "T0AAAAAAA", APIAppID: "A0AAAAAA1", SigningSecret: "s1"},
			{TeamID: "T0AAAAAAA", APIAppID: "A0GHOST", SigningSecret: "s2"},
		},
	}
	a, err := ParseAgentApps(marshalAgentApps(t, f))
	if err != nil {
		t.Fatalf("ParseAgentApps: %v", err)
	}
	warnings := a.JoinWarnings(dir)
	if len(warnings) != 1 {
		t.Fatalf("JoinWarnings = %v, want exactly one (A0GHOST)", warnings)
	}
	// A nil directory makes every registered app unjoined.
	if got := a.JoinWarnings(nil); len(got) != 2 {
		t.Errorf("JoinWarnings(nil) = %d, want 2", len(got))
	}
}

// TestAgentAppsStoreRefusesLoosePerms pins m4: the secret-bearing agent_apps.json
// is refused (nil snapshot) when its mode carries any group/world bit, mirroring
// the Python register verb's 0o077 refusal — the read path now guards the
// signing-secret store at rest just like the write path and the token files.
func TestAgentAppsStoreRefusesLoosePerms(t *testing.T) {
	dir := testDirectory(t)
	p := filepath.Join(t.TempDir(), "agent_apps.json")
	if err := os.WriteFile(p, marshalAgentApps(t, baseAgentAppsFile()), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	// 0600 loads cleanly.
	s := &agentAppsStore{}
	if err := s.Load(p, dir); err != nil {
		t.Fatalf("0600 load: %v", err)
	}
	if s.Snapshot().Len() != 2 {
		t.Fatalf("0600 snapshot len = %d, want 2", s.Snapshot().Len())
	}

	// Loosen to group-readable: Load refuses (returns error, installs nil).
	if err := os.Chmod(p, 0o640); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	s2 := &agentAppsStore{}
	if err := s2.Load(p, dir); err == nil {
		t.Error("group/world-accessible secret registry must be refused")
	}
	if s2.Snapshot() != nil {
		t.Error("refused secret registry must install a nil snapshot")
	}

	// A live store keeps last-known-good on a bad reload (StageReload contract).
	if err := s.StageReload(p, dir); err == nil {
		t.Error("StageReload must refuse loose perms")
	}
	if s.Snapshot().Len() != 2 {
		t.Error("StageReload refusal must retain last-known-good snapshot")
	}
}

func TestAgentAppsNilSafe(t *testing.T) {
	var a *AgentApps
	if _, ok := a.Get("x"); ok {
		t.Error("nil Get ok=true")
	}
	if a.Len() != 0 || a.SigningSecrets() != nil || a.isRegisteredSecret("x") {
		t.Error("nil AgentApps not fail-closed")
	}
	if a.JoinWarnings(testDirectory(t)) != nil {
		t.Error("nil JoinWarnings not nil")
	}
}
