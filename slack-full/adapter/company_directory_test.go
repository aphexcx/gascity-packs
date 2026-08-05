package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func marshalDirectory(t *testing.T, f companyDirectoryFile) []byte {
	t.Helper()
	data, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal directory: %v", err)
	}
	return data
}

// baseDirectoryFile is a minimal valid directory mirroring the doc's
// Directory Contract example (two agents, one room).
func baseDirectoryFile() companyDirectoryFile {
	return companyDirectoryFile{
		SchemaVersion: 1,
		SourceSHA256:  "deadbeef",
		ImportedAt:    "2026-07-17T00:00:00Z",
		Agents: []CompanyAgent{
			{Name: "ollie", AppID: "A0AAAAAA1", BotUserID: "U0AAAAAA1"},
			{Name: "riley", AppID: "A0AAAAAA2", BotUserID: "U0AAAAAA2"},
		},
		Rooms: []CompanyRoom{
			{
				Name:        "orchestrator-team",
				TeamID:      "T0AAAAAAA",
				ChannelID:   "C0AAAAAAA",
				Members:     []string{"ollie", "riley"},
				AmbientWake: []string{"ollie"},
				MentionWake: []string{"ollie", "riley"},
			},
		},
	}
}

func TestParseCompanyDirectoryValid(t *testing.T) {
	dir, err := ParseCompanyDirectory(marshalDirectory(t, baseDirectoryFile()))
	if err != nil {
		t.Fatalf("ParseCompanyDirectory: %v", err)
	}
	if got := len(dir.Agents()); got != 2 {
		t.Errorf("agents = %d, want 2", got)
	}
	if got := len(dir.Rooms()); got != 1 {
		t.Errorf("rooms = %d, want 1", got)
	}
	if dir.SourceSHA256 != "deadbeef" {
		t.Errorf("SourceSHA256 = %q, want deadbeef", dir.SourceSHA256)
	}
}

// TestParseCompanyDirectoryValidation is the table-driven port of every
// fail-closed validation rule in plan 1a / the Directory Contract.
func TestParseCompanyDirectoryValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(f *companyDirectoryFile)
		wantErr bool
	}{
		{"valid", func(f *companyDirectoryFile) {}, false},
		{"empty registries valid", func(f *companyDirectoryFile) {
			f.Agents = nil
			f.Rooms = nil
		}, false},
		{"unsupported schema_version", func(f *companyDirectoryFile) {
			f.SchemaVersion = 2
		}, true},
		{"missing schema_version", func(f *companyDirectoryFile) {
			f.SchemaVersion = 0
		}, true},
		{"agent name uppercase not slug", func(f *companyDirectoryFile) {
			f.Agents[0].Name = "Ollie"
		}, true},
		{"agent name with space not slug", func(f *companyDirectoryFile) {
			f.Agents[0].Name = "ollie two"
		}, true},
		{"agent name leading hyphen not slug", func(f *companyDirectoryFile) {
			f.Agents[0].Name = "-ollie"
		}, true},
		{"agent name underscore not slug", func(f *companyDirectoryFile) {
			f.Agents[0].Name = "ollie_two"
		}, true},
		// Slug-parity with the Python importer (companySlugRE is canonical):
		// underscore, trailing hyphen, and double hyphen are all rejected for
		// agent and room names.
		{"agent name underscore data_bot", func(f *companyDirectoryFile) {
			f.Agents[0].Name = "data_bot"
		}, true},
		{"agent name trailing hyphen", func(f *companyDirectoryFile) {
			f.Agents[0].Name = "room-"
		}, true},
		{"agent name double hyphen", func(f *companyDirectoryFile) {
			f.Agents[0].Name = "a--b"
		}, true},
		{"room name underscore data_bot", func(f *companyDirectoryFile) {
			f.Rooms[0].Name = "data_bot"
		}, true},
		{"room name trailing hyphen", func(f *companyDirectoryFile) {
			f.Rooms[0].Name = "room-"
		}, true},
		{"room name double hyphen", func(f *companyDirectoryFile) {
			f.Rooms[0].Name = "a--b"
		}, true},
		{"agent missing app_id", func(f *companyDirectoryFile) {
			f.Agents[0].AppID = ""
		}, true},
		{"agent missing bot_user_id", func(f *companyDirectoryFile) {
			f.Agents[0].BotUserID = ""
		}, true},
		{"duplicate agent name", func(f *companyDirectoryFile) {
			f.Agents[1].Name = "ollie"
		}, true},
		{"duplicate app_id", func(f *companyDirectoryFile) {
			f.Agents[1].AppID = "A0AAAAAA1"
		}, true},
		{"duplicate bot_user_id", func(f *companyDirectoryFile) {
			f.Agents[1].BotUserID = "U0AAAAAA1"
		}, true},
		{"room name not slug", func(f *companyDirectoryFile) {
			f.Rooms[0].Name = "Orchestrator Team"
		}, true},
		{"room missing team_id", func(f *companyDirectoryFile) {
			f.Rooms[0].TeamID = ""
		}, true},
		{"room missing channel_id", func(f *companyDirectoryFile) {
			f.Rooms[0].ChannelID = ""
		}, true},
		{"duplicate room name", func(f *companyDirectoryFile) {
			r := f.Rooms[0]
			r.ChannelID = "C0BBBBBBB"
			f.Rooms = append(f.Rooms, r)
		}, true},
		{"duplicate team+channel pair", func(f *companyDirectoryFile) {
			r := f.Rooms[0]
			r.Name = "second-room"
			f.Rooms = append(f.Rooms, r)
		}, true},
		{"members references unknown agent", func(f *companyDirectoryFile) {
			f.Rooms[0].Members = []string{"ollie", "ghost"}
		}, true},
		{"members wildcard survivor", func(f *companyDirectoryFile) {
			f.Rooms[0].Members = []string{"*"}
		}, true},
		{"mention_wake wildcard survivor", func(f *companyDirectoryFile) {
			f.Rooms[0].MentionWake = []string{"*"}
		}, true},
		{"ambient_wake wildcard survivor", func(f *companyDirectoryFile) {
			f.Rooms[0].AmbientWake = []string{"*"}
		}, true},
		{"ambient_wake not subset of members", func(f *companyDirectoryFile) {
			f.Rooms[0].Members = []string{"ollie"}
			f.Rooms[0].AmbientWake = []string{"riley"}
			f.Rooms[0].MentionWake = []string{"ollie"}
		}, true},
		{"mention_wake not subset of members", func(f *companyDirectoryFile) {
			f.Rooms[0].Members = []string{"ollie"}
			f.Rooms[0].AmbientWake = []string{"ollie"}
			f.Rooms[0].MentionWake = []string{"riley"}
		}, true},
		{"member listed twice", func(f *companyDirectoryFile) {
			f.Rooms[0].Members = []string{"ollie", "ollie", "riley"}
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := baseDirectoryFile()
			tt.mutate(&f)
			_, err := ParseCompanyDirectory(marshalDirectory(t, f))
			if tt.wantErr && err == nil {
				t.Fatalf("ParseCompanyDirectory: err=nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ParseCompanyDirectory: err=%v, want nil", err)
			}
		})
	}
}

func TestParseCompanyDirectoryCorruptJSON(t *testing.T) {
	if _, err := ParseCompanyDirectory([]byte("{not json")); err == nil {
		t.Fatal("ParseCompanyDirectory on corrupt JSON: err=nil, want error")
	}
}

func TestCompanyDirectoryLookups(t *testing.T) {
	dir, err := ParseCompanyDirectory(marshalDirectory(t, baseDirectoryFile()))
	if err != nil {
		t.Fatalf("ParseCompanyDirectory: %v", err)
	}

	room, ok := dir.RoomByChannel("T0AAAAAAA", "C0AAAAAAA")
	if !ok {
		t.Fatal("RoomByChannel ok=false for known room")
	}
	if room.Name != "orchestrator-team" {
		t.Errorf("room.Name = %q, want orchestrator-team", room.Name)
	}
	if _, ok := dir.RoomByChannel("T0AAAAAAA", "C_UNKNOWN"); ok {
		t.Error("RoomByChannel ok=true for unknown channel")
	}
	if _, ok := dir.RoomByChannel("T_WRONG", "C0AAAAAAA"); ok {
		t.Error("RoomByChannel matched on channel with wrong team")
	}

	agent, ok := dir.AgentByBotUserID("U0AAAAAA2")
	if !ok || agent.Name != "riley" {
		t.Errorf("AgentByBotUserID(U0AAAAAA2) = (%v, %v), want riley", agent, ok)
	}
	if _, ok := dir.AgentByBotUserID("U_UNKNOWN"); ok {
		t.Error("AgentByBotUserID ok=true for unknown id")
	}

	if a, ok := dir.AgentByName("ollie"); !ok || a.AppID != "A0AAAAAA1" {
		t.Errorf("AgentByName(ollie) = (%v, %v), want app A0AAAAAA1", a, ok)
	}
	if _, ok := dir.AgentByName("ghost"); ok {
		t.Error("AgentByName ok=true for unknown name")
	}

	if !dir.IsMember(room, "ollie") || !dir.IsMember(room, "riley") {
		t.Error("IsMember false for a listed member")
	}
	if dir.IsMember(room, "ghost") {
		t.Error("IsMember true for a non-member")
	}
	if !dir.IsMentionEligible(room, "riley") {
		t.Error("IsMentionEligible false for a mention_wake member")
	}

	// ollie is a member and ambient, but the room's mention_wake includes
	// ollie too, so use a directory where ollie is a member but not
	// mention-eligible to prove the eligibility gate bites.
	f := baseDirectoryFile()
	f.Rooms[0].MentionWake = []string{"riley"}
	dir2, err := ParseCompanyDirectory(marshalDirectory(t, f))
	if err != nil {
		t.Fatalf("ParseCompanyDirectory: %v", err)
	}
	room2, _ := dir2.RoomByChannel("T0AAAAAAA", "C0AAAAAAA")
	if dir2.IsMentionEligible(room2, "ollie") {
		t.Error("IsMentionEligible true for member absent from mention_wake")
	}
}

// TestCompanyDirectoryStoreLoadMissingFile — an absent directory is the
// normal disabled state: nil snapshot, no error, adapter keeps running.
func TestCompanyDirectoryStoreLoadMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "company_directory.json")
	var store companyDirectoryStore
	if err := store.Load(path); err != nil {
		t.Fatalf("Load(missing) err = %v, want nil", err)
	}
	if store.Snapshot() != nil {
		t.Error("Snapshot non-nil for missing file")
	}
}

// TestCompanyDirectoryStoreLoadCorruptNeverFatal — the deliberate
// divergence: a corrupt file at construction installs a nil snapshot and
// surfaces the error, and the process stays viable (a later valid reload
// recovers).
func TestCompanyDirectoryStoreLoadCorruptNeverFatal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "company_directory.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("seed corrupt: %v", err)
	}
	var store companyDirectoryStore
	err := store.Load(path)
	if err == nil {
		t.Fatal("Load(corrupt) err = nil, want surfaced error")
	}
	if store.Snapshot() != nil {
		t.Error("Snapshot non-nil after corrupt load; want nil (routing disabled)")
	}
	// Process viable: a subsequent valid reload installs a snapshot.
	if err := os.WriteFile(path, marshalDirectory(t, baseDirectoryFile()), 0o600); err != nil {
		t.Fatalf("seed valid: %v", err)
	}
	if err := store.StageReload(path); err != nil {
		t.Fatalf("StageReload(valid) err = %v", err)
	}
	if store.Snapshot() == nil {
		t.Error("Snapshot nil after valid reload following corrupt load")
	}
}

// TestCompanyDirectoryStoreReloadKeepsLastKnownGood — a malformed reload
// retains the previously-installed snapshot instead of blanking routing.
func TestCompanyDirectoryStoreReloadKeepsLastKnownGood(t *testing.T) {
	path := filepath.Join(t.TempDir(), "company_directory.json")
	if err := os.WriteFile(path, marshalDirectory(t, baseDirectoryFile()), 0o600); err != nil {
		t.Fatalf("seed valid: %v", err)
	}
	var store companyDirectoryStore
	if err := store.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
	good := store.Snapshot()
	if good == nil {
		t.Fatal("Snapshot nil after valid load")
	}

	if err := os.WriteFile(path, []byte("{bad"), 0o600); err != nil {
		t.Fatalf("seed corrupt: %v", err)
	}
	if err := store.StageReload(path); err == nil {
		t.Fatal("StageReload(corrupt) err = nil, want surfaced error")
	}
	if store.Snapshot() != good {
		t.Error("Snapshot changed on bad reload; want last-known-good retained")
	}
	if _, ok := store.Snapshot().RoomByChannel("T0AAAAAAA", "C0AAAAAAA"); !ok {
		t.Error("last-known-good directory lost its room after bad reload")
	}
}

// TestCompanyDirectoryStoreReloadRemovedFileDisables — deleting the file
// (an explicit removal, not a corruption) disables routing on reload.
func TestCompanyDirectoryStoreReloadRemovedFileDisables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "company_directory.json")
	if err := os.WriteFile(path, marshalDirectory(t, baseDirectoryFile()), 0o600); err != nil {
		t.Fatalf("seed valid: %v", err)
	}
	var store companyDirectoryStore
	if err := store.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if store.Snapshot() == nil {
		t.Fatal("Snapshot nil after valid load")
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := store.StageReload(path); err != nil {
		t.Fatalf("StageReload(removed) err = %v, want nil", err)
	}
	if store.Snapshot() != nil {
		t.Error("Snapshot non-nil after directory file removed; want nil")
	}
}

// TestCompanyDirectoryStoreRejectsSymlink — the shared symlink guard
// refuses a symlinked registry file.
func TestCompanyDirectoryStoreRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "real.json")
	if err := os.WriteFile(target, marshalDirectory(t, baseDirectoryFile()), 0o600); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	link := filepath.Join(dir, "company_directory.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	var store companyDirectoryStore
	if err := store.Load(link); err == nil {
		t.Fatal("Load(symlink) err = nil, want refusal")
	}
	if store.Snapshot() != nil {
		t.Error("Snapshot non-nil after symlink refusal")
	}
}
