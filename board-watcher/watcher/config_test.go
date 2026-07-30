package main

import (
	"strings"
	"testing"
	"time"
)

// fakeEnv returns a getenv func over a map.
func fakeEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func baseEnv() map[string]string {
	return map[string]string{
		"BOARD_WATCHER_CITY":       "citadel",
		"BOARD_WATCHER_MODE":       "local-path",
		"BOARD_WATCHER_BOARD_PATH": "/share/coordination",
		"GC_CITY_PATH":             "/cities/citadel",
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg, err := loadConfigFromEnv(fakeEnv(baseEnv()))
	if err != nil {
		t.Fatalf("loadConfigFromEnv: %v", err)
	}
	if cfg.pollInterval != 60*time.Second {
		t.Errorf("pollInterval = %s, want 60s", cfg.pollInterval)
	}
	if cfg.debounce != 60*time.Second {
		t.Errorf("debounce = %s, want 60s", cfg.debounce)
	}
	if cfg.mailTo != "mayor" {
		t.Errorf("mailTo = %q, want mayor", cfg.mailTo)
	}
	if cfg.gcBin != "gc" {
		t.Errorf("gcBin = %q, want gc", cfg.gcBin)
	}
	if cfg.stateDir != "/cities/citadel/.gc/board-watcher" {
		t.Errorf("stateDir = %q", cfg.stateDir)
	}
	if cfg.snapshotPath() != "/cities/citadel/.gc/board-watcher/snapshot.json" {
		t.Errorf("snapshotPath = %q", cfg.snapshotPath())
	}
	if cfg.ledgerPath() != "/cities/citadel/.gc/board-watcher/own-writes.jsonl" {
		t.Errorf("ledgerPath = %q", cfg.ledgerPath())
	}
	if cfg.boardDescr() != "/share/coordination" {
		t.Errorf("boardDescr = %q", cfg.boardDescr())
	}
}

func TestConfigMissingRequired(t *testing.T) {
	_, err := loadConfigFromEnv(fakeEnv(map[string]string{}))
	if err == nil {
		t.Fatal("want error for empty env")
	}
	for _, key := range []string{"BOARD_WATCHER_CITY", "BOARD_WATCHER_MODE", "BOARD_WATCHER_BOARD_PATH"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q does not name %s", err, key)
		}
	}
}

func TestConfigRemotePollRequiresTarget(t *testing.T) {
	env := baseEnv()
	env["BOARD_WATCHER_MODE"] = "remote-poll"
	if _, err := loadConfigFromEnv(fakeEnv(env)); err == nil {
		t.Fatal("want error when remote-poll has no ssh target")
	}
	env["BOARD_WATCHER_SSH_TARGET"] = "afik@citadel"
	cfg, err := loadConfigFromEnv(fakeEnv(env))
	if err != nil {
		t.Fatalf("loadConfigFromEnv: %v", err)
	}
	if cfg.boardDescr() != "afik@citadel:/share/coordination" {
		t.Errorf("boardDescr = %q", cfg.boardDescr())
	}
}

func TestConfigRejectsOptionLookingTarget(t *testing.T) {
	env := baseEnv()
	env["BOARD_WATCHER_MODE"] = "remote-poll"
	env["BOARD_WATCHER_SSH_TARGET"] = "-oProxyCommand=evil"
	if _, err := loadConfigFromEnv(fakeEnv(env)); err == nil {
		t.Fatal("want error for '-'-prefixed ssh target")
	}
}

func TestConfigRejectsUnknownMode(t *testing.T) {
	env := baseEnv()
	env["BOARD_WATCHER_MODE"] = "fsevents"
	if _, err := loadConfigFromEnv(fakeEnv(env)); err == nil {
		t.Fatal("want error for unknown mode")
	}
}

func TestConfigDurations(t *testing.T) {
	env := baseEnv()
	env["BOARD_WATCHER_POLL_INTERVAL"] = "90s"
	env["BOARD_WATCHER_DEBOUNCE"] = "2m"
	cfg, err := loadConfigFromEnv(fakeEnv(env))
	if err != nil {
		t.Fatalf("loadConfigFromEnv: %v", err)
	}
	if cfg.pollInterval != 90*time.Second || cfg.debounce != 2*time.Minute {
		t.Errorf("got poll=%s debounce=%s", cfg.pollInterval, cfg.debounce)
	}

	env["BOARD_WATCHER_POLL_INTERVAL"] = "sixty"
	if _, err := loadConfigFromEnv(fakeEnv(env)); err == nil {
		t.Fatal("want error for unparseable duration")
	}
	env["BOARD_WATCHER_POLL_INTERVAL"] = "1s"
	if _, err := loadConfigFromEnv(fakeEnv(env)); err == nil {
		t.Fatal("want error for sub-minimum poll interval")
	}
}

func TestConfigStateDirOverrideAndRequirement(t *testing.T) {
	env := baseEnv()
	env["BOARD_WATCHER_STATE_DIR"] = "/elsewhere/state"
	cfg, err := loadConfigFromEnv(fakeEnv(env))
	if err != nil {
		t.Fatalf("loadConfigFromEnv: %v", err)
	}
	if cfg.stateDir != "/elsewhere/state" {
		t.Errorf("stateDir = %q", cfg.stateDir)
	}

	delete(env, "BOARD_WATCHER_STATE_DIR")
	delete(env, "GC_CITY_PATH")
	if _, err := loadConfigFromEnv(fakeEnv(env)); err == nil {
		t.Fatal("want error when neither GC_CITY_PATH nor BOARD_WATCHER_STATE_DIR is set")
	}
}
