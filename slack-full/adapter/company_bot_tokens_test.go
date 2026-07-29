package main

import (
	"os"
	"path/filepath"
	"testing"
)

// writeTokenFile writes a bot-token-<agent>.txt under a 0700 secrets dir with
// the given file mode and returns the secrets dir.
func writeTokenFile(t *testing.T, agent, token string, dirMode, fileMode os.FileMode) string {
	t.Helper()
	dir := t.TempDir()
	sdir := filepath.Join(dir, "secrets")
	if err := os.Mkdir(sdir, dirMode); err != nil {
		t.Fatalf("mkdir secrets: %v", err)
	}
	// t.TempDir cleanup needs traversable dirs.
	t.Cleanup(func() { _ = os.Chmod(sdir, 0o700) })
	path := filepath.Join(sdir, "bot-token-"+agent+".txt")
	if err := os.WriteFile(path, []byte(token), fileMode); err != nil {
		t.Fatalf("write token: %v", err)
	}
	if err := os.Chmod(path, fileMode); err != nil {
		t.Fatalf("chmod token: %v", err)
	}
	if err := os.Chmod(sdir, dirMode); err != nil {
		t.Fatalf("chmod secrets: %v", err)
	}
	return sdir
}

func TestLoadOwnerBotTokenHappy(t *testing.T) {
	sdir := writeTokenFile(t, "ollie", "  xoxb-ollie-token\n", 0o700, 0o600)
	got, err := loadOwnerBotToken(sdir, "ollie")
	if err != nil {
		t.Fatalf("loadOwnerBotToken: %v", err)
	}
	if got != "xoxb-ollie-token" {
		t.Errorf("token = %q, want trimmed xoxb-ollie-token", got)
	}
}

func TestLoadOwnerBotTokenRefusals(t *testing.T) {
	t.Run("absent file", func(t *testing.T) {
		sdir := writeTokenFile(t, "ollie", "x", 0o700, 0o600)
		if _, err := loadOwnerBotToken(sdir, "riley"); err == nil {
			t.Error("missing token file must error")
		}
	})
	t.Run("dir not 0700", func(t *testing.T) {
		sdir := writeTokenFile(t, "ollie", "x", 0o755, 0o600)
		if _, err := loadOwnerBotToken(sdir, "ollie"); err == nil {
			t.Error("lax secrets dir mode must be refused")
		}
	})
	t.Run("file not 0600", func(t *testing.T) {
		sdir := writeTokenFile(t, "ollie", "x", 0o700, 0o644)
		if _, err := loadOwnerBotToken(sdir, "ollie"); err == nil {
			t.Error("lax token file mode must be refused")
		}
	})
	t.Run("empty token", func(t *testing.T) {
		sdir := writeTokenFile(t, "ollie", "   \n", 0o700, 0o600)
		if _, err := loadOwnerBotToken(sdir, "ollie"); err == nil {
			t.Error("empty token must be refused")
		}
	})
	t.Run("symlink token", func(t *testing.T) {
		sdir := writeTokenFile(t, "ollie", "x", 0o700, 0o600)
		real := filepath.Join(t.TempDir(), "real-token")
		if err := os.WriteFile(real, []byte("sneaky"), 0o600); err != nil {
			t.Fatalf("write real: %v", err)
		}
		link := filepath.Join(sdir, "bot-token-riley.txt")
		if err := os.Symlink(real, link); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		if _, err := loadOwnerBotToken(sdir, "riley"); err == nil {
			t.Error("symlinked token file must be refused")
		}
	})
	t.Run("non-slug agent rejected before path join", func(t *testing.T) {
		sdir := writeTokenFile(t, "ollie", "x", 0o700, 0o600)
		if _, err := loadOwnerBotToken(sdir, "../etc/passwd"); err == nil {
			t.Error("hostile agent name must be refused")
		}
	})
	t.Run("empty agent", func(t *testing.T) {
		if _, err := loadOwnerBotToken(t.TempDir(), ""); err == nil {
			t.Error("empty agent must be refused")
		}
	})
}
