package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// watchMode selects the listing backend — the one config knob the two
// cities disagree on. Everything downstream of the listing (diff,
// debounce, own-write filter, delivery) is identical in both modes.
type watchMode string

const (
	// modeLocalPath scans a board directory on local disk (citadel:
	// ~/city-share/coordination/). Also the mode a city runs against an
	// rsync-pull mirror if it prefers mirroring over remote polling.
	modeLocalPath watchMode = "local-path"
	// modeRemotePoll lists the board over ssh (boomtown: no local board
	// copy; mtime/size scan of citadel's board dir every poll).
	modeRemotePoll watchMode = "remote-poll"
)

const (
	defaultPollInterval   = 60 * time.Second
	defaultDebounce       = 60 * time.Second
	defaultMailTo         = "mayor"
	defaultGCBin          = "gc"
	defaultInternalListen = "127.0.0.1:8791"

	// minPollInterval guards against a typo'd sub-second interval turning
	// the ssh backend into a connection storm against the peer box.
	minPollInterval = 5 * time.Second

	// sshConnectTimeout bounds ssh connection setup. The whole remote
	// command additionally runs under the per-cycle context timeout.
	sshConnectTimeout = 10 * time.Second
)

// config holds the watcher's resolved runtime configuration.
type config struct {
	city           string        // this city's identity (logs + mail body)
	mode           watchMode     // local-path | remote-poll
	boardPath      string        // board dir: local path, or path on the ssh target
	sshTarget      string        // remote-poll only: ssh destination (user@host or ssh alias)
	pollInterval   time.Duration // snapshot-diff cadence
	debounce       time.Duration // per-file quiet window before a change settles
	mailTo         string        // gc mail recipient (default mayor)
	stateDir       string        // snapshot.json + own-writes.jsonl live here
	gcBin          string        // gc binary to exec for mail delivery
	cityPath       string        // GC_CITY_PATH; passed as gc --city
	serviceSocket  string        // GC_SERVICE_SOCKET (proxy_process mode)
	internalListen string        // TCP healthz bind when no service socket
}

func (c config) snapshotPath() string {
	return filepath.Join(c.stateDir, "snapshot.json")
}

func (c config) ledgerPath() string {
	return filepath.Join(c.stateDir, "own-writes.jsonl")
}

// boardDescr is the human-readable board locator used in logs and mail
// bodies.
func (c config) boardDescr() string {
	if c.mode == modeRemotePoll {
		return c.sshTarget + ":" + c.boardPath
	}
	return c.boardPath
}

// loadConfigFromEnv builds and validates a config from a getenv function.
// Split out so tests can supply a fake environment.
func loadConfigFromEnv(getenv func(string) string) (config, error) {
	envOr := func(key, fallback string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return fallback
	}
	cfg := config{
		city:           getenv("BOARD_WATCHER_CITY"),
		mode:           watchMode(getenv("BOARD_WATCHER_MODE")),
		boardPath:      getenv("BOARD_WATCHER_BOARD_PATH"),
		sshTarget:      getenv("BOARD_WATCHER_SSH_TARGET"),
		mailTo:         envOr("BOARD_WATCHER_MAIL_TO", defaultMailTo),
		gcBin:          envOr("BOARD_WATCHER_GC_BIN", defaultGCBin),
		cityPath:       getenv("GC_CITY_PATH"),
		serviceSocket:  getenv("GC_SERVICE_SOCKET"),
		internalListen: envOr("BOARD_WATCHER_LISTEN", defaultInternalListen),
	}

	var missing []string
	if cfg.city == "" {
		missing = append(missing, "BOARD_WATCHER_CITY")
	}
	if cfg.mode == "" {
		missing = append(missing, "BOARD_WATCHER_MODE")
	}
	if cfg.boardPath == "" {
		missing = append(missing, "BOARD_WATCHER_BOARD_PATH")
	}
	if len(missing) > 0 {
		return cfg, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}

	switch cfg.mode {
	case modeLocalPath:
		// sshTarget is ignored in local mode; tolerate it being set so a
		// city can flip modes by changing one variable.
	case modeRemotePoll:
		if cfg.sshTarget == "" {
			return cfg, errors.New("BOARD_WATCHER_MODE=remote-poll requires BOARD_WATCHER_SSH_TARGET")
		}
		// The target is passed to ssh as a positional argument; a leading
		// dash would be parsed as an ssh option.
		if strings.HasPrefix(cfg.sshTarget, "-") {
			return cfg, fmt.Errorf("BOARD_WATCHER_SSH_TARGET must not start with '-': %q", cfg.sshTarget)
		}
	default:
		return cfg, fmt.Errorf("BOARD_WATCHER_MODE must be %q or %q, got %q",
			modeLocalPath, modeRemotePoll, cfg.mode)
	}

	var err error
	if cfg.pollInterval, err = durationOr(getenv, "BOARD_WATCHER_POLL_INTERVAL", defaultPollInterval); err != nil {
		return cfg, err
	}
	if cfg.pollInterval < minPollInterval {
		return cfg, fmt.Errorf("BOARD_WATCHER_POLL_INTERVAL must be >= %s, got %s", minPollInterval, cfg.pollInterval)
	}
	if cfg.debounce, err = durationOr(getenv, "BOARD_WATCHER_DEBOUNCE", defaultDebounce); err != nil {
		return cfg, err
	}
	if cfg.debounce < 0 {
		return cfg, fmt.Errorf("BOARD_WATCHER_DEBOUNCE must be >= 0, got %s", cfg.debounce)
	}

	// The state directory holds the settled snapshot and the own-write
	// ledger. It defaults to <GC_CITY_PATH>/.gc/board-watcher when
	// GC_CITY_PATH is set; BOARD_WATCHER_STATE_DIR overrides it outright
	// (tests, or a deployment that stores state elsewhere).
	cfg.stateDir = getenv("BOARD_WATCHER_STATE_DIR")
	if cfg.stateDir == "" && cfg.cityPath != "" {
		cfg.stateDir = filepath.Join(cfg.cityPath, ".gc", "board-watcher")
	}
	if cfg.stateDir == "" {
		return cfg, errors.New("state directory is unset: set GC_CITY_PATH (preferred) or BOARD_WATCHER_STATE_DIR")
	}
	return cfg, nil
}

func durationOr(getenv func(string) string, key string, fallback time.Duration) (time.Duration, error) {
	v := getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid duration %q (want e.g. \"60s\", \"2m\")", key, v)
	}
	return d, nil
}
