// gc-board-watcher — cross-city coordination-board change nudger.
//
// Both cities run this one binary. It polls the shared coordination board
// with a snapshot-diff loop, debounces per-file changes, filters out the
// city's OWN writes via the own-writes hash ledger, and nudges the LOCAL
// mayor over `gc mail send --notify` when the PEER city adds, updates, or
// removes a board doc. No Slack, no funnel, no cross-city mail — the
// watcher exists precisely to keep board changes flowing when those are
// down.
//
// The one per-city difference is the listing backend (BOARD_WATCHER_MODE):
//
//	local-path   The board is a local directory (citadel:
//	             ~/city-share/coordination/, or any city running an
//	             rsync-pull mirror of the board).
//	remote-poll  No local board copy; the board directory is listed over
//	             ssh (boomtown polling citadel's disk).
//
// Everything else — diff, debounce, own-write filter, snapshot persistence,
// delivery — is identical in both modes. See ../README.md for the full
// design, config reference, and the own-writes ledger interop contract.
//
// Required env:
//
//	BOARD_WATCHER_CITY        This city's identity (e.g. "citadel").
//	BOARD_WATCHER_MODE        "local-path" or "remote-poll".
//	BOARD_WATCHER_BOARD_PATH  Board directory: local path (local-path) or
//	                          path on the ssh target (remote-poll).
//	BOARD_WATCHER_SSH_TARGET  remote-poll only: ssh destination
//	                          (user@host or an ssh_config alias). Must
//	                          connect non-interactively (BatchMode).
//	GC_CITY_PATH              On-disk city root. State defaults to
//	                          <GC_CITY_PATH>/.gc/board-watcher and gc is
//	                          invoked with --city <GC_CITY_PATH>.
//
// Controller-injected env (proxy_process mode):
//
//	GC_SERVICE_SOCKET         UDS path the /healthz listener binds.
//
// Optional env:
//
//	BOARD_WATCHER_POLL_INTERVAL  Poll cadence (default 60s; 60–90s
//	                             recommended for remote-poll).
//	BOARD_WATCHER_DEBOUNCE       Per-file quiet window before a change is
//	                             reported (default 60s).
//	BOARD_WATCHER_MAIL_TO        gc mail recipient (default "mayor").
//	BOARD_WATCHER_STATE_DIR      Override for the state directory.
//	BOARD_WATCHER_GC_BIN         gc binary to exec (default "gc").
//	BOARD_WATCHER_LISTEN         TCP healthz bind when GC_SERVICE_SOCKET
//	                             is unset (default 127.0.0.1:8791).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := loadConfigFromEnv(os.Getenv)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	var be backend
	switch cfg.mode {
	case modeLocalPath:
		be = &localBackend{dir: cfg.boardPath}
	case modeRemotePoll:
		be = &sshBackend{target: cfg.sshTarget, dir: cfg.boardPath, run: execRunner}
	}

	eng := newEngine(cfg, be, &mailNotifier{cfg: cfg, run: execRunner})

	listenDescr := cfg.internalListen
	if cfg.serviceSocket != "" {
		listenDescr = "uds:" + cfg.serviceSocket
	}
	log.Printf("starting gc-board-watcher city=%s mode=%s board=%s poll=%s debounce=%s mail_to=%s state=%s healthz=%s",
		cfg.city, cfg.mode, cfg.boardDescr(), cfg.pollInterval, cfg.debounce, cfg.mailTo, cfg.stateDir, listenDescr)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "ok city=%s mode=%s %s\n", cfg.city, cfg.mode, eng.status)
	})
	mux.HandleFunc("/", http.NotFound)
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		if cfg.serviceSocket != "" {
			lis, err := listenUDS(cfg.serviceSocket)
			if err != nil {
				errCh <- err
				return
			}
			errCh <- srv.Serve(lis)
			return
		}
		srv.Addr = cfg.internalListen
		errCh <- srv.ListenAndServe()
	}()

	go eng.run(ctx)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case <-stop:
		log.Println("shutting down (signal)")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("healthz listener error: %v", err)
		}
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

// listenUDS binds a Unix domain socket, removing any stale entry first so
// restarts succeed, and tightens it to owner-only.
func listenUDS(path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale socket: %w", err)
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen unix %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = lis.Close()
		return nil, fmt.Errorf("chmod uds: %w", err)
	}
	return lis, nil
}
