// gc-slack-mini-adapter — the Tier-1 ("slack-mini") Slack ↔ gc bridge.
//
// The minimal viable Slack→mayor surface, single-file by design:
//
//   - Inbound: a public HTTPS receiver for the Slack Events API. Only
//     `app_mention` is handled. Each verified mention is bridged to gc by
//     POSTing /v0/city/{city}/extmsg/inbound, addressed to the mayor
//     session (override with SLACK_MINI_INBOUND_TARGET). The mention is
//     spooled to disk before the first forward attempt and the POST is
//     retried with backoff, because Slack has already been 200-acked and
//     will never redeliver: a dropped forward is a lost message. Exhausted
//     retries dead-letter the spool entry for manual replay; entries left
//     mid-retry by a crash are re-delivered at startup.
//
//   - Outbound: a UDS endpoint (/post-message) that posts plain text to a
//     Slack channel via chat.postMessage using the workspace bot token.
//     The pack's commands/post-message.sh wrapper reaches it through gc's
//     /svc/slack-mini reverse proxy. This is the only outbound verb at Tier 1.
//
// Tier 1 keeps NO on-disk registries: no channel bindings, no per-session
// identity, no apps registry, no rig/room state. Those belong to
// slack-channel (Tier 2) and slack-full (Tier 3). See
// docs/design/slack-pack-tiering.md.
//
// Required env:
//
//	SLACK_BOT_TOKEN        Bot token (xoxb-...) for outbound chat.postMessage.
//	                       Not used on the inbound path (which only verifies
//	                       the signing secret and POSTs to gc).
//	SLACK_SIGNING_SECRET   HMAC secret for verifying Slack request signatures
//	                       on the inbound bridge. Required at Tier 1 — there is
//	                       no apps-registry fallback, so without it every
//	                       inbound is rejected.
//	SLACK_WORKSPACE_ID     Slack workspace (team) id; the extmsg account id.
//	GC_CITY_NAME           gc city the adapter bridges into.
//
// Controller-injected env (proxy_process mode):
//
//	GC_SERVICE_SOCKET      UDS path the internal listener binds. When set, the
//	                       adapter runs as a gc proxy_process service.
//	GC_SERVICE_URL_PREFIX  Reverse-proxy prefix gc routes to this service;
//	                       used to compute the self-registration callback URL.
//	GC_API_BASE_URL        gc API base (default http://127.0.0.1:9443).
//
// Optional env:
//
//	LISTEN_PUBLIC              Public bind for /slack/events (default 0.0.0.0:8775).
//	LISTEN_INTERNAL            TCP bind for the internal mux when GC_SERVICE_SOCKET
//	                           is unset (default 127.0.0.1:8776).
//	REGISTER_ON_START          "true" (default) self-registers as an extmsg adapter.
//	ADAPTER_PROVIDER           extmsg provider name (default "slack").
//	SLACK_MINI_INBOUND_TARGET  Session handle inbound mentions address (default "mayor").
//	SLACK_API_BASE             Slack web API base (default https://slack.com/api).
//	SLACK_MINI_SPOOL_DIR       Directory for the inbound persist-and-retry spool
//	                           (default $GC_SERVICE_STATE_ROOT/data/inbound-spool
//	                           in proxy_process mode; unset otherwise, which
//	                           disables spooling but keeps in-memory retries).
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultPublicListen   = "0.0.0.0:8775"
	defaultInternalListen = "127.0.0.1:8776"
	defaultGCAPIBase      = "http://127.0.0.1:9443"
	defaultProvider       = "slack"
	defaultInboundTarget  = "mayor"

	// maxInboundBody caps the /slack/events body read. The body is
	// unsigned until HMAC-verified, so bounding it pre-verify limits a
	// memory-amplification vector. Slack event payloads are small.
	maxInboundBody = 1 << 20 // 1 MiB

	// slackReplayWindow rejects requests whose signed timestamp is older
	// than this, mitigating replay of a captured signature.
	slackReplayWindow = 5 * time.Minute

	slackPostTimeout = 15 * time.Second
)

// defaultSlackAPIBase is the production Slack web API origin. Overridable
// via SLACK_API_BASE (and via cfg.slackAPIBase in tests).
const defaultSlackAPIBase = "https://slack.com/api"

// gcCallTimeout bounds outbound calls to the gc API so a stalled gc cannot
// pin an inbound-bridge goroutine (or block startup registration) forever.
const gcCallTimeout = 15 * time.Second

// inboundRetryDelays spaces the forward retries after a failed inbound POST
// to gc: 5 attempts total over ~2 minutes, then dead-letter (bug hq-1q1).
// Package-level var so tests can compress the schedule.
var inboundRetryDelays = []time.Duration{
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
	60 * time.Second,
}

type config struct {
	publicListen        string
	internalListen      string
	serviceSocket       string
	gcAPIBase           string
	internalCallbackURL string
	cityName            string
	provider            string
	workspaceID         string
	botToken            string
	signingSecret       string
	inboundTarget       string
	slackAPIBase        string
	spoolDir            string
	registerOnStart     bool
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	internalDescr := cfg.internalListen
	if cfg.serviceSocket != "" {
		internalDescr = "uds:" + cfg.serviceSocket
	}
	log.Printf("starting gc-slack-mini-adapter public=%s internal=%s gc=%s city=%s target=%s",
		cfg.publicListen, internalDescr, cfg.gcAPIBase, cfg.cityName, cfg.inboundTarget)

	publicMux := http.NewServeMux()
	publicMux.HandleFunc("/slack/events", handleSlackEvents(cfg))
	publicMux.HandleFunc("/healthz", handleHealthz)
	publicMux.HandleFunc("/", http.NotFound)

	internalMux := http.NewServeMux()
	internalMux.HandleFunc("POST /post-message", handlePostMessage(cfg))
	internalMux.HandleFunc("/healthz", handleHealthz)

	publicSrv := &http.Server{
		Addr:              cfg.publicListen,
		Handler:           publicMux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	internalSrv := &http.Server{
		Handler:           internalMux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	if cfg.registerOnStart {
		regCtx, cancel := context.WithTimeout(context.Background(), gcCallTimeout)
		err := registerAdapter(regCtx, cfg)
		cancel()
		if err != nil {
			log.Fatalf("register adapter: %v", err)
		}
		log.Printf("registered with gc as provider=%s account=%s callback=%s/post-message",
			cfg.provider, cfg.workspaceID, cfg.internalCallbackURL)
	}

	// Re-deliver any inbound events a previous run spooled but never
	// confirmed forwarded (bug hq-1q1: Slack was already 200-acked).
	replaySpool(cfg)

	errCh := make(chan error, 2)
	go func() {
		log.Printf("public listener serving on %s (Slack events)", cfg.publicListen)
		errCh <- publicSrv.ListenAndServe()
	}()
	go func() {
		if cfg.serviceSocket != "" {
			log.Printf("internal listener serving on UDS %s (gc proxy_process)", cfg.serviceSocket)
			lis, err := listenUDS(cfg.serviceSocket)
			if err != nil {
				errCh <- err
				return
			}
			errCh <- internalSrv.Serve(lis)
			return
		}
		internalSrv.Addr = cfg.internalListen
		log.Printf("internal listener serving on %s (post-message only)", cfg.internalListen)
		errCh <- internalSrv.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case <-stop:
		log.Println("shutting down (signal)")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("listener error: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = publicSrv.Shutdown(ctx)
	_ = internalSrv.Shutdown(ctx)
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// loadConfig reads the process environment.
func loadConfig() (config, error) { return loadConfigFromEnv(os.Getenv) }

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
		publicListen:    envOr("LISTEN_PUBLIC", defaultPublicListen),
		internalListen:  envOr("LISTEN_INTERNAL", defaultInternalListen),
		serviceSocket:   getenv("GC_SERVICE_SOCKET"),
		gcAPIBase:       strings.TrimRight(envOr("GC_API_BASE_URL", defaultGCAPIBase), "/"),
		cityName:        getenv("GC_CITY_NAME"),
		provider:        envOr("ADAPTER_PROVIDER", defaultProvider),
		workspaceID:     getenv("SLACK_WORKSPACE_ID"),
		botToken:        getenv("SLACK_BOT_TOKEN"),
		signingSecret:   getenv("SLACK_SIGNING_SECRET"),
		inboundTarget:   envOr("SLACK_MINI_INBOUND_TARGET", defaultInboundTarget),
		slackAPIBase:    strings.TrimRight(envOr("SLACK_API_BASE", defaultSlackAPIBase), "/"),
		spoolDir:        getenv("SLACK_MINI_SPOOL_DIR"),
		registerOnStart: envOr("REGISTER_ON_START", "true") == "true",
	}

	// Default the persist-and-retry spool into the controller-provided state
	// root (proxy_process mode). Standalone runs without an explicit
	// SLACK_MINI_SPOOL_DIR get in-memory retries only.
	if cfg.spoolDir == "" {
		if stateRoot := getenv("GC_SERVICE_STATE_ROOT"); stateRoot != "" {
			cfg.spoolDir = filepath.Join(stateRoot, "data", "inbound-spool")
		}
	}

	// proxy_process mode: gc reaches the adapter via GC_API_BASE_URL +
	// GC_SERVICE_URL_PREFIX. gc appends the endpoint path itself, so the
	// registered callback base must not include it.
	if cfg.serviceSocket != "" {
		urlPrefix := strings.TrimRight(getenv("GC_SERVICE_URL_PREFIX"), "/")
		if urlPrefix == "" {
			return cfg, errors.New("GC_SERVICE_SOCKET is set but GC_SERVICE_URL_PREFIX is empty — controller-injected env is incomplete")
		}
		cfg.internalCallbackURL = cfg.gcAPIBase + urlPrefix
	} else {
		// Standalone TCP mode: no controller-injected URL prefix, so derive
		// the callback base from the internal listener. Leaving it empty would
		// self-register an empty callback_url and break gc→adapter callbacks.
		cfg.internalCallbackURL = tcpCallbackURL(cfg.internalListen)
	}

	var missing []string
	if cfg.workspaceID == "" {
		missing = append(missing, "SLACK_WORKSPACE_ID")
	}
	if cfg.botToken == "" {
		missing = append(missing, "SLACK_BOT_TOKEN")
	}
	if cfg.signingSecret == "" {
		missing = append(missing, "SLACK_SIGNING_SECRET")
	}
	if cfg.cityName == "" {
		missing = append(missing, "GC_CITY_NAME")
	}
	if len(missing) > 0 {
		return cfg, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}
	// cityName is interpolated into every /v0/city/{city}/... URL. Reject
	// URL-significant characters so a city name cannot alter routing.
	if strings.ContainsAny(cfg.cityName, "/?#%") {
		return cfg, fmt.Errorf("GC_CITY_NAME must not contain '/', '?', '#', or '%%': %q", cfg.cityName)
	}
	return cfg, nil
}

// tcpCallbackURL derives the gc→adapter callback base from the internal
// listener address for standalone (TCP) mode, where there is no
// proxy_process URL prefix. gc appends the endpoint path itself, so the
// returned URL carries no trailing path. A wildcard or empty bind host is
// rewritten to loopback because gc dials a concrete address.
func tcpCallbackURL(internalListen string) string {
	host, port, err := net.SplitHostPort(internalListen)
	if err != nil {
		return "http://" + internalListen
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// --- inbound: Slack events → gc extmsg ------------------------------------

type slackEventEnvelope struct {
	Type      string          `json:"type"`
	Challenge string          `json:"challenge,omitempty"`
	TeamID    string          `json:"team_id,omitempty"`
	Event     json.RawMessage `json:"event,omitempty"`
}

// slackMessageEvent is the subset of an app_mention event Tier 1 reads.
type slackMessageEvent struct {
	Type        string `json:"type"`
	Subtype     string `json:"subtype,omitempty"`
	User        string `json:"user,omitempty"`
	BotID       string `json:"bot_id,omitempty"`
	Text        string `json:"text,omitempty"`
	Channel     string `json:"channel,omitempty"`
	TS          string `json:"ts,omitempty"`
	ThreadTS    string `json:"thread_ts,omitempty"`
	ChannelType string `json:"channel_type,omitempty"`
}

func handleSlackEvents(cfg config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxInboundBody))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		ts := r.Header.Get("X-Slack-Request-Timestamp")
		sig := r.Header.Get("X-Slack-Signature")
		if !verifySlackSignature(cfg.signingSecret, ts, body, sig) {
			log.Printf("slack signature verify FAILED")
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}

		var env slackEventEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
			return
		}

		// URL verification handshake (Slack sends this once when the
		// Events API endpoint is registered).
		if env.Type == "url_verification" && env.Challenge != "" {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(env.Challenge))
			return
		}

		// Ack immediately so Slack does not retry, then bridge.
		w.WriteHeader(http.StatusOK)
		go bridgeEvent(cfg, env)
	}
}

// bridgeEvent decodes an app_mention and hands it to deliverInbound.
// Non-app_mention events, bot/system messages, and empty bodies are
// dropped — Tier 1 handles only direct human mentions. It runs in its own
// goroutine; each gc call is bounded by gcCallTimeout and the retry
// schedule bounds the goroutine's total lifetime to ~2 minutes.
func bridgeEvent(cfg config, env slackEventEnvelope) {
	if env.Type != "event_callback" || len(env.Event) == 0 {
		return
	}
	var msg slackMessageEvent
	if err := json.Unmarshal(env.Event, &msg); err != nil {
		log.Printf("decode slack event: %v", err)
		return
	}
	if msg.Type != "app_mention" {
		return
	}
	if msg.BotID != "" || msg.Subtype != "" || msg.User == "" {
		return
	}
	text := stripLeadingMention(msg.Text)
	if text == "" {
		return
	}

	// Log receipt before the first forward attempt so a message that later
	// fails every retry is still identifiable (and replayable from Slack).
	log.Printf("inbound received: chan=%s user=%s ts=%s thread=%s target=%s text=%dch",
		msg.Channel, msg.User, msg.TS, msg.ThreadTS, cfg.inboundTarget, len(text))

	inbound := externalInboundMessage{
		ProviderMessageID: msg.TS,
		Conversation: conversationRef{
			ScopeID:        cfg.cityName,
			Provider:       cfg.provider,
			AccountID:      cfg.workspaceID,
			ConversationID: msg.Channel,
			Kind:           slackKindFromChannelType(msg.ChannelType, msg.Channel),
		},
		Actor: externalActor{
			ID:          msg.User,
			DisplayName: resolveUserDisplayName(context.Background(), userNames, cfg, msg.User),
		},
		Text:             text,
		ExplicitTarget:   cfg.inboundTarget,
		ReplyToMessageID: msg.ThreadTS,
		DedupKey:         "slack-" + msg.TS,
		ReceivedAt:       time.Now().UTC(),
	}
	deliverInbound(cfg, inbound, spoolInbound(cfg.spoolDir, inbound))
}

// spoolInbound persists a decoded inbound event before the first forward
// attempt, so a crash mid-retry cannot silently lose a Slack-acked message.
// Returns the spool file path, or "" when spooling is disabled (no spool
// dir) or the write fails — persistence is best-effort and never blocks
// the bridge.
func spoolInbound(spoolDir string, msg externalInboundMessage) string {
	if spoolDir == "" {
		return ""
	}
	if err := os.MkdirAll(spoolDir, 0o700); err != nil {
		log.Printf("spool: mkdir %s: %v", spoolDir, err)
		return ""
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("spool: marshal: %v", err)
		return ""
	}
	name := fmt.Sprintf("%d-%s.json", time.Now().UnixNano(), sanitizeSpoolName(msg.DedupKey))
	path := filepath.Join(spoolDir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		log.Printf("spool: write %s: %v", path, err)
		return ""
	}
	return path
}

// spoolNameRE matches characters unsafe in a spool filename.
var spoolNameRE = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

func sanitizeSpoolName(s string) string {
	return spoolNameRE.ReplaceAllString(s, "_")
}

// deliverInbound forwards one inbound message to gc, retrying failures per
// inboundRetryDelays. On success the spool entry is removed; when every
// attempt fails the entry moves to the dead-letter directory. The final
// failure line deliberately contains "inbound POST failed" — external
// log-watchers (slack-nudge-bridge) key on that exact substring.
func deliverInbound(cfg config, msg externalInboundMessage, spoolPath string) {
	attempts := len(inboundRetryDelays) + 1
	var lastErr error
	for i := range attempts {
		if i > 0 {
			time.Sleep(inboundRetryDelays[i-1])
		}
		ctx, cancel := context.WithTimeout(context.Background(), gcCallTimeout)
		err := postInbound(ctx, cfg, msg)
		cancel()
		if err == nil {
			if spoolPath != "" {
				_ = os.Remove(spoolPath)
			}
			log.Printf("inbound: chan=%s user=%s ts=%s thread=%s target=%s text=%dch",
				msg.Conversation.ConversationID, msg.Actor.ID, msg.ProviderMessageID,
				msg.ReplyToMessageID, msg.ExplicitTarget, len(msg.Text))
			return
		}
		lastErr = err
		if i < attempts-1 {
			log.Printf("inbound forward attempt %d/%d failed (retry in %s): %v",
				i+1, attempts, inboundRetryDelays[i], err)
		}
	}
	log.Printf("inbound POST failed after %d attempts (dead-letter=%s) chan=%s ts=%s: %v",
		attempts, moveToDeadLetter(spoolPath), msg.Conversation.ConversationID,
		msg.ProviderMessageID, lastErr)
}

// moveToDeadLetter quarantines an exhausted spool entry in the sibling
// "dead" directory so it can be replayed by hand; returns the new path, or
// "none" when spooling was disabled for this message. If the move fails the
// entry stays in the spool, where startup replay will retry it.
func moveToDeadLetter(spoolPath string) string {
	if spoolPath == "" {
		return "none"
	}
	deadDir := filepath.Join(filepath.Dir(spoolPath), "dead")
	if err := os.MkdirAll(deadDir, 0o700); err != nil {
		log.Printf("dead-letter: mkdir: %v", err)
		return spoolPath
	}
	dest := filepath.Join(deadDir, filepath.Base(spoolPath))
	if err := os.Rename(spoolPath, dest); err != nil {
		log.Printf("dead-letter: rename: %v", err)
		return spoolPath
	}
	return dest
}

// replaySpool re-delivers inbound events a previous run persisted but never
// confirmed forwarded (crash mid-retry). Redelivery may duplicate an event
// gc already accepted — the message DedupKey makes that safe. Dead-lettered
// entries are NOT replayed automatically; they stay under spool/dead for
// manual replay.
func replaySpool(cfg config) {
	if cfg.spoolDir == "" {
		return
	}
	entries, err := os.ReadDir(cfg.spoolDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("spool replay: %v", err)
		}
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join(cfg.spoolDir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			log.Printf("spool replay: read %s: %v", path, err)
			continue
		}
		var msg externalInboundMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("spool replay: decode %s: %v (dead-letter=%s)", path, err, moveToDeadLetter(path))
			continue
		}
		log.Printf("spool replay: re-delivering %s (chan=%s ts=%s)",
			e.Name(), msg.Conversation.ConversationID, msg.ProviderMessageID)
		go deliverInbound(cfg, msg, path)
	}
}

// --- inbound: Slack user display-name resolution ---------------------------

// userNameCacheTTL bounds how long a resolved display name is reused before
// users.info is consulted again. Names change rarely; an hour keeps the
// per-mention lookup cost near zero without pinning stale names forever.
const userNameCacheTTL = time.Hour

// userNameFailureTTL negative-caches a failed lookup (missing_scope on a
// bot token without users:read, transient Slack errors) so a burst of
// mentions does not hammer users.info, while healing quickly once the
// scope is granted or the outage passes.
const userNameFailureTTL = 5 * time.Minute

// userInfoTimeout bounds the users.info call on the inbound bridge path.
const userInfoTimeout = 5 * time.Second

type cachedUserName struct {
	name      string
	expiresAt time.Time
}

// userNameCache is an in-memory users.info result cache. Tier 1 keeps no
// on-disk registries by design, so entries live for the process lifetime.
type userNameCache struct {
	mu      sync.Mutex
	entries map[string]cachedUserName
}

func newUserNameCache() *userNameCache {
	return &userNameCache{entries: make(map[string]cachedUserName)}
}

func (c *userNameCache) get(userID string, now time.Time) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[userID]
	if !ok || now.After(entry.expiresAt) {
		return "", false
	}
	return entry.name, true
}

func (c *userNameCache) put(userID, name string, now time.Time, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[userID] = cachedUserName{name: name, expiresAt: now.Add(ttl)}
}

// userNames caches users.info lookups across inbound mentions.
var userNames = newUserNameCache()

// slackUserInfoResp is the subset of a users.info response Tier 1 reads.
type slackUserInfoResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	User  struct {
		Name     string `json:"name"`
		RealName string `json:"real_name"`
		Profile  struct {
			DisplayName string `json:"display_name"`
			RealName    string `json:"real_name"`
		} `json:"profile"`
	} `json:"user"`
}

// resolveUserDisplayName resolves a Slack user id to a human-readable name
// via users.info so the injected gc reminder shows "Afik Cohen (human)"
// instead of a raw "U0AN32RPBFT" (hq-fh9). Successes are cached for
// userNameCacheTTL; failures fall back to the raw id and are
// negative-cached for userNameFailureTTL (a bot token without users:read
// fails on every mention — see manifest/app.json, which grants the scope
// for fresh installs; existing installs must re-approve it).
func resolveUserDisplayName(ctx context.Context, cache *userNameCache, cfg config, userID string) string {
	if userID == "" {
		return userID
	}
	now := time.Now()
	if name, ok := cache.get(userID, now); ok {
		return name
	}
	name := fetchUserDisplayName(ctx, cfg, userID)
	if name == "" {
		cache.put(userID, userID, now, userNameFailureTTL)
		return userID
	}
	cache.put(userID, name, now, userNameCacheTTL)
	return name
}

// fetchUserDisplayName performs the users.info call, returning "" on any
// failure (logged with the reason).
func fetchUserDisplayName(ctx context.Context, cfg config, userID string) string {
	ctx, cancel := context.WithTimeout(ctx, userInfoTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		cfg.slackAPIBase+"/users.info?user="+url.QueryEscape(userID), nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Authorization", "Bearer "+cfg.botToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("users.info %s failed (using raw id): %v", userID, err)
		return ""
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("users.info %s: http %d (using raw id)", userID, resp.StatusCode)
		return ""
	}
	var info slackUserInfoResp
	if err := json.Unmarshal(respBody, &info); err != nil || !info.OK {
		log.Printf("users.info %s: ok=false error=%q (using raw id)", userID, info.Error)
		return ""
	}
	return firstNonEmpty(info.User.Profile.DisplayName, info.User.Profile.RealName, info.User.RealName, info.User.Name)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// leadingMentionRE matches one or more leading Slack user-mention tokens
// (`<@U…>`) and surrounding whitespace. Slack delivers app_mention text
// with the bot mention inline (e.g. "<@U0BOT> status?"); stripping it
// yields the human-meant message body.
var leadingMentionRE = regexp.MustCompile(`^\s*(?:<@[A-Z0-9]+>\s*)+`)

func stripLeadingMention(text string) string {
	return strings.TrimSpace(leadingMentionRE.ReplaceAllString(text, ""))
}

// slackKindFromChannelType maps a Slack channel_type onto a gc
// ConversationKind, falling back to the channel-id prefix.
func slackKindFromChannelType(channelType, channelID string) string {
	switch channelType {
	case "channel", "group", "mpim":
		return "room"
	case "im":
		return "dm"
	}
	if len(channelID) > 0 {
		switch channelID[0] {
		case 'C', 'G':
			return "room"
		case 'D':
			return "dm"
		}
	}
	return "dm"
}

// verifySlackSignature validates Slack's v0 HMAC request signature and
// rejects timestamps whose absolute age exceeds the replay window — both
// stale (past) and far-future. Fails closed on any missing field or parse
// error.
func verifySlackSignature(secret, ts string, body []byte, sig string) bool {
	if secret == "" || ts == "" || sig == "" {
		return false
	}
	tsInt, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return false
	}
	// Reject when the absolute age exceeds the replay window — both stale
	// (past) and far-future timestamps. time.Since yields a negative
	// duration for future timestamps, so a one-sided ">" check would
	// silently accept them.
	age := time.Since(time.Unix(tsInt, 0))
	if age < 0 {
		age = -age
	}
	if age > slackReplayWindow {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + ts + ":"))
	_, _ = mac.Write(body)
	expected := "v0=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(sig))
}

// --- gc extmsg wire types (mirrored, wire-compatible only) ----------------

type conversationRef struct {
	ScopeID        string `json:"scope_id"`
	Provider       string `json:"provider"`
	AccountID      string `json:"account_id"`
	ConversationID string `json:"conversation_id"`
	Kind           string `json:"kind"`
}

type externalActor struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	IsBot       bool   `json:"is_bot"`
}

type externalInboundMessage struct {
	ProviderMessageID string          `json:"provider_message_id"`
	Conversation      conversationRef `json:"conversation"`
	Actor             externalActor   `json:"actor"`
	Text              string          `json:"text"`
	ExplicitTarget    string          `json:"explicit_target,omitempty"`
	ReplyToMessageID  string          `json:"reply_to_message_id,omitempty"`
	DedupKey          string          `json:"dedup_key,omitempty"`
	ReceivedAt        time.Time       `json:"received_at"`
}

type adapterCapabilities struct {
	SupportsChildConversations bool `json:"SupportsChildConversations"`
	SupportsAttachments        bool `json:"SupportsAttachments"`
	MaxMessageLength           int  `json:"MaxMessageLength"`
}

type adapterRegisterRequest struct {
	Provider          string              `json:"provider"`
	AccountID         string              `json:"account_id"`
	Name              string              `json:"name,omitempty"`
	CallbackURL       string              `json:"callback_url,omitempty"`
	Capabilities      adapterCapabilities `json:"capabilities,omitempty"`
	ReplyInstructions string              `json:"reply_instructions,omitempty"`
}

// replyInstructionsTemplate is the Tier-1 reply instruction block gc renders
// into the inbound-message <system-reminder> nudge in place of its generic
// "gc slack reply-current ..." fallback, which does not exist at Tier 1
// (hq-fh9). gc substitutes {conversation_id}, {message_ts}, {thread_ts}
// (the inbound message's thread, falling back to the message itself), and
// {handle}; the [bracketed segment] is dropped when no thread id is
// available. Agents write standard Markdown — handlePostMessage converts it
// to Slack mrkdwn on the way out.
const replyInstructionsTemplate = "To reply in Slack, run:\n" +
	"  gc slack-mini post-message --channel {conversation_id}[ --thread-ts {thread_ts}] --text '<your reply>'\n" +
	"Keep --thread-ts so the reply lands in the thread. Write the reply in standard Markdown " +
	"(it is converted to Slack formatting on post) and prefix it with your agent handle in bold " +
	"(e.g., **{handle}:** your message)."

// postInbound bridges a verified Slack mention into gc.
func postInbound(ctx context.Context, cfg config, msg externalInboundMessage) error {
	body, err := json.Marshal(map[string]any{"message": msg})
	if err != nil {
		return err
	}
	target := fmt.Sprintf("%s/v0/city/%s/extmsg/inbound", cfg.gcAPIBase, url.PathEscape(cfg.cityName))
	if err := postJSON(ctx, target, body); err != nil {
		return fmt.Errorf("post inbound: %w", err)
	}
	return nil
}

// registerAdapter self-registers as an extmsg adapter so gc accepts this
// provider's inbound messages.
func registerAdapter(ctx context.Context, cfg config) error {
	body, err := json.Marshal(adapterRegisterRequest{
		Provider:    cfg.provider,
		AccountID:   cfg.workspaceID,
		Name:        "slack-mini-adapter",
		CallbackURL: cfg.internalCallbackURL,
		Capabilities: adapterCapabilities{
			SupportsChildConversations: false,
			SupportsAttachments:        false,
			MaxMessageLength:           40000, // Slack's chat.postMessage limit
		},
		ReplyInstructions: replyInstructionsTemplate,
	})
	if err != nil {
		return err
	}
	target := fmt.Sprintf("%s/v0/city/%s/extmsg/adapters", cfg.gcAPIBase, url.PathEscape(cfg.cityName))
	return postJSON(ctx, target, body)
}

// postJSON POSTs a JSON body to a gc API endpoint and treats any >=400
// status as an error, surfacing the response body for diagnostics. ctx
// bounds the call so callers can enforce a timeout.
func postJSON(ctx context.Context, target string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GC-Request", "gc-slack-mini-adapter")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}
	return nil
}

// --- outbound: post-message → Slack chat.postMessage ----------------------

// slackPostMessageReq is both the JSON body the post-message.sh wrapper
// POSTs to /post-message and the chat.postMessage payload — the wrapper
// deliberately speaks Slack's own field names, so one type serves both.
type slackPostMessageReq struct {
	Channel  string `json:"channel"`
	Text     string `json:"text"`
	ThreadTS string `json:"thread_ts,omitempty"`
}

type slackPostMessageResp struct {
	OK      bool   `json:"ok"`
	TS      string `json:"ts,omitempty"`
	Channel string `json:"channel,omitempty"`
	Error   string `json:"error,omitempty"`
}

func handlePostMessage(cfg config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req slackPostMessageReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("decode: %v", err))
			return
		}
		if strings.TrimSpace(req.Channel) == "" {
			writeJSONError(w, http.StatusBadRequest, "channel is required")
			return
		}
		if strings.TrimSpace(req.Text) == "" {
			writeJSONError(w, http.StatusBadRequest, "text is required")
			return
		}
		req.Text = slackifyMarkdown(req.Text)
		resp, err := postToSlack(r.Context(), cfg.slackAPIBase, cfg.botToken, req)
		if err != nil {
			writeJSONError(w, http.StatusBadGateway, err.Error())
			return
		}
		if !resp.OK {
			writeJSONError(w, http.StatusBadGateway, "slack: "+resp.Error)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"ts":      resp.TS,
			"channel": resp.Channel,
		})
	}
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": msg})
}

// --- outbound: GitHub Markdown → Slack mrkdwn -------------------------------

var (
	fencedCodeRE = regexp.MustCompile("(?s)```.*?```")
	inlineCodeRE = regexp.MustCompile("`[^`\n]+`")
	boldStarsRE  = regexp.MustCompile(`\*\*(.+?)\*\*`)
	boldUnderRE  = regexp.MustCompile(`__(.+?)__`)
	strikeRE     = regexp.MustCompile(`~~(.+?)~~`)
	mdLinkRE     = regexp.MustCompile(`\[([^\]\n]+)\]\((https?://[^)\s]+)\)`)
	headingRE    = regexp.MustCompile(`(?m)^#{1,6}[ \t]+(.+?)[ \t]*#*[ \t]*$`)
)

// slackifyMarkdown converts the GitHub-flavored Markdown constructs agents
// habitually write into Slack mrkdwn, which renders **bold** as literal
// asterisks (hq-fh9). Conservative by design: fenced code blocks and inline
// code spans pass through verbatim; only **bold**/__bold__ → *bold*,
// ~~strike~~ → ~strike~, [text](url) → <url|text>, and #-headings →
// *bold lines* are rewritten. Single-asterisk/underscore emphasis is left
// untouched so text already written as mrkdwn survives unchanged.
func slackifyMarkdown(text string) string {
	if text == "" {
		return text
	}
	var protected []string
	protect := func(s string) string {
		protected = append(protected, s)
		return fmt.Sprintf("\x00%d\x00", len(protected)-1)
	}
	out := fencedCodeRE.ReplaceAllStringFunc(text, protect)
	// An unterminated fence swallows the rest of the message: protect it
	// whole rather than reformatting half a code block.
	if idx := strings.Index(out, "```"); idx >= 0 {
		out = out[:idx] + protect(out[idx:])
	}
	out = inlineCodeRE.ReplaceAllStringFunc(out, protect)
	out = headingRE.ReplaceAllString(out, "*$1*")
	out = boldStarsRE.ReplaceAllString(out, "*$1*")
	out = boldUnderRE.ReplaceAllString(out, "*$1*")
	out = strikeRE.ReplaceAllString(out, "~$1~")
	out = mdLinkRE.ReplaceAllString(out, "<$2|$1>")
	for i := len(protected) - 1; i >= 0; i-- {
		out = strings.Replace(out, fmt.Sprintf("\x00%d\x00", i), protected[i], 1)
	}
	return out
}

// postToSlack posts a message via chat.postMessage using the bot token.
func postToSlack(ctx context.Context, apiBase, token string, req slackPostMessageReq) (*slackPostMessageResp, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, slackPostTimeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, apiBase+"/chat.postMessage", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("post chat.postMessage: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read slack response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("slack http %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var sr slackPostMessageResp
	if err := json.Unmarshal(respBody, &sr); err != nil {
		return nil, fmt.Errorf("decode slack response: %w", err)
	}
	return &sr, nil
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
