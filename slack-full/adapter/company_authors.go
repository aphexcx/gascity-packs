package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// company_authors.go — the switchboard's bot_id -> bots.info -> user_id
// resolver (Phase 2c). A company-room message authored by a bot is trusted
// only when it resolves, through this cache, to exactly one registered
// directory agent whose app_id, user_id, and (when present) event-declared
// identifiers all corroborate. Resolution outcomes are split so a transient
// Slack failure parks the receipt for the sweep instead of terminally
// discarding a real peer turn.

// botResolveOutcome classifies a bots.info lookup.
type botResolveOutcome int

const (
	// botResolveOK: bots.info returned a live bot; see companyBotInfo.
	botResolveOK botResolveOutcome = iota
	// botResolveUnknown: a definitive negative (bot_not_found or deleted).
	// Terminal for this message — the author is an unknown bot.
	botResolveUnknown
	// botResolveTransient: ratelimit (honor Retry-After), timeout, 5xx, or
	// network error. The caller parks the receipt non-terminally and the
	// sweep retries; never a terminal no_delivery.
	botResolveTransient
)

// companyBotInfo is the subset of a bots.info result the resolver keeps.
type companyBotInfo struct {
	UserID  string
	AppID   string
	Deleted bool
}

// companyAuthorResolver resolves and caches bot identities. The interface
// keeps the delivery worker testable without a live Slack endpoint.
type companyAuthorResolver interface {
	Resolve(botID string) (companyBotInfo, botResolveOutcome)
}

// botInfoTTL bounds how long a definitive resolution (ok or unknown) is
// cached. Transient outcomes are never cached, so a parked receipt re-drives
// against Slack until it resolves definitively.
const botInfoTTL = 10 * time.Minute

// botInfoResolver is the production HTTP-backed resolver: a TTL cache in
// front of bots.info, with per-bot singleflight so a burst of receipts for
// one bot collapses to a single API call.
type botInfoResolver struct {
	token  string
	client *http.Client
	now    func() time.Time
	ttl    time.Duration

	mu       sync.Mutex
	cache    map[string]botInfoCacheEntry
	inflight map[string]*botInfoCall
}

type botInfoCacheEntry struct {
	info    companyBotInfo
	outcome botResolveOutcome
	expires time.Time
}

// botInfoCall is a singleflight slot: waiters block on done, then read the
// shared result.
type botInfoCall struct {
	done    chan struct{}
	info    companyBotInfo
	outcome botResolveOutcome
}

func newBotInfoResolver(token string) *botInfoResolver {
	return &botInfoResolver{
		token:    token,
		client:   &http.Client{Timeout: companyDeliverTimeout},
		now:      time.Now,
		ttl:      botInfoTTL,
		cache:    make(map[string]botInfoCacheEntry),
		inflight: make(map[string]*botInfoCall),
	}
}

// Resolve returns the cached or freshly fetched identity for botID. A
// transient outcome is returned uncached so the caller re-drives later.
func (r *botInfoResolver) Resolve(botID string) (companyBotInfo, botResolveOutcome) {
	if botID == "" {
		return companyBotInfo{}, botResolveUnknown
	}
	if r.token == "" {
		// No switchboard token to call bots.info with: we cannot make a
		// definitive negative, so treat as transient (park + retry) rather
		// than silently drop a possibly-real peer turn.
		return companyBotInfo{}, botResolveTransient
	}

	r.mu.Lock()
	if e, ok := r.cache[botID]; ok && r.now().Before(e.expires) {
		r.mu.Unlock()
		return e.info, e.outcome
	}
	if call, ok := r.inflight[botID]; ok {
		r.mu.Unlock()
		<-call.done
		return call.info, call.outcome
	}
	call := &botInfoCall{done: make(chan struct{})}
	r.inflight[botID] = call
	r.mu.Unlock()

	info, outcome := r.fetch(botID)

	r.mu.Lock()
	call.info, call.outcome = info, outcome
	close(call.done)
	delete(r.inflight, botID)
	if outcome != botResolveTransient {
		r.cache[botID] = botInfoCacheEntry{info: info, outcome: outcome, expires: r.now().Add(r.ttl)}
	}
	r.mu.Unlock()
	return info, outcome
}

type slackBotsInfoResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Bot   struct {
		ID      string `json:"id"`
		AppID   string `json:"app_id"`
		UserID  string `json:"user_id"`
		Deleted bool   `json:"deleted"`
	} `json:"bot"`
}

// fetch performs one bots.info call and maps the result to an outcome.
func (r *botInfoResolver) fetch(botID string) (companyBotInfo, botResolveOutcome) {
	reqURL := slackAPIBase + "/bots.info?bot=" + botID
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return companyBotInfo{}, botResolveTransient
	}
	req.Header.Set("Authorization", "Bearer "+r.token)

	resp, err := r.client.Do(req)
	if err != nil {
		return companyBotInfo{}, botResolveTransient // network / timeout
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		// Honor Retry-After so a parked receipt's next sweep does not hammer
		// Slack; the value is advisory here (the sweep interval already
		// spaces retries) but we surface it for logging parity with the
		// Python side.
		_ = retryAfterSeconds(resp.Header)
		return companyBotInfo{}, botResolveTransient
	}
	if resp.StatusCode >= 500 {
		return companyBotInfo{}, botResolveTransient
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return companyBotInfo{}, botResolveTransient
	}
	if resp.StatusCode >= 400 {
		return companyBotInfo{}, botResolveTransient
	}
	var sr slackBotsInfoResp
	if err := json.Unmarshal(body, &sr); err != nil {
		return companyBotInfo{}, botResolveTransient
	}
	if !sr.OK {
		switch sr.Error {
		case "bot_not_found":
			return companyBotInfo{}, botResolveUnknown
		case "ratelimited", "rate_limited", "internal_error", "service_unavailable", "fatal_error":
			return companyBotInfo{}, botResolveTransient
		default:
			// Any other API error (bad auth, bad scope, …) is treated as
			// transient: a misconfigured switchboard must not manufacture
			// terminal no-delivery for a real peer.
			return companyBotInfo{}, botResolveTransient
		}
	}
	if sr.Bot.Deleted {
		return companyBotInfo{}, botResolveUnknown
	}
	return companyBotInfo{UserID: sr.Bot.UserID, AppID: sr.Bot.AppID, Deleted: false}, botResolveOK
}

// retryAfterSeconds parses a Retry-After header (integer seconds); 0 when
// absent or unparseable.
func retryAfterSeconds(h http.Header) int {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// companyAuthorResolution is the corroborated result of classifying a
// bot-authored company message: the matched directory agent (nil when
// unknown) and the outcome.
type companyAuthorResolution struct {
	Agent   *CompanyAgent
	Outcome botResolveOutcome
}

// resolveCompanyAuthor resolves a bot-authored message to its registered
// directory agent, applying the corroboration checklist. A definitive
// mismatch collapses to an unknown bot (fail-closed). A transient bots.info
// failure surfaces so the caller can park the receipt.
func resolveCompanyAuthor(resolver companyAuthorResolver, dir *CompanyDirectory, msg CompanyMessage) companyAuthorResolution {
	if dir == nil {
		return companyAuthorResolution{Outcome: botResolveUnknown}
	}
	// Cheap pre-check: if the event self-declares an app_id (or bot_profile
	// app_id) that matches no directory agent at all, this cannot be a
	// company bot — skip the API call and fail closed.
	if msg.AppID != "" && !dir.hasAppID(msg.AppID) {
		return companyAuthorResolution{Outcome: botResolveUnknown}
	}
	if msg.BotProfileAppID != "" && !dir.hasAppID(msg.BotProfileAppID) {
		return companyAuthorResolution{Outcome: botResolveUnknown}
	}
	if msg.BotID == "" {
		// bot_message subtype without a bot_id: nothing to resolve against.
		return companyAuthorResolution{Outcome: botResolveUnknown}
	}

	info, outcome := resolver.Resolve(msg.BotID)
	switch outcome {
	case botResolveTransient:
		return companyAuthorResolution{Outcome: botResolveTransient}
	case botResolveUnknown:
		return companyAuthorResolution{Outcome: botResolveUnknown}
	}

	// botResolveOK — corroborate against the directory and the event.
	agent, ok := dir.AgentByBotUserID(info.UserID)
	if !ok || agent.AppID != info.AppID {
		return companyAuthorResolution{Outcome: botResolveUnknown}
	}
	if msg.AppID != "" && msg.AppID != info.AppID {
		return companyAuthorResolution{Outcome: botResolveUnknown}
	}
	if msg.BotProfileAppID != "" && msg.BotProfileAppID != info.AppID {
		return companyAuthorResolution{Outcome: botResolveUnknown}
	}
	if msg.UserID != "" && msg.UserID != info.UserID {
		return companyAuthorResolution{Outcome: botResolveUnknown}
	}
	return companyAuthorResolution{Agent: agent, Outcome: botResolveOK}
}

// parseBotProfileAppID extracts bot_profile.app_id from a raw bot_profile
// object, tolerating absence.
func parseBotProfileAppID(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var bp struct {
		AppID string `json:"app_id"`
	}
	if err := json.Unmarshal(raw, &bp); err != nil {
		return ""
	}
	return bp.AppID
}

// hasAppID reports whether any directory agent declares app_id.
func (d *CompanyDirectory) hasAppID(appID string) bool {
	if d == nil || appID == "" {
		return false
	}
	for i := range d.agents {
		if d.agents[i].AppID == appID {
			return true
		}
	}
	return false
}

// compile-time assertion that the HTTP resolver satisfies the interface.
var _ companyAuthorResolver = (*botInfoResolver)(nil)
