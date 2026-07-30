package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

// --- inbound: Slack user display-name resolution (hq-uxln9) -----------------
//
// gc's extmsg reminder renders Actor.DisplayName verbatim, so forwarding the
// raw Slack user id (U0AN32RPBFT) left bound sessions reading ids instead of
// names. Ported from slack-mini's hq-fh9 fix: resolve via users.info with an
// in-memory TTL cache and fall back to the raw id on any failure. The same
// cache backs inline `<@U…>` mention rewriting in forwarded body text and
// the thread-context preamble's author lines. Requires the users:read scope;
// without it every lookup fails and raw ids pass through unchanged (the
// pre-fix behavior).

// userNameCacheTTL bounds how long a resolved display name is reused before
// users.info is consulted again. Names change rarely; an hour keeps the
// per-mention lookup cost near zero without pinning stale names forever.
const userNameCacheTTL = time.Hour

// userNameFailureTTL negative-caches a failed lookup (missing_scope on a
// bot token without users:read, transient Slack errors) so a burst of
// mentions does not hammer users.info, while healing quickly once the
// scope is granted or the outage passes.
const userNameFailureTTL = 5 * time.Minute

// userInfoTimeout bounds the users.info call on the inbound dispatch path.
const userInfoTimeout = 5 * time.Second

type cachedUserName struct {
	name      string
	expiresAt time.Time
}

// userNameCache is an in-memory users.info result cache. Entries live for
// the process lifetime, bounded by the workspace's user population.
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

// slackUserInfoResp is the subset of a users.info response the resolver reads.
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

// resolveUserDisplayName resolves a Slack user id to a human-readable name.
// The operator-curated slack-user-aliases.json wins first (inverse lookup,
// no Slack call — an id the operator bound to a gc handle always renders as
// that handle, and locked-down workspaces without users:read still get
// names for curated identities); otherwise users.info resolves it.
// Successes are cached for userNameCacheTTL; failures fall back to the raw
// id and are negative-cached for userNameFailureTTL. A nil cfg.userNames
// disables the users.info leg entirely (raw ids pass through) so tests
// that build a bare config stay network-inert.
func resolveUserDisplayName(cfg config, userID string) string {
	if userID == "" {
		return userID
	}
	if handle, ok := cfg.userAliases.handleForUserID(userID); ok {
		return handle
	}
	if cfg.userNames == nil || cfg.slackBotToken == "" {
		return userID
	}
	now := time.Now()
	if name, ok := cfg.userNames.get(userID, now); ok {
		return name
	}
	name := fetchUserDisplayName(cfg, userID)
	if name == "" {
		cfg.userNames.put(userID, userID, now, userNameFailureTTL)
		return userID
	}
	cfg.userNames.put(userID, name, now, userNameCacheTTL)
	return name
}

// fetchUserDisplayName performs the users.info call, returning "" on any
// failure (logged with the reason).
func fetchUserDisplayName(cfg config, userID string) string {
	ctx, cancel := context.WithTimeout(context.Background(), userInfoTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		slackAPIBase+"/users.info?user="+url.QueryEscape(userID), nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Authorization", "Bearer "+cfg.slackBotToken)
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
	return firstNonEmptyName(info.User.Profile.DisplayName, info.User.Profile.RealName,
		info.User.RealName, info.User.Name)
}

func firstNonEmptyName(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// slackUserMentionRE matches inline Slack user mentions: `<@U0AN32RPBFT>`
// or the labeled `<@U0AN32RPBFT|name>` form. W-prefixed ids cover
// Enterprise Grid users.
var slackUserMentionRE = regexp.MustCompile(`<@([UW][A-Z0-9]+)(?:\|([^>]*))?>`)

// slackTextMentionsUser reports whether text carries an inline Slack
// mention token for exactly userID — `<@U…>` or the labeled `<@U…|name>`
// form, anywhere in the text. Token-boundary matching via
// slackUserMentionRE: a plain substring check would false-positive on
// ids sharing a prefix (U0AB matching inside <@U0ABC>). Backs the
// busy-affordance bot-self-mention gate (gp-4vq).
func slackTextMentionsUser(text, userID string) bool {
	if userID == "" || !strings.Contains(text, "<@") {
		return false
	}
	for _, m := range slackUserMentionRE.FindAllStringSubmatch(text, -1) {
		if m[1] == userID {
			return true
		}
	}
	return false
}

// rewriteSlackUserMentions replaces inline `<@U…>` mention tokens in
// forwarded text with `@<display name>` so bound sessions read names, not
// ids (hq-uxln9). Resolution failures degrade gracefully: the mention's own
// label (`<@U…|name>`) is used when present, otherwise the token is left
// verbatim so no information is destroyed.
func rewriteSlackUserMentions(cfg config, text string) string {
	if cfg.userNames == nil || !strings.Contains(text, "<@") {
		return text
	}
	return slackUserMentionRE.ReplaceAllStringFunc(text, func(tok string) string {
		m := slackUserMentionRE.FindStringSubmatch(tok)
		if m == nil {
			return tok
		}
		id, label := m[1], m[2]
		if name := resolveUserDisplayName(cfg, id); name != id {
			return "@" + name
		}
		if label != "" {
			return "@" + label
		}
		return tok
	})
}
