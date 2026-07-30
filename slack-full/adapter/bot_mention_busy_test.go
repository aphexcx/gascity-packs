package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Tests for the bot-self-mention busy-affordance gate (gp-4vq).
//
// Live finding: nobody addresses agents with the `@handle:` prefix
// syntax in real use — they @-mention the adapter's bot user — so the
// hq-xizo busy-reaction lifecycle, gated on a parsed target, never
// fired in practice. A mention of the adapter's OWN bot user (id
// sourced from the event envelope's authorizations, or implied by the
// app_mention event type) now makes the inbound busy-eligible without
// touching routing: ExplicitTarget stays empty and no alias dispatch
// fires.

const testBotUserID = "U0BHE265YNN"

// botMentionEnvelope builds an event_callback envelope whose event
// carries text mentioning the adapter's own bot user. eventType is
// "message" or "app_mention" — Slack delivers a real bot mention as
// BOTH, under distinct event_ids (hw-vzd5y edge case 2). withAuth
// controls whether the envelope carries the authorizations block that
// names the bot's user id.
func botMentionEnvelope(t *testing.T, eventType, eventID, channel, ts, threadTS, text string, withAuth bool) slackEventEnvelope {
	t.Helper()
	rawMsg, err := json.Marshal(slackMessageEvent{
		Type:     eventType,
		Channel:  channel,
		User:     "U_ALICE",
		TS:       ts,
		ThreadTS: threadTS,
		Text:     text,
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	env := slackEventEnvelope{Type: "event_callback", EventID: eventID, Event: rawMsg}
	if withAuth {
		env.Authorizations = []slackEventAuthorization{{UserID: testBotUserID, IsBot: true}}
	}
	return env
}

// A plain channel message @-mentioning the adapter's own bot user —
// the live-repro shape "<@bot> test message, react to this please",
// no handle prefix, no thread — gets the busy reaction and a pending
// mark, while routing stays generic: ExplicitTarget empty, text
// unaltered.
func TestBusyReaction_BotMentionMakesInboundBusyEligible(t *testing.T) {
	slackStub, reactions := newReactionRecordingSlackStub(t)
	withSlackAPIStub(t, slackStub)
	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := busyTestConfig(gcStub.URL)
	text := "<@" + testBotUserID + "> test message, react to this please"
	env := botMentionEnvelope(t, "message", "Ev1", "C1", "100.000010", "", text, true)
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	got := reactions.await(t, 2*time.Second)
	if got.op != "add" || got.name != "hourglass" {
		t.Errorf("reaction = (%s, %s), want (add, hourglass)", got.op, got.name)
	}
	if got.channel != "C1" || got.timestamp != "100.000010" {
		t.Errorf("reaction target = (%s, %s), want (C1, 100.000010)", got.channel, got.timestamp)
	}
	reactions.assertNoCall(t, 300*time.Millisecond)

	if ts, ok := cfg.busyMarks.pending("C1", "100.000010"); !ok || ts != "100.000010" {
		t.Errorf("busy mark = (%q, %v), want (100.000010, true)", ts, ok)
	}

	msgs := capture.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("captured %d inbound messages, want 1", len(msgs))
	}
	// Routing must be untouched: a bot mention is not a handle target.
	// A fabricated ExplicitTarget would tell the channel-bound session
	// "this is for someone else" and mute it.
	if msgs[0].ExplicitTarget != "" {
		t.Errorf("ExplicitTarget = %q, want empty (bot mention must not fabricate a target)", msgs[0].ExplicitTarget)
	}
	if msgs[0].Text != text {
		t.Errorf("forwarded text = %q, want unaltered %q", msgs[0].Text, text)
	}
}

// A mid-text mention (not a leading address token) still counts — the
// affordance is about the bot being tagged at all, not about prefix
// addressing syntax.
func TestBusyReaction_MidTextBotMentionCounts(t *testing.T) {
	slackStub, reactions := newReactionRecordingSlackStub(t)
	withSlackAPIStub(t, slackStub)
	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := busyTestConfig(gcStub.URL)
	env := botMentionEnvelope(t, "message", "Ev1", "C1", "100.000011", "",
		"hey <@"+testBotUserID+"> can you take a look?", true)
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	if got := reactions.await(t, 2*time.Second); got.op != "add" || got.timestamp != "100.000011" {
		t.Errorf("reaction = (%s on %s), want add on 100.000011", got.op, got.timestamp)
	}
}

// An app_mention delivery with NO authorizations block still fires:
// the event type itself is proof the bot was tagged.
func TestBusyReaction_AppMentionTypeIsFallbackSignal(t *testing.T) {
	slackStub, reactions := newReactionRecordingSlackStub(t)
	withSlackAPIStub(t, slackStub)
	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := busyTestConfig(gcStub.URL)
	env := botMentionEnvelope(t, "app_mention", "Ev1", "C1", "100.000012", "",
		"<@"+testBotUserID+"> ping", false)
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	if got := reactions.await(t, 2*time.Second); got.op != "add" || got.timestamp != "100.000012" {
		t.Errorf("reaction = (%s on %s), want add on 100.000012", got.op, got.timestamp)
	}
}

// Mentioning some OTHER user is still generic channel chatter: no
// reaction, no mark. Guards against the busy affordance firing on
// every human-to-human mention in a bound room.
func TestBusyReaction_ForeignMentionStaysGeneric(t *testing.T) {
	slackStub, reactions := newReactionRecordingSlackStub(t)
	withSlackAPIStub(t, slackStub)
	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := busyTestConfig(gcStub.URL)
	env := botMentionEnvelope(t, "message", "Ev1", "C1", "100.000013", "",
		"<@U_SOMEBODY_ELSE> what do you think?", true)
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	reactions.assertNoCall(t, 300*time.Millisecond)
	if n := cfg.busyMarks.size(); n != 0 {
		t.Errorf("registry has %d marks after a foreign mention, want 0", n)
	}
	if msgs := capture.snapshot(); len(msgs) != 1 {
		t.Fatalf("captured %d inbound messages, want 1 (message still forwards)", len(msgs))
	}
}

// The dual delivery Slack actually sends for one bot mention — a
// message event AND an app_mention event, distinct event_ids, same ts
// (hw-vzd5y edge case 2, live repro in gp-4vq) — must not double-mark:
// markBoth's same-message merge keeps ONE registry entry, and the
// reply's publish removes the reaction exactly once.
func TestBusyReaction_DualDeliverySingleMarkSingleRemove(t *testing.T) {
	slackStub, reactions := newReactionRecordingSlackStub(t)
	withSlackAPIStub(t, slackStub)
	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := busyTestConfig(gcStub.URL)
	text := "<@" + testBotUserID + "> test message, react to this please"
	envMsg := botMentionEnvelope(t, "message", "Ev1", "C1", "100.000014", "", text, true)
	envAM := botMentionEnvelope(t, "app_mention", "Ev2", "C1", "100.000014", "", text, true)

	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, envMsg, func() {})
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, envAM, func() {})

	// Each delivery independently fires its add after its own forward
	// succeeded (the second is Slack's benign already_reacted — the
	// stub answers ok either way); the registry must still hold ONE
	// merged mark for the ts.
	first := reactions.await(t, 2*time.Second)
	second := reactions.await(t, 2*time.Second)
	for _, r := range []recordedReaction{first, second} {
		if r.op != "add" || r.timestamp != "100.000014" {
			t.Errorf("reaction = (%s on %s), want add on 100.000014", r.op, r.timestamp)
		}
	}
	if n := cfg.busyMarks.size(); n != 1 {
		t.Fatalf("registry has %d entries after dual delivery, want 1 (same-message merge)", n)
	}
	if ts, ok := cfg.busyMarks.pending("C1", "100.000014"); !ok || ts != "100.000014" {
		t.Errorf("busy mark = (%q, %v), want (100.000014, true)", ts, ok)
	}

	// The agent's reply clears the merged mark: exactly one remove.
	req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(publishBody("C1", "100.000014")))
	rec := httptest.NewRecorder()
	handlePublish(cfg, nil, nil, newPublishDedupCache(publishDedupTTL))(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("publish status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	if got := reactions.await(t, 2*time.Second); got.op != "remove" || got.timestamp != "100.000014" {
		t.Errorf("reaction = (%s on %s), want remove on 100.000014", got.op, got.timestamp)
	}
	reactions.assertNoCall(t, 300*time.Millisecond)
	if n := cfg.busyMarks.size(); n != 0 {
		t.Errorf("registry has %d entries after the reply cleared, want 0", n)
	}
}

// An explicit handle target and a bot mention in the same message
// coexist: the handle path's semantics (ExplicitTarget set) win, and
// the affordance fires once.
func TestBusyReaction_HandleTargetStillWinsRouting(t *testing.T) {
	slackStub, reactions := newReactionRecordingSlackStub(t)
	withSlackAPIStub(t, slackStub)
	capture := &inboundCapture{}
	gcStub := httptest.NewServer(capture.handler())
	t.Cleanup(gcStub.Close)

	cfg := busyTestConfig(gcStub.URL)
	env := botMentionEnvelope(t, "message", "Ev1", "C1", "100.000015", "",
		"@mayor deploy <@"+testBotUserID+"> knows why", true)
	processSlackEvent(cfg, newTestHandleAliasRegistry(t), nil, nil, nil, nil, env, func() {})

	if got := reactions.await(t, 2*time.Second); got.op != "add" || got.timestamp != "100.000015" {
		t.Errorf("reaction = (%s on %s), want add on 100.000015", got.op, got.timestamp)
	}
	reactions.assertNoCall(t, 300*time.Millisecond)
	msgs := capture.snapshot()
	if len(msgs) != 1 {
		t.Fatalf("captured %d inbound messages, want 1", len(msgs))
	}
	if msgs[0].ExplicitTarget != "mayor" {
		t.Errorf("ExplicitTarget = %q, want %q (handle parse unaffected by bot mention)", msgs[0].ExplicitTarget, "mayor")
	}
}

func TestSlackTextMentionsUser(t *testing.T) {
	for _, tc := range []struct {
		name   string
		text   string
		userID string
		want   bool
	}{
		{"leading mention", "<@U0BHE265YNN> hi", "U0BHE265YNN", true},
		{"mid-text mention", "hey <@U0BHE265YNN> hi", "U0BHE265YNN", true},
		{"labeled form", "<@U0BHE265YNN|gascity> hi", "U0BHE265YNN", true},
		{"different user", "<@U0OTHER11> hi", "U0BHE265YNN", false},
		{"id prefix must not match longer id", "<@U0BHE265YNNX> hi", "U0BHE265YNN", false},
		{"no mention token", "U0BHE265YNN plain text", "U0BHE265YNN", false},
		{"empty user id", "<@U0BHE265YNN> hi", "", false},
		{"empty text", "", "U0BHE265YNN", false},
		{"enterprise W id", "<@W0BHE265YNN> hi", "W0BHE265YNN", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := slackTextMentionsUser(tc.text, tc.userID); got != tc.want {
				t.Errorf("slackTextMentionsUser(%q, %q) = %v, want %v", tc.text, tc.userID, got, tc.want)
			}
		})
	}
}

// botUserID reads the authorizations block off the raw JSON Slack
// actually sends, gating on is_bot: a user-token install's
// authorization names a HUMAN id and must not be treated as the bot.
func TestEnvelopeBotUserID(t *testing.T) {
	var env slackEventEnvelope
	raw := `{"type":"event_callback","event_id":"Ev123",` +
		`"authorizations":[{"enterprise_id":null,"team_id":"T1","user_id":"U0BHE265YNN","is_bot":true,"is_enterprise_install":false}]}`
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if got := env.botUserID(); got != "U0BHE265YNN" {
		t.Errorf("botUserID = %q, want U0BHE265YNN", got)
	}

	human := slackEventEnvelope{Authorizations: []slackEventAuthorization{{UserID: "U_HUMAN", IsBot: false}}}
	if got := human.botUserID(); got != "" {
		t.Errorf("botUserID for user-token install = %q, want empty", got)
	}
	if got := (slackEventEnvelope{}).botUserID(); got != "" {
		t.Errorf("botUserID with no authorizations = %q, want empty", got)
	}
	mixed := slackEventEnvelope{Authorizations: []slackEventAuthorization{
		{UserID: "U_HUMAN", IsBot: false},
		{UserID: "U_BOT", IsBot: true},
	}}
	if got := mixed.botUserID(); got != "U_BOT" {
		t.Errorf("botUserID with mixed authorizations = %q, want U_BOT", got)
	}
}
