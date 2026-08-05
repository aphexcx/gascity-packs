package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

const (
	testTeam        = "T0AAAAAAA"
	testChannel     = "C0AAAAAAA"
	botOllie        = "U0AAAAAA1"
	botRiley        = "U0AAAAAA2"
	selfSwitchboard = "U0SWITCH"
)

func routingDirectory(t *testing.T) *CompanyDirectory {
	t.Helper()
	return testDirectory(t) // ollie ambient; ollie+riley members and mention-eligible
}

// richTextUserBlocks builds the wire shape Slack emits for end-user client
// messages: a rich_text block wrapping a rich_text_section whose elements
// include {"type":"user","user_id":"U…"} leaves. This pins the shape the
// extractor parses.
func richTextUserBlocks(userIDs ...string) json.RawMessage {
	var elems []string
	for _, id := range userIDs {
		elems = append(elems, fmt.Sprintf(`{"type":"user","user_id":%q}`, id))
	}
	elems = append(elems, `{"type":"text","text":" hello"}`)
	block := fmt.Sprintf(
		`[{"type":"rich_text","elements":[{"type":"rich_text_section","elements":[%s]}]}]`,
		strings.Join(elems, ","),
	)
	return json.RawMessage(block)
}

func wakeNames(dec RouteDecision) map[string]string {
	m := make(map[string]string, len(dec.Wakes))
	for _, w := range dec.Wakes {
		m[w.Agent.Name] = w.Kind
	}
	return m
}

func TestAdmissibleSubtype(t *testing.T) {
	tests := []struct {
		subtype string
		want    bool
	}{
		{"", true},
		{"file_share", true},
		{"thread_broadcast", true},
		{"bot_message", true},
		{"channel_join", false},
		{"channel_topic", false},
		{"channel_leave", false},
		{"message_changed", false},
		{"message_deleted", false},
		{"tombstone", false},
	}
	for _, tt := range tests {
		if got := AdmissibleSubtype(tt.subtype); got != tt.want {
			t.Errorf("AdmissibleSubtype(%q) = %v, want %v", tt.subtype, got, tt.want)
		}
	}
}

func TestExtractMentionIDs(t *testing.T) {
	tests := []struct {
		name   string
		blocks json.RawMessage
		text   string
		want   []string
	}{
		{"rich_text alone", richTextUserBlocks(botRiley), "", []string{botRiley}},
		{"text token alone", nil, fmt.Sprintf("hey <@%s> look", botRiley), []string{botRiley}},
		{"text token with label", nil, fmt.Sprintf("hey <@%s|riley> look", botRiley), []string{botRiley}},
		{"union of both deduped", richTextUserBlocks(botOllie), fmt.Sprintf("<@%s> <@%s>", botOllie, botRiley), []string{botOllie, botRiley}},
		{"literal at-name never matches", nil, "hey @riley please", nil},
		{"empty inputs", nil, "", nil},
		{"malformed blocks fall back to text", json.RawMessage(`{not an array`), fmt.Sprintf("<@%s>", botOllie), []string{botOllie}},
		{"non-user elements ignored", json.RawMessage(`[{"type":"rich_text","elements":[{"type":"rich_text_section","elements":[{"type":"text","text":"@riley"}]}]}]`), "", nil},
		{"channel and special tokens ignored", nil, "<!here> <#C1|general> <!subteam^S1|team>", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractMentionIDs(tt.blocks, tt.text)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExtractMentionIDs = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestComputeWakeSetAcceptanceRule1 — an unmentioned human message wakes
// exactly the configured ambient agents.
func TestComputeWakeSetAcceptanceRule1(t *testing.T) {
	dir := routingDirectory(t)
	msg := CompanyMessage{TeamID: testTeam, ChannelID: testChannel, UserID: "U0HUMAN", Text: "hello team"}
	dec := ComputeWakeSet(dir, msg, selfSwitchboard)
	if dec.Room == nil {
		t.Fatal("Room nil for company channel")
	}
	if dec.Author != AuthorHuman {
		t.Errorf("Author = %v, want human", dec.Author)
	}
	got := wakeNames(dec)
	if len(got) != 1 || got["ollie"] != wakeKindAmbient {
		t.Errorf("wakes = %v, want {ollie: ambient}", got)
	}
}

// TestComputeWakeSetAcceptanceRule2 — a human @riley wakes Riley only, not
// ambient Ollie (mentions are exclusive).
func TestComputeWakeSetAcceptanceRule2(t *testing.T) {
	dir := routingDirectory(t)
	msg := CompanyMessage{
		TeamID: testTeam, ChannelID: testChannel, UserID: "U0HUMAN",
		Text: fmt.Sprintf("<@%s> please look", botRiley),
	}
	dec := ComputeWakeSet(dir, msg, selfSwitchboard)
	got := wakeNames(dec)
	if len(got) != 1 || got["riley"] != wakeKindTargeted {
		t.Errorf("wakes = %v, want {riley: targeted}", got)
	}
	if _, ambient := got["ollie"]; ambient {
		t.Error("ambient Ollie woken despite exclusive @riley mention")
	}
}

// TestComputeWakeSetMentionViaRichTextOnly — a mention carried only in a
// rich_text block (no text token) still wakes the targeted agent.
func TestComputeWakeSetMentionViaRichTextOnly(t *testing.T) {
	dir := routingDirectory(t)
	msg := CompanyMessage{
		TeamID: testTeam, ChannelID: testChannel, UserID: "U0HUMAN",
		Blocks: richTextUserBlocks(botRiley),
	}
	dec := ComputeWakeSet(dir, msg, selfSwitchboard)
	got := wakeNames(dec)
	if len(got) != 1 || got["riley"] != wakeKindTargeted {
		t.Errorf("wakes = %v, want {riley: targeted}", got)
	}
}

// TestComputeWakeSetMultipleMentionsUnion — mentions from blocks and text
// union, each named receiver woken exactly once.
func TestComputeWakeSetMultipleMentionsUnion(t *testing.T) {
	dir := routingDirectory(t)
	msg := CompanyMessage{
		TeamID: testTeam, ChannelID: testChannel, UserID: "U0HUMAN",
		Blocks: richTextUserBlocks(botOllie),
		Text:   fmt.Sprintf("also <@%s> and <@%s>", botRiley, botRiley),
	}
	dec := ComputeWakeSet(dir, msg, selfSwitchboard)
	got := wakeNames(dec)
	if len(got) != 2 || got["ollie"] != wakeKindTargeted || got["riley"] != wakeKindTargeted {
		t.Errorf("wakes = %v, want {ollie: targeted, riley: targeted}", got)
	}
	if len(dec.Wakes) != 2 {
		t.Errorf("wake count = %d, want 2 (each receiver once)", len(dec.Wakes))
	}
}

// TestComputeWakeSetAcceptanceRule4 — the deliver-nothing cases: textual
// mentions, unknown bots, webhook posts, self messages, wrong rooms, and
// non-allowlisted subtypes.
func TestComputeWakeSetAcceptanceRule4(t *testing.T) {
	dir := routingDirectory(t)
	tests := []struct {
		name       string
		msg        CompanyMessage
		wantRoom   bool // whether a company room should be resolved
		wantReason string
	}{
		{
			name:       "unknown bot",
			msg:        CompanyMessage{TeamID: testTeam, ChannelID: testChannel, BotID: "B999", UserID: "U0OUTSIDER", Text: fmt.Sprintf("<@%s>", botRiley)},
			wantRoom:   true,
			wantReason: wakeReasonUnknownBot,
		},
		{
			name:       "webhook post no user",
			msg:        CompanyMessage{TeamID: testTeam, ChannelID: testChannel, Subtype: "bot_message", Text: fmt.Sprintf("<@%s>", botRiley)},
			wantRoom:   true,
			wantReason: wakeReasonUnknownBot,
		},
		{
			name:       "self message",
			msg:        CompanyMessage{TeamID: testTeam, ChannelID: testChannel, UserID: selfSwitchboard, Text: fmt.Sprintf("<@%s>", botRiley)},
			wantRoom:   true,
			wantReason: wakeReasonCompanySelf,
		},
		{
			name:       "wrong room",
			msg:        CompanyMessage{TeamID: testTeam, ChannelID: "C_OTHER", UserID: "U0HUMAN", Text: "hello"},
			wantRoom:   false,
			wantReason: wakeReasonNotCompanyRoom,
		},
		{
			name:       "non-allowlisted subtype channel_join",
			msg:        CompanyMessage{TeamID: testTeam, ChannelID: testChannel, Subtype: "channel_join", UserID: "U0HUMAN"},
			wantRoom:   true,
			wantReason: wakeReasonSubtypeNotAdmissible,
		},
		{
			name:       "non-allowlisted subtype channel_topic",
			msg:        CompanyMessage{TeamID: testTeam, ChannelID: testChannel, Subtype: "channel_topic", UserID: "U0HUMAN"},
			wantRoom:   true,
			wantReason: wakeReasonSubtypeNotAdmissible,
		},
		{
			name:       "textual mention wakes nobody targeted",
			msg:        CompanyMessage{TeamID: testTeam, ChannelID: testChannel, BotID: "B999", UserID: "U0OUTSIDER", Text: "hey @riley"},
			wantRoom:   true,
			wantReason: wakeReasonUnknownBot,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec := ComputeWakeSet(dir, tt.msg, selfSwitchboard)
			if len(dec.Wakes) != 0 {
				t.Errorf("wakes = %v, want none", wakeNames(dec))
			}
			if tt.wantRoom && dec.Room == nil {
				t.Errorf("Room nil, want resolved company room")
			}
			if !tt.wantRoom && dec.Room != nil {
				t.Errorf("Room resolved, want nil (fall through to legacy)")
			}
			if dec.Reason != tt.wantReason {
				t.Errorf("Reason = %q, want %q", dec.Reason, tt.wantReason)
			}
		})
	}
}

// TestComputeWakeSetTextualMentionKeepsAmbient — a literal @riley in a
// human message does not target Riley; ambient routing still applies.
func TestComputeWakeSetTextualMentionKeepsAmbient(t *testing.T) {
	dir := routingDirectory(t)
	msg := CompanyMessage{TeamID: testTeam, ChannelID: testChannel, UserID: "U0HUMAN", Text: "hey @riley can you help"}
	dec := ComputeWakeSet(dir, msg, selfSwitchboard)
	got := wakeNames(dec)
	if _, targeted := got["riley"]; targeted {
		t.Error("literal @riley targeted Riley; want no targeted wake")
	}
	if got["ollie"] != wakeKindAmbient {
		t.Errorf("wakes = %v, want ambient ollie", got)
	}
}

// TestComputeWakeSetAcceptanceRule6 — no unmentioned agent (bot) message
// produces ambient delivery or a loop.
func TestComputeWakeSetAcceptanceRule6(t *testing.T) {
	dir := routingDirectory(t)
	tests := []struct {
		name       string
		msg        CompanyMessage
		wantAuthor AuthorClass
		wantReason string
	}{
		{
			// Phase 2c: a resolved company bot with no native company mention
			// wakes nobody (was company_bot_phase2 in Phase 1). ResolvedBotUserID
			// stands in for the delivery worker's bots.info resolution so the
			// pure router stays offline.
			name:       "registered company bot no mention",
			msg:        CompanyMessage{TeamID: testTeam, ChannelID: testChannel, Subtype: "bot_message", BotID: "B0RILEY", UserID: botRiley, ResolvedBotUserID: botRiley, Text: "status update"},
			wantAuthor: AuthorCompanyBot,
			wantReason: wakeReasonCompanyBotNoMention,
		},
		{
			name:       "unknown bot no mention",
			msg:        CompanyMessage{TeamID: testTeam, ChannelID: testChannel, BotID: "B999", UserID: "U0OUTSIDER", Text: "beep"},
			wantAuthor: AuthorBot,
			wantReason: wakeReasonUnknownBot,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec := ComputeWakeSet(dir, tt.msg, selfSwitchboard)
			if len(dec.Wakes) != 0 {
				t.Errorf("wakes = %v, want none (no ambient for bot author)", wakeNames(dec))
			}
			if dec.Author != tt.wantAuthor {
				t.Errorf("Author = %v, want %v", dec.Author, tt.wantAuthor)
			}
			if dec.Reason != tt.wantReason {
				t.Errorf("Reason = %q, want %q", dec.Reason, tt.wantReason)
			}
		})
	}
}

// TestClassifyAuthorFailsClosedOnRawUser — a bot-authored message whose raw
// `user` equals a registered agent's bot_user_id must NOT be classified as a
// company bot when authoritative resolution left ResolvedBotUserID empty (a
// definitive unknown / corroboration mismatch during delivery). It fails closed
// to an unknown bot, so no peer wake — and thus no keyless pointer — is emitted.
func TestClassifyAuthorFailsClosedOnRawUser(t *testing.T) {
	dir := routingDirectory(t)
	// Raw user is Riley's bot_user_id, but bots.info resolution failed so the
	// delivery worker left ResolvedBotUserID empty.
	msg := CompanyMessage{
		TeamID:    testTeam,
		ChannelID: testChannel,
		Subtype:   "bot_message",
		BotID:     "B0RILEY",
		UserID:    botRiley, // matches a directory agent's bot_user_id
		Text:      "<@" + botOllie + "> please review",
	}
	if got := classifyAuthor(dir, msg, selfSwitchboard); got != AuthorBot {
		t.Fatalf("classifyAuthor = %v, want AuthorBot (no raw-user fallback to company bot)", got)
	}
	dec := ComputeWakeSet(dir, msg, selfSwitchboard)
	if dec.Author != AuthorBot || dec.Reason != wakeReasonUnknownBot {
		t.Errorf("decision author=%v reason=%q, want bot/unknown_bot", dec.Author, dec.Reason)
	}
	if len(dec.Wakes) != 0 {
		t.Errorf("fail-open woke %v, want nobody", wakeNames(dec))
	}
	// The corroborated resolution still classifies a company bot.
	msg.ResolvedBotUserID = botRiley
	if got := classifyAuthor(dir, msg, selfSwitchboard); got != AuthorCompanyBot {
		t.Errorf("resolved company bot = %v, want AuthorCompanyBot", got)
	}
}

// TestComputeWakeSetNoDirectory — a nil directory yields Room == nil so the
// caller falls through to the legacy path.
func TestComputeWakeSetNoDirectory(t *testing.T) {
	msg := CompanyMessage{TeamID: testTeam, ChannelID: testChannel, UserID: "U0HUMAN", Text: "hi"}
	dec := ComputeWakeSet(nil, msg, selfSwitchboard)
	if dec.Room != nil {
		t.Error("Room non-nil with nil directory")
	}
	if len(dec.Wakes) != 0 {
		t.Error("wakes with nil directory")
	}
	if dec.Reason != wakeReasonNoDirectory {
		t.Errorf("Reason = %q, want %q", dec.Reason, wakeReasonNoDirectory)
	}
}

// TestComputeWakeSetMentionedNotEligible — a human mention of a member that
// is not mention-eligible wakes nobody and suppresses ambient.
func TestComputeWakeSetMentionedNotEligible(t *testing.T) {
	f := baseDirectoryFile()
	f.Rooms[0].MentionWake = []string{"riley"} // ollie member but not mention-eligible
	dir, err := ParseCompanyDirectory(marshalDirectory(t, f))
	if err != nil {
		t.Fatalf("ParseCompanyDirectory: %v", err)
	}
	msg := CompanyMessage{TeamID: testTeam, ChannelID: testChannel, UserID: "U0HUMAN", Text: fmt.Sprintf("<@%s>", botOllie)}
	dec := ComputeWakeSet(dir, msg, selfSwitchboard)
	if len(dec.Wakes) != 0 {
		t.Errorf("wakes = %v, want none (ollie not mention-eligible)", wakeNames(dec))
	}
	if dec.Reason != wakeReasonMentionedNoEligible {
		t.Errorf("Reason = %q, want %q", dec.Reason, wakeReasonMentionedNoEligible)
	}
}

// TestComputeWakeSetMentionedNotMember — a human mention of a directory
// agent that is not a member of this room wakes nobody and suppresses
// ambient (exclusivity still bites).
func TestComputeWakeSetMentionedNotMember(t *testing.T) {
	f := baseDirectoryFile()
	// Add a third agent that is NOT a member of the room.
	f.Agents = append(f.Agents, CompanyAgent{Name: "quinn", AppID: "A0AAAAAA3", BotUserID: "U0AAAAAA3"})
	dir, err := ParseCompanyDirectory(marshalDirectory(t, f))
	if err != nil {
		t.Fatalf("ParseCompanyDirectory: %v", err)
	}
	msg := CompanyMessage{TeamID: testTeam, ChannelID: testChannel, UserID: "U0HUMAN", Text: "<@U0AAAAAA3>"}
	dec := ComputeWakeSet(dir, msg, selfSwitchboard)
	if len(dec.Wakes) != 0 {
		t.Errorf("wakes = %v, want none (quinn not a member)", wakeNames(dec))
	}
	if _, ambient := wakeNames(dec)["ollie"]; ambient {
		t.Error("ambient Ollie woken despite a company-agent mention (exclusivity broken)")
	}
	if dec.Reason != wakeReasonMentionedNoEligible {
		t.Errorf("Reason = %q, want %q", dec.Reason, wakeReasonMentionedNoEligible)
	}
}

// TestComputeWakeSetEmptyDirectoryInert — an empty (but valid) directory
// resolves no rooms.
func TestComputeWakeSetEmptyDirectoryInert(t *testing.T) {
	dir, err := ParseCompanyDirectory(marshalDirectory(t, companyDirectoryFile{SchemaVersion: 1}))
	if err != nil {
		t.Fatalf("ParseCompanyDirectory(empty): %v", err)
	}
	msg := CompanyMessage{TeamID: testTeam, ChannelID: testChannel, UserID: "U0HUMAN", Text: "hi"}
	dec := ComputeWakeSet(dir, msg, selfSwitchboard)
	if dec.Room != nil {
		t.Error("Room non-nil for empty directory")
	}
	if dec.Reason != wakeReasonNotCompanyRoom {
		t.Errorf("Reason = %q, want %q", dec.Reason, wakeReasonNotCompanyRoom)
	}
}
