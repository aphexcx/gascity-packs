package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fixturePath resolves a golden fixture under tests/fixtures/company/.
func fixturePath(name string) string {
	return filepath.Join("..", "tests", "fixtures", "company", name)
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(fixturePath(name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

// ---- fixture parity --------------------------------------------------------

// TestSanitizerFixtureParity reproduces every golden delegation filename
// byte-for-byte from its raw (team, channel, ts).
func TestSanitizerFixtureParity(t *testing.T) {
	var f struct {
		Cases []struct {
			TeamID    string `json:"team_id"`
			ChannelID string `json:"channel_id"`
			TS        string `json:"ts"`
			Expected  string `json:"expected_filename"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(readFixture(t, "sanitizer.json"), &f); err != nil {
		t.Fatalf("decode sanitizer.json: %v", err)
	}
	if len(f.Cases) == 0 {
		t.Fatal("no sanitizer cases")
	}
	for _, c := range f.Cases {
		got := companyDelegationFilename(c.TeamID, c.ChannelID, c.TS)
		if got != c.Expected {
			t.Errorf("companyDelegationFilename(%q,%q,%q) = %q, want %q", c.TeamID, c.ChannelID, c.TS, got, c.Expected)
		}
	}
}

// TestDelegationFixtureRoundTrip parses the golden delegation record and
// re-marshals it byte-for-byte.
func TestDelegationFixtureRoundTrip(t *testing.T) {
	raw := readFixture(t, "delegation.json")
	rec, err := companyParseDelegation(raw)
	if err != nil {
		t.Fatalf("parse delegation: %v", err)
	}
	out, err := companyMarshalDelegation(rec)
	if err != nil {
		t.Fatalf("marshal delegation: %v", err)
	}
	if !bytes.Equal(bytes.TrimRight(raw, "\n"), out) {
		t.Errorf("delegation re-marshal not byte-identical:\n got: %s\nwant: %s", out, raw)
	}
}

// TestIntentFixtureRoundTrip parses the golden intent record and re-marshals
// it byte-for-byte.
func TestIntentFixtureRoundTrip(t *testing.T) {
	raw := readFixture(t, "intent.json")
	rec, err := companyParseIntent(raw)
	if err != nil {
		t.Fatalf("parse intent: %v", err)
	}
	out, err := companyMarshalIntent(rec)
	if err != nil {
		t.Fatalf("marshal intent: %v", err)
	}
	if !bytes.Equal(bytes.TrimRight(raw, "\n"), out) {
		t.Errorf("intent re-marshal not byte-identical:\n got: %s\nwant: %s", out, raw)
	}
}

// ---- trust checklist -------------------------------------------------------

func TestEvaluatePeerTrust(t *testing.T) {
	dir := testDirectory(t)
	room, _ := dir.RoomByChannel(testTeam, testChannel)
	binds, _, _ := ParseCompanyBindings(marshalBindings(t, baseBindingsFile()), dir)
	ollie, _ := dir.AgentByName("ollie")
	riley, _ := dir.AgentByName("riley")
	mentioned := map[string]bool{riley.BotUserID: true}

	if got := evaluatePeerTrust(dir, binds, room, ollie, riley, mentioned); got != peerTrustOK {
		t.Errorf("valid pair reason = %q, want ok", got)
	}
	if got := evaluatePeerTrust(dir, binds, room, ollie, ollie, map[string]bool{ollie.BotUserID: true}); got != peerTrustAuthorIsReceiver {
		t.Errorf("self pair reason = %q, want author_is_receiver", got)
	}
	if got := evaluatePeerTrust(dir, binds, room, ollie, riley, map[string]bool{}); got != peerTrustReceiverNotMentioned {
		t.Errorf("unmentioned reason = %q, want not_mentioned", got)
	}
	if got := evaluatePeerTrust(dir, binds, room, nil, riley, mentioned); got != peerTrustAuthorNotCompanyBot {
		t.Errorf("nil author reason = %q, want author_not_company_bot", got)
	}
	// Unbound receiver: bind only ollie.
	onlyOllie, _, _ := ParseCompanyBindings(marshalBindings(t, companyBindingsFile{
		SchemaVersion: 1,
		Bindings:      []CompanyBinding{{Room: "orchestrator-team", Agent: "ollie", Session: "ollie-main"}},
	}), dir)
	if got := evaluatePeerTrust(dir, onlyOllie, room, ollie, riley, mentioned); got != peerTrustReceiverUnbound {
		t.Errorf("unbound reason = %q, want receiver_unbound", got)
	}
}

// ---- correlation + claim ---------------------------------------------------

const (
	fixedNow      = "2026-07-17T12:05:00Z"
	delegationTS  = "1700000000.000500"
	humanRootTS   = "1700000000.000100"
	resultTS      = "1700000000.000900"
	fixtureNonce  = "gcs-0123456789abcdef0123"
	fixtureBodySH = "d5306ea54901f4318eff9ef7982b411b3b798c70dbc3b120111983b43fb940d9"
)

func fixedClock(t *testing.T) func() time.Time {
	t.Helper()
	now, err := time.Parse(time.RFC3339, fixedNow)
	if err != nil {
		t.Fatalf("parse fixed now: %v", err)
	}
	return func() time.Time { return now }
}

func peerTestEnv(t *testing.T) companyPeerEnv {
	t.Helper()
	root := t.TempDir()
	env := companyPeerEnv{
		delegationsDir: filepath.Join(root, "company-delegations"),
		intentsDir:     filepath.Join(root, "company-delegation-intents"),
		locksDir:       filepath.Join(root, "locks"),
		retention:      companyReceiptRetention,
		now:            fixedClock(t),
	}
	for _, d := range []string{env.delegationsDir, env.intentsDir, env.locksDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	return env
}

func pendingRecord(ts string) *companyDelegationRecord {
	return &companyDelegationRecord{
		SchemaVersion:              1,
		Generation:                 1,
		Nonce:                      fixtureNonce,
		Room:                       "orchestrator-team",
		TeamID:                     testTeam,
		ChannelID:                  testChannel,
		TS:                         ts,
		ThreadRootTS:               humanRootTS,
		RequesterAgent:             "ollie",
		RequesterBotUserID:         botOllie,
		RequesterSession:           "ollie-main",
		ExpectedResponderAgent:     "riley",
		ExpectedResponderBotUserID: botRiley,
		CreatedAt:                  "2026-07-17T12:00:05Z",
		TTLSeconds:                 86400,
		Status:                     companyDelegationPending,
	}
}

func writeRecord(t *testing.T, env companyPeerEnv, rec *companyDelegationRecord) string {
	t.Helper()
	name := companyDelegationFilename(rec.TeamID, rec.ChannelID, rec.TS)
	data, err := companyMarshalDelegation(rec)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	if err := os.WriteFile(filepath.Join(env.delegationsDir, name), data, 0o600); err != nil {
		t.Fatalf("write record: %v", err)
	}
	return name
}

func resultMessage(nonce, delegTS string) CompanyMessage {
	meta := `{"event_type":"gc_delegation_result","event_payload":{"v":1,"nonce":"` + nonce + `","delegation_ts":"` + delegTS + `"}}`
	return CompanyMessage{
		TeamID:            testTeam,
		ChannelID:         testChannel,
		TS:                resultTS,
		ThreadTS:          humanRootTS,
		BotID:             "B0RILEY",
		ResolvedBotUserID: botRiley,
		Subtype:           "bot_message",
		Text:              "<@" + botOllie + "> done",
		Metadata:          json.RawMessage(meta),
	}
}

func peerAgents(t *testing.T) (author *CompanyAgent, ollieWake []WakeTarget) {
	t.Helper()
	dir := testDirectory(t)
	riley, _ := dir.AgentByName("riley")
	ollie, _ := dir.AgentByName("ollie")
	return riley, []WakeTarget{{Agent: *ollie, Kind: wakeKindPeerDelegation}}
}

// TestResolvePeerWakesClaimsResult — a metadata-gated threaded result claims
// the pending record and wakes the requester as peer_result.
func TestResolvePeerWakesClaimsResult(t *testing.T) {
	env := peerTestEnv(t)
	name := writeRecord(t, env, pendingRecord(delegationTS))
	author, wakes := peerAgents(t)

	out, park, err := env.resolvePeerWakes(resultMessage(fixtureNonce, delegationTS), author, wakes)
	if err != nil || park != "" {
		t.Fatalf("resolvePeerWakes err=%v park=%q", err, park)
	}
	if len(out) != 1 || out[0].Kind != wakeKindPeerResult || out[0].Agent.Name != "ollie" {
		t.Fatalf("out = %+v, want single peer_result ollie", out)
	}
	if out[0].DelegationKey != name {
		t.Errorf("delegation key = %q, want %q", out[0].DelegationKey, name)
	}
	// Record transitioned to result_claimed with this result ts.
	claimed := readRecord(t, env, name)
	if claimed.Status != companyDelegationClaimed || claimed.ResultTS != resultTS {
		t.Errorf("record = %+v, want result_claimed ts=%s", claimed, resultTS)
	}

	// Replay: a second delivery claims nothing new and stays peer_result.
	gen := claimed.Generation
	out2, park2, err2 := env.resolvePeerWakes(resultMessage(fixtureNonce, delegationTS), author, wakes)
	if err2 != nil || park2 != "" || len(out2) != 1 || out2[0].Kind != wakeKindPeerResult {
		t.Fatalf("replay out=%+v park=%q err=%v", out2, park2, err2)
	}
	if again := readRecord(t, env, name); again.Generation != gen {
		t.Errorf("replay bumped generation %d -> %d (claimed twice)", gen, again.Generation)
	}
}

// TestResolvePeerWakesClarifyingQuestion — a responder post without result
// metadata delivers as ordinary peer input and claims nothing.
func TestResolvePeerWakesClarifyingQuestion(t *testing.T) {
	env := peerTestEnv(t)
	name := writeRecord(t, env, pendingRecord(delegationTS))
	author, wakes := peerAgents(t)

	msg := resultMessage(fixtureNonce, delegationTS)
	msg.Metadata = nil // hand-typed clarifying question, no breadcrumb

	out, park, err := env.resolvePeerWakes(msg, author, wakes)
	if err != nil || park != "" {
		t.Fatalf("err=%v park=%q", err, park)
	}
	if len(out) != 1 || out[0].Kind != wakeKindPeerInput {
		t.Fatalf("out = %+v, want peer_input (no claim)", out)
	}
	if out[0].DelegationKey != "" {
		t.Errorf("peer_input carried a delegation key: %q", out[0].DelegationKey)
	}
	if rec := readRecord(t, env, name); rec.Status != companyDelegationPending {
		t.Errorf("record status = %s, want still pending (claimed nothing)", rec.Status)
	}
}

// TestResolvePeerWakesWrongNonce — result metadata that fails the nonce gate
// delivers as peer input, claiming nothing.
func TestResolvePeerWakesWrongNonce(t *testing.T) {
	env := peerTestEnv(t)
	name := writeRecord(t, env, pendingRecord(delegationTS))
	author, wakes := peerAgents(t)

	out, park, err := env.resolvePeerWakes(resultMessage("gcs-deadbeefdeadbeefdead", delegationTS), author, wakes)
	if err != nil || park != "" {
		t.Fatalf("err=%v park=%q", err, park)
	}
	if len(out) != 1 || out[0].Kind != wakeKindPeerInput {
		t.Fatalf("out = %+v, want peer_input on nonce mismatch", out)
	}
	if rec := readRecord(t, env, name); rec.Status != companyDelegationPending {
		t.Errorf("record claimed on nonce mismatch: %s", rec.Status)
	}
}

// TestResolvePeerWakesAmbiguous — two pending records for one claim tuple park
// the receipt fail-closed.
func TestResolvePeerWakesAmbiguous(t *testing.T) {
	env := peerTestEnv(t)
	writeRecord(t, env, pendingRecord(delegationTS))
	writeRecord(t, env, pendingRecord("1700000000.000600")) // same tuple, different ts
	author, wakes := peerAgents(t)

	out, park, err := env.resolvePeerWakes(resultMessage(fixtureNonce, delegationTS), author, wakes)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if park != peerParkAmbiguousPending {
		t.Fatalf("park = %q, want ambiguous_pending_delegations; out=%+v", park, out)
	}
}

// TestResolvePeerWakesCorrelationPending — no delegation record but a posting
// intent for the tuple parks correlation_pending.
func TestResolvePeerWakesCorrelationPending(t *testing.T) {
	env := peerTestEnv(t)
	writePostingIntent(t, env, false)
	author, wakes := peerAgents(t)

	out, park, err := env.resolvePeerWakes(resultMessage(fixtureNonce, delegationTS), author, wakes)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if park != peerParkCorrelationPending {
		t.Fatalf("park = %q, want correlation_pending; out=%+v", park, out)
	}
}

// TestResolvePeerWakesUnmatchedIsPeerInput — result metadata with neither a
// record nor an intent delivers as ordinary peer input.
func TestResolvePeerWakesUnmatchedIsPeerInput(t *testing.T) {
	env := peerTestEnv(t)
	author, wakes := peerAgents(t)
	out, park, err := env.resolvePeerWakes(resultMessage(fixtureNonce, delegationTS), author, wakes)
	if err != nil || park != "" {
		t.Fatalf("err=%v park=%q", err, park)
	}
	if len(out) != 1 || out[0].Kind != wakeKindPeerInput {
		t.Fatalf("out = %+v, want peer_input for an unmatched reply", out)
	}
}

// TestResolvePeerWakesExpiredRecord — a TTL-expired pending record is rewritten
// expired and does not claim; with no intent the reply is peer input.
func TestResolvePeerWakesExpiredRecord(t *testing.T) {
	env := peerTestEnv(t)
	rec := pendingRecord(delegationTS)
	rec.CreatedAt = "2026-07-16T00:00:00Z" // > ttl (86400s) before fixedNow
	name := writeRecord(t, env, rec)
	author, wakes := peerAgents(t)

	out, park, err := env.resolvePeerWakes(resultMessage(fixtureNonce, delegationTS), author, wakes)
	if err != nil || park != "" {
		t.Fatalf("err=%v park=%q", err, park)
	}
	if len(out) != 1 || out[0].Kind != wakeKindPeerInput {
		t.Fatalf("out = %+v, want peer_input for an expired record", out)
	}
	if got := readRecord(t, env, name); got.Status != companyDelegationExpired {
		t.Errorf("expired record status = %s, want expired", got.Status)
	}
}

// TestCountStalePostingIntents — an intent past its retry_deadline is counted.
func TestCountStalePostingIntents(t *testing.T) {
	env := peerTestEnv(t)
	if got := env.countStalePostingIntents(); got != 0 {
		t.Fatalf("empty count = %d, want 0", got)
	}
	writePostingIntent(t, env, true) // stale (deadline in the past)
	if got := env.countStalePostingIntents(); got != 1 {
		t.Errorf("stale count = %d, want 1", got)
	}
}

// ---- fail-closed parse + unknown-field-preserving rewrite (G5) -------------

// TestCompanyParseDelegationFailClosed — the parser rejects a record missing
// any required field (or with generation < 1 / an unknown status), matching
// Python's parse_delegation, so a corrupt record is never treated as live.
func TestCompanyParseDelegationFailClosed(t *testing.T) {
	raw, err := companyMarshalDelegation(pendingRecord(delegationTS))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := companyParseDelegation(raw); err != nil {
		t.Fatalf("golden record rejected: %v", err)
	}
	required := []string{
		"nonce", "room", "team_id", "channel_id", "ts", "thread_root_ts",
		"requester_agent", "requester_bot_user_id", "requester_session",
		"expected_responder_agent", "expected_responder_bot_user_id",
		"created_at", "status",
	}
	for _, field := range required {
		obj := decodeObj(t, raw)
		delete(obj, field)
		if _, err := companyParseDelegation(reencode(t, obj)); err == nil {
			t.Errorf("parser accepted record missing required field %q", field)
		}
	}
	// generation < 1 and an unknown status are also fail-closed.
	genZero := decodeObj(t, raw)
	genZero["generation"] = json.RawMessage(`0`)
	if _, err := companyParseDelegation(reencode(t, genZero)); err == nil {
		t.Error("parser accepted generation 0")
	}
	badStatus := decodeObj(t, raw)
	badStatus["status"] = json.RawMessage(`"garbage"`)
	if _, err := companyParseDelegation(reencode(t, badStatus)); err == nil {
		t.Error("parser accepted unknown status")
	}
}

// TestClaimPreservesUnknownFields — a claim rewrite retains a record field this
// Go version does not model, so an additive field a newer (Python) writer added
// survives the pending -> result_claimed transition byte-meaningfully.
func TestClaimPreservesUnknownFields(t *testing.T) {
	env := peerTestEnv(t)
	raw, err := companyMarshalDelegation(pendingRecord(delegationTS))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	obj := decodeObj(t, raw)
	obj["future_field"] = json.RawMessage(`"keep-me"`)
	name := companyDelegationFilename(testTeam, testChannel, delegationTS)
	if err := os.WriteFile(filepath.Join(env.delegationsDir, name), reencode(t, obj), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	author, wakes := peerAgents(t)
	out, park, err := env.resolvePeerWakes(resultMessage(fixtureNonce, delegationTS), author, wakes)
	if err != nil || park != "" || len(out) != 1 || out[0].Kind != wakeKindPeerResult {
		t.Fatalf("resolve out=%+v park=%q err=%v, want peer_result claim", out, park, err)
	}

	after := decodeObj(t, readRaw(t, env, name))
	if string(after["future_field"]) != `"keep-me"` {
		t.Errorf("unknown field dropped by claim rewrite: %q", after["future_field"])
	}
	if string(after["status"]) != `"result_claimed"` {
		t.Errorf("status = %s, want result_claimed", after["status"])
	}
	if string(after["result_ts"]) != `"`+resultTS+`"` {
		t.Errorf("result_ts = %s, want %s", after["result_ts"], resultTS)
	}
}

// TestResolveResultWakeMalformedRecordParksNoRewrite — a record missing a
// required field fails closed: with a posting intent for the tuple the result
// parks correlation_pending (claim refuses), and the malformed record is never
// rewritten.
func TestResolveResultWakeMalformedRecordParksNoRewrite(t *testing.T) {
	env := peerTestEnv(t)
	raw, err := companyMarshalDelegation(pendingRecord(delegationTS))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	obj := decodeObj(t, raw)
	delete(obj, "nonce") // missing required field -> not claimable
	malformed := reencode(t, obj)
	name := companyDelegationFilename(testTeam, testChannel, delegationTS)
	if err := os.WriteFile(filepath.Join(env.delegationsDir, name), malformed, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	writePostingIntent(t, env, false) // in-flight intent for the tuple

	author, wakes := peerAgents(t)
	_, park, err := env.resolvePeerWakes(resultMessage(fixtureNonce, delegationTS), author, wakes)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if park != peerParkCorrelationPending {
		t.Fatalf("park = %q, want correlation_pending (malformed record not claimable)", park)
	}
	if got := readRaw(t, env, name); !bytes.Equal(got, malformed) {
		t.Errorf("malformed record was rewritten:\n got: %s\nwant: %s", got, malformed)
	}
}

// ---- helpers ---------------------------------------------------------------

func decodeObj(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("decode object: %v", err)
	}
	return obj
}

func reencode(t *testing.T, obj map[string]json.RawMessage) []byte {
	t.Helper()
	data, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		t.Fatalf("reencode object: %v", err)
	}
	return data
}

func readRaw(t *testing.T, env companyPeerEnv, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(env.delegationsDir, name))
	if err != nil {
		t.Fatalf("read raw %s: %v", name, err)
	}
	return data
}

func readRecord(t *testing.T, env companyPeerEnv, name string) *companyDelegationRecord {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(env.delegationsDir, name))
	if err != nil {
		t.Fatalf("read record %s: %v", name, err)
	}
	rec, err := companyParseDelegation(data)
	if err != nil {
		t.Fatalf("parse record %s: %v", name, err)
	}
	return rec
}

// writePostingIntent writes a "posting" intent for the (ollie -> riley) tuple.
// stale controls whether retry_deadline is in the past relative to fixedNow.
func writePostingIntent(t *testing.T, env companyPeerEnv, stale bool) {
	t.Helper()
	deadline := "2026-07-17T12:30:00Z" // after fixedNow
	if stale {
		deadline = "2026-07-17T11:30:00Z" // before fixedNow
	}
	rec := &companyIntentRecord{
		SchemaVersion:    1,
		Nonce:            fixtureNonce,
		RetrySeq:         0,
		Status:           companyIntentStatusPosted,
		Attempts:         1,
		MaxAttempts:      3,
		CreatedAt:        "2026-07-17T12:00:00Z",
		UpdatedAt:        "2026-07-17T12:00:00Z",
		RetryDeadline:    deadline,
		TTLSeconds:       86400,
		SourceAgent:      "ollie",
		SourceAppID:      "A0AAAAAA1",
		SourceBotUserID:  botOllie,
		TargetAgent:      "riley",
		TargetBotUserID:  botRiley,
		TeamID:           testTeam,
		ChannelID:        testChannel,
		Room:             "orchestrator-team",
		HumanRootTS:      humanRootTS,
		RequesterSession: "ollie-main",
		BodySHA256:       fixtureBodySH,
		PostedTS:         "",
	}
	data, err := companyMarshalIntent(rec)
	if err != nil {
		t.Fatalf("marshal intent: %v", err)
	}
	if err := os.WriteFile(filepath.Join(env.intentsDir, rec.Nonce+".json"), data, 0o600); err != nil {
		t.Fatalf("write intent: %v", err)
	}
}
