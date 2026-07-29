package main

import (
	"log"
	"net/http"
)

// company_acks.go — the config-gated visible-ack reactions (Slack company-rooms
// Phase 3b). The switchboard token (which already owns admission and the receipt
// lifecycle and carries reactions:write) puts a 👀 on the origin message at the
// first delivery attempt and converts it to ✅ / ⚠️ / removes it at the terminal
// transition, keyed off the durable AckState cursor so redrives are idempotent.
// All ack traffic is best-effort, runs inside the delivery worker (never the
// HTTP admission handler), never changes receipt status, and never counts as a
// delivery failure — the durable receipt, not the emoji, stays authoritative.

// AckState cursor values (durable, on the receipt).
const (
	ackStateNone = ""
	ackStateEyes = "eyes"
	// ackStateWarned is the durable intermediate cursor on the terminal-failed
	// path: the one threaded failure reply has been posted, and only the ⚠️
	// reaction remains. Committed in the same Update that records the successful
	// reply post, so a sweep-heal re-run (the ⚠️ add was rate-limited) retries
	// ONLY the reaction — the reply is never re-posted.
	ackStateWarned   = "warned"
	ackStateDone     = "done"
	ackStateDegraded = "degraded"
)

// Pinned ack emoji (reactions API names, no colons).
const (
	ackEmojiEyes    = "eyes"
	ackEmojiCheck   = "white_check_mark"
	ackEmojiWarning = "warning"
)

// ackOutcome classifies one reaction call per the S-taxonomy.
type ackOutcome int

const (
	ackSuccess   ackOutcome = iota // 2xx, already_reacted, or no_reaction on remove
	ackDegrade                     // too_many_emoji / too_many_reactions / any other definitive error
	ackUnchanged                   // ratelimited or transient HTTP/network — leave AckState as is
)

// react / reply dispatch each visible-ack call. The untyped hook takes
// precedence when set — it is the room-only test spy, which a test installs
// AFTER newCompanyGateway has wired the production token-aware hook — so a room
// ack test observes its spy. Production leaves the untyped hooks nil and routes
// through the token-parameterized hook, choosing the actor token per receipt
// (switchboard for rooms, owner agent for DMs).
func (g *companyGateway) react(token, method, channel, ts, name string) ackOutcome {
	if g.reactHook != nil {
		return g.reactHook(method, channel, ts, name)
	}
	if g.reactHookTok != nil {
		return g.reactHookTok(token, method, channel, ts, name)
	}
	return ackUnchanged
}

func (g *companyGateway) reply(token, channel, threadTS, text string) bool {
	if g.replyHook != nil {
		return g.replyHook(channel, threadTS, text)
	}
	if g.replyHookTok != nil {
		return g.replyHookTok(token, channel, threadTS, text)
	}
	return true
}

// hasReactHook reports whether any reaction hook is wired (either variant).
func (g *companyGateway) hasReactHook() bool {
	return g.reactHookTok != nil || g.reactHook != nil
}

// applyAdmissionAck runs the "" → eyes hook on the first delivery attempt for a
// receipt whose AckState is still empty. Idempotent across redrives (the
// AckState guard), gated on SLACK_COMPANY_VISIBLE_ACKS, never blocks delivery.
// The ack actor's token is chosen per receipt (switchboard for rooms, owner
// agent for DMs); a DM whose owner token is missing degrades silently.
func (g *companyGateway) applyAdmissionAck(r *IngressReceipt) {
	if g == nil || !g.visibleAcks || !g.hasReactHook() || r == nil {
		return
	}
	if r.AckState != ackStateNone {
		return
	}
	token, ok := g.ackActorToken(r)
	if !ok {
		g.noteDMAckDegraded(r) // DM owner token missing → acks degrade, but COUNTED
		return
	}
	switch g.react(token, "reactions.add", r.Origin.ChannelID, r.Origin.TS, ackEmojiEyes) {
	case ackSuccess:
		g.commitAckState(r, ackStateEyes)
	case ackDegrade:
		log.Printf("company: visible-ack admission degraded receipt=%s (no further ack calls)", r.ID)
		g.commitAckState(r, ackStateDegraded)
	case ackUnchanged:
		// ratelimited / transient: leave AckState "" so the next attempt retries.
	}
}

// applyTerminalAck runs the eyes → check/warn/remove hook when a receipt reaches
// a terminal status while its AckState is still "eyes". Keyed strictly off the
// "eyes" cursor so a degraded/absent 👀 is skipped and a terminal ack ratelimit
// leaves "eyes" for the sweep to heal. Gated, best-effort, never fails delivery.
func (g *companyGateway) applyTerminalAck(r *IngressReceipt) {
	if g == nil || !g.visibleAcks || !g.hasReactHook() || r == nil {
		return
	}
	// "eyes" is the pre-terminal cursor; "warned" is the terminal-failed
	// intermediate (reply posted, ⚠️ reaction still pending). Both are healable.
	if r.AckState != ackStateEyes && r.AckState != ackStateWarned {
		return
	}
	token, ok := g.ackActorToken(r)
	if !ok {
		g.noteDMAckDegraded(r) // DM owner token missing → acks degrade, but COUNTED
		return
	}
	switch r.Status {
	case ingressStatusDelivered:
		// 👀 → ✅ : remove is best-effort (its outcome does not drive the cursor).
		g.react(token, "reactions.remove", r.Origin.ChannelID, r.Origin.TS, ackEmojiEyes)
		g.settleTerminalAck(r, g.react(token, "reactions.add", r.Origin.ChannelID, r.Origin.TS, ackEmojiCheck))
	case ingressStatusFailed:
		// 👀 → ⚠️ plus EXACTLY ONE concise switchboard reply into the message's
		// thread root (entity-escaped, no live mentions). The reply is posted at
		// most once, guarded by the durable "warned" cursor: it is sent only on
		// the first terminal transition (AckState=="eyes") and the cursor advances
		// to "warned" in the same Update that records the successful post, so a
		// sweep-heal re-run for a rate-limited ⚠️ add (AckState=="warned") retries
		// only the reaction, never the reply. The sole re-post window is a crash
		// between the reply POST and the warned commit — the same narrow
		// at-most-once window the receipt lifecycle already tolerates.
		g.react(token, "reactions.remove", r.Origin.ChannelID, r.Origin.TS, ackEmojiEyes)
		if r.AckState == ackStateEyes {
			if !g.postFailureReply(r, token) {
				// Reply POST failed (transient/unknown): leave "eyes" so the next
				// sweep retries the reply before any ⚠️. Never advance the cursor
				// or apply the reaction on a receipt with no posted reply.
				return
			}
			g.commitAckState(r, ackStateWarned)
		}
		g.settleTerminalAck(r, g.react(token, "reactions.add", r.Origin.ChannelID, r.Origin.TS, ackEmojiWarning))
	case ingressStatusNoDelivery:
		// A green check on a message that woke nobody would misreport — remove
		// the 👀 only; the remove outcome drives the cursor.
		g.settleTerminalAck(r, g.react(token, "reactions.remove", r.Origin.ChannelID, r.Origin.TS, ackEmojiEyes))
	}
}

// settleTerminalAck advances the AckState after a terminal reaction: success →
// done, definitive degrade → degraded (stops further ack calls), ratelimit /
// transient → leave "eyes" so the retention sweep heals it.
func (g *companyGateway) settleTerminalAck(r *IngressReceipt, out ackOutcome) {
	switch out {
	case ackSuccess:
		g.commitAckState(r, ackStateDone)
	case ackDegrade:
		log.Printf("company: visible-ack terminal degraded receipt=%s", r.ID)
		g.commitAckState(r, ackStateDegraded)
	case ackUnchanged:
		// Leave the cursor as-is ("eyes", or "warned" on the failed path once the
		// reply has been posted): the terminal reaction was rate-limited (the
		// expected case for reactions.remove Tier 2) — the next sweep re-applies
		// only the reaction.
	}
}

// postFailureReply posts the threaded failure reply into the message's derived
// human root: body exactly "delivery failed for receipt <id>" (entity-escaped,
// no live mentions), through the ack actor's token (switchboard for rooms,
// owner agent for DMs). It reports whether the post succeeded so the caller can
// advance the durable "warned" cursor only on a confirmed post (at most once).
// A nil hook is treated as a successful no-op so a test/config without a reply
// hook still advances the cursor rather than looping on the reply forever.
func (g *companyGateway) postFailureReply(r *IngressReceipt, token string) bool {
	if g.replyHookTok == nil && g.replyHook == nil {
		return true
	}
	store := g.store()
	if store == nil {
		return false // degraded: cannot resolve the body to derive the reply root
	}
	msg := decodeCompanyMessage(r.Origin, store.receiptBody(r))
	return g.reply(token, r.Origin.ChannelID, receiptRootTS(r, msg),
		"delivery failed for receipt "+neutralizeMarkupBoundaries(r.ID))
}

// noteDMAckDegraded records that a DM receipt's visible ack could not be placed
// because the owner token is missing (spec §Acks: "Missing owner token → acks
// silently degrade (counted)"). The switchboard token must never touch a DM
// channel, so the ack genuinely cannot happen — this makes the degradation
// observable on company_dm_token_missing instead of being a silent no-op. It
// commits the durable ackStateDegraded cursor so the count is once-per-receipt
// (a redrive short-circuits) and no further ack calls are attempted. No-op for
// non-dm-family receipts (rooms always resolve the switchboard token, so they
// never reach here) and idempotent once the cursor is degraded. The mpim ack
// actor is the admission owner, so its missing token degrades and counts on the
// shared company_dm_token_missing gauge exactly as a DM (spec §Acks: degradation
// counters shared).
func (g *companyGateway) noteDMAckDegraded(r *IngressReceipt) {
	if r == nil || !isDMFamilyKind(r.Kind) || r.AckState == ackStateDegraded {
		return
	}
	g.dmTokenMissing.Add(1)
	g.commitAckState(r, ackStateDegraded)
}

// commitAckState persists an AckState transition through the normal
// generation-bumping receipt Update. Ack commits refresh UpdatedAt (extending
// the stale-claim window sweepEligible reads — documented, accepted). Errors are
// logged, never propagated: an ack write never fails delivery.
func (g *companyGateway) commitAckState(r *IngressReceipt, state string) {
	if r.AckState == state {
		return
	}
	if err := g.commitReceipt(r, func(cur *IngressReceipt) {
		cur.AckState = state
	}); err != nil {
		log.Printf("company: ack-state commit %s -> %s: %v", r.ID, state, err)
	}
}

// slackReact performs one Slack reaction call (add or remove) over the given
// client and switchboard token and maps the result into the ack taxonomy. It
// reuses the single reactions POST path (postReactionMethod), so there is no
// second Slack reactions POST implementation.
func slackReact(client *http.Client, token, method, channel, ts, name string) ackOutcome {
	resp, err := postReactionMethod(client, token, method, slackReactionsAddReq{
		Channel:   channel,
		Name:      name,
		Timestamp: ts,
	})
	if err != nil {
		// Transient HTTP / network: outcome unknown, leave the cursor unchanged.
		return ackUnchanged
	}
	if resp.OK {
		return ackSuccess
	}
	switch resp.Error {
	case "already_reacted", "no_reaction":
		// Already in the desired state — treat as success (idempotent).
		return ackSuccess
	case "too_many_emoji", "too_many_reactions":
		// Silent, permanent degradation for this receipt.
		return ackDegrade
	case "ratelimited", "rate_limited":
		// Leave the cursor unchanged; retried on the next attempt / sweep heal.
		return ackUnchanged
	default:
		// Any other definitive error degrades.
		return ackDegrade
	}
}
