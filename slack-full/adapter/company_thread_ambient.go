package main

import (
	"log"
	"sort"
	"time"
)

type threadParticipantKey struct {
	TeamID, ChannelID, ThreadRootTS string
}

type threadParticipantEvent struct {
	Agent      string
	TS         string
	ReceivedAt time.Time
}

// isCompanyThreadReply distinguishes an actual in-thread message from a root
// channel post. Slack normally omits thread_ts on roots; the inequality keeps a
// malformed self-root from granting participation.
func isCompanyThreadReply(msg CompanyMessage) bool {
	_, _, tsOK := splitSlackTS(msg.TS)
	_, _, rootOK := splitSlackTS(msg.ThreadTS)
	return tsOK && rootOK && msg.ThreadTS != msg.TS
}

// verifiedThreadParticipant returns the agent identity that may be persisted as
// participation evidence for this bot-authored turn. Text and raw event identity
// are never authority: the delivery worker must already have corroborated the
// author through bots.info, ComputeWakeSet must classify it as a company bot,
// and the current directory must declare it a member of this exact room.
func verifiedThreadParticipant(dir *CompanyDirectory, room *CompanyRoom, msg CompanyMessage, decision RouteDecision, author *CompanyAgent) string {
	if dir == nil || room == nil || author == nil || decision.Author != AuthorCompanyBot || !isCompanyThreadReply(msg) {
		return ""
	}
	if !dir.IsMember(room, author.Name) {
		return ""
	}
	return author.Name
}

// addThreadAmbientWakes augments an untagged human thread reply with every
// previously authenticated company-agent participant in the same
// (team, channel, thread_root_ts). Participation is derived from retained
// receipts, making it restart-safe without a second mutable registry.
//
// Existing native company mentions stay exclusive: the pure router has already
// selected targeted wakes, and this function returns without adding readers.
// Stored identities are revalidated against the current directory and room
// membership before use. Only earlier Slack timestamps count, so a delayed
// redrive cannot enroll an agent from a future message in the same thread.
func (g *companyGateway) addThreadAmbientWakes(dir *CompanyDirectory, room *CompanyRoom, current *IngressReceipt, msg CompanyMessage, decision RouteDecision) RouteDecision {
	if g == nil || dir == nil || room == nil || decision.Author != AuthorHuman || !isCompanyThreadReply(msg) {
		return decision
	}
	if len(companyMentionAgents(dir, msg)) > 0 {
		return decision // native company mentions remain exclusive
	}
	existing := make(map[string]bool, len(decision.Wakes))
	for _, wake := range decision.Wakes {
		existing[wake.Agent.Name] = true
	}
	participants := make(map[string]*CompanyAgent)
	root := receiptRootTS(current, msg)
	for _, participant := range g.threadParticipantsBefore(threadParticipantKey{
		TeamID: msg.TeamID, ChannelID: msg.ChannelID, ThreadRootTS: root,
	}, msg.TS) {
		agent, ok := dir.AgentByName(participant)
		if !ok || !dir.IsMember(room, agent.Name) || existing[agent.Name] {
			continue
		}
		participants[agent.Name] = agent
	}

	// Directory agent names are unique slugs; sorting makes the frozen wake set
	// deterministic even if receipt file enumeration order changes.
	names := make([]string, 0, len(participants))
	for name := range participants {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		decision.Wakes = append(decision.Wakes, WakeTarget{Agent: *participants[name], Kind: wakeKindThreadAmbient})
	}
	if len(decision.Wakes) > 0 {
		decision.Reason = ""
	}
	return decision
}

// rememberThreadParticipant updates the derived index only after the caller has
// durably committed ThreadParticipantAgent to the receipt. If the index has not
// been hydrated yet, the later ledger scan replaces this provisional entry and
// includes the same receipt; if it has, this makes the new participant visible
// without another filesystem scan.
func (g *companyGateway) rememberThreadParticipant(r *IngressReceipt) {
	key, event, ok := threadParticipantFromReceipt(r)
	if g == nil || !ok {
		return
	}
	g.threadParticipantsMu.Lock()
	defer g.threadParticipantsMu.Unlock()
	if g.threadParticipants == nil {
		g.threadParticipants = make(map[threadParticipantKey][]threadParticipantEvent)
	}
	for _, existing := range g.threadParticipants[key] {
		if existing.Agent == event.Agent && existing.TS == event.TS {
			return // a concurrent first ledger scan already indexed this receipt
		}
	}
	g.threadParticipants[key] = append(g.threadParticipants[key], event)
}

// loadThreadParticipants hydrates the process index once from retained durable
// receipts. A directory scan failure leaves loaded=false so a later human turn
// retries; until then thread ambient broadening fails closed.
func (g *companyGateway) loadThreadParticipants() bool {
	if g == nil {
		return false
	}
	g.threadParticipantsMu.Lock()
	defer g.threadParticipantsMu.Unlock()
	if g.threadParticipantsLoaded {
		return true
	}
	store := g.store()
	if store == nil {
		return false
	}
	receipts, err := store.List()
	if err != nil {
		log.Printf("company: thread-participant ledger scan failed: %v", err)
		return false
	}
	index := make(map[threadParticipantKey][]threadParticipantEvent)
	cutoff := g.now().Add(-g.retention)
	for _, receipt := range receipts {
		key, event, ok := threadParticipantFromReceipt(receipt)
		if !ok || event.ReceivedAt.Before(cutoff) {
			continue
		}
		index[key] = append(index[key], event)
	}
	g.threadParticipants = index
	g.threadParticipantsLoaded = true
	return true
}

// threadParticipantsBefore returns the deduplicated participant names with an
// authenticated post earlier than the current Slack turn. Timestamp ordering is
// checked again at lookup so a delayed redrive cannot import a future post.
func (g *companyGateway) threadParticipantsBefore(key threadParticipantKey, beforeTS string) []string {
	if !g.loadThreadParticipants() {
		return nil
	}
	cutoff := g.now().Add(-g.retention)
	g.threadParticipantsMu.Lock()
	defer g.threadParticipantsMu.Unlock()
	seen := make(map[string]bool)
	for _, event := range g.threadParticipants[key] {
		if event.ReceivedAt.Before(cutoff) || compareSlackTS(event.TS, beforeTS) >= 0 {
			continue
		}
		seen[event.Agent] = true
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// pruneThreadParticipants mirrors receipt retention for a long-lived adapter,
// preventing an old participant from remaining enrolled after its durable
// evidence has aged out. It is called from the existing receipt sweep.
func (g *companyGateway) pruneThreadParticipants(now time.Time) {
	if g == nil {
		return
	}
	cutoff := now.Add(-g.retention)
	g.threadParticipantsMu.Lock()
	defer g.threadParticipantsMu.Unlock()
	if !g.threadParticipantsLoaded {
		return
	}
	for key, events := range g.threadParticipants {
		kept := events[:0]
		for _, event := range events {
			if !event.ReceivedAt.Before(cutoff) {
				kept = append(kept, event)
			}
		}
		if len(kept) == 0 {
			delete(g.threadParticipants, key)
			continue
		}
		g.threadParticipants[key] = kept
	}
}

// threadParticipantFromReceipt validates the durable tuple before it enters the
// derived index. Room membership and agent existence are deliberately checked
// later against the current directory snapshot; this layer validates only the
// receipt-owned fields and excludes DMs, roots, and malformed Slack timestamps.
func threadParticipantFromReceipt(r *IngressReceipt) (threadParticipantKey, threadParticipantEvent, bool) {
	if r == nil || r.Kind != "" || r.ThreadParticipantAgent == "" || r.ThreadRootTS == "" || r.ThreadRootTS == r.Origin.TS || r.ReceivedAt.IsZero() {
		return threadParticipantKey{}, threadParticipantEvent{}, false
	}
	if _, _, ok := splitSlackTS(r.Origin.TS); !ok {
		return threadParticipantKey{}, threadParticipantEvent{}, false
	}
	if _, _, ok := splitSlackTS(r.ThreadRootTS); !ok {
		return threadParticipantKey{}, threadParticipantEvent{}, false
	}
	if r.Origin.TeamID == "" || r.Origin.ChannelID == "" {
		return threadParticipantKey{}, threadParticipantEvent{}, false
	}
	return threadParticipantKey{
			TeamID: r.Origin.TeamID, ChannelID: r.Origin.ChannelID, ThreadRootTS: r.ThreadRootTS,
		}, threadParticipantEvent{
			Agent: r.ThreadParticipantAgent, TS: r.Origin.TS, ReceivedAt: r.ReceivedAt,
		}, true
}
