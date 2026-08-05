package main

import (
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

// company_replay.go — the Go ordering half of Slack company-rooms Phase 3b.
// Startup recovery and every sweep pass partition pending receipts into
// per-root chains (result-bearing receipts and correlation parks, keyed by the
// root triple (team, channel, thread_root_ts)) and drive each chain strictly
// sequentially and synchronously so snapshot order at the requester equals
// delivery order (S5). All other receipts pass through in the store's existing
// (ReceivedAt, Origin.TS) order and are triggered concurrently as before.

// replayChain is one root's ordered run of receipts. Origins are delivered in
// slice order; the sequencer stops at the first receipt whose outcome is not
// safe-to-advance and lets the next sweep pass rebuild and resume the remainder.
type replayChain struct{ Origins []ReceiptOrigin }

// deliverOutcome is the typed result the chain sequencer needs: the bare void
// deliverReceipt cannot distinguish "safe to advance the chain" from "must
// abort the chain remainder".
type deliverOutcome int

const (
	deliverTerminal       deliverOutcome = iota // terminal status committed
	deliverParkedPreclaim                       // parked before any claim — safe to advance
	deliverPending                              // transient failure / pending target — abort chain
	deliverBusy                                 // single-flight held elsewhere — abort chain
	deliverError                                // commit/pointer/hydration error — abort chain
)

// deliverReceiptOutcome is the chain sequencer's entry point: it runs one
// receipt's delivery synchronously and returns its typed outcome (the S5 chain
// contract). It is a thin wrapper over deliverReceipt, which now returns the
// outcome directly.
func (g *companyGateway) deliverReceiptOutcome(origin ReceiptOrigin) deliverOutcome {
	return g.deliverReceipt(origin)
}

// Correlation-park backoff tunables (S7, rule 17 parity).
const (
	companyPeerRecoveryMaxAttempts = 6
	companyPeerRecoveryBaseDelay   = 60 * time.Second
	companyPeerRecoveryMaxDelay    = 15 * time.Minute
	companyReasonRecoveryExhausted = "correlation_recovery_exhausted"
)

// recoveryDue reports whether a receipt's correlation-park backoff has elapsed:
// a zero RecoveryNextAt (a fresh park, immediately eligible per S7) or a
// RecoveryNextAt at/behind now is due. sweepEligible and recoverPending both
// consult it so an adapter restart neither bypasses the backoff nor bumps the
// attempt count.
func recoveryDue(r *IngressReceipt, now time.Time) bool {
	if r.RecoveryNextAt.IsZero() {
		return true
	}
	return !now.Before(r.RecoveryNextAt)
}

// nextRecoveryDelay returns the S7 backoff for the n-th counted attempt:
// min(60s * 2^(n-1), 15min). n=1..5 yields 60/120/240/480/900 (the last capped
// from 960); n>=5 stays pinned at the 15-minute cap, which is what the never-
// terminal ambiguous park (D5) rides after attempt 6.
func nextRecoveryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := companyPeerRecoveryBaseDelay
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= companyPeerRecoveryMaxDelay {
			return companyPeerRecoveryMaxDelay
		}
	}
	return d
}

// compareSlackTS orders two Slack ts strings. A well-formed ts is exactly two
// non-empty all-digit components separated by a single '.'; well-formed ts
// order numerically on the (seconds, fraction) components. A malformed ts sorts
// after every well-formed ts, and two malformed ts order by raw string — a
// deliberate simplification of Discord's (1, created_at, raw_id) malformed
// fallback (both are deterministic total orders, S5).
func compareSlackTS(a, b string) int {
	aSec, aFrac, aOK := splitSlackTS(a)
	bSec, bFrac, bOK := splitSlackTS(b)
	switch {
	case aOK && bOK:
		if c := compareDigits(aSec, bSec); c != 0 {
			return c
		}
		return compareDigits(aFrac, bFrac)
	case aOK && !bOK:
		return -1 // well-formed sorts before malformed
	case !aOK && bOK:
		return 1
	default:
		return strings.Compare(a, b) // both malformed: raw-string order
	}
}

// splitSlackTS reports whether ts is a well-formed Slack ts and returns its two
// components. Well-formed = exactly one '.' with non-empty all-digit halves.
func splitSlackTS(ts string) (sec, frac string, ok bool) {
	sec, frac, found := strings.Cut(ts, ".")
	if !found || sec == "" || frac == "" || !allDigits(sec) || !allDigits(frac) {
		return "", "", false
	}
	return sec, frac, true
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

// compareDigits numerically compares two all-digit strings without overflow:
// strip leading zeros, then the shorter magnitude is smaller, then compare
// lexicographically at equal length.
func compareDigits(x, y string) int {
	x = strings.TrimLeft(x, "0")
	y = strings.TrimLeft(y, "0")
	if len(x) != len(y) {
		if len(x) < len(y) {
			return -1
		}
		return 1
	}
	return strings.Compare(x, y)
}

// rootTriple is the per-root chain key: the derived (team, channel,
// thread_root_ts). It matches rootSerialLockName's dgser key dimensions so the
// in-process chain owner and the cross-process advisory lock partition on the
// same root.
type rootTriple struct {
	TeamID       string
	ChannelID    string
	ThreadRootTS string
}

// chainRegistry tracks the single active in-process chain per root so a sweep
// pass or a live trigger for an owned root enqueues into the running chain
// instead of racing it. It is process-scoped (advisory, like dgser): the
// generation counter and single-flight remain the correctness floor.
type chainRegistry struct {
	mu    sync.Mutex
	owned map[rootTriple]*ownedChain
}

// ownedChain is one root's live sequencer state: the queue of not-yet-delivered
// origins (protected by the registry mutex) processed by the owning goroutine.
type ownedChain struct {
	queue []ReceiptOrigin
	seen  map[string]bool // receipt ids already queued/delivered this run (dedup)
}

func newChainRegistry() *chainRegistry {
	return &chainRegistry{owned: map[rootTriple]*ownedChain{}}
}

// acquire claims ownership of root for the caller if it is free, seeding the
// queue with origins. It returns (chain, true) when the caller became the owner
// and must run it, or (chain, false) when another goroutine already owns it —
// in which case origins have been enqueued onto the existing owner.
func (cr *chainRegistry) acquire(root rootTriple, origins []ReceiptOrigin) (*ownedChain, bool) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if oc := cr.owned[root]; oc != nil {
		oc.enqueueLocked(origins)
		return oc, false
	}
	oc := &ownedChain{seen: map[string]bool{}}
	oc.enqueueLocked(origins)
	cr.owned[root] = oc
	return oc, true
}

// enqueue routes a single origin into an already-owned chain, reporting whether
// an owner existed (and therefore absorbed it). A live result-bearing trigger
// for an owned root routes through here instead of starting a racing delivery.
func (cr *chainRegistry) enqueue(root rootTriple, origin ReceiptOrigin) bool {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	oc := cr.owned[root]
	if oc == nil {
		return false
	}
	oc.enqueueLocked([]ReceiptOrigin{origin})
	return true
}

func (oc *ownedChain) enqueueLocked(origins []ReceiptOrigin) {
	for _, o := range origins {
		id := receiptID(o)
		if oc.seen[id] {
			continue
		}
		oc.seen[id] = true
		oc.queue = append(oc.queue, o)
	}
}

// next pops the head of the queue under the registry lock, or releases
// ownership (deleting the registry entry) when the queue is drained. The atomic
// pop-or-release closes the race where a late enqueue arrives just as the owner
// is about to exit.
func (cr *chainRegistry) next(root rootTriple, oc *ownedChain) (ReceiptOrigin, bool) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if len(oc.queue) == 0 {
		delete(cr.owned, root)
		return ReceiptOrigin{}, false
	}
	head := oc.queue[0]
	oc.queue = oc.queue[1:]
	return head, true
}

// orderPendingForReplay partitions pending receipts into per-root chains sorted
// per S5 and passes every other receipt through in store order. A receipt joins
// a chain iff it is result-bearing or a correlation park with a derivable root
// triple; the rest (human legs, directory parks, non-peer receipts) stay in the
// caller-supplied (store) order so cross-root ordering is unchanged (Discord
// permutes only sibling positions).
func (g *companyGateway) orderPendingForReplay(pending []*IngressReceipt) (chains []replayChain, rest []*IngressReceipt) {
	type bucket struct {
		legacy   []*IngressReceipt // no valid snapshot: delivered first
		snapshot []*IngressReceipt // snapshot-bearing: ordered by (responded, snapshot_at, ...)
	}
	buckets := map[rootTriple]*bucket{}
	var order []rootTriple // first-seen root order, so chain emission is deterministic

	for _, r := range pending {
		root, ok := g.replayRoot(r)
		if !ok {
			rest = append(rest, r)
			continue
		}
		b := buckets[root]
		if b == nil {
			b = &bucket{}
			buckets[root] = b
			order = append(order, root)
		}
		if snap := receiptSnapshot(r); snap.Available {
			b.snapshot = append(b.snapshot, r)
		} else {
			b.legacy = append(b.legacy, r)
		}
	}

	for _, root := range order {
		b := buckets[root]
		sort.SliceStable(b.legacy, func(i, j int) bool {
			return lessLegacy(b.legacy[i], b.legacy[j])
		})
		sort.SliceStable(b.snapshot, func(i, j int) bool {
			return lessSnapshot(b.snapshot[i], b.snapshot[j])
		})
		origins := make([]ReceiptOrigin, 0, len(b.legacy)+len(b.snapshot))
		for _, r := range b.legacy {
			origins = append(origins, r.Origin)
		}
		for _, r := range b.snapshot {
			origins = append(origins, r.Origin)
		}
		chains = append(chains, replayChain{Origins: origins})
	}
	return chains, rest
}

// replayRoot reports whether a receipt belongs to a per-root chain and returns
// its root triple. Result-bearing receipts (a claimed peer_result target) and
// correlation/ambiguity parks partition by their derived root; everything else
// passes through in store order.
func (g *companyGateway) replayRoot(r *IngressReceipt) (rootTriple, bool) {
	if r == nil {
		return rootTriple{}, false
	}
	store := g.store()
	if store == nil {
		return rootTriple{}, false
	}
	msg := decodeCompanyMessage(r.Origin, store.receiptBody(r))
	// A receipt joins its root chain when its recorded state marks it a chain
	// member (a peer_result target, a frozen synthesis snapshot, or a correlation
	// park) OR when its MESSAGE is result-bearing (S6). The message classification
	// closes the crash window between the durable claim commit and the receipt
	// routing commit: a result whose delegation record was already claimed but
	// whose targets have not yet frozen (Status received, Reason "", no targets,
	// no synthesis) is still ordered within its root, so a restart cannot deliver
	// a later sibling's wake before it. The message is already decoded here, so
	// this is zero extra I/O.
	if !isReplayChainReceipt(r) && !isResultBearing(msg) {
		return rootTriple{}, false
	}
	return receiptRootTriple(r, msg)
}

// isReplayChainReceipt reports whether a receipt is subject to per-root replay
// ordering: it carries a peer_result target (claimed or frozen), or it is
// parked under a correlation reason (its later delivery, once resolved, is a
// result whose order must be preserved).
func isReplayChainReceipt(r *IngressReceipt) bool {
	if r.Status == ingressStatusReceived {
		switch r.Reason {
		case peerParkCorrelationPending, peerParkAmbiguousPending, peerParkCorrelationError:
			return true
		}
	}
	for _, td := range r.Targets {
		if td.Kind == wakeKindPeerResult {
			return true
		}
	}
	return len(r.Synthesis) > 0
}

// receiptSnapshot normalizes a receipt's frozen synthesis bytes (S10). Absent
// or malformed bytes yield the unavailable shape, landing the receipt in the
// chain's legacy-first bucket.
func receiptSnapshot(r *IngressReceipt) companySynthesisSnapshot {
	if len(r.Synthesis) == 0 {
		return normalizeSynthesisState(nil)
	}
	return normalizeSynthesisBytes(r.Synthesis)
}

// lessLegacy orders no-snapshot receipts by (result_claimed_at, result ts,
// receipt id). result_claimed_at and the result ts are not stored on the
// receipt, so this falls back to the received time then the origin ts+id — a
// deterministic total order over the legacy bucket (S5).
func lessLegacy(a, b *IngressReceipt) bool {
	if !a.ReceivedAt.Equal(b.ReceivedAt) {
		return a.ReceivedAt.Before(b.ReceivedAt)
	}
	if c := compareSlackTS(a.Origin.TS, b.Origin.TS); c != 0 {
		return c < 0
	}
	return a.ID < b.ID
}

// lessSnapshot orders snapshot-bearing receipts by ascending
// (responded_delegation_count, synthesis_snapshot_at, result ts, receipt id)
// (S5).
func lessSnapshot(a, b *IngressReceipt) bool {
	sa, sb := receiptSnapshot(a), receiptSnapshot(b)
	if sa.Responded != sb.Responded {
		return sa.Responded < sb.Responded
	}
	if sa.SnapshotAt != sb.SnapshotAt {
		return sa.SnapshotAt < sb.SnapshotAt
	}
	if c := compareSlackTS(a.Origin.TS, b.Origin.TS); c != 0 {
		return c < 0
	}
	return a.ID < b.ID
}

// deliverChain delivers a chain strictly sequentially and SYNCHRONOUSLY (never
// via triggerDelivery): it advances past deliverTerminal and
// deliverParkedPreclaim and aborts the chain remainder on pending/busy/error,
// letting the next sweep pass rebuild and resume. The per-root ownership
// registry guarantees one active chain per root — a concurrent sweep pass or
// live trigger for the owned root enqueues into this chain instead of racing.
func (g *companyGateway) deliverChain(c replayChain) {
	if g == nil || len(c.Origins) == 0 {
		return
	}
	root, ok := g.chainRootOf(c.Origins[0])
	if !ok {
		// No derivable root (defensive): deliver in slice order without owning.
		for _, o := range c.Origins {
			if !advanceOutcome(g.deliverReceiptOutcome(o)) {
				return
			}
		}
		return
	}
	oc, owner := g.chains.acquire(root, c.Origins)
	if !owner {
		return // another goroutine owns this root; our origins were enqueued
	}
	for {
		origin, ok := g.chains.next(root, oc)
		if !ok {
			return
		}
		if !advanceOutcome(g.deliverReceiptOutcome(origin)) {
			// Abort the remainder: release ownership so the next sweep rebuilds
			// and resumes. Drain under the lock to avoid stranding late enqueues
			// (they will be rediscovered by Pending on the next pass).
			g.chains.abort(root)
			return
		}
	}
}

// chainRootOf derives the root triple for one origin by reading its receipt.
func (g *companyGateway) chainRootOf(origin ReceiptOrigin) (rootTriple, bool) {
	store := g.store()
	if store == nil {
		return rootTriple{}, false
	}
	r, err := store.Get(origin)
	if err != nil || r == nil {
		return rootTriple{}, false
	}
	return receiptRootTriple(r, decodeCompanyMessage(origin, store.receiptBody(r)))
}

// receiptRootTriple derives a receipt's chain root triple from its FROZEN
// thread_root_ts (receiptRootTS), so every per-root chain derivation — replayRoot,
// chainRootOf, enqueueForRoot — agrees even after the body is redacted or lost
// (which would otherwise collapse the body-derived root to origin.TS and split one
// thread into parallel chains). Legacy receipts fall back to body-derivation.
func receiptRootTriple(r *IngressReceipt, msg CompanyMessage) (rootTriple, bool) {
	root := receiptRootTS(r, msg)
	if r.Origin.TeamID == "" || r.Origin.ChannelID == "" || root == "" {
		return rootTriple{}, false
	}
	return rootTriple{TeamID: r.Origin.TeamID, ChannelID: r.Origin.ChannelID, ThreadRootTS: root}, true
}

// isResultBearing classifies the S6 result-bearing message: bot-authored AND
// metadata.event_type == "gc_delegation_result" (the only messages that can
// claim). The dgser lock and the live-trigger chain routing both gate on it.
func isResultBearing(msg CompanyMessage) bool {
	if !isBotAuthored(msg) {
		return false
	}
	return parseMessageMetadata(msg.Metadata).EventType == companyResultEventType
}

// abort releases ownership of a root, discarding any queued origins. The
// discarded receipts stay durably pending on disk, so the next sweep pass
// rediscovers and re-chains them.
func (cr *chainRegistry) abort(root rootTriple) {
	cr.mu.Lock()
	delete(cr.owned, root)
	cr.mu.Unlock()
}

// hasOwners reports whether any chain is currently active — the cheap fast-path
// guard for the live-trigger routing check.
func (cr *chainRegistry) hasOwners() bool {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	return len(cr.owned) > 0
}

// advanceOutcome reports whether the chain sequencer may proceed to the next
// receipt: it advances past a committed terminal state or a pre-claim park, and
// aborts the remainder on a transient pending/busy/commit outcome.
func advanceOutcome(o deliverOutcome) bool {
	switch o {
	case deliverTerminal, deliverParkedPreclaim:
		return true
	default:
		return false
	}
}

// enqueueForRoot routes a live result-bearing trigger through the active chain
// owner for its root when one exists, returning true if the origin was absorbed
// (so the caller must not start a racing delivery). Cheap common-case bail: when
// no chain is active it takes only the registry lock + len check and never reads
// the receipt. Only result-bearing triggers (S6 classification) are routed;
// human legs and other receipts deliver normally even into an owned root.
func (g *companyGateway) enqueueForRoot(origin ReceiptOrigin) bool {
	if g == nil || g.chains == nil || !g.chains.hasOwners() {
		return false
	}
	store := g.store()
	if store == nil {
		return false
	}
	r, err := store.Get(origin)
	if err != nil || r == nil {
		return false
	}
	msg := decodeCompanyMessage(origin, store.receiptBody(r))
	if !isResultBearing(msg) {
		return false
	}
	root, ok := receiptRootTriple(r, msg)
	if !ok {
		return false
	}
	if g.chains.enqueue(root, origin) {
		log.Printf("company: live result trigger for owned root routed into active chain receipt=%s", receiptID(origin))
		return true
	}
	return false
}
