package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// company_admin.go — the operator redrive surface (Slack company-rooms Phase
// 3b), served on the adapter's internal mux only. Two receipt-native endpoints
// back the `gc slack company-status` / `company-redrive` Python verbs, which are
// thin clients (the receipt store keeps exactly one writer — Go, generation
// CAS — so the verbs never rewrite receipts):
//
//   - GET  /internal/company/receipts?origin=…|root=…|status=…  bounded listing
//   - POST /internal/company/redrive                            two-leg redrive
//   - POST /internal/company/redact                             body redaction hook
//
// Both operate under the receipt's in-process single-flight and the store's
// generation-checked commit, exactly like the delivery worker.

// adminTargetView is the per-target slice of a receipt exposed to the operator
// verbs (session, kind, status, attempts, detail, delegation_key).
type adminTargetView struct {
	Session       string `json:"session,omitempty"`
	Agent         string `json:"agent,omitempty"`
	Kind          string `json:"kind"`
	Status        string `json:"status"`
	Attempts      int    `json:"attempts"`
	Detail        string `json:"detail,omitempty"`
	DelegationKey string `json:"delegation_key,omitempty"`
}

// adminReceiptView is one receipt's operator-facing shape: id/origin/status/
// reason, the S7 recovery fields, the ack cursor, and the per-target state.
type adminReceiptView struct {
	ID               string        `json:"id"`
	Origin           ReceiptOrigin `json:"origin"`
	Status           string        `json:"status"`
	Reason           string        `json:"reason,omitempty"`
	RecoveryAttempts int           `json:"recovery_attempts,omitempty"`
	RecoveryNextAt   string        `json:"recovery_next_at,omitempty"`
	RecoveryReason   string        `json:"recovery_reason,omitempty"`
	AckState         string        `json:"ack_state,omitempty"`
	ReceivedAt       string        `json:"received_at,omitempty"`
	UpdatedAt        string        `json:"updated_at,omitempty"`
	// BodyState is the receipt's body resolution (embedded/ok/redacted/missing/
	// mismatch), so an operator listing distinguishes a redacted receipt from an
	// intact one and from a hard integrity error (m9) — a redacted receipt stays in
	// the listing with a visible marker rather than silently vanishing.
	BodyState string            `json:"body_state,omitempty"`
	Targets   []adminTargetView `json:"targets"`
}

// bodyStateLabel renders a bodyStatus for the operator listing.
func bodyStateLabel(st bodyStatus) string {
	switch st {
	case bodyEmbedded:
		return "embedded"
	case bodyOK:
		return "ok"
	case bodyRedacted:
		return "redacted"
	case bodyMissing:
		return "missing"
	case bodyMismatch:
		return "mismatch"
	default:
		return ""
	}
}

func newAdminReceiptView(r *IngressReceipt, bodyState string) adminReceiptView {
	v := adminReceiptView{
		ID:               r.ID,
		Origin:           r.Origin,
		Status:           r.Status,
		Reason:           r.Reason,
		RecoveryAttempts: r.RecoveryAttempts,
		RecoveryReason:   r.RecoveryReason,
		AckState:         r.AckState,
		BodyState:        bodyState,
		Targets:          []adminTargetView{},
	}
	if !r.RecoveryNextAt.IsZero() {
		v.RecoveryNextAt = r.RecoveryNextAt.UTC().Format(time.RFC3339)
	}
	if !r.ReceivedAt.IsZero() {
		v.ReceivedAt = r.ReceivedAt.UTC().Format(time.RFC3339)
	}
	if !r.UpdatedAt.IsZero() {
		v.UpdatedAt = r.UpdatedAt.UTC().Format(time.RFC3339)
	}
	for _, td := range r.Targets {
		v.Targets = append(v.Targets, adminTargetView{
			Session:       td.Session,
			Agent:         td.Agent,
			Kind:          td.Kind,
			Status:        td.Status,
			Attempts:      td.Attempts,
			Detail:        td.Detail,
			DelegationKey: td.DelegationKey,
		})
	}
	return v
}

// handleCompanyReceipts serves GET /internal/company/receipts: a bounded JSON
// listing of receipts, filtered by origin (team:channel:ts), root
// (team:channel:root_ts), and/or status. Read-only.
func (g *companyGateway) handleCompanyReceipts(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	store := g.storeOrNil()
	if store == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "receipt store unavailable")
		return
	}
	originFilter, ok := parseTripleParam(w, req.URL.Query().Get("origin"), "origin")
	if !ok {
		return
	}
	rootFilter, ok := parseTripleParam(w, req.URL.Query().Get("root"), "root")
	if !ok {
		return
	}
	statusFilter := req.URL.Query().Get("status")

	all, err := store.List()
	if err != nil {
		writeAdminError(w, http.StatusInternalServerError, "list receipts: "+err.Error())
		return
	}
	views := make([]adminReceiptView, 0, len(all))
	for _, r := range all {
		if originFilter != nil && r.Origin != *originFilter {
			continue
		}
		if statusFilter != "" && r.Status != statusFilter {
			continue
		}
		// Resolve the body once: its classification drives the operator marker AND
		// the root filter derives the thread root from the FROZEN ThreadRootTS
		// (receiptRootTS), not the decoded body — so a redacted or missing-body
		// thread reply still matches its true root instead of dropping out of the
		// exact listing an operator uses to audit the thread they just redacted (m9).
		body, bodyStat := store.loadBody(r)
		if rootFilter != nil {
			msg := decodeCompanyMessage(r.Origin, body)
			root := receiptRootTS(r, msg)
			if root == "" || r.Origin.TeamID != rootFilter.TeamID ||
				r.Origin.ChannelID != rootFilter.ChannelID ||
				root != rootFilter.TS {
				continue
			}
		}
		views = append(views, newAdminReceiptView(r, bodyStateLabel(bodyStat)))
	}
	writeAdminJSON(w, http.StatusOK, map[string]any{"receipts": views})
}

// companyRedriveRequest is the POST /internal/company/redrive body: a receipt id
// OR an origin triple, the optional target-session filter, and the
// include_failed flag (required to touch an attempts_exhausted target).
type companyRedriveRequest struct {
	Receipt       string         `json:"receipt"`
	Origin        *ReceiptOrigin `json:"origin"`
	Targets       []string       `json:"targets"`
	IncludeFailed bool           `json:"include_failed"`
}

// handleCompanyRedrive serves POST /internal/company/redrive. It resolves the
// receipt, holds its single-flight (409 when held elsewhere), and applies one of
// the two redrive legs under a generation-checked commit, then re-triggers
// delivery. A terminal receipt swept past retention is 404.
func (g *companyGateway) handleCompanyRedrive(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	store := g.storeOrNil()
	if store == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "receipt store unavailable")
		return
	}
	var body companyRedriveRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeAdminError(w, http.StatusBadRequest, "decode: "+err.Error())
		return
	}
	id, selector, err := redriveReceiptID(body)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Single-flight: a concurrent delivery or redrive holding the receipt's
	// claim gets a 409 (the verb retries). Released before triggerDelivery so
	// the spawned delivery goroutine can re-acquire it.
	if !g.acquireSingleFlight(id) {
		writeAdminError(w, http.StatusConflict, "receipt single-flight held elsewhere")
		return
	}

	r, gerr := store.GetByID(id)
	if gerr != nil {
		g.releaseSingleFlight(id)
		writeAdminError(w, http.StatusInternalServerError, "read receipt: "+gerr.Error())
		return
	}
	if r == nil {
		g.releaseSingleFlight(id)
		writeAdminError(w, http.StatusNotFound, "receipt "+selector+" not found (terminal and swept, or never admitted)")
		return
	}

	res, cerr := g.applyRedrive(r, body)
	g.releaseSingleFlight(id)
	if cerr != nil {
		writeAdminError(w, http.StatusInternalServerError, "redrive commit: "+cerr.Error())
		return
	}
	if res.empty {
		// Empty effective selection with recoverable-but-unbound targets: an
		// explicit machine-readable 422, never a success-shaped empty reset that
		// a client would mistake for a completed redrive (the parked result wake
		// is still lost). The operator must repair the binding, then re-run.
		writeAdminJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"receipt":      r.ID,
			"leg":          res.leg,
			"reason":       "unresolved_targets",
			"error":        fmt.Sprintf("redrive selected no deliverable target: %d unbound target(s) do not resolve to a session under the current bindings", len(res.unresolvable)),
			"unresolvable": res.unresolvable,
			"status":       r.Status,
		})
		return
	}
	if res.trigger {
		g.triggerDelivery(r.Origin)
	}
	writeAdminJSON(w, http.StatusOK, map[string]any{
		"receipt":       r.ID,
		"leg":           res.leg,
		"reset_targets": res.resetSessions,
		"unresolvable":  res.unresolvable,
		"status":        r.Status,
	})
}

// companyRedactRequest is the POST /internal/company/redact body: the receipt id
// OR an origin triple whose body sidecar to redact. Exactly one, mirroring
// redrive's selector.
type companyRedactRequest struct {
	Receipt string         `json:"receipt"`
	Origin  *ReceiptOrigin `json:"origin"`
}

// handleCompanyRedact serves POST /internal/company/redact. It resolves the
// receipt, holds its single-flight (409 when held elsewhere), and truncates the
// receipt's body sidecar to the redacted tombstone — the operator-only redaction
// hook (NOT wired to any automatic trigger this phase). A terminal receipt swept
// past retention is 404; a legacy embedded receipt (no separable body) is 409.
func (g *companyGateway) handleCompanyRedact(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	store := g.storeOrNil()
	if store == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "receipt store unavailable")
		return
	}
	var body companyRedactRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeAdminError(w, http.StatusBadRequest, "decode: "+err.Error())
		return
	}
	id, selector, err := redactReceiptID(body)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Single-flight: a concurrent delivery or redrive holding the claim gets a
	// 409 (the verb retries). Released before returning.
	if !g.acquireSingleFlight(id) {
		writeAdminError(w, http.StatusConflict, "receipt single-flight held elsewhere")
		return
	}
	defer g.releaseSingleFlight(id)

	r, gerr := store.GetByID(id)
	if gerr != nil {
		writeAdminError(w, http.StatusInternalServerError, "read receipt: "+gerr.Error())
		return
	}
	if r == nil {
		writeAdminError(w, http.StatusNotFound, "receipt "+selector+" not found (terminal and swept, or never admitted)")
		return
	}
	if r.BodyRef == "" {
		// Legacy embedded receipt: the event is inline, there is no separable body
		// file to truncate. It ages out at retention.
		writeAdminError(w, http.StatusConflict, "receipt "+r.ID+" is a legacy embedded receipt with no separable body to redact")
		return
	}
	// Terminal-status guard (C4/C6/C7): redaction is the core_bound fence — the raw
	// payload is retained until delivery is durable, then redacted. Truncating the
	// body of a non-terminal (received/routing) receipt would recompute routing (or
	// re-render a redrive) from a null body: an empty wake set terminalizes it under
	// a misleading reason, a redrive re-POSTs empty bytes under the same
	// Idempotency-Key. Refuse until the receipt is terminal.
	if !isTerminalStatus(r.Status) {
		writeAdminError(w, http.StatusConflict, "receipt "+r.ID+" is not terminal (status "+r.Status+"); redaction is refused until delivery is durable (the core_bound fence)")
		return
	}
	// Reconciliation-horizon guard (C6): a self-echo receipt goes terminal within
	// seconds, but a stuck Python "posting" intent may still reconcile against this
	// receipt's body (its metadata nonce) for up to the intent TTL. Truncating the
	// body inside that window erases the only copy of the nonce and wedges the
	// intent forever. Refuse until the receipt is older than the reconciliation
	// horizon (mirrors the outbound INTENT_TTL_SECONDS).
	if age := g.now().UTC().Sub(r.ReceivedAt); age < redactReconciliationHorizon {
		writeAdminError(w, http.StatusConflict,
			fmt.Sprintf("receipt %s is younger than the %s reconciliation horizon (age %s); redaction is refused so a stuck posting intent can still reconcile against its body",
				r.ID, redactReconciliationHorizon, age.Round(time.Second)))
		return
	}
	if rerr := store.redactReceiptBody(r); rerr != nil {
		if errors.Is(rerr, ErrReceiptSwept) {
			// The receipt was pair-deleted by the retention sweep between our read
			// and the mutex-guarded redact (m2): report the truth, not a false 200.
			writeAdminError(w, http.StatusNotFound, "receipt "+r.ID+" not found (terminal and swept during redact)")
			return
		}
		writeAdminError(w, http.StatusInternalServerError, "redact body: "+rerr.Error())
		return
	}
	writeAdminJSON(w, http.StatusOK, map[string]any{
		"receipt":      r.ID,
		"redacted":     true,
		"event_digest": r.EventDigest,
	})
}

// redactReceiptID resolves the receipt id and a human-readable selector from a
// redact request: an explicit receipt id, or the derived id of an origin triple.
// Exactly one must be present (redrive's selector rules).
func redactReceiptID(body companyRedactRequest) (id, selector string, err error) {
	if body.Receipt != "" {
		if body.Origin != nil {
			return "", "", errBadRequest("provide exactly one of receipt or origin")
		}
		if !isReceiptID(body.Receipt) {
			return "", "", errBadRequest("receipt must match the receipt-id shape in-<...>")
		}
		return body.Receipt, body.Receipt, nil
	}
	if body.Origin == nil {
		return "", "", errBadRequest("one of receipt or origin is required")
	}
	o := *body.Origin
	if o.TeamID == "" || o.ChannelID == "" || o.TS == "" {
		return "", "", errBadRequest("origin requires team_id, channel_id, and ts")
	}
	return receiptID(o), o.TeamID + ":" + o.ChannelID + ":" + o.TS, nil
}

// redriveOutcome is the result of applyRedrive: the sessions reset to pending,
// the agent names of failed-unbound targets that still do not resolve to a
// session (surfaced to the operator), the leg taken, whether a delivery should
// be triggered, and whether the leg-1 selection was empty-but-unresolvable (a
// 422, never a 200 empty reset).
type redriveOutcome struct {
	resetSessions []string
	unresolvable  []string
	leg           string
	trigger       bool
	empty         bool
}

// applyRedrive mutates the receipt in place under a generation-checked commit
// per the two legs (S7 / redrive parity). The caller holds the receipt's
// single-flight and releases it before triggering.
func (g *companyGateway) applyRedrive(r *IngressReceipt, body companyRedriveRequest) (redriveOutcome, error) {
	// Leg 2: no recorded targets (a correlation_recovery_exhausted / parked
	// receipt) — reset to received, clear reason + recovery fields, re-enter
	// correlation from first-routing resolution.
	if len(r.Targets) == 0 {
		if cerr := g.commitReceipt(r, func(cur *IngressReceipt) {
			cur.Status = ingressStatusReceived
			cur.Reason = ""
			cur.RecoveryAttempts = 0
			cur.RecoveryNextAt = time.Time{}
			cur.RecoveryReason = ""
		}); cerr != nil {
			return redriveOutcome{leg: "correlation"}, cerr
		}
		return redriveOutcome{resetSessions: []string{}, unresolvable: []string{}, leg: "correlation", trigger: true}, nil
	}

	// Leg 1: frozen targets — reset the selected failed targets to pending.
	//
	// EVERY failed target re-resolves its session from the CURRENT bindings (P0
	// item 3 / petra parity): a bound target frozen to a now-dead session name
	// adopts the live binding value so the redrive drives the reachable session,
	// not the stale frozen one; a re-resolution to the same session is a no-op
	// rewrite that keeps the IdempotencyKey byte-for-byte (gc dedups a re-post);
	// a bound target whose binding row has since vanished keeps its recorded
	// session (nothing better to resolve to). A failed-UNBOUND target (frozen
	// with Session=="" when its binding was stale at route time) resolves from
	// its recorded Agent name and is bound on success or surfaced as unresolvable
	// on failure. An empty reset that still has unresolvable unbound targets is a
	// 422 (never a silent success).
	leg := "targets"
	targetFilter := map[string]bool{}
	for _, s := range body.Targets {
		targetFilter[s] = true
	}
	room, _ := g.dirStore.Snapshot().RoomByChannel(r.Origin.TeamID, r.Origin.ChannelID)
	bindings := g.bindStore.Snapshot()
	// resolveBinding re-resolves a target from its recorded Agent name against the
	// CURRENT bindings: a dm-family receipt resolves through dm_bindings (keyed by
	// agent), a room receipt through the room bindings (keyed by room+agent). Used
	// for both bound (session re-resolution) and unbound (session materialization)
	// targets so the operator redrive and the retry path re-resolve identically.
	resolveBinding := func(agent string) (session, city string) {
		if isDMFamilyKind(r.Kind) {
			// dm-family (dm + mpim): re-resolve via dm_bindings keyed by agent
			// (spec §Kind-dispatch inventory). An mpim receipt's failed_dm_unbound
			// / failed_mpim_not_member target rebinds through the same registry.
			if bd, ok := g.dmBindStore.Snapshot().BindingFor(agent); ok {
				return bd.Session, bd.City
			}
			return "", ""
		}
		if room != nil {
			if bd, ok := bindings.BindingFor(room.Name, agent); ok {
				return bd.Session, bd.City
			}
		}
		return "", ""
	}

	resetSessions := []string{}
	unresolvable := []string{}
	// plan maps a current target key to the session it will be reset under; a
	// key present here is committed to pending.
	// redrivePlanEntry carries the target session plus, for a re-resolved
	// unbound target, the binding's city qualifier.
	type redrivePlanEntry struct{ Session, City string }
	plan := map[string]redrivePlanEntry{}
	for key, td := range r.Targets {
		if td.Status != companyTargetFailed {
			continue
		}
		if td.Session != "" {
			if !redriveSelectsTarget(td, targetFilter, body.IncludeFailed) {
				continue
			}
			// Re-resolve a bound failed target from the CURRENT bindings. A missing
			// binding row (or an unrecorded agent) leaves the recorded session in
			// place — never worse than the pre-P0 behavior.
			planSession, planCity := td.Session, td.City
			if td.Agent != "" {
				if s, c := resolveBinding(td.Agent); s != "" {
					planSession, planCity = s, c
				}
			}
			plan[key] = redrivePlanEntry{Session: planSession, City: planCity}
			resetSessions = append(resetSessions, planSession)
			continue
		}
		// Failed-unbound target: re-resolve from the recorded Agent name.
		if td.Agent == "" {
			continue // no agent recorded — cannot re-resolve (defensive)
		}
		session, city := resolveBinding(td.Agent)
		// Scope: default (no --target) selects every failed-unbound target; a
		// --target filter matches the recorded agent name or the resolved session.
		if len(targetFilter) > 0 && !targetFilter[td.Agent] && (session == "" || !targetFilter[session]) {
			continue
		}
		if session == "" {
			unresolvable = append(unresolvable, td.Agent)
			continue
		}
		plan[key] = redrivePlanEntry{Session: session, City: city}
		resetSessions = append(resetSessions, session)
	}

	if len(resetSessions) == 0 {
		if len(unresolvable) > 0 {
			// Recoverable-but-unbound targets remain: 422, not a success-shaped
			// empty reset.
			return redriveOutcome{resetSessions: resetSessions, unresolvable: unresolvable, leg: leg, empty: true}, nil
		}
		// Genuinely nothing to do (all delivered, filtered out, or an
		// attempts_exhausted target without --include-failed): benign no-op, no
		// generation churn, no re-trigger.
		return redriveOutcome{resetSessions: resetSessions, unresolvable: unresolvable, leg: leg}, nil
	}

	if cerr := g.commitReceipt(r, func(cur *IngressReceipt) {
		now := g.now().UTC()
		for key, pt := range plan {
			td, ok := cur.Targets[key]
			if !ok {
				continue
			}
			if td.Session != pt.Session {
				// The session changed — a newly re-resolved unbound target (Session
				// was ""), or a bound target whose live binding now names a different
				// session (petra). Adopt the new session + city, re-derive the
				// idempotency key (a new session is a genuinely new delivery), and
				// relocate to the bound key namespace under the new session.
				td.Session = pt.Session
				td.City = pt.City
				td.IdempotencyKey = companyIdempotencyKey(cur.ID, pt.Session)
				delete(cur.Targets, key)
				key = companyBoundTargetKeyPrefix + pt.Session
			} else if td.City != pt.City {
				// Same session, city qualifier moved: update the city but keep the
				// key and IdempotencyKey (gc dedups a re-post to the same session).
				td.City = pt.City
			}
			// A same-session re-resolution leaves the IdempotencyKey untouched.
			td.Status = companyTargetPending
			td.Attempts = 0
			td.Detail = "operator_redrive"
			// The prior asynchronous request is terminal (this endpoint only
			// selects failed targets). An explicit operator redrive authorizes a
			// new POST, so discard the old request correlation while preserving
			// the frozen message idempotency key and route.
			td.RequestID = ""
			td.EventCursor = ""
			td.UpdatedAt = now
			cur.Targets[key] = td
		}
		cur.Status = ingressStatusRouting
		cur.Reason = ""
		cur.RecoveryAttempts = 0
		cur.RecoveryNextAt = time.Time{}
		cur.RecoveryReason = ""
	}); cerr != nil {
		return redriveOutcome{leg: leg}, cerr
	}
	return redriveOutcome{resetSessions: resetSessions, unresolvable: unresolvable, leg: leg, trigger: true}, nil
}

// redriveSelectsTarget reports whether a redrive touches this BOUND target: only
// a failed, session-bearing target is eligible here; --target restricts to the
// listed sessions; an attempts_exhausted target requires include_failed. Failed-
// unbound targets (Session=="") are handled separately in applyRedrive, which
// re-resolves them against the current bindings.
func redriveSelectsTarget(td TargetDelivery, targetFilter map[string]bool, includeFailed bool) bool {
	if td.Status != companyTargetFailed || td.Session == "" {
		return false
	}
	if len(targetFilter) > 0 && !targetFilter[td.Session] {
		return false
	}
	if strings.HasPrefix(td.Detail, companyReasonAttemptsExhausted) && !includeFailed {
		return false
	}
	return true
}

// redriveReceiptID resolves the receipt id and a human-readable selector from a
// redrive request: an explicit receipt id, or the derived id of an origin
// triple. Exactly one must be present.
func redriveReceiptID(body companyRedriveRequest) (id, selector string, err error) {
	if body.Receipt != "" {
		if body.Origin != nil {
			return "", "", errBadRequest("provide exactly one of receipt or origin")
		}
		// Validate the id shape before any path use (^in-[A-Za-z0-9._-]+$): a
		// traversal- or NUL-shaped receipt id must be rejected here, not joined
		// into a store path by GetByID.
		if !isReceiptID(body.Receipt) {
			return "", "", errBadRequest("receipt must match the receipt-id shape in-<...>")
		}
		return body.Receipt, body.Receipt, nil
	}
	if body.Origin == nil {
		return "", "", errBadRequest("one of receipt or origin is required")
	}
	o := *body.Origin
	if o.TeamID == "" || o.ChannelID == "" || o.TS == "" {
		return "", "", errBadRequest("origin requires team_id, channel_id, and ts")
	}
	return receiptID(o), o.TeamID + ":" + o.ChannelID + ":" + o.TS, nil
}

type adminBadRequest struct{ msg string }

func (e adminBadRequest) Error() string { return e.msg }
func errBadRequest(msg string) error    { return adminBadRequest{msg: msg} }

// storeOrNil returns the gateway's receipt store or nil (degraded).
func (g *companyGateway) storeOrNil() *IngressReceiptStore {
	if g == nil {
		return nil
	}
	return g.store()
}

// parseTripleParam parses a "a:b:c" query param into a ReceiptOrigin-shaped
// triple (team, channel, ts-or-root_ts). Empty is a nil filter (no restriction);
// a malformed value writes a 400 and reports ok=false.
func parseTripleParam(w http.ResponseWriter, value, what string) (*ReceiptOrigin, bool) {
	if value == "" {
		return nil, true
	}
	parts := strings.Split(value, ":")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		writeAdminError(w, http.StatusBadRequest, what+" must be team:channel:ts")
		return nil, false
	}
	return &ReceiptOrigin{TeamID: parts[0], ChannelID: parts[1], TS: parts[2]}, true
}

func writeAdminJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("company admin: encode response: %v", err)
	}
}

func writeAdminError(w http.ResponseWriter, status int, msg string) {
	writeAdminJSON(w, status, map[string]any{"error": msg})
}
