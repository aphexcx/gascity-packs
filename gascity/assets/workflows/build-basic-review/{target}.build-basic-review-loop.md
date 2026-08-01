Run the build-basic starter factory review loop.

The child beads are three review lanes plus synthesis and fix application:
acceptance/correctness, test evidence, and simplicity/maintainability. These are
starter factory lanes: broad enough to demonstrate Gas City fanout/fanin, but
small enough for first-time factory users to understand.

The apply-review-findings lane owns `code_review.verdict=done|iterate` and
`code_review.report_path=<starter review summary path>`. The implementation
review check repeats this loop until the latest verdict is `done`.

Honor interaction_mode {{interaction_mode}} at the loop approval.

In `autonomous` mode, close the loop when the latest verdict is `done` without
waiting on a human. In `headless` mode, never ask questions and never wait on
a human; if the work item strictly requires human review approval, stop
blocked with a machine-readable `gc.blocked_reason` (for example
`interactive-approval-required:headless`) instead of waiting.

In `interactive` mode, the loop must not conclude on its own verdict. When the
latest verdict is `done`, send the starter review summary to the human gate
using the passive wait + mail pattern before closing this loop. This is not a
timeout-driven task.

1. Before waiting, update workflow root metadata with:
   - `gc.build.review_gate=waiting-human`
   - `gc.build.review_gate_bead_id=<this bead id>`
   - preserve any existing `gc.build.review_gate_mail_sent=true`
2. Park the current session so idle handling does not recycle it while the
   human decides:
   ```bash
   SESSION_TARGET="${GC_SESSION_ID:-${GC_SESSION_NAME:-}}"
   SESSION_ATTACH="${GC_SESSION_NAME:-$SESSION_TARGET}"
   WAIT_NOTE="Waiting for human approval of the implementation review on bead $GC_BEAD_ID."
   if [ -n "$SESSION_ATTACH" ]; then
     WAIT_NOTE="$WAIT_NOTE Resume with: gc session attach $SESSION_ATTACH"
   fi
   if [ -n "$SESSION_TARGET" ] && ! gc wait list --session "$SESSION_TARGET" | grep -Fq "$WAIT_NOTE"; then
     gc session wait "$SESSION_TARGET" \
       --sleep \
       --on-beads "$GC_BEAD_ID" \
       --note "$WAIT_NOTE"
   fi
   ```
3. If workflow root metadata does not already have
   `gc.build.review_gate_mail_sent=true`, send exactly one mail with
   `gc mail send human ...`. Include the starter review summary path, the
   workflow root id, this bead id, and the requested response options: approve,
   request changes, or reject. After sending, update workflow root metadata
   with `gc.build.review_gate_mail_sent=true` and
   `gc.build.review_gate_mail_to=human`.
4. Wait for explicit human feedback from the active session or mail thread. If
   the session idles, detaches, or restarts before the human responds, do not
   close this bead. A resumed worker must read the gate metadata and continue
   waiting from this gate.

Record exactly one terminal workflow-root metadata value after explicit human
feedback: `gc.build.review_gate=approved`, `rejected`, or
`revision_requested`. Use `approved` only after explicit human approval,
`revision_requested` when required fixes remain, and `rejected` when the
build must not proceed. Close fail only for explicit rejection or abort, not
for silence.

On `revision_requested`, before closing this bead:

1. Persist the human findings: append them to the starter review summary at
   `code_review.report_path` under a dated "Human review round" heading, so
   the next iteration's lanes and apply step consume them as required fixes.
2. Clear `gc.build.review_gate_mail_sent` on the workflow root so the next
   gate round notifies the human again.
3. Close this bead per its contract. The implementation review check reads
   `gc.build.review_gate=revision_requested` and schedules the next loop
   iteration — the gate metadata, not this bead staying open, is what drives
   the iteration.

Do not invoke provider-native subagents. Continue only through this Gas City
graph loop.

