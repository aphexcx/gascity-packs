Record the singleton per-agent DM binding (agent -> session).

Each agent identity app owns exactly one DM-bound session: a human DM to
the agent's app wakes this session with `kind: dm`. The binding is
singleton by construction — rebinding the same agent replaces the session
in place, so ambiguity is impossible. A woken DM with no binding is a
recorded delivery failure (`failed_dm_unbound`), recoverable via
`gc slack company-redrive` after a binding is imported — never a legacy
fallback.

The agent must already exist in the imported company directory
(<cityPath>/.gc/slack/company_directory.json). A missing directory or an
unknown agent name is rejected non-zero. The binding is written to
<cityPath>/.gc/slack/dm_bindings.json (override with
$SLACK_COMPANY_DM_BINDINGS_PATH) via an atomic temp-file + rename.

`--city` city-qualifies the binding when the session lives in a different
gc city on the same host (the adapter needs a matching
$SLACK_COMPANY_CITY_APIS entry); empty means the adapter's own city. A
(session, city) pair may DM-bind only one agent.

Examples
--------

  gc slack bind-company-dm \
      --agent ollie \
      --session ollie

Rebind (replaces the existing entry, still one DM binding for the agent):

  gc slack bind-company-dm \
      --agent ollie \
      --session ollie-backup

Cross-city binding:

  gc slack bind-company-dm \
      --agent ollie \
      --session ollie-main \
      --city orchestration

Routes to: scripts/slack_company_directory.py bind-company-dm
