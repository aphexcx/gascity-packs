Record the singleton (room, agent) -> session company binding.

A woken company agent delivers only to the session bound here; a woken
agent with no binding is a recorded delivery failure, never a legacy
fallback. The binding is singleton by construction — rebinding the same
(room, agent) replaces the session in place, so ambiguity is impossible.

The room and agent must already exist in the imported company directory
(<cityPath>/.gc/slack/company_directory.json). A missing directory or an
unknown room/agent name is rejected non-zero. The binding is written to
<cityPath>/.gc/slack/company_bindings.json (override with
$SLACK_COMPANY_BINDINGS_PATH) via an atomic temp-file + rename.

Examples
--------

  gc slack bind-company-agent \
      --room orchestrator-team \
      --agent ollie \
      --session ollie-main

Rebind (replaces the existing entry, still one binding for the pair):

  gc slack bind-company-agent \
      --room orchestrator-team \
      --agent ollie \
      --session ollie-backup

Routes to: scripts/slack_company_directory.py bind-company-agent
