Register a per-agent Slack app signing secret for app-bound DM verification.

Each agent identity app that subscribes to `message.im` (Phase 4 DMs) must
have its signing secret registered so the adapter can verify DM event POSTs
against THAT app's secret — the multi-secret trial alone proves only that
*some* registered app signed the request, which must never let one app's
secret admit events into another agent's DM spool.

The record `{team_id, api_app_id, signing_secret}` is written to
<cityPath>/.gc/slack/agent_apps.json (override with
$SLACK_COMPANY_AGENT_APPS_PATH), mode 0600. This registry is secret-bearing:
if it already exists with group/world-accessible permissions the verb refuses
to overwrite it (a possible leak — fix perms and rotate before re-running).
Re-registering an api_app_id replaces its secret (rotation).

Owner-agent identity is not stored here; it derives by joining api_app_id
against company_directory.json agents[].app_id. Registering an api_app_id with
no directory agent succeeds with a warning (it admits nothing until the
directory lists it), so a secret can be registered before the directory import
(runbook ordering).

Validation: api_app_id `^A[A-Z0-9]{6,}$`, team_id `^[TE][A-Z0-9]{6,}$`,
signing secret a 32-character hex string.

Examples
--------

Prefer the file form (a --signing-secret flag leaks via ps):

  gc slack register-agent-app \
      --team-id T0ARJCFV8QL \
      --api-app-id A0OLLIEAPP \
      --signing-secret-file /run/secrets/ollie-signing-secret

Direct (diagnostics / trusted shells only):

  gc slack register-agent-app \
      --team-id T0ARJCFV8QL \
      --api-app-id A0OLLIEAPP \
      --signing-secret 0123456789abcdef0123456789abcdef

Routes to: scripts/slack_register_agent_app.py
