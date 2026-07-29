Report the company directory: rooms, members, wake policy, and bindings.

Reads the normalized company_directory.json and company_bindings.json
registries and prints, per room:

  - members (agents that may be activated in the room)
  - ambient_wake (members woken by plain human messages)
  - mention_wake (members eligible for native @-mention activation)
  - bindings (the (agent -> session) delivery targets for the room)

Bindings that reference a room or agent no longer in the directory are
dropped and surfaced under `binding_warnings` (the reader drops missing
references rather than failing).

Switchboard/member presence is checked best-effort against
conversations.info / conversations.members and surfaced under
`membership_warnings` — a room the switchboard has not joined is flagged
inert. A missing token, missing scope, or failed check degrades to a
warning and never errors.

Examples
--------

  gc slack peers
  gc slack peers --room orchestrator-team

Routes to: scripts/slack_company_directory.py peers
