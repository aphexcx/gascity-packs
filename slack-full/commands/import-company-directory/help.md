Validate, normalize, and atomically import a company-rooms directory.

Reads the non-secret directory TOML, enforces every directory rule
(lowercase slug names; unique agent names, app_ids, bot_user_ids, room
names, and (team_id, channel_id) pairs; wake lists that are subsets of
members; `*` allowed only in `members`/`mention_wake`, never in
`ambient_wake`; no unknown agent references), expands `*` wildcards, and
writes the normalized registry to
<cityPath>/.gc/slack/company_directory.json (override with
$SLACK_COMPANY_DIRECTORY_PATH). The written record carries a
`source_sha256` of the TOML bytes and an RFC3339 `imported_at`.

Validation is fail-closed: an invalid file is rejected non-zero and any
existing registry is left byte-for-byte untouched — routing never runs
against a half-written directory.

Switchboard/member presence is verified best-effort via
conversations.info / conversations.members. A missing token, missing
scope, or failed check is reported as a warning and never fails the
import — a directory room the switchboard has not joined stays configured
but is flagged inert.

Example directory TOML
----------------------

  schema_version = 1

  [[agents]]
  name = "ollie"
  app_id = "A0AAAAAA1"
  bot_user_id = "U0AAAAAA1"

  [[rooms]]
  name = "orchestrator-team"
  team_id = "T0AAAAAAA"
  channel_id = "C0AAAAAAA"
  members = ["*"]           # every listed agent
  ambient_wake = ["ollie"]  # wake on plain human messages (no wildcard)
  mention_wake = ["*"]      # every member is mention-eligible

Examples
--------

  gc slack import-company-directory --file /path/to/rooms.toml

Re-importing the same TOML is idempotent — the normalized agents/rooms and
`source_sha256` are unchanged.

Routes to: scripts/slack_company_directory.py import-company-directory
