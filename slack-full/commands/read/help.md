# gc slack read

Read Slack channel or thread history directly from the Slack Web API
(`conversations.history` / `conversations.replies`) using the pack's
bot token. No claude.ai MCP, no adapter process in the path — reads
work whenever slack.com and the adapter env file are reachable, which
is what makes mayors outage-proof for history recovery.

## Usage

```
gc slack read --conversation-id Cxxx [--limit N] [--oldest ts] [--newest ts]
gc slack read --conversation-id Cxxx --thread-ts 1234.5678 [...]
```

## Modes

- **Channel history** (default): latest messages of the conversation,
  printed oldest → newest. `--limit` keeps the *newest* N of the
  window — the "what did I miss?" case.
- **Thread** (`--thread-ts`): the thread rooted at that parent ts,
  oldest-first, parent included. `--thread-ts` must be the PARENT
  message's ts (shown in the `↳ N replies` hint of channel output).

## Flags

- `--conversation-id Cxxx` — channel (Cxxx), private group (Gxxx), or
  DM (Dxxx) id. Required.
- `--thread-ts TS` — read a thread instead of channel history.
- `--limit N` — max messages, paginating under the hood. Default: 50.
- `--oldest TS` / `--newest TS` — bound the window by Slack ts.
- `--download` — spool attachments to the adapter's inbound file
  store layout (`$INBOUND_FILE_STORE/<channel>/<ts>-<name>`, default
  `/tmp/gc-slack-adapter/inbound`); output shows the local paths.
- `--download-dir PATH` — override the store root.
- `--no-names` — skip `users.info` author resolution (faster).
- `--json` — machine-readable output.

## Requirements & failure modes

- Bot token comes from `~/.config/gc-slack-adapter/env` (or
  `$GC_SLACK_ADAPTER_ENV`); scopes `channels:history`,
  `groups:history`, `im:history`, `mpim:history` cover the read,
  `users:read` the author names, `files:read` the downloads.
- The bot must be **in the conversation**: Slack answers
  `not_in_channel` otherwise and the command tells you to invite it.
- Rate limits (HTTP 429) are retried honoring `Retry-After`.

## No `gc slack search`

Slack's `search.messages` API only accepts a **user** token carrying
`search:read` — a bot token cannot search, period. This pack stays
bot-token-only by policy (the bot must never act as Afik), so there is
deliberately no search verb. Recover history by reading the channels
you know (`gc slack read --conversation-id ...`), bounding with
`--oldest`/`--newest` as needed.

## Examples

```bash
# Catch up on the last 20 messages of a rig channel:
gc slack read --conversation-id C0B1A0CKEH0 --limit 20

# Recover a verdict thread lost during an MCP outage:
gc slack read --conversation-id C0B1A0CKEH0 --thread-ts 1754323330.000100

# Everything since a known ts, with attachments spooled locally:
gc slack read --conversation-id C0B1A0CKEH0 --oldest 1754300000.000000 --download

# Feed a script:
gc slack read --conversation-id C0B1A0CKEH0 --limit 200 --json | jq '.messages[].text'
```
