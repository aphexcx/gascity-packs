Reply to the latest Slack inbound event seen by the current session.

The session's recent transcript is scanned for the most recent
`extmsg.inbound` system-reminder. The reply is published through the
local Slack adapter's /publish endpoint to the same conversation.

Examples:
  gc slack reply-current --body "ack"
  gc slack reply-current --body-file /tmp/reply.txt
  gc slack reply-current --turn-ref gct-0123456789abcdef0123 --body-file /tmp/reply.txt
  gc slack reply-current --conversation-id D0B0TTS550F --body "explicit channel"

If the session has no inbound history, --conversation-id is required.

Company reminders include an immutable `turn_ref` and an exact command. Copy
that command: `--turn-ref` binds the reply to the originating channel/thread
even if a newer message in another channel wakes the same agent session.
Post-rollout company turns fail closed when the flag is omitted.
