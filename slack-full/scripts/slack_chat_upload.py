#!/usr/bin/env python3
"""Upload a file to a session's bound Slack channel.

Two routing paths are supported, mirroring ``slack_chat_reply_current``:

* **gc-routed (default)** — POST to gc's
  ``/v0/city/{city}/extmsg/outbound-file``. gc records the upload in the
  conversation transcript, fans out a peer notification to other
  sessions bound to the same room, and emits an ``extmsg.outbound``
  event. This is the production path because it keeps the multi-session
  coordination story consistent between text and files.

* **adapter-direct (``--via adapter``)** — POST straight to the local
  adapter's ``/publish-file``. Bypasses gc entirely; no transcript, no
  peer fanout. Kept as a fast path for adapter-only smoke tests and
  diagnostics — when the gc API is down or you're testing the adapter
  in isolation.

A third, bindingless mode mirrors ``slack_chat_publish_to_channel``:
pass ``--conversation-id`` (plus optional ``--kind``) to skip the
extmsg-binding lookup entirely and post the file straight to the
adapter's ``/publish-file``. This is how sessions without a binding
(mayor, chief-of-staff) attach files to a channel they were addressed
from — the text-side equivalent is ``gc slack publish-to-channel``.
Like that command, the bindingless path exits non-zero when the
adapter receipt reports ``delivered=false``.

Either path delegates to the adapter (``adapter/``, pack-relative),
which handles Slack's three-step files-upload-v2 protocol
(``files.getUploadURLExternal`` → ``PUT`` bytes →
``files.completeUploadExternal``).

The bot token must hold the ``files:write`` scope. Without it, the
adapter returns ``failure_kind=auth`` with ``error=missing_scope`` and
the post falls through cleanly (no exception); the user can grant the
scope without restarting anything and the next upload picks it up.

Files post under the bot's default identity, NOT the per-session
``chat:write.customize`` identity — Slack's file-upload API doesn't
honor that override on the file post itself. Use a chat reply
(``gc slack reply-current``) when identity matters more than the file
preview, or pair an upload with a follow-up reply.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

import slack_intake_common as common


def _resolve_conversation(session_id: str) -> dict[str, str]:
    binding = common.look_up_binding(session_id)
    if not binding:
        raise SystemExit(
            f"session {session_id!r} has no active extmsg binding; "
            f"run `gc slack bind-dm` or `gc slack bind-room` first")
    return {
        "scope_id": binding.get("scope_id", common.gc_city_name()),
        "provider": binding.get("provider", "slack"),
        "account_id": binding.get("account_id",
                                  os.environ.get("SLACK_WORKSPACE_ID", "")),
        "conversation_id": binding.get("conversation_id", ""),
        "kind": binding.get("kind", "dm"),
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Upload a file to a session's bound Slack channel "
                    "via the local adapter (files-upload-v2)",
    )
    parser.add_argument("--file", dest="file_path", required=True,
                        help="Path to the local file to upload.")
    parser.add_argument("--session", default="",
                        help="Session id whose binding to upload into. "
                             "Defaults to the current session ($GC_SESSION_ID).")
    parser.add_argument("--conversation-id", default="",
                        help="Explicit Slack channel id (C..., G..., or D...). "
                             "Skips the extmsg-binding lookup and posts direct "
                             "to the adapter (files-upload-v2), matching "
                             "publish-to-channel. For sessions with no binding "
                             "(mayor / chief-of-staff).")
    parser.add_argument("--kind", default="",
                        choices=("dm", "room", "thread"),
                        help="Conversation kind for the conversation envelope "
                             "when --conversation-id is used. Defaults to "
                             "'room' (channels).")
    parser.add_argument("--filename", default="",
                        help="Override the displayed filename. Defaults to "
                             "basename(--file).")
    parser.add_argument("--title", default="",
                        help="Display title in Slack. Defaults to filename.")
    parser.add_argument("--initial-comment", default="",
                        help="Comment posted alongside the file (acts as the "
                             "message body for threading purposes).")
    parser.add_argument("--thread-ts", default="",
                        help="Slack message ts to thread under. Mutually "
                             "exclusive with --thread-current.")
    parser.add_argument("--thread-current", action="store_true",
                        help="Thread under the latest inbound for this session "
                             "(same logic as `gc slack reply-current`). "
                             "Mutually exclusive with --thread-ts.")
    parser.add_argument("--idempotency-key", default="",
                        help="Caller-supplied idempotency key for retries.")
    parser.add_argument("--via", choices=("gc", "adapter"), default="",
                        help="Routing path. 'gc' (default) records the upload "
                             "in the transcript and fans out to peer sessions; "
                             "'adapter' bypasses gc for diagnostics only. "
                             "--conversation-id implies 'adapter' (gc's "
                             "outbound-file endpoint requires a binding).")
    args = parser.parse_args(argv)

    if args.thread_ts and args.thread_current:
        raise SystemExit("pass --thread-ts OR --thread-current, not both")

    conversation_id = args.conversation_id.strip()
    if args.kind and not conversation_id:
        raise SystemExit(
            "--kind only applies with --conversation-id; without it the "
            "kind comes from the session's binding")
    if conversation_id:
        if args.via == "gc":
            raise SystemExit(
                "--conversation-id posts bindingless via the adapter and "
                "cannot be combined with --via gc (gc's outbound-file "
                "endpoint requires an extmsg binding)")
        if args.thread_current:
            raise SystemExit(
                "--thread-current resolves the latest inbound via the "
                "session's binding and cannot be combined with "
                "--conversation-id; pass --thread-ts <ts> explicitly")
    via = args.via or ("adapter" if conversation_id else "gc")

    file_path = pathlib.Path(args.file_path).expanduser().resolve()
    if not file_path.exists():
        raise SystemExit(f"file not found: {file_path}")
    if not file_path.is_file():
        raise SystemExit(f"not a regular file: {file_path}")

    session_id = (args.session or "").strip()
    if not session_id:
        try:
            session_id = common.current_session_id()
        except common.GCAPIError as exc:
            raise SystemExit(str(exc)) from exc

    if conversation_id:
        if not os.environ.get("SLACK_WORKSPACE_ID", "").strip():
            raise SystemExit(
                "SLACK_WORKSPACE_ID is not set; cannot construct "
                "conversation envelope")
        conv = {
            "scope_id": common.gc_city_name(),
            "provider": "slack",
            "account_id": os.environ["SLACK_WORKSPACE_ID"],
            "conversation_id": conversation_id,
            "kind": args.kind or "room",
        }
    else:
        conv = _resolve_conversation(session_id)
        if not conv.get("conversation_id"):
            raise SystemExit(
                f"session {session_id!r} binding has no conversation_id "
                "(corrupt binding record?)")

    thread_ts = args.thread_ts.strip()
    if args.thread_current:
        match = common.find_latest_inbound_message_id_for_session(session_id)
        if not match:
            raise SystemExit(
                f"session {session_id!r} has no inbound to thread under; "
                "pass --thread-ts <ts> explicitly or omit threading")
        thread_ts = match[0]

    try:
        if via == "adapter":
            result = common.upload_via_adapter(
                session_id=session_id,
                conversation_id=conv["conversation_id"],
                kind=conv["kind"],
                file_path=str(file_path),
                filename=args.filename,
                initial_comment=args.initial_comment,
                thread_ts=thread_ts,
                title=args.title,
                idempotency_key=args.idempotency_key,
            )
        else:
            result = common.upload_via_gc_outbound_file(
                session_id=session_id,
                scope_id=conv["scope_id"],
                provider=conv["provider"],
                account_id=conv["account_id"],
                conversation_id=conv["conversation_id"],
                kind=conv["kind"],
                file_path=str(file_path),
                filename=args.filename,
                initial_comment=args.initial_comment,
                thread_ts=thread_ts,
                title=args.title,
                idempotency_key=args.idempotency_key,
            )
    except (common.AdapterError, common.GCAPIError) as exc:
        raise SystemExit(str(exc)) from exc

    print(json.dumps({
        "session_id": session_id,
        "conversation_id": conv["conversation_id"],
        "kind": conv["kind"],
        "file_path": str(file_path),
        "thread_ts": thread_ts,
        "via": via,
        "result": result,
    }, indent=2))

    if conversation_id:
        # Bindingless mode mirrors publish-to-channel's receipt contract:
        # an HTTP 200 with delivered=false (auth, missing scope, archived
        # channel) must surface as a non-zero exit so the caller notices.
        delivered, failure_kind = common.interpret_publish_receipt(result)
        if not delivered:
            print(
                f"slack upload failed: delivered=false "
                f"failure_kind={failure_kind or 'unknown'}",
                file=sys.stderr,
            )
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
