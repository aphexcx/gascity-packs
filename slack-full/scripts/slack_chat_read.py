#!/usr/bin/env python3
"""Read Slack channel or thread history natively via the pack's bot token.

`gc slack read` is the read-side complement to publish/reply-current:
it calls Slack's ``conversations.history`` / ``conversations.replies``
Web API directly with the adapter's bot token, so history reads keep
working even when the claude.ai Slack MCP — or the local adapter
process itself — is down. The only moving parts are this script, the
env file at ``~/.config/gc-slack-adapter/env``, and slack.com.

Modes:

  * channel history (default): ``--conversation-id Cxxx`` →
    ``conversations.history``. Slack returns newest-first; output is
    re-ordered oldest → newest for reading.
  * thread: add ``--thread-ts <ts>`` → ``conversations.replies``,
    oldest-first from the thread parent.

Scope notes (verified against the live app 2026-08-04): the bot token
carries ``channels:history``, ``groups:history``, ``im:history``,
``mpim:history`` (reads), ``users:read`` (author names), and
``files:read`` (attachment downloads). There is deliberately NO
``gc slack search``: Slack's ``search.messages`` only accepts a USER
token with ``search:read``, and this pack is bot-token-only by policy
— acting under Afik's user identity is forbidden (memory
slack-web-compose-hazard). Scope known channels with ``read`` instead.

The bot must be a member of the conversation it reads — Slack returns
``not_in_channel`` otherwise, which this script maps to a clear
invite-the-bot error.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import pathlib
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import slack_intake_common as common  # noqa: F401 — import loads the adapter env file


DEFAULT_SLACK_API_BASE = "https://slack.com/api"
DEFAULT_INBOUND_FILE_STORE = "/tmp/gc-slack-adapter/inbound"

# Slack caps conversations.history/replies page size at 200 (and warns
# above); requests beyond --limit paginate via response_metadata cursors.
_PAGE_MAX = 200
# 429 retries: Slack Tier-3 methods allow ~50 req/min; a handful of
# honored Retry-After waits rides out a burst without hanging forever.
_RATE_LIMIT_RETRIES = 4
_RATE_LIMIT_MAX_SLEEP = 60.0


class SlackAPIError(RuntimeError):
    """A Slack Web API call failed. ``error`` carries Slack's error code."""

    def __init__(self, message: str, error: str = "") -> None:
        super().__init__(message)
        self.error = error


def slack_api_base() -> str:
    return os.environ.get("SLACK_API_BASE_URL", DEFAULT_SLACK_API_BASE).rstrip("/")


def bot_token() -> str:
    token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit(
            "SLACK_BOT_TOKEN is not set. The adapter env file "
            "(~/.config/gc-slack-adapter/env or $GC_SLACK_ADAPTER_ENV) was not "
            "found or does not export it; history reads need the bot token."
        )
    return token


def _sleep(seconds: float) -> None:
    """Indirection for tests to stub out rate-limit waits."""
    time.sleep(seconds)


def slack_get(method: str, params: dict[str, str], token: str) -> dict[str, Any]:
    """GET a Slack Web API method, honoring 429 Retry-After.

    Raises SlackAPIError on transport failure, non-2xx status (after
    retries), or an ``ok: false`` payload. Rate-limit responses (HTTP
    429 or ``error: ratelimited``) are retried up to
    ``_RATE_LIMIT_RETRIES`` times, sleeping the server-advised interval.
    """
    url = f"{slack_api_base()}/{method}?" + urllib.parse.urlencode(params)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    attempts = _RATE_LIMIT_RETRIES + 1
    for attempt in range(attempts):
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                if attempt < attempts - 1:
                    _sleep(_retry_after_seconds(exc.headers.get("Retry-After")))
                    continue
                raise SlackAPIError(
                    f"{method}: rate limited after {attempts} attempts",
                    error="ratelimited") from exc
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            raise SlackAPIError(f"{method} -> HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise SlackAPIError(f"{method} failed: {exc}") from exc
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SlackAPIError(f"{method}: response is not JSON: {raw[:200]!r}") from exc
        if not isinstance(data, dict):
            raise SlackAPIError(f"{method}: unexpected response shape: {raw[:200]!r}")
        if data.get("ok"):
            return data
        error = str(data.get("error", "unknown_error"))
        if error == "ratelimited" and attempt < attempts - 1:
            _sleep(_retry_after_seconds(None))
            continue
        raise SlackAPIError(f"{method} not ok: {error}", error=error)
    raise SlackAPIError(f"{method}: rate limited after {attempts} attempts", error="ratelimited")


def _retry_after_seconds(header: str | None) -> float:
    try:
        seconds = float(header) if header else 1.0
    except ValueError:
        seconds = 1.0
    return max(0.5, min(seconds, _RATE_LIMIT_MAX_SLEEP))


def _friendly_api_error(exc: SlackAPIError, conversation_id: str, thread_ts: str) -> str:
    """Map Slack error codes to actionable operator messages."""
    hints = {
        "not_in_channel": (
            f"the bot is not a member of {conversation_id} — invite it in Slack "
            "(/invite the app's bot user in that channel) and retry"
        ),
        "channel_not_found": (
            f"channel {conversation_id} not found or not visible to the bot "
            "(wrong id, or a private channel the bot was never invited to)"
        ),
        "thread_not_found": (
            f"no thread rooted at ts {thread_ts or '?'} in {conversation_id} — "
            "thread-ts must be the PARENT message's ts"
        ),
        "missing_scope": (
            "the bot token lacks a history scope for this conversation type "
            "(needs channels:history / groups:history / im:history / mpim:history); "
            "grant it at api.slack.com and reinstall the app"
        ),
        "invalid_auth": "Slack rejected the bot token (invalid_auth) — check the adapter env file",
        "account_inactive": "Slack rejected the bot token (account_inactive)",
        "token_revoked": "Slack rejected the bot token (token_revoked) — reissue it at api.slack.com",
        "ratelimited": "Slack rate limit persisted after retries — wait a minute and retry",
    }
    return hints.get(exc.error, str(exc))


# --- fetch ----------------------------------------------------------------

def _paginate(method: str, base_params: dict[str, str], limit: int,
              token: str) -> tuple[list[dict[str, Any]], bool]:
    """Collect up to ``limit`` messages across cursor pages.

    Returns (messages in Slack's native order for the method, has_more).
    has_more is True when Slack reported more matching messages beyond
    what was collected.
    """
    messages: list[dict[str, Any]] = []
    cursor = ""
    while True:
        params = dict(base_params)
        params["limit"] = str(min(_PAGE_MAX, limit - len(messages)))
        if cursor:
            params["cursor"] = cursor
        data = slack_get(method, params, token)
        page = [m for m in (data.get("messages") or []) if isinstance(m, dict)]
        messages.extend(page)
        cursor = ((data.get("response_metadata") or {}).get("next_cursor") or "").strip()
        more_reported = bool(data.get("has_more")) or bool(cursor)
        if len(messages) >= limit:
            return messages[:limit], more_reported or len(messages) > limit
        if not cursor or not page:
            return messages, False


def fetch_history(conversation_id: str, *, limit: int, oldest: str, newest: str,
                  token: str) -> tuple[list[dict[str, Any]], bool]:
    """Channel history, returned chronologically (oldest → newest).

    conversations.history yields newest-first, so ``limit`` naturally
    keeps the LATEST messages of the window — the outage-recovery case
    ("what did I miss?") — and the result is reversed for display.
    """
    params = {"channel": conversation_id}
    if oldest:
        params["oldest"] = oldest
    if newest:
        params["latest"] = newest
    messages, has_more = _paginate("conversations.history", params, limit, token)
    return list(reversed(messages)), has_more


def fetch_replies(conversation_id: str, thread_ts: str, *, limit: int, oldest: str,
                  newest: str, token: str) -> tuple[list[dict[str, Any]], bool]:
    """Thread messages, oldest-first from the parent (Slack's native order)."""
    params = {"channel": conversation_id, "ts": thread_ts}
    if oldest:
        params["oldest"] = oldest
    if newest:
        params["latest"] = newest
    return _paginate("conversations.replies", params, limit, token)


# --- author resolution ----------------------------------------------------

def resolve_authors(messages: list[dict[str, Any]], token: str) -> dict[str, str]:
    """Map user ids appearing in ``messages`` to display names via users.info.

    Best-effort: any lookup failure (missing scope, deleted user, rate
    limit) leaves that id unmapped and the renderer falls back to the
    raw id. One API call per unique id, so cost is bounded by the
    distinct-author count of the fetched window.
    """
    names: dict[str, str] = {}
    for msg in messages:
        user = (msg.get("user") or "").strip()
        if not user or user in names:
            continue
        try:
            data = slack_get("users.info", {"user": user}, token)
        except SlackAPIError:
            continue
        profile = (data.get("user") or {}).get("profile") or {}
        name = (
            (profile.get("display_name") or "").strip()
            or (profile.get("real_name") or "").strip()
            or ((data.get("user") or {}).get("name") or "").strip()
        )
        if name:
            names[user] = name
    return names


def author_label(msg: dict[str, Any], names: dict[str, str]) -> str:
    user = (msg.get("user") or "").strip()
    if user:
        name = names.get(user, "")
        return f"{name} ({user})" if name else user
    # Bot-authored messages carry bot_profile/username instead of user.
    bot_name = ((msg.get("bot_profile") or {}).get("name") or msg.get("username") or "").strip()
    bot_id = (msg.get("bot_id") or "").strip()
    if bot_name and bot_id:
        return f"{bot_name} (bot {bot_id})"
    return bot_name or (f"bot {bot_id}" if bot_id else "?")


# --- attachment downloads -------------------------------------------------

def _safe_path_component(value: str) -> str:
    """Mirror the adapter's safePathComponent: [A-Za-z0-9_.-] allowlist,
    leading dot replaced so the result is never hidden/`.`/`..`."""
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", value)
    if cleaned.startswith("."):
        cleaned = "_" + cleaned[1:]
    return cleaned or "_"


def _is_slack_file_url(raw: str) -> bool:
    """Mirror the adapter's isSlackFileURL gate: https, port 443, host
    (sub)domain of slack.com / slack-files.com. The bot token rides the
    Authorization header of the download, so it must never be sent to a
    host outside Slack's CDN — even one named by a Slack API response."""
    try:
        u = urllib.parse.urlsplit(raw)
    except ValueError:
        return False
    if u.scheme != "https" or not u.hostname:
        return False
    try:
        if u.port not in (None, 443):
            return False
    except ValueError:
        return False
    host = u.hostname.lower()
    return (
        host in ("slack.com", "slack-files.com")
        or host.endswith(".slack.com")
        or host.endswith(".slack-files.com")
    )


def download_file(url: str, dest: pathlib.Path, token: str) -> None:
    """Bearer-auth GET ``url`` to ``dest``. Redirects are refused — a 3xx
    would make urllib re-send the Authorization header to the new host."""
    if not _is_slack_file_url(url):
        raise SlackAPIError(f"refusing non-Slack file host: {urllib.parse.urlsplit(url).hostname!r}")

    class _NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args: Any, **kwargs: Any) -> None:
            return None

    opener = urllib.request.build_opener(_NoRedirect)
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with opener.open(req, timeout=60) as resp:
            body = resp.read()
    except urllib.error.HTTPError as exc:
        raise SlackAPIError(f"file download -> HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise SlackAPIError(f"file download failed: {exc}") from exc
    dest.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(dest.parent, 0o700)  # store may hold DM content — owner-only, like the adapter
    dest.write_bytes(body)


def spool_files(messages: list[dict[str, Any]], conversation_id: str, store: str,
                token: str) -> dict[str, str]:
    """Download every attachment to the adapter's inbound-store layout:
    ``<store>/<channel>/<ts>-<safe-filename>``. Returns file-id → local
    path for the downloads that succeeded; failures warn and are skipped
    so one bad file doesn't sink the read."""
    local: dict[str, str] = {}
    base = pathlib.Path(store) / _safe_path_component(conversation_id)
    for msg in messages:
        ts = _safe_path_component(str(msg.get("ts") or ""))
        for f in msg.get("files") or []:
            fid = str(f.get("id") or "")
            url = str(f.get("url_private") or "")
            if not fid or not url:
                continue
            name = str(f.get("name") or f.get("title") or fid)
            dest = base / f"{ts}-{_safe_path_component(name)}"
            try:
                download_file(url, dest, token)
            except SlackAPIError as exc:
                print(f"warning: file {fid} ({name}) download failed: {exc}", file=sys.stderr)
                continue
            local[fid] = str(dest)
    return local


# --- rendering ------------------------------------------------------------

def _ts_human(ts: str) -> str:
    try:
        moment = _dt.datetime.fromtimestamp(float(ts), tz=_dt.timezone.utc)
    except (ValueError, OverflowError, OSError):
        return "?"
    return moment.strftime("%Y-%m-%d %H:%M:%SZ")


def message_record(msg: dict[str, Any], names: dict[str, str],
                   local_files: dict[str, str]) -> dict[str, Any]:
    """Curated per-message record shared by the JSON and text renderers."""
    files = []
    for f in msg.get("files") or []:
        fid = str(f.get("id") or "")
        entry: dict[str, Any] = {
            "id": fid,
            "name": str(f.get("name") or f.get("title") or fid),
            "mimetype": str(f.get("mimetype") or ""),
            "size": f.get("size"),
            "url_private": str(f.get("url_private") or ""),
        }
        if fid in local_files:
            entry["local_path"] = local_files[fid]
        files.append(entry)
    record: dict[str, Any] = {
        "ts": str(msg.get("ts") or ""),
        "user": str(msg.get("user") or ""),
        "author": author_label(msg, names),
        "text": str(msg.get("text") or ""),
    }
    if msg.get("subtype"):
        record["subtype"] = str(msg["subtype"])
    thread_ts = str(msg.get("thread_ts") or "")
    if thread_ts:
        record["thread_ts"] = thread_ts
    if msg.get("reply_count"):
        record["reply_count"] = msg["reply_count"]
    if files:
        record["files"] = files
    return record


def render_text(records: list[dict[str, Any]], *, conversation_id: str, mode: str,
                thread_ts: str, has_more: bool, downloaded: bool) -> str:
    lines: list[str] = []
    scope = f"thread {thread_ts}" if mode == "replies" else "history"
    noun = "message" if len(records) == 1 else "messages"
    header = f"{conversation_id} {scope} — {len(records)} {noun} (oldest → newest)"
    if has_more:
        header += "; more available (raise --limit or narrow with --oldest/--newest)"
    lines.append(header)
    for rec in records:
        lines.append("")
        head = f"[{_ts_human(rec['ts'])}] {rec['author']} ts={rec['ts']}"
        if rec.get("subtype"):
            head += f" ({rec['subtype']})"
        lines.append(head)
        for text_line in (rec["text"] or "(no text)").splitlines() or ["(no text)"]:
            lines.append(f"  {text_line}")
        for f in rec.get("files", []):
            size = f" {f['size']} bytes," if f.get("size") else ""
            desc = f"  [file] {f['name']} ({f['mimetype'] or 'unknown type'},{size} id {f['id']})"
            if f.get("local_path"):
                desc += f" — saved to {f['local_path']}"
            elif not downloaded:
                desc += " — pass --download to spool locally"
            lines.append(desc)
        replies = rec.get("reply_count") or 0
        if mode == "history" and replies:
            lines.append(
                f"  ↳ {replies} replies in thread — "
                f"gc slack read --conversation-id {conversation_id} --thread-ts {rec['ts']}"
            )
    return "\n".join(lines)


# --- entry point ----------------------------------------------------------

def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Read Slack channel or thread history via the pack's bot token",
    )
    parser.add_argument("--conversation-id", required=True,
                        help="Slack conversation id (Cxxx channel, Gxxx group, Dxxx DM)")
    parser.add_argument("--thread-ts", default="",
                        help="Read the thread rooted at this parent ts instead of channel history")
    parser.add_argument("--limit", type=int, default=50,
                        help="Max messages to return (paginates as needed). Default: 50")
    parser.add_argument("--oldest", default="",
                        help="Only messages after this Slack ts (e.g. 1754323330.000100)")
    parser.add_argument("--newest", default="",
                        help="Only messages before this Slack ts")
    parser.add_argument("--download", action="store_true",
                        help="Download attachments to the inbound file store")
    parser.add_argument("--download-dir", default="",
                        help="Attachment store root. Default: $INBOUND_FILE_STORE "
                             f"or {DEFAULT_INBOUND_FILE_STORE}")
    parser.add_argument("--no-names", action="store_true",
                        help="Skip users.info author resolution (faster; raw ids only)")
    parser.add_argument("--json", dest="as_json", action="store_true",
                        help="Emit machine-readable JSON")
    args = parser.parse_args(argv)

    if args.limit < 1:
        raise SystemExit("--limit must be a positive integer")
    conversation_id = args.conversation_id.strip()
    thread_ts = args.thread_ts.strip()
    token = bot_token()

    mode = "replies" if thread_ts else "history"
    try:
        if thread_ts:
            messages, has_more = fetch_replies(
                conversation_id, thread_ts,
                limit=args.limit, oldest=args.oldest.strip(),
                newest=args.newest.strip(), token=token)
        else:
            messages, has_more = fetch_history(
                conversation_id,
                limit=args.limit, oldest=args.oldest.strip(),
                newest=args.newest.strip(), token=token)
    except SlackAPIError as exc:
        raise SystemExit(f"gc slack read: {_friendly_api_error(exc, conversation_id, thread_ts)}") from exc

    names = {} if args.no_names else resolve_authors(messages, token)
    local_files: dict[str, str] = {}
    if args.download:
        store = args.download_dir.strip() or os.environ.get(
            "INBOUND_FILE_STORE", DEFAULT_INBOUND_FILE_STORE)
        local_files = spool_files(messages, conversation_id, store, token)

    records = [message_record(m, names, local_files) for m in messages]
    if args.as_json:
        out: dict[str, Any] = {
            "conversation_id": conversation_id,
            "mode": mode,
            "count": len(records),
            "has_more": has_more,
            "messages": records,
        }
        if thread_ts:
            out["thread_ts"] = thread_ts
        print(json.dumps(out, indent=2, sort_keys=True))
    else:
        print(render_text(records, conversation_id=conversation_id, mode=mode,
                          thread_ts=thread_ts, has_more=has_more,
                          downloaded=args.download))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
