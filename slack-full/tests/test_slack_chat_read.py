"""Tests for ``gc slack read`` — native channel/thread history via bot token.

The script talks straight to the Slack Web API (conversations.history,
conversations.replies, users.info) with the adapter's bot token. Tests
run ``main()`` in-process against a scripted local HTTP server standing
in for slack.com, so pagination, rate-limit retries, auth headers, and
error mapping are exercised over the real urllib transport.
"""

from __future__ import annotations

import http.server
import json
import pathlib
import socketserver
import sys
import threading
import urllib.parse
from typing import Any, Callable

import pytest

PACK_DIR = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PACK_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

Responder = Callable[[dict[str, str], dict[str, str]], tuple[int, Any]]


class ScriptedSlackAPI:
    """Local HTTP server whose per-path behavior is a test-supplied callable.

    A responder receives (query-params, request-headers) and returns
    (status, payload); dict payloads are JSON-encoded, bytes pass
    through. Every request is recorded for assertions.
    """

    def __init__(self) -> None:
        self.handlers: dict[str, Responder] = {}
        self.requests: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        outer = self

        class _Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 — stdlib API name
                parsed = urllib.parse.urlsplit(self.path)
                query = dict(urllib.parse.parse_qsl(parsed.query))
                with outer._lock:
                    outer.requests.append({
                        "path": parsed.path,
                        "query": query,
                        "auth": self.headers.get("Authorization", ""),
                    })
                responder = outer.handlers.get(parsed.path)
                if responder is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                status, payload = responder(query, dict(self.headers))
                body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
                self.send_response(status)
                if status == 429:
                    self.send_header("Retry-After", "1")
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: Any, **kwargs: Any) -> None:
                return

        self._server = socketserver.TCPServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True, name="scripted-slack-api")
        self._thread.start()

    @property
    def url(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def calls(self, path: str) -> list[dict[str, Any]]:
        with self._lock:
            return [r for r in self.requests if r["path"] == path]


@pytest.fixture()
def slack_api():
    api = ScriptedSlackAPI()
    yield api
    api.close()


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    # Point the adapter-env loader at a nonexistent file so importing
    # slack_intake_common on a dev box never slurps the REAL bot token
    # into the test environment.
    monkeypatch.setenv("GC_SLACK_ADAPTER_ENV", str(tmp_path / "no-adapter-env"))
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test-token")
    monkeypatch.setenv("GC_CITY_NAME", "test-city")
    monkeypatch.setenv("GC_CITY_PATH", str(tmp_path))


def _import_module(monkeypatch: pytest.MonkeyPatch, base_url: str):
    monkeypatch.setenv("SLACK_API_BASE_URL", base_url)
    for name in ("slack_chat_read", "slack_intake_common"):
        sys.modules.pop(name, None)
    import slack_chat_read  # type: ignore
    return slack_chat_read


def _msg(ts: str, user: str, text: str, **extra: Any) -> dict[str, Any]:
    return {"type": "message", "ts": ts, "user": user, "text": text, **extra}


def _static(payload: Any, status: int = 200) -> Responder:
    return lambda query, headers: (status, payload)


def _users_info(names: dict[str, str]) -> Responder:
    def responder(query: dict[str, str], headers: dict[str, str]) -> tuple[int, Any]:
        uid = query.get("user", "")
        if uid not in names:
            return 200, {"ok": False, "error": "user_not_found"}
        return 200, {"ok": True, "user": {"name": uid.lower(),
                                          "profile": {"display_name": names[uid]}}}
    return responder


# --- channel history ------------------------------------------------------

def test_history_renders_chronological_with_names(slack_api, monkeypatch, capsys):
    mod = _import_module(monkeypatch, slack_api.url)
    # Slack returns newest-first; the renderer must flip to oldest-first.
    slack_api.handlers["/conversations.history"] = _static({
        "ok": True,
        "messages": [
            _msg("200.0002", "U2", "second", reply_count=3),
            _msg("100.0001", "U1", "first\nsecond line"),
        ],
        "has_more": False,
    })
    slack_api.handlers["/users.info"] = _users_info({"U1": "Afik", "U2": "Milena"})

    assert mod.main(["--conversation-id", "C0TEST"]) == 0
    out = capsys.readouterr().out

    assert out.index("first") < out.index("second line") < out.index("Milena")
    assert "Afik (U1)" in out
    assert "  second line" in out  # multi-line text stays indented under its header
    # Thread hint points at the parent ts with a runnable command.
    assert "--thread-ts 200.0002" in out
    # The read carried the bot token.
    assert slack_api.calls("/conversations.history")[0]["auth"] == "Bearer xoxb-test-token"


def test_history_paginates_with_cursor(slack_api, monkeypatch, capsys):
    mod = _import_module(monkeypatch, slack_api.url)
    pages = [
        {"ok": True, "messages": [_msg("300.0", "U1", "newest")],
         "has_more": True, "response_metadata": {"next_cursor": "CUR1"}},
        {"ok": True, "messages": [_msg("200.0", "U1", "older")], "has_more": False},
    ]

    def history(query: dict[str, str], headers: dict[str, str]) -> tuple[int, Any]:
        return 200, (pages[1] if query.get("cursor") == "CUR1" else pages[0])

    slack_api.handlers["/conversations.history"] = history

    assert mod.main(["--conversation-id", "C0TEST", "--limit", "5",
                     "--no-names", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert [m["text"] for m in data["messages"]] == ["older", "newest"]
    assert data["has_more"] is False
    calls = slack_api.calls("/conversations.history")
    assert len(calls) == 2
    assert calls[1]["query"]["cursor"] == "CUR1"


def test_history_limit_stops_early_and_reports_more(slack_api, monkeypatch, capsys):
    mod = _import_module(monkeypatch, slack_api.url)
    slack_api.handlers["/conversations.history"] = _static({
        "ok": True,
        "messages": [_msg("300.0", "U1", "newest"), _msg("200.0", "U1", "older")],
        "has_more": True,
        "response_metadata": {"next_cursor": "CUR1"},
    })

    assert mod.main(["--conversation-id", "C0TEST", "--limit", "2",
                     "--no-names", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["count"] == 2
    assert data["has_more"] is True
    # limit satisfied on page one — no second fetch.
    assert len(slack_api.calls("/conversations.history")) == 1


def test_history_passes_window_bounds(slack_api, monkeypatch, capsys):
    mod = _import_module(monkeypatch, slack_api.url)
    slack_api.handlers["/conversations.history"] = _static(
        {"ok": True, "messages": [], "has_more": False})

    assert mod.main(["--conversation-id", "C0TEST", "--oldest", "100.0",
                     "--newest", "900.0", "--no-names", "--json"]) == 0
    query = slack_api.calls("/conversations.history")[0]["query"]
    assert query["oldest"] == "100.0"
    assert query["latest"] == "900.0"


# --- thread replies -------------------------------------------------------

def test_thread_mode_uses_replies_endpoint(slack_api, monkeypatch, capsys):
    mod = _import_module(monkeypatch, slack_api.url)
    slack_api.handlers["/conversations.replies"] = _static({
        "ok": True,
        "messages": [
            _msg("100.0", "U1", "parent", thread_ts="100.0", reply_count=1),
            _msg("150.0", "U2", "reply", thread_ts="100.0"),
        ],
        "has_more": False,
    })

    assert mod.main(["--conversation-id", "C0TEST", "--thread-ts", "100.0",
                     "--no-names", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["mode"] == "replies"
    assert data["thread_ts"] == "100.0"
    assert [m["text"] for m in data["messages"]] == ["parent", "reply"]
    query = slack_api.calls("/conversations.replies")[0]["query"]
    assert query["ts"] == "100.0"
    assert query["channel"] == "C0TEST"


# --- rate limits ----------------------------------------------------------

def test_rate_limited_request_retries_after_wait(slack_api, monkeypatch, capsys):
    mod = _import_module(monkeypatch, slack_api.url)
    sleeps: list[float] = []
    monkeypatch.setattr(mod, "_sleep", sleeps.append)
    state = {"n": 0}

    def history(query: dict[str, str], headers: dict[str, str]) -> tuple[int, Any]:
        state["n"] += 1
        if state["n"] == 1:
            return 429, {"ok": False, "error": "ratelimited"}
        return 200, {"ok": True, "messages": [_msg("1.0", "U1", "hi")], "has_more": False}

    slack_api.handlers["/conversations.history"] = history

    assert mod.main(["--conversation-id", "C0TEST", "--no-names", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["count"] == 1
    assert sleeps == [1.0]  # honored the mock's Retry-After: 1
    assert state["n"] == 2


def test_rate_limit_exhaustion_is_actionable(slack_api, monkeypatch):
    mod = _import_module(monkeypatch, slack_api.url)
    monkeypatch.setattr(mod, "_sleep", lambda s: None)
    slack_api.handlers["/conversations.history"] = _static(
        {"ok": False, "error": "ratelimited"}, status=429)

    with pytest.raises(SystemExit) as excinfo:
        mod.main(["--conversation-id", "C0TEST", "--no-names"])
    assert "rate limit" in str(excinfo.value)


# --- error mapping --------------------------------------------------------

@pytest.mark.parametrize("error,needle", [
    ("not_in_channel", "invite it"),
    ("channel_not_found", "not found"),
    ("missing_scope", "channels:history"),
    ("invalid_auth", "bot token"),
])
def test_slack_errors_map_to_clear_messages(slack_api, monkeypatch, error, needle):
    mod = _import_module(monkeypatch, slack_api.url)
    slack_api.handlers["/conversations.history"] = _static({"ok": False, "error": error})

    with pytest.raises(SystemExit) as excinfo:
        mod.main(["--conversation-id", "C0TEST", "--no-names"])
    assert needle in str(excinfo.value)
    assert "C0TEST" in str(excinfo.value) or error not in ("not_in_channel", "channel_not_found")


def test_missing_token_is_actionable(slack_api, monkeypatch):
    mod = _import_module(monkeypatch, slack_api.url)
    monkeypatch.delenv("SLACK_BOT_TOKEN")
    with pytest.raises(SystemExit) as excinfo:
        mod.main(["--conversation-id", "C0TEST"])
    assert "SLACK_BOT_TOKEN" in str(excinfo.value)


# --- attachments ----------------------------------------------------------

def _file_message(api_url: str) -> dict[str, Any]:
    return _msg("100.0", "U1", "see attached", files=[{
        "id": "F0AAA",
        "name": "trace.log",
        "mimetype": "text/plain",
        "size": 5,
        "url_private": f"{api_url}/files-pri/T0/F0AAA/trace.log",
    }])


def test_attachment_pointers_without_download(slack_api, monkeypatch, capsys):
    mod = _import_module(monkeypatch, slack_api.url)
    slack_api.handlers["/conversations.history"] = _static(
        {"ok": True, "messages": [_file_message(slack_api.url)], "has_more": False})

    assert mod.main(["--conversation-id", "C0TEST", "--no-names"]) == 0
    out = capsys.readouterr().out
    assert "[file] trace.log (text/plain, 5 bytes, id F0AAA)" in out
    assert "--download" in out
    assert not slack_api.calls("/files-pri/T0/F0AAA/trace.log")


def test_download_spools_to_inbound_store_layout(slack_api, monkeypatch, capsys, tmp_path):
    mod = _import_module(monkeypatch, slack_api.url)
    # The mock serves over http://127.0.0.1 which the Slack-host gate
    # rightly refuses; tests bypass the gate, not the transport.
    monkeypatch.setattr(mod, "_is_slack_file_url", lambda url: True)
    slack_api.handlers["/conversations.history"] = _static(
        {"ok": True, "messages": [_file_message(slack_api.url)], "has_more": False})
    slack_api.handlers["/files-pri/T0/F0AAA/trace.log"] = _static(b"bytes")
    store = tmp_path / "inbound"

    assert mod.main(["--conversation-id", "C0TEST", "--no-names", "--json",
                     "--download", "--download-dir", str(store)]) == 0
    dest = store / "C0TEST" / "100.0-trace.log"
    assert dest.read_bytes() == b"bytes"
    assert (dest.parent.stat().st_mode & 0o777) == 0o700
    data = json.loads(capsys.readouterr().out)
    assert data["messages"][0]["files"][0]["local_path"] == str(dest)
    # Download carried the bot token to the (mock) Slack CDN.
    assert slack_api.calls("/files-pri/T0/F0AAA/trace.log")[0]["auth"] == "Bearer xoxb-test-token"


def test_download_refuses_non_slack_hosts(slack_api, monkeypatch, capsys, tmp_path):
    mod = _import_module(monkeypatch, slack_api.url)
    slack_api.handlers["/conversations.history"] = _static(
        {"ok": True, "messages": [_file_message(slack_api.url)], "has_more": False})

    assert mod.main(["--conversation-id", "C0TEST", "--no-names", "--json",
                     "--download", "--download-dir", str(tmp_path / "inbound")]) == 0
    err = capsys.readouterr().err
    assert "refusing non-Slack file host" in err
    # Token never sent to the non-Slack host.
    assert not slack_api.calls("/files-pri/T0/F0AAA/trace.log")


def test_slack_file_url_gate():
    for name in ("slack_chat_read",):
        sys.modules.pop(name, None)
    import slack_chat_read as mod  # type: ignore

    assert mod._is_slack_file_url("https://files.slack.com/files-pri/T0/F0/x.png")
    assert mod._is_slack_file_url("https://slack-files.com/x")
    assert not mod._is_slack_file_url("http://files.slack.com/x")  # not https
    assert not mod._is_slack_file_url("https://files.slack.com:8443/x")  # odd port
    assert not mod._is_slack_file_url("https://evil.com/files.slack.com/x")
    assert not mod._is_slack_file_url("https://notslack.com/x")
    assert not mod._is_slack_file_url("//files.slack.com/x")  # protocol-relative


# --- adapter env loading --------------------------------------------------

def test_adapter_env_export_prefix_is_stripped(monkeypatch, tmp_path):
    """The live env file writes `export KEY=value`; the loader must strip
    the shell prefix or nothing loads (and bot-token reads break)."""
    env_file = tmp_path / "adapter-env"
    env_file.write_text(
        "# comment\n"
        "export SLACK_BOT_TOKEN='xoxb-from-file'\n"
        "SLACK_WORKSPACE_ID=T0PLAIN\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("GC_SLACK_ADAPTER_ENV", str(env_file))
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_WORKSPACE_ID", raising=False)
    for name in ("slack_chat_read", "slack_intake_common"):
        sys.modules.pop(name, None)
    import slack_intake_common  # noqa: F401  # type: ignore

    import os
    assert os.environ["SLACK_BOT_TOKEN"] == "xoxb-from-file"
    assert os.environ["SLACK_WORKSPACE_ID"] == "T0PLAIN"
