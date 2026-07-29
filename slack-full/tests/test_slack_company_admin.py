"""Tests for the slack-pack company-rooms operator verbs (company rooms 3c).

Hermetic: the internal-listener HTTP layer is mocked (the adapter endpoints do
not exist until 3b), and Python-owned state lives under ``tmp_path``. Covers the
UDS/TCP connection factory, ``company-status`` rendering from mixed on-disk +
endpoint state, and the ``company-redrive`` client (targets/include_failed
passthrough, 404 surfacing, 409 single-flight retry, 422 empty-selection
reason, and 2xx per-target unresolvable warnings).
"""

from __future__ import annotations

import json
import pathlib
import sys

import pytest

PACK_DIR = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PACK_DIR / "scripts"
FIXTURES = PACK_DIR / "tests" / "fixtures" / "company"
sys.path.insert(0, str(SCRIPTS_DIR))


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.setenv("GC_CITY_NAME", "test-city")
    monkeypatch.setenv("GC_CITY_PATH", str(tmp_path))
    monkeypatch.setenv("SLACK_WORKSPACE_ID", "T0AAAAAAA")
    for var in (
        "SLACK_COMPANY_SECRETS_DIR", "SLACK_COMPANY_INTENTS_DIR",
        "SLACK_COMPANY_DELEGATIONS_DIR", "SLACK_COMPANY_TURNS_DIR",
        "SLACK_COMPANY_LOCKS_DIR", "SLACK_COMPANY_INGRESS_DIR",
        "GC_SERVICE_SOCKET", "LISTEN_INTERNAL",
    ):
        monkeypatch.delenv(var, raising=False)


def _mod():
    sys.modules.pop("slack_company_admin", None)
    sys.modules.pop("slack_company_outbound", None)
    import slack_company_admin  # type: ignore
    return slack_company_admin


# --- fake internal-listener connection -------------------------------------

class _FakeResp:
    def __init__(self, status: int, body) -> None:
        self.status = status
        self._body = body

    def read(self) -> bytes:
        if self._body is None:
            return b""
        return json.dumps(self._body).encode("utf-8")


class _FakeConn:
    """Records requests and replays a shared queue of (status, body) responses."""

    def __init__(self, responses: list, calls: list) -> None:
        self._responses = responses
        self._calls = calls

    def request(self, method, url, body=None, headers=None) -> None:
        self._calls.append({
            "method": method, "url": url,
            "body": json.loads(body) if body else None,
            "headers": headers or {},
        })

    def getresponse(self) -> _FakeResp:
        status, body = self._responses.pop(0)
        return _FakeResp(status, body)

    def close(self) -> None:
        pass


def _mock_conn(mod, monkeypatch, responses: list) -> list:
    calls: list = []
    # One shared response queue across connections: each _internal_request opens
    # a fresh connection, so retries must advance the SAME queue.
    queue = list(responses)
    monkeypatch.setattr(mod, "internal_connection",
                        lambda *a, **k: _FakeConn(queue, calls))
    return calls


# --------------------------------------------------------------------------
# 1. Connection factory — UDS vs TCP construction (GC_SERVICE_SOCKET wins).
# --------------------------------------------------------------------------

def test_internal_connection_uses_uds_when_socket_set(monkeypatch) -> None:
    mod = _mod()
    monkeypatch.setenv("GC_SERVICE_SOCKET", "/run/gc/slack.sock")
    conn = mod.internal_connection()
    assert isinstance(conn, mod._UnixHTTPConnection)
    assert conn._unix_socket_path == "/run/gc/slack.sock"


def test_internal_connection_uses_tcp_listen_internal(monkeypatch) -> None:
    mod = _mod()
    monkeypatch.setenv("LISTEN_INTERNAL", "127.0.0.1:9999")
    conn = mod.internal_connection()
    assert not isinstance(conn, mod._UnixHTTPConnection)
    assert conn.host == "127.0.0.1" and conn.port == 9999


def test_internal_connection_default_tcp(monkeypatch) -> None:
    mod = _mod()
    conn = mod.internal_connection()
    assert conn.host == "127.0.0.1" and conn.port == 8766


def test_internal_connection_socket_wins_over_tcp(monkeypatch) -> None:
    mod = _mod()
    monkeypatch.setenv("LISTEN_INTERNAL", "127.0.0.1:9999")
    monkeypatch.setenv("GC_SERVICE_SOCKET", "/run/gc/slack.sock")
    conn = mod.internal_connection()
    assert isinstance(conn, mod._UnixHTTPConnection)


# --------------------------------------------------------------------------
# 2. company-status — rendering from mixed on-disk + endpoint state.
# --------------------------------------------------------------------------

def _install_claimed(mod, fixture_name: str) -> str:
    # Prune evicts terminal records keyed on result_claimed_at (fallback
    # created_at); the golden fixtures freeze both to their authoring date,
    # which ages past the retention floor. Re-stamp both to now.
    data = json.loads((FIXTURES / fixture_name).read_text())
    _fresh = mod.outbound._rfc3339(mod.outbound._now())
    data["created_at"] = _fresh
    if data.get("result_claimed_at"):
        data["result_claimed_at"] = _fresh
    key = mod.outbound.delegation_filename(data["team_id"], data["channel_id"], data["ts"])
    ddir = mod.outbound.delegations_dir()
    ddir.mkdir(parents=True, exist_ok=True)
    (ddir / key).write_text(json.dumps(data))
    return key


def _write_stale_posting_intent(mod) -> None:
    ob = mod.outbound
    intent = ob.build_intent(
        nonce="gcs-stale00000000000", retry_seq=0, source_agent="ollie",
        source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1", target_agent="riley",
        target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA", channel_id="C0AAAAAAA",
        room="orchestrator-team", human_root_ts="1700000000.000100",
        requester_session="ollie-main", body_hex="66" * 32)
    intent["status"] = "posting"
    from datetime import timedelta
    intent["retry_deadline"] = ob._rfc3339(ob._now() - timedelta(minutes=10))
    ob.write_intent(intent)


def test_company_status_renders_groups_snapshots_stale_and_receipts(
        monkeypatch, capsys) -> None:
    mod = _mod()
    _install_claimed(mod, "claimed_delegation_not_ready.json")
    _write_stale_posting_intent(mod)
    receipts_body = {"receipts": [{
        "id": "in-abc", "status": "failed", "reason": "correlation_pending",
        "targets": [{"session": "ollie-main", "kind": "peer_result",
                     "status": "failed", "attempts": 3, "detail": "attempts_exhausted"}],
        "ack_state": "eyes",
    }]}
    calls = _mock_conn(mod, monkeypatch, [(200, receipts_body)])

    rc = mod.cmd_company_status([])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)

    assert len(out["groups"]) == 1
    grp = out["groups"][0]
    assert grp["group"] == [
        "T0AAAAAAA", "C0AAAAAAA", "1700000000.000100", "U0AAAAAA1", "ollie-main"]
    snap = grp["delegations"][0]["synthesis"]
    assert snap["synthesis_state_available"] is True
    assert snap["synthesis_ready"] is False
    assert snap["compatible_delegation_count"] == 2

    assert len(out["stale_posting_intents"]) == 1
    assert out["stale_posting_intents"][0]["nonce"] == "gcs-stale00000000000"

    assert out["receipts"][0]["id"] == "in-abc"
    # The Python-owned reads and the endpoint read are one command.
    assert calls[0]["method"] == "GET"
    assert calls[0]["url"].startswith("/internal/company/receipts")


def test_company_status_scopes_receipt_query_with_origin(monkeypatch, capsys) -> None:
    mod = _mod()
    calls = _mock_conn(mod, monkeypatch, [(200, {"receipts": []})])
    rc = mod.cmd_company_status(["--origin", "T0AAAAAAA:C0AAAAAAA:1700000000.000500"])
    assert rc == 0
    assert "origin=T0AAAAAAA" in calls[0]["url"]


def test_company_status_filters_by_receipt_id(monkeypatch, capsys) -> None:
    mod = _mod()
    body = {"receipts": [{"id": "in-abc"}, {"id": "in-def"}]}
    _mock_conn(mod, monkeypatch, [(200, body)])
    rc = mod.cmd_company_status(["--receipt", "in-def"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert [r["id"] for r in out["receipts"]] == ["in-def"]


def test_company_status_surfaces_endpoint_unreachable(monkeypatch, capsys) -> None:
    mod = _mod()

    class _BoomConn:
        def request(self, *a, **k):
            raise ConnectionRefusedError("connection refused")

        def getresponse(self):  # pragma: no cover - never reached
            raise AssertionError

        def close(self):
            pass

    monkeypatch.setattr(mod, "internal_connection", lambda *a, **k: _BoomConn())
    rc = mod.cmd_company_status([])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["receipts"] == []
    assert "unreachable" in out["receipts_error"]


# --------------------------------------------------------------------------
# 3. company-redrive — client behavior.
# --------------------------------------------------------------------------

def test_redrive_passes_targets_and_include_failed(monkeypatch, capsys) -> None:
    mod = _mod()
    calls = _mock_conn(mod, monkeypatch, [(200, {"redriven": ["ollie-main"]})])
    rc = mod.cmd_company_redrive([
        "--origin", "T0AAAAAAA:C0AAAAAAA:1700000000.000500",
        "--target", "ollie-main", "--target", "seth-main", "--include-failed"])
    assert rc == 0
    assert calls[0]["method"] == "POST"
    assert calls[0]["url"] == "/internal/company/redrive"
    body = calls[0]["body"]
    assert body["targets"] == ["ollie-main", "seth-main"]
    assert body["include_failed"] is True
    assert body["origin"] == {"team_id": "T0AAAAAAA", "channel_id": "C0AAAAAAA",
                              "ts": "1700000000.000500"}


def test_redrive_by_receipt_id(monkeypatch, capsys) -> None:
    mod = _mod()
    calls = _mock_conn(mod, monkeypatch, [(200, {"ok": True})])
    rc = mod.cmd_company_redrive(["--receipt", "in-abc"])
    assert rc == 0
    body = calls[0]["body"]
    assert body["receipt"] == "in-abc"
    assert body["targets"] == [] and body["include_failed"] is False
    assert "origin" not in body


def test_redrive_requires_a_selector(monkeypatch) -> None:
    mod = _mod()
    _mock_conn(mod, monkeypatch, [])
    with pytest.raises(SystemExit):  # argparse mutually-exclusive required group
        mod.cmd_company_redrive(["--target", "ollie-main"])


def test_redrive_404_surfaced(monkeypatch, capsys) -> None:
    mod = _mod()
    _mock_conn(mod, monkeypatch, [(404, {"error": "gone"})])
    rc = mod.cmd_company_redrive(["--receipt", "in-old"])
    assert rc == 1
    assert "gone" in capsys.readouterr().err.lower() or "swept" in capsys.readouterr().err


def test_redrive_409_retries_then_succeeds(monkeypatch, capsys) -> None:
    mod = _mod()
    monkeypatch.setattr(mod, "_sleep", lambda *_a, **_k: None)
    calls = _mock_conn(mod, monkeypatch,
                       [(409, None), (409, None), (200, {"ok": True})])
    rc = mod.cmd_company_redrive(["--receipt", "in-abc"])
    assert rc == 0
    assert len(calls) == 3  # two 409s then the successful retry


def test_redrive_409_exhausts_retries(monkeypatch, capsys) -> None:
    mod = _mod()
    monkeypatch.setattr(mod, "_sleep", lambda *_a, **_k: None)
    responses = [(409, None)] * (mod._REDRIVE_MAX_RETRIES + 1)
    calls = _mock_conn(mod, monkeypatch, responses)
    rc = mod.cmd_company_redrive(["--receipt", "in-abc"])
    assert rc == 1
    assert len(calls) == mod._REDRIVE_MAX_RETRIES + 1
    assert "409" in capsys.readouterr().err


def test_redrive_422_empty_selection_surfaced(monkeypatch, capsys) -> None:
    """Empty effective selection: the endpoint returns 422 with a machine-
    readable reason instead of a success-shaped no-op — surface it, exit 1."""
    mod = _mod()
    _mock_conn(mod, monkeypatch, [(422, {
        "error": "no eligible targets to redrive", "reason": "empty_selection"})])
    rc = mod.cmd_company_redrive(["--receipt", "in-abc"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "empty_selection" in err


def test_redrive_200_warns_on_unresolvable_targets(monkeypatch, capsys) -> None:
    """Mixed reset + unresolvable: the reset lands (exit 0), but still-unbound
    targets are surfaced per-target as a warning on stderr."""
    mod = _mod()
    body = {
        "receipt": "in-abc", "leg": "targets", "status": "routing",
        "reset_targets": ["ollie-main"],
        "unresolvable": [
            {"agent": "riley", "reason": "no company binding for riley"}],
    }
    _mock_conn(mod, monkeypatch, [(200, body)])
    rc = mod.cmd_company_redrive(["--receipt", "in-abc", "--include-failed"])
    assert rc == 0  # partial success: the bound reset applied
    cap = capsys.readouterr()
    out = json.loads(cap.out)
    assert out["reset_targets"] == ["ollie-main"]
    assert "riley" in cap.err
    assert "unresolvable" in cap.err.lower()


# --------------------------------------------------------------------------
# 4. company-redact — client behavior.
# --------------------------------------------------------------------------

def test_redact_by_receipt_id(monkeypatch, capsys) -> None:
    mod = _mod()
    calls = _mock_conn(mod, monkeypatch, [(200, {"receipt": "in-abc", "redacted": True})])
    rc = mod.cmd_company_redact(["--receipt", "in-abc"])
    assert rc == 0
    assert calls[0]["method"] == "POST"
    assert calls[0]["url"] == "/internal/company/redact"
    assert calls[0]["body"] == {"receipt": "in-abc"}
    out = json.loads(capsys.readouterr().out)
    assert out["redacted"] is True


def test_redact_by_origin(monkeypatch, capsys) -> None:
    mod = _mod()
    calls = _mock_conn(mod, monkeypatch, [(200, {"redacted": True})])
    rc = mod.cmd_company_redact(["--origin", "T0AAAAAAA:C0AAAAAAA:1700000000.000500"])
    assert rc == 0
    assert calls[0]["body"]["origin"] == {
        "team_id": "T0AAAAAAA", "channel_id": "C0AAAAAAA", "ts": "1700000000.000500"}


def test_redact_requires_a_selector(monkeypatch) -> None:
    mod = _mod()
    _mock_conn(mod, monkeypatch, [])
    with pytest.raises(SystemExit):  # argparse mutually-exclusive required group
        mod.cmd_company_redact([])


def test_redact_404_surfaced(monkeypatch, capsys) -> None:
    mod = _mod()
    _mock_conn(mod, monkeypatch, [(404, {"error": "gone"})])
    rc = mod.cmd_company_redact(["--receipt", "in-old"])
    assert rc == 1
    assert "swept" in capsys.readouterr().err


def test_redact_409_single_flight_retries_then_succeeds(monkeypatch, capsys) -> None:
    mod = _mod()
    monkeypatch.setattr(mod, "_sleep", lambda *_a, **_k: None)
    calls = _mock_conn(mod, monkeypatch, [
        (409, {"error": "receipt single-flight held elsewhere"}),
        (200, {"redacted": True})])
    rc = mod.cmd_company_redact(["--receipt", "in-abc"])
    assert rc == 0
    assert len(calls) == 2  # one held 409 then the successful retry


def test_redact_409_legacy_embedded_no_retry(monkeypatch, capsys) -> None:
    """A legacy embedded receipt (no separable body) is a non-retryable 409."""
    mod = _mod()
    monkeypatch.setattr(mod, "_sleep", lambda *_a, **_k: None)
    calls = _mock_conn(mod, monkeypatch, [
        (409, {"error": "receipt in-abc is a legacy embedded receipt with no separable body to redact"})])
    rc = mod.cmd_company_redact(["--receipt", "in-abc"])
    assert rc == 1
    assert len(calls) == 1  # no retry on the legacy case
    assert "legacy" in capsys.readouterr().err
