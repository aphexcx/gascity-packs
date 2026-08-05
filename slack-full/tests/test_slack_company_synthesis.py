"""Tests for the slack-pack company-rooms synthesis surface (company rooms 3c).

Covers the Python side of Phase 3: the synthesis group tuple (D1), the S10
snapshot normalizer, the ``dgroup``/``dgser`` lock-name parity pins, the D2
reply-current synthesis gate (ready / not-ready / --allow-partial /
unavailable), the S8 strict one-hop delegate gate, ``parse_delegation``
additive-field passthrough of the snapshot fields, and the pruner retention
floor (>= max record ttl + 1h clock-skew margin). Hermetic: the single
provider seam is monkeypatched and all state lives under ``tmp_path``.
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
from datetime import timedelta

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
    monkeypatch.setenv("GC_SLACK_ADAPTER_ENV", str(tmp_path / "no-such-env"))
    for var in (
        "SLACK_COMPANY_SECRETS_DIR", "SLACK_COMPANY_INTENTS_DIR",
        "SLACK_COMPANY_DELEGATIONS_DIR", "SLACK_COMPANY_TURNS_DIR",
        "SLACK_COMPANY_LOCKS_DIR", "SLACK_COMPANY_INGRESS_DIR",
        "SLACK_COMPANY_DIRECTORY_PATH", "SLACK_COMPANY_BINDINGS_PATH",
    ):
        monkeypatch.delenv(var, raising=False)


def _mod():
    sys.modules.pop("slack_company_outbound", None)
    sys.modules.pop("slack_company_directory", None)
    import slack_company_outbound  # type: ignore
    return slack_company_outbound


DIRECTORY = {
    "schema_version": 1,
    "agents": [
        {"name": "ollie", "app_id": "A0AAAAAA1", "bot_user_id": "U0AAAAAA1"},
        {"name": "riley", "app_id": "A0AAAAAA2", "bot_user_id": "U0AAAAAA2"},
    ],
    "rooms": [{
        "name": "orchestrator-team", "team_id": "T0AAAAAAA", "channel_id": "C0AAAAAAA",
        "members": ["ollie", "riley"], "ambient_wake": ["ollie"],
        "mention_wake": ["ollie", "riley"],
    }],
}
BINDINGS = {
    "schema_version": 1,
    "bindings": [
        {"room": "orchestrator-team", "agent": "ollie", "session": "ollie-main"},
        {"room": "orchestrator-team", "agent": "riley", "session": "riley-main"},
    ],
}


def _setup_company(mod, tmp_path: pathlib.Path) -> None:
    slackdir = tmp_path / ".gc" / "slack"
    slackdir.mkdir(parents=True, exist_ok=True)
    (slackdir / "company_directory.json").write_text(json.dumps(DIRECTORY))
    (slackdir / "company_bindings.json").write_text(json.dumps(BINDINGS))
    for agent in ("ollie", "riley"):
        sdir = mod.secrets_dir()
        sdir.mkdir(parents=True, exist_ok=True)
        os.chmod(sdir, 0o700)
        p = sdir / f"bot-token-{agent}.txt"
        p.write_text(f"xoxb-{agent}")
        os.chmod(p, 0o600)


def _write_turn(mod, session: str, **overrides) -> None:
    tdir = mod.turns_dir()
    tdir.mkdir(parents=True, exist_ok=True)
    turn = {
        "schema_version": 1, "session": session, "receipt_id": "in-x",
        "team_id": "T0AAAAAAA", "channel_id": "C0AAAAAAA", "ts": "1700000000.000700",
        "room": "orchestrator-team", "kind": "targeted",
        "thread_root_ts": "1700000000.000100", "agent": "ollie",
        "delegation_key": "", "delivered_at": "2026-07-17T12:00:00Z",
    }
    turn.update(overrides)
    (tdir / f"{session}.json").write_text(json.dumps(turn))


def _install_claimed(mod, fixture_name: str) -> str:
    """Copy a golden claimed record into the delegations dir under its own key.

    The pruner evicts terminal records keyed on ``result_claimed_at`` (falling
    back to ``created_at``); the golden fixtures freeze both to their authoring
    date, which ages past the retention floor. Re-stamp both to now so the
    record survives the prune inside post_peer_synthesis and reaches the gate.
    """
    data = json.loads((FIXTURES / fixture_name).read_text())
    _fresh = mod._rfc3339(mod._now())
    data["created_at"] = _fresh
    if data.get("result_claimed_at"):
        data["result_claimed_at"] = _fresh
    key = mod.delegation_filename(data["team_id"], data["channel_id"], data["ts"])
    ddir = mod.delegations_dir()
    ddir.mkdir(parents=True, exist_ok=True)
    (ddir / key).write_text(json.dumps(data))
    return key


def _capturing_post(captured: list, ts: str = "1700000000.000900"):
    def fn(method, token, payload, *, api_base, timeout):
        captured.append({"token": token, "payload": payload})
        return 200, {}, {"ok": True, "ts": ts}
    return fn


# --------------------------------------------------------------------------
# 1. Synthesis group (D1) — the 5-tuple or None.
# --------------------------------------------------------------------------

def test_synthesis_group_full_record() -> None:
    mod = _mod()
    record = json.loads((FIXTURES / "claimed_delegation_ready.json").read_text())
    assert mod.synthesis_group(record) == (
        "T0AAAAAAA", "C0AAAAAAA", "1700000000.000100", "U0AAAAAA1", "ollie-main")


@pytest.mark.parametrize("missing", [
    "team_id", "channel_id", "thread_root_ts",
    "requester_bot_user_id", "requester_session",
])
def test_synthesis_group_none_when_field_empty(missing: str) -> None:
    mod = _mod()
    record = json.loads((FIXTURES / "claimed_delegation_ready.json").read_text())
    record[missing] = ""
    assert mod.synthesis_group(record) is None


def test_synthesis_group_none_for_non_dict() -> None:
    mod = _mod()
    assert mod.synthesis_group("nope") is None  # type: ignore[arg-type]


# --------------------------------------------------------------------------
# 2. Lock-name parity pins (dgroup 5-field + fallback, dgser).
# --------------------------------------------------------------------------

def test_lock_name_parity_pins() -> None:
    mod = _mod()
    pins = json.loads((FIXTURES / "synthesis_locks.json").read_text())
    assert mod.dgroup_lock_name(*pins["dgroup"]["group_fields"]) == \
        pins["dgroup"]["expected_lock_name"]
    assert mod.dgroup_lock_name(*pins["dgroup_fallback"]["fallback_fields"]) == \
        pins["dgroup_fallback"]["expected_lock_name"]
    assert mod.dgser_lock_name(*pins["dgser"]["root_fields"]) == \
        pins["dgser"]["expected_lock_name"]
    # Never-taken parity helpers still produce the shared filename shape.
    assert pins["dgroup"]["expected_lock_name"].startswith("dgroup-")
    assert pins["dgroup"]["expected_lock_name"].endswith(".lock")
    assert pins["dgser"]["expected_lock_name"].startswith("dgser-")


# --------------------------------------------------------------------------
# 3. S10 snapshot normalizer — case-for-case table.
# --------------------------------------------------------------------------

def _available_snapshot() -> dict:
    return {
        "synthesis_state_version": 1,
        "synthesis_state_available": True,
        "compatible_delegation_count": 2,
        "responded_delegation_count": 1,
        "pending_delegation_count": 1,
        "pending_delegations": [{
            "delegation_ts": "1700000000.000200",
            "delegation_key": "dg-x.json",
            "expected_responder_agent": "seth",
            "expected_responder_bot_user_id": "U0AAAAAA3",
        }],
        "synthesis_ready": False,
        "synthesis_snapshot_at": "2026-07-17T12:00:07Z",
    }


def test_synthesis_state_available_preserves_and_recomputes() -> None:
    mod = _mod()
    state = mod.synthesis_state(_available_snapshot())
    assert state["synthesis_state_available"] is True
    assert state["synthesis_state_version"] == 1
    assert state["compatible_delegation_count"] == 2
    assert state["responded_delegation_count"] == 1
    assert state["pending_delegation_count"] == 1
    assert state["synthesis_ready"] is False
    assert state["pending_delegations"][0]["expected_responder_agent"] == "seth"


def test_synthesis_state_ready_true_when_all_responded() -> None:
    mod = _mod()
    snap = _available_snapshot()
    snap.update(compatible_delegation_count=1, responded_delegation_count=1,
                pending_delegation_count=0, pending_delegations=[],
                synthesis_ready=True)
    state = mod.synthesis_state(snap)
    assert state["synthesis_state_available"] is True and state["synthesis_ready"] is True


@pytest.mark.parametrize("mutate", [
    lambda s: s.update(synthesis_state_version=2),
    lambda s: s.update(synthesis_state_version=0),
    lambda s: s.update(synthesis_state_available=False),
    lambda s: s.update(responded_delegation_count=2),          # 2+1 != 2
    lambda s: s.update(compatible_delegation_count=0, responded_delegation_count=0,
                       pending_delegation_count=0, pending_delegations=[]),  # compatible 0
    lambda s: s.update(pending_delegation_count=2),            # list len != count
    lambda s: s.update(pending_delegations=[
        {"delegation_ts": "1700000000.000200"},
        {"delegation_ts": "1700000000.000200"}], pending_delegation_count=2,
        compatible_delegation_count=3, responded_delegation_count=1),  # dup ts
    lambda s: s.update(compatible_delegation_count="2"),       # non-int count
    lambda s: s.update(responded_delegation_count=True),       # bool is not int here
    lambda s: s.update(compatible_delegation_count=2.0),       # float count rejected
    lambda s: s.update(pending_delegation_count=True),         # bool count (True == 1)
    lambda s: s.update(synthesis_state_version=True),          # bool version (True == 1)
    lambda s: s.update(synthesis_state_version=1.0),           # float version
    lambda s: s.update(synthesis_state_version="1"),           # numeric-string version
    lambda s: s.update(synthesis_snapshot_at=""),              # empty snapshot_at
    lambda s: s.update(synthesis_ready=True),                  # stored_ready contradiction
    lambda s: s.update(pending_delegations=[{"delegation_ts": ""}]),  # empty ts
])
def test_synthesis_state_normalizes_to_unavailable(mutate) -> None:
    mod = _mod()
    snap = _available_snapshot()
    mutate(snap)
    state = mod.synthesis_state(snap)
    assert state == {
        "synthesis_state_version": 0,
        "synthesis_state_available": False,
        "compatible_delegation_count": 0,
        "responded_delegation_count": 0,
        "pending_delegation_count": 0,
        "pending_delegations": [],
        "synthesis_ready": False,
        "synthesis_snapshot_at": "",
    }


def test_synthesis_state_snapshot_at_not_trimmed() -> None:
    """Canonical rule: no string field is whitespace-trimmed. A padded
    ``synthesis_snapshot_at`` is non-empty, so the snapshot stays available and
    the field is preserved verbatim (Go, with its TrimSpace dropped, agrees)."""
    mod = _mod()
    snap = _available_snapshot()
    snap["synthesis_snapshot_at"] = "  2026-07-17T12:00:07Z  "
    state = mod.synthesis_state(snap)
    assert state["synthesis_state_available"] is True
    assert state["synthesis_snapshot_at"] == "  2026-07-17T12:00:07Z  "


def test_synthesis_state_padded_pending_ts_stay_distinct() -> None:
    """No trimming means a padded and an unpadded ``delegation_ts`` are distinct
    entries (not a collapsed duplicate), so the snapshot stays available."""
    mod = _mod()
    snap = _available_snapshot()
    snap.update(
        compatible_delegation_count=2,
        responded_delegation_count=0,
        pending_delegation_count=2,
        synthesis_ready=False,
        pending_delegations=[
            {"delegation_ts": "1700000000.000200"},
            {"delegation_ts": " 1700000000.000200"},
        ],
    )
    state = mod.synthesis_state(snap)
    assert state["synthesis_state_available"] is True
    assert [p["delegation_ts"] for p in state["pending_delegations"]] == [
        "1700000000.000200", " 1700000000.000200"]


def test_synthesis_state_of_fixtures() -> None:
    mod = _mod()
    ready = json.loads((FIXTURES / "claimed_delegation_ready.json").read_text())
    not_ready = json.loads((FIXTURES / "claimed_delegation_not_ready.json").read_text())
    invalid = json.loads((FIXTURES / "claimed_delegation_invalid_snapshot.json").read_text())
    assert mod.synthesis_state(ready)["synthesis_ready"] is True
    assert mod.synthesis_state(not_ready)["synthesis_state_available"] is True
    assert mod.synthesis_state(not_ready)["synthesis_ready"] is False
    assert mod.synthesis_state(invalid)["synthesis_state_available"] is False


# --------------------------------------------------------------------------
# 4. parse_delegation additive-field passthrough of the snapshot fields.
# --------------------------------------------------------------------------

def test_parse_delegation_passes_snapshot_fields_through() -> None:
    mod = _mod()
    raw = json.loads((FIXTURES / "claimed_delegation_not_ready.json").read_text())
    parsed = mod.parse_delegation(raw)
    for key in ("synthesis_state_version", "synthesis_state_available",
                "compatible_delegation_count", "responded_delegation_count",
                "pending_delegation_count", "pending_delegations",
                "synthesis_ready", "synthesis_snapshot_at"):
        assert parsed[key] == raw[key], key
    # Byte-stable under the Python writer (both suites re-derive the golden).
    assert mod._dumps(mod.parse_delegation(raw)) == \
        (FIXTURES / "claimed_delegation_not_ready.json").read_bytes()


# --------------------------------------------------------------------------
# 5. D2 reply-current synthesis gate.
# --------------------------------------------------------------------------

def _synthesis_turn(mod, key: str) -> None:
    _write_turn(mod, "ollie-main", kind="peer_result", agent="ollie",
                ts="1700000000.000700", delegation_key=key)


def test_gate_ready_posts(monkeypatch, tmp_path) -> None:
    mod = _mod()
    _setup_company(mod, tmp_path)
    monkeypatch.setattr(mod, "_sleep", lambda *_a, **_k: None)
    key = _install_claimed(mod, "claimed_delegation_ready.json")
    _synthesis_turn(mod, key)
    captured: list = []
    monkeypatch.setattr(mod, "_slack_web_post", _capturing_post(captured))
    result = mod.post_peer_synthesis(body="synthesis body", origin_ts="",
                                     session_name="ollie-main")
    assert result["status"] == "posted"
    assert "allow_partial" not in result
    assert len(captured) == 1
    assert "<@" not in captured[0]["payload"]["text"]


def test_gate_not_ready_exits_one_listing_pending(monkeypatch, tmp_path) -> None:
    mod = _mod()
    _setup_company(mod, tmp_path)
    monkeypatch.setattr(mod, "_sleep", lambda *_a, **_k: None)
    key = _install_claimed(mod, "claimed_delegation_not_ready.json")
    _synthesis_turn(mod, key)
    captured: list = []
    monkeypatch.setattr(mod, "_slack_web_post", _capturing_post(captured))
    with pytest.raises(mod.OutboundError) as exc:
        mod.post_peer_synthesis(body="early", origin_ts="", session_name="ollie-main")
    msg = str(exc.value)
    assert "not ready" in msg
    assert "seth" in msg and "1700000000.000200" in msg
    assert "--allow-partial" in msg and "--cancel" in msg
    assert captured == []  # nothing posted on a refused synthesis


def test_gate_allow_partial_posts_and_flags(monkeypatch, tmp_path) -> None:
    mod = _mod()
    _setup_company(mod, tmp_path)
    monkeypatch.setattr(mod, "_sleep", lambda *_a, **_k: None)
    key = _install_claimed(mod, "claimed_delegation_not_ready.json")
    _synthesis_turn(mod, key)
    captured: list = []
    monkeypatch.setattr(mod, "_slack_web_post", _capturing_post(captured))
    result = mod.post_peer_synthesis(body="partial", origin_ts="",
                                     session_name="ollie-main", allow_partial=True)
    assert result["status"] == "posted"
    assert result["allow_partial"] is True
    assert len(captured) == 1


def test_gate_unavailable_warns_and_posts(monkeypatch, tmp_path, capsys) -> None:
    mod = _mod()
    _setup_company(mod, tmp_path)
    monkeypatch.setattr(mod, "_sleep", lambda *_a, **_k: None)
    key = _install_claimed(mod, "claimed_delegation_invalid_snapshot.json")
    _synthesis_turn(mod, key)
    captured: list = []
    monkeypatch.setattr(mod, "_slack_web_post", _capturing_post(captured))
    result = mod.post_peer_synthesis(body="legacy", origin_ts="",
                                     session_name="ollie-main")
    assert result["status"] == "posted"
    assert "allow_partial" not in result
    assert len(captured) == 1
    assert "unavailable" in capsys.readouterr().err


def test_gate_missing_record_warns_and_posts(monkeypatch, tmp_path, capsys) -> None:
    """A pruned/absent claimed record normalizes to unavailable → warn + proceed."""
    mod = _mod()
    _setup_company(mod, tmp_path)
    monkeypatch.setattr(mod, "_sleep", lambda *_a, **_k: None)
    key = mod.delegation_filename("T0AAAAAAA", "C0AAAAAAA", "1700000000.000500")
    _synthesis_turn(mod, key)  # pointer references a record that does not exist
    captured: list = []
    monkeypatch.setattr(mod, "_slack_web_post", _capturing_post(captured))
    result = mod.post_peer_synthesis(body="x", origin_ts="", session_name="ollie-main")
    assert result["status"] == "posted"
    assert "unavailable" in capsys.readouterr().err
    assert len(captured) == 1


# --------------------------------------------------------------------------
# 6. S8 strict one-hop delegate gate.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("kind", ["peer_delegation", "peer_input", "peer_result"])
def test_delegate_rejected_from_peer_turn(monkeypatch, tmp_path, kind: str) -> None:
    mod = _mod()
    _setup_company(mod, tmp_path)
    monkeypatch.setattr(mod, "_sleep", lambda *_a, **_k: None)
    monkeypatch.setenv("GC_SESSION_NAME", "riley-main")
    delegation_key = ""
    if kind in ("peer_delegation", "peer_result"):
        delegation_key = _install_claimed(mod, "claimed_delegation_ready.json")
    _write_turn(mod, "riley-main", kind=kind, agent="riley",
                ts="1700000000.000700", delegation_key=delegation_key)
    posted: list = []
    monkeypatch.setattr(mod, "_slack_web_post", _capturing_post(posted))
    with pytest.raises(mod.OutboundError) as exc:
        mod.run_delegate(to="ollie", body="redelegate", origin_ts="",
                         session_name="riley-main")
    assert "one-hop" in str(exc.value)
    assert posted == []  # rejected before any post
    assert mod.list_intents() == []  # and before any intent was created


@pytest.mark.parametrize("kind", ["ambient", "targeted"])
def test_delegate_proceeds_from_human_rooted_turn(monkeypatch, tmp_path, kind: str) -> None:
    mod = _mod()
    _setup_company(mod, tmp_path)
    monkeypatch.setattr(mod, "_sleep", lambda *_a, **_k: None)
    monkeypatch.setenv("GC_SESSION_NAME", "ollie-main")
    _write_turn(mod, "ollie-main", kind=kind, agent="ollie", ts="1700000000.000100")
    monkeypatch.setattr(mod, "_slack_web_post", _capturing_post([], "1700000000.000500"))
    result = mod.run_delegate(to="riley", body="please review", origin_ts="",
                              session_name="ollie-main")
    assert result["status"] == "published"


# --------------------------------------------------------------------------
# 7. Pruner retention floor (>= max record ttl + 1h clock-skew margin).
# --------------------------------------------------------------------------

def test_prune_floor_constant_is_max_ttl_plus_margin() -> None:
    mod = _mod()
    assert mod.PRUNE_RETENTION_FLOOR_SECONDS == mod.INTENT_TTL_SECONDS + 3600
    assert mod.PRUNE_RETENTION_FLOOR_SECONDS > 24 * 3600  # strictly above the old floor


def _write_expired_delegation(mod, *, ts: str, nonce: str, created_at: str) -> None:
    record = {
        "schema_version": 1, "generation": 2, "nonce": nonce,
        "room": "orchestrator-team", "team_id": "T0AAAAAAA", "channel_id": "C0AAAAAAA",
        "ts": ts, "thread_root_ts": "1700000000.000100",
        "requester_agent": "ollie", "requester_bot_user_id": "U0AAAAAA1",
        "requester_session": "ollie-main",
        "expected_responder_agent": "riley", "expected_responder_bot_user_id": "U0AAAAAA2",
        "created_at": created_at, "ttl_seconds": 86400,
        "status": "expired", "result_ts": "", "result_claimed_at": "",
    }
    ddir = mod.delegations_dir()
    ddir.mkdir(parents=True, exist_ok=True)
    key = mod.delegation_filename("T0AAAAAAA", "C0AAAAAAA", ts)
    (ddir / key).write_text(json.dumps(record))


def test_prune_floor_keeps_record_below_floor(tmp_path) -> None:
    mod = _mod()
    _setup_company(mod, tmp_path)
    # 24.5h old: past the OLD 24h floor but under the 25h (ttl + 1h) floor.
    created = mod._rfc3339(mod._now() - timedelta(hours=24, minutes=30))
    _write_expired_delegation(mod, ts="1700000000.000400",
                              nonce="gcs-floorkeep00000000", created_at=created)
    removed = mod.prune(retention_seconds=0)
    assert removed["delegations"] == 0
    assert len(mod.list_delegations()) == 1


def test_prune_floor_prunes_record_past_floor(tmp_path) -> None:
    mod = _mod()
    _setup_company(mod, tmp_path)
    created = mod._rfc3339(mod._now() - timedelta(hours=26))
    _write_expired_delegation(mod, ts="1700000000.000400",
                              nonce="gcs-floorgone00000000", created_at=created)
    removed = mod.prune(retention_seconds=0)
    assert removed["delegations"] == 1
    assert mod.list_delegations() == []


def test_prune_floor_lifts_for_larger_ttl(tmp_path) -> None:
    mod = _mod()
    _setup_company(mod, tmp_path)
    # A record with a 40h ttl must survive at 40.5h old (floor lifts to 41h).
    record = {
        "schema_version": 1, "generation": 2, "nonce": "gcs-bigttl0000000000",
        "room": "orchestrator-team", "team_id": "T0AAAAAAA", "channel_id": "C0AAAAAAA",
        "ts": "1700000000.000400", "thread_root_ts": "1700000000.000100",
        "requester_agent": "ollie", "requester_bot_user_id": "U0AAAAAA1",
        "requester_session": "ollie-main",
        "expected_responder_agent": "riley", "expected_responder_bot_user_id": "U0AAAAAA2",
        "created_at": mod._rfc3339(mod._now() - timedelta(hours=40, minutes=30)),
        "ttl_seconds": 40 * 3600, "status": "expired",
        "result_ts": "", "result_claimed_at": "",
    }
    ddir = mod.delegations_dir()
    ddir.mkdir(parents=True, exist_ok=True)
    (ddir / mod.delegation_filename("T0AAAAAAA", "C0AAAAAAA", "1700000000.000400")).write_text(
        json.dumps(record))
    removed = mod.prune(retention_seconds=0)
    assert removed["delegations"] == 0


def test_prune_watermark_retained_but_older_seq_pruned_by_floor(tmp_path) -> None:
    """Two same-tuple terminal intents: the highest-seq watermark is retained
    regardless of age; a lower-seq intent past the floor is pruned."""
    mod = _mod()
    _setup_company(mod, tmp_path)
    common = dict(
        source_agent="ollie", source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1",
        target_agent="riley", target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA",
        channel_id="C0AAAAAAA", room="orchestrator-team",
        human_root_ts="1700000000.000100", requester_session="ollie-main",
        body_hex="55" * 32)
    old = mod.build_intent(nonce="gcs-seq0000000000000", retry_seq=0, **common)
    old["status"] = "failed"
    old["updated_at"] = mod._rfc3339(mod._now() - timedelta(hours=26))
    mod.write_intent(old)
    new = mod.build_intent(nonce="gcs-seq1111111111111", retry_seq=1, **common)
    new["status"] = "failed"
    new["updated_at"] = mod._rfc3339(mod._now() - timedelta(hours=26))
    mod.write_intent(new)

    removed = mod.prune(retention_seconds=0)
    assert removed["intents"] == 1
    remaining = {i["nonce"] for i in mod.list_intents()}
    assert remaining == {"gcs-seq1111111111111"}  # the watermark survives
