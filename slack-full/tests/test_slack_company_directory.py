"""Tests for the slack-pack company-directory CLI surface (company rooms 1e).

Covers the full validation table from the "Normalized registries" spec
(positive and negative), TOML->JSON normalization, idempotent re-import,
the invalid-input-leaves-registry-untouched guarantee, bind create/replace
semantics, and `peers` output including membership warnings. All Slack API
access is mocked (monkeypatched ``_slack_api_call``); the suite is fully
hermetic — no network and no token.
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import sys
import tomllib

import pytest

PACK_DIR = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PACK_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.setenv("GC_CITY_NAME", "test-city")
    monkeypatch.setenv("GC_CITY_PATH", str(tmp_path))
    # Never touch a real adapter env / token; membership defaults to skipped.
    monkeypatch.setenv("GC_SLACK_ADAPTER_ENV", str(tmp_path / "no-such-env"))
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_COMPANY_DIRECTORY_PATH", raising=False)
    monkeypatch.delenv("SLACK_COMPANY_BINDINGS_PATH", raising=False)
    monkeypatch.setenv("SLACK_API_BASE_URL", "http://127.0.0.1:1/never")


def _mod():
    if "slack_company_directory" in sys.modules:
        del sys.modules["slack_company_directory"]
    import slack_company_directory  # type: ignore

    return slack_company_directory


VALID_TOML = """\
schema_version = 1

[[agents]]
name = "ollie"
app_id = "A0AAAAAA1"
bot_user_id = "U0AAAAAA1"

[[agents]]
name = "riley"
app_id = "A0AAAAAA2"
bot_user_id = "U0AAAAAA2"

[[rooms]]
name = "orchestrator-team"
team_id = "T0AAAAAAA"
channel_id = "C0AAAAAAA"
members = ["*"]
ambient_wake = ["ollie"]
mention_wake = ["*"]
"""


def _normalize(toml_str: str):
    mod = _mod()
    return mod.normalize_directory(tomllib.loads(toml_str))


# --------------------------------------------------------------------------
# Validation table — every rule, positive and negative.
# --------------------------------------------------------------------------

# Each entry: (id, toml, expected_error_substring | None-for-valid)
_CASES = [
    (
        "valid_baseline",
        VALID_TOML,
        None,
    ),
    (
        "empty_agents_and_rooms_valid",
        "schema_version = 1\n",
        None,
    ),
    (
        "uppercase_agent_name_rejected",
        """
schema_version = 1
[[agents]]
name = "Ollie"
app_id = "A1"
bot_user_id = "U1"
""",
        "lowercase slug",
    ),
    (
        "uppercase_room_name_rejected",
        """
schema_version = 1
[[rooms]]
name = "Room-One"
team_id = "T1"
channel_id = "C1"
""",
        "lowercase slug",
    ),
    # Slug-grammar parity with the Go loader's companySlugRE
    # (^[a-z0-9]+(?:-[a-z0-9]+)*$): underscores, trailing hyphens, and
    # double hyphens are rejected on BOTH agent and room names, so the CLI
    # can never write a name the adapter would reject (which silently
    # disables all company routing).
    (
        "underscore_agent_name_rejected",
        'schema_version = 1\n[[agents]]\nname = "data_bot"\napp_id = "A1"\nbot_user_id = "U1"\n',
        "lowercase slug",
    ),
    (
        "trailing_hyphen_agent_name_rejected",
        'schema_version = 1\n[[agents]]\nname = "room-"\napp_id = "A1"\nbot_user_id = "U1"\n',
        "lowercase slug",
    ),
    (
        "double_hyphen_agent_name_rejected",
        'schema_version = 1\n[[agents]]\nname = "a--b"\napp_id = "A1"\nbot_user_id = "U1"\n',
        "lowercase slug",
    ),
    (
        "underscore_room_name_rejected",
        'schema_version = 1\n[[rooms]]\nname = "data_bot"\nteam_id = "T1"\nchannel_id = "C1"\n',
        "lowercase slug",
    ),
    (
        "trailing_hyphen_room_name_rejected",
        'schema_version = 1\n[[rooms]]\nname = "room-"\nteam_id = "T1"\nchannel_id = "C1"\n',
        "lowercase slug",
    ),
    (
        "double_hyphen_room_name_rejected",
        'schema_version = 1\n[[rooms]]\nname = "a--b"\nteam_id = "T1"\nchannel_id = "C1"\n',
        "lowercase slug",
    ),
    (
        "duplicate_agent_name",
        """
schema_version = 1
[[agents]]
name = "ollie"
app_id = "A1"
bot_user_id = "U1"
[[agents]]
name = "ollie"
app_id = "A2"
bot_user_id = "U2"
""",
        "duplicate agent name",
    ),
    (
        "duplicate_app_id",
        """
schema_version = 1
[[agents]]
name = "ollie"
app_id = "A1"
bot_user_id = "U1"
[[agents]]
name = "riley"
app_id = "A1"
bot_user_id = "U2"
""",
        "duplicate app_id",
    ),
    (
        "duplicate_bot_user_id",
        """
schema_version = 1
[[agents]]
name = "ollie"
app_id = "A1"
bot_user_id = "U1"
[[agents]]
name = "riley"
app_id = "A2"
bot_user_id = "U1"
""",
        "duplicate bot_user_id",
    ),
    (
        "duplicate_room_name",
        """
schema_version = 1
[[rooms]]
name = "team"
team_id = "T1"
channel_id = "C1"
[[rooms]]
name = "team"
team_id = "T1"
channel_id = "C2"
""",
        "duplicate room name",
    ),
    (
        "duplicate_team_channel_pair",
        """
schema_version = 1
[[rooms]]
name = "team-a"
team_id = "T1"
channel_id = "C1"
[[rooms]]
name = "team-b"
team_id = "T1"
channel_id = "C1"
""",
        "duplicate (team_id, channel_id)",
    ),
    (
        "ambient_wake_not_subset_of_members",
        """
schema_version = 1
[[agents]]
name = "ollie"
app_id = "A1"
bot_user_id = "U1"
[[agents]]
name = "riley"
app_id = "A2"
bot_user_id = "U2"
[[rooms]]
name = "team"
team_id = "T1"
channel_id = "C1"
members = ["ollie"]
ambient_wake = ["riley"]
""",
        "not a room member",
    ),
    (
        "mention_wake_not_subset_of_members",
        """
schema_version = 1
[[agents]]
name = "ollie"
app_id = "A1"
bot_user_id = "U1"
[[agents]]
name = "riley"
app_id = "A2"
bot_user_id = "U2"
[[rooms]]
name = "team"
team_id = "T1"
channel_id = "C1"
members = ["ollie"]
mention_wake = ["riley"]
""",
        "not a room member",
    ),
    (
        "wildcard_in_ambient_wake_rejected",
        """
schema_version = 1
[[agents]]
name = "ollie"
app_id = "A1"
bot_user_id = "U1"
[[rooms]]
name = "team"
team_id = "T1"
channel_id = "C1"
members = ["*"]
ambient_wake = ["*"]
""",
        "may not contain the '*' wildcard",
    ),
    (
        "wildcard_in_members_ok",
        """
schema_version = 1
[[agents]]
name = "ollie"
app_id = "A1"
bot_user_id = "U1"
[[rooms]]
name = "team"
team_id = "T1"
channel_id = "C1"
members = ["*"]
""",
        None,
    ),
    (
        "wildcard_in_mention_wake_ok",
        """
schema_version = 1
[[agents]]
name = "ollie"
app_id = "A1"
bot_user_id = "U1"
[[rooms]]
name = "team"
team_id = "T1"
channel_id = "C1"
members = ["ollie"]
mention_wake = ["*"]
""",
        None,
    ),
    (
        "unknown_agent_ref_in_members",
        """
schema_version = 1
[[rooms]]
name = "team"
team_id = "T1"
channel_id = "C1"
members = ["ghost"]
""",
        "unknown agent",
    ),
    (
        "unknown_agent_ref_in_ambient_wake",
        """
schema_version = 1
[[agents]]
name = "ollie"
app_id = "A1"
bot_user_id = "U1"
[[rooms]]
name = "team"
team_id = "T1"
channel_id = "C1"
members = ["ollie"]
ambient_wake = ["ghost"]
""",
        "unknown agent",
    ),
    (
        "unknown_agent_ref_in_mention_wake",
        """
schema_version = 1
[[agents]]
name = "ollie"
app_id = "A1"
bot_user_id = "U1"
[[rooms]]
name = "team"
team_id = "T1"
channel_id = "C1"
members = ["ollie"]
mention_wake = ["ghost"]
""",
        "unknown agent",
    ),
    (
        "wrong_schema_version",
        "schema_version = 2\n",
        "schema_version must be 1",
    ),
    (
        "missing_schema_version",
        '[[agents]]\nname = "ollie"\napp_id = "A1"\nbot_user_id = "U1"\n',
        "schema_version must be 1",
    ),
    (
        "unknown_top_level_key",
        'schema_version = 1\nbogus = true\n',
        "unknown key",
    ),
    (
        "unknown_room_key_typo",
        """
schema_version = 1
[[agents]]
name = "ollie"
app_id = "A1"
bot_user_id = "U1"
[[rooms]]
name = "team"
team_id = "T1"
channel_id = "C1"
menbers = ["ollie"]
""",
        "unknown key",
    ),
    (
        "missing_agent_app_id",
        'schema_version = 1\n[[agents]]\nname = "ollie"\nbot_user_id = "U1"\n',
        "app_id must be a non-empty string",
    ),
]


@pytest.mark.parametrize("case_id,toml_str,err", _CASES, ids=[c[0] for c in _CASES])
def test_validation_table(case_id: str, toml_str: str, err: str | None):
    mod = _mod()
    if err is None:
        # Must normalize without raising.
        mod.normalize_directory(tomllib.loads(toml_str))
    else:
        with pytest.raises(mod.DirectoryError) as exc:
            mod.normalize_directory(tomllib.loads(toml_str))
        assert err in str(exc.value)


# --------------------------------------------------------------------------
# Wildcard expansion.
# --------------------------------------------------------------------------

def test_wildcard_members_expand_to_all_agents_in_order():
    body = _normalize(VALID_TOML)
    room = body["rooms"][0]
    assert room["members"] == ["ollie", "riley"]


def test_wildcard_mention_wake_expands_to_all_members():
    body = _normalize(VALID_TOML)
    room = body["rooms"][0]
    assert room["mention_wake"] == ["ollie", "riley"]
    assert room["ambient_wake"] == ["ollie"]


def test_no_wildcards_survive_into_normalized_json():
    body = _normalize(VALID_TOML)
    blob = json.dumps(body)
    assert "*" not in blob


# --------------------------------------------------------------------------
# TOML -> JSON golden comparison + import.
# --------------------------------------------------------------------------

_GOLDEN_AGENTS = [
    {"name": "ollie", "app_id": "A0AAAAAA1", "bot_user_id": "U0AAAAAA1"},
    {"name": "riley", "app_id": "A0AAAAAA2", "bot_user_id": "U0AAAAAA2"},
]
_GOLDEN_ROOMS = [
    {
        "name": "orchestrator-team",
        "team_id": "T0AAAAAAA",
        "channel_id": "C0AAAAAAA",
        "members": ["ollie", "riley"],
        "ambient_wake": ["ollie"],
        "mention_wake": ["ollie", "riley"],
    },
]


def _write_toml(tmp_path: pathlib.Path, content: str) -> pathlib.Path:
    src = tmp_path / "rooms.toml"
    src.write_text(content, encoding="utf-8")
    return src


def test_import_writes_golden_normalized_json(tmp_path: pathlib.Path, capsys):
    mod = _mod()
    src = _write_toml(tmp_path, VALID_TOML)
    rc = mod.main(["import-company-directory", "--file", str(src)])
    assert rc == 0

    dest = mod.company_directory_path()
    written = json.loads(dest.read_text())
    assert written["schema_version"] == 1
    assert written["agents"] == _GOLDEN_AGENTS
    assert written["rooms"] == _GOLDEN_ROOMS
    assert written["source_sha256"] == hashlib.sha256(VALID_TOML.encode("utf-8")).hexdigest()
    # RFC3339 UTC timestamp shape.
    assert written["imported_at"].endswith("Z")
    assert written["imported_at"][4] == "-" and written["imported_at"][10] == "T"

    out = json.loads(capsys.readouterr().out)
    assert out["rooms"] == ["orchestrator-team"]
    assert out["agents"] == 2


def test_import_registry_path_honors_env_override(tmp_path: pathlib.Path, monkeypatch):
    mod = _mod()
    override = tmp_path / "custom" / "dir.json"
    monkeypatch.setenv("SLACK_COMPANY_DIRECTORY_PATH", str(override))
    src = _write_toml(tmp_path, VALID_TOML)
    assert mod.main(["import-company-directory", "--file", str(src)]) == 0
    assert override.exists()
    assert json.loads(override.read_text())["agents"] == _GOLDEN_AGENTS


def test_import_is_idempotent(tmp_path: pathlib.Path):
    mod = _mod()
    src = _write_toml(tmp_path, VALID_TOML)
    assert mod.main(["import-company-directory", "--file", str(src)]) == 0
    first = json.loads(mod.company_directory_path().read_text())
    assert mod.main(["import-company-directory", "--file", str(src)]) == 0
    second = json.loads(mod.company_directory_path().read_text())
    assert first["agents"] == second["agents"]
    assert first["rooms"] == second["rooms"]
    assert first["source_sha256"] == second["source_sha256"]


def test_invalid_import_leaves_existing_registry_untouched(tmp_path: pathlib.Path):
    mod = _mod()
    good = _write_toml(tmp_path, VALID_TOML)
    assert mod.main(["import-company-directory", "--file", str(good)]) == 0
    dest = mod.company_directory_path()
    before = dest.read_bytes()

    bad = tmp_path / "bad.toml"
    bad.write_text(
        'schema_version = 1\n[[agents]]\nname = "ollie"\napp_id = "A1"\nbot_user_id = "U1"\n'
        '[[agents]]\nname = "ollie"\napp_id = "A2"\nbot_user_id = "U2"\n',
        encoding="utf-8",
    )
    rc = mod.main(["import-company-directory", "--file", str(bad)])
    assert rc != 0
    assert dest.read_bytes() == before  # byte-for-byte untouched


def test_import_rejects_unparseable_toml(tmp_path: pathlib.Path):
    mod = _mod()
    src = tmp_path / "broken.toml"
    src.write_text("this is not = = toml\n", encoding="utf-8")
    assert mod.main(["import-company-directory", "--file", str(src)]) != 0
    assert not mod.company_directory_path().exists()


# --------------------------------------------------------------------------
# Atomic-write + registry-read hardening (symlink / size / crash-safety).
# --------------------------------------------------------------------------

def test_atomic_write_refuses_symlinked_destination(tmp_path: pathlib.Path):
    """A symlinked registry destination must not be written through."""
    mod = _mod()
    victim = tmp_path / "victim.json"
    victim.write_text('{"do":"not-touch"}', encoding="utf-8")

    dest = mod.company_directory_path()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.symlink_to(victim)

    src = _write_toml(tmp_path, VALID_TOML)
    assert mod.main(["import-company-directory", "--file", str(src)]) != 0
    # The victim was never followed/overwritten, and the path is still the
    # untouched symlink (no write landed through the link).
    assert victim.read_text(encoding="utf-8") == '{"do":"not-touch"}'
    assert dest.is_symlink()


def test_load_directory_refuses_symlinked_registry(tmp_path: pathlib.Path):
    mod = _mod()
    real = tmp_path / "real.json"
    real.write_text('{"schema_version": 1, "agents": [], "rooms": []}', encoding="utf-8")

    dest = mod.company_directory_path()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.symlink_to(real)

    with pytest.raises(mod.DirectoryError) as exc:
        mod.load_directory()
    assert "symlink" in str(exc.value)


def test_load_bindings_refuses_symlinked_registry(tmp_path: pathlib.Path):
    mod = _mod()
    real = tmp_path / "real_bindings.json"
    real.write_text('{"schema_version": 1, "bindings": []}', encoding="utf-8")

    dest = mod.company_bindings_path()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.symlink_to(real)

    with pytest.raises(mod.DirectoryError) as exc:
        mod.load_bindings()
    assert "symlink" in str(exc.value)


def test_load_directory_refuses_oversized_registry(tmp_path: pathlib.Path):
    mod = _mod()
    dest = mod.company_directory_path()
    dest.parent.mkdir(parents=True, exist_ok=True)
    # One byte over the mirrored Go cap; the size guard fires before parse.
    dest.write_bytes(b"{" + b" " * mod._MAX_REGISTRY_BYTES)
    with pytest.raises(mod.DirectoryError) as exc:
        mod.load_directory()
    assert "exceeds" in str(exc.value)


def test_atomic_write_preserves_old_file_on_failure(tmp_path: pathlib.Path, monkeypatch):
    """A failure before os.replace leaves the old file intact, no temp leak."""
    mod = _mod()
    src = _write_toml(tmp_path, VALID_TOML)
    assert mod.main(["import-company-directory", "--file", str(src)]) == 0
    dest = mod.company_directory_path()
    before = dest.read_bytes()

    def boom(*_a, **_k):
        raise OSError("simulated fsync failure before replace")

    monkeypatch.setattr(os, "fsync", boom)
    with pytest.raises(OSError):
        mod._atomic_write_json(dest, {"schema_version": 1, "agents": [], "rooms": []})

    # Old registry byte-for-byte untouched, and no temp file left behind.
    assert dest.read_bytes() == before
    leftovers = [p.name for p in dest.parent.iterdir() if p.name != dest.name]
    assert leftovers == []


# --------------------------------------------------------------------------
# bind-company-agent: create + replace semantics.
# --------------------------------------------------------------------------

def _import_valid(mod, tmp_path: pathlib.Path) -> None:
    src = _write_toml(tmp_path, VALID_TOML)
    assert mod.main(["import-company-directory", "--file", str(src)]) == 0


def test_bind_create_then_replace(tmp_path: pathlib.Path):
    mod = _mod()
    _import_valid(mod, tmp_path)

    assert mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "ollie-main",
    ]) == 0
    bindings = json.loads(mod.company_bindings_path().read_text())
    assert bindings["bindings"] == [
        {"room": "orchestrator-team", "agent": "ollie", "session": "ollie-main"},
    ]

    # Rebind same (room, agent): replaces, still a single entry.
    assert mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "ollie-backup",
    ]) == 0
    bindings = json.loads(mod.company_bindings_path().read_text())
    assert bindings["bindings"] == [
        {"room": "orchestrator-team", "agent": "ollie", "session": "ollie-backup"},
    ]

    # Bind a second agent: two entries now.
    assert mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "riley", "--session", "riley-main",
    ]) == 0
    bindings = json.loads(mod.company_bindings_path().read_text())
    assert {(b["agent"], b["session"]) for b in bindings["bindings"]} == {
        ("ollie", "ollie-backup"), ("riley", "riley-main"),
    }
    assert len(bindings["bindings"]) == 2


def test_bind_reports_created_vs_replaced(tmp_path: pathlib.Path, capsys):
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()  # drain import summary
    mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "s1",
    ])
    assert json.loads(capsys.readouterr().out)["action"] == "created"
    mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "s2",
    ])
    assert json.loads(capsys.readouterr().out)["action"] == "replaced"
    # Re-binding the same (room, agent) to the SAME session is a no-op:
    # "unchanged", never "created" or "replaced".
    mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "s2",
    ])
    assert json.loads(capsys.readouterr().out)["action"] == "unchanged"


def test_bind_rejects_session_already_bound_to_different_agent(tmp_path: pathlib.Path):
    """A session is one agent's identity in a room: binding the same session to
    a DIFFERENT agent in the same room is rejected (would clobber the pointer)."""
    mod = _mod()
    _import_valid(mod, tmp_path)
    assert mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "shared-session",
    ]) == 0
    # Same room, same session, different agent -> hard error.
    assert mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "riley", "--session", "shared-session",
    ]) != 0
    # The rejected bind left the registry with only the first binding.
    bindings = json.loads(mod.company_bindings_path().read_text())
    assert bindings["bindings"] == [
        {"room": "orchestrator-team", "agent": "ollie", "session": "shared-session"},
    ]
    # Re-binding the SAME (room, agent) to that session is still fine (idempotent).
    assert mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "shared-session",
    ]) == 0


def test_bind_unknown_room_or_agent_errors(tmp_path: pathlib.Path):
    mod = _mod()
    _import_valid(mod, tmp_path)
    assert mod.main([
        "bind-company-agent", "--room", "ghost-room",
        "--agent", "ollie", "--session", "s1",
    ]) != 0
    assert mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ghost", "--session", "s1",
    ]) != 0


def test_bind_without_directory_errors(tmp_path: pathlib.Path):
    mod = _mod()
    # No import first.
    assert mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "s1",
    ]) != 0
    assert not mod.company_bindings_path().exists()


# --------------------------------------------------------------------------
# peers: report content + membership warnings (Slack API mocked).
# --------------------------------------------------------------------------

def _all_present_api_call(method, params, token, api_base):
    if method == "conversations.info":
        return {"ok": True, "channel": {"id": params["channel"], "is_member": True}}
    if method == "conversations.members":
        return {"ok": True, "members": ["U0AAAAAA1", "U0AAAAAA2"]}
    raise AssertionError(f"unexpected method {method}")


def test_peers_reports_rooms_members_wake_and_bindings(tmp_path: pathlib.Path, monkeypatch, capsys):
    mod = _mod()
    _import_valid(mod, tmp_path)
    mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "ollie-main",
    ])
    capsys.readouterr()  # drain bind output

    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setattr(mod, "_slack_api_call", _all_present_api_call)

    assert mod.main(["peers"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert len(report["rooms"]) == 1
    room = report["rooms"][0]
    assert room["name"] == "orchestrator-team"
    assert room["members"] == ["ollie", "riley"]
    assert room["ambient_wake"] == ["ollie"]
    assert room["mention_wake"] == ["ollie", "riley"]
    assert room["bindings"] == [{"agent": "ollie", "session": "ollie-main"}]
    # Everyone present -> no membership warnings.
    assert report["membership_warnings"] == []
    assert report["binding_warnings"] == []


def test_peers_surfaces_switchboard_not_member_warning(tmp_path: pathlib.Path, monkeypatch, capsys):
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()

    def api_call(method, params, token, api_base):
        if method == "conversations.info":
            return {"ok": True, "channel": {"id": params["channel"], "is_member": False}}
        if method == "conversations.members":
            return {"ok": True, "members": []}
        raise AssertionError(method)

    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setattr(mod, "_slack_api_call", api_call)

    assert mod.main(["peers"]) == 0
    report = json.loads(capsys.readouterr().out)
    warnings = report["membership_warnings"]
    assert any("switchboard bot is not a member" in w for w in warnings)
    # Member bots also flagged absent.
    assert any("ollie" in w and "is not a member" in w for w in warnings)


def test_peers_membership_skipped_without_token(tmp_path: pathlib.Path, capsys):
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()
    # No SLACK_BOT_TOKEN (isolated env) -> best-effort skip, never a failure.
    assert mod.main(["peers"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert any("skipped" in w for w in report["membership_warnings"])


def test_peers_missing_scope_degrades_to_warning(tmp_path: pathlib.Path, monkeypatch, capsys):
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()

    def api_call(method, params, token, api_base):
        return {"ok": False, "error": "missing_scope"}

    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setattr(mod, "_slack_api_call", api_call)

    assert mod.main(["peers"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert any("missing_scope" in w for w in report["membership_warnings"])


def test_peers_room_filter(tmp_path: pathlib.Path, capsys):
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()
    assert mod.main(["peers", "--room", "orchestrator-team"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert [r["name"] for r in report["rooms"]] == ["orchestrator-team"]
    # Unknown room filter errors.
    assert mod.main(["peers", "--room", "ghost"]) != 0


def test_peers_without_directory_errors(tmp_path: pathlib.Path):
    mod = _mod()
    assert mod.main(["peers"]) != 0


def test_peers_drops_stale_binding_with_warning(tmp_path: pathlib.Path, monkeypatch, capsys):
    """A binding whose room left the directory is dropped and warned about."""
    mod = _mod()
    _import_valid(mod, tmp_path)
    mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "ollie-main",
    ])
    capsys.readouterr()

    # Re-import a directory that no longer contains orchestrator-team.
    shrunk = """\
schema_version = 1

[[agents]]
name = "ollie"
app_id = "A0AAAAAA1"
bot_user_id = "U0AAAAAA1"

[[rooms]]
name = "other-room"
team_id = "T0BBBBBBB"
channel_id = "C0BBBBBBB"
members = ["ollie"]
"""
    src = tmp_path / "shrunk.toml"
    src.write_text(shrunk, encoding="utf-8")
    assert mod.main(["import-company-directory", "--file", str(src)]) == 0
    capsys.readouterr()

    assert mod.main(["peers"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert any("orchestrator-team" in w for w in report["binding_warnings"])
    # The surviving room reports no bindings.
    other = next(r for r in report["rooms"] if r["name"] == "other-room")
    assert other["bindings"] == []


def test_bind_city_qualified(tmp_path: pathlib.Path, capsys):
    """City-qualified bindings: --city persists, changes count as replaced,
    and URL-significant city values are rejected (mirrors the Go loader)."""
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()

    mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "teams__pm", "--city", "platform",
    ])
    out = json.loads(capsys.readouterr().out)
    assert out["action"] == "created"
    assert out["city"] == "platform"
    stored = json.loads(mod.company_bindings_path().read_text())
    assert stored["bindings"][0]["city"] == "platform"

    # Identical re-run (same session AND city) is unchanged.
    mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "teams__pm", "--city", "platform",
    ])
    assert json.loads(capsys.readouterr().out)["action"] == "unchanged"

    # Same session, different city -> replaced.
    mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "teams__pm", "--city", "trust",
    ])
    assert json.loads(capsys.readouterr().out)["action"] == "replaced"

    # Dropping --city clears the qualifier (replaced; key absent on disk).
    mod.main([
        "bind-company-agent", "--room", "orchestrator-team",
        "--agent", "ollie", "--session", "teams__pm",
    ])
    assert json.loads(capsys.readouterr().out)["action"] == "replaced"
    stored = json.loads(mod.company_bindings_path().read_text())
    assert "city" not in stored["bindings"][0]

    # URL-significant / whitespace city values fail closed.
    for bad in ("a/b", "a?b", "a#b", "a%b", "a b"):
        assert mod.main([
            "bind-company-agent", "--room", "orchestrator-team",
            "--agent", "ollie", "--session", "teams__pm", "--city", bad,
        ]) != 0
        capsys.readouterr()


def test_bind_same_session_name_different_city_allowed(tmp_path: pathlib.Path, capsys):
    """The one-session-one-agent-per-room guard compares (session, city):
    the same session NAME in different cities is a different session."""
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()
    mod.main(["bind-company-agent", "--room", "orchestrator-team",
              "--agent", "ollie", "--session", "teams.pm", "--city", "orchestration"])
    capsys.readouterr()
    # Same name, different city: allowed.
    assert mod.main(["bind-company-agent", "--room", "orchestrator-team",
                     "--agent", "riley", "--session", "teams.pm", "--city", "substrate"]) == 0
    capsys.readouterr()
    # Same name, SAME city, different agent: rejected.
    assert mod.main(["bind-company-agent", "--room", "orchestrator-team",
                     "--agent", "riley", "--session", "teams.pm", "--city", "orchestration"]) != 0


# --------------------------------------------------------------------------
# bind-company-dm: per-agent singleton DM bindings (Phase 4, D-DM1).
# --------------------------------------------------------------------------

def test_dm_bind_create_replace_unchanged(tmp_path: pathlib.Path, capsys):
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()

    assert mod.main(["bind-company-dm", "--agent", "ollie", "--session", "ollie"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["action"] == "created" and out["total_dm_bindings"] == 1
    stored = json.loads(mod.company_dm_bindings_path().read_text())
    assert stored["schema_version"] == 1
    assert stored["dm_bindings"] == [{"agent": "ollie", "session": "ollie"}]

    # Rebind same agent, different session -> replaced (still one entry).
    assert mod.main(["bind-company-dm", "--agent", "ollie", "--session", "ollie-2"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["action"] == "replaced"
    stored = json.loads(mod.company_dm_bindings_path().read_text())
    assert stored["dm_bindings"] == [{"agent": "ollie", "session": "ollie-2"}]

    # Identical re-run -> unchanged.
    assert mod.main(["bind-company-dm", "--agent", "ollie", "--session", "ollie-2"]) == 0
    assert json.loads(capsys.readouterr().out)["action"] == "unchanged"


def test_dm_bind_singleton_per_agent_and_session_guard(tmp_path: pathlib.Path, capsys):
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()
    assert mod.main(["bind-company-dm", "--agent", "ollie", "--session", "shared"]) == 0
    capsys.readouterr()
    # A different agent may not DM-bind the same (session, city).
    assert mod.main(["bind-company-dm", "--agent", "riley", "--session", "shared"]) != 0
    err = capsys.readouterr().err
    assert "already bound to DM agent 'ollie'" in err
    # Two agents remain distinct singletons on distinct sessions.
    assert mod.main(["bind-company-dm", "--agent", "riley", "--session", "riley"]) == 0
    stored = json.loads(mod.company_dm_bindings_path().read_text())
    assert {e["agent"] for e in stored["dm_bindings"]} == {"ollie", "riley"}


def test_dm_bind_city_qualified(tmp_path: pathlib.Path, capsys):
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()
    assert mod.main(["bind-company-dm", "--agent", "ollie",
                     "--session", "teams__pm", "--city", "platform"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["city"] == "platform"
    stored = json.loads(mod.company_dm_bindings_path().read_text())
    assert stored["dm_bindings"][0]["city"] == "platform"
    # Same session NAME, different city is a different session: another agent
    # may bind it.
    assert mod.main(["bind-company-dm", "--agent", "riley",
                     "--session", "teams__pm", "--city", "substrate"]) == 0
    # Same session, SAME city, different agent: rejected.
    assert mod.main(["bind-company-dm", "--agent", "riley",
                     "--session", "teams__pm", "--city", "platform"]) != 0
    capsys.readouterr()
    # Dropping --city clears the qualifier (replaced; key absent on disk).
    assert mod.main(["bind-company-dm", "--agent", "ollie", "--session", "teams__pm"]) == 0
    capsys.readouterr()
    stored = json.loads(mod.company_dm_bindings_path().read_text())
    ollie = next(e for e in stored["dm_bindings"] if e["agent"] == "ollie")
    assert "city" not in ollie


def test_dm_bind_rejects_url_significant_city(tmp_path: pathlib.Path, capsys):
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()
    for bad in ("a/b", "a b", "a#b", "a?b", "a%b"):
        assert mod.main(["bind-company-dm", "--agent", "ollie",
                         "--session", "ollie", "--city", bad]) != 0
        capsys.readouterr()


def test_dm_bindings_golden_fixture_matches_writer_bytes(tmp_path: pathlib.Path, capsys):
    """C4/m12: the committed dm_bindings.json fixture is BYTE-IDENTICAL to what
    cmd_bind_dm actually writes (omitted-empty city for ollie, city-qualified for
    riley), so a drift in the writer's serialization breaks a test rather than
    shipping a fixture no writer can produce."""
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()
    assert mod.main(["bind-company-dm", "--agent", "ollie", "--session", "ollie"]) == 0
    assert mod.main(["bind-company-dm", "--agent", "riley",
                     "--session", "riley-main", "--city", "riley-city"]) == 0
    capsys.readouterr()
    produced = mod.company_dm_bindings_path().read_bytes()
    fixture = (PACK_DIR / "tests" / "fixtures" / "company" / "dm_bindings.json").read_bytes()
    assert produced == fixture


def test_dm_bind_unknown_agent_errors(tmp_path: pathlib.Path):
    mod = _mod()
    _import_valid(mod, tmp_path)
    assert mod.main(["bind-company-dm", "--agent", "nobody", "--session", "x"]) != 0


def test_dm_bind_session_guard_normalizes_aliases(tmp_path: pathlib.Path, capsys):
    """m5: two agents cannot bind alias-equivalent spellings of one running
    session (config dot form vs gc-runtime dunder form)."""
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()
    assert mod.main(["bind-company-dm", "--agent", "ollie", "--session", "a.b"]) == 0
    capsys.readouterr()
    # riley tries the dunder spelling of the SAME session — rejected.
    assert mod.main(["bind-company-dm", "--agent", "riley", "--session", "a__b"]) != 0
    assert "already bound to DM agent 'ollie'" in capsys.readouterr().err


def test_dm_unbind_removes_binding(tmp_path: pathlib.Path, capsys):
    """m11: --remove clears a binding (the unbind recovery path)."""
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()
    assert mod.main(["bind-company-dm", "--agent", "ollie", "--session", "ollie"]) == 0
    capsys.readouterr()
    assert mod.main(["bind-company-dm", "--remove", "ollie"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["action"] == "removed" and out["total_dm_bindings"] == 0
    assert mod.load_dm_bindings()["dm_bindings"] == []
    # Removing a non-existent binding errors.
    assert mod.main(["bind-company-dm", "--remove", "ollie"]) != 0


def test_dm_bind_stale_row_does_not_block_and_unbind_recovers(tmp_path: pathlib.Path, capsys):
    """m11: a row whose agent left the directory must not block binding that
    session to a live agent; --remove clears the stale row outright."""
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()
    # Hand-write a stale binding: agent "ghost" (not in the directory) on session "s".
    dest = mod.company_dm_bindings_path()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps({
        "schema_version": 1,
        "dm_bindings": [{"agent": "ghost", "session": "s"}]}))
    # A live agent may claim that session despite the stale row (the guard skips
    # directory-absent agents, matching every read surface).
    assert mod.main(["bind-company-dm", "--agent", "ollie", "--session", "s"]) == 0
    capsys.readouterr()
    # The stale row is still recoverable via --remove.
    assert mod.main(["bind-company-dm", "--remove", "ghost"]) == 0
    agents = {e["agent"] for e in mod.load_dm_bindings()["dm_bindings"]}
    assert agents == {"ollie"}


def test_load_dm_bindings_rejects_bad_schema_version(tmp_path: pathlib.Path):
    """m10: Python honors schema_version, failing closed on the same document
    the Go loader rejects."""
    mod = _mod()
    dest = mod.company_dm_bindings_path()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps({"schema_version": 9, "dm_bindings": []}))
    with pytest.raises(mod.DirectoryError) as exc:
        mod.load_dm_bindings()
    assert "schema_version" in str(exc.value)


def test_dm_bind_without_directory_errors(tmp_path: pathlib.Path):
    mod = _mod()
    assert mod.main(["bind-company-dm", "--agent", "ollie", "--session", "ollie"]) != 0


def test_load_dm_bindings_empty_when_absent(tmp_path: pathlib.Path):
    mod = _mod()
    data = mod.load_dm_bindings()
    assert data == {"schema_version": 1, "dm_bindings": []}


def test_peers_reports_dm_bindings(tmp_path: pathlib.Path, monkeypatch, capsys):
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()
    monkeypatch.setattr(mod, "verify_memberships", lambda directory: [])
    mod.main(["bind-company-dm", "--agent", "ollie", "--session", "ollie"])
    capsys.readouterr()
    assert mod.main(["peers"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["dm_bindings"] == [{"agent": "ollie", "session": "ollie"}]


def test_peers_drops_stale_dm_binding_with_warning(tmp_path: pathlib.Path, monkeypatch, capsys):
    mod = _mod()
    _import_valid(mod, tmp_path)
    capsys.readouterr()
    monkeypatch.setattr(mod, "verify_memberships", lambda directory: [])
    # Write a dm binding referencing an agent not in the directory.
    mod.company_dm_bindings_path().write_text(json.dumps({
        "schema_version": 1,
        "dm_bindings": [{"agent": "ghost", "session": "ghost"}]}))
    assert mod.main(["peers"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["dm_bindings"] == []
    assert any("dm binding agent='ghost'" in w for w in report["binding_warnings"])


# --------------------------------------------------------------------------
# dm_allowed_humans directory import (Phase 4, D-DM2) — the Python IMPORT
# surface. The allow/deny POLICY evaluation is Go-only (admission), so it is
# not exercised here.
# --------------------------------------------------------------------------

_ALLOW_TOML = VALID_TOML  # reused base; allowlist prepended per case


def test_import_preserves_dm_allowed_humans(tmp_path: pathlib.Path):
    mod = _mod()
    toml = 'dm_allowed_humans = ["U111", "U222", "U111"]\n' + VALID_TOML
    src = _write_toml(tmp_path, toml)
    assert mod.main(["import-company-directory", "--file", str(src)]) == 0
    data = json.loads(mod.company_directory_path().read_text())
    # Deduped, order-preserving.
    assert data["dm_allowed_humans"] == ["U111", "U222"]


def test_import_preserves_present_empty_allowlist(tmp_path: pathlib.Path):
    mod = _mod()
    toml = "dm_allowed_humans = []\n" + VALID_TOML
    src = _write_toml(tmp_path, toml)
    assert mod.main(["import-company-directory", "--file", str(src)]) == 0
    data = json.loads(mod.company_directory_path().read_text())
    # Present-but-empty must survive as a distinct signal (Go: nobody allowed).
    assert "dm_allowed_humans" in data and data["dm_allowed_humans"] == []


def test_import_absent_allowlist_omits_key(tmp_path: pathlib.Path):
    mod = _mod()
    src = _write_toml(tmp_path, VALID_TOML)
    assert mod.main(["import-company-directory", "--file", str(src)]) == 0
    data = json.loads(mod.company_directory_path().read_text())
    # Absent must stay absent (Go: nil => all workspace humans allowed).
    assert "dm_allowed_humans" not in data


def test_import_rejects_non_string_allowlist_entry(tmp_path: pathlib.Path):
    mod = _mod()
    toml = "dm_allowed_humans = [123]\n" + VALID_TOML
    src = _write_toml(tmp_path, toml)
    assert mod.main(["import-company-directory", "--file", str(src)]) != 0


def test_import_rejects_non_array_allowlist(tmp_path: pathlib.Path):
    mod = _mod()
    toml = 'dm_allowed_humans = "U111"\n' + VALID_TOML
    src = _write_toml(tmp_path, toml)
    assert mod.main(["import-company-directory", "--file", str(src)]) != 0
