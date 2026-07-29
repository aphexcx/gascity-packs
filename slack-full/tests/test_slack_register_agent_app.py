"""Tests for the slack-pack register-agent-app CLI surface (company rooms 4).

Covers the register verb's validation table (api_app_id / team_id / 32-hex
secret shapes), the 0600 secret-store write + create/replace/unchanged
semantics, the group/world-readable refusal, the symlink refusal, the
--signing-secret-file input, and the best-effort directory-join warning
(present ⇒ silent, absent/unmatched ⇒ warned). Hermetic: no network, state
under tmp_path.
"""

from __future__ import annotations

import json
import os
import pathlib
import stat
import sys

import pytest

PACK_DIR = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PACK_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

_SECRET = "0123456789abcdef0123456789abcdef"


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.setenv("GC_CITY_NAME", "test-city")
    monkeypatch.setenv("GC_CITY_PATH", str(tmp_path))
    monkeypatch.delenv("SLACK_COMPANY_AGENT_APPS_PATH", raising=False)
    monkeypatch.delenv("SLACK_COMPANY_DIRECTORY_PATH", raising=False)


def _mod():
    sys.modules.pop("slack_register_agent_app", None)
    import slack_register_agent_app  # type: ignore
    return slack_register_agent_app


def _apps(mod) -> dict:
    return json.loads(mod.agent_apps_path().read_text())


def test_agent_apps_golden_fixture_matches_writer_bytes() -> None:
    """C4/m12: the committed agent_apps.json fixture is BYTE-IDENTICAL to what
    register-agent-app writes (sorted-by-id, 32-hex secrets the verb accepts), so
    a fixture the CLI could never produce cannot be shipped."""
    mod = _mod()
    # Register out of order — the writer sorts by api_app_id, so bytes are stable.
    assert mod.register_agent_app(
        team_id="T0AAAAAAA", api_app_id="A0AAAAAA2",
        signing_secret="fedcba9876543210fedcba9876543210")["action"] == "created"
    assert mod.register_agent_app(
        team_id="T0AAAAAAA", api_app_id="A0AAAAAA1",
        signing_secret="0123456789abcdef0123456789abcdef")["action"] == "created"
    produced = mod.agent_apps_path().read_bytes()
    fixture = (PACK_DIR / "tests" / "fixtures" / "company" / "agent_apps.json").read_bytes()
    assert produced == fixture


def test_register_writes_0600_record() -> None:
    mod = _mod()
    rc = mod.main(["--team-id", "T0AAAAAAA", "--api-app-id", "A0AAAAAA1",
                   "--signing-secret", _SECRET])
    assert rc == 0
    path = mod.agent_apps_path()
    assert stat.S_IMODE(os.lstat(path).st_mode) == 0o600
    data = _apps(mod)
    assert data["schema_version"] == 1
    assert data["agent_apps"] == [
        {"team_id": "T0AAAAAAA", "api_app_id": "A0AAAAAA1", "signing_secret": _SECRET}]


def test_register_create_replace_unchanged() -> None:
    mod = _mod()
    r1 = mod.register_agent_app(team_id="T0AAAAAAA", api_app_id="A0AAAAAA1", signing_secret=_SECRET)
    assert r1["action"] == "created"
    r2 = mod.register_agent_app(team_id="T0AAAAAAA", api_app_id="A0AAAAAA1", signing_secret=_SECRET)
    assert r2["action"] == "unchanged"
    rotated = "ffffffffffffffffffffffffffffffff"
    r3 = mod.register_agent_app(team_id="T0AAAAAAA", api_app_id="A0AAAAAA1", signing_secret=rotated)
    assert r3["action"] == "replaced"
    data = _apps(mod)
    assert len(data["agent_apps"]) == 1
    assert data["agent_apps"][0]["signing_secret"] == rotated


def test_multiple_apps_sorted_by_id() -> None:
    mod = _mod()
    mod.register_agent_app(team_id="T0AAAAAAA", api_app_id="A0ZZZZZZ9", signing_secret=_SECRET)
    mod.register_agent_app(team_id="T0AAAAAAA", api_app_id="A0AAAAAA1", signing_secret=_SECRET)
    ids = [a["api_app_id"] for a in _apps(mod)["agent_apps"]]
    assert ids == ["A0AAAAAA1", "A0ZZZZZZ9"]


@pytest.mark.parametrize("bad", ["", "B0AAAAAA1", "A", "a0aaaaaa1", "A0 AAAA"])
def test_bad_api_app_id_rejected(bad: str) -> None:
    mod = _mod()
    with pytest.raises(mod.RegisterError):
        mod.register_agent_app(team_id="T0AAAAAAA", api_app_id=bad, signing_secret=_SECRET)


@pytest.mark.parametrize("bad", ["", "X0AAAAAA", "T", "t0aaaaaa1"])
def test_bad_team_id_rejected(bad: str) -> None:
    mod = _mod()
    with pytest.raises(mod.RegisterError):
        mod.register_agent_app(team_id=bad, api_app_id="A0AAAAAA1", signing_secret=_SECRET)


@pytest.mark.parametrize("bad", [
    "", "nothex", _SECRET[:31], _SECRET + "0", "0123456789abcdef0123456789abcdeg"])
def test_bad_signing_secret_rejected(bad: str) -> None:
    mod = _mod()
    with pytest.raises(mod.RegisterError):
        mod.register_agent_app(team_id="T0AAAAAAA", api_app_id="A0AAAAAA1", signing_secret=bad)


def test_uppercase_hex_secret_accepted() -> None:
    mod = _mod()
    r = mod.register_agent_app(
        team_id="T0AAAAAAA", api_app_id="A0AAAAAA1",
        signing_secret="0123456789ABCDEF0123456789ABCDEF")
    assert r["action"] == "created"


def test_enterprise_grid_team_id_accepted() -> None:
    mod = _mod()
    r = mod.register_agent_app(team_id="E0AAAAAAA", api_app_id="A0AAAAAA1", signing_secret=_SECRET)
    assert r["action"] == "created"


def test_signing_secret_file_input(tmp_path: pathlib.Path) -> None:
    mod = _mod()
    secret_file = tmp_path / "secret.txt"
    secret_file.write_text(_SECRET + "\n")
    rc = mod.main(["--team-id", "T0AAAAAAA", "--api-app-id", "A0AAAAAA1",
                   "--signing-secret-file", str(secret_file)])
    assert rc == 0
    assert _apps(mod)["agent_apps"][0]["signing_secret"] == _SECRET


def test_both_secret_inputs_rejected(tmp_path: pathlib.Path) -> None:
    mod = _mod()
    rc = mod.main(["--team-id", "T0AAAAAAA", "--api-app-id", "A0AAAAAA1",
                   "--signing-secret", _SECRET, "--signing-secret-file", str(tmp_path / "x")])
    assert rc == 1


def test_refuses_to_overwrite_group_readable_registry() -> None:
    mod = _mod()
    mod.register_agent_app(team_id="T0AAAAAAA", api_app_id="A0AAAAAA1", signing_secret=_SECRET)
    path = mod.agent_apps_path()
    os.chmod(path, 0o644)
    with pytest.raises(mod.RegisterError) as exc:
        mod.register_agent_app(team_id="T0AAAAAAA", api_app_id="A0AAAAAA2", signing_secret=_SECRET)
    assert "group/world" in str(exc.value)


def test_refuses_symlinked_registry(tmp_path: pathlib.Path) -> None:
    mod = _mod()
    path = mod.agent_apps_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    real = tmp_path / "real_apps.json"
    real.write_text(json.dumps({"schema_version": 1, "agent_apps": []}))
    os.chmod(real, 0o600)
    os.symlink(real, path)
    with pytest.raises(mod.RegisterError) as exc:
        mod.register_agent_app(team_id="T0AAAAAAA", api_app_id="A0AAAAAA1", signing_secret=_SECRET)
    assert "symlink" in str(exc.value)


def _write_directory(mod, tmp_path: pathlib.Path, app_ids: list[str]) -> None:
    slackdir = tmp_path / ".gc" / "slack"
    slackdir.mkdir(parents=True, exist_ok=True)
    (slackdir / "company_directory.json").write_text(json.dumps({
        "schema_version": 1,
        "agents": [{"name": f"a{i}", "app_id": a, "bot_user_id": f"U{i}"}
                   for i, a in enumerate(app_ids)],
        "rooms": [],
    }))


def test_directory_join_warning_when_no_match(tmp_path: pathlib.Path) -> None:
    mod = _mod()
    _write_directory(mod, tmp_path, ["A0OTHER99"])
    r = mod.register_agent_app(team_id="T0AAAAAAA", api_app_id="A0AAAAAA1", signing_secret=_SECRET)
    assert "directory_join_warning" in r
    assert r["action"] == "created"  # warned, not failed


def test_no_warning_when_directory_has_the_app(tmp_path: pathlib.Path) -> None:
    mod = _mod()
    _write_directory(mod, tmp_path, ["A0AAAAAA1"])
    r = mod.register_agent_app(team_id="T0AAAAAAA", api_app_id="A0AAAAAA1", signing_secret=_SECRET)
    assert "directory_join_warning" not in r


def test_warns_when_directory_absent(tmp_path: pathlib.Path) -> None:
    mod = _mod()
    r = mod.register_agent_app(team_id="T0AAAAAAA", api_app_id="A0AAAAAA1", signing_secret=_SECRET)
    assert "directory_join_warning" in r
