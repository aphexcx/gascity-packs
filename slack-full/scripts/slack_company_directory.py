#!/usr/bin/env python3
"""Company-directory CLI surface for the slack-full pack (company rooms 1e).

Three subcommands back three `gc slack` verbs:

  * ``import-company-directory --file <rooms.toml>`` — parse and validate
    the non-secret company directory TOML, expand ``*`` wildcards, and
    atomically write the normalized ``company_directory.json`` registry.
  * ``bind-company-agent --room --agent --session`` — record the singleton
    ``(room, agent) -> session`` binding in ``company_bindings.json``.
  * ``peers [--room]`` — report rooms, members, wake policy, bindings, and
    best-effort switchboard-membership warnings from the normalized JSON.

Both registries resolve to the same directory the adapter's other registries
live in (``<GC_CITY_PATH>/.gc/slack/`` when set, else
``/tmp/gc-slack-adapter/``), with the ``SLACK_COMPANY_DIRECTORY_PATH`` and
``SLACK_COMPANY_BINDINGS_PATH`` env overrides taking precedence — mirroring
``adapter/main.go``'s per-registry override convention.

The directory is versioned, non-secret data and never contains tokens.
Membership verification (``conversations.info`` / ``conversations.members``)
is strictly best-effort: a missing token, a missing scope, or a failed check
produces warnings, never an import failure.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import stat
import sys
import tempfile
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

import slack_intake_common as common

SCHEMA_VERSION = 1

# Cap on a registry file's on-disk size, mirroring the Go adapter's
# maxRegistryBytes (adapter/interactions.go: 10 << 20). Reads above this are
# a hard error on both sides so a hostile or corrupt file cannot be slurped
# into memory.
_MAX_REGISTRY_BYTES = 10 << 20  # 10 MiB

# Agent and room names are lowercase stable slugs. team_id/channel_id/app_id/
# bot_user_id are opaque Slack IDs and are NOT slug-checked (only uniqueness
# and non-emptiness matter for them).
#
# This pattern is byte-for-byte identical to the Go loader's companySlugRE
# (adapter/company_directory.go): lowercase alphanumerics in hyphen-separated
# groups, no leading/trailing/double hyphen and no underscores. Writer and
# reader MUST agree — a name the CLI accepts but the Go loader rejects makes
# the adapter install a nil snapshot and silently disable ALL company routing.
_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")

DEFAULT_SLACK_API_BASE = "https://slack.com/api"

_AGENT_KEYS = frozenset({"name", "app_id", "bot_user_id"})
_ROOM_KEYS = frozenset({"name", "team_id", "channel_id", "members", "ambient_wake", "mention_wake"})
_TOP_KEYS = frozenset({"schema_version", "agents", "rooms", "dm_allowed_humans"})


class DirectoryError(RuntimeError):
    """Raised when a directory/binding input is invalid (routing fails closed)."""


class MembershipCheckError(RuntimeError):
    """Raised on a transport-level failure of a best-effort membership check."""


# --- registry path resolution --------------------------------------------

def _registry_path(env_override: str, filename: str) -> pathlib.Path:
    """Resolve a company registry path the same way the adapter does.

    Precedence: explicit env override > ``<GC_CITY_PATH>/.gc/slack`` >
    ``/tmp/gc-slack-adapter``. This mirrors the per-registry override style
    around ``adapter/main.go`` (e.g. ``SLACK_APPS_REGISTRY_PATH``), so the
    Python CLI and the Go loader resolve identical files.
    """
    override = os.environ.get(env_override, "").strip()
    if override:
        return pathlib.Path(override)
    city = os.environ.get("GC_CITY_PATH", "").strip()
    if city:
        return pathlib.Path(city) / ".gc" / "slack" / filename
    return pathlib.Path("/tmp/gc-slack-adapter") / filename


def company_directory_path() -> pathlib.Path:
    return _registry_path("SLACK_COMPANY_DIRECTORY_PATH", "company_directory.json")


def company_bindings_path() -> pathlib.Path:
    return _registry_path("SLACK_COMPANY_BINDINGS_PATH", "company_bindings.json")


def company_dm_bindings_path() -> pathlib.Path:
    return _registry_path("SLACK_COMPANY_DM_BINDINGS_PATH", "dm_bindings.json")


# --- validation helpers ---------------------------------------------------

def _check_unknown_keys(table: dict[str, Any], allowed: frozenset[str], ctx: str) -> None:
    extra = sorted(set(table) - allowed)
    if extra:
        raise DirectoryError(f"{ctx} has unknown key(s): {extra}")


def _require_slug(value: Any, ctx: str) -> str:
    if not isinstance(value, str) or not _SLUG_RE.match(value):
        raise DirectoryError(f"{ctx} must be a lowercase slug, got {value!r}")
    return value


def _require_id(value: Any, ctx: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DirectoryError(f"{ctx} must be a non-empty string, got {value!r}")
    return value


def _expand_members(raw: Any, known: set[str], agent_order: list[str], ctx: str) -> list[str]:
    """Resolve a room ``members`` list; ``*`` expands to every directory agent."""
    if not isinstance(raw, list):
        raise DirectoryError(f"{ctx} must be an array")
    wildcard = False
    explicit: list[str] = []
    seen: set[str] = set()
    for tok in raw:
        if not isinstance(tok, str) or not tok:
            raise DirectoryError(f"{ctx} entries must be non-empty strings")
        if tok == "*":
            wildcard = True
            continue
        if tok not in known:
            raise DirectoryError(f"{ctx} references unknown agent {tok!r}")
        if tok not in seen:
            seen.add(tok)
            explicit.append(tok)
    if wildcard:
        return list(agent_order)
    return explicit


def _resolve_wake(
    raw: Any,
    known: set[str],
    member_set: set[str],
    members_order: list[str],
    *,
    allow_wildcard: bool,
    ctx: str,
) -> list[str]:
    """Resolve a wake list. ``*`` (only where allowed) expands to all members.

    Every explicit entry must be a known agent AND a member of the room
    (wake lists must be subsets of members).
    """
    if not isinstance(raw, list):
        raise DirectoryError(f"{ctx} must be an array")
    wildcard = False
    explicit: list[str] = []
    seen: set[str] = set()
    for tok in raw:
        if not isinstance(tok, str) or not tok:
            raise DirectoryError(f"{ctx} entries must be non-empty strings")
        if tok == "*":
            if not allow_wildcard:
                raise DirectoryError(f"{ctx} may not contain the '*' wildcard")
            wildcard = True
            continue
        if tok not in known:
            raise DirectoryError(f"{ctx} references unknown agent {tok!r}")
        if tok not in member_set:
            raise DirectoryError(f"{ctx} agent {tok!r} is not a room member")
        if tok not in seen:
            seen.add(tok)
            explicit.append(tok)
    if wildcard:
        return list(members_order)
    return explicit


def normalize_directory(parsed: Any) -> dict[str, Any]:
    """Validate parsed TOML and return the normalized directory body.

    The returned dict carries ``schema_version``, ``agents`` and ``rooms``
    (wildcards expanded). ``source_sha256`` / ``imported_at`` are stamped by
    the caller. Raises :class:`DirectoryError` on any rule violation.
    """
    if not isinstance(parsed, dict):
        raise DirectoryError("directory TOML must be a table")
    _check_unknown_keys(parsed, _TOP_KEYS, "directory")

    sv = parsed.get("schema_version")
    if isinstance(sv, bool) or not isinstance(sv, int) or sv != SCHEMA_VERSION:
        raise DirectoryError(f"schema_version must be {SCHEMA_VERSION}, got {sv!r}")

    raw_agents = parsed.get("agents", [])
    if not isinstance(raw_agents, list):
        raise DirectoryError("agents must be an array")
    agents: list[dict[str, str]] = []
    names: set[str] = set()
    app_ids: set[str] = set()
    bot_ids: set[str] = set()
    for i, raw in enumerate(raw_agents):
        ctx = f"agents[{i}]"
        if not isinstance(raw, dict):
            raise DirectoryError(f"{ctx} must be a table")
        _check_unknown_keys(raw, _AGENT_KEYS, ctx)
        name = _require_slug(raw.get("name"), f"{ctx}.name")
        app_id = _require_id(raw.get("app_id"), f"{ctx}.app_id")
        bot_id = _require_id(raw.get("bot_user_id"), f"{ctx}.bot_user_id")
        if name in names:
            raise DirectoryError(f"duplicate agent name {name!r}")
        if app_id in app_ids:
            raise DirectoryError(f"duplicate app_id {app_id!r}")
        if bot_id in bot_ids:
            raise DirectoryError(f"duplicate bot_user_id {bot_id!r}")
        names.add(name)
        app_ids.add(app_id)
        bot_ids.add(bot_id)
        agents.append({"name": name, "app_id": app_id, "bot_user_id": bot_id})

    agent_order = [a["name"] for a in agents]
    known = set(agent_order)

    raw_rooms = parsed.get("rooms", [])
    if not isinstance(raw_rooms, list):
        raise DirectoryError("rooms must be an array")
    rooms: list[dict[str, Any]] = []
    room_names: set[str] = set()
    tc_pairs: set[tuple[str, str]] = set()
    for i, raw in enumerate(raw_rooms):
        ctx = f"rooms[{i}]"
        if not isinstance(raw, dict):
            raise DirectoryError(f"{ctx} must be a table")
        _check_unknown_keys(raw, _ROOM_KEYS, ctx)
        name = _require_slug(raw.get("name"), f"{ctx}.name")
        team_id = _require_id(raw.get("team_id"), f"{ctx}.team_id")
        channel_id = _require_id(raw.get("channel_id"), f"{ctx}.channel_id")
        if name in room_names:
            raise DirectoryError(f"duplicate room name {name!r}")
        pair = (team_id, channel_id)
        if pair in tc_pairs:
            raise DirectoryError(f"duplicate (team_id, channel_id) pair {pair!r}")
        room_names.add(name)
        tc_pairs.add(pair)

        members = _expand_members(raw.get("members", []), known, agent_order, f"{ctx}.members")
        member_set = set(members)
        ambient = _resolve_wake(
            raw.get("ambient_wake", []), known, member_set, members,
            allow_wildcard=False, ctx=f"{ctx}.ambient_wake",
        )
        mention = _resolve_wake(
            raw.get("mention_wake", []), known, member_set, members,
            allow_wildcard=True, ctx=f"{ctx}.mention_wake",
        )
        rooms.append({
            "name": name,
            "team_id": team_id,
            "channel_id": channel_id,
            "members": members,
            "ambient_wake": ambient,
            "mention_wake": mention,
        })

    body: dict[str, Any] = {"schema_version": SCHEMA_VERSION, "agents": agents, "rooms": rooms}

    # dm_allowed_humans (D-DM2, Phase 4): optional directory-wide DM allowlist.
    # The absent-vs-present distinction is load-bearing and fail-closed — the Go
    # loader reads it as a *[]string (nil = key absent = all workspace humans
    # allowed; non-nil, even empty = allowlist mode where an empty list allows
    # nobody). So the key is emitted ONLY when present in the source, preserving
    # that distinction across the JSON round-trip.
    if "dm_allowed_humans" in parsed:
        raw_allow = parsed.get("dm_allowed_humans")
        if not isinstance(raw_allow, list):
            raise DirectoryError("dm_allowed_humans must be an array of Slack user ids")
        allow: list[str] = []
        seen_allow: set[str] = set()
        for tok in raw_allow:
            if not isinstance(tok, str) or not tok:
                raise DirectoryError(
                    "dm_allowed_humans entries must be non-empty strings")
            if tok not in seen_allow:
                seen_allow.add(tok)
                allow.append(tok)
        body["dm_allowed_humans"] = allow

    return body


# --- atomic JSON writer ---------------------------------------------------

def _atomic_write_json(path: pathlib.Path, obj: dict[str, Any]) -> None:
    """Atomically write ``obj`` as pretty JSON: an unpredictable temp file in
    the destination dir, fchmod 0600, fsync, then ``os.replace``.

    Security hardening (matches the Go side's os.CreateTemp/O_NOFOLLOW bar):

      * ``tempfile.mkstemp`` opens the temp file with O_EXCL and a random
        suffix, so a pre-planted symlink at a predictable temp name can
        neither be followed nor collided with (CWE-59/CWE-377).
      * The destination is ``lstat``-checked first; a symlinked destination
        is refused so the final rename can never redirect the write onto an
        attacker-chosen victim file.

    On any failure before the rename lands, the temp file is removed and the
    existing destination is left byte-for-byte untouched.
    """
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)

    # Refuse a symlinked destination (lstat does not follow the link).
    try:
        dest_info = os.lstat(path)
    except FileNotFoundError:
        dest_info = None
    if dest_info is not None and stat.S_ISLNK(dest_info.st_mode):
        raise DirectoryError(
            f"refusing to write {path}: destination is a symlink")

    data = (json.dumps(obj, indent=2, sort_keys=True) + "\n").encode("utf-8")
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    tmp = pathlib.Path(tmp_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    try:
        dfd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError:
        pass


# --- registry loaders -----------------------------------------------------

def _read_registry_bytes(path: pathlib.Path, label: str) -> bytes:
    """Read a registry file, refusing symlinks and capping the size.

    Mirrors the Go loader's ``readCompanyRegistryBytes`` on the same two
    files: an ``lstat`` symlink rejection (a symlinked registry cannot
    redirect the read to an attacker-chosen file) plus a
    ``_MAX_REGISTRY_BYTES`` cap (an oversized file is a hard error, never a
    slurp-into-memory OOM). ``O_NOFOLLOW`` closes the lstat/open TOCTOU on
    the final path component.
    """
    try:
        info = os.lstat(path)
    except OSError as exc:
        raise DirectoryError(f"cannot read {label} {path}: {exc}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise DirectoryError(f"{label} {path} is a symlink; refusing to read")
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as fh:
            data = fh.read(_MAX_REGISTRY_BYTES + 1)
    except OSError as exc:
        raise DirectoryError(f"cannot read {label} {path}: {exc}") from exc
    if len(data) > _MAX_REGISTRY_BYTES:
        raise DirectoryError(
            f"{label} {path} exceeds {_MAX_REGISTRY_BYTES} bytes")
    return data


def load_directory() -> dict[str, Any]:
    path = company_directory_path()
    if not path.exists():
        raise DirectoryError(
            f"no company directory at {path}; run `gc slack import-company-directory` first")
    raw = _read_registry_bytes(path, "company directory")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DirectoryError(f"cannot read company directory {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise DirectoryError(f"company directory {path} is malformed (not an object)")
    return data


def load_bindings() -> dict[str, Any]:
    path = company_bindings_path()
    if not path.exists():
        return {"schema_version": SCHEMA_VERSION, "bindings": []}
    raw = _read_registry_bytes(path, "company bindings")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DirectoryError(f"cannot read company bindings {path}: {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("bindings"), list):
        raise DirectoryError(f"company bindings {path} is malformed")
    return data


def load_dm_bindings() -> dict[str, Any]:
    """Read ``dm_bindings.json`` (per-agent singleton DM bindings).

    Mirrors :func:`load_bindings`: a missing file is the empty registry.
    ``schema_version`` is honored (rejected when != 1) so Python fails closed on
    the SAME unsupported document the Go loader rejects (m10) rather than
    silently driving the spoof guard from bytes Go refuses. ``dm_bindings`` is
    the sibling of ``company_bindings`` in the same state dir.
    """
    path = company_dm_bindings_path()
    if not path.exists():
        return {"schema_version": SCHEMA_VERSION, "dm_bindings": []}
    raw = _read_registry_bytes(path, "dm bindings")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DirectoryError(f"cannot read dm bindings {path}: {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("dm_bindings"), list):
        raise DirectoryError(f"dm bindings {path} is malformed")
    if data.get("schema_version") != SCHEMA_VERSION:
        raise DirectoryError(
            f"dm bindings {path} has unsupported schema_version "
            f"{data.get('schema_version')!r} (want {SCHEMA_VERSION})")
    return data


def _canonical_dm_session(session: str) -> str:
    """Collapse a session name to the form gc actually runs (dot -> dunder).

    gc sanitizes a configured named session by replacing every dot with a double
    underscore (config ``teams.it`` runs as ``GC_SESSION_NAME=teams__it``), so a
    config-form ``a.b`` and a runtime-form ``a__b`` name the same session. This
    is the shared cross-language DM-binding collision key (the Go loader's
    ``canonicalSessionKey`` normalizes identically), so two alias-equivalent
    spellings can never bind two different agents to one running session (m5).
    """
    return (session or "").replace(".", "__")


# --- membership verification (best-effort) --------------------------------

def _switchboard_token() -> str:
    """Resolve the switchboard bot token, loading the adapter env if needed."""
    common._maybe_load_adapter_env()
    return os.environ.get("SLACK_BOT_TOKEN", "").strip()


def _slack_api_base() -> str:
    return os.environ.get("SLACK_API_BASE_URL", DEFAULT_SLACK_API_BASE).rstrip("/")


def _slack_api_call(method: str, params: dict[str, Any], token: str, api_base: str) -> dict[str, Any]:
    """GET a Slack Web API method. Returns the parsed JSON body.

    Raises :class:`MembershipCheckError` on any transport/decoding failure.
    """
    url = f"{api_base}/{method}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        raise MembershipCheckError(f"{method} -> HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise MembershipCheckError(f"{method} failed: {exc}") from exc
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise MembershipCheckError(f"{method}: non-JSON response") from exc
    if not isinstance(parsed, dict):
        raise MembershipCheckError(f"{method}: response is not an object")
    return parsed


def _channel_members(channel_id: str, token: str, api_base: str) -> set[str]:
    """Fetch the full member set for a channel, following pagination."""
    members: set[str] = set()
    cursor = ""
    for _ in range(50):  # bounded; ~10k members at 200/page
        params: dict[str, Any] = {"channel": channel_id, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        resp = _slack_api_call("conversations.members", params, token, api_base)
        if not resp.get("ok"):
            raise MembershipCheckError(
                f"conversations.members error: {resp.get('error', 'unknown')}")
        members.update(resp.get("members") or [])
        cursor = ((resp.get("response_metadata") or {}).get("next_cursor") or "").strip()
        if not cursor:
            break
    return members


def verify_memberships(directory: dict[str, Any]) -> list[str]:
    """Best-effort switchboard/member presence check. Returns warnings only.

    Never raises: a missing token, a missing scope, an API error, or an
    unreachable Slack all degrade to a warning so a membership problem can
    never fail an import or a `peers` report.
    """
    warnings: list[str] = []
    rooms = directory.get("rooms") or []
    if not rooms:
        return warnings
    token = _switchboard_token()
    if not token:
        warnings.append("membership verification skipped: SLACK_BOT_TOKEN not available")
        return warnings
    api_base = _slack_api_base()
    agents_by_name = {a["name"]: a for a in directory.get("agents", [])}

    for room in rooms:
        name = room.get("name", "?")
        channel_id = room.get("channel_id", "")
        try:
            info = _slack_api_call("conversations.info", {"channel": channel_id}, token, api_base)
        except MembershipCheckError as exc:
            warnings.append(f"room {name}: switchboard membership check failed: {exc}")
            continue
        if not info.get("ok"):
            warnings.append(
                f"room {name}: conversations.info error: {info.get('error', 'unknown')}")
            continue
        channel = info.get("channel") or {}
        if not channel.get("is_member", False):
            warnings.append(
                f"room {name} ({channel_id}): switchboard bot is not a member; room is inert")

        try:
            present = _channel_members(channel_id, token, api_base)
        except MembershipCheckError as exc:
            warnings.append(f"room {name}: member check failed: {exc}")
            continue
        for agent_name in room.get("members", []):
            agent = agents_by_name.get(agent_name)
            if agent is None:
                continue
            if agent["bot_user_id"] not in present:
                warnings.append(
                    f"room {name}: agent {agent_name} bot {agent['bot_user_id']} is not a member")
    return warnings


# --- subcommand: import-company-directory ---------------------------------

def cmd_import(args: argparse.Namespace) -> int:
    src = pathlib.Path(args.file)
    try:
        raw_bytes = src.read_bytes()
    except OSError as exc:
        raise DirectoryError(f"cannot read {src}: {exc}") from exc
    try:
        parsed = tomllib.loads(raw_bytes.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise DirectoryError(f"{src} is not valid UTF-8: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise DirectoryError(f"{src} is not valid TOML: {exc}") from exc

    # Full validation happens here, before any write, so an invalid input
    # leaves any existing registry file untouched.
    body = normalize_directory(parsed)
    body["source_sha256"] = hashlib.sha256(raw_bytes).hexdigest()
    body["imported_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    dest = company_directory_path()
    _atomic_write_json(dest, body)

    # Membership verification is best-effort and never affects the import.
    warnings = verify_memberships(body)
    for warning in warnings:
        print(f"warning: {warning}", file=sys.stderr)

    print(json.dumps({
        "directory_path": str(dest),
        "schema_version": body["schema_version"],
        "source_sha256": body["source_sha256"],
        "imported_at": body["imported_at"],
        "agents": len(body["agents"]),
        "rooms": [r["name"] for r in body["rooms"]],
        "membership_warnings": warnings,
    }, indent=2))
    return 0


# --- subcommand: bind-company-agent ---------------------------------------

def cmd_bind(args: argparse.Namespace) -> int:
    room = args.room.strip()
    agent = args.agent.strip()
    session = args.session.strip()
    city = (getattr(args, "city", "") or "").strip()
    if not session:
        raise DirectoryError("--session must be non-empty")
    # City is optional; when present it is interpolated into
    # /v0/city/{city}/... URLs by the adapter, so URL-significant or
    # whitespace bytes fail closed (mirrors the Go loader's rule).
    if any(ch in city for ch in "/?#% \t"):
        raise DirectoryError(
            f"--city {city!r} contains URL-significant or whitespace characters")

    directory = load_directory()
    room_names = {r.get("name") for r in directory.get("rooms", [])}
    agent_names = {a.get("name") for a in directory.get("agents", [])}
    if room not in room_names:
        raise DirectoryError(f"unknown room {room!r} (not in the company directory)")
    if agent not in agent_names:
        raise DirectoryError(f"unknown agent {agent!r} (not in the company directory)")

    # Non-fatal nicety: flag binding an agent that isn't a member of the room.
    for r in directory.get("rooms", []):
        if r.get("name") == room and agent not in (r.get("members") or []):
            print(
                f"warning: agent {agent!r} is not a member of room {room!r}; "
                f"it will never wake there",
                file=sys.stderr,
            )
            break

    bindings = load_bindings()
    entries: list[dict[str, str]] = bindings["bindings"]

    # A session is a single agent's identity in a room: the delivery worker
    # keys the current-turn pointer by <session>.json, so two agents sharing
    # one session in the same room would clobber each other's pointer and the
    # recorded identity would depend on wake order. Reject binding a session
    # that is already bound to a DIFFERENT agent in the same room.
    for entry in entries:
        if (
            entry.get("room") == room
            and entry.get("session") == session
            and (entry.get("city") or "") == city
            and entry.get("agent") != agent
        ):
            # Same (session, city) pair only — an identical session NAME in a
            # different city is a different session (city-qualified bindings).
            raise DirectoryError(
                f"session {session!r} (city {city or 'own'!r}) is already "
                f"bound to agent {entry.get('agent')!r} in room {room!r}; a "
                "session may bind only one agent per room")

    action = "created"
    for entry in entries:
        if entry.get("room") == room and entry.get("agent") == agent:
            # The (room, agent) binding already exists: "replaced" when the
            # session or city differs, "unchanged" when identical — never
            # "created", so an operator scripting on the action can't
            # misfire first-time side effects on an idempotent re-run.
            unchanged = (entry.get("session") == session
                         and (entry.get("city") or "") == city)
            action = "unchanged" if unchanged else "replaced"
            entry["session"] = session
            if city:
                entry["city"] = city
            else:
                entry.pop("city", None)
            break
    else:
        new_entry = {"room": room, "agent": agent, "session": session}
        if city:
            new_entry["city"] = city
        entries.append(new_entry)
    entries.sort(key=lambda e: (e.get("room", ""), e.get("agent", "")))

    dest = company_bindings_path()
    _atomic_write_json(dest, {"schema_version": SCHEMA_VERSION, "bindings": entries})

    print(json.dumps({
        "bindings_path": str(dest),
        "room": room,
        "agent": agent,
        "session": session,
        **({"city": city} if city else {}),
        "action": action,
        "total_bindings": len(entries),
    }, indent=2))
    return 0


# --- subcommand: bind-company-dm ------------------------------------------

def cmd_bind_dm(args: argparse.Namespace) -> int:
    """Record the singleton ``agent -> (session, city)`` DM binding, or unbind.

    Mirrors :func:`cmd_bind` (the identical read-validate-write mechanism and
    ``(session, city)`` guard the room bindings use) but keyed by agent alone:
    each agent has exactly one DM-bound session (D-DM1). ``city`` empty/absent
    means the adapter's own city, matching ``company_bindings``. ``--remove
    <agent>`` deletes an agent's binding (the unbind recovery path, D-DM1 /
    test-plan item 6); it does not require the agent to still be in the directory
    so a stale row left by a rename can be cleared.
    """
    remove = (getattr(args, "remove", "") or "").strip()
    if remove:
        return _cmd_unbind_dm(remove)

    agent = (getattr(args, "agent", "") or "").strip()
    session = (getattr(args, "session", "") or "").strip()
    city = (getattr(args, "city", "") or "").strip()
    if not agent:
        raise DirectoryError("--agent is required (or use --remove <agent> to unbind)")
    if not session:
        raise DirectoryError("--session must be non-empty")
    # City is optional; when present it is interpolated into
    # /v0/city/{city}/... URLs by the adapter, so URL-significant or whitespace
    # bytes fail closed (identical rule to bind-company-agent / the Go loader).
    if any(ch in city for ch in "/?#% \t"):
        raise DirectoryError(
            f"--city {city!r} contains URL-significant or whitespace characters")

    directory = load_directory()
    agent_names = {a.get("name") for a in directory.get("agents", [])}
    if agent not in agent_names:
        raise DirectoryError(f"unknown agent {agent!r} (not in the company directory)")

    dm_bindings = load_dm_bindings()
    entries: list[dict[str, str]] = dm_bindings["dm_bindings"]

    # (session, city) guard: a DM session is one agent's identity, and the
    # delivery worker keys the DM current-turn pointer by the session name, so
    # two agents sharing one running session could reply as each other. Reject
    # binding a (session, city) a DIFFERENT agent already claims, comparing the
    # gc-runtime CANONICAL session (dot->dunder) so alias-equivalent spellings
    # cannot slip past (m5, matching the Go loader). A row whose agent is no
    # longer in the directory is dropped by every read surface (Go loader,
    # cmd_peers), so it must not block a live binding either (m11).
    canon = _canonical_dm_session(session)
    for entry in entries:
        e_agent = entry.get("agent")
        if e_agent == agent or e_agent not in agent_names:
            continue
        if (
            _canonical_dm_session(entry.get("session") or "") == canon
            and (entry.get("city") or "") == city
        ):
            raise DirectoryError(
                f"session {session!r} (city {city or 'own'!r}) is already bound "
                f"to DM agent {e_agent!r}; a session may DM-bind only "
                "one agent")

    action = "created"
    for entry in entries:
        if entry.get("agent") == agent:
            # Singleton per agent: the existing binding is replaced when the
            # session or city differs, unchanged when identical.
            unchanged = (entry.get("session") == session
                         and (entry.get("city") or "") == city)
            action = "unchanged" if unchanged else "replaced"
            entry["session"] = session
            if city:
                entry["city"] = city
            else:
                entry.pop("city", None)
            break
    else:
        new_entry = {"agent": agent, "session": session}
        if city:
            new_entry["city"] = city
        entries.append(new_entry)
    entries.sort(key=lambda e: e.get("agent", ""))

    dest = company_dm_bindings_path()
    _atomic_write_json(dest, {"schema_version": SCHEMA_VERSION, "dm_bindings": entries})

    print(json.dumps({
        "dm_bindings_path": str(dest),
        "agent": agent,
        "session": session,
        **({"city": city} if city else {}),
        "action": action,
        "total_dm_bindings": len(entries),
    }, indent=2))
    return 0


def _cmd_unbind_dm(agent: str) -> int:
    """Remove ``agent``'s DM binding (the unbind recovery path).

    The agent need NOT be in the directory: the whole point is to clear a stale
    row whose agent was renamed/removed, which the (session, city) guard would
    otherwise treat as a live conflict blocking the replacement binding.
    """
    dm_bindings = load_dm_bindings()
    entries: list[dict[str, str]] = dm_bindings["dm_bindings"]
    remaining = [e for e in entries if e.get("agent") != agent]
    if len(remaining) == len(entries):
        raise DirectoryError(f"no DM binding for agent {agent!r} to remove")
    remaining.sort(key=lambda e: e.get("agent", ""))

    dest = company_dm_bindings_path()
    _atomic_write_json(dest, {"schema_version": SCHEMA_VERSION, "dm_bindings": remaining})

    print(json.dumps({
        "dm_bindings_path": str(dest),
        "agent": agent,
        "action": "removed",
        "total_dm_bindings": len(remaining),
    }, indent=2))
    return 0


# --- subcommand: peers ----------------------------------------------------

def cmd_peers(args: argparse.Namespace) -> int:
    directory = load_directory()
    bindings = load_bindings()

    room_names = {r.get("name") for r in directory.get("rooms", [])}
    agent_names = {a.get("name") for a in directory.get("agents", [])}

    # Index bindings per room, dropping any that reference a room/agent no
    # longer in the directory (surfaced as a warning — reader-side rule).
    per_room: dict[str, list[dict[str, str]]] = {}
    binding_warnings: list[str] = []
    for entry in bindings.get("bindings", []):
        b_room = entry.get("room", "")
        b_agent = entry.get("agent", "")
        if b_room not in room_names or b_agent not in agent_names:
            binding_warnings.append(
                f"binding room={b_room!r} agent={b_agent!r} references a name not in "
                f"the directory; dropped")
            continue
        per_room.setdefault(b_room, []).append(
            {"agent": b_agent, "session": entry.get("session", "")})

    # DM bindings are per-agent singletons (no room); drop any referencing an
    # agent no longer in the directory (surfaced as a warning, reader-side rule).
    dm_bindings = load_dm_bindings()
    dm_report: list[dict[str, str]] = []
    for entry in dm_bindings.get("dm_bindings", []):
        d_agent = entry.get("agent", "")
        if d_agent not in agent_names:
            binding_warnings.append(
                f"dm binding agent={d_agent!r} references a name not in the "
                f"directory; dropped")
            continue
        rec: dict[str, str] = {"agent": d_agent, "session": entry.get("session", "")}
        if entry.get("city"):
            rec["city"] = entry["city"]
        dm_report.append(rec)
    dm_report.sort(key=lambda b: b["agent"])

    rooms = directory.get("rooms", [])
    if args.room:
        rooms = [r for r in rooms if r.get("name") == args.room]
        if not rooms:
            raise DirectoryError(f"unknown room {args.room!r} (not in the company directory)")

    membership_warnings = verify_memberships({
        "agents": directory.get("agents", []),
        "rooms": rooms,
    })

    report_rooms = []
    for r in rooms:
        name = r.get("name", "")
        report_rooms.append({
            "name": name,
            "team_id": r.get("team_id", ""),
            "channel_id": r.get("channel_id", ""),
            "members": r.get("members", []),
            "ambient_wake": r.get("ambient_wake", []),
            "mention_wake": r.get("mention_wake", []),
            "bindings": sorted(per_room.get(name, []), key=lambda b: b["agent"]),
        })

    print(json.dumps({
        "rooms": report_rooms,
        "dm_bindings": dm_report,
        "membership_warnings": membership_warnings,
        "binding_warnings": binding_warnings,
    }, indent=2, sort_keys=True))
    return 0


# --- entry point ----------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="slack_company_directory",
        description="Company-directory CLI surface for the slack-full pack.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_import = sub.add_parser(
        "import-company-directory",
        help="Validate and atomically import a company directory TOML.",
    )
    p_import.add_argument("--file", required=True, help="Path to the rooms.toml directory file")
    p_import.set_defaults(func=cmd_import)

    p_bind = sub.add_parser(
        "bind-company-agent",
        help="Record the singleton (room, agent) -> session binding.",
    )
    p_bind.add_argument("--room", required=True, help="Directory room name (slug)")
    p_bind.add_argument("--agent", required=True, help="Directory agent name (slug)")
    p_bind.add_argument("--session", required=True, help="gc session name to bind")
    p_bind.add_argument(
        "--city", default="",
        help="gc city hosting the session when it differs from the adapter's "
             "own city (city-qualified binding; the adapter needs a matching "
             "SLACK_COMPANY_CITY_APIS entry)")
    p_bind.set_defaults(func=cmd_bind)

    p_bind_dm = sub.add_parser(
        "bind-company-dm",
        help="Record (or --remove) the singleton agent -> session per-agent DM "
             "binding.",
    )
    p_bind_dm.add_argument("--agent", default="", help="Directory agent name (slug)")
    p_bind_dm.add_argument("--session", default="", help="gc session name to DM-bind")
    p_bind_dm.add_argument(
        "--city", default="",
        help="gc city hosting the session when it differs from the adapter's "
             "own city (city-qualified binding; the adapter needs a matching "
             "SLACK_COMPANY_CITY_APIS entry)")
    p_bind_dm.add_argument(
        "--remove", default="", metavar="AGENT",
        help="Unbind: remove AGENT's DM binding. The agent need not still be in "
             "the directory, so a stale row left by a rename can be cleared "
             "(the documented redrive-recovery path).")
    p_bind_dm.set_defaults(func=cmd_bind_dm)

    p_peers = sub.add_parser(
        "peers",
        help="Report rooms, members, wake policy, bindings, and membership warnings.",
    )
    p_peers.add_argument("--room", default="", help="Limit the report to one room")
    p_peers.set_defaults(func=cmd_peers)

    return parser


def main(argv: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except DirectoryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
