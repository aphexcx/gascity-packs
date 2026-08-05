#!/usr/bin/env python3
"""Register a per-agent Slack app's signing secret (company rooms Phase 4).

Backs the ``gc slack register-agent-app`` verb. It writes a
``{team_id, api_app_id, signing_secret}`` record into the ``agent_apps.json``
registry (schema_version'd, mode 0600), the app-bound signature-verification
store the adapter loads at startup and on SIGHUP.

This registry is SECRET-bearing (each record carries a signing secret), so it
lives apart from the non-secret ``company_directory.json`` /
``company_bindings.json`` surface: the file is written 0600 in a 0700 dir and
this verb refuses to overwrite an existing registry whose permissions have been
loosened to group/world (a possible leak the operator must investigate first).

Owner-agent identity is NOT stored here — it derives by joining the envelope's
``api_app_id`` against ``company_directory.json`` ``agents[].app_id`` (the
directory is the canonical name<->app_id<->bot_user_id source). Registering an
``api_app_id`` with no directory agent is a warning, never a failure: the app
opts into strict binding immediately, but it admits nothing until the directory
lists it.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import stat
import sys
import tempfile
from typing import Any

SCHEMA_VERSION = 1

# Slack identifier shapes. api_app_id and team_id are opaque uppercase Slack ids
# (app ids start with 'A'; team ids with 'T', or 'E' for an enterprise grid);
# signing secrets are 32 hex characters (Slack Basic Info → App Credentials).
_API_APP_ID_RE = re.compile(r"^A[A-Z0-9]{6,}$")
_TEAM_ID_RE = re.compile(r"^[TE][A-Z0-9]{6,}$")
_SIGNING_SECRET_RE = re.compile(r"^[0-9a-fA-F]{32}$")

# Cap mirrors the other company registries' maxRegistryBytes (10 MiB).
_MAX_REGISTRY_BYTES = 10 << 20


class RegisterError(RuntimeError):
    """Raised on an invalid input or a refusal to store the record."""


# --- path resolution ------------------------------------------------------
#
# Precedence mirrors the other company registries exactly: explicit env
# override > <GC_CITY_PATH>/.gc/slack/<leaf> > /tmp/gc-slack-adapter/<leaf>.

def agent_apps_path() -> pathlib.Path:
    override = os.environ.get("SLACK_COMPANY_AGENT_APPS_PATH", "").strip()
    if override:
        return pathlib.Path(override)
    city = os.environ.get("GC_CITY_PATH", "").strip()
    if city:
        return pathlib.Path(city) / ".gc" / "slack" / "agent_apps.json"
    return pathlib.Path("/tmp/gc-slack-adapter") / "agent_apps.json"


# --- validation -----------------------------------------------------------

def _validate_api_app_id(value: str) -> str:
    value = value.strip()
    if not _API_APP_ID_RE.match(value):
        raise RegisterError(
            f"--api-app-id {value!r} is not a valid Slack app id "
            "(^A[A-Z0-9]{6,}$)")
    return value


def _validate_team_id(value: str) -> str:
    value = value.strip()
    if not _TEAM_ID_RE.match(value):
        raise RegisterError(
            f"--team-id {value!r} is not a valid Slack team id "
            "(^[TE][A-Z0-9]{6,}$)")
    return value


def _validate_signing_secret(value: str) -> str:
    value = value.strip()
    if not _SIGNING_SECRET_RE.match(value):
        raise RegisterError(
            "signing secret is not a 32-character hex string "
            "(Slack Basic Info → App Credentials → Signing Secret)")
    return value


# --- registry read/write (0600, symlink- and loosened-perm-refusing) ------

def _read_agent_apps(path: pathlib.Path) -> dict[str, Any]:
    """Read the existing registry, refusing a symlink or a loosened-perm file.

    A missing file is the empty registry. An existing file wider than 0600
    (any group/world bit) is refused rather than silently overwritten: a secret
    registry that was world-readable may already have leaked, and the operator
    must investigate before this verb rewrites it.
    """
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return {"schema_version": SCHEMA_VERSION, "agent_apps": []}
    except OSError as exc:
        raise RegisterError(f"cannot stat agent apps registry {path}: {exc}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise RegisterError(f"agent apps registry {path} is a symlink; refusing")
    if not stat.S_ISREG(info.st_mode):
        raise RegisterError(f"agent apps registry {path} is not a regular file")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise RegisterError(
            f"agent apps registry {path} is group/world-accessible "
            f"(mode {stat.S_IMODE(info.st_mode):04o}); it must be 0600. This "
            "secret store may have leaked — fix the permissions and rotate the "
            "affected signing secrets before re-running")
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as fh:
            raw = fh.read(_MAX_REGISTRY_BYTES + 1)
    except OSError as exc:
        raise RegisterError(f"cannot read agent apps registry {path}: {exc}") from exc
    if len(raw) > _MAX_REGISTRY_BYTES:
        raise RegisterError(f"agent apps registry {path} exceeds {_MAX_REGISTRY_BYTES} bytes")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RegisterError(f"agent apps registry {path} is malformed: {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("agent_apps"), list):
        raise RegisterError(f"agent apps registry {path} is malformed")
    return data


def _atomic_write_0600(path: pathlib.Path, obj: dict[str, Any]) -> None:
    """Atomic tmp+fchmod(0600)+fsync+rename; symlinked dest refused, dir 0700."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        dest = os.lstat(path)
    except FileNotFoundError:
        dest = None
    if dest is not None and stat.S_ISLNK(dest.st_mode):
        raise RegisterError(f"refusing to write {path}: destination is a symlink")
    data = (json.dumps(obj, indent=2, sort_keys=True) + "\n").encode("utf-8")
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
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


# --- directory-join warning (best-effort) ---------------------------------

def _directory_join_warning(api_app_id: str) -> str | None:
    """Warn (never fail) when ``api_app_id`` matches no directory ``app_id``.

    Best-effort: a missing/unreadable directory means we cannot check the join,
    which is itself a warning — registration still succeeds so the operator can
    register a secret before importing the directory (runbook ordering).
    """
    try:
        import slack_company_directory as directory  # type: ignore
    except ImportError:
        return None
    try:
        data = directory.load_directory()
    except directory.DirectoryError:
        return (f"api_app_id {api_app_id} could not be joined to a directory "
                "agent (no readable company_directory.json); it admits nothing "
                "until the directory lists it")
    app_ids = {a.get("app_id") for a in data.get("agents", [])}
    if api_app_id not in app_ids:
        return (f"api_app_id {api_app_id} has no company_directory.json "
                "agents[].app_id match; it admits nothing until the directory "
                "lists it")
    return None


# --- verb -----------------------------------------------------------------

def register_agent_app(*, team_id: str, api_app_id: str, signing_secret: str) -> dict[str, Any]:
    team_id = _validate_team_id(team_id)
    api_app_id = _validate_api_app_id(api_app_id)
    signing_secret = _validate_signing_secret(signing_secret)

    path = agent_apps_path()
    registry = _read_agent_apps(path)
    entries: list[dict[str, str]] = registry["agent_apps"]

    action = "created"
    for entry in entries:
        if entry.get("api_app_id") == api_app_id:
            unchanged = (entry.get("team_id") == team_id
                         and entry.get("signing_secret") == signing_secret)
            action = "unchanged" if unchanged else "replaced"
            entry["team_id"] = team_id
            entry["signing_secret"] = signing_secret
            break
    else:
        entries.append({
            "team_id": team_id,
            "api_app_id": api_app_id,
            "signing_secret": signing_secret,
        })
    entries.sort(key=lambda e: e.get("api_app_id", ""))

    _atomic_write_0600(path, {"schema_version": SCHEMA_VERSION, "agent_apps": entries})

    warning = _directory_join_warning(api_app_id)
    report: dict[str, Any] = {
        "agent_apps_path": str(path),
        "team_id": team_id,
        "api_app_id": api_app_id,
        "action": action,
        "total_agent_apps": len(entries),
    }
    if warning is not None:
        report["directory_join_warning"] = warning
    return report


# --- CLI entry point ------------------------------------------------------

def _resolve_secret(args: argparse.Namespace) -> str:
    if args.signing_secret and args.signing_secret_file:
        raise RegisterError("pass --signing-secret OR --signing-secret-file, not both")
    if args.signing_secret_file:
        try:
            return pathlib.Path(args.signing_secret_file).read_text(encoding="utf-8")
        except OSError as exc:
            raise RegisterError(
                f"cannot read --signing-secret-file {args.signing_secret_file}: {exc}") from exc
    if args.signing_secret:
        return args.signing_secret
    raise RegisterError("either --signing-secret or --signing-secret-file is required")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="slack register-agent-app",
        description="Register a per-agent Slack app's signing secret for "
                    "app-bound DM signature verification.",
    )
    parser.add_argument("--team-id", required=True, help="Slack workspace team id")
    parser.add_argument("--api-app-id", required=True, help="Slack app id (Basic Info)")
    parser.add_argument("--signing-secret", default="",
                        help="32-hex signing secret (leaks via ps; prefer "
                             "--signing-secret-file)")
    parser.add_argument("--signing-secret-file", default="",
                        help="Path to a file holding the 32-hex signing secret")
    args = parser.parse_args(argv)

    try:
        secret = _resolve_secret(args)
        result = register_agent_app(
            team_id=args.team_id, api_app_id=args.api_app_id, signing_secret=secret)
    except RegisterError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if "directory_join_warning" in result:
        print(f"warning: {result['directory_join_warning']}", file=sys.stderr)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
