#!/usr/bin/env python3
"""Company-rooms operator verbs for the slack-full pack (company rooms 3c).

Two receipt-native operator surfaces layered on the adapter's internal
listener:

  * ``gc slack company-status`` — one view of a wedged flow end to end:
    Python-owned reads (delegation records grouped by synthesis group with
    S10-normalized snapshots, plus stale ``posting`` intents) joined with the
    adapter's receipt/park/target state (``GET /internal/company/receipts``).
  * ``gc slack company-redrive`` — reset selected receipt targets (or a
    target-less parked receipt) and re-trigger delivery
    (``POST /internal/company/redrive``), reusing the recorded idempotency
    keys — never reposting to Slack.

The receipt store has exactly one writer (Go, generation-CAS), so these verbs
are thin clients: they never rewrite receipts. The internal listener is a UDS
(``GC_SERVICE_SOCKET``, primary in gc's proxy_process mode — ``LISTEN_INTERNAL``
is ignored when it is set) or a loopback TCP address. A base-URL string cannot
express the UDS case, so the client is a connection factory:
:func:`internal_connection` returns an :class:`http.client.HTTPConnection`
subclass that dials ``GC_SERVICE_SOCKET`` when set, else TCP to
``LISTEN_INTERNAL`` (default ``127.0.0.1:8766``). ``GC_SERVICE_SOCKET`` wins.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import socket
import sys
import time
from typing import Any

import slack_company_outbound as outbound

DEFAULT_INTERNAL_LISTEN = "127.0.0.1:8766"

# 409 = the receipt's single-flight is held elsewhere (a concurrent delivery or
# redrive); the verb retries a bounded number of times.
_REDRIVE_MAX_RETRIES = 5
_REDRIVE_RETRY_DELAY = 0.25

# Seam so tests can drive the 409 retry loop without real sleeping.
_sleep = time.sleep


class AdminError(RuntimeError):
    """Raised on invalid operator input or an unrecoverable admin failure."""


# --- internal listener client (UDS / TCP) ----------------------------------

class _UnixHTTPConnection(http.client.HTTPConnection):
    """``HTTPConnection`` that dials a Unix domain socket instead of TCP."""

    def __init__(self, socket_path: str, *, timeout: float | None = None) -> None:
        eff = timeout if timeout is not None else socket._GLOBAL_DEFAULT_TIMEOUT
        super().__init__("localhost", timeout=eff)
        self._unix_socket_path = socket_path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if self.timeout is not None and self.timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
            sock.settimeout(self.timeout)
        sock.connect(self._unix_socket_path)
        self.sock = sock


def internal_connection(*, timeout: float = 15.0) -> http.client.HTTPConnection:
    """Factory for a connection to the adapter's internal listener.

    ``GC_SERVICE_SOCKET`` (a UDS path) wins when set; otherwise TCP to
    ``LISTEN_INTERNAL`` (default ``127.0.0.1:8766``). The connection is lazy —
    it does not dial until the first request is issued.
    """
    socket_path = os.environ.get("GC_SERVICE_SOCKET", "").strip()
    if socket_path:
        return _UnixHTTPConnection(socket_path, timeout=timeout)
    addr = os.environ.get("LISTEN_INTERNAL", "").strip() or DEFAULT_INTERNAL_LISTEN
    host, sep, port = addr.rpartition(":")
    if not sep:
        host, port = addr, "8766"
    try:
        port_num = int(port)
    except ValueError as exc:
        raise AdminError(f"LISTEN_INTERNAL {addr!r} is not host:port: {exc}") from exc
    return http.client.HTTPConnection(host or "127.0.0.1", port_num, timeout=timeout)


def _internal_request(
    method: str, path: str, body: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    """Issue one request to the internal listener; return (status, parsed_json)."""
    conn = internal_connection()
    try:
        headers = {"Accept": "application/json"}
        payload = None
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        conn.request(method, path, body=payload, headers=headers)
        resp = conn.getresponse()
        status = resp.status
        raw = resp.read()
    finally:
        conn.close()
    data: Any = None
    if raw:
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            data = None
    return status, data


# --- argument parsing helpers ----------------------------------------------

def _parse_triple(value: str, *, what: str) -> tuple[str, str, str]:
    parts = value.split(":")
    if len(parts) != 3 or not all(parts):
        raise AdminError(
            f"--{what} must be <team>:<channel>:<ts>, got {value!r}")
    return parts[0], parts[1], parts[2]


def _extract_receipts(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict) and isinstance(data.get("receipts"), list):
        return [r for r in data["receipts"] if isinstance(r, dict)]
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    return []


# --- company-status --------------------------------------------------------

def _collect_local_state(
    *, root_filter: tuple[str, str, str] | None,
    origin_filter: tuple[str, str, str] | None,
) -> dict[str, Any]:
    """Python-owned reads: delegation groups (with normalized snapshots) and
    stale ``posting`` intents. Snapshot scans tolerate concurrent deletion."""
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    ungrouped: list[dict[str, Any]] = []
    for path, record in outbound.list_delegations():
        triple = (record.get("team_id"), record.get("channel_id"), record.get("thread_root_ts"))
        if root_filter is not None and triple != root_filter:
            continue
        origin_triple = (record.get("team_id"), record.get("channel_id"), record.get("ts"))
        if origin_filter is not None and origin_triple != origin_filter:
            continue
        summary = {
            "delegation_key": path.name,
            "ts": record.get("ts"),
            "status": record.get("status"),
            "room": record.get("room"),
            "expected_responder_agent": record.get("expected_responder_agent"),
            "requester_agent": record.get("requester_agent"),
            "synthesis": outbound.synthesis_state(record),
        }
        grp = outbound.synthesis_group(record)
        if grp is None:
            ungrouped.append(summary)
        else:
            groups.setdefault(grp, []).append(summary)

    group_list: list[dict[str, Any]] = []
    for grp in sorted(groups):
        dels = sorted(groups[grp], key=lambda d: (d.get("ts") or ""))
        group_list.append({
            "group": list(grp),
            "room": dels[0].get("room"),
            "delegations": dels,
        })

    now = outbound._now()
    stale_intents: list[dict[str, Any]] = []
    for intent in outbound.list_intents():
        if intent.get("status") != "posting":
            continue
        deadline = outbound._parse_rfc3339(intent.get("retry_deadline", ""))
        if deadline is None or now <= deadline:
            continue
        if root_filter is not None and (
            intent.get("team_id"), intent.get("channel_id"), intent.get("human_root_ts"),
        ) != root_filter:
            continue
        stale_intents.append({
            "nonce": intent.get("nonce"),
            "op": intent.get("op"),
            "target_agent": intent.get("target_agent"),
            "retry_deadline": intent.get("retry_deadline"),
            "age_seconds": int((now - deadline).total_seconds()),
        })

    return {
        "groups": group_list,
        "ungrouped_delegations": ungrouped,
        "stale_posting_intents": stale_intents,
    }


def cmd_company_status(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="slack company-status")
    parser.add_argument("--receipt", default="", help="Scope receipts to one receipt id")
    parser.add_argument("--origin", default="", help="<team>:<channel>:<ts>")
    parser.add_argument("--root", default="", help="<team>:<channel>:<root_ts>")
    parser.add_argument("--status", default="", help="Filter receipts by status")
    args = parser.parse_args(argv)

    origin_filter = _parse_triple(args.origin, what="origin") if args.origin else None
    root_filter = _parse_triple(args.root, what="root") if args.root else None

    local = _collect_local_state(root_filter=root_filter, origin_filter=origin_filter)

    params: list[tuple[str, str]] = []
    if args.origin:
        params.append(("origin", args.origin))
    if args.root:
        params.append(("root", args.root))
    if args.status:
        params.append(("status", args.status))
    path = "/internal/company/receipts"
    if params:
        import urllib.parse
        path += "?" + urllib.parse.urlencode(params)

    receipts: list[dict[str, Any]] = []
    receipts_error: str | None = None
    try:
        status, data = _internal_request("GET", path)
        if status == 200:
            receipts = _extract_receipts(data)
        else:
            receipts_error = f"receipts endpoint returned HTTP {status}"
    except OSError as exc:
        receipts_error = f"adapter internal listener unreachable: {exc}"

    if args.receipt:
        receipts = [r for r in receipts if r.get("id") == args.receipt]

    out: dict[str, Any] = dict(local)
    out["receipts"] = receipts
    if receipts_error:
        out["receipts_error"] = receipts_error
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


# --- company-redrive -------------------------------------------------------

def _redrive_reason(data: Any, default: str) -> str:
    """The machine-readable reason from an error response body.

    The endpoint returns a structured ``reason`` (preferred) and/or a
    human-readable ``error``; fall back to ``default`` when neither is present.
    """
    if isinstance(data, dict):
        for key in ("reason", "error"):
            val = data.get(key)
            if isinstance(val, str) and val.strip():
                return val
    return default


def _format_unresolvable(entry: Any) -> str:
    """One readable line for a still-unbound redrive target."""
    if isinstance(entry, dict):
        who = entry.get("agent") or entry.get("session") or entry.get("target") or "?"
        detail = entry.get("reason") or entry.get("detail") or ""
        return f"{who}: {detail}" if detail else str(who)
    return str(entry)


def _warn_unresolvable_targets(unresolvable: Any) -> None:
    """Warn (to stderr) about targets a 2xx redrive could not resolve.

    Still-unbound targets are reported per-target by the endpoint alongside the
    reset it *did* apply; the redrive is a partial success, so we surface them
    clearly and leave the exit code at 0.
    """
    if not isinstance(unresolvable, list) or not unresolvable:
        return
    print(
        f"company-redrive: warning: {len(unresolvable)} target(s) still "
        "unresolvable (no current company binding); repair the binding and "
        "re-run:", file=sys.stderr)
    for entry in unresolvable:
        print(f"  - {_format_unresolvable(entry)}", file=sys.stderr)


def cmd_company_redrive(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="slack company-redrive")
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--receipt", default="", help="Receipt id to redrive")
    selector.add_argument("--origin", default="", help="<team>:<channel>:<ts>")
    parser.add_argument("--target", action="append", default=[],
                        help="Restrict to this target session (repeatable)")
    parser.add_argument("--include-failed", action="store_true",
                        help="Also redrive targets whose detail begins attempts_exhausted")
    args = parser.parse_args(argv)

    body: dict[str, Any] = {
        "targets": list(args.target),
        "include_failed": bool(args.include_failed),
    }
    if args.receipt:
        body["receipt"] = args.receipt
    else:
        team, channel, ts = _parse_triple(args.origin, what="origin")
        body["origin"] = {"team_id": team, "channel_id": channel, "ts": ts}

    attempt = 0
    while True:
        status, data = _internal_request("POST", "/internal/company/redrive", body)
        if status == 200:
            print(json.dumps(data if data is not None else {}, indent=2, sort_keys=True))
            # Partial success: the reset landed, but some targets are still
            # unbound. Surface them and keep exit 0.
            _warn_unresolvable_targets(data.get("unresolvable") if isinstance(data, dict) else None)
            return 0
        if status == 404:
            target = args.receipt or args.origin
            print(
                f"company-redrive: receipt {target!r} is gone — terminal and "
                "swept past the 7-day retention horizon; nothing to redrive",
                file=sys.stderr)
            return 1
        if status == 409:
            attempt += 1
            if attempt <= _REDRIVE_MAX_RETRIES:
                _sleep(_REDRIVE_RETRY_DELAY)
                continue
            print(
                "company-redrive: the receipt's single-flight is held elsewhere "
                f"(HTTP 409) after {_REDRIVE_MAX_RETRIES} retries; try again",
                file=sys.stderr)
            return 1
        if status == 422:
            # The effective selection was empty (no eligible failed target, or
            # every selected target is still unbound) — the endpoint returns a
            # machine-readable reason instead of a success-shaped no-op.
            print(
                "company-redrive: empty effective selection — "
                f"{_redrive_reason(data, 'no eligible targets to redrive')}",
                file=sys.stderr)
            return 1
        reason = _redrive_reason(data, "")
        suffix = f": {reason}" if reason else ""
        print(f"company-redrive: unexpected HTTP {status}{suffix}", file=sys.stderr)
        return 1


# --- company-redact --------------------------------------------------------

def cmd_company_redact(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="slack company-redact")
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--receipt", default="", help="Receipt id whose body to redact")
    selector.add_argument("--origin", default="", help="<team>:<channel>:<ts>")
    args = parser.parse_args(argv)

    body: dict[str, Any] = {}
    if args.receipt:
        body["receipt"] = args.receipt
    else:
        team, channel, ts = _parse_triple(args.origin, what="origin")
        body["origin"] = {"team_id": team, "channel_id": channel, "ts": ts}

    attempt = 0
    while True:
        status, data = _internal_request("POST", "/internal/company/redact", body)
        if status == 200:
            print(json.dumps(data if data is not None else {}, indent=2, sort_keys=True))
            return 0
        if status == 404:
            target = args.receipt or args.origin
            print(
                f"company-redact: receipt {target!r} is gone — terminal and "
                "swept past the 7-day retention horizon; nothing to redact",
                file=sys.stderr)
            return 1
        if status == 409:
            # Either a held single-flight or a legacy embedded receipt (no
            # separable body). The endpoint's reason distinguishes them; retry
            # only makes sense for a held claim, so surface and stop.
            reason = _redrive_reason(data, "")
            if "single-flight" in reason:
                attempt += 1
                if attempt <= _REDRIVE_MAX_RETRIES:
                    _sleep(_REDRIVE_RETRY_DELAY)
                    continue
            suffix = f": {reason}" if reason else ""
            print(f"company-redact: HTTP 409{suffix}", file=sys.stderr)
            return 1
        reason = _redrive_reason(data, "")
        suffix = f": {reason}" if reason else ""
        print(f"company-redact: unexpected HTTP {status}{suffix}", file=sys.stderr)
        return 1


# --- CLI entry point -------------------------------------------------------

def main(argv: list[str]) -> int:
    if not argv:
        print("error: expected a subcommand "
              "(company-status | company-redrive | company-redact)",
              file=sys.stderr)
        return 2
    sub, rest = argv[0], argv[1:]
    try:
        if sub == "company-status":
            return cmd_company_status(rest)
        if sub == "company-redrive":
            return cmd_company_redrive(rest)
        if sub == "company-redact":
            return cmd_company_redact(rest)
        print(f"error: unknown subcommand {sub!r}", file=sys.stderr)
        return 2
    except AdminError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except outbound.OutboundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
