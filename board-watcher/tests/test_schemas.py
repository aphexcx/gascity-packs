"""Validate own-writes ledger records against the shipped JSON Schema.

The schema in ../schema/own_writes.schema.json is the documented interop
contract between a city's board write path (gc board-record-write,
boomtown's board-post helper, any custom hook) and its board-watcher.
This test asserts that records matching the write hook's serialized shape
pass, and that the documented constraints (required keys, hash/name
patterns, tombstones) actually reject bad records — so "contract
documented" and "contract validated" stay in lockstep.

Run: python3 -m pytest board-watcher/tests/
"""

from __future__ import annotations

import json
import pathlib

import pytest
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

SCHEMA_DIR = pathlib.Path(__file__).resolve().parent.parent / "schema"

SHA = "ab" * 32


def _validator() -> Draft202012Validator:
    schema = json.loads(
        (SCHEMA_DIR / "own_writes.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def _entry(**overrides):
    entry = {
        "schema": 1,
        "file": "2026-07-29-board-watcher-approved.md",
        "sha256": SHA,
        "ts": "2026-07-30T01:00:00Z",
    }
    entry.update(overrides)
    return entry


def test_content_write_entry_passes():
    _validator().validate(_entry())


def test_deletion_tombstone_passes():
    _validator().validate(_entry(sha256="-"))


def test_extra_keys_are_forward_compatible():
    _validator().validate(_entry(note="recorded by board-post", host="boomtown"))


@pytest.mark.parametrize("missing", ["schema", "file", "sha256", "ts"])
def test_required_keys(missing):
    entry = _entry()
    del entry[missing]
    with pytest.raises(ValidationError):
        _validator().validate(entry)


@pytest.mark.parametrize(
    "sha",
    [
        SHA.upper(),  # uppercase hex is out of contract
        SHA[:-2],  # wrong length
        SHA + "ab",  # wrong length
        "",  # empty
        "--",  # not the single-dash tombstone
    ],
)
def test_bad_hashes_rejected(sha):
    with pytest.raises(ValidationError):
        _validator().validate(_entry(sha256=sha))


@pytest.mark.parametrize(
    "name",
    [
        "path/to/doc.md",  # path separator
        "../escape.md",  # traversal
        ".hidden.md",  # leading dot
        "-dash-first.md",  # option-looking
        "",  # empty
    ],
)
def test_bad_names_rejected(name):
    with pytest.raises(ValidationError):
        _validator().validate(_entry(file=name))


def test_schema_version_is_pinned():
    with pytest.raises(ValidationError):
        _validator().validate(_entry(schema=2))
