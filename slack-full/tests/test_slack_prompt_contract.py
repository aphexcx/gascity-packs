"""Regression tests for the company-room agent response contract."""

from pathlib import Path


FRAGMENT = (
    Path(__file__).resolve().parents[1]
    / "template-fragments"
    / "slack-v0.template.md"
)


def test_thread_ambient_response_gate_and_native_identity_contract() -> None:
    prompt = FRAGMENT.read_text(encoding="utf-8")
    normalized = " ".join(prompt.lower().split())

    assert "`thread_ambient`" in prompt
    assert "distinct case-insensitive word" in normalized
    assert "directly and strongly relevant or actionable" in normalized
    assert "otherwise, do not post" in normalized
    assert "slack already attributes every reply to your agent identity" in normalized
    assert "do not prefix the message with your name or handle" in normalized

    assert "always prefix your slack message" not in normalized
    assert "prefixed with your identity" not in normalized
    assert "single asterisks" not in normalized
