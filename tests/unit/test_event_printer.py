"""Contracts for CLI event printer formatting helpers."""

from __future__ import annotations

from types import SimpleNamespace

from factorial.cli.event_printer import _format_verification_message


def test_format_verification_passed_message_includes_attempt_budget() -> None:
    event = SimpleNamespace(data={"attempts_used": 1, "max_attempts": 3})
    message = _format_verification_message("verification_passed", event)

    assert "verification passed" in message
    assert "(1/3 attempts)" in message


def test_format_verification_rejected_message_includes_code_and_message() -> None:
    event = SimpleNamespace(
        data={
            "attempts_used": 2,
            "max_attempts": 5,
            "code": "score_low",
            "message": "Score below acceptance threshold",
        }
    )
    message = _format_verification_message("verification_rejected", event)

    assert "verification rejected" in message
    assert "(2/5 attempts)" in message
    assert "[score_low]" in message
    assert "Score below acceptance threshold" in message


def test_format_verification_exhausted_message_includes_terminal_state() -> None:
    event = SimpleNamespace(
        data={
            "attempts_used": 3,
            "max_attempts": 3,
            "code": "tests_failed",
            "message": "Output did not satisfy verifier",
        }
    )
    message = _format_verification_message("verification_exhausted", event)

    assert "verification exhausted" in message
    assert "(3/3 attempts)" in message
    assert "[tests_failed]" in message
    assert "Output did not satisfy verifier" in message
