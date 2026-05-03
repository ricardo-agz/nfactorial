from __future__ import annotations

import json
import os
import time
from functools import lru_cache
from typing import Any

import redis.asyncio as redis
from models import MafiaPlayerContext, PlayerActionResult

from factorial import ExecutionContext, WaitInstruction, messaging, signals, tool, wait
from factorial.core.utils import resolve_awaitable

DAY_PHASE_POLL_SECONDS = 5
DAY_VOTE_WARNING_POLLS_REMAINING = 2
PHASE_SIGNAL_WAIT_SECONDS = 180
MAX_DAY_MESSAGES_PER_ROUND = 3

def _set_phase(agent_ctx: MafiaPlayerContext, phase: str) -> None:
    agent_ctx.state.phase = phase


def _is_phase(agent_ctx: MafiaPlayerContext, *phases: str) -> bool:
    return str(agent_ctx.state.phase) in phases


def _coerce_round_no(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _ensure_discussion_round(agent_ctx: MafiaPlayerContext, round_no: int) -> None:
    if agent_ctx.state.discussion_round_no == round_no:
        return
    agent_ctx.state.discussion_round_no = round_no
    agent_ctx.state.discussion_messages_sent = 0
    agent_ctx.state.has_called_vote = False


def _signal_payload() -> dict[str, Any]:
    current_signal = signals.current()
    if current_signal is None:
        return {}
    if isinstance(current_signal.payload, dict):
        return dict(current_signal.payload)
    return {}


@lru_cache(maxsize=1)
def _player_signal_redis_client() -> redis.Redis:
    redis_url = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
    max_connections = int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))
    if redis_url:
        return redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            max_connections=max_connections,
        )
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True,
        max_connections=max_connections,
    )


def _candidate_signal_ids(agent_ctx: MafiaPlayerContext) -> list[str]:
    round_no = agent_ctx.state.round_no
    phase = str(agent_ctx.state.phase)
    raw_candidates: list[str] = []
    if phase == "await_day_discussion":
        raw_candidates.extend(
            [
                f"day_discussion_open:{round_no}",
                f"day_vote_open:{round_no}",
                f"night_action_open:{round_no}",
            ]
        )
    elif phase == "day_discussion":
        raw_candidates.extend(
            [
                f"day_vote_open:{round_no}",
                f"night_action_open:{round_no}",
                f"day_discussion_open:{round_no}",
            ]
        )
    elif phase in {"day_vote", "day_vote_must_vote"}:
        raw_candidates.extend(
            [
                f"day_vote_open:{round_no}",
                f"night_action_open:{round_no}",
            ]
        )
    elif phase == "await_night_action":
        raw_candidates.extend(
            [
                f"night_action_open:{round_no}",
                f"day_discussion_open:{round_no + 1}",
            ]
        )
    elif phase == "night_action":
        raw_candidates.extend(
            [
                f"day_discussion_open:{round_no + 1}",
                f"night_action_open:{round_no}",
            ]
        )
    else:
        raw_candidates.extend(
            [
                f"day_discussion_open:{round_no}",
                f"day_vote_open:{round_no}",
                f"night_action_open:{round_no}",
                f"day_discussion_open:{round_no + 1}",
            ]
        )

    deduped_candidates: list[str] = []
    for signal_id in raw_candidates:
        if signal_id not in deduped_candidates:
            deduped_candidates.append(signal_id)
    return deduped_candidates


async def _buffered_signal_payload(
    agent_ctx: MafiaPlayerContext,
    signal_id: str,
) -> dict[str, Any]:
    del agent_ctx
    execution_ctx = ExecutionContext.current()
    namespace = os.getenv("FACTORIAL_NAMESPACE", "factorial")
    raw_value_obj = await resolve_awaitable(
        _player_signal_redis_client().hget(
            f"{namespace}:signals:{execution_ctx.task_id}:pending",
            signal_id,
        )
    )
    if raw_value_obj is None:
        return {}
    raw_value = (
        raw_value_obj.decode("utf-8")
        if isinstance(raw_value_obj, bytes)
        else str(raw_value_obj)
    )
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    payload = parsed.get("payload")
    if isinstance(payload, dict):
        return dict(payload)
    return {}


async def _phase_payload(agent_ctx: MafiaPlayerContext) -> dict[str, Any]:
    payload = _signal_payload()
    if payload:
        return payload
    for signal_id in _candidate_signal_ids(agent_ctx):
        buffered_payload = await _buffered_signal_payload(agent_ctx, signal_id)
        if buffered_payload:
            return buffered_payload
    return {}


def _normalize_day_message(message: str | None) -> str | None:
    candidate = (message or "").strip()
    if not candidate:
        return None
    return candidate[:400]


def _resolve_target_input(
    *,
    player_id: str | None = None,
    target_player_id: str | None = None,
    target: str | None = None,
    target_id: str | None = None,
) -> str:
    for value in (player_id, target_player_id, target, target_id):
        if isinstance(value, str):
            candidate = value.strip()
            if candidate:
                return candidate
    return ""


def _resolve_reason_input(
    *,
    rationale: str | None = None,
    reason: str | None = None,
) -> str:
    for value in (rationale, reason):
        if isinstance(value, str):
            candidate = value.strip()
            if candidate:
                return candidate
    return ""


def _result(
    summary: str,
    *,
    channel: str | None = None,
    message: str | None = None,
) -> PlayerActionResult:
    return PlayerActionResult(summary=summary, channel=channel, message=message)


def _sync_night_state(agent_ctx: MafiaPlayerContext, payload: dict[str, Any]) -> None:
    allowed_targets = payload.get("allowed_targets")
    agent_ctx.state.night_kill_allowed_targets = (
        [item for item in allowed_targets if isinstance(item, str)]
        if isinstance(allowed_targets, list)
        else []
    )
    alive_werewolf_count = _coerce_round_no(payload.get("alive_werewolf_count"))
    if alive_werewolf_count is not None and alive_werewolf_count > 0:
        agent_ctx.state.night_alive_werewolf_count = alive_werewolf_count


def _sync_day_vote_state(
    agent_ctx: MafiaPlayerContext, payload: dict[str, Any]
) -> None:
    allowed_targets = payload.get("allowed_targets")
    agent_ctx.state.day_vote_allowed_targets = (
        [item for item in allowed_targets if isinstance(item, str)]
        if isinstance(allowed_targets, list)
        else []
    )
    agent_ctx.state.day_vote_deadline_ts = _coerce_float(payload.get("deadline_ts"))


def _night_signal_is_active(
    agent_ctx: MafiaPlayerContext,
    payload: dict[str, Any],
) -> bool:
    signal_kind = payload.get("kind")
    if signal_kind == "night_action_open":
        _sync_night_state(agent_ctx, payload)
        _set_phase(agent_ctx, "night_action")
        return True
    return _is_phase(agent_ctx, "night_action")


def _day_vote_seconds_remaining(agent_ctx: MafiaPlayerContext) -> float | None:
    deadline_ts = agent_ctx.state.day_vote_deadline_ts
    if deadline_ts is None:
        return None
    return max(0.0, deadline_ts - time.time())


def _night_coordination_allowed(agent_ctx: MafiaPlayerContext) -> bool:
    return (
        agent_ctx.state.role == "werewolf"
        and agent_ctx.state.night_alive_werewolf_count > 1
    )


def _chat_enabled(agent_ctx: MafiaPlayerContext) -> bool:
    return _is_phase(agent_ctx, "day_discussion") and (
        agent_ctx.state.discussion_messages_sent < MAX_DAY_MESSAGES_PER_ROUND
    )


def _think_enabled(agent_ctx: MafiaPlayerContext) -> bool:
    return _is_phase(
        agent_ctx,
        "await_day_discussion",
        "day_discussion",
        "await_night_action",
    )


def _poll_enabled(agent_ctx: MafiaPlayerContext) -> bool:
    if _is_phase(agent_ctx, "night_action"):
        if agent_ctx.state.role != "werewolf":
            return True
        return _night_coordination_allowed(agent_ctx)
    return _is_phase(
        agent_ctx,
        "await_day_discussion",
        "day_discussion",
        "await_night_action",
    )


def _vote_enabled(agent_ctx: MafiaPlayerContext) -> bool:
    return _is_phase(agent_ctx, "day_vote", "day_vote_must_vote")


def _kill_enabled(agent_ctx: MafiaPlayerContext) -> bool:
    return _is_phase(agent_ctx, "night_action") and agent_ctx.state.role == "werewolf"


def _call_vote_enabled(agent_ctx: MafiaPlayerContext) -> bool:
    return (
        _is_phase(agent_ctx, "day_discussion")
        and not agent_ctx.state.has_called_vote
    )


def _chat_with_werewolves_enabled(agent_ctx: MafiaPlayerContext) -> bool:
    return _is_phase(agent_ctx, "night_action") and _night_coordination_allowed(
        agent_ctx
    )


@tool(is_enabled=_chat_enabled)
async def chat(
    agent_ctx: MafiaPlayerContext,
    message: str | None = None,
    content: str | None = None,
    channel: str | None = None,
) -> PlayerActionResult:
    """
    Publish a day discussion message to the town square.
    """
    payload = await _phase_payload(agent_ctx)
    round_no = _coerce_round_no(payload.get("round_no"))
    if round_no is not None:
        agent_ctx.state.round_no = round_no
        _ensure_discussion_round(agent_ctx, round_no)

    signal_kind = payload.get("kind")
    if signal_kind == "day_vote_open":
        _sync_day_vote_state(agent_ctx, payload)
        _set_phase(agent_ctx, "day_vote")
        return _result("Day vote opened; switch to vote or poll.")
    if signal_kind == "night_action_open":
        _sync_night_state(agent_ctx, payload)
        _set_phase(agent_ctx, "night_action")
        return _result("Night phase opened; switching from day actions.")

    normalized_channel = (channel or "").strip().lower()
    if normalized_channel and normalized_channel not in {"town", "public"}:
        raise ValueError(
            "chat() only supports the town channel. "
            "Use chat_with_werewolves() for wolf chat."
        )

    resolved_content = _normalize_day_message(message or content)
    if resolved_content is None:
        return _result(
            "No valid chat message provided. "
            "Write your own message or poll for updates."
        )
    labeled_content = f"{agent_ctx.state.display_name}: {resolved_content}"
    await messaging.group.send(
        agent_ctx.state.town_group_name,
        labeled_content,
        data={
            "kind": "day_chat",
            "round_no": agent_ctx.state.round_no,
            "player_id": agent_ctx.state.player_id,
            "display_name": agent_ctx.state.display_name,
        },
    )
    agent_ctx.state.discussion_messages_sent += 1
    _set_phase(agent_ctx, "day_discussion")
    return _result(
        "Posted a day discussion message.",
        channel="town",
        message=resolved_content,
    )


@tool(is_enabled=_call_vote_enabled)
async def call_vote(agent_ctx: MafiaPlayerContext) -> PlayerActionResult:
    """
    Signal the game master that you are ready to move to the voting phase.
    Once a majority of alive players call for a vote, discussion ends
    and voting begins. You can still chat after calling.
    """
    payload = await _phase_payload(agent_ctx)
    signal_kind = payload.get("kind")
    if signal_kind == "day_vote_open":
        _sync_day_vote_state(agent_ctx, payload)
        _set_phase(agent_ctx, "day_vote")
        return _result("Day vote already opened; switch to vote or poll.")
    if signal_kind == "night_action_open":
        _sync_night_state(agent_ctx, payload)
        _set_phase(agent_ctx, "night_action")
        return _result("Night phase opened; switching from day actions.")

    await messaging.direct.send(
        to_task_id=agent_ctx.state.parent_task_id,
        content=f"{agent_ctx.state.display_name} called for a vote.",
        data={
            "kind": "call_vote",
            "round_no": agent_ctx.state.round_no,
            "voter_id": agent_ctx.state.player_id,
        },
    )
    agent_ctx.state.has_called_vote = True
    _set_phase(agent_ctx, "day_discussion")
    return _result("Called for a vote. You can still chat while waiting.")


@tool(is_enabled=_think_enabled)
async def think(
    agent_ctx: MafiaPlayerContext,
    thought: str | None = None,
) -> PlayerActionResult:
    """
    Log a private thought visible only in omniscient UI mode.
    """
    payload = await _phase_payload(agent_ctx)
    signal_kind = payload.get("kind")
    round_no = _coerce_round_no(payload.get("round_no"))
    if round_no is not None:
        agent_ctx.state.round_no = round_no

    if signal_kind == "day_vote_open":
        _sync_day_vote_state(agent_ctx, payload)
        remaining_seconds = _day_vote_seconds_remaining(agent_ctx)
        warning_threshold = float(
            DAY_PHASE_POLL_SECONDS * DAY_VOTE_WARNING_POLLS_REMAINING
        )
        if remaining_seconds is not None and remaining_seconds <= warning_threshold:
            _set_phase(agent_ctx, "day_vote_must_vote")
            return _result("Time is about to run out; you must vote in the next turn.")
        _set_phase(agent_ctx, "day_vote")
        return _result("Day vote is open; vote soon.")
    if signal_kind == "night_action_open":
        agent_ctx.state.day_vote_allowed_targets = []
        agent_ctx.state.day_vote_deadline_ts = None
        _sync_night_state(agent_ctx, payload)
        _set_phase(agent_ctx, "night_action")
        return _result("Night phase opened; switching from day actions.")
    if signal_kind == "day_discussion_open":
        _ensure_discussion_round(agent_ctx, agent_ctx.state.round_no)
        agent_ctx.state.day_vote_allowed_targets = []
        agent_ctx.state.day_vote_deadline_ts = None
        agent_ctx.state.night_kill_allowed_targets = []
        agent_ctx.state.night_alive_werewolf_count = 1
        _set_phase(agent_ctx, "day_discussion")

    content = (thought or "").strip()
    if not content:
        return _result("No thought provided. Write a brief internal note.")

    if _is_phase(agent_ctx, "day_vote"):
        remaining_seconds = _day_vote_seconds_remaining(agent_ctx)
        warning_threshold = float(
            DAY_PHASE_POLL_SECONDS * DAY_VOTE_WARNING_POLLS_REMAINING
        )
        if remaining_seconds is not None and remaining_seconds <= warning_threshold:
            _set_phase(agent_ctx, "day_vote_must_vote")
            return _result("Time is about to run out; you must vote in the next turn.")

    return _result(
        "Logged a private thought.",
        channel="thought",
        message=content[:600],
    )


@tool(is_enabled=_poll_enabled)
async def poll(
    agent_ctx: MafiaPlayerContext,
) -> WaitInstruction | PlayerActionResult:
    """
    Wait for phase updates.

    Timing behavior:
    - Active day discussion/day vote polling waits 5 seconds per poll.
    - Awaiting phase-open signals waits up to 180 seconds.
    - At night, villagers wait up to 180 seconds; multi-werewolves poll for 5 seconds.
    - During day vote, if <= 10 seconds remain, returns a warning and forces
      vote next turn.
    """
    payload = await _phase_payload(agent_ctx)
    round_no = _coerce_round_no(payload.get("round_no"))
    if round_no is not None:
        agent_ctx.state.round_no = round_no

    signal_kind = payload.get("kind")
    if signal_kind == "day_discussion_open":
        _ensure_discussion_round(agent_ctx, agent_ctx.state.round_no)
        agent_ctx.state.day_vote_allowed_targets = []
        agent_ctx.state.day_vote_deadline_ts = None
        agent_ctx.state.night_kill_allowed_targets = []
        agent_ctx.state.night_alive_werewolf_count = 1
        _set_phase(agent_ctx, "day_discussion")
        return _result("Day discussion is active.")
    if signal_kind == "day_vote_open":
        _sync_day_vote_state(agent_ctx, payload)
        agent_ctx.state.night_kill_allowed_targets = []
        agent_ctx.state.night_alive_werewolf_count = 1
        _set_phase(agent_ctx, "day_vote")
        return _result("Day vote is open; you can now vote or keep polling.")
    if signal_kind == "night_action_open":
        agent_ctx.state.day_vote_allowed_targets = []
        agent_ctx.state.day_vote_deadline_ts = None
        _sync_night_state(agent_ctx, payload)
        _set_phase(agent_ctx, "night_action")
        if agent_ctx.state.role != "werewolf":
            return _result("Night phase is active; villager waits for dawn.")
        if _night_coordination_allowed(agent_ctx):
            return _result("Night is active; coordinate, kill, or poll.")
        return _result("Night is active; submit kill.")

    if _is_phase(agent_ctx, "await_day_discussion"):
        _set_phase(agent_ctx, "await_day_discussion")
        return wait.until_signal(
            f"day_discussion_open:{agent_ctx.state.round_no}",
            timeout=wait.sleep(float(PHASE_SIGNAL_WAIT_SECONDS)),
            data={
                "phase": "await_day_discussion",
                "player_id": agent_ctx.state.player_id,
                "round_no": agent_ctx.state.round_no,
            },
        )

    if _is_phase(agent_ctx, "await_night_action"):
        _set_phase(agent_ctx, "await_night_action")
        return wait.until_signal(
            f"night_action_open:{agent_ctx.state.round_no}",
            timeout=wait.sleep(float(PHASE_SIGNAL_WAIT_SECONDS)),
            data={
                "phase": "await_night_action",
                "player_id": agent_ctx.state.player_id,
                "round_no": agent_ctx.state.round_no,
            },
        )

    if _is_phase(agent_ctx, "day_vote"):
        remaining_seconds = _day_vote_seconds_remaining(agent_ctx)
        warning_threshold = float(
            DAY_PHASE_POLL_SECONDS * DAY_VOTE_WARNING_POLLS_REMAINING
        )
        if remaining_seconds is not None and remaining_seconds <= warning_threshold:
            _set_phase(agent_ctx, "day_vote_must_vote")
            return _result("Time is about to run out; you must vote in the next turn.")

        poll_timeout = float(DAY_PHASE_POLL_SECONDS)
        if remaining_seconds is not None:
            poll_timeout = max(0.0, min(poll_timeout, remaining_seconds))

        _set_phase(agent_ctx, "day_vote")
        return wait.until_signal(
            f"night_action_open:{agent_ctx.state.round_no}",
            timeout=wait.sleep(poll_timeout),
            data={
                "phase": "day_vote",
                "player_id": agent_ctx.state.player_id,
                "round_no": agent_ctx.state.round_no,
            },
        )

    if _is_phase(agent_ctx, "day_discussion"):
        _set_phase(agent_ctx, "day_discussion")
        return wait.until_signal(
            f"day_vote_open:{agent_ctx.state.round_no}",
            timeout=wait.sleep(float(DAY_PHASE_POLL_SECONDS)),
            data={
                "phase": "day_discussion",
                "player_id": agent_ctx.state.player_id,
                "round_no": agent_ctx.state.round_no,
            },
        )

    if _is_phase(agent_ctx, "night_action"):
        next_round_no = agent_ctx.state.round_no + 1
        if agent_ctx.state.role != "werewolf":
            _set_phase(agent_ctx, "night_action")
            return wait.until_signal(
                f"day_discussion_open:{next_round_no}",
                timeout=wait.sleep(float(PHASE_SIGNAL_WAIT_SECONDS)),
                data={
                    "phase": "night_action",
                    "player_id": agent_ctx.state.player_id,
                    "round_no": agent_ctx.state.round_no,
                },
            )
        if _night_coordination_allowed(agent_ctx):
            _set_phase(agent_ctx, "night_action")
            return wait.until_signal(
                f"day_discussion_open:{next_round_no}",
                timeout=wait.sleep(float(DAY_PHASE_POLL_SECONDS)),
                data={
                    "phase": "night_action",
                    "player_id": agent_ctx.state.player_id,
                    "round_no": agent_ctx.state.round_no,
                },
            )
        return _result("Single werewolf cannot poll at night; submit kill.")

    raise RuntimeError(
        "poll() reached an unexpected player phase "
        f"{agent_ctx.state.phase!r} in round {agent_ctx.state.round_no}."
    )


@tool(is_enabled=_vote_enabled)
async def vote(
    agent_ctx: MafiaPlayerContext,
    player_id: str | None = None,
    target_player_id: str | None = None,
    rationale: str | None = None,
    target: str | None = None,
    target_id: str | None = None,
    reason: str | None = None,
) -> PlayerActionResult:
    """
    Submit one day vote to the game master.
    """
    payload = await _phase_payload(agent_ctx)
    signal_kind = payload.get("kind")
    if signal_kind == "night_action_open":
        agent_ctx.state.day_vote_allowed_targets = []
        agent_ctx.state.day_vote_deadline_ts = None
        _set_phase(agent_ctx, "night_action")
        return _result("Day vote phase already ended; switching to night action.")

    round_no = _coerce_round_no(payload.get("round_no"))
    if round_no is not None:
        agent_ctx.state.round_no = round_no

    if signal_kind == "day_vote_open":
        _sync_day_vote_state(agent_ctx, payload)

    normalized_targets = [
        candidate
        for candidate in agent_ctx.state.day_vote_allowed_targets
        if candidate != agent_ctx.state.player_id
    ]
    if not normalized_targets:
        raise RuntimeError(
            "Day vote is active but no valid vote targets are known. "
            "The player did not receive usable day_vote_open target data."
        )

    candidate = _resolve_target_input(
        player_id=player_id,
        target_player_id=target_player_id,
        target=target,
        target_id=target_id,
    )
    if candidate not in normalized_targets:
        raise ValueError(
            "Invalid day vote target. "
            f"Expected one of {sorted(normalized_targets)!r}, "
            f"received {candidate or '<empty>'!r}."
        )

    resolved_reason = _resolve_reason_input(rationale=rationale, reason=reason)
    if not resolved_reason:
        resolved_reason = "Based on current discussion and inconsistencies."

    await messaging.direct.send(
        to_task_id=agent_ctx.state.parent_task_id,
        content=f"{agent_ctx.state.display_name} submitted a day vote.",
        data={
            "kind": "day_vote",
            "round_no": agent_ctx.state.round_no,
            "voter_id": agent_ctx.state.player_id,
            "target_player_id": candidate,
            "rationale": resolved_reason,
        },
    )
    agent_ctx.state.day_vote_allowed_targets = []
    agent_ctx.state.day_vote_deadline_ts = None
    _set_phase(agent_ctx, "await_night_action")
    return _result(f"Submitted day vote for {candidate}.")


@tool(is_enabled=_chat_with_werewolves_enabled)
async def chat_with_werewolves(
    agent_ctx: MafiaPlayerContext,
    message: str | None = None,
    content: str | None = None,
    channel: str | None = None,
) -> PlayerActionResult:
    """
    Share a short private message with living werewolf teammates at night.
    """
    payload = await _phase_payload(agent_ctx)
    signal_kind = payload.get("kind")
    if signal_kind == "day_discussion_open":
        round_no = _coerce_round_no(payload.get("round_no"))
        if round_no is not None:
            agent_ctx.state.round_no = round_no
            _ensure_discussion_round(agent_ctx, round_no)
        agent_ctx.state.night_kill_allowed_targets = []
        agent_ctx.state.night_alive_werewolf_count = 1
        _set_phase(agent_ctx, "day_discussion")
        return _result("Night ended; returning to day discussion.")
    if not _night_signal_is_active(agent_ctx, payload):
        _set_phase(agent_ctx, "await_night_action")
        return _result("Night signal was unavailable; waiting again.")

    if not _night_coordination_allowed(agent_ctx):
        return _result("Wolf coordination is unavailable; submit kill.")

    normalized_channel = (channel or "").strip().lower()
    if normalized_channel and normalized_channel not in {"wolf", "werewolf", "private"}:
        raise ValueError(
            "chat_with_werewolves() only supports the werewolf private channel."
        )

    resolved_content = (message or content or "").strip()
    if not resolved_content:
        return _result(
            "No wolf chat message provided. "
            "Write your own message or choose kill/poll."
        )

    resolved_content = resolved_content[:400]
    labeled_content = f"{agent_ctx.state.display_name}: {resolved_content}"
    await messaging.group.send(
        agent_ctx.state.wolf_group_name,
        labeled_content,
        data={
            "kind": "wolf_chat",
            "round_no": agent_ctx.state.round_no,
            "player_id": agent_ctx.state.player_id,
            "display_name": agent_ctx.state.display_name,
        },
    )
    _set_phase(agent_ctx, "night_action")
    return _result(
        "Posted a werewolf team chat message.",
        channel="wolf",
        message=resolved_content,
    )


@tool(is_enabled=_kill_enabled)
async def kill(
    agent_ctx: MafiaPlayerContext,
    player_id: str | None = None,
    target_player_id: str | None = None,
    rationale: str | None = None,
    target: str | None = None,
    target_id: str | None = None,
    reason: str | None = None,
) -> PlayerActionResult:
    """
    Submit the werewolf night kill target to the game master.
    """
    payload = await _phase_payload(agent_ctx)
    signal_kind = payload.get("kind")
    if signal_kind == "day_discussion_open":
        round_no = _coerce_round_no(payload.get("round_no"))
        if round_no is not None:
            agent_ctx.state.round_no = round_no
            _ensure_discussion_round(agent_ctx, round_no)
        agent_ctx.state.night_kill_allowed_targets = []
        agent_ctx.state.night_alive_werewolf_count = 1
        _set_phase(agent_ctx, "day_discussion")
        return _result("Night ended; returning to day discussion.")
    if not _night_signal_is_active(agent_ctx, payload):
        _set_phase(agent_ctx, "await_night_action")
        return _result("Night signal was unavailable; waiting again.")

    round_no = _coerce_round_no(payload.get("round_no"))
    if round_no is not None:
        agent_ctx.state.round_no = round_no

    normalized_targets = [
        candidate
        for candidate in agent_ctx.state.night_kill_allowed_targets
        if candidate != agent_ctx.state.player_id
    ]
    if not normalized_targets:
        raise RuntimeError(
            "Night action is active but no valid kill targets are known. "
            "The player did not receive usable night_action_open target data."
        )

    candidate = _resolve_target_input(
        player_id=player_id,
        target_player_id=target_player_id,
        target=target,
        target_id=target_id,
    )
    if candidate not in normalized_targets:
        raise ValueError(
            "Invalid night action target. "
            f"Expected one of {sorted(normalized_targets)!r}, "
            f"received {candidate or '<empty>'!r}."
        )

    resolved_reason = _resolve_reason_input(rationale=rationale, reason=reason)
    if not resolved_reason:
        resolved_reason = "Target appears less trusted by the town."

    await messaging.direct.send(
        to_task_id=agent_ctx.state.parent_task_id,
        content=f"{agent_ctx.state.display_name} submitted a kill.",
        data={
            "kind": "night_action",
            "round_no": agent_ctx.state.round_no,
            "voter_id": agent_ctx.state.player_id,
            "target_player_id": candidate,
            "rationale": resolved_reason,
        },
    )

    agent_ctx.state.night_kill_allowed_targets = []
    agent_ctx.state.round_no += 1
    _set_phase(agent_ctx, "await_day_discussion")
    return _result(f"Submitted kill for {candidate}.")


def _player_tool_choice(agent_ctx) -> str | dict[str, Any]:
    phase = str(agent_ctx.state.phase)
    if phase in {"day_vote", "day_vote_must_vote"}:
        return "required"
    if (
        phase == "night_action"
        and agent_ctx.state.role == "werewolf"
        and not _night_coordination_allowed(agent_ctx)
    ):
        return "required"
    return "auto"


PLAYER_TOOLS = [
    chat,
    call_vote,
    think,
    poll,
    vote,
    chat_with_werewolves,
    kill,
]


__all__ = [
    "chat",
    "call_vote",
    "think",
    "poll",
    "vote",
    "chat_with_werewolves",
    "kill",
    "_player_tool_choice",
    "PLAYER_TOOLS",
]
