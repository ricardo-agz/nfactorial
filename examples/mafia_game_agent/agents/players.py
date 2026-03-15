from __future__ import annotations

import random
import time
from typing import Any

from models import MafiaPlayerContext, PlayerActionResult

from factorial import WaitInstruction, messaging, signals, tool, wait

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


def _deterministic_choice(options: list[str], seed: str) -> str:
    rng = random.Random(seed)
    return rng.choice(options)


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


def _normalize_day_message(message: str | None) -> str | None:
    candidate = (message or "").strip()
    if not candidate:
        return None
    return candidate[:400]


def _resolve_target_input(
    *,
    target_player_id: str | None = None,
    target: str | None = None,
    target_id: str | None = None,
) -> str:
    for value in (target_player_id, target, target_id):
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


def _day_vote_seconds_remaining(agent_ctx: MafiaPlayerContext) -> float | None:
    deadline_ts = agent_ctx.state.day_vote_deadline_ts
    if deadline_ts is None:
        return None
    return max(0.0, deadline_ts - time.time())


def _night_coordination_allowed(agent_ctx: MafiaPlayerContext) -> bool:
    return agent_ctx.state.role == "werewolf" and agent_ctx.state.night_alive_werewolf_count > 1


def _chat_enabled(agent_ctx: MafiaPlayerContext) -> bool:
    return _is_phase(agent_ctx, "day_discussion") and (
        agent_ctx.state.discussion_messages_sent < MAX_DAY_MESSAGES_PER_ROUND
    )


def _think_enabled(agent_ctx: MafiaPlayerContext) -> bool:
    return _is_phase(
        agent_ctx,
        "await_day_discussion",
        "day_discussion",
        "day_vote",
        "await_night_action",
        "night_action",
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
        "day_vote",
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
) -> PlayerActionResult:
    """
    Publish a day discussion message to the town square.
    """
    payload = _signal_payload()
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

    content = _normalize_day_message(message)
    if content is None:
        return _result(
            "No valid chat message provided. "
            "Write your own message or poll for updates."
        )
    labeled_content = f"{agent_ctx.state.display_name}: {content}"
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
    return _result("Posted a day discussion message.", channel="town", message=content)


@tool(is_enabled=_call_vote_enabled)
async def call_vote(agent_ctx: MafiaPlayerContext) -> PlayerActionResult:
    """
    Signal the game master that you are ready to move to the voting phase.
    Once a majority of alive players call for a vote, discussion ends
    and voting begins. You can still chat after calling.
    """
    payload = _signal_payload()
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
def think(
    agent_ctx: MafiaPlayerContext,
    thought: str | None = None,
) -> PlayerActionResult:
    """
    Log a private thought visible only in omniscient UI mode.
    """
    payload = _signal_payload()
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

    _set_phase(agent_ctx, str(getattr(agent_ctx, "phase", "day_discussion")))
    return _result(
        "Logged a private thought.",
        channel="thought",
        message=content[:600],
    )


@tool(is_enabled=_poll_enabled)
def poll(agent_ctx: MafiaPlayerContext) -> WaitInstruction | PlayerActionResult:
    """
    Wait for phase updates.

    Timing behavior:
    - Active day discussion/day vote polling waits 5 seconds per poll.
    - Awaiting phase-open signals waits up to 180 seconds.
    - At night, villagers wait up to 180 seconds; multi-werewolves poll for 5 seconds.
    - During day vote, if <= 10 seconds remain, returns a warning and forces
      vote next turn.
    """
    payload = _signal_payload()
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


@tool(is_enabled=_vote_enabled)
async def vote(
    agent_ctx: MafiaPlayerContext,
    target_player_id: str | None = None,
    rationale: str | None = None,
    target: str | None = None,
    target_id: str | None = None,
    reason: str | None = None,
) -> PlayerActionResult:
    """
    Submit one day vote to the game master.
    """
    payload = _signal_payload()
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
        agent_ctx.state.day_vote_deadline_ts = None
        _set_phase(agent_ctx, "await_night_action")
        return _result("No valid day vote targets were available.")

    candidate = _resolve_target_input(
        target_player_id=target_player_id,
        target=target,
        target_id=target_id,
    )
    if candidate not in normalized_targets:
        candidate = _deterministic_choice(
            normalized_targets,
            seed=(
                f"{agent_ctx.state.player_id}:day_vote:{agent_ctx.state.round_no}:"
                f"{','.join(sorted(normalized_targets))}"
            ),
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
) -> PlayerActionResult:
    """
    Share a short private message with living werewolf teammates at night.
    """
    payload = _signal_payload()
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
    if signal_kind != "night_action_open":
        _set_phase(agent_ctx, "await_night_action")
        return _result("Night signal was unavailable; waiting again.")

    _sync_night_state(agent_ctx, payload)
    if not _night_coordination_allowed(agent_ctx):
        return _result("Wolf coordination is unavailable; submit kill.")

    content = (message or "").strip()
    if not content:
        return _result(
            "No wolf chat message provided. "
            "Write your own message or choose kill/poll."
        )

    content = content[:400]
    labeled_content = f"{agent_ctx.state.display_name}: {content}"
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
        message=content,
    )


@tool(is_enabled=_kill_enabled)
async def kill(
    agent_ctx: MafiaPlayerContext,
    target_player_id: str | None = None,
    rationale: str | None = None,
    target: str | None = None,
    target_id: str | None = None,
    reason: str | None = None,
) -> PlayerActionResult:
    """
    Submit the werewolf night kill target to the game master.
    """
    payload = _signal_payload()
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
    if signal_kind != "night_action_open":
        _set_phase(agent_ctx, "await_night_action")
        return _result("Night signal was unavailable; waiting again.")

    round_no = _coerce_round_no(payload.get("round_no"))
    if round_no is not None:
        agent_ctx.state.round_no = round_no

    _sync_night_state(agent_ctx, payload)
    normalized_targets = [
        candidate
        for candidate in agent_ctx.state.night_kill_allowed_targets
        if candidate != agent_ctx.state.player_id
    ]
    if not normalized_targets:
        agent_ctx.state.round_no += 1
        agent_ctx.state.night_kill_allowed_targets = []
        _set_phase(agent_ctx, "await_day_discussion")
        return _result("No valid night targets were available.")

    candidate = _resolve_target_input(
        target_player_id=target_player_id,
        target=target,
        target_id=target_id,
    )
    if candidate not in normalized_targets:
        candidate = _deterministic_choice(
            normalized_targets,
            seed=(
                f"{agent_ctx.state.player_id}:night_action:{agent_ctx.state.round_no}:"
                f"{','.join(sorted(normalized_targets))}"
            ),
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
    if phase == "day_vote_must_vote":
        return {"type": "function", "function": {"name": "vote"}}
    if phase in {"await_day_discussion", "await_night_action"}:
        return {"type": "function", "function": {"name": "poll"}}
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
