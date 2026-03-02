from __future__ import annotations

import random
import re
from typing import Any

from models import MafiaPlayerContext, PlayerActionResult

from factorial import ExecutionContext, WaitInstruction, inbox, messaging, signals, tool, wait

DAY_PHASE_POLL_SECONDS = 5
MAX_DAY_MESSAGES_PER_ROUND = 3
MAX_DAY_VOTE_POLLS_PER_ROUND = 2


def _coerce_round_no(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _deterministic_choice(options: list[str], seed: str) -> str:
    rng = random.Random(seed)
    return rng.choice(options)


def _ensure_discussion_round(agent_ctx: MafiaPlayerContext, round_no: int) -> None:
    if agent_ctx.discussion_round_no == round_no:
        return
    agent_ctx.discussion_round_no = round_no
    agent_ctx.discussion_messages_sent = 0
    agent_ctx.pending_day_prompt = None


def _ensure_vote_poll_round(agent_ctx: MafiaPlayerContext, round_no: int) -> None:
    if agent_ctx.day_vote_poll_round_no == round_no:
        return
    agent_ctx.day_vote_poll_round_no = round_no
    agent_ctx.day_vote_poll_count = 0


def _signal_payload() -> dict[str, Any]:
    current_signal = signals.current()
    if current_signal is None:
        return {}
    if isinstance(current_signal.payload, dict):
        return dict(current_signal.payload)
    return {}


_SELF_ELIMINATION_PATTERNS = [
    re.compile(r"\bi was eliminated\b", flags=re.IGNORECASE),
    re.compile(r"\bi am eliminated\b", flags=re.IGNORECASE),
    re.compile(r"\bi'm eliminated\b", flags=re.IGNORECASE),
    re.compile(r"\bi was killed\b", flags=re.IGNORECASE),
    re.compile(r"\bi am dead\b", flags=re.IGNORECASE),
    re.compile(r"\bi'm dead\b", flags=re.IGNORECASE),
    re.compile(r"\bwell[, ]+i'?m out\b", flags=re.IGNORECASE),
    re.compile(r"\bi did not survive\b", flags=re.IGNORECASE),
]


def _implies_self_elimination(message: str) -> bool:
    return any(pattern.search(message) for pattern in _SELF_ELIMINATION_PATTERNS)


def _fallback_day_statement(
    agent_ctx: MafiaPlayerContext, payload: dict[str, Any]
) -> str:
    last_eliminated = payload.get("last_eliminated_display_name")
    if isinstance(last_eliminated, str) and last_eliminated.strip():
        return (
            f"{agent_ctx.display_name}: "
            f"{last_eliminated.strip()} is out. Let's compare vote patterns and tone shifts."
        )
    if agent_ctx.role == "werewolf":
        return f"{agent_ctx.display_name}: I am suspicious of quiet players."
    return f"{agent_ctx.display_name}: Let's compare inconsistencies before voting."


def _prompt_based_day_statement(agent_ctx: MafiaPlayerContext, prompt: str) -> str:
    compact = " ".join(prompt.split())
    snippet = compact[:120] if len(compact) > 120 else compact
    return (
        f"{agent_ctx.display_name}: I hear you on \"{snippet}\". "
        "Let's compare voting patterns before locking our vote."
    )


def _normalize_day_message(
    agent_ctx: MafiaPlayerContext,
    payload: dict[str, Any],
    message: str | None,
) -> str:
    candidate = (message or "").strip()
    if not candidate:
        pending = (agent_ctx.pending_day_prompt or "").strip()
        if pending:
            return _prompt_based_day_statement(agent_ctx, pending)
        return _fallback_day_statement(agent_ctx, payload)

    # This tool is only called for alive players; reject self-elimination roleplay.
    if _implies_self_elimination(candidate):
        pending = (agent_ctx.pending_day_prompt or "").strip()
        if pending:
            return _prompt_based_day_statement(agent_ctx, pending)
        return _fallback_day_statement(agent_ctx, payload)

    return candidate[:400]


async def _read_latest_human_town_prompt(agent_ctx: MafiaPlayerContext) -> str | None:
    current_task_id = ExecutionContext.current().task_id
    cursor: str | None = None
    latest_prompt: str | None = None

    while True:
        page = await inbox.group.peek(
            agent_ctx.town_group_name,
            unread_only=True,
            limit=50,
            cursor=cursor,
        )
        consumed_message_ids: list[str] = []

        for message in page.messages:
            consumed_message_ids.append(message.message_id)
            if message.from_task_id == current_task_id:
                continue

            payload = message.data if isinstance(message.data, dict) else {}
            kind = payload.get("kind")
            is_human_message = kind == "human_chat" or (
                isinstance(message.from_owner_id, str)
                and bool(message.from_owner_id.strip())
                and message.from_task_id is None
            )
            if not is_human_message:
                continue

            candidate = message.content.strip()
            if candidate:
                latest_prompt = candidate

        if consumed_message_ids:
            await inbox.group.mark_read(
                agent_ctx.town_group_name,
                message_ids=consumed_message_ids,
                notify_sender=False,
            )

        if not page.has_more or not page.next_cursor:
            break
        cursor = page.next_cursor

    return latest_prompt


def _result(
    summary: str,
    *,
    channel: str | None = None,
    message: str | None = None,
) -> PlayerActionResult:
    return PlayerActionResult(summary=summary, channel=channel, message=message)


def _wait_for_signal(
    agent_ctx: MafiaPlayerContext,
    *,
    signal_id: str,
    next_phase: str,
    waiting_phase: str,
    timeout_seconds: int,
) -> WaitInstruction:
    agent_ctx.phase = next_phase
    return wait.until_signal(
        signal_id,
        timeout=wait.sleep(float(timeout_seconds)),
        data={
            "phase": waiting_phase,
            "player_id": agent_ctx.player_id,
            "round_no": agent_ctx.round_no,
        },
    )


@tool
def wait_for_game_start(agent_ctx: MafiaPlayerContext) -> WaitInstruction:
    """
    Player waits for initial game setup signal.
    """
    return _wait_for_signal(
        agent_ctx,
        signal_id="game_start",
        next_phase="configure_after_start",
        waiting_phase="await_game_start",
        timeout_seconds=600,
    )


@tool
async def configure_after_start(agent_ctx: MafiaPlayerContext) -> PlayerActionResult:
    """
    Read role assignment from inbox and transition to day discussion.
    """
    payload = _signal_payload()
    if not payload:
        agent_ctx.phase = "await_game_start"
        return _result("No game_start signal payload was available. Waiting again.")

    role_assignment = None
    cursor: str | None = None
    while True:
        page = await inbox.direct.peek(unread_only=True, limit=50, cursor=cursor)
        consumed_message_ids: list[str] = []
        for message in page.messages:
            candidate = message.data if isinstance(message.data, dict) else {}
            if candidate.get("kind") != "role_assignment":
                continue
            consumed_message_ids.append(message.message_id)
            if candidate.get("player_id") != agent_ctx.player_id:
                continue
            role_assignment = candidate
        if consumed_message_ids:
            await inbox.direct.mark_read(
                message_ids=consumed_message_ids,
                notify_sender=False,
            )
        if not page.has_more or not page.next_cursor:
            break
        cursor = page.next_cursor

    if isinstance(role_assignment, dict):
        role = role_assignment.get("role")
        if role in {"villager", "werewolf"}:
            agent_ctx.role = role

    agent_ctx.phase = "await_day_discussion"
    return _result(
        f"Configured role as {agent_ctx.role} and waiting for day discussion."
    )


@tool
def wait_for_day_discussion(agent_ctx: MafiaPlayerContext) -> WaitInstruction:
    """
    Wait for current-round day discussion signal from game master.
    """
    return _wait_for_signal(
        agent_ctx,
        signal_id=f"day_discussion_open:{agent_ctx.round_no}",
        next_phase="day_discussion_action",
        waiting_phase="await_day_discussion",
        timeout_seconds=180,
    )


@tool
async def send_public_statement(
    agent_ctx: MafiaPlayerContext,
    message: str | None = None,
) -> PlayerActionResult:
    """
    Publish one short day discussion statement to the town square.
    """
    payload = _signal_payload()
    if payload.get("kind") != "day_discussion_open":
        agent_ctx.phase = "await_day_discussion"
        return _result("Day discussion signal was unavailable; waiting again.")

    round_no = _coerce_round_no(payload.get("round_no"))
    if round_no is not None:
        agent_ctx.round_no = round_no
    _ensure_discussion_round(agent_ctx, agent_ctx.round_no)

    alive_player_ids = payload.get("alive_player_ids")
    if (
        isinstance(alive_player_ids, list)
        and agent_ctx.player_id not in alive_player_ids
    ):
        agent_ctx.phase = "await_day_vote"
        return _result("Skipped statement because this player is not alive.")

    content = _normalize_day_message(agent_ctx, payload, message)

    await messaging.group.send(
        agent_ctx.town_group_name,
        content,
        data={
            "kind": "day_chat",
            "round_no": agent_ctx.round_no,
            "player_id": agent_ctx.player_id,
        },
    )
    agent_ctx.pending_day_prompt = None
    agent_ctx.discussion_messages_sent += 1
    agent_ctx.phase = "await_day_vote"
    return _result(
        "Published a day discussion statement.",
        channel="town",
        message=content,
    )


@tool
def wait_for_day_vote(agent_ctx: MafiaPlayerContext) -> WaitInstruction:
    """
    Poll for day vote signal while staying discussion-capable.
    """
    return _wait_for_signal(
        agent_ctx,
        signal_id=f"day_vote_open:{agent_ctx.round_no}",
        next_phase="decide_day_vote_or_discussion",
        waiting_phase="await_day_vote",
        timeout_seconds=DAY_PHASE_POLL_SECONDS,
    )


@tool
async def decide_day_vote_or_discussion(
    agent_ctx: MafiaPlayerContext,
) -> PlayerActionResult:
    """
    If vote is open, proceed to voting; otherwise react to new human town chat.
    """
    payload = _signal_payload()
    if payload.get("kind") == "day_vote_open":
        round_no = _coerce_round_no(payload.get("round_no"))
        if round_no is not None:
            agent_ctx.round_no = round_no
        _ensure_discussion_round(agent_ctx, agent_ctx.round_no)
        _ensure_vote_poll_round(agent_ctx, agent_ctx.round_no)
        allowed_targets = payload.get("allowed_targets")
        agent_ctx.day_vote_allowed_targets = (
            [item for item in allowed_targets if isinstance(item, str)]
            if isinstance(allowed_targets, list)
            else []
        )
        agent_ctx.phase = "day_vote_choice"
        return _result("Day vote signal received; choosing whether to vote or poll.")

    if agent_ctx.discussion_messages_sent >= MAX_DAY_MESSAGES_PER_ROUND:
        agent_ctx.phase = "await_day_vote"
        return _result("Holding position and waiting for day vote to open.")

    latest_human_prompt = await _read_latest_human_town_prompt(agent_ctx)
    if latest_human_prompt:
        agent_ctx.pending_day_prompt = latest_human_prompt
        agent_ctx.phase = "day_discussion_action"
        return _result("Detected new human town chat; preparing a follow-up statement.")

    agent_ctx.phase = "await_day_vote"
    return _result("No new human prompts; continuing to wait for day vote.")


@tool
async def choose_day_vote_action(
    agent_ctx: MafiaPlayerContext,
    action: str | None = None,
) -> PlayerActionResult:
    """
    During day vote, either commit vote now or poll for short updates.
    """
    payload = _signal_payload()
    signal_kind = payload.get("kind")
    round_no = _coerce_round_no(payload.get("round_no"))
    if round_no is not None:
        agent_ctx.round_no = round_no
        _ensure_vote_poll_round(agent_ctx, agent_ctx.round_no)

    if signal_kind == "night_action_open":
        agent_ctx.phase = "night_action_action"
        return _result("Day vote phase ended; transitioning to night action.")

    if signal_kind == "day_vote_open":
        allowed_targets = payload.get("allowed_targets")
        agent_ctx.day_vote_allowed_targets = (
            [item for item in allowed_targets if isinstance(item, str)]
            if isinstance(allowed_targets, list)
            else []
        )

    if not agent_ctx.day_vote_allowed_targets:
        agent_ctx.phase = "await_night_action"
        return _result("No vote targets available; waiting for night signal.")

    normalized_action = (action or "").strip().lower()
    wants_poll = normalized_action == "poll"
    can_poll = agent_ctx.day_vote_poll_count < MAX_DAY_VOTE_POLLS_PER_ROUND
    if wants_poll and can_poll:
        agent_ctx.day_vote_poll_count += 1
        agent_ctx.phase = "day_vote_poll"
        return _result("Polling vote window for fresh updates.")

    agent_ctx.phase = "day_vote_action"
    return _result("Committing a day vote now.")


@tool
def poll_day_vote(agent_ctx: MafiaPlayerContext) -> WaitInstruction:
    """
    Poll day-vote updates for a short window.
    """
    return _wait_for_signal(
        agent_ctx,
        signal_id=f"night_action_open:{agent_ctx.round_no}",
        next_phase="day_vote_choice",
        waiting_phase="day_vote_poll",
        timeout_seconds=DAY_PHASE_POLL_SECONDS,
    )


@tool
async def submit_day_vote(
    agent_ctx: MafiaPlayerContext,
    target_player_id: str | None = None,
    rationale: str | None = None,
) -> PlayerActionResult:
    """
    Submit one day vote to the game master via direct inbox payload.
    """
    payload = _signal_payload()
    signal_kind = payload.get("kind")
    if signal_kind == "night_action_open":
        agent_ctx.phase = "night_action_action"
        return _result("Day vote phase already ended; switching to night action.")

    round_no = _coerce_round_no(payload.get("round_no"))
    if round_no is not None:
        agent_ctx.round_no = round_no
        _ensure_vote_poll_round(agent_ctx, agent_ctx.round_no)

    if signal_kind == "day_vote_open":
        allowed_targets = payload.get("allowed_targets")
        normalized_targets = (
            [item for item in allowed_targets if isinstance(item, str)]
            if isinstance(allowed_targets, list)
            else []
        )
        agent_ctx.day_vote_allowed_targets = normalized_targets
    else:
        normalized_targets = list(agent_ctx.day_vote_allowed_targets)

    normalized_targets = [
        candidate
        for candidate in normalized_targets
        if candidate != agent_ctx.player_id
    ]
    if not normalized_targets:
        agent_ctx.phase = "await_night_action"
        return _result("No valid day vote targets were available.")

    candidate = target_player_id.strip() if isinstance(target_player_id, str) else ""
    if candidate not in normalized_targets:
        candidate = _deterministic_choice(
            normalized_targets,
            seed=(
                f"{agent_ctx.player_id}:day_vote:{agent_ctx.round_no}:"
                f"{','.join(sorted(normalized_targets))}"
            ),
        )

    reason = rationale.strip() if isinstance(rationale, str) else ""
    if not reason:
        reason = "Based on current discussion and inconsistencies."

    await messaging.direct.send(
        to_task_id=agent_ctx.parent_task_id,
        content=f"{agent_ctx.display_name} submitted a day vote.",
        data={
            "kind": "day_vote",
            "round_no": agent_ctx.round_no,
            "voter_id": agent_ctx.player_id,
            "target_player_id": candidate,
            "rationale": reason,
        },
    )
    agent_ctx.day_vote_allowed_targets = []
    agent_ctx.phase = "await_night_action"
    return _result(f"Submitted day vote for {candidate}.")


@tool
def wait_for_night_action(agent_ctx: MafiaPlayerContext) -> WaitInstruction:
    """
    Wait for current-round night action signal.
    """
    return _wait_for_signal(
        agent_ctx,
        signal_id=f"night_action_open:{agent_ctx.round_no}",
        next_phase="night_action_action",
        waiting_phase="await_night_action",
        timeout_seconds=180,
    )


@tool
async def submit_night_action(
    agent_ctx: MafiaPlayerContext,
    target_player_id: str | None = None,
    rationale: str | None = None,
) -> PlayerActionResult:
    """
    Werewolf submits a night target; villagers pass through night.
    """
    payload = _signal_payload()
    if payload.get("kind") != "night_action_open":
        agent_ctx.phase = "await_night_action"
        return _result("Night signal was unavailable; waiting again.")

    round_no = _coerce_round_no(payload.get("round_no"))
    if round_no is not None:
        agent_ctx.round_no = round_no

    if agent_ctx.role != "werewolf":
        agent_ctx.round_no += 1
        agent_ctx.phase = "await_day_discussion"
        return _result("Villager stayed passive through the night.")

    allowed_targets = payload.get("allowed_targets")
    normalized_targets = (
        [item for item in allowed_targets if isinstance(item, str)]
        if isinstance(allowed_targets, list)
        else []
    )
    if not normalized_targets:
        agent_ctx.round_no += 1
        agent_ctx.phase = "await_day_discussion"
        return _result("No valid night targets were available.")

    candidate = target_player_id.strip() if isinstance(target_player_id, str) else ""
    if candidate not in normalized_targets:
        candidate = _deterministic_choice(
            normalized_targets,
            seed=(
                f"{agent_ctx.player_id}:night_action:{agent_ctx.round_no}:"
                f"{','.join(sorted(normalized_targets))}"
            ),
        )

    reason = rationale.strip() if isinstance(rationale, str) else ""
    if not reason:
        reason = "Target appears less trusted by the town."

    await messaging.direct.send(
        to_task_id=agent_ctx.parent_task_id,
        content=f"{agent_ctx.display_name} submitted a night action.",
        data={
            "kind": "night_action",
            "round_no": agent_ctx.round_no,
            "voter_id": agent_ctx.player_id,
            "target_player_id": candidate,
            "rationale": reason,
        },
    )
    wolf_message = f"{agent_ctx.display_name}: I selected a night target."
    await messaging.group.send(
        agent_ctx.wolf_group_name,
        wolf_message,
        data={
            "kind": "wolf_chat",
            "round_no": agent_ctx.round_no,
            "player_id": agent_ctx.player_id,
        },
    )

    agent_ctx.round_no += 1
    agent_ctx.phase = "await_day_discussion"
    return _result(
        f"Submitted night action for {candidate}.",
        channel="wolf",
        message=wolf_message,
    )


_PLAYER_PHASE_TOOL = {
    "await_game_start": "wait_for_game_start",
    "configure_after_start": "configure_after_start",
    "await_day_discussion": "wait_for_day_discussion",
    "day_discussion_action": "send_public_statement",
    "await_day_vote": "wait_for_day_vote",
    "decide_day_vote_or_discussion": "decide_day_vote_or_discussion",
    "day_vote_choice": "choose_day_vote_action",
    "day_vote_poll": "poll_day_vote",
    "day_vote_action": "submit_day_vote",
    "await_night_action": "wait_for_night_action",
    "night_action_action": "submit_night_action",
}


def _player_tool_choice(agent_ctx: MafiaPlayerContext) -> str | dict[str, Any]:
    phase = str(getattr(agent_ctx, "phase", ""))
    tool_name = _PLAYER_PHASE_TOOL.get(phase)
    if tool_name:
        return {"type": "function", "function": {"name": tool_name}}
    return "auto"


PLAYER_TOOLS = [
    wait_for_game_start,
    configure_after_start,
    wait_for_day_discussion,
    send_public_statement,
    wait_for_day_vote,
    decide_day_vote_or_discussion,
    choose_day_vote_action,
    poll_day_vote,
    submit_day_vote,
    wait_for_night_action,
    submit_night_action,
]


__all__ = [
    "wait_for_game_start",
    "configure_after_start",
    "wait_for_day_discussion",
    "send_public_statement",
    "wait_for_day_vote",
    "decide_day_vote_or_discussion",
    "choose_day_vote_action",
    "poll_day_vote",
    "submit_day_vote",
    "wait_for_night_action",
    "submit_night_action",
    "_player_tool_choice",
    "PLAYER_TOOLS",
]
