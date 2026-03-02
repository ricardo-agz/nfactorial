from __future__ import annotations

import random
import time
from collections import Counter
from typing import Any

from factorial import (
    ExecutionContext,
    WaitInstruction,
    inbox,
    messaging,
    subagents,
    tool,
    wait,
)

from constants import HUMAN_PLAYER_ID
from models import (
    GameActionResult,
    GameStateSnapshot,
    MafiaGameContext,
    MafiaPlayerContext,
    PlayerRecord,
)

_player_agent_ref: Any | None = None


def _normalize_ai_player_count(value: int) -> int:
    if value < 3:
        return 3
    if value > 10:
        return 10
    return value


def _normalize_timeout_seconds(value: int, *, default: int) -> int:
    if value < 10:
        return default
    if value > 300:
        return 300
    return value


def _player_by_id(agent_ctx: MafiaGameContext, player_id: str) -> PlayerRecord | None:
    for player in agent_ctx.players:
        if player.player_id == player_id:
            return player
    return None


def _human_player(agent_ctx: MafiaGameContext) -> PlayerRecord | None:
    return _player_by_id(agent_ctx, HUMAN_PLAYER_ID)


def _alive_players(agent_ctx: MafiaGameContext) -> list[PlayerRecord]:
    return [player for player in agent_ctx.players if player.alive]


def _alive_player_ids(agent_ctx: MafiaGameContext) -> list[str]:
    return [player.player_id for player in _alive_players(agent_ctx)]


def _alive_ai_task_ids(agent_ctx: MafiaGameContext) -> list[str]:
    return [
        player.task_id
        for player in _alive_players(agent_ctx)
        if player.task_id is not None and not player.is_human
    ]


def _alive_werewolf_ids(agent_ctx: MafiaGameContext) -> list[str]:
    return [
        player.player_id
        for player in _alive_players(agent_ctx)
        if player.role == "werewolf"
    ]


def _alive_villager_ids(agent_ctx: MafiaGameContext) -> list[str]:
    return [
        player.player_id
        for player in _alive_players(agent_ctx)
        if player.role == "villager"
    ]


def _deterministic_choice(options: list[str], seed: str) -> str:
    rng = random.Random(seed)
    return rng.choice(options)


def _phase_deadline_ts(agent_ctx: MafiaGameContext) -> float | None:
    phase = agent_ctx.phase
    if phase in {"open_day_discussion", "wait_day_discussion_window"}:
        return agent_ctx.day_discussion_deadline_ts
    if phase in {"open_day_vote", "collect_day_votes"}:
        return agent_ctx.day_vote_deadline_ts
    if phase in {"open_night_action", "collect_night_actions"}:
        return agent_ctx.night_deadline_ts
    return None


def _state_snapshot(agent_ctx: MafiaGameContext) -> GameStateSnapshot:
    alive_werewolves = sum(
        1 for player in agent_ctx.players if player.alive and player.role == "werewolf"
    )
    alive_villagers = sum(
        1 for player in agent_ctx.players if player.alive and player.role == "villager"
    )
    players_public: list[dict[str, Any]] = []
    players_omniscient: list[dict[str, Any]] = []
    for player in agent_ctx.players:
        reveal_role = player.role if (agent_ctx.winner or not player.alive) else None
        players_public.append(
            {
                "player_id": player.player_id,
                "display_name": player.display_name,
                "is_human": player.is_human,
                "task_id": player.task_id,
                "alive": player.alive,
                "role": reveal_role,
            }
        )
        players_omniscient.append(
            {
                "player_id": player.player_id,
                "display_name": player.display_name,
                "is_human": player.is_human,
                "task_id": player.task_id,
                "alive": player.alive,
                "role": player.role,
            }
        )
    human = _human_player(agent_ctx)
    human_role = human.role if human else None
    return GameStateSnapshot(
        phase=agent_ctx.phase,
        round_no=agent_ctx.round_no,
        phase_deadline_ts=_phase_deadline_ts(agent_ctx),
        winner=agent_ctx.winner,
        winner_reason=agent_ctx.winner_reason,
        alive_total=alive_werewolves + alive_villagers,
        alive_villagers=alive_villagers,
        alive_werewolves=alive_werewolves,
        players_public=players_public,
        players_omniscient=players_omniscient,
        human_player_id=(HUMAN_PLAYER_ID if human else None),
        human_private_role=human_role,
        elimination_log=list(agent_ctx.elimination_log),
    )


def _make_result(
    agent_ctx: MafiaGameContext,
    *,
    summary: str,
    channel: str | None = None,
    message: str | None = None,
) -> GameActionResult:
    return GameActionResult(
        summary=summary,
        game_state=_state_snapshot(agent_ctx),
        channel=channel,
        message=message,
    )


async def _cancel_all_ai_children(agent_ctx: MafiaGameContext) -> None:
    task_ids = [
        player.task_id
        for player in agent_ctx.players
        if player.task_id is not None and not player.is_human
    ]
    if not task_ids:
        return
    try:
        await subagents.cancel(task_ids)
    except Exception:
        # Cancellation is best effort at game over.
        pass


async def _mark_player_eliminated(
    agent_ctx: MafiaGameContext,
    *,
    player_id: str,
    reason: str,
) -> PlayerRecord | None:
    player = _player_by_id(agent_ctx, player_id)
    if player is None or not player.alive:
        return None
    player.alive = False
    agent_ctx.elimination_log.append(
        {
            "round_no": agent_ctx.round_no,
            "player_id": player.player_id,
            "display_name": player.display_name,
            "role": player.role,
            "reason": reason,
        }
    )

    if player.task_id:
        try:
            await messaging.groups.remove_members(
                agent_ctx.town_group_name,
                [player.task_id],
            )
        except Exception:
            pass
        try:
            await messaging.groups.remove_members(
                agent_ctx.wolf_group_name,
                [player.task_id],
            )
        except Exception:
            pass
        try:
            await subagents.cancel(player.task_id)
        except Exception:
            pass
    return player


def _apply_win_condition(agent_ctx: MafiaGameContext) -> bool:
    alive_werewolves = len(_alive_werewolf_ids(agent_ctx))
    alive_villagers = len(_alive_villager_ids(agent_ctx))
    if alive_werewolves == 0:
        agent_ctx.winner = "villagers"
        agent_ctx.winner_reason = "All werewolves were eliminated."
        agent_ctx.phase = "game_over"
        return True
    if alive_werewolves >= alive_villagers:
        agent_ctx.winner = "werewolves"
        agent_ctx.winner_reason = "Werewolves reached parity with villagers."
        agent_ctx.phase = "game_over"
        return True
    return False


def _coerce_round_no(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _resolve_voter_id(
    agent_ctx: MafiaGameContext,
    message: Any,
    payload: dict[str, Any],
) -> str | None:
    from_task_id = getattr(message, "from_task_id", None)
    if isinstance(from_task_id, str) and from_task_id in agent_ctx.task_id_to_player_id:
        return agent_ctx.task_id_to_player_id[from_task_id]

    from_owner_id = getattr(message, "from_owner_id", None)
    if isinstance(from_owner_id, str) and _human_player(agent_ctx):
        return HUMAN_PLAYER_ID

    candidate = payload.get("voter_id")
    if isinstance(candidate, str) and _player_by_id(agent_ctx, candidate):
        return candidate
    return None


async def _drain_day_votes(agent_ctx: MafiaGameContext) -> dict[str, str]:
    accepted: dict[str, str] = {}
    cursor: str | None = None
    while True:
        page = await inbox.direct.peek(unread_only=True, limit=100, cursor=cursor)
        consumed_message_ids: list[str] = []
        for message in page.messages:
            payload = message.data if isinstance(message.data, dict) else {}
            if payload.get("kind") != "day_vote":
                continue
            consumed_message_ids.append(message.message_id)
            round_no = _coerce_round_no(payload.get("round_no"))
            if round_no is not None and round_no != agent_ctx.round_no:
                continue
            voter_id = _resolve_voter_id(agent_ctx, message, payload)
            if voter_id is None:
                continue
            voter = _player_by_id(agent_ctx, voter_id)
            if voter is None or not voter.alive:
                continue
            if voter_id in agent_ctx.pending_day_votes or voter_id in accepted:
                continue
            target_id = payload.get("target_player_id")
            if not isinstance(target_id, str):
                continue
            target = _player_by_id(agent_ctx, target_id)
            if target is None or not target.alive:
                continue
            if target.player_id == voter.player_id:
                continue
            accepted[voter_id] = target_id
        if consumed_message_ids:
            await inbox.direct.mark_read(
                message_ids=consumed_message_ids,
                notify_sender=False,
            )
        if not page.has_more or not page.next_cursor:
            break
        cursor = page.next_cursor
    return accepted


async def _drain_night_actions(agent_ctx: MafiaGameContext) -> dict[str, str]:
    accepted: dict[str, str] = {}
    cursor: str | None = None
    while True:
        page = await inbox.direct.peek(unread_only=True, limit=100, cursor=cursor)
        consumed_message_ids: list[str] = []
        for message in page.messages:
            payload = message.data if isinstance(message.data, dict) else {}
            if payload.get("kind") != "night_action":
                continue
            consumed_message_ids.append(message.message_id)
            round_no = _coerce_round_no(payload.get("round_no"))
            if round_no is not None and round_no != agent_ctx.round_no:
                continue
            voter_id = _resolve_voter_id(agent_ctx, message, payload)
            if voter_id is None:
                continue
            voter = _player_by_id(agent_ctx, voter_id)
            if voter is None or not voter.alive or voter.role != "werewolf":
                continue
            if voter_id in agent_ctx.pending_night_actions or voter_id in accepted:
                continue
            target_id = payload.get("target_player_id")
            if not isinstance(target_id, str):
                continue
            target = _player_by_id(agent_ctx, target_id)
            if target is None or not target.alive or target.role == "werewolf":
                continue
            accepted[voter_id] = target_id
        if consumed_message_ids:
            await inbox.direct.mark_read(
                message_ids=consumed_message_ids,
                notify_sender=False,
            )
        if not page.has_more or not page.next_cursor:
            break
        cursor = page.next_cursor
    return accepted


def set_player_agent_for_game_master(player_agent: Any) -> None:
    global _player_agent_ref
    _player_agent_ref = player_agent


@tool
async def setup_game(agent_ctx: MafiaGameContext) -> GameActionResult:
    """
    Spawn AI players, assign hidden roles, and initialize mafia channels.
    """
    if agent_ctx.players:
        return _make_result(agent_ctx, summary="Game is already initialized.")

    if _player_agent_ref is None:
        raise RuntimeError(
            "Player agent is not configured. "
            "Call set_player_agent_for_game_master(...) during startup."
        )

    agent_ctx.ai_player_count = _normalize_ai_player_count(agent_ctx.ai_player_count)
    agent_ctx.day_discussion_seconds = _normalize_timeout_seconds(
        agent_ctx.day_discussion_seconds,
        default=25,
    )
    agent_ctx.day_vote_seconds = _normalize_timeout_seconds(
        agent_ctx.day_vote_seconds,
        default=35,
    )
    agent_ctx.night_seconds = _normalize_timeout_seconds(
        agent_ctx.night_seconds,
        default=25,
    )

    parent_task_id = ExecutionContext.current().task_id
    players: list[PlayerRecord] = []
    player_jobs_for_group: list[dict[str, Any]] = []
    task_id_to_player_id: dict[str, str] = {}

    for index in range(agent_ctx.ai_player_count):
        player_id = f"ai_player_{index + 1}"
        display_name = f"Agent {index + 1}"
        payload = MafiaPlayerContext(
            query=agent_ctx.query,
            player_id=player_id,
            display_name=display_name,
            parent_task_id=parent_task_id,
            phase="await_game_start",
            round_no=1,
            town_group_name=agent_ctx.town_group_name,
            wolf_group_name=agent_ctx.wolf_group_name,
        )
        job = (
            await subagents.spawn(
                agent=_player_agent_ref,
                inputs=[payload],
                key=f"{parent_task_id}:{player_id}",
            )
        )[0]
        player_jobs_for_group.append(job.to_dict())
        task_id_to_player_id[job.task_id] = player_id
        players.append(
            PlayerRecord(
                player_id=player_id,
                display_name=display_name,
                is_human=False,
                task_id=job.task_id,
                role="villager",
                alive=True,
            )
        )

    if agent_ctx.include_human:
        normalized_human_name = agent_ctx.human_name.strip()
        human_name = normalized_human_name if normalized_human_name else "You"
        players.append(
            PlayerRecord(
                player_id=HUMAN_PLAYER_ID,
                display_name=human_name,
                is_human=True,
                role="villager",
                alive=True,
            )
        )

    candidate_ids = [player.player_id for player in players]
    ai_candidate_ids = [player.player_id for player in players if not player.is_human]
    random_seed = f"{parent_task_id}:{agent_ctx.query}:{agent_ctx.game_name}"
    randomizer = random.Random(random_seed)

    preferred = agent_ctx.human_role_preference
    if agent_ctx.include_human and preferred == "werewolf":
        werewolf_id = HUMAN_PLAYER_ID
    elif agent_ctx.include_human and preferred == "villager" and ai_candidate_ids:
        werewolf_id = randomizer.choice(ai_candidate_ids)
    else:
        werewolf_id = randomizer.choice(candidate_ids)

    for player in players:
        player.role = "werewolf" if player.player_id == werewolf_id else "villager"

    await messaging.groups.create(
        agent_ctx.town_group_name,
        members=player_jobs_for_group,
    )
    await messaging.groups.create(agent_ctx.wolf_group_name, members=[])

    werewolf_task_ids = [
        player.task_id
        for player in players
        if player.task_id and player.role == "werewolf"
    ]
    if werewolf_task_ids:
        await messaging.groups.add_members(agent_ctx.wolf_group_name, werewolf_task_ids)

    werewolf_ids = [player.player_id for player in players if player.role == "werewolf"]
    player_brief = [
        {
            "player_id": player.player_id,
            "display_name": player.display_name,
            "is_human": player.is_human,
        }
        for player in players
    ]
    for player in players:
        if not player.task_id:
            continue
        role_message = (
            "You are the WEREWOLF. Survive, deceive, and eliminate villagers."
            if player.role == "werewolf"
            else "You are a VILLAGER. Find and vote out the werewolf."
        )
        await messaging.direct.send(
            player.task_id,
            role_message,
            data={
                "kind": "role_assignment",
                "player_id": player.player_id,
                "role": player.role,
                "werewolf_ids": werewolf_ids,
                "round_no": 1,
                "players": player_brief,
            },
        )

    await subagents.signal(
        list(task_id_to_player_id.keys()),
        signal_id="game_start",
        payload={
            "kind": "game_start",
            "round_no": 1,
            "town_group_name": agent_ctx.town_group_name,
            "wolf_group_name": agent_ctx.wolf_group_name,
        },
    )

    kickoff_message = (
        f"{agent_ctx.game_name}: setup complete. "
        f"{len(players)} players entered the town square."
    )
    await messaging.group.send(
        agent_ctx.town_group_name,
        kickoff_message,
        data={
            "kind": "system_announcement",
            "phase": "setup",
            "round_no": 1,
        },
    )

    agent_ctx.players = players
    agent_ctx.task_id_to_player_id = task_id_to_player_id
    agent_ctx.pending_day_votes = {}
    agent_ctx.pending_night_actions = {}
    agent_ctx.day_discussion_deadline_ts = None
    agent_ctx.day_vote_deadline_ts = None
    agent_ctx.night_deadline_ts = None
    agent_ctx.round_no = 1
    agent_ctx.phase = "open_day_discussion"
    agent_ctx.winner = None
    agent_ctx.winner_reason = None
    agent_ctx.elimination_log = []

    return _make_result(
        agent_ctx,
        summary="Initialized mafia game and assigned hidden roles.",
        channel="town",
        message=kickoff_message,
    )


@tool
async def open_day_discussion(agent_ctx: MafiaGameContext) -> GameActionResult:
    """
    Open the day discussion window and signal all alive AI players.
    """
    alive_ids = _alive_player_ids(agent_ctx)
    alive_ai_task_ids = _alive_ai_task_ids(agent_ctx)
    last_elimination = (
        agent_ctx.elimination_log[-1] if agent_ctx.elimination_log else None
    )
    agent_ctx.day_discussion_deadline_ts = (
        time.time() + float(agent_ctx.day_discussion_seconds)
    )
    await subagents.signal(
        alive_ai_task_ids,
        signal_id=f"day_discussion_open:{agent_ctx.round_no}",
        payload={
            "kind": "day_discussion_open",
            "round_no": agent_ctx.round_no,
            "alive_player_ids": alive_ids,
            "last_eliminated_player_id": (
                last_elimination.get("player_id")
                if isinstance(last_elimination, dict)
                else None
            ),
            "last_eliminated_display_name": (
                last_elimination.get("display_name")
                if isinstance(last_elimination, dict)
                else None
            ),
        },
    )
    announcement = (
        f"Round {agent_ctx.round_no}: Day discussion is open "
        f"for {agent_ctx.day_discussion_seconds} seconds."
    )
    await messaging.group.send(
        agent_ctx.town_group_name,
        announcement,
        data={
            "kind": "system_announcement",
            "phase": "day_discussion",
            "round_no": agent_ctx.round_no,
        },
    )
    agent_ctx.phase = "wait_day_discussion_window"
    return _make_result(
        agent_ctx,
        summary="Opened day discussion and signaled alive players.",
        channel="town",
        message=announcement,
    )


@tool
def wait_day_discussion_window(agent_ctx: MafiaGameContext) -> WaitInstruction:
    """
    Pause the game master while players discuss publicly.
    """
    agent_ctx.phase = "open_day_vote"
    return wait.sleep(
        float(agent_ctx.day_discussion_seconds),
        data={
            "phase": "day_discussion_window",
            "round_no": agent_ctx.round_no,
        },
    )


@tool
async def open_day_vote(agent_ctx: MafiaGameContext) -> GameActionResult:
    """
    Open day vote collection and signal all alive AI players.
    """
    alive_ids = _alive_player_ids(agent_ctx)
    alive_ai_task_ids = _alive_ai_task_ids(agent_ctx)
    agent_ctx.pending_day_votes = {}
    agent_ctx.day_discussion_deadline_ts = None
    agent_ctx.day_vote_deadline_ts = time.time() + float(agent_ctx.day_vote_seconds)

    await subagents.signal(
        alive_ai_task_ids,
        signal_id=f"day_vote_open:{agent_ctx.round_no}",
        payload={
            "kind": "day_vote_open",
            "round_no": agent_ctx.round_no,
            "allowed_targets": alive_ids,
        },
    )
    message_text = (
        f"Round {agent_ctx.round_no}: Voting is open for {agent_ctx.day_vote_seconds} "
        "seconds. Submit exactly one vote."
    )
    await messaging.group.send(
        agent_ctx.town_group_name,
        message_text,
        data={
            "kind": "system_announcement",
            "phase": "day_vote_open",
            "round_no": agent_ctx.round_no,
        },
    )

    agent_ctx.phase = "collect_day_votes"
    return _make_result(
        agent_ctx,
        summary="Opened day voting and started vote collection.",
        channel="town",
        message=message_text,
    )


@tool
async def collect_day_votes(
    agent_ctx: MafiaGameContext,
) -> WaitInstruction | GameActionResult:
    """
    Collect valid day votes from AI + human via direct inbox data payloads.
    """
    accepted = await _drain_day_votes(agent_ctx)
    if accepted:
        agent_ctx.pending_day_votes.update(accepted)

    expected_voters = _alive_player_ids(agent_ctx)
    expected_count = len(expected_voters)
    current_count = len(agent_ctx.pending_day_votes)

    deadline = agent_ctx.day_vote_deadline_ts or time.time()
    remaining = max(0.0, deadline - time.time())

    if expected_count == 0 or current_count >= expected_count or remaining <= 0:
        agent_ctx.day_vote_deadline_ts = None
        agent_ctx.phase = "resolve_day_vote"
        summary = (
            "Day vote collection complete "
            f"({current_count}/{expected_count} votes)."
        )
        return _make_result(agent_ctx, summary=summary)

    return wait.activity(
        timeout=wait.sleep(remaining),
        data={
            "phase": "collect_day_votes",
            "round_no": agent_ctx.round_no,
            "expected_submissions": expected_count,
            "received_submissions": current_count,
        },
    )


@tool
async def resolve_day_vote(agent_ctx: MafiaGameContext) -> GameActionResult:
    """
    Resolve the day vote and eliminate one player.
    """
    alive_ids = _alive_player_ids(agent_ctx)
    if not alive_ids:
        agent_ctx.phase = "game_over"
        agent_ctx.winner = "none"
        agent_ctx.winner_reason = "No alive players remained."
        await _cancel_all_ai_children(agent_ctx)
        return _make_result(agent_ctx, summary="No alive players remained.")

    if not agent_ctx.pending_day_votes:
        chosen_player_id = _deterministic_choice(
            alive_ids,
            seed=f"day_vote_fallback:{agent_ctx.round_no}:{','.join(sorted(alive_ids))}",
        )
    else:
        tally = Counter(agent_ctx.pending_day_votes.values())
        top_votes = max(tally.values())
        tied_player_ids = sorted(
            player_id for player_id, count in tally.items() if count == top_votes
        )
        chosen_player_id = _deterministic_choice(
            tied_player_ids,
            seed=(
                f"day_vote_tiebreak:{agent_ctx.round_no}:"
                + ",".join(
                    f"{voter_id}->{target_id}"
                    for voter_id, target_id in sorted(
                        agent_ctx.pending_day_votes.items()
                    )
                )
            ),
        )

    eliminated = await _mark_player_eliminated(
        agent_ctx,
        player_id=chosen_player_id,
        reason="day_vote",
    )
    eliminated_label = (
        eliminated.display_name if eliminated else f"Unknown ({chosen_player_id})"
    )
    eliminated_role = eliminated.role if eliminated else "unknown"

    announcement = (
        f"Round {agent_ctx.round_no}: {eliminated_label} was voted out. "
        f"They were a {eliminated_role}."
    )
    await messaging.group.send(
        agent_ctx.town_group_name,
        announcement,
        data={
            "kind": "day_resolution",
            "round_no": agent_ctx.round_no,
            "eliminated_player_id": chosen_player_id,
            "eliminated_role": eliminated_role,
        },
    )

    agent_ctx.pending_day_votes = {}
    agent_ctx.day_vote_deadline_ts = None

    winner_declared = _apply_win_condition(agent_ctx)
    if winner_declared:
        await _cancel_all_ai_children(agent_ctx)
        winner_announcement = (
            f"Game over: {agent_ctx.winner} win. {agent_ctx.winner_reason}"
        )
        await messaging.group.send(
            agent_ctx.town_group_name,
            winner_announcement,
            data={
                "kind": "game_over",
                "round_no": agent_ctx.round_no,
                "winner": agent_ctx.winner,
            },
        )
        return _make_result(
            agent_ctx,
            summary="Resolved day vote and reached game over.",
            channel="town",
            message=winner_announcement,
        )

    agent_ctx.phase = "open_night_action"
    return _make_result(
        agent_ctx,
        summary="Resolved day vote and advanced to night phase.",
        channel="town",
        message=announcement,
    )


@tool
async def open_night_action(agent_ctx: MafiaGameContext) -> GameActionResult:
    """
    Open night action phase and signal alive AI players.
    """
    alive_ai_task_ids = _alive_ai_task_ids(agent_ctx)
    allowed_targets = _alive_villager_ids(agent_ctx)

    agent_ctx.pending_night_actions = {}
    agent_ctx.day_vote_deadline_ts = None
    agent_ctx.night_deadline_ts = time.time() + float(agent_ctx.night_seconds)

    await subagents.signal(
        alive_ai_task_ids,
        signal_id=f"night_action_open:{agent_ctx.round_no}",
        payload={
            "kind": "night_action_open",
            "round_no": agent_ctx.round_no,
            "allowed_targets": allowed_targets,
        },
    )
    message_text = (
        f"Round {agent_ctx.round_no}: Night falls for {agent_ctx.night_seconds} "
        "seconds. Werewolves choose a target."
    )
    await messaging.group.send(
        agent_ctx.town_group_name,
        message_text,
        data={
            "kind": "night_open",
            "round_no": agent_ctx.round_no,
        },
    )
    agent_ctx.phase = "collect_night_actions"
    return _make_result(
        agent_ctx,
        summary="Opened night action collection.",
        channel="town",
        message=message_text,
    )


@tool
async def collect_night_actions(
    agent_ctx: MafiaGameContext,
) -> WaitInstruction | GameActionResult:
    """
    Collect werewolf night actions from AI + human via direct inbox payloads.
    """
    accepted = await _drain_night_actions(agent_ctx)
    if accepted:
        agent_ctx.pending_night_actions.update(accepted)

    expected_voters = _alive_werewolf_ids(agent_ctx)
    expected_count = len(expected_voters)
    current_count = len(agent_ctx.pending_night_actions)
    # This implementation has exactly one werewolf, so one valid action is decisive.
    has_decisive_action = current_count > 0

    deadline = agent_ctx.night_deadline_ts or time.time()
    remaining = max(0.0, deadline - time.time())

    if expected_count == 0 or has_decisive_action or remaining <= 0:
        agent_ctx.night_deadline_ts = None
        agent_ctx.phase = "resolve_night_action"
        if has_decisive_action and remaining > 0:
            summary = (
                "Night action received; resolving immediately "
                f"({current_count}/{expected_count} actions)."
            )
        else:
            summary = (
                "Night action collection complete "
                f"({current_count}/{expected_count} actions)."
            )
        return _make_result(
            agent_ctx,
            summary=summary,
        )

    return wait.activity(
        timeout=wait.sleep(remaining),
        data={
            "phase": "collect_night_actions",
            "round_no": agent_ctx.round_no,
            "expected_submissions": expected_count,
            "received_submissions": current_count,
        },
    )


@tool
async def resolve_night_action(agent_ctx: MafiaGameContext) -> GameActionResult:
    """
    Resolve night action and apply werewolf elimination.
    """
    alive_villagers = _alive_villager_ids(agent_ctx)
    chosen_target: str | None = None
    if alive_villagers:
        if agent_ctx.pending_night_actions:
            tally = Counter(agent_ctx.pending_night_actions.values())
            top_votes = max(tally.values())
            tied_target_ids = sorted(
                player_id for player_id, count in tally.items() if count == top_votes
            )
            chosen_target = _deterministic_choice(
                tied_target_ids,
                seed=(
                    f"night_action_tiebreak:{agent_ctx.round_no}:"
                    + ",".join(
                        f"{voter_id}->{target_id}"
                        for voter_id, target_id in sorted(
                            agent_ctx.pending_night_actions.items()
                        )
                    )
                ),
            )
        else:
            chosen_target = _deterministic_choice(
                alive_villagers,
                seed=(
                    f"night_action_fallback:{agent_ctx.round_no}:"
                    f"{','.join(sorted(alive_villagers))}"
                ),
            )

    if chosen_target:
        eliminated = await _mark_player_eliminated(
            agent_ctx,
            player_id=chosen_target,
            reason="night_action",
        )
        eliminated_label = (
            eliminated.display_name if eliminated else f"Unknown ({chosen_target})"
        )
        dawn_message = (
            f"Dawn of round {agent_ctx.round_no}: {eliminated_label} did not survive "
            "the night."
        )
    else:
        dawn_message = (
            f"Dawn of round {agent_ctx.round_no}: no valid night target was available."
        )

    await messaging.group.send(
        agent_ctx.town_group_name,
        dawn_message,
        data={
            "kind": "night_resolution",
            "round_no": agent_ctx.round_no,
            "target_player_id": chosen_target,
        },
    )

    agent_ctx.pending_night_actions = {}
    agent_ctx.night_deadline_ts = None

    winner_declared = _apply_win_condition(agent_ctx)
    if winner_declared:
        await _cancel_all_ai_children(agent_ctx)
        winner_announcement = (
            f"Game over: {agent_ctx.winner} win. {agent_ctx.winner_reason}"
        )
        await messaging.group.send(
            agent_ctx.town_group_name,
            winner_announcement,
            data={
                "kind": "game_over",
                "round_no": agent_ctx.round_no,
                "winner": agent_ctx.winner,
            },
        )
        return _make_result(
            agent_ctx,
            summary="Resolved night action and reached game over.",
            channel="town",
            message=winner_announcement,
        )

    agent_ctx.round_no += 1
    agent_ctx.phase = "open_day_discussion"
    return _make_result(
        agent_ctx,
        summary="Resolved night action and advanced to the next day.",
        channel="town",
        message=dawn_message,
    )


_GM_PHASE_TOOL = {
    "init": "setup_game",
    "open_day_discussion": "open_day_discussion",
    "wait_day_discussion_window": "wait_day_discussion_window",
    "open_day_vote": "open_day_vote",
    "collect_day_votes": "collect_day_votes",
    "resolve_day_vote": "resolve_day_vote",
    "open_night_action": "open_night_action",
    "collect_night_actions": "collect_night_actions",
    "resolve_night_action": "resolve_night_action",
}


def _gm_tool_choice(agent_ctx: MafiaGameContext) -> str | dict[str, Any]:
    phase = str(getattr(agent_ctx, "phase", ""))
    if phase == "game_over":
        return "none"
    tool_name = _GM_PHASE_TOOL.get(phase)
    if tool_name:
        return {"type": "function", "function": {"name": tool_name}}
    return "auto"


GAME_MASTER_TOOLS = [
    setup_game,
    open_day_discussion,
    wait_day_discussion_window,
    open_day_vote,
    collect_day_votes,
    resolve_day_vote,
    open_night_action,
    collect_night_actions,
    resolve_night_action,
]


__all__ = [
    "setup_game",
    "open_day_discussion",
    "wait_day_discussion_window",
    "open_day_vote",
    "collect_day_votes",
    "resolve_day_vote",
    "open_night_action",
    "collect_night_actions",
    "resolve_night_action",
    "_gm_tool_choice",
    "GAME_MASTER_TOOLS",
    "set_player_agent_for_game_master",
]
