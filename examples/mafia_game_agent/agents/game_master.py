from __future__ import annotations

import math
import os
import random
import time
from collections import Counter
from functools import lru_cache
from typing import Any

import redis.asyncio as redis
from constants import HUMAN_PLAYER_ID
from models import (
    DayVoteHistoryEntry,
    GameActionResult,
    GameStateSnapshot,
    MafiaGameContext,
    MafiaPlayerState,
    PlayerRecord,
    VoteRecord,
)

from factorial import (
    ExecutionContext,
    FatalAgentError,
    WaitInstruction,
    messaging,
    subagents,
    tool,
    wait,
)
from factorial.queue.operations.messaging import (
    messaging_inbox_direct_mark_read,
    messaging_inbox_direct_peek,
)

_player_agent_ref: Any | None = None
PLAYER_ACTIVE_POLL_SECONDS = 5
PLAYER_AWAIT_SIGNAL_SECONDS = 180
NIGHT_COLLECTION_POLL_SECONDS = 1.0
DAY_VOTE_COLLECTION_POLL_SECONDS = 1.0
VOTE_CALL_POLL_SECONDS = 2.0

_NAME_POOL = [
    "Marcus",
    "Elena",
    "Kai",
    "Sofia",
    "Dmitri",
    "Luna",
    "Oscar",
    "Nadia",
    "Felix",
    "Aria",
    "Viktor",
    "Mira",
    "Jasper",
    "Zara",
    "Theo",
    "Leila",
]

_TRAIT_LABELS: dict[str, list[str]] = {
    "aggression": [
        "very passive and avoids conflict",
        "mild-mannered and dislikes confrontation",
        "balanced -- you push back only when it matters",
        "assertive and unafraid to accuse others",
        "fiery and confrontational, quick to call people out",
    ],
    "suspicion": [
        "extremely trusting, you give everyone the benefit of the doubt",
        "fairly trusting, slow to suspect others",
        "cautiously open-minded",
        "naturally suspicious, you look for inconsistencies",
        "deeply paranoid, you question everyone's motives",
    ],
    "talkativeness": [
        "very quiet, you only speak when you have something important to say",
        "reserved, you keep messages brief",
        "moderate speaker",
        "chatty, you enjoy engaging in conversation",
        "very vocal, you dominate discussions and react to everything",
    ],
    "leadership": [
        "a pure follower who goes along with the group",
        "usually follows but occasionally offers input",
        "flexible -- you lead or follow depending on the situation",
        "naturally takes charge and proposes plans",
        "a strong leader who organizes the group and drives strategy",
    ],
    "humor": [
        "dead serious at all times",
        "mostly serious with rare dry remarks",
        "has a normal sense of humor",
        "lighthearted and witty, often cracks jokes",
        "a constant joker who uses humor to defuse or deflect",
    ],
}


def _generate_personality(rng: random.Random) -> dict[str, int]:
    return {trait: rng.randint(1, 5) for trait in _TRAIT_LABELS}


def _personality_description(traits: dict[str, int]) -> str:
    lines: list[str] = []
    for trait, level in traits.items():
        labels = _TRAIT_LABELS[trait]
        lines.append(f"- {labels[level - 1].capitalize()}.")
    return "\n".join(lines)


def _normalize_ai_player_count(value: int) -> int:
    if value < 3:
        return 3
    if value > 15:
        return 15
    return value


def _normalize_timeout_seconds(value: int, *, default: int) -> int:
    if value < 10:
        return default
    if value > 300:
        return 300
    return value


def _build_player_spawn_query(
    *,
    game_name: str,
    display_name: str,
    player_id: str,
    role: str,
    personality: str,
    all_players: list[dict[str, str]],
    day_discussion_seconds: int,
    day_vote_seconds: int,
    night_seconds: int,
) -> str:
    role_label = role.upper()
    role_contract = (
        "- Night role contract: you are a werewolf. Use kill() every night.\n"
        "- You may use chat_with_werewolves() and night poll() only when more than\n"
        "  one werewolf is alive."
        if role == "werewolf"
        else (
            "- Night role contract: you are a villager. At night, only poll() "
            "and wait."
        )
    )
    roster_lines = "\n".join(
        f"  - {p['display_name']} (player_id={p['player_id']})"
        for p in all_players
    )
    return (
        f"You are {display_name} (player_id={player_id}) in {game_name}.\n"
        f"Your hidden role is {role_label}.\n\n"
        f"Your personality:\n{personality}\n"
        "Stay in character throughout the game. Your personality should come "
        "through in how you speak, when you choose to speak, and how you react "
        "to others.\n\n"
        f"Player roster ({len(all_players)} players):\n"
        f"{roster_lines}\n\n"
        "IMPORTANT: Always use the exact player_id values above when voting or "
        "targeting players. Refer to players by their display_name in chat.\n\n"
        "Runtime configuration:\n"
        f"- Day discussion window: {day_discussion_seconds} seconds.\n"
        f"- Day vote window: {day_vote_seconds} seconds.\n"
        f"- Night window: {night_seconds} seconds.\n"
        f"- poll() timing: {PLAYER_ACTIVE_POLL_SECONDS}s in active phases, up to "
        f"{PLAYER_AWAIT_SIGNAL_SECONDS}s while waiting for next phase-open signal.\n\n"
        "Action contract:\n"
        "- Day discussion: use chat(), think(), poll(), or call_vote().\n"
        "- Day discussion: use call_vote() when you feel the discussion has been "
        "sufficient. Once a majority of alive players call, voting begins.\n"
        "- Day vote: you MUST submit exactly one vote() before the vote window "
        "closes.\n"
        "- If uncertain, still submit vote() with your best target.\n"
        f"{role_contract}\n"
        "- Keep chat messages concise and believable.\n"
        "- Never reveal your hidden role in town chat unless eliminated or game over."
    )


def _player_by_id(agent_ctx: MafiaGameContext, player_id: str) -> PlayerRecord | None:
    for player in agent_ctx.state.players:
        if player.player_id == player_id:
            return player
    return None


def _human_player(agent_ctx: MafiaGameContext) -> PlayerRecord | None:
    return _player_by_id(agent_ctx, HUMAN_PLAYER_ID)


def _alive_players(agent_ctx: MafiaGameContext) -> list[PlayerRecord]:
    return [player for player in agent_ctx.state.players if player.alive]


def _alive_player_ids(agent_ctx: MafiaGameContext) -> list[str]:
    return [player.player_id for player in _alive_players(agent_ctx)]


def _alive_ai_task_ids(agent_ctx: MafiaGameContext) -> list[str]:
    return [
        player.task_id
        for player in _alive_players(agent_ctx)
        if player.task_id is not None and not player.is_human
    ]


def _player_roster(agent_ctx: MafiaGameContext) -> list[dict[str, Any]]:
    return [
        {
            "player_id": p.player_id,
            "display_name": p.display_name,
            "alive": p.alive,
        }
        for p in agent_ctx.state.players
    ]


def _player_display_name(agent_ctx: MafiaGameContext, player_id: str) -> str:
    player = _player_by_id(agent_ctx, player_id)
    return player.display_name if player is not None else player_id


def _vote_records(
    agent_ctx: MafiaGameContext,
    votes: dict[str, str],
) -> list[VoteRecord]:
    return [
        VoteRecord(
            voter_id=voter_id,
            voter_display_name=_player_display_name(agent_ctx, voter_id),
            target_player_id=target_id,
            target_display_name=_player_display_name(agent_ctx, target_id),
        )
        for voter_id, target_id in sorted(votes.items())
    ]


def _day_vote_history(agent_ctx: MafiaGameContext) -> list[DayVoteHistoryEntry]:
    history: list[DayVoteHistoryEntry] = []
    for entry in agent_ctx.state.elimination_log:
        if not isinstance(entry, dict) or entry.get("reason") != "day_vote":
            continue
        raw_votes = entry.get("votes")
        history.append(
            DayVoteHistoryEntry(
                round_no=_coerce_round_no(entry.get("round_no")) or 0,
                eliminated_player_id=entry.get("player_id"),
                eliminated_display_name=entry.get("display_name"),
                votes=raw_votes if isinstance(raw_votes, list) else [],
            )
        )
    return history


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


@lru_cache(maxsize=1)
def _direct_inbox_redis_client() -> redis.Redis:
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


def _message_field(message: Any, field_name: str) -> Any:
    if isinstance(message, dict):
        return message.get(field_name)
    return getattr(message, field_name, None)


async def _peek_direct_inbox_page(
    *,
    unread_only: bool = True,
    limit: int = 100,
    cursor: str | None = None,
) -> dict[str, Any]:
    execution_ctx = ExecutionContext.current()
    return await messaging_inbox_direct_peek(
        redis_client=_direct_inbox_redis_client(),
        namespace=os.getenv("FACTORIAL_NAMESPACE", "factorial"),
        task_id=execution_ctx.task_id,
        unread_only=unread_only,
        limit=limit,
        cursor=cursor,
    )


async def _mark_direct_inbox_read(message_ids: list[str]) -> None:
    if not message_ids:
        return
    execution_ctx = ExecutionContext.current()
    await messaging_inbox_direct_mark_read(
        redis_client=_direct_inbox_redis_client(),
        namespace=os.getenv("FACTORIAL_NAMESPACE", "factorial"),
        task_id=execution_ctx.task_id,
        message_ids=message_ids,
        notify_sender=False,
        data=None,
    )


def _phase_deadline_ts(agent_ctx: MafiaGameContext) -> float | None:
    phase = agent_ctx.state.phase
    if phase in {"open_day_discussion", "collect_vote_calls"}:
        return agent_ctx.state.day_discussion_deadline_ts
    if phase in {"open_day_vote", "collect_day_votes"}:
        return agent_ctx.state.day_vote_deadline_ts
    if phase in {"open_night_action", "collect_night_actions"}:
        return agent_ctx.state.night_deadline_ts
    return None


def _state_snapshot(agent_ctx: MafiaGameContext) -> GameStateSnapshot:
    alive_werewolves = sum(
        1
        for player in agent_ctx.state.players
        if player.alive and player.role == "werewolf"
    )
    alive_villagers = sum(
        1
        for player in agent_ctx.state.players
        if player.alive and player.role == "villager"
    )
    players_public: list[dict[str, Any]] = []
    players_omniscient: list[dict[str, Any]] = []
    for player in agent_ctx.state.players:
        reveal_role = (
            player.role if (agent_ctx.state.winner or not player.alive) else None
        )
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
    alive_total = alive_werewolves + alive_villagers
    return GameStateSnapshot(
        phase=agent_ctx.state.phase,
        round_no=agent_ctx.state.round_no,
        phase_deadline_ts=_phase_deadline_ts(agent_ctx),
        winner=agent_ctx.state.winner,
        winner_reason=agent_ctx.state.winner_reason,
        alive_total=alive_total,
        alive_villagers=alive_villagers,
        alive_werewolves=alive_werewolves,
        vote_calls_received=len(agent_ctx.state.pending_vote_calls),
        vote_calls_threshold=math.ceil(alive_total / 2) if alive_total > 0 else 0,
        players_public=players_public,
        players_omniscient=players_omniscient,
        human_player_id=(HUMAN_PLAYER_ID if human else None),
        human_private_role=human_role,
        current_day_votes=_vote_records(agent_ctx, agent_ctx.state.pending_day_votes),
        day_vote_history=_day_vote_history(agent_ctx),
        elimination_log=list(agent_ctx.state.elimination_log),
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


def _vote_call_announcement(
    agent_ctx: MafiaGameContext,
    *,
    caller_id: str,
    current_count: int,
    threshold: int,
) -> str:
    caller = _player_by_id(agent_ctx, caller_id)
    caller_name = caller.display_name if caller else caller_id
    return f"{caller_name} has called for a vote ({current_count}/{threshold} needed)."


async def _cancel_all_ai_children(agent_ctx: MafiaGameContext) -> None:
    task_ids = [
        player.task_id
        for player in agent_ctx.state.players
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
    agent_ctx.state.elimination_log.append(
        {
            "round_no": agent_ctx.state.round_no,
            "player_id": player.player_id,
            "display_name": player.display_name,
            "role": player.role,
            "reason": reason,
        }
    )

    if player.task_id:
        try:
            await messaging.groups.remove_members(
                agent_ctx.state.town_group_name,
                [player.task_id],
            )
        except Exception:
            pass
        try:
            await messaging.groups.remove_members(
                agent_ctx.state.wolf_group_name,
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
        agent_ctx.state.winner = "villagers"
        agent_ctx.state.winner_reason = "All werewolves were eliminated."
        agent_ctx.state.phase = "game_over"
        return True
    if alive_werewolves >= alive_villagers:
        agent_ctx.state.winner = "werewolves"
        agent_ctx.state.winner_reason = "Werewolves reached parity with villagers."
        agent_ctx.state.phase = "game_over"
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
    from_task_id = _message_field(message, "from_task_id")
    if (
        isinstance(from_task_id, str)
        and from_task_id in agent_ctx.state.task_id_to_player_id
    ):
        return agent_ctx.state.task_id_to_player_id[from_task_id]

    from_owner_id = _message_field(message, "from_owner_id")
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
        page = await _peek_direct_inbox_page(
            unread_only=True,
            limit=100,
            cursor=cursor,
        )
        consumed_message_ids: list[str] = []
        messages = page.get("messages", [])
        if not isinstance(messages, list):
            break
        for message in messages:
            payload = message.get("data") if isinstance(message, dict) else None
            payload = payload if isinstance(payload, dict) else {}
            if payload.get("kind") != "day_vote":
                continue
            message_id = _message_field(message, "message_id")
            if not isinstance(message_id, str):
                continue
            consumed_message_ids.append(message_id)
            round_no = _coerce_round_no(payload.get("round_no"))
            if round_no is not None and round_no != agent_ctx.state.round_no:
                continue
            voter_id = _resolve_voter_id(agent_ctx, message, payload)
            if voter_id is None:
                continue
            voter = _player_by_id(agent_ctx, voter_id)
            if voter is None or not voter.alive:
                continue
            if voter_id in agent_ctx.state.pending_day_votes or voter_id in accepted:
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
            await _mark_direct_inbox_read(consumed_message_ids)
        next_cursor = page.get("next_cursor")
        if not page.get("has_more") or not isinstance(next_cursor, str):
            break
        cursor = next_cursor
    return accepted


async def _drain_night_actions(agent_ctx: MafiaGameContext) -> dict[str, str]:
    accepted: dict[str, str] = {}
    cursor: str | None = None
    while True:
        page = await _peek_direct_inbox_page(
            unread_only=True,
            limit=100,
            cursor=cursor,
        )
        consumed_message_ids: list[str] = []
        messages = page.get("messages", [])
        if not isinstance(messages, list):
            break
        for message in messages:
            payload = message.get("data") if isinstance(message, dict) else None
            payload = payload if isinstance(payload, dict) else {}
            if payload.get("kind") != "night_action":
                continue
            message_id = _message_field(message, "message_id")
            if not isinstance(message_id, str):
                continue
            consumed_message_ids.append(message_id)
            round_no = _coerce_round_no(payload.get("round_no"))
            if round_no is not None and round_no != agent_ctx.state.round_no:
                continue
            voter_id = _resolve_voter_id(agent_ctx, message, payload)
            if voter_id is None:
                continue
            voter = _player_by_id(agent_ctx, voter_id)
            if voter is None or not voter.alive or voter.role != "werewolf":
                continue
            if (
                voter_id in agent_ctx.state.pending_night_actions
                or voter_id in accepted
            ):
                continue
            target_id = payload.get("target_player_id")
            if not isinstance(target_id, str):
                continue
            target = _player_by_id(agent_ctx, target_id)
            if target is None or not target.alive or target.role == "werewolf":
                continue
            accepted[voter_id] = target_id
        if consumed_message_ids:
            await _mark_direct_inbox_read(consumed_message_ids)
        next_cursor = page.get("next_cursor")
        if not page.get("has_more") or not isinstance(next_cursor, str):
            break
        cursor = next_cursor
    return accepted


async def _drain_vote_calls(agent_ctx: MafiaGameContext) -> set[str]:
    accepted: set[str] = set()
    cursor: str | None = None
    while True:
        page = await _peek_direct_inbox_page(
            unread_only=True,
            limit=100,
            cursor=cursor,
        )
        consumed_message_ids: list[str] = []
        messages = page.get("messages", [])
        if not isinstance(messages, list):
            break
        for message in messages:
            payload = message.get("data") if isinstance(message, dict) else None
            payload = payload if isinstance(payload, dict) else {}
            if payload.get("kind") != "call_vote":
                continue
            message_id = _message_field(message, "message_id")
            if not isinstance(message_id, str):
                continue
            consumed_message_ids.append(message_id)
            round_no = _coerce_round_no(payload.get("round_no"))
            if round_no is not None and round_no != agent_ctx.state.round_no:
                continue
            voter_id = _resolve_voter_id(agent_ctx, message, payload)
            if voter_id is None:
                continue
            voter = _player_by_id(agent_ctx, voter_id)
            if voter is None or not voter.alive:
                continue
            if voter_id in agent_ctx.state.pending_vote_calls:
                continue
            accepted.add(voter_id)
        if consumed_message_ids:
            await _mark_direct_inbox_read(consumed_message_ids)
        next_cursor = page.get("next_cursor")
        if not page.get("has_more") or not isinstance(next_cursor, str):
            break
        cursor = next_cursor
    return accepted


def set_player_agent_for_game_master(player_agent: Any) -> None:
    global _player_agent_ref
    _player_agent_ref = player_agent


@tool
async def setup_game(agent_ctx: MafiaGameContext) -> GameActionResult:
    """
    Spawn AI players, assign hidden roles, and initialize mafia channels.
    """
    if agent_ctx.state.players:
        return _make_result(agent_ctx, summary="Game is already initialized.")

    if _player_agent_ref is None:
        raise RuntimeError(
            "Player agent is not configured. "
            "Call set_player_agent_for_game_master(...) during startup."
        )

    agent_ctx.state.ai_player_count = _normalize_ai_player_count(
        agent_ctx.state.ai_player_count
    )
    agent_ctx.state.day_discussion_seconds = _normalize_timeout_seconds(
        agent_ctx.state.day_discussion_seconds,
        default=90,
    )
    agent_ctx.state.day_vote_seconds = _normalize_timeout_seconds(
        agent_ctx.state.day_vote_seconds,
        default=35,
    )
    agent_ctx.state.night_seconds = _normalize_timeout_seconds(
        agent_ctx.state.night_seconds,
        default=25,
    )

    parent_task_id = ExecutionContext.current().task_id
    players: list[PlayerRecord] = []
    player_jobs_for_group: list[dict[str, Any]] = []
    task_id_to_player_id: dict[str, str] = {}
    random_seed = (
        f"{parent_task_id}:{agent_ctx.state.query}:{agent_ctx.state.game_name}"
    )
    randomizer = random.Random(random_seed)

    shuffled_names = list(_NAME_POOL)
    randomizer.shuffle(shuffled_names)
    ai_specs: list[tuple[str, str, dict[str, int]]] = []
    for index in range(agent_ctx.state.ai_player_count):
        pid = f"ai_player_{index + 1}"
        name = shuffled_names[index % len(shuffled_names)]
        traits = _generate_personality(randomizer)
        ai_specs.append((pid, name, traits))

    ai_candidate_ids = [pid for pid, _, _ in ai_specs]
    normalized_human_name = agent_ctx.state.human_name.strip()
    human_name = normalized_human_name if normalized_human_name else "You"
    candidate_ids = list(ai_candidate_ids)
    if agent_ctx.state.include_human:
        candidate_ids.append(HUMAN_PLAYER_ID)

    preferred = agent_ctx.state.human_role_preference
    if agent_ctx.state.include_human and preferred == "werewolf":
        werewolf_id = HUMAN_PLAYER_ID
    elif agent_ctx.state.include_human and preferred == "villager" and ai_candidate_ids:
        werewolf_id = randomizer.choice(ai_candidate_ids)
    else:
        werewolf_id = randomizer.choice(candidate_ids)

    all_players_roster: list[dict[str, str]] = [
        {"player_id": pid, "display_name": dname} for pid, dname, _ in ai_specs
    ]
    if agent_ctx.state.include_human:
        all_players_roster.append(
            {"player_id": HUMAN_PLAYER_ID, "display_name": human_name}
        )

    for player_id, display_name, traits in ai_specs:
        role = "werewolf" if player_id == werewolf_id else "villager"
        personality = _personality_description(traits)
        player_state = MafiaPlayerState(
            player_id=player_id,
            display_name=display_name,
            parent_task_id=parent_task_id,
            role=role,
            phase="await_day_discussion",
            round_no=1,
            town_group_name=agent_ctx.state.town_group_name,
            wolf_group_name=agent_ctx.state.wolf_group_name,
        )
        query = _build_player_spawn_query(
            game_name=agent_ctx.state.game_name,
            display_name=display_name,
            player_id=player_id,
            role=role,
            personality=personality,
            all_players=all_players_roster,
            day_discussion_seconds=agent_ctx.state.day_discussion_seconds,
            day_vote_seconds=agent_ctx.state.day_vote_seconds,
            night_seconds=agent_ctx.state.night_seconds,
        )
        payload = _player_agent_ref.build_context(input=query, state=player_state)
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
                role=role,
                alive=True,
            )
        )

    if agent_ctx.state.include_human:
        players.append(
            PlayerRecord(
                player_id=HUMAN_PLAYER_ID,
                display_name=human_name,
                is_human=True,
                role=("werewolf" if werewolf_id == HUMAN_PLAYER_ID else "villager"),
                alive=True,
            )
        )

    await messaging.groups.create(
        agent_ctx.state.town_group_name,
        members=player_jobs_for_group,
    )
    await messaging.groups.create(agent_ctx.state.wolf_group_name, members=[])

    werewolf_task_ids = [
        player.task_id
        for player in players
        if player.task_id and player.role == "werewolf"
    ]
    if werewolf_task_ids:
        await messaging.groups.add_members(
            agent_ctx.state.wolf_group_name,
            werewolf_task_ids,
        )

    kickoff_message = (
        f"{agent_ctx.state.game_name}: setup complete. "
        f"{len(players)} players entered the town square."
    )
    await messaging.group.send(
        agent_ctx.state.town_group_name,
        kickoff_message,
        data={
            "kind": "system_announcement",
            "phase": "setup",
            "round_no": 1,
        },
    )

    agent_ctx.state.players = players
    agent_ctx.state.task_id_to_player_id = task_id_to_player_id
    agent_ctx.state.pending_vote_calls = {}
    agent_ctx.state.pending_day_votes = {}
    agent_ctx.state.pending_night_actions = {}
    agent_ctx.state.day_discussion_deadline_ts = None
    agent_ctx.state.day_vote_deadline_ts = None
    agent_ctx.state.night_deadline_ts = None
    agent_ctx.state.round_no = 1
    agent_ctx.state.phase = "open_day_discussion"
    agent_ctx.state.winner = None
    agent_ctx.state.winner_reason = None
    agent_ctx.state.elimination_log = []

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
        agent_ctx.state.elimination_log[-1] if agent_ctx.state.elimination_log else None
    )
    agent_ctx.state.day_discussion_deadline_ts = (
        time.time() + float(agent_ctx.state.day_discussion_seconds)
    )
    await subagents.signal(
        alive_ai_task_ids,
        signal_id=f"day_discussion_open:{agent_ctx.state.round_no}",
        payload={
            "kind": "day_discussion_open",
            "round_no": agent_ctx.state.round_no,
            "alive_player_ids": alive_ids,
            "player_roster": _player_roster(agent_ctx),
            "elimination_log": list(agent_ctx.state.elimination_log),
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
            "last_eliminated_role": (
                last_elimination.get("role")
                if isinstance(last_elimination, dict)
                else None
            ),
        },
    )
    alive_names = [p.display_name for p in _alive_players(agent_ctx)]
    alive_count = len(alive_names)
    vote_call_threshold = math.ceil(alive_count / 2)
    announcement = (
        f"Round {agent_ctx.state.round_no}: Day discussion is open "
        f"(up to {agent_ctx.state.day_discussion_seconds}s). "
        f"Alive players: {', '.join(alive_names)}. "
        f"Use call_vote when ready ({vote_call_threshold}/{alive_count} needed to "
        f"start voting)."
    )
    await messaging.group.send(
        agent_ctx.state.town_group_name,
        announcement,
        data={
            "kind": "system_announcement",
            "phase": "day_discussion",
            "round_no": agent_ctx.state.round_no,
            "alive_player_ids": alive_ids,
        },
    )
    agent_ctx.state.pending_vote_calls = {}
    agent_ctx.state.phase = "collect_vote_calls"
    return _make_result(
        agent_ctx,
        summary="Opened day discussion and signaled alive players.",
        channel="town",
        message=announcement,
    )


@tool
async def collect_vote_calls(
    agent_ctx: MafiaGameContext,
) -> WaitInstruction | GameActionResult:
    """
    Poll for call_vote messages during the discussion window.
    Transitions to voting once a majority of alive players have called,
    or when the discussion timer expires.
    """
    new_calls = await _drain_vote_calls(agent_ctx)
    newly_announced: list[str] = []
    for caller_id in new_calls:
        if caller_id not in agent_ctx.state.pending_vote_calls:
            agent_ctx.state.pending_vote_calls[caller_id] = True
            newly_announced.append(caller_id)

    alive_count = len(_alive_player_ids(agent_ctx))
    threshold = math.ceil(alive_count / 2)
    current_count = len(agent_ctx.state.pending_vote_calls)

    for caller_id in newly_announced:
        await messaging.group.send(
            agent_ctx.state.town_group_name,
            _vote_call_announcement(
                agent_ctx,
                caller_id=caller_id,
                current_count=current_count,
                threshold=threshold,
            ),
            data={
                "kind": "system_announcement",
                "phase": "vote_call",
                "round_no": agent_ctx.state.round_no,
                "caller_player_id": caller_id,
                "vote_calls_received": current_count,
                "vote_calls_threshold": threshold,
            },
        )

    deadline = agent_ctx.state.day_discussion_deadline_ts or time.time()
    remaining = max(0.0, deadline - time.time())
    threshold_reached = current_count >= threshold

    if threshold_reached or remaining <= 0:
        agent_ctx.state.day_discussion_deadline_ts = None
        agent_ctx.state.phase = "open_day_vote"
        reason = (
            "Majority called for a vote."
            if threshold_reached
            else "Discussion time expired."
        )
        summary = (
            f"Discussion ended: {reason} "
            f"({current_count}/{threshold} vote calls received)."
        )
        return _make_result(agent_ctx, summary=summary)

    if newly_announced:
        announcement_text = " ".join(
            _vote_call_announcement(
                agent_ctx,
                caller_id=caller_id,
                current_count=current_count,
                threshold=threshold,
            )
            for caller_id in newly_announced
        )
        return _make_result(
            agent_ctx,
            summary=(
                f"Registered {len(newly_announced)} new vote call(s) "
                f"({current_count}/{threshold})."
            ),
            channel="town",
            message=announcement_text,
        )

    poll_sleep = min(remaining, VOTE_CALL_POLL_SECONDS)
    return wait.activity(
        timeout=wait.sleep(poll_sleep),
        data={
            "phase": "collect_vote_calls",
            "round_no": agent_ctx.state.round_no,
            "vote_calls_received": current_count,
            "vote_calls_threshold": threshold,
        },
    )


@tool
async def open_day_vote(agent_ctx: MafiaGameContext) -> GameActionResult:
    """
    Open day vote collection and signal all alive AI players.
    """
    alive_ids = _alive_player_ids(agent_ctx)
    alive_ai_task_ids = _alive_ai_task_ids(agent_ctx)
    agent_ctx.state.pending_day_votes = {}
    agent_ctx.state.day_discussion_deadline_ts = None
    agent_ctx.state.day_vote_deadline_ts = time.time() + float(
        agent_ctx.state.day_vote_seconds
    )

    await subagents.signal(
        alive_ai_task_ids,
        signal_id=f"day_vote_open:{agent_ctx.state.round_no}",
        payload={
            "kind": "day_vote_open",
            "round_no": agent_ctx.state.round_no,
            "allowed_targets": alive_ids,
            "player_roster": _player_roster(agent_ctx),
            "deadline_ts": agent_ctx.state.day_vote_deadline_ts,
        },
    )
    alive_names = [p.display_name for p in _alive_players(agent_ctx)]
    message_text = (
        f"Round {agent_ctx.state.round_no}: Voting is open for "
        f"{agent_ctx.state.day_vote_seconds} "
        f"seconds. Submit exactly one vote. "
        f"Alive players: {', '.join(alive_names)}."
    )
    await messaging.group.send(
        agent_ctx.state.town_group_name,
        message_text,
        data={
            "kind": "system_announcement",
            "phase": "day_vote_open",
            "round_no": agent_ctx.state.round_no,
            "alive_player_ids": alive_ids,
        },
    )

    agent_ctx.state.phase = "collect_day_votes"
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
        agent_ctx.state.pending_day_votes.update(accepted)

    expected_voters = _alive_player_ids(agent_ctx)
    expected_count = len(expected_voters)
    current_count = len(agent_ctx.state.pending_day_votes)

    deadline = agent_ctx.state.day_vote_deadline_ts or time.time()
    remaining = max(0.0, deadline - time.time())

    if expected_count == 0 or current_count >= expected_count or remaining <= 0:
        agent_ctx.state.day_vote_deadline_ts = None
        agent_ctx.state.phase = "resolve_day_vote"
        summary = (
            "Day vote collection complete "
            f"({current_count}/{expected_count} votes)."
        )
        return _make_result(agent_ctx, summary=summary)

    poll_sleep_seconds = min(remaining, DAY_VOTE_COLLECTION_POLL_SECONDS)
    return wait.activity(
        timeout=wait.sleep(poll_sleep_seconds),
        data={
            "phase": "collect_day_votes",
            "round_no": agent_ctx.state.round_no,
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
        agent_ctx.state.phase = "game_over"
        agent_ctx.state.winner = "none"
        agent_ctx.state.winner_reason = "No alive players remained."
        await _cancel_all_ai_children(agent_ctx)
        return _make_result(agent_ctx, summary="No alive players remained.")

    if not agent_ctx.state.pending_day_votes:
        raise FatalAgentError(
            "No day votes were collected for the current round. "
            "Refusing to eliminate a random player."
        )
    vote_records = _vote_records(agent_ctx, agent_ctx.state.pending_day_votes)
    tally = Counter(agent_ctx.state.pending_day_votes.values())
    top_votes = max(tally.values())
    tied_player_ids = sorted(
        player_id for player_id, count in tally.items() if count == top_votes
    )
    chosen_player_id = _deterministic_choice(
        tied_player_ids,
        seed=(
            f"day_vote_tiebreak:{agent_ctx.state.round_no}:"
            + ",".join(
                f"{voter_id}->{target_id}"
                for voter_id, target_id in sorted(
                    agent_ctx.state.pending_day_votes.items()
                )
            )
        )
    )

    eliminated = await _mark_player_eliminated(
        agent_ctx,
        player_id=chosen_player_id,
        reason="day_vote",
    )
    if agent_ctx.state.elimination_log:
        latest_entry = agent_ctx.state.elimination_log[-1]
        if (
            isinstance(latest_entry, dict)
            and latest_entry.get("reason") == "day_vote"
            and latest_entry.get("round_no") == agent_ctx.state.round_no
        ):
            latest_entry["votes"] = vote_records
    eliminated_label = (
        eliminated.display_name if eliminated else f"Unknown ({chosen_player_id})"
    )
    eliminated_role = eliminated.role if eliminated else "unknown"

    announcement = (
        f"Round {agent_ctx.state.round_no}: {eliminated_label} was voted out. "
        f"They were a {eliminated_role}."
    )
    await messaging.group.send(
        agent_ctx.state.town_group_name,
        announcement,
        data={
            "kind": "day_resolution",
            "round_no": agent_ctx.state.round_no,
            "eliminated_player_id": chosen_player_id,
            "eliminated_role": eliminated_role,
        },
    )

    agent_ctx.state.pending_day_votes = {}
    agent_ctx.state.day_vote_deadline_ts = None

    winner_declared = _apply_win_condition(agent_ctx)
    if winner_declared:
        await _cancel_all_ai_children(agent_ctx)
        winner_announcement = (
            f"Game over: {agent_ctx.state.winner} win. {agent_ctx.state.winner_reason}"
        )
        await messaging.group.send(
            agent_ctx.state.town_group_name,
            winner_announcement,
            data={
                "kind": "game_over",
                "round_no": agent_ctx.state.round_no,
                "winner": agent_ctx.state.winner,
            },
        )
        return _make_result(
            agent_ctx,
            summary="Resolved day vote and reached game over.",
            channel="town",
            message=winner_announcement,
        )

    agent_ctx.state.phase = "open_night_action"
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
    alive_werewolf_count = len(_alive_werewolf_ids(agent_ctx))

    agent_ctx.state.pending_night_actions = {}
    agent_ctx.state.day_vote_deadline_ts = None
    agent_ctx.state.night_deadline_ts = time.time() + float(
        agent_ctx.state.night_seconds
    )

    await subagents.signal(
        alive_ai_task_ids,
        signal_id=f"night_action_open:{agent_ctx.state.round_no}",
        payload={
            "kind": "night_action_open",
            "round_no": agent_ctx.state.round_no,
            "allowed_targets": allowed_targets,
            "player_roster": _player_roster(agent_ctx),
            "alive_werewolf_count": alive_werewolf_count,
        },
    )
    message_text = (
        f"Round {agent_ctx.state.round_no}: Night falls for "
        f"{agent_ctx.state.night_seconds} "
        "seconds. Werewolves choose a target."
    )
    await messaging.group.send(
        agent_ctx.state.town_group_name,
        message_text,
        data={
            "kind": "night_open",
            "round_no": agent_ctx.state.round_no,
        },
    )
    agent_ctx.state.phase = "collect_night_actions"
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
        agent_ctx.state.pending_night_actions.update(accepted)

    expected_voters = _alive_werewolf_ids(agent_ctx)
    expected_count = len(expected_voters)
    current_count = len(agent_ctx.state.pending_night_actions)
    has_decisive_action = expected_count <= 1 and current_count > 0
    all_expected_actions_received = (
        expected_count > 1 and current_count >= expected_count
    )

    deadline = agent_ctx.state.night_deadline_ts or time.time()
    remaining = max(0.0, deadline - time.time())

    if (
        expected_count == 0
        or has_decisive_action
        or all_expected_actions_received
        or remaining <= 0
    ):
        agent_ctx.state.night_deadline_ts = None
        agent_ctx.state.phase = "resolve_night_action"
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

    poll_sleep_seconds = min(remaining, NIGHT_COLLECTION_POLL_SECONDS)
    return wait.activity(
        timeout=wait.sleep(poll_sleep_seconds),
        data={
            "phase": "collect_night_actions",
            "round_no": agent_ctx.state.round_no,
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
        if agent_ctx.state.pending_night_actions:
            tally = Counter(agent_ctx.state.pending_night_actions.values())
            top_votes = max(tally.values())
            tied_target_ids = sorted(
                player_id for player_id, count in tally.items() if count == top_votes
            )
            chosen_target = _deterministic_choice(
                tied_target_ids,
                seed=(
                    f"night_action_tiebreak:{agent_ctx.state.round_no}:"
                    + ",".join(
                        f"{voter_id}->{target_id}"
                        for voter_id, target_id in sorted(
                            agent_ctx.state.pending_night_actions.items()
                        )
                    )
                ),
            )
        else:
            raise FatalAgentError(
                "No night actions were collected for the current round. "
                "Refusing to eliminate a random villager."
            )

    eliminated_role: str | None = None
    if chosen_target:
        eliminated = await _mark_player_eliminated(
            agent_ctx,
            player_id=chosen_target,
            reason="night_action",
        )
        eliminated_label = (
            eliminated.display_name if eliminated else f"Unknown ({chosen_target})"
        )
        eliminated_role = eliminated.role if eliminated else "unknown"
        dawn_message = (
            f"Dawn of round {agent_ctx.state.round_no}: "
            f"{eliminated_label} did not survive "
            f"the night. They were a {eliminated_role}."
        )
    else:
        dawn_message = (
            f"Dawn of round {agent_ctx.state.round_no}: "
            "no valid night target was available."
        )

    await messaging.group.send(
        agent_ctx.state.town_group_name,
        dawn_message,
        data={
            "kind": "night_resolution",
            "round_no": agent_ctx.state.round_no,
            "target_player_id": chosen_target,
            "eliminated_role": eliminated_role,
        },
    )

    agent_ctx.state.pending_night_actions = {}
    agent_ctx.state.night_deadline_ts = None

    winner_declared = _apply_win_condition(agent_ctx)
    if winner_declared:
        await _cancel_all_ai_children(agent_ctx)
        winner_announcement = (
            f"Game over: {agent_ctx.state.winner} win. {agent_ctx.state.winner_reason}"
        )
        await messaging.group.send(
            agent_ctx.state.town_group_name,
            winner_announcement,
            data={
                "kind": "game_over",
                "round_no": agent_ctx.state.round_no,
                "winner": agent_ctx.state.winner,
            },
        )
        return _make_result(
            agent_ctx,
            summary="Resolved night action and reached game over.",
            channel="town",
            message=winner_announcement,
        )

    agent_ctx.state.round_no += 1
    agent_ctx.state.phase = "open_day_discussion"
    return _make_result(
        agent_ctx,
        summary="Resolved night action and advanced to the next day.",
        channel="town",
        message=dawn_message,
    )


_GM_PHASE_TOOL = {
    "init": "setup_game",
    "open_day_discussion": "open_day_discussion",
    "collect_vote_calls": "collect_vote_calls",
    "open_day_vote": "open_day_vote",
    "collect_day_votes": "collect_day_votes",
    "resolve_day_vote": "resolve_day_vote",
    "open_night_action": "open_night_action",
    "collect_night_actions": "collect_night_actions",
    "resolve_night_action": "resolve_night_action",
}


def _gm_tool_choice(agent_ctx: MafiaGameContext) -> str | dict[str, Any]:
    phase = str(agent_ctx.state.phase)
    if phase == "game_over":
        return "none"
    tool_name = _GM_PHASE_TOOL.get(phase)
    if tool_name:
        return {"type": "function", "function": {"name": tool_name}}
    raise RuntimeError(f"Unexpected game master phase {phase!r}.")


GAME_MASTER_TOOLS = [
    setup_game,
    open_day_discussion,
    collect_vote_calls,
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
    "collect_vote_calls",
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
