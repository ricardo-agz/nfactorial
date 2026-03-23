from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, TypeAlias

from constants import TOWN_GROUP_NAME, WOLF_GROUP_NAME
from pydantic import BaseModel, Field

from factorial import AgentContext


class PlayerRecord(BaseModel):
    player_id: str
    display_name: str
    is_human: bool = False
    task_id: str | None = None
    role: Literal["villager", "werewolf"] = "villager"
    alive: bool = True


class VoteRecord(BaseModel):
    voter_id: str
    voter_display_name: str
    target_player_id: str
    target_display_name: str


class DayVoteHistoryEntry(BaseModel):
    round_no: int
    eliminated_player_id: str | None = None
    eliminated_display_name: str | None = None
    votes: list[VoteRecord] = Field(default_factory=list)


class GameStateSnapshot(BaseModel):
    phase: str
    round_no: int
    phase_deadline_ts: float | None = None
    winner: str | None = None
    winner_reason: str | None = None
    alive_total: int
    alive_villagers: int
    alive_werewolves: int
    vote_calls_received: int = 0
    vote_calls_threshold: int = 0
    players_public: list[dict[str, Any]]
    players_omniscient: list[dict[str, Any]]
    human_player_id: str | None = None
    human_private_role: str | None = None
    current_day_votes: list[VoteRecord] = Field(default_factory=list)
    day_vote_history: list[DayVoteHistoryEntry] = Field(default_factory=list)
    elimination_log: list[dict[str, Any]] = Field(default_factory=list)


class GameActionResult(BaseModel):
    summary: str
    game_state: GameStateSnapshot
    channel: str | None = None
    message: str | None = None


class PlayerActionResult(BaseModel):
    summary: str
    channel: str | None = None
    message: str | None = None


class FinalGameOutput(BaseModel):
    final_output: str


class PlayerFinalOutput(BaseModel):
    final_output: str


@dataclass
class MafiaGameState:
    """State for the game master agent."""

    phase: str = "init"
    game_name: str = "Mafia in nfactorial"
    include_human: bool = True
    human_name: str = "You"
    human_role_preference: Literal["random", "werewolf", "villager"] = "random"
    ai_player_count: int = 7
    day_discussion_seconds: int = 90
    day_vote_seconds: int = 35
    night_seconds: int = 25
    round_no: int = 1
    town_group_name: str = TOWN_GROUP_NAME
    wolf_group_name: str = WOLF_GROUP_NAME
    players: list[PlayerRecord] = field(default_factory=list)
    task_id_to_player_id: dict[str, str] = field(default_factory=dict)
    pending_vote_calls: dict[str, bool] = field(default_factory=dict)
    pending_day_votes: dict[str, str] = field(default_factory=dict)
    pending_night_actions: dict[str, str] = field(default_factory=dict)
    day_discussion_deadline_ts: float | None = None
    day_vote_deadline_ts: float | None = None
    night_deadline_ts: float | None = None
    winner: str | None = None
    winner_reason: str | None = None
    elimination_log: list[dict[str, Any]] = field(default_factory=list)
    query: str = ""


@dataclass
class MafiaPlayerState:
    """State for a player agent."""

    player_id: str = ""
    display_name: str = ""
    parent_task_id: str = ""
    role: Literal["villager", "werewolf"] = "villager"
    phase: str = "await_day_discussion"
    round_no: int = 1
    town_group_name: str = TOWN_GROUP_NAME
    wolf_group_name: str = WOLF_GROUP_NAME
    discussion_round_no: int = 0
    discussion_messages_sent: int = 0
    has_called_vote: bool = False
    day_vote_allowed_targets: list[str] = field(default_factory=list)
    day_vote_deadline_ts: float | None = None
    night_kill_allowed_targets: list[str] = field(default_factory=list)
    night_alive_werewolf_count: int = 1


# Context aliases for agent tool signatures
MafiaGameContext: TypeAlias = AgentContext[MafiaGameState]
MafiaPlayerContext: TypeAlias = AgentContext[MafiaPlayerState]


__all__ = [
    "PlayerRecord",
    "VoteRecord",
    "DayVoteHistoryEntry",
    "GameStateSnapshot",
    "GameActionResult",
    "PlayerActionResult",
    "FinalGameOutput",
    "PlayerFinalOutput",
    "MafiaGameState",
    "MafiaPlayerState",
    "MafiaGameContext",
    "MafiaPlayerContext",
]
