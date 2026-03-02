from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from factorial import AgentContext

from constants import HUMAN_PLAYER_ID, TOWN_GROUP_NAME, WOLF_GROUP_NAME


class PlayerRecord(BaseModel):
    player_id: str
    display_name: str
    is_human: bool = False
    task_id: str | None = None
    role: Literal["villager", "werewolf"] = "villager"
    alive: bool = True


class GameStateSnapshot(BaseModel):
    phase: str
    round_no: int
    phase_deadline_ts: float | None = None
    winner: str | None = None
    winner_reason: str | None = None
    alive_total: int
    alive_villagers: int
    alive_werewolves: int
    players_public: list[dict[str, Any]]
    players_omniscient: list[dict[str, Any]]
    human_player_id: str | None = None
    human_private_role: str | None = None
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


class MafiaGameContext(AgentContext):
    phase: str = "init"
    game_name: str = "Mafia in nfactorial"
    include_human: bool = True
    human_name: str = "You"
    human_role_preference: Literal["random", "werewolf", "villager"] = "random"
    ai_player_count: int = 5
    day_discussion_seconds: int = 25
    day_vote_seconds: int = 35
    night_seconds: int = 25
    round_no: int = 1
    town_group_name: str = TOWN_GROUP_NAME
    wolf_group_name: str = WOLF_GROUP_NAME
    players: list[PlayerRecord] = Field(default_factory=list)
    task_id_to_player_id: dict[str, str] = Field(default_factory=dict)
    pending_day_votes: dict[str, str] = Field(default_factory=dict)
    pending_night_actions: dict[str, str] = Field(default_factory=dict)
    day_discussion_deadline_ts: float | None = None
    day_vote_deadline_ts: float | None = None
    night_deadline_ts: float | None = None
    winner: str | None = None
    winner_reason: str | None = None
    elimination_log: list[dict[str, Any]] = Field(default_factory=list)


class MafiaPlayerContext(AgentContext):
    player_id: str = ""
    display_name: str = ""
    parent_task_id: str = ""
    role: Literal["villager", "werewolf"] = "villager"
    phase: str = "await_game_start"
    round_no: int = 1
    town_group_name: str = TOWN_GROUP_NAME
    wolf_group_name: str = WOLF_GROUP_NAME
    discussion_round_no: int = 0
    discussion_messages_sent: int = 0
    pending_day_prompt: str | None = None
    day_vote_poll_round_no: int = 0
    day_vote_poll_count: int = 0
    day_vote_allowed_targets: list[str] = Field(default_factory=list)


__all__ = [
    "HUMAN_PLAYER_ID",
    "PlayerRecord",
    "GameStateSnapshot",
    "GameActionResult",
    "PlayerActionResult",
    "FinalGameOutput",
    "PlayerFinalOutput",
    "MafiaGameContext",
    "MafiaPlayerContext",
]
