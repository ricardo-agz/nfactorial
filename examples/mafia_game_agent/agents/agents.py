from __future__ import annotations

import os

from constants import HUMAN_PLAYER_ID, TOWN_GROUP_NAME, WOLF_GROUP_NAME
from dotenv import load_dotenv
from game_master import (
    GAME_MASTER_TOOLS,
    _gm_tool_choice,
    set_player_agent_for_game_master,
)
from models import (
    FinalGameOutput,
    MafiaGameState,
    MafiaPlayerState,
    PlayerFinalOutput,
)
from players import PLAYER_TOOLS, _player_tool_choice

from factorial import (
    Agent,
    ai_gateway,
    claude_45_sonnet,
    system,
    turn_count_is,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(current_dir)
env_path = os.path.join(project_dir, ".env")
load_dotenv(env_path, override=True)

GAME_MASTER_INSTRUCTIONS = """
You are the game master for a Mafia/Werewolf social deduction game.

Rules for this implementation:
- Exactly one werewolf; everyone else is a villager.
- Day: discussion, then one vote per alive player.
- Night: werewolf submits one target.
- Villagers win when all werewolves are eliminated.
- Werewolves win when they reach parity with villagers.

Behavior contract:
- Keep public announcements concise and easy to follow.
- Never leak hidden role information in public messages unless a player is
  eliminated or game over has occurred.
- Respect phase data in context and move the game forward deterministically.
- Before game over, every turn must produce exactly one tool call.
- If a prior turn failed to call a tool or a tool returned an error, correct it
  on the next turn with a valid tool call instead of replying with narration.
- When the phase reaches game_over, stop calling tools and return the final
  markdown report.
- At game over, provide a concise markdown report of winner, reason, final
  roster, and elimination log.
"""

PLAYER_INSTRUCTIONS = """
You are an AI player in a Mafia game. You have a unique name and personality
described in your initial prompt. Stay in character throughout the entire game:
your personality should shape how you speak, who you suspect, how quickly you
call for votes, and how you react to accusations. Do NOT speak like a generic
assistant -- be the character.

Behavior contract:
- Write chat messages that reflect your personality. A quiet player sends
  short messages rarely; an aggressive player calls people out; a joker cracks
  jokes under pressure. Make each message feel like it comes from a real person.
- Your initial user prompt includes your exact role, personality, and phase
  timers; follow it strictly.
- Use think to log private reasoning notes for omniscient debugging.
- During day discussion, chat, think, or poll for updates.
- During day discussion, use call_vote() when you feel discussion has been
  sufficient and you are ready to vote. Once a majority of alive players have
  called, the vote phase begins.
- When day vote opens, spend your very next turn on vote(). Do not spend a
  vote turn on think() or poll() unless vote() is unavailable because the phase
  already ended.
- Once you have voted, wait for the next phase signal.
- During night, villagers should only poll and wait for dawn.
- During night, werewolves can always kill.
- If you are the only living werewolf and night is open, spend your very next
  turn on kill(). Do not stall with think().
- During night, werewolves may poll or use chat_with_werewolves only when more
  than one werewolf is alive.
- Every turn must produce exactly one tool call. Never answer with plain text
  only.
- If your previous turn failed to call a tool or a tool returned an error,
  immediately correct it with a valid tool call on the next turn.
- If you are unsure what to do, call poll() rather than replying in prose.
- Do not reveal your role in town messages unless eliminated or game over.
- If a tool argument is optional and you are unsure, still proceed with a
  reasonable choice.
"""

def _finish_reason_kind(finish_reason: str | None) -> str | None:
    if not isinstance(finish_reason, str) or not finish_reason:
        return None
    kind, _, _ = finish_reason.partition(":")
    return kind or None


def _last_turn_called_tool(execution_ctx) -> bool:
    last_turn = execution_ctx.last_turn
    if last_turn is None:
        return False
    return _finish_reason_kind(last_turn.finish_reason) == "tool_called"


def _allowed_target_ids(target_ids: list[str], self_id: str) -> str:
    allowed = [target_id for target_id in target_ids if target_id != self_id]
    if not allowed:
        return "No valid target_player_id values are currently available."
    return "Use one of these exact target_player_id values: " + ", ".join(allowed) + "."


def _player_turn_prompt(agent_ctx, execution_ctx) -> str:
    phase = str(agent_ctx.state.phase)
    if phase in {"day_vote", "day_vote_must_vote"}:
        directive = (
            "Day vote is open. Call vote() this turn. "
            "Do not spend this turn on think() or poll(). "
            + _allowed_target_ids(
                agent_ctx.state.day_vote_allowed_targets,
                agent_ctx.state.player_id,
            )
        )
    elif (
        phase == "night_action"
        and agent_ctx.state.role == "werewolf"
        and agent_ctx.state.night_alive_werewolf_count <= 1
    ):
        directive = (
            "Night is open and you are the only werewolf alive. "
            "Call kill() this turn. "
            + _allowed_target_ids(
                agent_ctx.state.night_kill_allowed_targets,
                agent_ctx.state.player_id,
            )
        )
    else:
        directive = (
            "Turn contract: call exactly one tool this turn. "
            "If you are unsure, call poll(). Do not answer with plain prose."
        )

    if execution_ctx.last_turn is None or _last_turn_called_tool(execution_ctx):
        return directive
    if phase in {"day_vote", "day_vote_must_vote"}:
        return (
            "Turn contract: call exactly one tool this turn. "
            "Your previous turn was invalid. Day vote is open, so correct it by "
            "calling vote() now. Do not think, poll, apologize, or narrate. "
            + _allowed_target_ids(
                agent_ctx.state.day_vote_allowed_targets,
                agent_ctx.state.player_id,
            )
        )
    if (
        phase == "night_action"
        and agent_ctx.state.role == "werewolf"
        and agent_ctx.state.night_alive_werewolf_count <= 1
    ):
        return (
            "Turn contract: call exactly one tool this turn. "
            "Your previous turn was invalid. Night is open and you are the only "
            "werewolf alive, so correct it by calling kill() now. "
            + _allowed_target_ids(
                agent_ctx.state.night_kill_allowed_targets,
                agent_ctx.state.player_id,
            )
        )
    return (
        "Your previous turn was invalid because it did not call a tool. "
        "Correct it now by calling exactly one valid tool. "
        "If you are unsure, call poll(). Do not apologize or narrate."
    )


def _gm_turn_prompt(agent_ctx, execution_ctx) -> str:
    if str(agent_ctx.state.phase) == "game_over":
        return (
            "The game is over. Do not call tools. "
            "Return the final markdown report now."
        )
    if execution_ctx.last_turn is None or _last_turn_called_tool(execution_ctx):
        return (
            "Turn contract: call exactly one tool this turn. "
            "Do not answer with plain narration before game over."
        )
    return (
        "Your previous turn was invalid because it did not call a tool. "
        "Correct it now by calling exactly one valid tool. "
        "Do not narrate without a tool before game over."
    )


def _player_prepare_turn(turn, agent_ctx, execution_ctx):
    turn.tool_choice = _player_tool_choice(agent_ctx)
    turn.parallel_tool_calls = False
    turn.messages = [
        system(_player_turn_prompt(agent_ctx, execution_ctx)),
        *turn.messages,
    ]


def _gm_prepare_turn(turn, agent_ctx, execution_ctx):
    turn.tool_choice = _gm_tool_choice(agent_ctx)
    turn.parallel_tool_calls = False
    turn.messages = [system(_gm_turn_prompt(agent_ctx, execution_ctx)), *turn.messages]


def _gm_stop_when(agent_ctx, execution_ctx) -> bool:
    if turn_count_is(96)(agent_ctx, execution_ctx):
        return True
    if str(agent_ctx.state.phase) != "game_over":
        return False
    last_turn = execution_ctx.last_turn
    if last_turn is None:
        return False
    return _finish_reason_kind(last_turn.finish_reason) != "tool_called"


mafia_player_agent = Agent[MafiaPlayerState](
    name="mafia_player_agent",
    description="Single AI player in a Mafia game",
    model=ai_gateway(claude_45_sonnet),
    instructions=PLAYER_INSTRUCTIONS,
    tools=PLAYER_TOOLS,
    temperature=0.7,
    prepare_turn=_player_prepare_turn,
    stop_when=turn_count_is(120),
)

set_player_agent_for_game_master(mafia_player_agent)

mafia_game_master_agent = Agent[MafiaGameState](
    name="mafia_game_master",
    description="Game master agent for multi-player Mafia",
    model=ai_gateway(claude_45_sonnet),
    instructions=GAME_MASTER_INSTRUCTIONS,
    tools=GAME_MASTER_TOOLS,
    temperature=0.2,
    prepare_turn=_gm_prepare_turn,
    stop_when=_gm_stop_when,
)

__all__ = [
    "HUMAN_PLAYER_ID",
    "TOWN_GROUP_NAME",
    "WOLF_GROUP_NAME",
    "MafiaGameState",
    "MafiaPlayerState",
    "FinalGameOutput",
    "PlayerFinalOutput",
    "mafia_game_master_agent",
    "mafia_player_agent",
]
