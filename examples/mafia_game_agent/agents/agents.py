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
    any_of,
    claude_45_sonnet,
    no_tool_calls,
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
- During day vote, either vote now or poll briefly,
  but you must cast one vote before timeout.
- Once you have voted, wait for the next phase signal.
- During night, villagers should only poll and wait for dawn.
- During night, werewolves can always kill.
- During night, werewolves may poll or use chat_with_werewolves only when more
  than one werewolf is alive.
- Do not reveal your role in town messages unless eliminated or game over.
- If a tool argument is optional and you are unsure, still proceed with a
  reasonable choice.
"""

def _player_prepare_turn(turn, agent_ctx):
    turn.tool_choice = _player_tool_choice(agent_ctx)
    turn.parallel_tool_calls = False


def _gm_prepare_turn(turn, agent_ctx):
    turn.tool_choice = _gm_tool_choice(agent_ctx)
    turn.parallel_tool_calls = False


mafia_player_agent = Agent[MafiaPlayerState](
    name="mafia_player_agent",
    description="Single AI player in a Mafia game",
    model=ai_gateway(claude_45_sonnet),
    instructions=PLAYER_INSTRUCTIONS,
    tools=PLAYER_TOOLS,
    temperature=0.7,
    prepare_turn=_player_prepare_turn,
    stop_when=any_of(no_tool_calls(), turn_count_is(120)),
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
    stop_when=any_of(no_tool_calls(), turn_count_is(96)),
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
