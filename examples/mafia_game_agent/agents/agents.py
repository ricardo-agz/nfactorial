from __future__ import annotations

import os

from dotenv import load_dotenv
from factorial import Agent, ModelSettings, ai_gateway, claude_45_sonnet

from constants import HUMAN_PLAYER_ID, TOWN_GROUP_NAME, WOLF_GROUP_NAME
from game_master import (
    GAME_MASTER_TOOLS,
    _gm_tool_choice,
    set_player_agent_for_game_master,
)
from models import (
    FinalGameOutput,
    MafiaGameContext,
    MafiaPlayerContext,
    PlayerFinalOutput,
)
from players import PLAYER_TOOLS, _player_tool_choice

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
You are an AI player in a Mafia game.

Behavior contract:
- Keep your public messages short and believable.
- During day discussion, either chat or poll for updates.
- During day vote, either vote now or poll for updates.
- Once you have voted, wait for the next phase signal.
- During night, do not chat; only submit a night action if you are the werewolf.
- Do not reveal your role in town messages unless eliminated or game over.
- If a tool argument is optional and you are unsure, still proceed with a
  reasonable choice.
"""

mafia_player_agent = Agent(
    name="mafia_player_agent",
    description="Single AI player in a Mafia game",
    model=ai_gateway(claude_45_sonnet),
    instructions=PLAYER_INSTRUCTIONS,
    context_class=MafiaPlayerContext,
    tools=PLAYER_TOOLS,
    output_type=PlayerFinalOutput,
    model_settings=ModelSettings(
        temperature=0.7,
        tool_choice=_player_tool_choice,
        parallel_tool_calls=False,
    ),
    max_turns=120,
)

set_player_agent_for_game_master(mafia_player_agent)

mafia_game_master_agent = Agent(
    name="mafia_game_master",
    description="Game master agent for multi-player Mafia",
    model=ai_gateway(claude_45_sonnet),
    instructions=GAME_MASTER_INSTRUCTIONS,
    context_class=MafiaGameContext,
    tools=GAME_MASTER_TOOLS,
    output_type=FinalGameOutput,
    model_settings=ModelSettings(
        temperature=0.2,
        tool_choice=_gm_tool_choice,
        parallel_tool_calls=False,
    ),
    max_turns=96,
)

__all__ = [
    "HUMAN_PLAYER_ID",
    "TOWN_GROUP_NAME",
    "WOLF_GROUP_NAME",
    "MafiaGameContext",
    "MafiaPlayerContext",
    "FinalGameOutput",
    "PlayerFinalOutput",
    "mafia_game_master_agent",
    "mafia_player_agent",
]
