import { Trophy } from "lucide-react";

import type {
  GameStateSnapshot,
  PlayerStateView,
  ThoughtEntry,
} from "../types";
import { formatRoleLabel, formatTimestamp } from "../utils";

interface SidebarProps {
  includeHuman: boolean;
  gameState: GameStateSnapshot | null;
  humanPlayerId: string | null;
  humanIsAlive: boolean;
  showOmniscient: boolean;
  isNightPhase: boolean;
  sortedVisiblePlayers: PlayerStateView[];
  selectedAgentPlayerId: string | null;
  votedRoundByPlayerId: Record<string, number>;
  selectedAgent: PlayerStateView | null;
  selectedAgentThoughts: ThoughtEntry[];
  finalReport: string | null;
  onSelectAgent: (id: string | null) => void;
  onShowFinalReport: () => void;
}

function PlayerCard({
  player,
  showOmniscient,
  isNightPhase,
  isSelected,
  canInspect,
  votedThisRound,
  onClick,
}: {
  player: PlayerStateView;
  showOmniscient: boolean;
  isNightPhase: boolean;
  isSelected: boolean;
  canInspect: boolean;
  votedThisRound: boolean;
  onClick: () => void;
}) {
  const isWolf = showOmniscient && player.role === "werewolf";
  const avatar = !player.alive
    ? "\u{1F480}"
    : player.is_human
      ? "\u{1F9D1}"
      : isWolf
        ? "\u{1F43A}"
        : isNightPhase && player.alive
          ? "\u{1F634}"
          : "\u{1F916}";

  return (
    <div
      onClick={canInspect ? onClick : undefined}
      className={`flex items-center gap-3 rounded-lg border px-3 py-2.5 transition ${
        !player.alive
          ? "border-neutral-800/50 bg-neutral-900/30 opacity-40"
          : isSelected
            ? "border-neutral-600 bg-neutral-800"
            : player.is_human
              ? "border-blue-500/15 bg-blue-500/5"
              : isWolf
                ? "border-rose-500/10 bg-neutral-900/50"
                : "border-neutral-800 bg-neutral-900/50"
      } ${canInspect ? "cursor-pointer hover:border-neutral-600 hover:bg-neutral-800/70" : ""}`}
    >
      <span className="text-lg leading-none">{avatar}</span>
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span
            className={`truncate text-sm font-medium ${player.alive ? "text-white" : "text-neutral-600 line-through"}`}
          >
            {player.display_name}
          </span>
          {player.is_human && (
            <span className="rounded bg-blue-500/15 px-1.5 py-0.5 text-[9px] font-bold uppercase text-blue-400">
              You
            </span>
          )}
        </div>
        <div className="flex items-center gap-2 text-[11px] text-neutral-500">
          {showOmniscient && player.role ? (
            <span className={isWolf ? "text-rose-400" : "text-emerald-400"}>
              {formatRoleLabel(player.role)}
            </span>
          ) : player.is_human ? (
            "Human"
          ) : (
            "AI Agent"
          )}
          <span className="text-neutral-700">&middot;</span>
          {!player.alive ? (
            <span className="text-neutral-600">Dead</span>
          ) : votedThisRound ? (
            <span className="text-amber-400">Voted</span>
          ) : (
            <span className="flex items-center gap-1">
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
              Alive
            </span>
          )}
        </div>
      </div>
      {isSelected && (
        <span className="shrink-0 rounded bg-neutral-700 px-1.5 py-0.5 text-[9px] font-bold uppercase text-neutral-300">
          Inspect
        </span>
      )}
    </div>
  );
}

export function Sidebar({
  includeHuman,
  gameState,
  humanPlayerId,
  humanIsAlive,
  showOmniscient,
  isNightPhase,
  sortedVisiblePlayers,
  selectedAgentPlayerId,
  votedRoundByPlayerId,
  selectedAgent,
  selectedAgentThoughts,
  finalReport,
  onSelectAgent,
  onShowFinalReport,
}: SidebarProps) {
  return (
    <aside className="flex w-[320px] shrink-0 flex-col border-r border-neutral-800 bg-neutral-950 lg:w-[340px]">
      <div className="flex-1 overflow-y-auto p-4">
        {/* Your Role */}
        {includeHuman && gameState?.human_private_role && (
          <div
            className={`mb-4 overflow-hidden rounded-lg border p-3 ${
              gameState.human_private_role === "werewolf"
                ? "border-rose-500/20 bg-rose-500/5"
                : "border-emerald-500/20 bg-emerald-500/5"
            }`}
          >
            <div className="mb-1 text-[10px] font-semibold uppercase tracking-widest text-neutral-500">
              Your Role
            </div>
            <div className="flex items-center gap-2">
              <span className="text-2xl">
                {gameState.human_private_role === "werewolf"
                  ? "\u{1F43A}"
                  : "\u{1F6E1}\u{FE0F}"}
              </span>
              <span
                className={`text-lg font-bold ${
                  gameState.human_private_role === "werewolf"
                    ? "text-rose-300"
                    : "text-emerald-300"
                }`}
              >
                {formatRoleLabel(gameState.human_private_role)}
              </span>
            </div>
            {!humanIsAlive && humanPlayerId && (
              <div className="mt-2 text-xs text-amber-400/70">
                You have been eliminated.
              </div>
            )}
          </div>
        )}

        {/* Game stats */}
        {gameState && (
          <div className="mb-4 grid grid-cols-3 gap-2">
            <div className="rounded-lg bg-neutral-900 px-2.5 py-2 text-center">
              <div className="text-[10px] font-medium uppercase tracking-wider text-neutral-500">
                Alive
              </div>
              <div className="text-lg font-bold text-white">
                {gameState.alive_total}
              </div>
            </div>
            <div className="rounded-lg bg-neutral-900 px-2.5 py-2 text-center">
              <div className="text-[10px] font-medium uppercase tracking-wider text-neutral-500">
                Village
              </div>
              <div className="text-lg font-bold text-emerald-400">
                {gameState.alive_villagers}
              </div>
            </div>
            <div className="rounded-lg bg-neutral-900 px-2.5 py-2 text-center">
              <div className="text-[10px] font-medium uppercase tracking-wider text-neutral-500">
                Wolves
              </div>
              <div className="text-lg font-bold text-rose-400">
                {gameState.alive_werewolves}
              </div>
            </div>
          </div>
        )}

        {/* Winner banner */}
        {gameState?.winner && (
          <div className="mb-4 rounded-lg border border-amber-500/15 bg-amber-500/5 p-3 text-center">
            <Trophy className="mx-auto mb-1 h-5 w-5 text-amber-400" />
            <div className="text-sm font-bold text-amber-200">
              {formatRoleLabel(gameState.winner)} Win
            </div>
            {gameState.winner_reason && (
              <div className="mt-1 text-[11px] text-amber-400/60">
                {gameState.winner_reason}
              </div>
            )}
            {finalReport && (
              <button
                type="button"
                onClick={onShowFinalReport}
                className="mt-2 rounded-lg bg-neutral-800 px-3 py-1 text-[11px] font-medium text-neutral-300 transition hover:bg-neutral-700"
              >
                View Report
              </button>
            )}
          </div>
        )}

        {/* Player roster */}
        <div className="mb-3">
          <h3 className="mb-2 text-[10px] font-semibold uppercase tracking-widest text-neutral-500">
            Players
            {gameState && (
              <span className="ml-1 text-neutral-600">
                ({gameState.alive_total} alive)
              </span>
            )}
          </h3>
          {!gameState ? (
            <div className="flex h-32 items-center justify-center rounded-lg border border-dashed border-neutral-800 text-xs text-neutral-600">
              Waiting for game to start...
            </div>
          ) : (
            <div className="space-y-1.5">
              {sortedVisiblePlayers.map((player) => {
                const canInspect = showOmniscient && !player.is_human;
                const isSelected =
                  canInspect &&
                  selectedAgentPlayerId === player.player_id;
                const votedThisRound = Boolean(
                  gameState?.round_no &&
                    votedRoundByPlayerId[player.player_id] ===
                      gameState.round_no,
                );
                return (
                  <PlayerCard
                    key={player.player_id}
                    player={player}
                    showOmniscient={showOmniscient}
                    isNightPhase={isNightPhase}
                    isSelected={isSelected}
                    canInspect={canInspect}
                    votedThisRound={votedThisRound}
                    onClick={() =>
                      onSelectAgent(
                        selectedAgentPlayerId === player.player_id
                          ? null
                          : player.player_id,
                      )
                    }
                  />
                );
              })}
            </div>
          )}
        </div>

        {/* Agent thoughts */}
        {showOmniscient && (
          <div className="mt-2">
            <h3 className="mb-2 flex items-center gap-2 text-[10px] font-semibold uppercase tracking-widest text-neutral-500">
              Agent Thoughts
              {selectedAgent && (
                <span className="rounded bg-neutral-800 px-1.5 py-0.5 text-[9px] font-bold normal-case text-neutral-300">
                  {selectedAgent.display_name}
                </span>
              )}
            </h3>
            {!selectedAgent ? (
              <p className="text-[11px] text-neutral-600">
                Click an AI player to inspect thoughts.
              </p>
            ) : selectedAgentThoughts.length === 0 ? (
              <p className="text-[11px] text-neutral-600">
                No thoughts logged yet.
              </p>
            ) : (
              <div className="max-h-52 space-y-1.5 overflow-y-auto">
                {selectedAgentThoughts.slice(-80).map((entry) => (
                  <div
                    key={entry.id}
                    className="rounded-lg border border-neutral-800 bg-neutral-900 px-2.5 py-2 text-xs text-neutral-300"
                  >
                    <div className="mb-0.5 text-[10px] text-neutral-600">
                      {formatTimestamp(entry.timestamp)}
                    </div>
                    <div className="whitespace-pre-wrap">
                      {entry.content}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </aside>
  );
}
