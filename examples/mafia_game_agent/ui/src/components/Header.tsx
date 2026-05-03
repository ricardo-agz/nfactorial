import {
  AlertTriangle,
  Eye,
  EyeOff,
  Loader2,
  MessageSquare,
  MoonStar,
  Settings2,
  Square,
  Sun,
  Trophy,
  X,
} from "lucide-react";

import type { GameStateSnapshot } from "../types";
import { formatPhaseLabel, formatRoleLabel } from "../utils";

interface HeaderProps {
  gameName: string;
  gameState: GameStateSnapshot | null;
  isNightPhase: boolean;
  starting: boolean;
  cancelling: boolean;
  isGameRunning: boolean;
  phaseTimerLabel: string;
  isPhaseTimerCritical: boolean;
  showOmniscient: boolean;
  showActivity: boolean;
  guidanceText: string;
  errorText: string | null;
  onToggleOmniscient: () => void;
  onToggleActivity: () => void;
  onOpenSetup: () => void;
  onDismissError: () => void;
  onCancel: () => void;
}

export function Header({
  gameName,
  gameState,
  isNightPhase,
  starting,
  cancelling,
  isGameRunning,
  phaseTimerLabel,
  isPhaseTimerCritical,
  showOmniscient,
  showActivity,
  guidanceText,
  errorText,
  onToggleOmniscient,
  onToggleActivity,
  onOpenSetup,
  onDismissError,
  onCancel,
}: HeaderProps) {
  return (
    <header className="relative z-20 shrink-0 border-b border-neutral-800 bg-neutral-950">
      <div className="flex items-center gap-4 px-5 py-3">
        {/* Logo & game name */}
        <div className="flex items-center gap-3">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-neutral-800 text-base">
            {isNightPhase ? (
              <MoonStar className="h-4 w-4 text-neutral-300" />
            ) : (
              <Sun className="h-4 w-4 text-neutral-300" />
            )}
          </div>
          <div>
            <h1 className="text-sm font-semibold tracking-tight text-white">
              Mafia
            </h1>
            <p className="text-[11px] text-neutral-500">{gameName}</p>
          </div>
        </div>

        {/* Phase info strip */}
        <div className="ml-2 flex items-center gap-2">
          {starting && (
            <span className="flex items-center gap-1.5 rounded-md bg-neutral-800 px-2.5 py-1 text-[11px] font-medium text-neutral-400">
              <Loader2 className="h-3 w-3 animate-spin" /> Starting
            </span>
          )}
          {cancelling && (
            <span className="flex items-center gap-1.5 rounded-md bg-neutral-800 px-2.5 py-1 text-[11px] font-medium text-amber-400">
              <Loader2 className="h-3 w-3 animate-spin" /> Cancelling
            </span>
          )}
          {isGameRunning && !starting && !cancelling && (
            <span className="flex items-center gap-1.5 rounded-md bg-emerald-500/10 px-2.5 py-1 text-[11px] font-medium text-emerald-400">
              <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-400" />
              Live
            </span>
          )}
          {gameState?.winner && (
            <span className="flex items-center gap-1.5 rounded-md bg-amber-500/10 px-2.5 py-1 text-[11px] font-medium text-amber-400">
              <Trophy className="h-3 w-3" />{" "}
              {formatRoleLabel(gameState.winner)} Win
            </span>
          )}

          {gameState && (
            <>
              <span className="rounded-md bg-neutral-800 px-2.5 py-1 text-[11px] font-medium text-neutral-300">
                {isNightPhase ? (
                  <MoonStar className="mr-1 inline h-3 w-3 text-violet-400" />
                ) : (
                  <Sun className="mr-1 inline h-3 w-3 text-amber-400" />
                )}
                {formatPhaseLabel(gameState.phase)}
              </span>
              <span className="rounded-md bg-neutral-800 px-2.5 py-1 text-[11px] text-neutral-500">
                R{gameState.round_no}
              </span>
            </>
          )}

          <div
            className={`rounded-md px-2.5 py-1 font-mono text-xs font-bold tabular-nums ${
              phaseTimerLabel === "--:--"
                ? "bg-neutral-900 text-neutral-700"
                : isPhaseTimerCritical
                  ? "animate-timer-critical bg-red-500/10"
                  : "bg-neutral-800 text-emerald-400"
            }`}
          >
            {phaseTimerLabel}
          </div>
        </div>

        <div className="flex-1" />

        {/* Action buttons */}
        <div className="flex items-center gap-1">
          <button
            type="button"
            onClick={onToggleOmniscient}
            className={`rounded-lg px-2.5 py-1.5 text-[11px] font-medium transition ${
              showOmniscient
                ? "bg-neutral-800 text-white"
                : "text-neutral-500 hover:bg-neutral-800/50 hover:text-neutral-300"
            }`}
          >
            {showOmniscient ? (
              <Eye className="mr-1 inline h-3.5 w-3.5" />
            ) : (
              <EyeOff className="mr-1 inline h-3.5 w-3.5" />
            )}
            Omni
          </button>
          <button
            type="button"
            onClick={onToggleActivity}
            className={`rounded-lg px-2.5 py-1.5 text-[11px] font-medium transition ${
              showActivity
                ? "bg-neutral-800 text-white"
                : "text-neutral-500 hover:bg-neutral-800/50 hover:text-neutral-300"
            }`}
          >
            <MessageSquare className="mr-1 inline h-3.5 w-3.5" />
            Activity
          </button>
          <button
            type="button"
            onClick={onOpenSetup}
            className="rounded-lg px-2.5 py-1.5 text-[11px] font-medium text-neutral-500 transition hover:bg-neutral-800/50 hover:text-neutral-300"
          >
            <Settings2 className="mr-1 inline h-3.5 w-3.5" />
            Setup
          </button>
          {isGameRunning && (
            <button
              type="button"
              onClick={onCancel}
              disabled={cancelling}
              className="rounded-lg bg-neutral-800 px-2.5 py-1.5 text-[11px] font-medium text-rose-400 transition hover:bg-neutral-700 disabled:opacity-50"
            >
              {cancelling ? (
                <Loader2 className="mr-1 inline h-3.5 w-3.5 animate-spin" />
              ) : (
                <Square className="mr-1 inline h-3.5 w-3.5" />
              )}
              {cancelling ? "Cancelling" : "Stop"}
            </button>
          )}
        </div>
      </div>

      {/* Guidance bar */}
      <div className="border-t border-neutral-800/60 px-5 py-2">
        <p className="text-xs text-neutral-500">{guidanceText}</p>
      </div>

      {/* Error bar */}
      {errorText && (
        <div className="flex items-center gap-2 border-t border-rose-500/20 bg-rose-500/5 px-5 py-2">
          <AlertTriangle className="h-3.5 w-3.5 shrink-0 text-rose-400" />
          <p className="flex-1 text-xs text-rose-300">{errorText}</p>
          <button
            type="button"
            onClick={onDismissError}
            className="text-rose-400 hover:text-rose-300"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      )}
    </header>
  );
}
