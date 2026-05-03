import { Loader2, Play, X } from "lucide-react";

import type { HumanRolePreference } from "../types";

interface SetupModalProps {
  gameName: string;
  includeHuman: boolean;
  humanName: string;
  humanRolePreference: HumanRolePreference;
  aiPlayerCount: number;
  dayDiscussionSeconds: number;
  dayVoteSeconds: number;
  nightSeconds: number;
  starting: boolean;
  cancelling: boolean;
  isGameRunning: boolean;
  errorText: string | null;
  onGameNameChange: (value: string) => void;
  onIncludeHumanChange: (value: boolean) => void;
  onHumanNameChange: (value: string) => void;
  onHumanRolePreferenceChange: (value: HumanRolePreference) => void;
  onAiPlayerCountChange: (value: number) => void;
  onDayDiscussionSecondsChange: (value: number) => void;
  onDayVoteSecondsChange: (value: number) => void;
  onNightSecondsChange: (value: number) => void;
  onClose: () => void;
  onStart: () => void;
}

export function SetupModal({
  gameName,
  includeHuman,
  humanName,
  humanRolePreference,
  aiPlayerCount,
  dayDiscussionSeconds,
  dayVoteSeconds,
  nightSeconds,
  starting,
  cancelling,
  isGameRunning,
  errorText,
  onGameNameChange,
  onIncludeHumanChange,
  onHumanNameChange,
  onHumanRolePreferenceChange,
  onAiPlayerCountChange,
  onDayDiscussionSecondsChange,
  onDayVoteSecondsChange,
  onNightSecondsChange,
  onClose,
  onStart,
}: SetupModalProps) {
  const disabled = starting || cancelling;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div
        className="absolute inset-0 bg-black/60 backdrop-blur-sm"
        onClick={() => {
          if (!starting) onClose();
        }}
      />
      <div className="animate-slide-up relative z-10 w-full max-w-lg overflow-hidden rounded-xl border border-neutral-800 bg-neutral-950 shadow-2xl shadow-black/50">
        {/* Modal header */}
        <div className="flex items-center justify-between border-b border-neutral-800 px-6 py-4">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-neutral-800 text-base">
              {"\u{1F43A}"}
            </div>
            <div>
              <h2 className="text-base font-semibold text-white">
                Game Setup
              </h2>
              <p className="text-xs text-neutral-500">
                Configure and launch a new game.
              </p>
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            disabled={starting}
            className="rounded-lg p-1.5 text-neutral-500 transition hover:bg-neutral-800 hover:text-white disabled:opacity-40"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        {/* Form body */}
        <div className="space-y-4 px-6 py-5">
          {/* Game name */}
          <label className="block">
            <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-wider text-neutral-500">
              Game Name
            </span>
            <input
              value={gameName}
              onChange={(e) => onGameNameChange(e.target.value)}
              disabled={disabled}
              className="w-full rounded-lg border border-neutral-800 bg-neutral-900 px-3 py-2 text-sm text-white placeholder:text-neutral-600 outline-none transition focus:border-neutral-600 disabled:opacity-40"
              placeholder="Mafia in nfactorial"
            />
          </label>

          {/* Player config */}
          <div className="grid grid-cols-2 gap-3">
            <label className="block">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-wider text-neutral-500">
                AI Players
              </span>
              <input
                type="number"
                min={3}
                max={15}
                value={aiPlayerCount}
                disabled={disabled}
                onChange={(e) =>
                  onAiPlayerCountChange(Number(e.target.value))
                }
                className="w-full rounded-lg border border-neutral-800 bg-neutral-900 px-3 py-2 text-sm text-white outline-none transition focus:border-neutral-600 disabled:opacity-40"
              />
            </label>
            <label className="block">
              <span className="mb-1.5 block text-[11px] font-semibold uppercase tracking-wider text-neutral-500">
                Your Role
              </span>
              <select
                value={humanRolePreference}
                disabled={disabled}
                onChange={(e) =>
                  onHumanRolePreferenceChange(
                    e.target.value as HumanRolePreference,
                  )
                }
                className="w-full rounded-lg border border-neutral-800 bg-neutral-900 px-3 py-2 text-sm text-white outline-none transition focus:border-neutral-600 disabled:opacity-40"
              >
                <option value="random">Random</option>
                <option value="villager">Force Villager</option>
                <option value="werewolf">Force Werewolf</option>
              </select>
            </label>
          </div>

          {/* Phase timers */}
          <div>
            <span className="mb-2 block text-[11px] font-semibold uppercase tracking-wider text-neutral-500">
              Phase Durations (seconds)
            </span>
            <div className="grid grid-cols-3 gap-2">
              <label className="block">
                <span className="mb-1 block text-[10px] text-neutral-600">
                  Discussion
                </span>
                <input
                  type="number"
                  min={10}
                  max={300}
                  value={dayDiscussionSeconds}
                  disabled={disabled}
                  onChange={(e) =>
                    onDayDiscussionSecondsChange(
                      Number(e.target.value),
                    )
                  }
                  className="w-full rounded-lg border border-neutral-800 bg-neutral-900 px-3 py-2 text-sm text-white outline-none transition focus:border-neutral-600 disabled:opacity-40"
                />
              </label>
              <label className="block">
                <span className="mb-1 block text-[10px] text-neutral-600">
                  Voting
                </span>
                <input
                  type="number"
                  min={10}
                  max={300}
                  value={dayVoteSeconds}
                  disabled={disabled}
                  onChange={(e) =>
                    onDayVoteSecondsChange(Number(e.target.value))
                  }
                  className="w-full rounded-lg border border-neutral-800 bg-neutral-900 px-3 py-2 text-sm text-white outline-none transition focus:border-neutral-600 disabled:opacity-40"
                />
              </label>
              <label className="block">
                <span className="mb-1 block text-[10px] text-neutral-600">
                  Night
                </span>
                <input
                  type="number"
                  min={10}
                  max={300}
                  value={nightSeconds}
                  disabled={disabled}
                  onChange={(e) =>
                    onNightSecondsChange(Number(e.target.value))
                  }
                  className="w-full rounded-lg border border-neutral-800 bg-neutral-900 px-3 py-2 text-sm text-white outline-none transition focus:border-neutral-600 disabled:opacity-40"
                />
              </label>
            </div>
          </div>

          {/* Human player toggle */}
          <div className="flex items-center gap-3 rounded-lg border border-neutral-800 bg-neutral-900 p-3">
            <input
              type="checkbox"
              checked={includeHuman}
              disabled={disabled}
              onChange={(e) => onIncludeHumanChange(e.target.checked)}
              className="h-4 w-4 rounded border-neutral-700 bg-neutral-800 text-white focus:ring-neutral-600"
            />
            <div className="flex-1">
              <div className="text-sm font-medium text-white">
                Play as human
              </div>
              <div className="text-[11px] text-neutral-500">
                Join the game as a human player alongside AI agents.
              </div>
            </div>
            {includeHuman && (
              <input
                value={humanName}
                onChange={(e) => onHumanNameChange(e.target.value)}
                disabled={disabled}
                className="w-28 rounded-lg border border-neutral-800 bg-neutral-800 px-2.5 py-1.5 text-xs text-white outline-none transition focus:border-neutral-600 disabled:opacity-40"
                placeholder="Your name"
              />
            )}
          </div>

          {/* Error */}
          {errorText && (
            <div className="rounded-lg border border-rose-500/15 bg-rose-500/5 p-3">
              <p className="text-xs text-rose-300">{errorText}</p>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between border-t border-neutral-800 px-6 py-4">
          <p className="text-[11px] text-neutral-600">
            {isGameRunning
              ? "A game is running. Cancel it first."
              : "Ready to launch."}
          </p>
          <button
            type="button"
            onClick={onStart}
            disabled={starting || cancelling || isGameRunning}
            className="inline-flex items-center gap-2 rounded-lg bg-white px-5 py-2.5 text-sm font-semibold text-neutral-900 transition hover:bg-neutral-200 disabled:opacity-40"
          >
            {starting ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Play className="h-4 w-4" />
            )}
            {starting ? "Starting..." : "Start Game"}
          </button>
        </div>
      </div>
    </div>
  );
}
