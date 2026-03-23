import {
  Clock3,
  Loader2,
  MessageSquare,
  MoonStar,
  Send,
  Vote,
} from "lucide-react";
import { useEffect, useRef } from "react";

import type { Channel, PlayerStateView, UiMessage } from "../types";
import {
  CHANNEL_LABEL,
  formatTimestamp,
  senderColor,
} from "../utils";

interface ChatPanelProps {
  selectedChannel: Channel;
  threads: Record<Channel, UiMessage[]>;
  canViewWolfThread: boolean;
  activeAgentCount: number;
  waitingAgentCount: number;
  failedAgentCount: number;
  isNightPhase: boolean;
  canUseHumanActions: boolean;
  chatInput: string;

  dayVoteOpen: boolean;
  humanHasVotedThisRound: boolean;
  voteTarget: string;
  aliveVoteTargets: PlayerStateView[];
  submittingVote: boolean;

  dayDiscussionOpen: boolean;
  humanHasCalledVoteThisRound: boolean;
  submittingCallVote: boolean;
  voteCallsReceived: number;
  voteCallsThreshold: number;

  canSubmitNightAction: boolean;
  humanHasSubmittedNightActionThisRound: boolean;
  nightTarget: string;
  aliveNightTargets: PlayerStateView[];
  submittingNightAction: boolean;

  onSelectChannel: (channel: Channel) => void;
  onChatInputChange: (value: string) => void;
  onSendChat: () => void;
  onVoteTargetChange: (value: string) => void;
  onSubmitVote: () => void;
  onNightTargetChange: (value: string) => void;
  onSubmitNightAction: () => void;
  onCallVote: () => void;
}

function ChatMessage({ message }: { message: UiMessage }) {
  const fromHuman = message.fromLabel === "You";
  const isSystem =
    message.channel === "system" || message.badge === "system";
  const initial = message.fromLabel.charAt(0).toUpperCase();
  const colorClass = senderColor(message.fromLabel);

  if (isSystem) {
    return (
      <div className="animate-fade-in flex justify-center py-1">
        <div className="max-w-lg rounded-md bg-amber-500/5 px-3 py-1.5 text-center text-xs text-amber-300/70">
          <span className="mr-1.5 text-[10px] text-amber-500/40">
            {formatTimestamp(message.timestamp)}
          </span>
          {message.content}
        </div>
      </div>
    );
  }

  return (
    <div
      className={`animate-fade-in flex gap-2.5 ${fromHuman ? "flex-row-reverse" : "flex-row"}`}
    >
      {!fromHuman && (
        <div
          className={`flex h-7 w-7 shrink-0 items-center justify-center rounded-full text-xs font-bold text-white ${colorClass}`}
        >
          {initial}
        </div>
      )}
      <div className={`max-w-[75%] ${fromHuman ? "text-right" : ""}`}>
        <div
          className={`mb-0.5 flex items-center gap-2 text-[10px] ${fromHuman ? "justify-end" : ""}`}
        >
          <span
            className={
              fromHuman
                ? "font-semibold text-blue-400"
                : "font-semibold text-neutral-400"
            }
          >
            {message.fromLabel}
          </span>
          <span className="text-neutral-600">
            {formatTimestamp(message.timestamp)}
          </span>
        </div>
        <div
          className={`inline-block rounded-2xl px-3.5 py-2 text-sm ${
            fromHuman
              ? "rounded-br-md bg-white text-neutral-900"
              : "rounded-bl-md bg-neutral-800 text-neutral-200"
          }`}
        >
          <p className="whitespace-pre-wrap">{message.content}</p>
        </div>
      </div>
    </div>
  );
}

export function ChatPanel({
  selectedChannel,
  threads,
  canViewWolfThread,
  activeAgentCount,
  waitingAgentCount,
  failedAgentCount,
  isNightPhase,
  canUseHumanActions,
  chatInput,
  dayVoteOpen,
  humanHasVotedThisRound,
  voteTarget,
  aliveVoteTargets,
  submittingVote,
  dayDiscussionOpen,
  humanHasCalledVoteThisRound,
  submittingCallVote,
  voteCallsReceived,
  voteCallsThreshold,
  canSubmitNightAction,
  humanHasSubmittedNightActionThisRound,
  nightTarget,
  aliveNightTargets,
  submittingNightAction,
  onSelectChannel,
  onChatInputChange,
  onSendChat,
  onVoteTargetChange,
  onSubmitVote,
  onNightTargetChange,
  onSubmitNightAction,
  onCallVote,
}: ChatPanelProps) {
  const chatEndRef = useRef<HTMLDivElement>(null);
  const messages = threads[selectedChannel];

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages.length, selectedChannel]);

  return (
    <section className="flex min-h-0 min-w-0 flex-1 flex-col">
      {/* Channel tabs */}
      <div className="flex items-center gap-1 border-b border-neutral-800 bg-neutral-950 px-4 py-2">
        {(["town", "wolf", "system"] as const).map((channel) => {
          if (channel === "wolf" && !canViewWolfThread) return null;
          const active = selectedChannel === channel;
          const count = threads[channel].length;
          const channelIcon =
            channel === "town"
              ? "\u{1F3D8}\u{FE0F}"
              : channel === "wolf"
                ? "\u{1F43A}"
                : "\u{2699}\u{FE0F}";
          return (
            <button
              key={channel}
              type="button"
              onClick={() => onSelectChannel(channel)}
              className={`flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-xs font-medium transition ${
                active
                  ? "bg-neutral-800 text-white"
                  : "text-neutral-500 hover:bg-neutral-800/50 hover:text-neutral-300"
              }`}
            >
              <span className="text-sm">{channelIcon}</span>
              {CHANNEL_LABEL[channel]}
              {count > 0 && (
                <span
                  className={`rounded-full px-1.5 py-0.5 text-[10px] font-bold ${
                    active
                      ? "bg-neutral-700 text-white"
                      : "bg-neutral-800 text-neutral-500"
                  }`}
                >
                  {count}
                </span>
              )}
            </button>
          );
        })}
        <div className="flex-1" />
        {(activeAgentCount > 0 ||
          waitingAgentCount > 0 ||
          failedAgentCount > 0) && (
          <div className="flex items-center gap-2 text-[10px] text-neutral-600">
            {activeAgentCount > 0 && (
              <span className="flex items-center gap-1">
                <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-400" />
                {activeAgentCount} active
              </span>
            )}
            {waitingAgentCount > 0 && (
              <span className="flex items-center gap-1">
                <Clock3 className="h-3 w-3 text-amber-500/50" />
                {waitingAgentCount} waiting
              </span>
            )}
            {failedAgentCount > 0 && (
              <span className="flex items-center gap-1 text-rose-300">
                <span className="h-1.5 w-1.5 rounded-full bg-rose-400" />
                {failedAgentCount} failed
              </span>
            )}
          </div>
        )}
      </div>

      {/* Messages area */}
      <div className="flex-1 overflow-y-auto px-4 py-3">
        {messages.length === 0 ? (
          <div className="flex h-full flex-col items-center justify-center gap-2 text-neutral-600">
            <MessageSquare className="h-8 w-8 text-neutral-700" />
            <p className="text-sm">
              No messages in {CHANNEL_LABEL[selectedChannel]} yet.
            </p>
          </div>
        ) : (
          <div className="space-y-2">
            {messages.map((msg) => (
              <ChatMessage key={msg.id} message={msg} />
            ))}
            <div ref={chatEndRef} />
          </div>
        )}
      </div>

      {/* Call vote bar */}
      {dayDiscussionOpen && canUseHumanActions && !dayVoteOpen && (
        <div className="border-t border-neutral-800 bg-amber-500/5 px-4 py-3">
          <div className="flex items-center gap-2">
            <Vote className="h-4 w-4 shrink-0 text-amber-400" />
            <span className="shrink-0 text-xs text-amber-300/70">
              {voteCallsReceived}/{voteCallsThreshold} calls to vote
            </span>
            <div className="flex-1" />
            <button
              type="button"
              onClick={onCallVote}
              disabled={humanHasCalledVoteThisRound || submittingCallVote}
              className="rounded-lg bg-amber-600 px-3.5 py-1.5 text-xs font-semibold text-white transition hover:bg-amber-500 disabled:opacity-40"
            >
              {submittingCallVote ? (
                <Loader2 className="inline h-3.5 w-3.5 animate-spin" />
              ) : humanHasCalledVoteThisRound ? (
                "Called"
              ) : (
                "Call Vote"
              )}
            </button>
          </div>
        </div>
      )}

      {/* Day vote bar */}
      {dayVoteOpen && canUseHumanActions && (
        <div className="border-t border-neutral-800 bg-violet-500/5 px-4 py-3">
          <div className="flex items-center gap-2">
            <Vote className="h-4 w-4 shrink-0 text-violet-400" />
            <span className="shrink-0 text-xs font-semibold text-violet-300">
              Day Vote
            </span>
            <select
              value={voteTarget}
              onChange={(e) => onVoteTargetChange(e.target.value)}
              disabled={humanHasVotedThisRound}
              className="flex-1 rounded-lg border border-neutral-700 bg-neutral-900 px-2.5 py-1.5 text-xs text-neutral-200 outline-none transition focus:border-neutral-600 disabled:opacity-40"
            >
              <option value="">Choose target...</option>
              {aliveVoteTargets.map((p) => (
                <option key={p.player_id} value={p.player_id}>
                  {p.display_name}
                </option>
              ))}
            </select>
            <button
              type="button"
              onClick={onSubmitVote}
              disabled={
                !voteTarget ||
                humanHasVotedThisRound ||
                submittingVote
              }
              className="rounded-lg bg-white px-3.5 py-1.5 text-xs font-semibold text-neutral-900 transition hover:bg-neutral-200 disabled:opacity-40"
            >
              {submittingVote ? (
                <Loader2 className="inline h-3.5 w-3.5 animate-spin" />
              ) : humanHasVotedThisRound ? (
                "Voted"
              ) : (
                "Vote"
              )}
            </button>
          </div>
        </div>
      )}

      {/* Night action bar */}
      {canSubmitNightAction && canUseHumanActions && (
        <div className="border-t border-neutral-800 bg-rose-500/5 px-4 py-3">
          <div className="flex items-center gap-2">
            <MoonStar className="h-4 w-4 shrink-0 text-rose-400" />
            <span className="shrink-0 text-xs font-semibold text-rose-300">
              Night Kill
            </span>
            <select
              value={nightTarget}
              onChange={(e) => onNightTargetChange(e.target.value)}
              disabled={
                humanHasSubmittedNightActionThisRound ||
                submittingNightAction
              }
              className="flex-1 rounded-lg border border-neutral-700 bg-neutral-900 px-2.5 py-1.5 text-xs text-neutral-200 outline-none transition focus:border-neutral-600 disabled:opacity-40"
            >
              <option value="">Choose target...</option>
              {aliveNightTargets.map((p) => (
                <option key={p.player_id} value={p.player_id}>
                  {p.display_name}
                </option>
              ))}
            </select>
            <button
              type="button"
              onClick={onSubmitNightAction}
              disabled={
                !nightTarget ||
                humanHasSubmittedNightActionThisRound ||
                submittingNightAction
              }
              className="rounded-lg bg-white px-3.5 py-1.5 text-xs font-semibold text-neutral-900 transition hover:bg-neutral-200 disabled:opacity-40"
            >
              {submittingNightAction ? (
                <Loader2 className="inline h-3.5 w-3.5 animate-spin" />
              ) : humanHasSubmittedNightActionThisRound ? (
                "Sent"
              ) : (
                "Kill"
              )}
            </button>
          </div>
        </div>
      )}

      {/* Chat input */}
      <div className="border-t border-neutral-800 bg-neutral-950 px-4 py-3">
        <div className="flex items-center gap-2">
          <input
            value={chatInput}
            onChange={(e) => onChatInputChange(e.target.value)}
            placeholder={
              isNightPhase
                ? "Chat locked during night phase..."
                : !canUseHumanActions
                  ? "Join the game as a human to chat..."
                  : selectedChannel === "system"
                    ? "Cannot send messages in System..."
                    : selectedChannel === "wolf"
                      ? "Whisper to the wolf den..."
                      : "Speak to the town..."
            }
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                onSendChat();
              }
            }}
            disabled={
              !canUseHumanActions ||
              selectedChannel === "system" ||
              isNightPhase
            }
            className="flex-1 rounded-lg border border-neutral-800 bg-neutral-900 px-4 py-2.5 text-sm text-neutral-200 placeholder:text-neutral-600 outline-none transition focus:border-neutral-700 disabled:cursor-not-allowed disabled:opacity-40"
          />
          <button
            type="button"
            onClick={onSendChat}
            disabled={
              !canUseHumanActions ||
              selectedChannel === "system" ||
              isNightPhase ||
              !chatInput.trim()
            }
            className="flex h-10 w-10 items-center justify-center rounded-lg bg-white text-neutral-900 transition hover:bg-neutral-200 disabled:cursor-not-allowed disabled:opacity-30"
          >
            <Send className="h-4 w-4" />
          </button>
        </div>
      </div>
    </section>
  );
}
