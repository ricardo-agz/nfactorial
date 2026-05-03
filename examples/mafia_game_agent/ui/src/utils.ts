import type { Channel } from "./types";

export const CHANNEL_LABEL: Record<Channel, string> = {
  town: "Town Square",
  wolf: "Wolf Den",
  system: "System",
};

const AVATAR_COLORS = [
  "bg-rose-500/80",
  "bg-sky-500/80",
  "bg-emerald-500/80",
  "bg-amber-500/80",
  "bg-violet-500/80",
  "bg-cyan-500/80",
  "bg-pink-500/80",
  "bg-teal-500/80",
  "bg-orange-500/80",
  "bg-indigo-500/80",
];

export function senderColor(name: string): string {
  let hash = 0;
  for (const char of name) hash = ((hash << 5) - hash + char.charCodeAt(0)) | 0;
  return AVATAR_COLORS[Math.abs(hash) % AVATAR_COLORS.length];
}

export function createUserId(): string {
  return `user_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

export function shortTaskId(taskId: string | null | undefined): string {
  if (!taskId) return "unknown";
  return taskId.slice(0, 8);
}

export function extractFinalOutput(data: unknown): string | null {
  if (typeof data === "string") {
    const text = data.trim();
    return text.length > 0 ? text : null;
  }
  if (!data || typeof data !== "object") return null;
  const payload = data as Record<string, any>;
  if (typeof payload.final_output === "string" && payload.final_output.trim()) {
    return payload.final_output.trim();
  }
  if (typeof payload.output === "string" && payload.output.trim()) {
    return payload.output.trim();
  }
  if (
    payload.output &&
    typeof payload.output === "object" &&
    typeof payload.output.final_output === "string" &&
    payload.output.final_output.trim()
  ) {
    return payload.output.final_output.trim();
  }
  return null;
}

export function normalizeChannel(value: unknown): Channel | null {
  if (value === "town" || value === "wolf" || value === "system") return value;
  return null;
}

export function parseToolArgs(rawArguments: unknown): Record<string, unknown> {
  if (rawArguments && typeof rawArguments === "object" && !Array.isArray(rawArguments)) {
    return rawArguments as Record<string, unknown>;
  }
  if (typeof rawArguments === "string") {
    try {
      const parsed = JSON.parse(rawArguments);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed as Record<string, unknown>;
      }
    } catch {
      return {};
    }
  }
  return {};
}

export function summarizeToolAction(
  toolName: string,
  toolArgs: Record<string, unknown>,
): string | undefined {
  if (toolName === "chat") {
    const message = toolArgs.message;
    if (typeof message === "string" && message.trim()) {
      return `drafting: "${message.trim().slice(0, 80)}"`;
    }
    return "drafting a town statement";
  }
  if (toolName === "vote") {
    const target =
      toolArgs.target_player_id ?? toolArgs.player_id ?? toolArgs.target_id;
    if (typeof target === "string" && target.trim()) return `targeting ${target.trim()}`;
    return "choosing day vote target";
  }
  if (toolName === "kill") {
    const target =
      toolArgs.target_player_id ?? toolArgs.player_id ?? toolArgs.target_id;
    if (typeof target === "string" && target.trim()) return `targeting ${target.trim()}`;
    return "choosing night kill target";
  }
  if (toolName === "chat_with_werewolves") {
    const message = toolArgs.message;
    if (typeof message === "string" && message.trim()) {
      return `coordinating: "${message.trim().slice(0, 80)}"`;
    }
    return "coordinating with werewolf team";
  }
  if (toolName === "think") {
    const thought = toolArgs.thought;
    if (typeof thought === "string" && thought.trim()) {
      return `thinking: "${thought.trim().slice(0, 80)}"`;
    }
    return "logging a private thought";
  }
  if (toolName === "call_vote") return "calling for a vote";
  if (toolName === "poll") return "polling for updates";
  return undefined;
}

export function formatPhaseLabel(phase: string | null | undefined): string {
  if (!phase) return "Awaiting Start";
  return phase
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

export function formatRoleLabel(role: string | null | undefined): string {
  if (!role) return "Unknown";
  return role.charAt(0).toUpperCase() + role.slice(1);
}

export function formatTimestamp(timestamp: string): string {
  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) return timestamp;
  return parsed.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function formatCountdown(totalSeconds: number): string {
  const safeSeconds = Math.max(0, totalSeconds);
  const minutes = Math.floor(safeSeconds / 60).toString().padStart(2, "0");
  const seconds = Math.floor(safeSeconds % 60).toString().padStart(2, "0");
  return `${minutes}:${seconds}`;
}
