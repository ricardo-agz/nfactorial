export type Channel = "town" | "wolf" | "system";
export type HumanRolePreference = "random" | "werewolf" | "villager";
export type AgentStatus =
  | "queued"
  | "active"
  | "waiting"
  | "completed"
  | "failed"
  | "cancelled";
export type ActivityKind =
  | "info"
  | "tool_started"
  | "tool_completed"
  | "tool_failed"
  | "wait"
  | "resume";

export interface AgentEvent {
  event_type: string;
  task_id?: string;
  owner_id?: string;
  agent_name?: string;
  timestamp: string;
  data?: any;
  error?: string;
  tool_name?: string;
  tool_call_id?: string;
  output?: any;
  is_error?: boolean;
  status?: string;
}

export interface UiMessage {
  id: string;
  channel: Channel;
  timestamp: string;
  fromLabel: string;
  content: string;
  badge: "system" | "chat" | "action";
}

export interface PlayerStateView {
  player_id: string;
  display_name: string;
  is_human: boolean;
  task_id?: string | null;
  alive: boolean;
  role?: string | null;
}

export interface VoteRecord {
  voter_id: string;
  voter_display_name: string;
  target_player_id: string;
  target_display_name: string;
}

export interface DayVoteHistoryEntry {
  round_no: number;
  eliminated_player_id?: string | null;
  eliminated_display_name?: string | null;
  votes: VoteRecord[];
}

export interface GameStateSnapshot {
  phase: string;
  round_no: number;
  phase_deadline_ts?: number | null;
  winner?: string | null;
  winner_reason?: string | null;
  alive_total: number;
  alive_villagers: number;
  alive_werewolves: number;
  vote_calls_received?: number;
  vote_calls_threshold?: number;
  players_public: PlayerStateView[];
  players_omniscient: PlayerStateView[];
  human_player_id?: string | null;
  human_private_role?: string | null;
  current_day_votes?: VoteRecord[];
  day_vote_history?: DayVoteHistoryEntry[];
  elimination_log: Array<Record<string, any>>;
}

export interface ActivityEntry {
  id: string;
  timestamp: string;
  text: string;
  kind: ActivityKind;
  actorLabel?: string;
  toolName?: string;
  detail?: string;
}

export interface ThoughtEntry {
  id: string;
  timestamp: string;
  content: string;
}
