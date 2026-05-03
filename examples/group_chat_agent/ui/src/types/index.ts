export type AgentStatus =
  | "queued"
  | "active"
  | "waiting"
  | "completed"
  | "failed"
  | "cancelled";

export interface AgentEvent {
  event_type: string;
  task_id: string;
  owner_id?: string;
  agent_name?: string;
  turn?: number;
  timestamp: string;
  data?: any;
  error?: string;
}

export interface AgentNode {
  taskId: string;
  parentTaskId: string | null;
  agentName: string;
  role: string;
  label: string;
  status: AgentStatus;
  lastEventAt: string;
}

export type ThreadKind = "group" | "dm" | "system";
export type MessageBadge = "group" | "dm" | "wait" | "woken" | "system";

export interface ThreadMessage {
  id: string;
  threadId: string;
  timestamp: string;
  fromTaskId?: string;
  fromLabel?: string;
  content: string;
  badge: MessageBadge;
  deliveredTaskIds?: string[];
  skippedTaskIds?: string[];
  failedTaskIds?: string[];
}

export interface ThreadState {
  id: string;
  kind: ThreadKind;
  title: string;
  participants: string[];
  messages: ThreadMessage[];
  unreadCount: number;
}

export type AgentTraceKind =
  | "run_started"
  | "run_completed"
  | "run_failed"
  | "run_cancelled"
  | "tool_started"
  | "tool_completed"
  | "tool_failed"
  | "wait_enter"
  | "wait_wake"
  | "message_sent"
  | "message_received"
  | "system";

export interface AgentTraceEntry {
  id: string;
  taskId: string;
  timestamp: string;
  kind: AgentTraceKind;
  text: string;
}

export interface FinalDeliverable {
  taskId: string;
  timestamp: string;
  content: string;
}
