export type ActionStatus = 'pending' | 'running' | 'done' | 'failed';

interface BaseAction {
  id: string;
  kind: string;
  status: ActionStatus;
  timestamp: string; // ISO string supplied by backend
  metadata?: Record<string, unknown>; // optional, additional info like turn number
}

export interface UserMessageAction extends BaseAction {
  kind: 'user_message';
  content: string;
}

export interface AssistantThoughtAction extends BaseAction {
  kind: 'assistant_thought';
  content: string;
}

export interface ToolStartedAction extends BaseAction {
  kind: 'tool_started';
  toolName: string;
  arguments: unknown;
}

export interface ToolCompletedAction extends BaseAction {
  kind: 'tool_completed';
  toolName: string;
  result: unknown;
}

export interface ToolFailedAction extends BaseAction {
  kind: 'tool_failed';
  toolName: string;
  error: string;
}

export interface ExecRequestAction extends BaseAction {
  kind: 'exec_request';
  responseOnReject?: string;
}

export interface ExecResultAction extends BaseAction {
  kind: 'exec_result';
  output: string;
}

export interface SystemNoticeAction extends BaseAction {
  kind: 'system_notice';
  message: string;
}

export interface FinalAnswerAction extends BaseAction {
  kind: 'final_answer';
  content: string;
}

export type Action =
  | UserMessageAction
  | AssistantThoughtAction
  | ToolStartedAction
  | ToolCompletedAction
  | ToolFailedAction
  | ExecRequestAction
  | ExecResultAction
  | SystemNoticeAction
  | FinalAnswerAction;

export interface Run {
  id: string;
  userPrompt: string;
  actions: Action[];
} 