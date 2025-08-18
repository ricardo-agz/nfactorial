export interface BaseEvent {
  event_type: string;
  task_id: string;
  agent_name?: string;
  timestamp: string;
  error?: string;
}

export type ToolFunctionPayload = {
  name: string;
  arguments?: Record<string, unknown>;
};

export type ToolCallPayload = {
  id: string;
  function: ToolFunctionPayload;
};

export interface ToolActionStartedEvent extends BaseEvent {
  event_type: 'progress_update_tool_action_started';
  data: { args: [ToolCallPayload] };
}

export interface ToolActionCompletedEvent extends BaseEvent {
  event_type: 'progress_update_tool_action_completed';
  data: { result: { tool_call: ToolCallPayload; output_data?: Record<string, unknown> } };
}

export interface ToolActionFailedEvent extends BaseEvent {
  event_type: 'progress_update_tool_action_failed';
  data: { args: [ToolCallPayload] };
}

export interface AgentOutputEvent extends BaseEvent {
  event_type: 'agent_output';
  data: string;
}

export interface RunCancelledEvent extends BaseEvent {
  event_type: 'run_cancelled';
}

export interface RunFailedEvent extends BaseEvent {
  event_type: 'run_failed';
}

export type DefaultAgentEvent =
  | ToolActionStartedEvent
  | ToolActionCompletedEvent
  | ToolActionFailedEvent
  | AgentOutputEvent
  | RunCancelledEvent
  | RunFailedEvent;

export type DefaultEventMap = {
  progress_update_tool_action_started: ToolActionStartedEvent;
  progress_update_tool_action_completed: ToolActionCompletedEvent;
  progress_update_tool_action_failed: ToolActionFailedEvent;
  agent_output: AgentOutputEvent;
  run_cancelled: RunCancelledEvent;
  run_failed: RunFailedEvent;
};


