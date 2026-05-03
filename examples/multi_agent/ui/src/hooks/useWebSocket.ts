import { useCallback, useEffect, useRef } from 'react';
import { AgentEvent, Message, ThinkingProgress } from '../types';
import { WS_BASE_URL } from '../constants';

interface UseWebSocketProps {
  userId: string;
  setCurrentThinking: (
    updater: (prev: ThinkingProgress | null) => ThinkingProgress | null,
  ) => void;
  setMessages: React.Dispatch<React.SetStateAction<Message[]>>;
  setLoading: (loading: boolean) => void;
  setCurrentTaskId: (taskId: string | null) => void;
  setCancelling: (cancelling: boolean) => void;
  setSteering: (steering: boolean) => void;
  setSteerMode: (steerMode: boolean) => void;
  setSteeringStatus: (
    status: 'idle' | 'sending' | 'applied' | 'failed' | null,
  ) => void;
  setSubAgentProgress?: React.Dispatch<
    React.SetStateAction<Record<string, ThinkingProgress>>
  >;
  setResearchProgress?: React.Dispatch<React.SetStateAction<number | null>>;
}

const HIDDEN_TOOL_NAMES = new Set(['done']);

const createThinkingProgress = (
  taskId: string,
  previous: ThinkingProgress | null,
): ThinkingProgress => ({
  task_id: taskId,
  tool_calls: previous?.tool_calls ?? {},
  is_complete: previous?.is_complete ?? false,
  final_output: previous?.final_output,
  error: previous?.error,
});

const resolveToolCallId = (event: AgentEvent): string =>
  event.tool_call_id ?? `${event.task_id}:${event.tool_name ?? 'tool'}`;

const resolveToolName = (event: AgentEvent): string => event.tool_name ?? 'tool';

const resolveEventError = (event: AgentEvent): string | undefined => {
  if (typeof event.error === 'string' && event.error.trim()) {
    return event.error;
  }

  if (
    event.run_error &&
    typeof event.run_error === 'object' &&
    typeof event.run_error.message === 'string' &&
    event.run_error.message.trim()
  ) {
    return event.run_error.message;
  }

  if (event.is_error && typeof event.output === 'string' && event.output.trim()) {
    return event.output;
  }

  return undefined;
};

const formatOutputForDisplay = (output: unknown): string => {
  if (typeof output === 'string') {
    return output.trim();
  }

  if (output && typeof output === 'object') {
    const payload = output as Record<string, unknown>;
    if (typeof payload.final_output === 'string') {
      return payload.final_output.trim();
    }
    if (typeof payload.summary === 'string') {
      return payload.summary.trim();
    }
    try {
      return JSON.stringify(output, null, 2);
    } catch {
      return String(output);
    }
  }

  if (output == null) {
    return '';
  }

  return String(output);
};

const extractResearchTaskIds = (outputData: unknown): string[] => {
  if (Array.isArray(outputData)) {
    return Array.from(
      new Set(
        outputData.filter(
          (id: unknown): id is string => typeof id === 'string' && id.length > 0,
        ),
      ),
    );
  }

  if (!outputData || typeof outputData !== 'object') {
    return [];
  }

  const payload = outputData as Record<string, unknown>;
  const childTaskIds = Array.isArray(payload.child_task_ids)
    ? payload.child_task_ids.filter(
        (id: unknown): id is string => typeof id === 'string' && id.length > 0,
      )
    : [];

  if (childTaskIds.length) {
    return Array.from(new Set(childTaskIds));
  }

  const jobRefs = Array.isArray(payload.job_refs) ? payload.job_refs : [];
  return Array.from(
    new Set(
      jobRefs
        .map((job) =>
          typeof (job as { task_id?: unknown })?.task_id === 'string'
            ? (job as { task_id: string }).task_id
            : null,
        )
        .filter((id): id is string => Boolean(id)),
    ),
  );
};

export const useWebSocket = ({
  userId,
  setCurrentThinking,
  setMessages,
  setLoading,
  setCurrentTaskId,
  setCancelling,
  setSteering,
  setSteerMode,
  setSteeringStatus,
  setSubAgentProgress,
  setResearchProgress,
}: UseWebSocketProps) => {
  const wsRef = useRef<WebSocket | null>(null);
  const thinkingRef = useRef<ThinkingProgress | null>(null);
  const processedTasksRef = useRef<Set<string>>(new Set());
  const subAgentProgressRef = useRef<Record<string, ThinkingProgress>>({});

  const updateThinking = useCallback(
    (updater: (prev: ThinkingProgress | null) => ThinkingProgress | null) => {
      setCurrentThinking((prev) => {
        const next = updater(prev);
        thinkingRef.current = next;
        return next;
      });
    },
    [setCurrentThinking],
  );

  const updateSubAgentThinking = useCallback(
    (
      taskId: string,
      updater: (prev: ThinkingProgress | null) => ThinkingProgress | null,
    ) => {
      setSubAgentProgress?.((prev) => {
        const next = updater(prev[taskId] ?? null);
        if (!next) {
          return prev;
        }
        const updated = { ...prev, [taskId]: next };
        subAgentProgressRef.current = updated;
        return updated;
      });
    },
    [setSubAgentProgress],
  );

  const ensureResearchTaskCards = useCallback(
    (taskIds: string[]) => {
      if (!taskIds.length) {
        return;
      }
      setResearchProgress?.(0);
      setSubAgentProgress?.((prev) => {
        const updated = { ...prev };
        for (const taskId of taskIds) {
          if (!updated[taskId]) {
            updated[taskId] = {
              task_id: taskId,
              tool_calls: {},
              is_complete: false,
            };
          }
        }
        subAgentProgressRef.current = updated;
        return updated;
      });
    },
    [setResearchProgress, setSubAgentProgress],
  );

  const resetRunUi = useCallback(() => {
    setCurrentThinking(() => null);
    thinkingRef.current = null;
    setLoading(false);
    setCancelling(false);
    setCurrentTaskId(null);
    setSteering(false);
    setSteerMode(false);
    setSteeringStatus(null);
    setResearchProgress?.(null);
  }, [
    setCurrentThinking,
    setLoading,
    setCancelling,
    setCurrentTaskId,
    setSteering,
    setSteerMode,
    setSteeringStatus,
    setResearchProgress,
  ]);

  const handleToolStart = useCallback(
    (
      event: AgentEvent,
      updateProgress: (
        updater: (prev: ThinkingProgress | null) => ThinkingProgress | null,
      ) => void,
    ) => {
      const toolName = resolveToolName(event);
      if (HIDDEN_TOOL_NAMES.has(toolName)) {
        return;
      }

      updateProgress((prev) => {
        const base = createThinkingProgress(event.task_id, prev);
        const toolCallId = resolveToolCallId(event);
        const existing = base.tool_calls[toolCallId];
        return {
          ...base,
          tool_calls: {
            ...base.tool_calls,
            [toolCallId]: {
              id: toolCallId,
              tool_name: toolName,
              arguments: existing?.arguments,
              status: 'started',
              result: undefined,
              error: undefined,
            },
          },
        };
      });
    },
    [],
  );

  const handleToolFinish = useCallback(
    (
      event: AgentEvent,
      updateProgress: (
        updater: (prev: ThinkingProgress | null) => ThinkingProgress | null,
      ) => void,
    ) => {
      const toolName = resolveToolName(event);
      if (toolName === 'research' && !event.is_error) {
        ensureResearchTaskCards(extractResearchTaskIds(event.output));
      }
      if (HIDDEN_TOOL_NAMES.has(toolName)) {
        return;
      }

      updateProgress((prev) => {
        const base = createThinkingProgress(event.task_id, prev);
        const toolCallId = resolveToolCallId(event);
        const existing = base.tool_calls[toolCallId];
        return {
          ...base,
          tool_calls: {
            ...base.tool_calls,
            [toolCallId]: {
              id: toolCallId,
              tool_name: toolName,
              arguments: existing?.arguments,
              status: event.is_error ? 'failed' : 'completed',
              result: event.is_error ? existing?.result : event.output,
              error: event.is_error ? resolveEventError(event) : undefined,
            },
          },
        };
      });
    },
    [ensureResearchTaskCards],
  );

  const handleSubAgentFinish = useCallback(
    (event: AgentEvent) => {
      if (processedTasksRef.current.has(event.task_id)) {
        return;
      }
      const isFailure = event.status === 'failed';
      const isCancelled = event.status === 'cancelled';
      updateSubAgentThinking(event.task_id, (prev) => ({
        ...createThinkingProgress(event.task_id, prev),
        is_complete: true,
        final_output: event.output,
        error: isFailure
          ? resolveEventError(event) ?? 'Agent failed to complete the task'
          : isCancelled
            ? 'Task cancelled by user'
            : undefined,
      }));
      processedTasksRef.current.add(event.task_id);
    },
    [updateSubAgentThinking],
  );

  const appendAssistantMessage = useCallback(
    (content: string, thinking?: ThinkingProgress) => {
      setMessages((prev) => [
        ...prev,
        {
          id: Date.now(),
          role: 'assistant',
          content,
          timestamp: new Date(),
          thinking,
        },
      ]);
    },
    [setMessages],
  );

  const handleMainFinish = useCallback(
    (event: AgentEvent) => {
      if (processedTasksRef.current.has(event.task_id)) {
        return;
      }

      const isFailure = event.status === 'failed';
      const isCancelled = event.status === 'cancelled';
      const snapshotThinking = thinkingRef.current
        ? {
            ...thinkingRef.current,
            is_complete: true,
            final_output: event.output,
            error: isFailure
              ? resolveEventError(event) ?? 'Agent failed to complete the task'
              : isCancelled
                ? 'Task cancelled by user'
                : undefined,
          }
        : undefined;

      const content = isFailure
        ? 'Failed to get agent response.'
        : isCancelled
          ? 'Task was cancelled.'
          : formatOutputForDisplay(event.output) || 'Completed.';

      appendAssistantMessage(content, snapshotThinking);
      processedTasksRef.current.add(event.task_id);
      resetRunUi();
    },
    [appendAssistantMessage, resetRunUi],
  );

  const handleWSMessage = useCallback(
    (evt: MessageEvent) => {
      const event: AgentEvent = JSON.parse(evt.data);

      if (event.event_type === 'batch_progress') {
        if (typeof event.progress === 'number') {
          setResearchProgress?.(event.progress);
        }
        return;
      }

      if (event.event_type === 'batch_completed') {
        setResearchProgress?.(100);
        return;
      }

      if (event.event_type === 'run_steering_applied') {
        setSteeringStatus('applied');
        setSteering(false);
        window.setTimeout(() => setSteeringStatus(null), 2000);
        return;
      }

      if (event.event_type === 'run_steering_failed') {
        setSteeringStatus('failed');
        setSteering(false);
        setSteerMode(false);
        window.setTimeout(() => setSteeringStatus(null), 3000);
        return;
      }

      const isTrackedSubAgentTask = Boolean(subAgentProgressRef.current[event.task_id]);
      const isResearchSubAgentEvent = event.agent_name === 'research_subagent';

      if (isTrackedSubAgentTask || isResearchSubAgentEvent) {
        if (event.event_type === 'tool_start') {
          handleToolStart(event, (updater) =>
            updateSubAgentThinking(event.task_id, updater),
          );
          return;
        }

        if (event.event_type === 'tool_finish') {
          handleToolFinish(event, (updater) =>
            updateSubAgentThinking(event.task_id, updater),
          );
          return;
        }

        if (event.event_type === 'finish') {
          handleSubAgentFinish(event);
          return;
        }
      }

      if (event.event_type === 'tool_start') {
        handleToolStart(event, updateThinking);
        return;
      }

      if (event.event_type === 'tool_finish') {
        handleToolFinish(event, updateThinking);
        return;
      }

      if (event.event_type === 'finish') {
        handleMainFinish(event);
        return;
      }

      if (event.event_type === 'turn_finish' && event.output && !thinkingRef.current) {
        updateThinking((prev) => createThinkingProgress(event.task_id, prev));
      }
    },
    [
      handleMainFinish,
      handleSubAgentFinish,
      handleToolFinish,
      handleToolStart,
      setResearchProgress,
      setSteering,
      setSteerMode,
      setSteeringStatus,
      updateSubAgentThinking,
      updateThinking,
    ],
  );

  useEffect(() => {
    let disposed = false;
    let reconnectTimer: number | null = null;

    const connect = () => {
      const ws = new WebSocket(`${WS_BASE_URL}/${userId}`);
      ws.onmessage = handleWSMessage;
      ws.onerror = () => ws.close();
      ws.onclose = () => {
        if (disposed) {
          return;
        }
        reconnectTimer = window.setTimeout(connect, 1000);
      };
      wsRef.current = ws;
    };

    connect();

    return () => {
      disposed = true;
      if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer);
      }
      wsRef.current?.close();
    };
  }, [handleWSMessage, userId]);

  return wsRef;
};