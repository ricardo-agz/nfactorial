import { useCallback, useRef, useEffect } from 'react';
import type { AgentEvent } from '../types';
import { WS_BASE } from '../constants';
import { useRuns } from '../context/RunContext';
import type { Action } from '../types/run';

interface UseWebSocketProps {
  userId: string;
  setLoading: (loading: boolean) => void;
  setCurrentTaskId: (taskId: string | null) => void;
  setCancelling: (cancelling: boolean) => void;
  setProposedCode: (code: string) => void;
}

export const useWebSocket = ({
  userId,
  setLoading,
  setCurrentTaskId,
  setCancelling,
  setProposedCode,
}: UseWebSocketProps) => {
  const wsRef = useRef<WebSocket | null>(null);
  const { addAction, updateAction } = useRuns();

  const parseToolArguments = (raw: unknown): Record<string, unknown> => {
    if (typeof raw === 'string') {
      try {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed === 'object') return parsed as Record<string, unknown>;
      } catch {
        return {};
      }
    }
    if (raw && typeof raw === 'object') return raw as Record<string, unknown>;
    return {};
  };

  const formatExecOutput = (outputData: Record<string, unknown> | null, fallback: string): string => {
    if (!outputData) return fallback;

    const exitCode = outputData.exit_code;
    const stdout = typeof outputData.stdout === 'string' ? outputData.stdout : '';
    const stderr = typeof outputData.stderr === 'string' ? outputData.stderr : '';
    const lines: string[] = [];

    if (typeof exitCode === 'number') lines.push(`Exit code: ${exitCode}`);
    if (stdout) lines.push(`stdout:\n${stdout}`);
    if (stderr) lines.push(`stderr:\n${stderr}`);

    if (lines.length > 0) return lines.join('\n\n');
    return fallback;
  };

  const handleWSMessage = useCallback((evt: MessageEvent) => {
    const event: AgentEvent = JSON.parse(evt.data);
    console.log('WS event:', event);

    switch (event.event_type) {
      case 'progress_update_tool_action_started': {
        const toolCall = event.data?.args?.[0];
        if (!toolCall) break;
        const parsedArgs = parseToolArguments(toolCall.function.arguments);

        const startAction: Action = {
          id: toolCall.id,
          kind: toolCall.function.name === 'request_code_execution' ? 'exec_request' : 'tool_started',
          status: 'running',
          timestamp: event.timestamp,
          ...(toolCall.function.name === 'request_code_execution'
            ? { responseOnReject: parsedArgs.response_on_reject as string | undefined }
            : {
                toolName: toolCall.function.name,
                arguments: toolCall.function.arguments,
              }),
        } as Action;
        addAction(event.task_id, startAction);
        break;
      }

      case 'progress_update_tool_action_completed': {
        const resp      = event.data?.result;
        const toolCall  = resp?.tool_call;
        if (!toolCall) break;

        // If this is an edit_code tool completion, propose the code change
        if (toolCall.function.name === 'edit_code' && resp.client_output?.new_code) {
          setProposedCode(resp.client_output.new_code);
        }

        if (toolCall.function.name === 'request_code_execution') {
          const outputData = (resp.client_output && typeof resp.client_output === 'object')
            ? (resp.client_output as Record<string, unknown>)
            : null;

          if (resp.pending_result && outputData?.kind === 'hook_session_pending') {
            const requestedHooks = Array.isArray(outputData.requested_hooks)
              ? (outputData.requested_hooks as Array<Record<string, unknown>>)
              : [];
            const firstHook = requestedHooks[0];

            updateAction(event.task_id, toolCall.id, prev => ({
              ...(prev ?? {
                id: toolCall.id,
                kind: 'exec_request',
                timestamp: event.timestamp,
              }),
              kind: 'exec_request',
              status: 'running',
              hookId: typeof firstHook?.hook_id === 'string' ? firstHook.hook_id : undefined,
              hookToken: typeof firstHook?.token === 'string' ? firstHook.token : undefined,
            }) as Action);
          } else {
            updateAction(event.task_id, toolCall.id, prev => ({
              ...(prev ?? {
                id: toolCall.id,
                kind: 'exec_request',
                timestamp: event.timestamp,
              }),
              kind: 'exec_request',
              status: 'done',
            }) as Action);

            const outputText = formatExecOutput(
              outputData,
              typeof resp.model_output === 'string' ? resp.model_output : 'Execution completed.'
            );

            const resultAction: Action = {
              id: `exec_result_${Date.now()}`,
              kind: 'exec_result',
              status: 'done',
              output: outputText,
              timestamp: event.timestamp,
            } as Action;
            addAction(event.task_id, resultAction);
          }
        } else {
          // mark tool completed
          updateAction(event.task_id, toolCall.id, prev => ({
            ...(prev ?? {
              id: toolCall.id,
              kind: 'tool_completed',
              toolName: toolCall.function.name,
              timestamp: event.timestamp,
              status: 'done',
            }),
            kind: 'tool_completed',
            status: 'done',
            result: resp.client_output,
          }) as Action);

          // Special handling for think tool – show thought content
          if (toolCall.function.name === 'think' && typeof resp.client_output === 'string') {
            const thoughtAction: Action = {
              id: `thought_${Date.now()}`,
              kind: 'assistant_thought',
              status: 'done',
              content: resp.client_output,
              timestamp: event.timestamp,
            } as Action;
            addAction(event.task_id, thoughtAction);
          }
        }
        break;
      }

      case 'progress_update_tool_action_failed': {
        const toolCall = event.data?.args?.[0];
        if (!toolCall) break;

        if (toolCall.function.name === 'request_code_execution') {
          updateAction(event.task_id, toolCall.id, prev => ({
            ...(prev ?? {
              id: toolCall.id,
              kind: 'exec_request',
              timestamp: event.timestamp,
            }),
            kind: 'exec_request',
            status: 'failed',
          }) as Action);
        } else {
          updateAction(event.task_id, toolCall.id, prev => ({
            ...(prev ?? {
              id: toolCall.id,
              kind: 'tool_failed',
              toolName: toolCall.function.name,
              timestamp: event.timestamp,
            }),
            kind: 'tool_failed',
            status: 'failed',
            error: event.error,
          }) as Action);
        }
        break;
      }

      case 'agent_output': {
        const content: string = event.data;

        const answerAction: Action = {
          id: `answer_${Date.now()}`,
          kind: 'final_answer',
          status: 'done',
          content,
          timestamp: event.timestamp,
        } as Action;
        addAction(event.task_id, answerAction);
        setLoading(false);
        setCurrentTaskId(null);
        break;
      }

      case 'run_cancelled': {
        const notice: Action = {
          id: `cancel_${Date.now()}`,
          kind: 'system_notice',
          status: 'done',
          message: 'Task was cancelled.',
          timestamp: event.timestamp,
        } as Action;
        addAction(event.task_id, notice);
        setLoading(false);
        setCancelling(false);
        setCurrentTaskId(null);
        break;
      }

      case 'run_failed': {
        const notice: Action = {
          id: `fail_${Date.now()}`,
          kind: 'system_notice',
          status: 'done',
          message: 'Failed to get agent response.',
          timestamp: event.timestamp,
        } as Action;
        addAction(event.task_id, notice);
        setLoading(false);
        setCancelling(false);
        setCurrentTaskId(null);
        break;
      }

      default:
        console.log('Unhandled event:', event);
    }
  }, [setLoading, setCurrentTaskId, setCancelling, setProposedCode, addAction, updateAction]);

  useEffect(() => {
    const ws = new WebSocket(`${WS_BASE}/${userId}`);
    ws.onmessage = handleWSMessage;
    wsRef.current = ws;
    return () => ws.close();
  }, [userId, handleWSMessage]);

  return wsRef;
}; 