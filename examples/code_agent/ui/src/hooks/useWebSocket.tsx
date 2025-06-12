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

  const handleWSMessage = useCallback((evt: MessageEvent) => {
    const event: AgentEvent = JSON.parse(evt.data);
    console.log('WS event:', event);

    switch (event.event_type) {
      case 'progress_update_tool_action_started': {
        const toolCall = event.data?.args?.[0];
        if (!toolCall) break;

        const startAction: Action = {
          id: toolCall.id,
          kind: toolCall.function.name === 'request_code_execution' ? 'exec_request' : 'tool_started',
          status: 'running',
          timestamp: event.timestamp,
          ...(toolCall.function.name === 'request_code_execution'
            ? { responseOnReject: toolCall.function.arguments?.response_on_reject }
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
        if (toolCall.function.name === 'edit_code' && resp.output_data?.new_code) {
          setProposedCode(resp.output_data.new_code);
        }

        if (toolCall.function.name === 'request_code_execution') {
          // exec result action created after user accepts, skip for now
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
            result: resp.output_data,
          }) as Action);

          // Special handling for think tool – show thought content
          if (toolCall.function.name === 'think' && typeof resp.output_data === 'string') {
            const thoughtAction: Action = {
              id: `thought_${Date.now()}`,
              kind: 'assistant_thought',
              status: 'done',
              content: resp.output_data,
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