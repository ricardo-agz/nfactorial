import { useCallback } from 'react';
import type { Action } from '../types/run';
import { useRuns } from '../context/RunContext';
import { API_BASE } from '../constants';

interface UseChatProps {
  userId: string;
  input: string;
  currentTaskId: string | null;
  cancelling: boolean;
  code: string;
  setInput: (input: string) => void;
  setLoading: (loading: boolean) => void;
  setCurrentTaskId: (taskId: string | null) => void;
  setCancelling: (cancelling: boolean) => void;
}

export const useChat = ({
  userId,
  input,
  currentTaskId,
  cancelling,
  code,
  setInput,
  setLoading,
  setCurrentTaskId,
  setCancelling,
}: UseChatProps) => {
  const { runs, runOrder, createRun, addAction } = useRuns();
  const sendPrompt = useCallback(async () => {
    if (!input.trim()) return;
    setLoading(true);
    setInput('');

    // Build message history consisting of final outputs (assistant answers) from previous runs
    const message_history = runOrder.flatMap((id) => {
      const run = runs[id];
      if (!run) return [] as { role: string; content: string }[];
      return run.actions
        .filter((a): a is Action & { kind: 'final_answer'; content: string } => a.kind === 'final_answer')
        .map((final) => ({ role: 'assistant', content: (final as { content: string }).content }));
    });

    const res = await fetch(`${API_BASE}/enqueue`, {
      method : 'POST',
      headers: { 'Content-Type': 'application/json' },
      body   : JSON.stringify({
        user_id : userId,
        query   : input,
        code    : code,
        message_history,
      }),
    });

    if (res.ok) {
      const { task_id } = await res.json();
      createRun(task_id, input);
      setCurrentTaskId(task_id);

      // store user message action
      const userAction: Action = {
        id: `user_${Date.now()}`,
        kind: 'user_message',
        status: 'done',
        content: input,
        timestamp: new Date().toISOString(),
      } as const;
      addAction(task_id, userAction);
    } else {
      console.error('enqueue failed');
      setLoading(false);
    }
  }, [input, userId, code, setInput, setLoading, createRun, addAction, setCurrentTaskId, runs, runOrder]);

  const cancelCurrentTask = useCallback(async () => {
    if (!currentTaskId || cancelling) return;
    setCancelling(true);

    try {
      const res = await fetch(`${API_BASE}/cancel`, {
        method : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body   : JSON.stringify({ user_id: userId, task_id: currentTaskId }),
      });
      if (!res.ok) setCancelling(false);
    } catch (err) {
      console.error('Error cancelling task:', err);
      setCancelling(false);
    }
  }, [currentTaskId, userId, cancelling, setCancelling]);

  return {
    sendPrompt,
    cancelCurrentTask,
  };
}; 