import { useCallback } from "react";

import { API_BASE_URL } from "../constants";

interface UseChatProps {
  userId: string;
  input: string;
  currentTaskId: string | null;
  cancelling: boolean;
  setInput: (value: string) => void;
  setLoading: (value: boolean) => void;
  setCurrentTaskId: (taskId: string | null) => void;
  setCancelling: (value: boolean) => void;
  onBeforeEnqueue?: () => void;
}

export function useChat({
  userId,
  input,
  currentTaskId,
  cancelling,
  setInput,
  setLoading,
  setCurrentTaskId,
  setCancelling,
  onBeforeEnqueue,
}: UseChatProps) {
  const sendPrompt = useCallback(async () => {
    if (!input.trim()) {
      return;
    }

    onBeforeEnqueue?.();
    setLoading(true);

    const query = input.trim();
    setInput("");

    const response = await fetch(`${API_BASE_URL}/enqueue`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        user_id: userId,
        query,
        message_history: [],
      }),
    });

    if (!response.ok) {
      setLoading(false);
      throw new Error("Failed to enqueue task");
    }

    const payload = (await response.json()) as { task_id: string };
    setCurrentTaskId(payload.task_id);
  }, [
    input,
    onBeforeEnqueue,
    setCurrentTaskId,
    setInput,
    setLoading,
    userId,
  ]);

  const cancelCurrentTask = useCallback(async () => {
    if (!currentTaskId || cancelling) {
      return;
    }
    setCancelling(true);

    try {
      const response = await fetch(`${API_BASE_URL}/cancel`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: userId,
          task_id: currentTaskId,
        }),
      });
      if (!response.ok) {
        setCancelling(false);
      }
    } catch (_error) {
      setCancelling(false);
    }
  }, [cancelling, currentTaskId, setCancelling, userId]);

  return { sendPrompt, cancelCurrentTask };
}
