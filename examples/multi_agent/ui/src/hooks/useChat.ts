import { useCallback } from 'react';
import { Message } from '../types';
import { API_BASE_URL } from '../constants';

interface UseChatProps {
  userId: string;
  input: string;
  messages: Message[];
  currentTaskId: string | null;
  cancelling: boolean;
  steering: boolean;
  setInput: (input: string) => void;
  setMessages: React.Dispatch<React.SetStateAction<Message[]>>;
  setLoading: (loading: boolean) => void;
  setCurrentThinking: (updater: (prev: any) => any) => void;
  setCurrentTaskId: (taskId: string | null) => void;
  setCancelling: (cancelling: boolean) => void;
  setSteering: (steering: boolean) => void;
  setSteerMode: (steerMode: boolean) => void;
  setSteeringStatus: (status: 'idle' | 'sending' | 'applied' | 'failed' | null) => void;
}

export const useChat = ({
  userId,
  input,
  messages,
  currentTaskId,
  cancelling,
  steering,
  setInput,
  setMessages,
  setLoading,
  setCurrentThinking,
  setCurrentTaskId,
  setCancelling,
  setSteering,
  setSteerMode,
  setSteeringStatus,
}: UseChatProps) => {
  const sendPrompt = useCallback(async () => {
    if (!input.trim()) return;
    const prev = messages;

    const userMsg: Message = {
      id: Date.now(),
      role: 'user',
      content: input,
      timestamp: new Date(),
    };

    setMessages(p => [...p, userMsg]);
    setInput('');
    setLoading(true);
    setCurrentThinking(() => null);

    const res = await fetch(`${API_BASE_URL}/enqueue`, {
      method : 'POST',
      headers: { 'Content-Type': 'application/json' },
      body   : JSON.stringify({
        user_id : userId,
        query   : input,
        message_history: prev
          .filter(m => m.content)
          .map(({ role, content }) => ({ role, content })),
      }),
    });

    if (res.ok) {
      const { task_id } = await res.json();
      setCurrentTaskId(task_id);
    } else {
      console.error('enqueue failed');
      setLoading(false);
    }
  }, [input, messages, userId, setMessages, setInput, setLoading, setCurrentThinking, setCurrentTaskId]);

  const cancelCurrentTask = useCallback(async () => {
    if (!currentTaskId || cancelling) return;
    setCancelling(true);

    try {
      const res = await fetch(`${API_BASE_URL}/cancel`, {
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

  const sendSteeringMessage = useCallback(async () => {
    if (!input.trim() || !currentTaskId || steering) return;
    setSteering(true);
    setSteeringStatus('sending');

    try {
      const res = await fetch(`${API_BASE_URL}/steer`, {
        method : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body   : JSON.stringify({
          user_id: userId,
          task_id: currentTaskId,
          messages: [{ role: 'user', content: input }],
        }),
      });
      if (res.ok) {
        setInput('');
        setSteerMode(false);
      } else {
        setSteeringStatus('failed');
        setSteering(false);
      }
    } catch (err) {
      console.error('Error steering:', err);
      setSteeringStatus('failed');
      setSteering(false);
    }
  }, [input, currentTaskId, userId, steering, setInput, setSteerMode, setSteering, setSteeringStatus]);

  return {
    sendPrompt,
    cancelCurrentTask,
    sendSteeringMessage,
  };
}; 