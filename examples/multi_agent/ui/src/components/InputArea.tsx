import React, { useRef } from 'react';
import {
  Send,
  X,
  Loader2,
  MessageSquare,
} from 'lucide-react';

interface InputAreaProps {
  input: string;
  setInput: (input: string) => void;
  loading: boolean;
  steerMode: boolean;
  setSteerMode: (steerMode: boolean) => void;
  steering: boolean;
  currentTaskId: string | null;
  cancelling: boolean;
  sendPrompt: () => void;
  cancelCurrentTask: () => void;
  sendSteeringMessage: () => void;
}

export const InputArea: React.FC<InputAreaProps> = ({
  input,
  setInput,
  loading,
  steerMode,
  setSteerMode,
  steering,
  currentTaskId,
  cancelling,
  sendPrompt,
  cancelCurrentTask,
  sendSteeringMessage,
}) => {
  const inputRef = useRef<HTMLTextAreaElement>(null);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (steerMode && !steering) {
      sendSteeringMessage();
    } else if (!loading) {
      sendPrompt();
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      if (steerMode && !steering) {
        sendSteeringMessage();
      } else if (!loading) {
        sendPrompt();
      }
    }
    if (e.key === 'Escape' && steerMode) {
      setSteerMode(false);
      setInput('');
    }
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInput(e.target.value);
    const el = e.target;
    el.style.height = 'auto';
    const maxHeight = window.innerHeight * 0.5;
    el.style.height = Math.min(Math.max(el.scrollHeight, 24), maxHeight) + 'px';
  };

  const handleSteerModeToggle = () => {
    setSteerMode(true);
    setTimeout(() => inputRef.current?.focus(), 100);
  };

  const handleCancelSteering = () => {
    setSteerMode(false);
    setInput('');
  };

  return (
    <div className="border-t border-gray-200 bg-white">
      <div className="max-w-3xl mx-auto px-4 py-3">
        <div
          className={`relative rounded-lg border transition-colors ${
            steerMode
              ? 'bg-blue-50 border-blue-200'
              : 'bg-gray-50 border-gray-200'
          }`}
        >
          <form className="flex items-center min-h-[44px]" onSubmit={handleSubmit}>
            {steerMode && (
              <div className="flex items-center pl-3 pr-2">
                <span className="inline-flex items-center gap-1 text-xs font-medium text-blue-600 bg-blue-100 px-2 py-1 rounded">
                  <MessageSquare className="w-3 h-3" />
                  Steer
                </span>
              </div>
            )}

            <textarea
              ref={inputRef}
              value={input}
              onChange={handleInputChange}
              onKeyDown={handleKeyDown}
              placeholder={
                steerMode ? "Guide the agent's next actions..." : 'Ask anything...'
              }
              rows={1}
              className={`flex-1 bg-transparent py-2.5 text-sm resize-none overflow-y-auto max-h-[50vh] min-h-6 focus:outline-none placeholder:text-gray-500 text-gray-900 ${
                steerMode ? 'pl-2 pr-4' : 'px-3'
              } [&::-webkit-scrollbar]:w-1 [&::-webkit-scrollbar-track]:bg-transparent [&::-webkit-scrollbar-thumb]:bg-gray-300 [&::-webkit-scrollbar-thumb]:rounded-full [&::-webkit-scrollbar-thumb:hover]:bg-gray-400`}
              disabled={loading && !steerMode}
            />

            <div className="absolute bottom-2 right-2 flex gap-1">
              {steerMode ? (
                <>
                  <button
                    type="button"
                    onClick={handleCancelSteering}
                    className="p-1.5 rounded bg-gray-200 hover:bg-gray-300 text-gray-600 transition-colors"
                    title="Cancel steering (Esc)"
                  >
                    <X className="w-4 h-4" />
                  </button>

                  <button
                    type="submit"
                    disabled={!input.trim() || steering}
                    className="p-1.5 rounded bg-blue-500 hover:bg-blue-600 text-white disabled:bg-blue-400 disabled:opacity-50 transition-colors"
                    title="Send steering message"
                  >
                    {steering ? (
                      <Loader2 className="w-4 h-4 animate-spin" />
                    ) : (
                      <Send className="w-4 h-4" />
                    )}
                  </button>
                </>
              ) : loading && currentTaskId ? (
                <>
                  <button
                    type="button"
                    onClick={handleSteerModeToggle}
                    className="p-1.5 rounded bg-blue-500 hover:bg-blue-600 text-white transition-colors"
                    title="Steer agent"
                  >
                    <MessageSquare className="w-4 h-4" />
                  </button>

                  <button
                    type="button"
                    onClick={cancelCurrentTask}
                    disabled={cancelling}
                    className="p-1.5 rounded bg-gray-200 hover:bg-gray-300 text-gray-600 disabled:text-gray-400 disabled:bg-gray-300 transition-colors cursor-pointer disabled:cursor-not-allowed"
                    title="Cancel task"
                  >
                    {cancelling ? (
                      <Loader2 className="w-4 h-4 animate-spin" />
                    ) : (
                      <X className="w-4 h-4" />
                    )}
                  </button>
                </>
              ) : (
                <button
                  type="submit"
                  disabled={!input.trim() || loading}
                  className="p-1.5 rounded bg-gray-700 hover:bg-gray-800 text-white disabled:bg-gray-300 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                >
                  <Send className="w-4 h-4" />
                </button>
              )}
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}; 