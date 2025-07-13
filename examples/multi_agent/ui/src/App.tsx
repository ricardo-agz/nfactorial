import React, {
  useState,
  useRef,
  useEffect,
  useMemo,
} from 'react';
import {
  Loader2,
  MessageSquare,
} from 'lucide-react';

import { Message, ThinkingProgress } from './types';
import { generateUserId } from './utils';
import { ThinkingDropdown } from './components/ThinkingDropdown';
import { InputArea } from './components/InputArea';
import { useWebSocket } from './hooks/useWebSocket';
import { useChat } from './hooks/useChat';

const App: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput]       = useState('');
  const [loading, setLoading]   = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [steering, setSteering]     = useState(false);
  const [steerMode, setSteerMode]   = useState(false);
  const [steeringStatus, setSteeringStatus] =
    useState<'idle' | 'sending' | 'applied' | 'failed' | null>(null);

  const [userId] = useState(generateUserId);
  const [currentThinking, setCurrentThinking] =
    useState<ThinkingProgress | null>(null);
  const [currentTaskId, setCurrentTaskId] = useState<string | null>(null);

  /* Track progress for research sub-agent tasks */
  const [subAgentProgress, setSubAgentProgress] = useState<Record<string, ThinkingProgress>>({});

  // Batch progress percentage for current research run (0-100)
  const [researchProgress, setResearchProgress] = useState<number | null>(null);

  const messagesEndRef = useRef<HTMLDivElement>(null);

  useWebSocket({
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
  });

  const { sendPrompt, cancelCurrentTask, sendSteeringMessage } = useChat({
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
  });

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, currentThinking]);

  // Helper to format message content, handling <sub_task_results> blocks
  const renderContent = (content: string) => {
    if (content.includes('<sub_task_result')) {
      const regex = /<sub_task_result[^>]*>([\s\S]*?)<\/sub_task_result>/g;
      const items: string[] = [];
      let match;
      while ((match = regex.exec(content)) !== null) {
        items.push(match[1].trim());
      }
      if (items.length) {
        return (
          <ul className="list-disc pl-4 space-y-1 text-sm text-gray-800">
            {items.map((it, idx) => (
              <li key={idx}>{it}</li>
            ))}
          </ul>
        );
      }
    }
    return <p className="text-sm text-gray-800 whitespace-pre-wrap">{content}</p>;
  };

  const renderedMessages = useMemo(
    () =>
      messages.map(m => (
        <div key={m.id} className="mb-4">
          {m.role === 'user' ? (
            <div className="flex justify-end">
              <div className="bg-gray-800 text-white rounded-lg px-3 py-2 max-w-[80%]">
                <p className="text-sm">{m.content}</p>
              </div>
            </div>
          ) : (
            <div className="flex flex-col gap-2">
              {m.thinking && (
                <ThinkingDropdown
                  thinking={m.thinking}
                  subAgentProgress={subAgentProgress}
                  researchProgress={researchProgress ?? undefined}
                />
              )}
              <div className="bg-white border border-gray-200 rounded-lg px-3 py-2">
                {renderContent(m.content)}
              </div>
            </div>
          )}
        </div>
      )),
    [messages]
  );

  return (
    <div className="flex flex-col h-screen bg-gray-50">
      {/* Header */}
      <div className="text-center border-b border-gray-200 bg-white py-2">
        <div className="text-xs font-mono text-gray-500">
          {userId}
        </div>
      </div>

      {/* Messages Area */}
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-3xl mx-auto px-4 py-6">
          {messages.length === 0 && (
            <div className="text-center my-12">
              <div className="w-12 h-12 mx-auto mb-4 rounded-lg bg-gray-700 flex items-center justify-center">
                <MessageSquare className="w-6 h-6 text-white" />
              </div>
              <h2 className="text-lg font-medium text-gray-900 mb-1">
                Start a conversation
              </h2>
              <p className="text-gray-500 text-sm">
                Ask anything
              </p>
            </div>
          )}

          {renderedMessages}

          {(loading && currentThinking) ||
          (currentThinking && currentThinking.is_complete) ? (
            <div className="mb-4">
              <ThinkingDropdown
                thinking={currentThinking as ThinkingProgress}
                subAgentProgress={subAgentProgress}
                researchProgress={researchProgress ?? undefined}
              />
            </div>
          ) : null}

          {loading && !currentThinking && (
            <div className="flex items-center gap-2 text-gray-500 text-sm">
              <Loader2 className="w-4 h-4 animate-spin" />
              <span>Starting...</span>
            </div>
          )}

          {steeringStatus === 'applied' && (
            <div className="mb-4 text-center">
              <span className="inline-flex items-center gap-1 text-xs text-green-600 bg-green-50 px-2 py-1 rounded">
                <div className="w-1.5 h-1.5 bg-green-500 rounded-full" />
                Steering applied
              </span>
            </div>
          )}

          {steeringStatus === 'failed' && (
            <div className="mb-4 text-center">
              <span className="inline-flex items-center gap-1 text-xs text-red-600 bg-red-50 px-2 py-1 rounded">
                <div className="w-1.5 h-1.5 bg-red-500 rounded-full" />
                Steering failed
              </span>
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>
      </div>

      {/* Input Area */}
      <InputArea
        input={input}
        setInput={setInput}
        loading={loading}
        steerMode={steerMode}
        setSteerMode={setSteerMode}
        steering={steering}
        currentTaskId={currentTaskId}
        cancelling={cancelling}
        sendPrompt={sendPrompt}
        cancelCurrentTask={cancelCurrentTask}
        sendSteeringMessage={sendSteeringMessage}
      />
    </div>
  );
};

export default App;
