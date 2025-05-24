import React, { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import { Send } from 'lucide-react';

const REST_ENDPOINT = 'http://localhost:8000/api/enqueue';
const WS_ROOT       = 'ws://localhost:8000/ws';

interface WSUpdate {
  task_id: string;
  event: 'started' | 'step' | 'complete';
  content?: string;
}

interface Message {
  id: number;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
}

const generateUserId = (): string => {
  return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
};

const getUserId = (): string => {
  const existingId = sessionStorage.getItem('userId');
  if (existingId) return existingId;
  
  const newId = generateUserId();
  sessionStorage.setItem('userId', newId);
  return newId;
};

const App: React.FC = () => {
  const [messages, setMessages]   = useState<Message[]>([]);
  const [input, setInput]         = useState('');
  const [loading, setLoading]     = useState(false);
  const [userId] = useState<string>(getUserId());
  const wsRef = useRef<WebSocket | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  /** auto-scroll every render */
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  /** focus input after sending a message */
  useEffect(() => {
    if (!loading && messages.length > 0) {
      inputRef.current?.focus();
    }
  }, [loading, messages.length]);

  const sendPrompt = useCallback(async () => {
    if (!input.trim()) return;
    const prevMessages = messages;
    const userMsg: Message = {
      id: Date.now(),
      role: 'user',
      content: input,
      timestamp: new Date(),
    };
    setMessages(prev => [...prev, userMsg]);
    setInput('');
    setLoading(true);
    
    const res = await fetch(REST_ENDPOINT, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ 
        user_id: userId, 
        query: input,
        message_history: prevMessages.filter(m => m.content).map(m => ({
          role: m.role,
          content: m.content,
        })),
      }),
    });
    if (!res.ok) console.error('enqueue failed');
  }, [input, messages, userId]);

  const handleWSMessage = useCallback((evt: MessageEvent) => {
    const data: WSUpdate = JSON.parse(evt.data);
    if (data.event === 'complete') {
      const text =
        typeof data.content === 'string'
          ? data.content
          : JSON.stringify(data.content, null, 2);
      setMessages(prev => [
        ...prev,
        { id: Date.now(), role: 'assistant', content: text, timestamp: new Date() },
      ]);
      setLoading(false);
    } else {
      console.log("event", data.event);
    }
  }, []);

  /** open WS once on mount */
  useEffect(() => {
    const ws = new WebSocket(`${WS_ROOT}/${userId}`);
    ws.onmessage = handleWSMessage;
    wsRef.current = ws;
    return () => ws.close();
  }, [userId, handleWSMessage]);

  const renderedMessages = useMemo(() => (
    messages.map(m => (
      <div key={m.id} className="mb-6">
        {m.role === 'user' ? (
          <div className="flex justify-end">
            <div className="bg-gray-800 text-white rounded-2xl px-4 py-2">
              <p className="text-sm">{m.content}</p>
            </div>
          </div>
        ) : (
          <div className="flex gap-3">
            <p className="text-gray-800 text-sm">{m.content}</p>
          </div>
        )}
      </div>
    ))
  ), [messages]);

  return (
    <div className="flex flex-col h-screen bg-gray-50">
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-3xl mx-auto px-4 py-8">
          {messages.length === 0 && (
            <div className="text-center text-gray-500 my-8">
              <p className="mt-2">Start a new conversation</p>
            </div>
          )}
          {renderedMessages}
          {loading && <p className="text-gray-500 text-sm">...</p>}
          <div ref={messagesEndRef} />
        </div>
      </div>

      {/* input */}
      <div className="border-t border-gray-200 bg-white">
        <div className="max-w-3xl mx-auto px-4 py-4">
          <div className="relative bg-gray-50 rounded-[26px] shadow-sm border border-gray-200">
            <form
              className="flex items-center min-h-[52px]"
              onSubmit={e => {
                e.preventDefault();
                sendPrompt();
                inputRef.current?.focus();
              }}
            >
              <textarea
                ref={inputRef}
                value={input}
                onChange={e => {
                  setInput(e.target.value);
                  e.target.style.height = 'auto';
                  e.target.style.height = Math.max(e.target.scrollHeight, 24) + 'px';
                }}
                onKeyDown={e => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    sendPrompt();
                  }
                }}
                placeholder="Ask anything"
                rows={1}
                className="w-full bg-transparent px-6 py-3 text-sm focus:outline-none placeholder:text-gray-500 text-black resize-none overflow-hidden leading-6 min-h-6"
                disabled={loading}
              />
              <div className="absolute bottom-2.5 right-2">
                <button
                  type="submit"
                  disabled={!input.trim() || loading}
                  className="p-2 bg-gray-800 rounded-full disabled:opacity-50"
                >
                  <Send className="w-4 h-4" />
                </button>
              </div>
            </form>
          </div>
        </div>
      </div>
    </div>
  );
};
export default App;