import React, { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import { Send, Brain, Search, ChevronDown, ChevronRight, ExternalLink } from 'lucide-react';

const REST_ENDPOINT = 'http://localhost:8000/api/enqueue';
const WS_ROOT       = 'ws://localhost:8000/ws';

interface AgentEvent {
  event_type: string;
  task_id: string;
  agent_name?: string;
  turn?: number;
  data?: any;
  error?: string;
  timestamp: string;
}

interface ToolCall {
  id: string;
  tool_name: string;
  arguments: any;
  status: 'started' | 'completed' | 'failed';
  result?: any;
  error?: string;
}

interface ThinkingProgress {
  task_id: string;
  tool_calls: Record<string, ToolCall>;
  is_complete: boolean;
  final_output?: any;
  error?: string;
}

interface Message {
  id: number;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  thinking?: ThinkingProgress;
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

const getToolIcon = (toolName: string) => {
  switch (toolName) {
    case 'plan':
      return <Brain className="w-4 h-4" />;
    case 'search':
      return <Search className="w-4 h-4" />;
    default:
      return <div className="w-4 h-4 rounded-full bg-gray-400" />;
  }
};

const formatToolArguments = (toolName: string, args: any) => {
  const parsed = typeof args === 'string' ? JSON.parse(args) : args;
  
  switch (toolName) {
    case 'search':
      return `Searching: "${parsed.query}"`;
    case 'plan':
      return 'Creating plan...';
    case 'reflect':
      return 'Reflecting...';
    default:
      return JSON.stringify(parsed);
  }
};

const formatToolResult = (toolName: string, result: any) => {
  if (toolName === 'search' && Array.isArray(result)) {
    return (
      <div className="mt-3 space-y-2">
        <div className="text-xs text-gray-400 mb-3">{result.length} results</div>
        {result.slice(0, 6).map((item, idx) => (
          <a 
            key={idx}
            href={item.url} 
            target="_blank" 
            rel="noopener noreferrer"
            className="flex items-center gap-3 p-2 rounded-lg hover:bg-gray-50 transition-colors group"
          >
            <img 
              src={`https://www.google.com/s2/favicons?domain=${new URL(item.url).hostname}&sz=16`}
              alt=""
              className="w-4 h-4 flex-shrink-0"
              onError={(e) => {
                e.currentTarget.style.display = 'none';
              }}
            />
            <div className="min-w-0 flex-1">
              <div className="text-sm text-gray-900 group-hover:text-blue-600 font-medium truncate">
                {item.title}
              </div>
              <div className="text-xs text-gray-500 truncate">
                {new URL(item.url).hostname}
              </div>
            </div>
            <ExternalLink className="w-3 h-3 text-gray-400 opacity-0 group-hover:opacity-100 transition-opacity" />
          </a>
        ))}
      </div>
    );
  }

  if (toolName === 'plan' && result?.overview && Array.isArray(result.steps)) {
    return (
      <div className="mt-3 space-y-3">
        <div className="text-sm text-gray-700">{result.overview}</div>
        <div className="space-y-1.5">
          {result.steps.map((step: string, idx: number) => (
            <div key={idx} className="flex gap-3">
              <div className="w-5 h-5 rounded-full bg-blue-100 flex items-center justify-center flex-shrink-0 mt-0.5">
                <span className="text-xs font-medium text-blue-600">{idx + 1}</span>
              </div>
              <span className="text-sm text-gray-600">{step}</span>
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (toolName === 'reflect') {
    return (
      <div className="mt-3 p-3 bg-amber-50 rounded-lg border border-amber-100">
        <div className="text-sm text-amber-900">{String(result)}</div>
      </div>
    );
  }

  return (
    <div className="mt-3 text-sm text-gray-600">
      {String(result).substring(0, 150)}...
    </div>
  );
};

const ThinkingDropdown: React.FC<{ thinking: ThinkingProgress }> = ({ thinking }) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const toolCallsArray = Object.values(thinking.tool_calls);
  const hasActiveCalls = toolCallsArray.some(call => call.status === 'started');
  
  return (
    <div className="bg-gray-50 rounded-xl p-4 mb-4 border border-gray-100">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="flex items-center gap-2 w-full text-left group"
      >
        {isExpanded ? <ChevronDown className="w-4 h-4 text-gray-400" /> : <ChevronRight className="w-4 h-4 text-gray-400" />}
        <span className="text-sm font-medium text-gray-600">
          {thinking.is_complete ? 'Thinking complete' : 'Thinking...'}
        </span>
        {hasActiveCalls && <div className="w-1.5 h-1.5 bg-blue-500 rounded-full animate-pulse" />}
      </button>
      
      {isExpanded && (
        <div className="mt-4 space-y-3">
          {toolCallsArray.map((toolCall) => (
            <div key={toolCall.id} className="bg-white rounded-lg border border-gray-100 p-3">
              <div className="flex items-start gap-3">
                <div className="flex-shrink-0 mt-0.5">
                  {getToolIcon(toolCall.tool_name)}
                </div>
                
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-2">
                    <span className="text-sm font-medium capitalize text-gray-700">{toolCall.tool_name}</span>
                    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                      toolCall.status === 'completed' ? 'bg-green-100 text-green-700' :
                      toolCall.status === 'failed' ? 'bg-red-100 text-red-700' :
                      'bg-blue-100 text-blue-700'
                    }`}>
                      {toolCall.status}
                    </span>
                  </div>
                  
                  <div className="text-xs text-gray-500 mb-3">
                    {formatToolArguments(toolCall.tool_name, toolCall.arguments)}
                  </div>
                  
                  {toolCall.status === 'completed' && toolCall.result && (
                    <div>{formatToolResult(toolCall.tool_name, toolCall.result)}</div>
                  )}
                  
                  {toolCall.status === 'failed' && toolCall.error && (
                    <div className="text-xs text-red-600 p-2 bg-red-50 rounded border border-red-100">
                      {toolCall.error}
                    </div>
                  )}
                </div>
              </div>
            </div>
          ))}
          
          {thinking.error && (
            <div className="p-3 bg-red-50 border border-red-100 rounded-lg text-sm text-red-700">
              {thinking.error}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

const App: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [userId] = useState<string>(getUserId());
  const [currentThinking, setCurrentThinking] = useState<ThinkingProgress | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  /** auto-scroll every render */
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, currentThinking]);

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
    setCurrentThinking(null);
    
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
    const event: AgentEvent = JSON.parse(evt.data);
    console.log("Received event:", event);
    
    switch (event.event_type) {
      case 'tool_call_started':
        if (event.data) {
          setCurrentThinking(prev => {
            const thinking = prev || {
              task_id: event.task_id,
              tool_calls: {},
              is_complete: false
            };
            
            return {
              ...thinking,
              tool_calls: {
                ...thinking.tool_calls,
                [event.data.tool_call_id]: {
                  id: event.data.tool_call_id,
                  tool_name: event.data.tool_name,
                  arguments: event.data.arguments,
                  status: 'started'
                }
              }
            };
          });
        }
        break;
        
      case 'tool_call_completed':
        if (event.data) {
          setCurrentThinking(prev => {
            if (!prev) return null;
            
            return {
              ...prev,
              tool_calls: {
                ...prev.tool_calls,
                [event.data.tool_call_id]: {
                  ...prev.tool_calls[event.data.tool_call_id],
                  status: 'completed',
                  result: event.data.result
                }
              }
            };
          });
        }
        break;
        
      case 'tool_call_failed':
        if (event.data) {
          setCurrentThinking(prev => {
            if (!prev) return null;
            
            return {
              ...prev,
              tool_calls: {
                ...prev.tool_calls,
                [event.data.tool_call_id]: {
                  ...prev.tool_calls[event.data.tool_call_id],
                  status: 'failed',
                  error: event.error
                }
              }
            };
          });
        }
        break;
        
      case 'agent_output':
        // Final output received
        const finalOutput = typeof event.data === 'string' ? event.data : 
                           event.data?.final_output || JSON.stringify(event.data, null, 2);
        
        setMessages(prev => [
          ...prev,
          { 
            id: Date.now(), 
            role: 'assistant', 
            content: finalOutput, 
            timestamp: new Date(),
            thinking: currentThinking ? { ...currentThinking, is_complete: true, final_output: event.data } : undefined
          },
        ]);
        
        setCurrentThinking(null);
        setLoading(false);
        break;
        
      case 'run_failed':
      case 'task_failed':
        // Handle errors
        setCurrentThinking(prev => prev ? { ...prev, is_complete: true, error: event.error } : null);
        setLoading(false);
        break;
        
      default:
        // Log other events for debugging
        console.log("Unhandled event type:", event.event_type);
    }
  }, [currentThinking]);

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
            <div className="bg-gray-800 text-white rounded-2xl px-4 py-2 max-w-[80%]">
              <p className="text-sm">{m.content}</p>
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-3">
            {m.thinking && <ThinkingDropdown thinking={m.thinking} />}
            <div className="bg-white border border-gray-200 rounded-2xl px-4 py-3">
              <p className="text-sm text-gray-800">{m.content}</p>
            </div>
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
          
          {/* Show current thinking progress */}
          {loading && currentThinking && (
            <div className="mb-6">
              <ThinkingDropdown thinking={currentThinking} />
            </div>
          )}
          
          {loading && !currentThinking && (
            <p className="text-gray-500 text-sm">Starting...</p>
          )}
          
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
                  className="p-2 bg-gray-800 rounded-full disabled:opacity-50 text-white"
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