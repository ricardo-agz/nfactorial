import React, {
  useState,
  useRef,
  useEffect,
  useCallback,
  useMemo,
} from 'react';
import {
  Send,
  Brain,
  Search,
  ChevronDown,
  ChevronRight,
  ExternalLink,
  X,
  Loader2,
  MessageSquare,
  Sparkles,
} from 'lucide-react';

const REST_ENDPOINT   = 'http://localhost:8000/api/enqueue';
const CANCEL_ENDPOINT = 'http://localhost:8000/api/cancel';
const STEER_ENDPOINT  = 'http://localhost:8000/api/steer';
const WS_ROOT         = 'ws://localhost:8000/ws';

/* -------------------------------------------------------------------------- */
/*                               Type Helpers                                 */
/* -------------------------------------------------------------------------- */
interface AgentEvent {
  event_type : string;
  task_id    : string;
  agent_name?: string;
  turn?      : number;
  data?      : any;
  error?     : string;
  timestamp  : string;
}

interface ToolCall {
  id        : string;
  tool_name : string;
  arguments : any;
  status    : 'started' | 'completed' | 'failed';
  result?   : any;
  error?    : string;
}

interface ThinkingProgress {
  task_id     : string;
  tool_calls  : Record<string, ToolCall>;
  is_complete : boolean;
  final_output?: any;
  error?       : string;
}

interface Message {
  id        : number;
  role      : 'user' | 'assistant';
  content   : string;
  timestamp : Date;
  thinking ?: ThinkingProgress;
}

/* -------------------------------------------------------------------------- */
/*                                Utilities                                   */
/* -------------------------------------------------------------------------- */
const generateUserId = () =>
  Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2);

const getToolIcon = (name: string) => {
  switch (name) {
    case 'plan'  : return <Brain className="w-3 h-3 text-blue-600" />;
    case 'search': return <Search className="w-3 h-3 text-blue-600" />;
    case 'reflect': return <Sparkles className="w-3 h-3 text-blue-600" />;
    default      : return <div className="w-3 h-3 rounded-full bg-gray-300" />;
  }
};

const formatToolArguments = (name: string, args: any) => {
  const parsed = typeof args === 'string' ? JSON.parse(args) : args;
  switch (name) {
    case 'search' : return `Searching: "${parsed.query}"`;
    case 'plan'   : return 'Creating plan...';
    case 'reflect': return 'Reflecting...';
    default       : return JSON.stringify(parsed);
  }
};

const formatToolResult = (name: string, result: any) => {
  if (name === 'search' && Array.isArray(result)) {
    return (
      <div className="mt-2 space-y-1">
        <div className="text-xs text-gray-500 mb-2">
          {result.length} results
        </div>
        {result.slice(0, 6).map((item, idx) => (
          <a
            key={idx}
            href={item.url}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 p-2 rounded hover:bg-gray-50 transition-colors group text-xs"
          >
            <img
              src={`https://www.google.com/s2/favicons?domain=${new URL(item.url).hostname}&sz=16`}
              alt=""
              className="w-3 h-3 flex-shrink-0"
              onError={e => { (e.currentTarget as HTMLImageElement).style.display = 'none'; }}
            />
            <div className="min-w-0 flex-1">
              <div className="font-medium truncate text-gray-900 group-hover:text-blue-600">
                {item.title}
              </div>
              <div className="text-gray-500 truncate">
                {new URL(item.url).hostname}
              </div>
            </div>
            <ExternalLink className="w-3 h-3 text-gray-400 opacity-0 group-hover:opacity-100 transition-opacity" />
          </a>
        ))}
      </div>
    );
  }

  if (name === 'plan' && result?.overview && Array.isArray(result.steps)) {
    return (
      <div className="mt-2 space-y-2">
        <div className="text-xs text-gray-700">
          {result.overview}
        </div>
        <div className="space-y-1">
          {result.steps.map((step: string, idx: number) => (
            <div key={idx} className="flex gap-2 text-xs">
              <div className="w-4 h-4 mt-0.5 rounded-full bg-blue-100 flex items-center justify-center flex-shrink-0">
                <span className="text-xs font-medium text-blue-600">{idx + 1}</span>
              </div>
              <span className="text-gray-600">{step}</span>
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (name === 'reflect') {
    return (
      <div className="mt-2 p-2 bg-amber-50 border border-amber-200 rounded text-xs">
        <div className="text-amber-900">{String(result)}</div>
      </div>
    );
  }

  return (
    <div className="mt-2 text-xs text-gray-600">
      {String(result).slice(0, 150)}...
    </div>
  );
};

/* -------------------------------------------------------------------------- */
/*                       Thinking Progress Dropdown UI                        */
/* -------------------------------------------------------------------------- */
const ThinkingDropdown: React.FC<{ thinking: ThinkingProgress }> = ({
  thinking,
}) => {
  const [open, setOpen] = useState(true);
  const calls = Object.values(thinking.tool_calls);
  const hasActive = calls.some(c => c.status === 'started');

  return (
    <div className="bg-gray-50 rounded-lg p-3 mb-3 border border-gray-200">
      <button
        className="flex items-center gap-2 w-full text-left group"
        onClick={() => setOpen(!open)}
      >
        {open ? (
          <ChevronDown className="w-4 h-4 text-gray-400" />
        ) : (
          <ChevronRight className="w-4 h-4 text-gray-400" />
        )}
        <span className="text-sm font-medium text-gray-700">
          {thinking.is_complete ? 'Thinking complete' : 'Thinking...'}
        </span>
        {hasActive && (
          <div className="w-1.5 h-1.5 bg-blue-500 rounded-full animate-pulse" />
        )}
      </button>

      {open && (
        <div className="mt-3 space-y-2">
          {calls.map(call => (
            <div
              key={call.id}
              className="bg-white rounded border border-gray-200 p-2"
            >
              <div className="flex items-start gap-2">
                <div className="flex-shrink-0 mt-1">
                  {getToolIcon(call.tool_name)}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-xs font-medium capitalize text-gray-700">
                      {call.tool_name}
                    </span>
                    <span
                      className={`text-xs px-1.5 py-0.5 rounded font-medium ${
                        call.status === 'completed'
                          ? 'bg-green-100 text-green-700'
                          : call.status === 'failed'
                          ? 'bg-red-100 text-red-700'
                          : 'bg-blue-100 text-blue-700'
                      }`}
                    >
                      {call.status}
                    </span>
                  </div>

                  <div className="text-xs text-gray-500 mb-2">
                    {formatToolArguments(call.tool_name, call.arguments)}
                  </div>

                  {call.status === 'completed' && call.result && (
                    <div>{formatToolResult(call.tool_name, call.result)}</div>
                  )}

                  {call.status === 'failed' && call.error && (
                    <div className="text-xs text-red-600 p-1 bg-red-50 border border-red-200 rounded">
                      {call.error}
                    </div>
                  )}
                </div>
              </div>
            </div>
          ))}

          {thinking.error && (
            <div className="p-2 bg-red-50 border border-red-200 rounded text-xs text-red-700">
              {thinking.error}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

/* -------------------------------------------------------------------------- */
/*                            Main App Component                              */
/* -------------------------------------------------------------------------- */
const App: React.FC = () => {
  /* ----------------------------- local state ----------------------------- */
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

  /* refs */
  const wsRef            = useRef<WebSocket | null>(null);
  const messagesEndRef   = useRef<HTMLDivElement>(null);
  const inputRef         = useRef<HTMLTextAreaElement>(null);
  const thinkingRef      = useRef<ThinkingProgress | null>(null);

  /* keep ref in sync with state */
  useEffect(() => { thinkingRef.current = currentThinking; }, [currentThinking]);

  /* --------------------------- helper callbacks -------------------------- */
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
    setCurrentThinking(null);

    const res = await fetch(REST_ENDPOINT, {
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
  }, [input, messages, userId]);

  const cancelCurrentTask = useCallback(async () => {
    if (!currentTaskId || cancelling) return;
    setCancelling(true);

    try {
      const res = await fetch(CANCEL_ENDPOINT, {
        method : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body   : JSON.stringify({ user_id: userId, task_id: currentTaskId }),
      });
      if (!res.ok) setCancelling(false);
    } catch (err) {
      console.error('Error cancelling task:', err);
      setCancelling(false);
    }
  }, [currentTaskId, userId, cancelling]);

  const sendSteeringMessage = useCallback(async () => {
    if (!input.trim() || !currentTaskId || steering) return;
    setSteering(true);
    setSteeringStatus('sending');

    try {
      const res = await fetch(STEER_ENDPOINT, {
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
  }, [input, currentTaskId, userId, steering]);

  /* ------------------------ websocket message handler ------------------- */
  const handleWSMessage = useCallback((evt: MessageEvent) => {
    const event: AgentEvent = JSON.parse(evt.data);
    console.log('WS event:', event);

    const updateThinking = (updater: (prev: ThinkingProgress | null) => ThinkingProgress | null) => {
      setCurrentThinking(prev => {
        const next = updater(prev);
        /* keep ref aligned */
        thinkingRef.current = next;
        return next;
      });
    };

    switch (event.event_type) {
      /* ----- tool events ----- */
      case 'progress_update_tool_action_started': {
        const toolCall = event.data?.args?.[0];
        if (!toolCall) break;

        updateThinking(prev => {
          const base: ThinkingProgress = prev ?? {
            task_id    : event.task_id,
            tool_calls : {},
            is_complete: false,
          };
          return {
            ...base,
            tool_calls: {
              ...base.tool_calls,
              [toolCall.id]: {
                id       : toolCall.id,
                tool_name: toolCall.function.name,
                arguments: toolCall.function.arguments,
                status   : 'started',
              },
            },
          };
        });
        break;
      }

      case 'progress_update_tool_action_completed': {
        const resp      = event.data?.result;
        const toolCall  = resp?.tool_call;
        if (!toolCall) break;

        updateThinking(prev => {
          if (!prev) return null;
          return {
            ...prev,
            tool_calls: {
              ...prev.tool_calls,
              [toolCall.id]: {
                ...prev.tool_calls[toolCall.id],
                status: 'completed',
                result: resp.output_data,
              },
            },
          };
        });
        break;
      }

      case 'progress_update_tool_action_failed': {
        const toolCall = event.data?.args?.[0];
        if (!toolCall) break;

        updateThinking(prev => {
          if (!prev) return null;
          return {
            ...prev,
            tool_calls: {
              ...prev.tool_calls,
              [toolCall.id]: {
                ...prev.tool_calls[toolCall.id],
                status: 'failed',
                error : event.error,
              },
            },
          };
        });
        break;
      }

      /* ----- completion events (optional) ----- */
      case 'progress_update_completion_failed':
        updateThinking(prev => (prev ? { ...prev, error: event.error } : null));
        break;

      /* ----- steering status ----- */
      case 'run_steering_applied':
        setSteeringStatus('applied');
        setSteering(false);
        setTimeout(() => setSteeringStatus(null), 2000);
        break;

      case 'run_steering_failed':
        setSteeringStatus('failed');
        setSteering(false);
        setSteerMode(false);
        setTimeout(() => setSteeringStatus(null), 3000);
        break;

      /* ----- final answer ----- */
      case 'agent_output': {
        const content =
          typeof event.data === 'string'
            ? event.data
            : event.data?.final_output ?? JSON.stringify(event.data, null, 2);

        setMessages(prev => [
          ...prev,
          {
            id: Date.now(),
            role: 'assistant',
            content,
            timestamp: new Date(),
          },
        ]);

        const finished = thinkingRef.current
          ? { ...thinkingRef.current, is_complete: true, final_output: event.data }
          : null;

        if (finished) {
          setMessages(prev => {
            const last = prev[prev.length - 1];
            return [
              ...prev.slice(0, -1),
              { ...last, thinking: finished },
            ];
          });
        }

        setCurrentThinking(null);
        setLoading(false);
        setCurrentTaskId(null);
        setSteering(false);
        setSteerMode(false);
        setSteeringStatus(null);
        break;
      }

      /* ----- cancellation ----- */
      case 'run_cancelled': {
        const snap = thinkingRef.current;
        let message = 'Task was cancelled.';
        let snapshotThinking: ThinkingProgress | undefined;

        if (snap && Object.keys(snap.tool_calls).length) {
          const completed = Object.values(snap.tool_calls).filter(
            c => c.status === 'completed'
          ).length;
          message = "Task was cancelled."

          snapshotThinking = {
            ...snap,
            is_complete: true,
            error: 'Task cancelled by user',
          };
        }

        setMessages(prev => [
          ...prev,
          {
            id: Date.now(),
            role: 'assistant',
            content: message,
            timestamp: new Date(),
            thinking: snapshotThinking,
          },
        ]);

        setCurrentThinking(null);
        setLoading(false);
        setCancelling(false);
        setCurrentTaskId(null);
        setSteering(false);
        setSteerMode(false);
        setSteeringStatus(null);
        break;
      }

      /* ----- generic failure ----- */
      case 'run_failed':
      case 'task_failed':
        updateThinking(prev =>
          prev ? { ...prev, is_complete: true, error: event.error } : null
        );
        setLoading(false);
        setCancelling(false);
        setCurrentTaskId(null);
        setSteering(false);
        setSteerMode(false);
        setSteeringStatus(null);
        break;

      default:
        console.log('Unhandled event:', event);
    }
  }, []);

  /* ------------------------------ side-effects --------------------------- */
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, currentThinking]);

  useEffect(() => {
    if (!loading && messages.length) inputRef.current?.focus();
  }, [loading, messages.length]);

  /* open websocket once */
  useEffect(() => {
    const ws = new WebSocket(`${WS_ROOT}/${userId}`);
    ws.onmessage = handleWSMessage;
    wsRef.current = ws;
    return () => ws.close();
  }, [userId, handleWSMessage]);

  /* ------------------------------ rendering ------------------------------ */
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
              {m.thinking && <ThinkingDropdown thinking={m.thinking} />}
              <div className="bg-white border border-gray-200 rounded-lg px-3 py-2">
                <p className="text-sm text-gray-800">{m.content}</p>
              </div>
            </div>
          )}
        </div>
      )),
    [messages]
  );

  /* ---------------------------------------------------------------------- */
  /*                                  JSX                                   */
  /* ---------------------------------------------------------------------- */
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
                Ask me anything and I'll help you think through it
              </p>
            </div>
          )}

          {renderedMessages}

          {(loading && currentThinking) ||
          (currentThinking && currentThinking.is_complete) ? (
            <div className="mb-4">
              <ThinkingDropdown thinking={currentThinking as ThinkingProgress} />
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
      <div className="border-t border-gray-200 bg-white">
        <div className="max-w-3xl mx-auto px-4 py-3">
          <div
            className={`relative rounded-lg border transition-colors ${
              steerMode
                ? 'bg-blue-50 border-blue-200'
                : 'bg-gray-50 border-gray-200'
            }`}
          >
            <form
              className="flex items-center min-h-[44px]"
              onSubmit={e => {
                e.preventDefault();
                if (steerMode && !steering) {
                  sendSteeringMessage();
                } else if (!loading) {
                  sendPrompt();
                }
              }}
            >
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
                onChange={e => {
                  setInput(e.target.value);
                  const el = e.target;
                  el.style.height = 'auto';
                  el.style.height = Math.max(el.scrollHeight, 24) + 'px';
                }}
                onKeyDown={e => {
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
                }}
                placeholder={
                  steerMode ? "Guide the agent's next actions..." : 'Ask anything...'
                }
                rows={1}
                className={`flex-1 bg-transparent py-2.5 text-sm resize-none overflow-hidden min-h-6 focus:outline-none placeholder:text-gray-500 text-gray-900 ${
                  steerMode ? 'pl-2 pr-4' : 'px-3'
                }`}
                disabled={loading && !steerMode}
              />

              <div className="absolute bottom-2 right-2 flex gap-1">
                {steerMode ? (
                  <>
                    <button
                      type="button"
                      onClick={() => {
                        setSteerMode(false);
                        setInput('');
                      }}
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
                      onClick={() => {
                        setSteerMode(true);
                        setTimeout(() => inputRef.current?.focus(), 100);
                      }}
                      className="p-1.5 rounded bg-blue-500 hover:bg-blue-600 text-white transition-colors"
                      title="Steer agent"
                    >
                      <MessageSquare className="w-4 h-4" />
                    </button>

                    <button
                      type="button"
                      onClick={cancelCurrentTask}
                      disabled={cancelling}
                      className="p-1.5 rounded bg-gray-200 hover:bg-gray-300 text-gray-600 disabled:text-gray-400 disabled:bg-gray-300 transition-colors"
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
    </div>
  );
};

export default App;
