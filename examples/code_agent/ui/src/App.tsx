import React, { useState, useRef, useCallback } from 'react';
import Editor, { DiffEditor } from '@monaco-editor/react';
import type { editor as MonacoEditor } from 'monaco-editor';
import { MessageSquare, X, Loader, Send, Check } from 'lucide-react';
import { useWebSocket } from './hooks/useWebSocket';
import { useChat } from './hooks/useChat';
import type { Action } from './types/run';
import { useRuns } from './context/RunContext';
import { API_BASE } from './constants';

const STARTER_CODE = `// QuickSort
function quickSort(arr) {
  if (arr.length <= 1) return arr;
  
  const pivot = arr[Math.floor(arr.length / 2)];
  const left = arr.filter(x => x < pivot);
  const middle = arr.filter(x => x === pivot);
  const right = arr.filter(x => x > pivot);
  
  return [...quickSort(left), ...middle, ...quickSort(right)];
}

// Example usage
const numbers = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5];
console.log(quickSort(numbers));`

// Generate a unique user ID for this session
const USER_ID = `user_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

function App() {
  const [code, setCode] = useState(STARTER_CODE);
  // Hold a potential code update coming from the agent that the user can accept/reject
  const [proposedCode, setProposedCode] = useState<string | null>(null);
  
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [currentTaskId, setCurrentTaskId] = useState<string | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const [sidebarWidth, setSidebarWidth] = useState(450);
  const [isResizing, setIsResizing] = useState(false);
  const resizeRef = useRef<HTMLDivElement>(null);
  // Ref and helper to auto-resize the chat textarea
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  // Track if we're currently processing a code-execution decision
  const [executingCode, setExecutingCode] = useState(false);
  // Track which execution action is currently being processed so we can show a loader
  const [executionAction, setExecutionAction] = useState<'accept' | 'reject' | null>(null);
  // Reference to the Monaco DiffEditor so we can read the edited value on "Accept"
  const diffEditorRef = useRef<MonacoEditor.IStandaloneDiffEditor | null>(null);

  const MIN_SIDEBAR_WIDTH = 250;
  const MAX_SIDEBAR_WIDTH = 800;

  // Runs context
  const { runs, runOrder, updateAction } = useRuns();

  const timelineActions = React.useMemo(() => {
    return runOrder.flatMap(id => runs[id]?.actions || []);
  }, [runOrder, runs]);

  // Initialize WebSocket connection
  useWebSocket({
    userId: USER_ID,
    setLoading,
    setCurrentTaskId,
    setCancelling,
    setProposedCode,
  });

  // Initialize chat functionality
  const { sendPrompt, cancelCurrentTask } = useChat({
    userId: USER_ID,
    input,
    currentTaskId,
    cancelling,
    code: proposedCode ?? code,
    setInput,
    setLoading,
    setCurrentTaskId,
    setCancelling,
  });

  const startResize = useCallback((e: React.MouseEvent) => {
    setIsResizing(true);
    e.preventDefault();
  }, []);

  const resize = useCallback((e: MouseEvent) => {
    if (!isResizing) return;
    
    const newWidth = window.innerWidth - e.clientX;
    const clampedWidth = Math.min(Math.max(newWidth, MIN_SIDEBAR_WIDTH), MAX_SIDEBAR_WIDTH);
    setSidebarWidth(clampedWidth);
  }, [isResizing]);

  const stopResize = useCallback(() => {
    setIsResizing(false);
  }, []);

  const adjustTextareaHeight = () => {
    const el = textareaRef.current;
    if (el) {
      el.style.height = 'auto';
      el.style.height = `${el.scrollHeight}px`;
    }
  };

  React.useEffect(() => {
    if (isResizing) {
      document.addEventListener('mousemove', resize);
      document.addEventListener('mouseup', stopResize);
      document.body.style.cursor = 'col-resize';
      document.body.style.userSelect = 'none';
    }

    return () => {
      document.removeEventListener('mousemove', resize);
      document.removeEventListener('mouseup', stopResize);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
  }, [isResizing, resize, stopResize]);

  // Keep the textarea height in sync with its content
  React.useEffect(() => {
    adjustTextareaHeight();
  }, [input]);

  const handleSendMessage = async () => {
    if (!input.trim() || loading) return;
    await sendPrompt();
  };

  const handleCancelTask = () => {
    if (currentTaskId && !cancelling) {
      cancelCurrentTask();
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  /* ------------------------------------------------------------------ */
  /* Helper – Run the current JS code and capture console output         */
  /* ------------------------------------------------------------------ */

  const runCurrentCode = () => {
    const logs: string[] = [];
    const originalLog = console.log;
    // Capture console.log output
    console.log = (...args: unknown[]) => {
      logs.push(
        (args as unknown[])
          .map((arg) =>
            typeof arg === 'object' ? JSON.stringify(arg) : String(arg)
          )
          .join(' ')
      );
      // Also forward to the real console
      originalLog(...(args as unknown[]));
    };

    try {
      // Execute the code in an isolated function scope
      const result = new Function(code)();
      if (result !== undefined) {
        logs.push(`Returned: ${String(result)}`);
      }
    } catch (err: unknown) {
      logs.push(`Error: ${String(err)}`);
    } finally {
      console.log = originalLog;
    }

    return logs.join('\n');
  };

  /* ------------------------------------------------------------------ */
  /* Deferred execution request helpers                                  */
  /* ------------------------------------------------------------------ */

  const pendingExecRequest = React.useMemo(() => {
    if (timelineActions.length === 0) return null;
    return [...timelineActions].reverse().find(
      (a) => a.kind === 'exec_request' && a.status === 'running'
    ) as (Action & { kind: 'exec_request' }) | undefined | null;
  }, [timelineActions]);

  const handleRejectExecution = async () => {
    if (!pendingExecRequest || !currentTaskId) return;
    setExecutingCode(true);
    setExecutionAction('reject');

    const rejectMsg = (pendingExecRequest.responseOnReject) ?? 'Execution rejected.';

    try {
      await fetch(`${API_BASE}/complete_tool`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          user_id: USER_ID,
          task_id: currentTaskId,
          tool_call_id: pendingExecRequest.id,
          result: rejectMsg,
        }),
      });
    } catch (err: unknown) {
      console.error('Failed to send reject for execution request', err);
    }

    // Update action locally
    updateAction(currentTaskId!, pendingExecRequest.id, prev => ({
      ...(prev as Action),
      status: 'failed',
    }));

    const noticeAction: Action = {
      id: `reject_${Date.now()}`,
      kind: 'system_notice',
      status: 'done',
      message: rejectMsg,
      timestamp: new Date().toISOString(),
    } as Action;
    updateAction(currentTaskId!, noticeAction.id, () => noticeAction);

    setExecutingCode(false);
    setExecutionAction(null);
  };

  const handleAcceptExecution = async () => {
    if (!pendingExecRequest || !currentTaskId) return;
    setExecutingCode(true);
    setExecutionAction('accept');

    const output = runCurrentCode();

    try {
      await fetch(`${API_BASE}/complete_tool`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          user_id: USER_ID,
          task_id: currentTaskId,
          tool_call_id: pendingExecRequest.id,
          result: output || 'Code executed successfully (no output).',
        }),
      });
    } catch (err: unknown) {
      console.error('Failed to complete execution request', err);
    }

    // add exec result action locally so timeline shows output immediately
    const resultAction: Action = {
      id: `exec_result_${Date.now()}`,
      kind: 'exec_result',
      status: 'done',
      output,
      timestamp: new Date().toISOString(),
    } as Action;
    updateAction(currentTaskId!, resultAction.id, () => resultAction);

    updateAction(currentTaskId!, pendingExecRequest.id, prev => ({
      ...(prev as Action),
      status: 'done',
    }));

    setExecutingCode(false);
    setExecutionAction(null);
  };

  return (
    <div className="h-screen flex" style={{ backgroundColor: '#1e1e1e' }}>
      {/* Code Editor */}
      <div className="flex-1 flex flex-col" style={{ width: `calc(100% - ${sidebarWidth}px)` }}>
        {/* Editor Header */}
        <div className="px-4 py-2" style={{ backgroundColor: '#2d2d30', borderBottom: '1px solid #3e3e42' }}>
          <h1 className="font-medium" style={{ color: '#cccccc' }}>main.js</h1>
        </div>
        
        {/* Code / Diff Editor */}
        <div className="flex-1">
          {proposedCode === null ? (
            <Editor
              height="100%"
              defaultLanguage="javascript"
              value={code}
              onChange={(value) => setCode(value || '')}
              theme="vs-dark"
              options={{
                minimap: { enabled: false },
                fontSize: 14,
                lineNumbers: 'on',
                roundedSelection: false,
                scrollBeyondLastLine: false,
                automaticLayout: true,
              }}
            />
          ) : (
            <div className="h-full flex flex-col">
              <DiffEditor
                keepCurrentOriginalModel={true}
                keepCurrentModifiedModel={true}
                height="100%"
                original={code}
                modified={proposedCode}
                language="javascript"
                theme="vs-dark"
                onMount={(editor) => {
                  diffEditorRef.current = editor;
                }}
                options={{
                  // Always show an inline (single-column) diff where deletions (red)
                  // appear above additions (green)
                  renderSideBySide: false,
                  // Allow editing in the diff so Monaco can surface per-hunk
                  // Accept / Reject controls (arrow icons and gutter menu).
                  // Users can still bulk-accept via our external button, and we
                  // read whatever merge result they leave in the modified pane.
                  readOnly: false,
                  originalEditable: false,
                  renderMarginRevertIcon: true,
                  lineNumbers: 'on',
                  minimap: { enabled: false },
                  automaticLayout: true,
                }}
              />
              <div className="p-2 flex gap-2 justify-end" style={{ backgroundColor: '#2d2d30', borderTop: '1px solid #3e3e42' }}>
                <button
                  onClick={() => setProposedCode(null)}
                  className="px-3 py-1 rounded-sm bg-red-600 hover:bg-red-700 cursor-pointer"
                >
                  Reject
                </button>
                <button
                  onClick={() => {
                    if (diffEditorRef.current) {
                      const updatedCode = diffEditorRef.current.getModifiedEditor().getValue();
                      setCode(updatedCode);
                      setProposedCode(null);
                    } else if (proposedCode !== null) {
                      setCode(proposedCode);
                      setProposedCode(null);
                    }
                  }}
                  className="px-3 py-1 rounded-sm bg-green-600 hover:bg-green-700 cursor-pointer"
                >
                  Accept
                </button>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Resize Handle */}
      <div
        ref={resizeRef}
        onMouseDown={startResize}
        className={`w-1 cursor-col-resize transition-colors ${
          isResizing ? '' : 'hover:bg-blue-500'
        }`}
        style={{ 
          backgroundColor: isResizing ? '#007acc' : '#3e3e42'
        }}
      />

      {/* Chat Sidebar */}
      <div 
        className="flex flex-col"
        style={{ 
          width: `${sidebarWidth}px`,
          backgroundColor: '#252526',
          borderLeft: '1px solid #3e3e42'
        }}
      > 
        {/* Timeline */}
        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          {(!runs[runOrder[0]] || runs[runOrder[0]].actions.length === 0) ? (
            <div className="text-center py-24" style={{ color: '#858585' }}>
              <MessageSquare className="w-12 h-12 mx-auto mb-4 opacity-50" />
              <p className="text-sm">Ask me anything about your code!</p>
            </div>
          ) : (
            <>
              {timelineActions.map((action) => {
                switch (action.kind) {
                  case 'user_message':
                    return (
                      <div key={action.id} className="ml-4">
                        <div className="px-3 py-2 rounded-lg ml-auto" style={{ backgroundColor: '#007acc', color: '#cccccc' }}>
                          <p className="text-sm whitespace-pre-wrap">{action.content}</p>
                        </div>
                      </div>
                    );
                  case 'system_notice':
                    return (
                      <div key={action.id} className="mr-4">
                        <div className="px-3 py-2 rounded-lg" style={{ backgroundColor: '#3c3c3c', color: '#cccccc' }}>
                          <p className="text-sm whitespace-pre-wrap">{'message' in action ? (action as Action & {message: string}).message : ''}</p>
                        </div>
                      </div>
                    );
                  case 'exec_result':
                    return (
                      <div key={action.id} className="mr-4">
                        <pre className="p-3 rounded bg-gray-800 text-gray-300 text-xs overflow-x-auto"><code>{'output' in action ? (action as Action & {output: string}).output : ''}</code></pre>
                      </div>
                    );
                  case 'tool_completed':
                    if (action.toolName === 'edit_code') {
                      const res = action.result as { find_start_line?: number; find_end_line?: number } | undefined;
                      if (res && res.find_start_line && res.find_end_line) {
                        return (
                          <div key={action.id} className="text-xs text-purple-400">
                            {res.find_start_line === res.find_end_line 
                              ? `Edited line ${res.find_start_line}`
                              : `Edited lines ${res.find_start_line}-${res.find_end_line}`}
                          </div>
                        );
                      }
                    }
                    if (typeof (action.result as { new_code?: string } | undefined)?.new_code !== 'undefined') return null; // diff handled elsewhere
                    return (
                      <div key={action.id} className="text-xs text-gray-400">Tool {action.toolName} completed</div>
                    );
                  case 'tool_started':
                    return (
                      <div key={action.id} className="text-xs text-gray-500">Running {action.toolName}...</div>
                    );
                  case 'assistant_thought':
                    return (
                      <div key={action.id} className="mr-4 px-2 text-xs" style={{ color: '#a0a0a0', fontStyle: 'italic' }}>
                        {'content' in action ? (action as Action & {content: string}).content : ''}
                      </div>
                    );
                  case 'final_answer':
                    return (
                      <div key={action.id} className="mr-4">
                        <div className="px-3 py-2 rounded-lg" style={{ backgroundColor: '#3c3c3c', color: '#cccccc' }}>
                          <p className="text-sm whitespace-pre-wrap">{'content' in action ? (action as Action & {content: string}).content : ''}</p>
                        </div>
                      </div>
                    );
                  default:
                    return null;
                }
              })}
              {(loading || timelineActions.some(a => a.status === 'running')) && (
                <div className="flex items-center gap-1 text-xs text-gray-500 ml-4">
                  <Loader className="w-3 h-3 animate-spin" />
                  <span>Thinking...</span>
                </div>
              )}
            </>
          )}
        </div>
        {/* Execution request prompt */}
        {pendingExecRequest && (
          <div className="p-4 border-t border-gray-700 flex flex-col gap-2">
            <p className="text-sm text-gray-200">The agent requests to execute the code.</p>
            <div className="flex gap-2 justify-end">
              <button
                onClick={handleRejectExecution}
                disabled={executingCode}
                className="px-3 py-2 rounded bg-red-600 hover:bg-red-700 disabled:opacity-50 flex items-center justify-center"
              >
                {executingCode && executionAction === 'reject' ? (
                  <Loader className="w-4 h-4 animate-spin" />
                ) : (
                  <X className="w-4 h-4" />
                )}
              </button>
              <button
                onClick={handleAcceptExecution}
                disabled={executingCode}
                className="px-3 py-2 rounded bg-green-600 hover:bg-green-700 disabled:opacity-50 flex items-center justify-center"
              >
                {executingCode && executionAction === 'accept' ? (
                  <Loader className="w-4 h-4 animate-spin" />
                ) : (
                  <Check className="w-4 h-4" />
                )}
              </button>
            </div>
          </div>
        )}
        {/* Chat Input */}
        <div className="p-4 border-t border-gray-700">
          <div className="flex gap-2 items-end">
            <textarea
              ref={textareaRef}
              rows={2}
              value={input}
              onChange={(e) => {
                setInput(e.target.value);
                adjustTextareaHeight();
              }}
              onKeyDown={handleKeyPress}
              placeholder="Plan, search, build anything"
              disabled={loading}
              className="flex-1 resize-none rounded-lg px-4 py-3 placeholder-gray-400 focus:outline-none focus:ring-1 focus:ring-blue-500 disabled:opacity-50"
              style={{
                backgroundColor: '#3c3c3c',
                border: '1px solid #5a5a5a',
                color: '#cccccc',
                fontFamily: 'system-ui, -apple-system, sans-serif',
                fontSize: '14px',
                overflow: 'hidden'
              }}
            />
            {!loading && (
              <button
                onClick={handleSendMessage}
                disabled={!input.trim()}
                className="w-8 h-8 rounded-full bg-gray-300 hover:bg-gray-500 disabled:opacity-50 flex-shrink-0 flex items-center justify-center cursor-pointer"
                title="Send message"
              >
                <Send className="w-4 h-4" />
              </button>
            )}
            {loading && currentTaskId && (
              <button
                onClick={handleCancelTask}
                disabled={cancelling}
                className="w-8 h-8 flex items-center justify-center rounded-lg bg-red-600 hover:bg-red-700 disabled:opacity-50 flex-shrink-0 cursor-pointer disabled:cursor-not-allowed"
                title="Cancel current task"
              >
                {cancelling ? (
                  <Loader className="w-4 h-4 animate-spin" />
                ) : (
                  <X className="w-4 h-4" />
                )}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
