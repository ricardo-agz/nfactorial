import { Loader2, Send, Square } from "lucide-react";
import { useMemo, useState } from "react";

import { AgentTracePanel } from "./components/AgentTracePanel";
import { FinalDeliverablePanel } from "./components/FinalDeliverablePanel";
import { OrgChart } from "./components/OrgChart";
import { ThreadSidebar } from "./components/ThreadSidebar";
import { ThreadView } from "./components/ThreadView";
import { useChat } from "./hooks/useChat";
import { useWebSocket } from "./hooks/useWebSocket";
import type {
  AgentNode,
  AgentTraceEntry,
  FinalDeliverable,
  ThreadState,
} from "./types";

const SYSTEM_THREAD_ID = "thread:system";

function createUserId(): string {
  return `user_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function initialSystemThread(): Record<string, ThreadState> {
  return {
    [SYSTEM_THREAD_ID]: {
      id: SYSTEM_THREAD_ID,
      kind: "system",
      title: "System Activity",
      participants: [],
      messages: [],
      unreadCount: 0,
    },
  };
}

export default function App() {
  const [userId] = useState(createUserId);
  const [input, setInput] = useState("Compare retrieval-augmented generation vs fine-tuning");
  const [loading, setLoading] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [currentTaskId, setCurrentTaskId] = useState<string | null>(null);

  const [agents, setAgents] = useState<Record<string, AgentNode>>({});
  const [agentTraces, setAgentTraces] = useState<Record<string, AgentTraceEntry[]>>({});
  const [threads, setThreads] = useState<Record<string, ThreadState>>(initialSystemThread);
  const [selectedThreadId, setSelectedThreadId] = useState<string | null>(SYSTEM_THREAD_ID);
  const [selectedAgentTaskId, setSelectedAgentTaskId] = useState<string | null>(null);
  const [finalDeliverable, setFinalDeliverable] = useState<FinalDeliverable | null>(
    null
  );

  const resetRunView = () => {
    setAgents({});
    setAgentTraces({});
    setThreads(initialSystemThread());
    setSelectedThreadId(SYSTEM_THREAD_ID);
    setSelectedAgentTaskId(null);
    setFinalDeliverable(null);
    setCancelling(false);
  };

  const { sendPrompt, cancelCurrentTask } = useChat({
    userId,
    input,
    currentTaskId,
    cancelling,
    setInput,
    setLoading,
    setCurrentTaskId,
    setCancelling,
    onBeforeEnqueue: resetRunView,
  });

  useWebSocket({
    userId,
    selectedThreadId,
    currentTaskId,
    setAgents,
    setAgentTraces,
    setThreads,
    setSelectedThreadId,
    setSelectedAgentTaskId,
    setLoading,
    setCurrentTaskId,
    setCancelling,
    setFinalDeliverable,
  });

  const selectedThread = selectedThreadId ? threads[selectedThreadId] ?? null : null;
  const highlightedTaskIds = useMemo(() => {
    if (!selectedThread || selectedThread.participants.length === 0) {
      return new Set<string>();
    }
    return new Set(selectedThread.participants);
  }, [selectedThread]);

  const selectThread = (threadId: string) => {
    setSelectedThreadId(threadId);
    setThreads((prev) => {
      const thread = prev[threadId];
      if (!thread) {
        return prev;
      }
      return {
        ...prev,
        [threadId]: { ...thread, unreadCount: 0 },
      };
    });
  };

  const runButtonLabel = loading ? "Running..." : "Run Demo";

  return (
    <div className="flex h-full flex-col bg-slate-100">
      <header className="border-b border-slate-200 bg-white px-4 py-3">
        <div className="mb-2 flex items-center justify-between gap-4">
          <div>
            <h1 className="text-lg font-semibold text-slate-900">Group Chat Agent Demo</h1>
            <p className="text-xs text-slate-500">
              Parent + subagents with group messaging, direct messaging, and wait.activity
            </p>
          </div>
          <div className="text-xs text-slate-500">session: {userId}</div>
        </div>

        <div className="flex items-center gap-2">
          <input
            type="text"
            value={input}
            onChange={(event) => setInput(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                event.preventDefault();
                void sendPrompt();
              }
            }}
            placeholder="Describe a topic for the team discussion..."
            className="flex-1 rounded-md border border-slate-300 bg-white px-3 py-2 text-sm outline-none focus:border-indigo-500"
            disabled={loading}
          />
          {!loading ? (
            <button
              type="button"
              onClick={() => void sendPrompt()}
              className="inline-flex items-center gap-1 rounded-md bg-indigo-600 px-3 py-2 text-sm font-medium text-white hover:bg-indigo-700"
            >
              <Send className="h-4 w-4" />
              {runButtonLabel}
            </button>
          ) : (
            <button
              type="button"
              onClick={() => void cancelCurrentTask()}
              disabled={cancelling}
              className="inline-flex items-center gap-1 rounded-md bg-red-600 px-3 py-2 text-sm font-medium text-white hover:bg-red-700 disabled:opacity-60"
            >
              {cancelling ? <Loader2 className="h-4 w-4 animate-spin" /> : <Square className="h-4 w-4" />}
              Cancel
            </button>
          )}
        </div>

        {currentTaskId && (
          <div className="mt-2 text-xs text-slate-500">
            parent task: <span className="font-mono">{currentTaskId}</span>
          </div>
        )}
      </header>

      <main className="flex min-h-0 flex-1 gap-3 p-3">
        <section className="w-[33%] min-w-[300px]">
          <div className="flex h-full min-h-0 flex-col">
            <OrgChart
              agents={agents}
              highlightedTaskIds={highlightedTaskIds}
              selectedAgentTaskId={selectedAgentTaskId}
              onSelectAgent={setSelectedAgentTaskId}
            />
            <AgentTracePanel
              selectedAgentTaskId={selectedAgentTaskId}
              agents={agents}
              traces={agentTraces}
            />
          </div>
        </section>
        <section className="w-[24%] min-w-[260px]">
          <ThreadSidebar
            threads={threads}
            selectedThreadId={selectedThreadId}
            onSelectThread={selectThread}
          />
        </section>
        <section className="min-w-0 flex flex-1 flex-col">
          <FinalDeliverablePanel deliverable={finalDeliverable} />
          <div className="min-h-0 flex-1">
            <ThreadView thread={selectedThread} agents={agents} />
          </div>
        </section>
      </main>
    </div>
  );
}
