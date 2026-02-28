import type { AgentNode, AgentTraceEntry } from "../types";

interface AgentTracePanelProps {
  selectedAgentTaskId: string | null;
  agents: Record<string, AgentNode>;
  traces: Record<string, AgentTraceEntry[]>;
}

function badgeClass(kind: AgentTraceEntry["kind"]): string {
  if (kind === "tool_started") return "bg-blue-100 text-blue-700";
  if (kind === "tool_completed") return "bg-green-100 text-green-700";
  if (kind === "tool_failed") return "bg-red-100 text-red-700";
  if (kind === "wait_enter") return "bg-amber-100 text-amber-700";
  if (kind === "wait_wake") return "bg-emerald-100 text-emerald-700";
  if (kind === "message_sent") return "bg-violet-100 text-violet-700";
  if (kind === "message_received") return "bg-fuchsia-100 text-fuchsia-700";
  if (kind === "run_completed") return "bg-green-100 text-green-700";
  if (kind === "run_failed" || kind === "run_cancelled") {
    return "bg-red-100 text-red-700";
  }
  return "bg-slate-100 text-slate-700";
}

export function AgentTracePanel({
  selectedAgentTaskId,
  agents,
  traces,
}: AgentTracePanelProps) {
  if (!selectedAgentTaskId) {
    return (
      <div className="mt-3 rounded-lg border border-slate-200 bg-white p-4 text-sm text-slate-500">
        Select an agent node to view its individual trace.
      </div>
    );
  }

  const agent = agents[selectedAgentTaskId];
  const entries = traces[selectedAgentTaskId] ?? [];

  return (
    <div className="mt-3 flex min-h-0 flex-1 flex-col rounded-lg border border-slate-200 bg-white">
      <div className="border-b border-slate-200 px-3 py-2">
        <div className="text-sm font-semibold text-slate-900">
          {agent?.label ?? selectedAgentTaskId.slice(0, 8)} Trace
        </div>
        <div className="text-[11px] text-slate-500 font-mono">{selectedAgentTaskId}</div>
      </div>

      <div className="flex-1 space-y-2 overflow-y-auto p-3">
        {entries.length === 0 ? (
          <div className="text-sm text-slate-500">No events recorded yet for this agent.</div>
        ) : (
          entries.map((entry) => (
            <div key={entry.id} className="rounded-md border border-slate-200 bg-slate-50 p-2">
              <div className="mb-1 flex items-center justify-between gap-2">
                <span
                  className={[
                    "rounded px-1.5 py-0.5 text-[10px] font-medium uppercase",
                    badgeClass(entry.kind),
                  ].join(" ")}
                >
                  {entry.kind}
                </span>
                <span className="text-[11px] text-slate-500">
                  {new Date(entry.timestamp).toLocaleTimeString()}
                </span>
              </div>
              <div className="text-sm text-slate-800">{entry.text}</div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
