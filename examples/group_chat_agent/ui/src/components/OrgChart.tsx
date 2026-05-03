import type { AgentNode } from "../types";

interface OrgChartProps {
  agents: Record<string, AgentNode>;
  highlightedTaskIds: Set<string>;
  selectedAgentTaskId: string | null;
  onSelectAgent: (taskId: string) => void;
}

function statusClass(status: AgentNode["status"]): string {
  if (status === "active") return "bg-blue-100 text-blue-700";
  if (status === "waiting") return "bg-amber-100 text-amber-700";
  if (status === "completed") return "bg-green-100 text-green-700";
  if (status === "failed" || status === "cancelled") return "bg-red-100 text-red-700";
  return "bg-slate-100 text-slate-700";
}

function NodeCard({
  node,
  highlighted,
  selected,
  onClick,
}: {
  node: AgentNode;
  highlighted: boolean;
  selected: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        "w-48 rounded-lg border bg-white p-3 text-left shadow-sm transition",
        selected
          ? "border-indigo-600 ring-2 ring-indigo-200"
          : highlighted
            ? "border-indigo-500 shadow-md"
            : "border-slate-200",
      ].join(" ")}
    >
      <div className="mb-1 text-xs text-slate-500">{node.agentName}</div>
      <div className="text-sm font-semibold text-slate-900">{node.label}</div>
      <div className="mt-2 flex items-center gap-2">
        <span
          className={[
            "inline-block rounded px-2 py-0.5 text-[11px] font-medium",
            statusClass(node.status),
          ].join(" ")}
        >
          {node.status}
        </span>
        <span className="text-[11px] text-slate-500">{node.taskId.slice(0, 8)}</span>
      </div>
    </button>
  );
}

export function OrgChart({
  agents,
  highlightedTaskIds,
  selectedAgentTaskId,
  onSelectAgent,
}: OrgChartProps) {
  const allNodes = Object.values(agents);
  const parent =
    allNodes.find((node) => node.role === "parent") ??
    allNodes.find((node) => node.parentTaskId === null);

  const children = allNodes
    .filter((node) => parent && node.taskId !== parent.taskId)
    .sort((a, b) => a.label.localeCompare(b.label));

  if (!parent) {
    return (
      <div className="rounded-lg border border-slate-200 bg-white p-4 text-sm text-slate-500">
        Start a run to see the hierarchy graph.
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <div className="mb-3 text-sm font-semibold text-slate-900">Hierarchy</div>

      <div className="flex flex-col items-center">
        <NodeCard
          node={parent}
          highlighted={highlightedTaskIds.size === 0 || highlightedTaskIds.has(parent.taskId)}
          selected={selectedAgentTaskId === parent.taskId}
          onClick={() => onSelectAgent(parent.taskId)}
        />

        {children.length > 0 && (
          <>
            <div className="h-6 w-px bg-slate-300" />
            <div className="relative w-full pt-2">
              <div className="absolute left-0 right-0 top-0 mx-12 h-px bg-slate-300" />
              <div className="flex flex-wrap justify-center gap-4 pt-3">
                {children.map((child) => (
                  <div key={child.taskId} className="flex flex-col items-center">
                    <div className="h-3 w-px bg-slate-300" />
                    <NodeCard
                      node={child}
                      highlighted={
                        highlightedTaskIds.size === 0 ||
                        highlightedTaskIds.has(child.taskId)
                      }
                      selected={selectedAgentTaskId === child.taskId}
                      onClick={() => onSelectAgent(child.taskId)}
                    />
                  </div>
                ))}
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
