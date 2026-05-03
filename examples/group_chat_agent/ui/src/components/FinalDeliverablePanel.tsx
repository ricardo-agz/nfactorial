import type { FinalDeliverable } from "../types";

interface FinalDeliverablePanelProps {
  deliverable: FinalDeliverable | null;
}

function renderLine(line: string, index: number) {
  const trimmed = line.trim();
  if (!trimmed) {
    return <div key={`spacer-${index}`} className="h-2" />;
  }
  if (trimmed.startsWith("### ")) {
    return (
      <h4 key={`h4-${index}`} className="text-sm font-semibold text-slate-900">
        {trimmed.slice(4)}
      </h4>
    );
  }
  if (trimmed.startsWith("## ")) {
    return (
      <h3 key={`h3-${index}`} className="text-base font-semibold text-slate-900">
        {trimmed.slice(3)}
      </h3>
    );
  }
  if (trimmed.startsWith("# ")) {
    return (
      <h2 key={`h2-${index}`} className="text-lg font-semibold text-slate-900">
        {trimmed.slice(2)}
      </h2>
    );
  }
  if (trimmed.startsWith("- ")) {
    return (
      <div key={`li-${index}`} className="flex gap-2 text-sm text-slate-800">
        <span className="mt-[2px] text-slate-500">•</span>
        <span>{trimmed.slice(2)}</span>
      </div>
    );
  }
  return (
    <p key={`p-${index}`} className="text-sm leading-relaxed text-slate-800">
      {trimmed}
    </p>
  );
}

export function FinalDeliverablePanel({ deliverable }: FinalDeliverablePanelProps) {
  return (
    <div className="mb-3 rounded-lg border border-emerald-200 bg-emerald-50">
      <div className="border-b border-emerald-200 px-4 py-3">
        <div className="text-sm font-semibold text-emerald-900">Final Deliverable</div>
        <div className="text-xs text-emerald-700">
          {deliverable
            ? `Completed at ${new Date(deliverable.timestamp).toLocaleTimeString()}`
            : "Appears when parent run completes"}
        </div>
      </div>
      <div className="max-h-[240px] space-y-1 overflow-y-auto px-4 py-3">
        {deliverable ? (
          deliverable.content
            .split(/\r?\n/)
            .map((line, index) => renderLine(line, index))
        ) : (
          <p className="text-sm text-slate-500">
            Waiting for the parent coordinator to publish the collaborative deliverable.
          </p>
        )}
      </div>
    </div>
  );
}
