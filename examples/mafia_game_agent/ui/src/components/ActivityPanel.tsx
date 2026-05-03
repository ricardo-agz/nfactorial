import type { ActivityEntry, ActivityKind } from "../types";
import { formatTimestamp } from "../utils";

interface ActivityPanelProps {
  activity: ActivityEntry[];
}

const KIND_STYLE: Record<ActivityKind, { dot: string; text: string }> = {
  info: { dot: "bg-neutral-500", text: "text-neutral-400" },
  tool_started: { dot: "bg-blue-400", text: "text-blue-300" },
  tool_completed: { dot: "bg-emerald-400", text: "text-emerald-300" },
  tool_failed: { dot: "bg-rose-400", text: "text-rose-300" },
  wait: { dot: "bg-amber-400", text: "text-amber-300" },
  resume: { dot: "bg-violet-400", text: "text-violet-300" },
};

export function ActivityPanel({ activity }: ActivityPanelProps) {
  return (
    <div className="h-52 shrink-0 overflow-y-auto border-t border-neutral-800 bg-neutral-950 px-5 py-3">
      <h3 className="mb-2 text-[10px] font-semibold uppercase tracking-widest text-neutral-500">
        Runtime Activity
      </h3>
      {activity.length === 0 ? (
        <p className="text-xs text-neutral-600">No activity yet.</p>
      ) : (
        <div className="space-y-1">
          {activity
            .slice(-50)
            .reverse()
            .map((entry) => {
              const style = KIND_STYLE[entry.kind];
              return (
                <div
                  key={entry.id}
                  className="flex items-start gap-2 py-1"
                >
                  <span
                    className={`mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full ${style.dot}`}
                  />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2 text-[10px] text-neutral-600">
                      <span>
                        {formatTimestamp(entry.timestamp)}
                      </span>
                      {entry.actorLabel && (
                        <span className="text-neutral-500">
                          {entry.actorLabel}
                        </span>
                      )}
                      {entry.toolName && (
                        <code className="text-neutral-500">
                          {entry.toolName}
                        </code>
                      )}
                    </div>
                    <div className={`text-xs ${style.text}`}>
                      {entry.text}
                    </div>
                    {entry.detail && (
                      <div className="text-[10px] text-neutral-600">
                        {entry.detail}
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
        </div>
      )}
    </div>
  );
}
