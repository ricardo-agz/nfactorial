import type { AgentNode, ThreadState } from "../types";

interface ThreadViewProps {
  thread: ThreadState | null;
  agents: Record<string, AgentNode>;
}

function badgeClass(badge: string): string {
  if (badge === "group") return "bg-blue-100 text-blue-700";
  if (badge === "dm") return "bg-purple-100 text-purple-700";
  if (badge === "wait") return "bg-amber-100 text-amber-700";
  if (badge === "woken") return "bg-green-100 text-green-700";
  return "bg-slate-100 text-slate-700";
}

function deliverySummary(
  delivered: string[] | undefined,
  skipped: string[] | undefined,
  failed: string[] | undefined
): string | null {
  const deliveredCount = delivered?.length ?? 0;
  const skippedCount = skipped?.length ?? 0;
  const failedCount = failed?.length ?? 0;
  if (deliveredCount + skippedCount + failedCount === 0) {
    return null;
  }
  return `delivered ${deliveredCount}, skipped ${skippedCount}, failed ${failedCount}`;
}

export function ThreadView({ thread, agents }: ThreadViewProps) {
  if (!thread) {
    return (
      <div className="flex h-full items-center justify-center rounded-lg border border-slate-200 bg-white">
        <p className="text-sm text-slate-500">Select a thread to inspect messages.</p>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col rounded-lg border border-slate-200 bg-white">
      <div className="border-b border-slate-200 px-4 py-3">
        <div className="text-sm font-semibold text-slate-900">{thread.title}</div>
        <div className="text-xs text-slate-500">
          {thread.participants.length > 0
            ? `${thread.participants.length} participant${
                thread.participants.length === 1 ? "" : "s"
              }`
            : "System events"}
        </div>
      </div>

      <div className="flex-1 space-y-2 overflow-y-auto p-4">
        {thread.messages.length === 0 ? (
          <div className="text-sm text-slate-500">No messages yet.</div>
        ) : (
          thread.messages.map((message) => {
            const senderLabel =
              message.fromTaskId && agents[message.fromTaskId]
                ? agents[message.fromTaskId].label
                : message.fromLabel ?? "System";
            const deliveryText = deliverySummary(
              message.deliveredTaskIds,
              message.skippedTaskIds,
              message.failedTaskIds
            );

            return (
              <div key={message.id} className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                <div className="mb-1 flex items-center justify-between gap-3">
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-semibold text-slate-800">{senderLabel}</span>
                    <span
                      className={[
                        "rounded px-1.5 py-0.5 text-[10px] font-medium uppercase",
                        badgeClass(message.badge),
                      ].join(" ")}
                    >
                      {message.badge}
                    </span>
                  </div>
                  <span className="text-[11px] text-slate-500">
                    {new Date(message.timestamp).toLocaleTimeString()}
                  </span>
                </div>
                <p className="whitespace-pre-wrap text-sm text-slate-900">{message.content}</p>
                {deliveryText && (
                  <div className="mt-2 text-[11px] text-slate-500">{deliveryText}</div>
                )}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
