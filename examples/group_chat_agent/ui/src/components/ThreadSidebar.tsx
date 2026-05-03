import type { ThreadState } from "../types";

interface ThreadSidebarProps {
  threads: Record<string, ThreadState>;
  selectedThreadId: string | null;
  onSelectThread: (threadId: string) => void;
}

function sortThreads(threads: ThreadState[]): ThreadState[] {
  const rank = (kind: ThreadState["kind"]) => {
    if (kind === "group") return 0;
    if (kind === "dm") return 1;
    return 2;
  };
  return [...threads].sort((a, b) => {
    const kindDelta = rank(a.kind) - rank(b.kind);
    if (kindDelta !== 0) {
      return kindDelta;
    }
    return a.title.localeCompare(b.title);
  });
}

export function ThreadSidebar({
  threads,
  selectedThreadId,
  onSelectThread,
}: ThreadSidebarProps) {
  const orderedThreads = sortThreads(Object.values(threads));

  return (
    <div className="h-full rounded-lg border border-slate-200 bg-white">
      <div className="border-b border-slate-200 px-3 py-2 text-sm font-semibold text-slate-900">
        Threads
      </div>
      <div className="overflow-y-auto p-2">
        {orderedThreads.length === 0 ? (
          <div className="p-2 text-sm text-slate-500">No threads yet.</div>
        ) : (
          orderedThreads.map((thread) => {
            const selected = selectedThreadId === thread.id;
            return (
              <button
                key={thread.id}
                type="button"
                onClick={() => onSelectThread(thread.id)}
                className={[
                  "mb-1 w-full rounded-md border px-3 py-2 text-left text-sm transition",
                  selected
                    ? "border-indigo-500 bg-indigo-50 text-indigo-900"
                    : "border-transparent bg-slate-50 text-slate-700 hover:border-slate-200",
                ].join(" ")}
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="truncate font-medium">{thread.title}</span>
                  {thread.unreadCount > 0 && (
                    <span className="rounded bg-indigo-600 px-1.5 py-0.5 text-[11px] text-white">
                      {thread.unreadCount}
                    </span>
                  )}
                </div>
                <div className="mt-1 text-[11px] text-slate-500">
                  {thread.messages.length} message{thread.messages.length === 1 ? "" : "s"}
                </div>
              </button>
            );
          })
        )}
      </div>
    </div>
  );
}
