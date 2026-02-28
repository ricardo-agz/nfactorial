import {
  useCallback,
  useEffect,
  useRef,
  type Dispatch,
  type SetStateAction,
} from "react";

import { WS_BASE_URL } from "../constants";
import type {
  AgentEvent,
  AgentNode,
  AgentTraceEntry,
  AgentStatus,
  FinalDeliverable,
  ThreadMessage,
  ThreadState,
} from "../types";

const SYSTEM_THREAD_ID = "thread:system";

function toTaskId(raw: unknown): string | null {
  if (typeof raw !== "string") {
    return null;
  }
  const normalized = raw.trim();
  return normalized.length > 0 ? normalized : null;
}

function shortTaskId(taskId: string | null): string {
  return taskId ? taskId.slice(0, 8) : "system";
}

function safeJsonParse(raw: unknown): Record<string, any> {
  if (typeof raw !== "string") {
    return {};
  }
  try {
    const parsed = JSON.parse(raw);
    return typeof parsed === "object" && parsed !== null ? parsed : {};
  } catch {
    return {};
  }
}

function roleFromAgentName(
  agentName: string | undefined,
  taskId: string | null
): string {
  if (!agentName) {
    return shortTaskId(taskId);
  }
  if (agentName.includes("parent")) {
    return "parent";
  }
  if (agentName.includes("researcher")) {
    return "researcher";
  }
  if (agentName.includes("skeptic")) {
    return "skeptic";
  }
  if (agentName.includes("synthesizer")) {
    return "synthesizer";
  }
  return agentName;
}

function labelFromRole(role: string): string {
  if (role === "parent") {
    return "Parent Coordinator";
  }
  if (role === "researcher") {
    return "Researcher";
  }
  if (role === "skeptic") {
    return "Skeptic";
  }
  if (role === "synthesizer") {
    return "Synthesizer";
  }
  return role;
}

function resolveThreadStatus(eventType: string): AgentStatus | null {
  if (eventType === "task_activity_waiting") {
    return "waiting";
  }
  if (eventType === "run_started") {
    return "active";
  }
  if (eventType === "run_completed") {
    return "completed";
  }
  if (eventType === "run_failed") {
    return "failed";
  }
  if (eventType === "run_cancelled") {
    return "cancelled";
  }
  return null;
}

function makeDmThreadId(taskA: string, taskB: string): string {
  const [left, right] = [taskA, taskB].sort();
  return `thread:dm:${left}:${right}`;
}

function stripRosterTag(content: string): string {
  return content.replace(/<team_roster>[\s\S]*?<\/team_roster>/g, "").trim();
}

function truncateText(content: string, maxLen: number = 96): string {
  const normalized = content.replace(/\s+/g, " ").trim();
  if (normalized.length <= maxLen) {
    return normalized;
  }
  return `${normalized.slice(0, maxLen - 1)}...`;
}

function extractDmMessage(
  toolName: string,
  args: Record<string, any>,
  output: Record<string, any>
): string {
  const outputMessage =
    typeof output.message === "string" ? output.message.trim() : "";
  if (outputMessage) {
    return outputMessage;
  }
  if (toolName === "reply_to_parent_dm") {
    return typeof args.reply === "string" ? args.reply : "";
  }
  return typeof args.message === "string" ? args.message : "";
}

function extractFinalOutput(data: unknown): string | null {
  if (typeof data === "string") {
    const text = data.trim();
    return text.length > 0 ? text : null;
  }
  if (!data || typeof data !== "object") {
    return null;
  }
  const payload = data as Record<string, any>;
  const directFinal =
    typeof payload.final_output === "string" ? payload.final_output.trim() : "";
  if (directFinal) {
    return directFinal;
  }

  const output = payload.output;
  if (typeof output === "string") {
    const text = output.trim();
    return text.length > 0 ? text : null;
  }
  if (output && typeof output === "object") {
    const nested = output as Record<string, any>;
    if (typeof nested.final_output === "string") {
      const text = nested.final_output.trim();
      return text.length > 0 ? text : null;
    }
  }
  return null;
}

interface UseWebSocketProps {
  userId: string;
  selectedThreadId: string | null;
  currentTaskId: string | null;
  setAgents: Dispatch<SetStateAction<Record<string, AgentNode>>>;
  setAgentTraces: Dispatch<SetStateAction<Record<string, AgentTraceEntry[]>>>;
  setThreads: Dispatch<SetStateAction<Record<string, ThreadState>>>;
  setSelectedThreadId: (threadId: string) => void;
  setSelectedAgentTaskId: Dispatch<SetStateAction<string | null>>;
  setLoading: (loading: boolean) => void;
  setCurrentTaskId: (taskId: string | null) => void;
  setCancelling: (cancelling: boolean) => void;
  setFinalDeliverable: Dispatch<SetStateAction<FinalDeliverable | null>>;
}

export function useWebSocket({
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
}: UseWebSocketProps) {
  const wsRef = useRef<WebSocket | null>(null);
  const selectedThreadIdRef = useRef<string | null>(selectedThreadId);
  const currentTaskIdRef = useRef<string | null>(currentTaskId);
  const waitingTasksRef = useRef<Set<string>>(new Set());
  const agentsRef = useRef<Record<string, AgentNode>>({});

  const appendTrace = useCallback(
    (
      taskId: string,
      timestamp: string,
      kind: AgentTraceEntry["kind"],
      text: string
    ) => {
      setAgentTraces((prev) => {
        const existing = prev[taskId] ?? [];
        const entry: AgentTraceEntry = {
          id: `${taskId}:${kind}:${timestamp}:${existing.length}`,
          taskId,
          timestamp,
          kind,
          text,
        };
        return {
          ...prev,
          [taskId]: [...existing, entry],
        };
      });
    },
    [setAgentTraces]
  );

  useEffect(() => {
    selectedThreadIdRef.current = selectedThreadId;
  }, [selectedThreadId]);

  useEffect(() => {
    currentTaskIdRef.current = currentTaskId;
  }, [currentTaskId]);

  const upsertAgent = useCallback(
    (taskId: string, event: AgentEvent, overrides?: Partial<AgentNode>) => {
      setAgents((prev) => {
        const existing = prev[taskId];
        const role = overrides?.role ?? roleFromAgentName(event.agent_name, taskId);
        const next: AgentNode = {
          taskId,
          parentTaskId: existing?.parentTaskId ?? null,
          agentName: overrides?.agentName ?? existing?.agentName ?? event.agent_name ?? role,
          role,
          label: overrides?.label ?? existing?.label ?? labelFromRole(role),
          status: overrides?.status ?? existing?.status ?? "queued",
          lastEventAt: event.timestamp,
        };
        const merged = { ...existing, ...next, ...overrides };
        const updated = { ...prev, [taskId]: merged };
        agentsRef.current = updated;
        return updated;
      });
    },
    [setAgents]
  );

  const appendThreadMessage = useCallback(
    (
      threadId: string,
      initialThread: Omit<ThreadState, "messages" | "unreadCount">,
      message: ThreadMessage
    ) => {
      setThreads((prev) => {
        const existing = prev[threadId];
        const base: ThreadState = existing ?? {
          ...initialThread,
          messages: [],
          unreadCount: 0,
        };
        const unreadDelta = selectedThreadIdRef.current === threadId ? 0 : 1;
        return {
          ...prev,
          [threadId]: {
            ...base,
            participants:
              base.participants.length > 0
                ? base.participants
                : initialThread.participants,
            messages: [...base.messages, message],
            unreadCount: base.unreadCount + unreadDelta,
          },
        };
      });
    },
    [setThreads]
  );

  const ensureSystemThread = useCallback(() => {
    setThreads((prev) => {
      if (prev[SYSTEM_THREAD_ID]) {
        return prev;
      }
      return {
        ...prev,
        [SYSTEM_THREAD_ID]: {
          id: SYSTEM_THREAD_ID,
          kind: "system",
          title: "System Activity",
          participants: [],
          messages: [],
          unreadCount: 0,
        },
      };
    });
  }, [setThreads]);

  const addSystemMessage = useCallback(
    (
      event: AgentEvent,
      eventTaskId: string | null,
      content: string,
      badge: ThreadMessage["badge"]
    ) => {
      appendThreadMessage(
        SYSTEM_THREAD_ID,
        {
          id: SYSTEM_THREAD_ID,
          kind: "system",
          title: "System Activity",
          participants: [],
        },
        {
          id: `${SYSTEM_THREAD_ID}:${shortTaskId(eventTaskId)}:${event.timestamp}:${badge}`,
          threadId: SYSTEM_THREAD_ID,
          timestamp: event.timestamp,
          fromTaskId: eventTaskId ?? undefined,
          fromLabel: eventTaskId
            ? agentsRef.current[eventTaskId]?.label
            : "System",
          content,
          badge,
        }
      );
    },
    [appendThreadMessage]
  );

  const handleWsMessage = useCallback(
    (evt: MessageEvent) => {
      const event = JSON.parse(evt.data) as AgentEvent;
      const eventTaskId = toTaskId(event.task_id);
      ensureSystemThread();
      if (eventTaskId) {
        upsertAgent(eventTaskId, event);
      }

      if (eventTaskId && event.event_type === "run_started") {
        appendTrace(eventTaskId, event.timestamp, "run_started", "Run started.");
      }
      if (eventTaskId && event.event_type === "run_completed") {
        appendTrace(eventTaskId, event.timestamp, "run_completed", "Run completed.");
      }
      if (eventTaskId && event.event_type === "run_failed") {
        appendTrace(
          eventTaskId,
          event.timestamp,
          "run_failed",
          `Run failed${event.error ? `: ${event.error}` : "."}`
        );
      }
      if (eventTaskId && event.event_type === "run_cancelled") {
        appendTrace(eventTaskId, event.timestamp, "run_cancelled", "Run cancelled.");
      }

      const status = resolveThreadStatus(event.event_type);
      if (status && eventTaskId) {
        upsertAgent(eventTaskId, event, { status });
      }

      const isParentEvent =
        event.agent_name === "parent_coordinator" ||
        (eventTaskId ? agentsRef.current[eventTaskId]?.role === "parent" : false);
      if (
        isParentEvent &&
        (event.event_type === "agent_output" || event.event_type === "run_completed")
      ) {
        const finalText = extractFinalOutput(event.data);
        if (finalText && eventTaskId) {
          setFinalDeliverable({
            taskId: eventTaskId,
            timestamp: event.timestamp,
            content: finalText,
          });
          addSystemMessage(
            event,
            eventTaskId,
            "Parent published the final collaborative deliverable.",
            "system"
          );
          appendTrace(
            eventTaskId,
            event.timestamp,
            "system",
            "Published final collaborative deliverable."
          );
        }
      }

      if (event.event_type === "task_activity_waiting" && eventTaskId) {
        waitingTasksRef.current.add(eventTaskId);
        const label =
          agentsRef.current[eventTaskId]?.label ?? shortTaskId(eventTaskId);
        appendTrace(
          eventTaskId,
          event.timestamp,
          "wait_enter",
          "Entered wait.activity."
        );
        addSystemMessage(
          event,
          eventTaskId,
          `${label} entered wait.activity`,
          "wait"
        );
      }

      if (
        event.event_type === "run_started" &&
        eventTaskId &&
        waitingTasksRef.current.has(eventTaskId)
      ) {
        waitingTasksRef.current.delete(eventTaskId);
        const label =
          agentsRef.current[eventTaskId]?.label ?? shortTaskId(eventTaskId);
        appendTrace(
          eventTaskId,
          event.timestamp,
          "wait_wake",
          "Woke from wait.activity."
        );
        addSystemMessage(
          event,
          eventTaskId,
          `${label} woke from wait.activity`,
          "woken"
        );
      }

      if (event.event_type === "progress_update_tool_action_completed") {
        const completion = event.data?.result;
        const toolCall = completion?.tool_call;
        const toolName = toolCall?.function?.name as string | undefined;
        const args = safeJsonParse(toolCall?.function?.arguments);
        const output = completion?.client_output ?? {};

        if (eventTaskId && toolName) {
          appendTrace(
            eventTaskId,
            event.timestamp,
            "tool_completed",
            `Completed tool: ${toolName}.`
          );
        }

        if (toolName === "spawn_team") {
          const roster = output.roster as Record<string, string> | undefined;
          const groupName = (output.group_name as string | undefined) ?? "team_room";
          if (roster) {
            for (const [role, taskId] of Object.entries(roster)) {
              upsertAgent(taskId, event, {
                role,
                label: labelFromRole(role),
                parentTaskId: role === "parent" ? null : eventTaskId,
              });
            }
          }
          const participants = roster ? Array.from(new Set(Object.values(roster))) : [];
          const groupThreadId = `thread:group:${groupName}`;
          setThreads((prev) => ({
            ...prev,
            [groupThreadId]: prev[groupThreadId] ?? {
              id: groupThreadId,
              kind: "group",
              title: `#${groupName}`,
              participants,
              messages: [],
              unreadCount: 0,
            },
          }));
          if (!selectedThreadIdRef.current) {
            setSelectedThreadId(groupThreadId);
          }
          setSelectedAgentTaskId(
            (prev) => prev ?? roster?.parent ?? eventTaskId ?? null
          );
          addSystemMessage(
            event,
            eventTaskId,
            "Parent spawned team and created group channel.",
            "system"
          );
        }

        if (toolName === "post_group") {
          const groupName = (output.group_name as string | undefined) ?? "team_room";
          const threadId = `thread:group:${groupName}`;
          const content = stripRosterTag((args.message as string | undefined) ?? "");
          const label =
            (eventTaskId && agentsRef.current[eventTaskId]?.label) ??
            shortTaskId(eventTaskId);
          appendThreadMessage(
            threadId,
            {
              id: threadId,
              kind: "group",
              title: `#${groupName}`,
              participants: [],
            },
            {
              id: `${threadId}:${shortTaskId(eventTaskId)}:${event.timestamp}`,
              threadId,
              timestamp: event.timestamp,
              fromTaskId: eventTaskId ?? undefined,
              fromLabel: label,
              content,
              badge: "group",
              deliveredTaskIds: output.delivered_task_ids,
              skippedTaskIds: output.skipped_inactive_task_ids,
              failedTaskIds: output.failed_task_ids,
            }
          );
          if (eventTaskId) {
            const deliveredTaskIds = Array.isArray(output.delivered_task_ids)
              ? (output.delivered_task_ids as string[])
              : [];
            appendTrace(
              eventTaskId,
              event.timestamp,
              "message_sent",
              `Sent group message to #${groupName}: "${truncateText(content)}" (${deliveredTaskIds.length} delivered).`
            );
            for (const recipientTaskId of deliveredTaskIds) {
              const senderLabel =
                agentsRef.current[eventTaskId]?.label ?? shortTaskId(eventTaskId);
              appendTrace(
                recipientTaskId,
                event.timestamp,
                "message_received",
                `Received group message from ${senderLabel}: "${truncateText(content)}".`
              );
            }
          }
        }

        if (
          toolName === "post_dm" ||
          toolName === "reply_to_parent_dm" ||
          toolName === "parent_followup_decision"
        ) {
          const messageText = extractDmMessage(toolName, args, output);
          const resolvedTarget =
            (output.resolved_to_task_id as string | undefined) ??
            (args.to_task_id as string | undefined) ??
            "";
          if (resolvedTarget && eventTaskId) {
            const threadId = makeDmThreadId(eventTaskId, resolvedTarget);
            const senderLabel =
              agentsRef.current[eventTaskId]?.label ?? shortTaskId(eventTaskId);
            const receiverLabel =
              agentsRef.current[resolvedTarget]?.label ?? resolvedTarget.slice(0, 8);
            appendThreadMessage(
              threadId,
              {
                id: threadId,
                kind: "dm",
                title: `DM ${senderLabel} <-> ${receiverLabel}`,
                participants: [eventTaskId, resolvedTarget],
              },
              {
                id: `${threadId}:${eventTaskId}:${event.timestamp}`,
                threadId,
                timestamp: event.timestamp,
                fromTaskId: eventTaskId,
                fromLabel: senderLabel,
                content: messageText,
                badge: "dm",
                deliveredTaskIds: output.delivered_task_ids,
                skippedTaskIds: output.skipped_inactive_task_ids,
                failedTaskIds: output.failed_task_ids,
              }
            );
            appendTrace(
              eventTaskId,
              event.timestamp,
              "message_sent",
              `Sent direct message to ${receiverLabel}: "${truncateText(
                messageText
              )}".`
            );
            appendTrace(
              resolvedTarget,
              event.timestamp,
              "message_received",
              `Received direct message from ${senderLabel}: "${truncateText(
                messageText
              )}".`
            );
          }
          if (
            toolName === "parent_followup_decision" &&
            output.action === "skipped_dm" &&
            eventTaskId
          ) {
            appendTrace(
              eventTaskId,
              event.timestamp,
              "system",
              "Skipped optional parent DM and moved to wait for child completion."
            );
          }
        }
      }

      if (event.event_type === "progress_update_tool_action_started" && eventTaskId) {
        const toolCall = event.data?.args?.[0];
        const toolName = toolCall?.function?.name as string | undefined;
        if (toolName) {
          appendTrace(
            eventTaskId,
            event.timestamp,
            "tool_started",
            `Started tool: ${toolName}.`
          );
        }
      }

      if (event.event_type === "progress_update_tool_action_failed" && eventTaskId) {
        const toolCall = event.data?.args?.[0];
        const toolName = toolCall?.function?.name as string | undefined;
        appendTrace(
          eventTaskId,
          event.timestamp,
          "tool_failed",
          `Tool failed${toolName ? ` (${toolName})` : ""}${
            event.error ? `: ${event.error}` : "."
          }`
        );
      }

      if (event.event_type === "messaging_group_message_sent") {
        const groupName = event.data?.group_name ?? "team_room";
        const delivered = (event.data?.delivered_task_ids ?? []).length;
        addSystemMessage(
          event,
          eventTaskId,
          `Group message sent to #${groupName} (${delivered} delivered).`,
          "system"
        );
      }

      if (event.event_type === "messaging_direct_message_sent") {
        const delivered = (event.data?.delivered_task_ids ?? []).length;
        addSystemMessage(
          event,
          eventTaskId,
          `Direct message sent (${delivered} delivered).`,
          "system"
        );
      }

      if (
        event.agent_name === "parent_coordinator" &&
        currentTaskIdRef.current &&
        eventTaskId === currentTaskIdRef.current &&
        ["run_completed", "run_failed", "run_cancelled"].includes(event.event_type)
      ) {
        setLoading(false);
        setCurrentTaskId(null);
        setCancelling(false);
      }
    },
    [
      appendTrace,
      addSystemMessage,
      appendThreadMessage,
      ensureSystemThread,
      setSelectedAgentTaskId,
      setCancelling,
      setCurrentTaskId,
      setFinalDeliverable,
      setLoading,
      setSelectedThreadId,
      setThreads,
      upsertAgent,
    ]
  );

  useEffect(() => {
    const ws = new WebSocket(`${WS_BASE_URL}/${userId}`);
    ws.onmessage = handleWsMessage;
    wsRef.current = ws;

    return () => {
      ws.close();
    };
  }, [handleWsMessage, userId]);

  return wsRef;
}
