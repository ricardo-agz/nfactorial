import {
  AlertTriangle,
  CheckCircle2,
  Clock3,
  Eye,
  EyeOff,
  Hammer,
  Loader2,
  MessageSquare,
  MoonStar,
  Play,
  Settings2,
  Send,
  Shield,
  Square,
  Vote,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { API_BASE_URL, SSE_BASE_URL } from "./constants";

type Channel = "town" | "wolf" | "system";
type HumanRolePreference = "random" | "werewolf" | "villager";
type AgentStatus = "queued" | "active" | "waiting" | "completed" | "failed" | "cancelled";
type ActivityKind =
  | "info"
  | "tool_started"
  | "tool_completed"
  | "tool_failed"
  | "wait"
  | "resume";

interface AgentEvent {
  event_type: string;
  task_id?: string;
  owner_id?: string;
  agent_name?: string;
  timestamp: string;
  data?: any;
  error?: string;
}

interface UiMessage {
  id: string;
  channel: Channel;
  timestamp: string;
  fromLabel: string;
  content: string;
  badge: "system" | "chat" | "action";
}

interface PlayerStateView {
  player_id: string;
  display_name: string;
  is_human: boolean;
  task_id?: string | null;
  alive: boolean;
  role?: string | null;
}

interface GameStateSnapshot {
  phase: string;
  round_no: number;
  phase_deadline_ts?: number | null;
  winner?: string | null;
  winner_reason?: string | null;
  alive_total: number;
  alive_villagers: number;
  alive_werewolves: number;
  players_public: PlayerStateView[];
  players_omniscient: PlayerStateView[];
  human_player_id?: string | null;
  human_private_role?: string | null;
  elimination_log: Array<Record<string, any>>;
}

interface ActivityEntry {
  id: string;
  timestamp: string;
  text: string;
  kind: ActivityKind;
  actorLabel?: string;
  toolName?: string;
  detail?: string;
}

const CHANNEL_LABEL: Record<Channel, string> = {
  town: "Town Square",
  wolf: "Wolf Den",
  system: "System",
};

function createUserId(): string {
  return `user_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function shortTaskId(taskId: string | null | undefined): string {
  if (!taskId) {
    return "unknown";
  }
  return taskId.slice(0, 8);
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
  if (typeof payload.final_output === "string" && payload.final_output.trim()) {
    return payload.final_output.trim();
  }
  if (typeof payload.output === "string" && payload.output.trim()) {
    return payload.output.trim();
  }
  if (
    payload.output &&
    typeof payload.output === "object" &&
    typeof payload.output.final_output === "string" &&
    payload.output.final_output.trim()
  ) {
    return payload.output.final_output.trim();
  }
  return null;
}

function normalizeChannel(value: unknown): Channel | null {
  if (value === "town" || value === "wolf" || value === "system") {
    return value;
  }
  return null;
}

function parseToolArgs(rawArguments: unknown): Record<string, unknown> {
  if (rawArguments && typeof rawArguments === "object" && !Array.isArray(rawArguments)) {
    return rawArguments as Record<string, unknown>;
  }
  if (typeof rawArguments === "string") {
    try {
      const parsed = JSON.parse(rawArguments);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed as Record<string, unknown>;
      }
    } catch {
      return {};
    }
  }
  return {};
}

function summarizeToolAction(
  toolName: string,
  toolArgs: Record<string, unknown>,
): string | undefined {
  if (toolName === "send_public_statement") {
    const message = toolArgs.message;
    if (typeof message === "string" && message.trim()) {
      return `drafting: "${message.trim().slice(0, 80)}"`;
    }
    return "drafting a town statement";
  }
  if (toolName === "submit_day_vote") {
    const target = toolArgs.target_player_id;
    if (typeof target === "string" && target.trim()) {
      return `targeting ${target.trim()}`;
    }
    return "choosing day vote target";
  }
  if (toolName === "submit_night_action") {
    const target = toolArgs.target_player_id;
    if (typeof target === "string" && target.trim()) {
      return `targeting ${target.trim()}`;
    }
    return "choosing night target";
  }
  if (toolName.startsWith("wait_for_")) {
    return "waiting for phase signal";
  }
  return undefined;
}

function formatPhaseLabel(phase: string | null | undefined): string {
  if (!phase) {
    return "Awaiting Start";
  }
  return phase
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatRoleLabel(role: string | null | undefined): string {
  if (!role) {
    return "Unknown";
  }
  return role.charAt(0).toUpperCase() + role.slice(1);
}

function formatTimestamp(timestamp: string): string {
  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) {
    return timestamp;
  }
  return parsed.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatCountdown(totalSeconds: number): string {
  const safeSeconds = Math.max(0, totalSeconds);
  const minutes = Math.floor(safeSeconds / 60)
    .toString()
    .padStart(2, "0");
  const seconds = Math.floor(safeSeconds % 60)
    .toString()
    .padStart(2, "0");
  return `${minutes}:${seconds}`;
}

export default function App() {
  const [userId] = useState(createUserId);

  const [gameName, setGameName] = useState("Mafia in nfactorial");
  const [includeHuman, setIncludeHuman] = useState(true);
  const [humanName, setHumanName] = useState("You");
  const [humanRolePreference, setHumanRolePreference] =
    useState<HumanRolePreference>("random");
  const [aiPlayerCount, setAiPlayerCount] = useState(5);
  const [dayDiscussionSeconds, setDayDiscussionSeconds] = useState(25);
  const [dayVoteSeconds, setDayVoteSeconds] = useState(35);
  const [nightSeconds, setNightSeconds] = useState(25);

  const [showOmniscient, setShowOmniscient] = useState(false);
  const [showActivity, setShowActivity] = useState(false);

  const [taskId, setTaskId] = useState<string | null>(null);
  const [humanPlayerId, setHumanPlayerId] = useState<string | null>(null);
  const [setupModalOpen, setSetupModalOpen] = useState(true);
  const [starting, setStarting] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [errorText, setErrorText] = useState<string | null>(null);

  const [gameState, setGameState] = useState<GameStateSnapshot | null>(null);
  const [finalReport, setFinalReport] = useState<string | null>(null);
  const [agentStatus, setAgentStatus] = useState<Record<string, AgentStatus>>({});

  const [threads, setThreads] = useState<Record<Channel, UiMessage[]>>({
    town: [],
    wolf: [],
    system: [],
  });
  const [selectedChannel, setSelectedChannel] = useState<Channel>("town");
  const [chatInput, setChatInput] = useState("");
  const [voteTarget, setVoteTarget] = useState("");
  const [nightTarget, setNightTarget] = useState("");
  const [activity, setActivity] = useState<ActivityEntry[]>([]);
  const [votedRoundByPlayerId, setVotedRoundByPlayerId] = useState<Record<string, number>>({});
  const [submittingVote, setSubmittingVote] = useState(false);

  const taskIdRef = useRef<string | null>(taskId);
  const gameStateRef = useRef<GameStateSnapshot | null>(gameState);
  const [nowEpochMs, setNowEpochMs] = useState(() => Date.now());

  const humanIsAlive = useMemo(() => {
    if (!includeHuman || !gameState || !humanPlayerId) {
      return false;
    }
    const humanPlayer =
      gameState.players_omniscient.find(
        (player) => player.player_id === humanPlayerId
      ) ??
      gameState.players_public.find((player) => player.player_id === humanPlayerId);
    return humanPlayer?.alive ?? false;
  }, [gameState, humanPlayerId, includeHuman]);

  useEffect(() => {
    taskIdRef.current = taskId;
  }, [taskId]);

  useEffect(() => {
    gameStateRef.current = gameState;
  }, [gameState]);

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      setNowEpochMs(Date.now());
    }, 1000);
    return () => {
      window.clearInterval(intervalId);
    };
  }, []);

  const resetView = useCallback(() => {
    setGameState(null);
    setFinalReport(null);
    setThreads({ town: [], wolf: [], system: [] });
    setActivity([]);
    setAgentStatus({});
    setVotedRoundByPlayerId({});
    setSubmittingVote(false);
    setSelectedChannel("town");
    setVoteTarget("");
    setNightTarget("");
    setErrorText(null);
    setCancelling(false);
  }, []);

  const pushActivity = useCallback(
    (
      text: string,
      timestamp?: string,
      meta?: {
        kind?: ActivityKind;
        actorLabel?: string;
        toolName?: string;
        detail?: string;
      }
    ) => {
    setActivity((prev) => {
      const next: ActivityEntry = {
        id: `${timestamp ?? new Date().toISOString()}:${prev.length}`,
        timestamp: timestamp ?? new Date().toISOString(),
        text,
        kind: meta?.kind ?? "info",
        actorLabel: meta?.actorLabel,
        toolName: meta?.toolName,
        detail: meta?.detail,
      };
      const merged = [...prev, next];
      if (merged.length > 500) {
        return merged.slice(merged.length - 500);
      }
      return merged;
    });
    },
    []
  );

  const appendThreadMessage = useCallback(
    (channel: Channel, fromLabel: string, content: string, badge: UiMessage["badge"], timestamp?: string) => {
      const normalized = content.trim();
      if (!normalized) {
        return;
      }
      setThreads((prev) => ({
        ...prev,
        [channel]: [
          ...prev[channel],
          {
            id: `${channel}:${timestamp ?? new Date().toISOString()}:${prev[channel].length}`,
            channel,
            timestamp: timestamp ?? new Date().toISOString(),
            fromLabel,
            content: normalized,
            badge,
          },
        ],
      }));
    },
    []
  );

  const resolveActorLabel = useCallback((event: AgentEvent): string => {
    const eventTaskId = typeof event.task_id === "string" ? event.task_id : null;
    const state = gameStateRef.current;
    if (eventTaskId && state) {
      for (const player of state.players_omniscient) {
        if (player.task_id === eventTaskId) {
          return player.display_name;
        }
      }
    }
    if (event.agent_name === "mafia_game_master") {
      return "Game Master";
    }
    if (eventTaskId) {
      return shortTaskId(eventTaskId);
    }
    return event.agent_name ?? "system";
  }, []);

  const setTaskStatus = useCallback((eventTaskId: string | null, status: AgentStatus) => {
    if (!eventTaskId) {
      return;
    }
    setAgentStatus((prev) => ({
      ...prev,
      [eventTaskId]: status,
    }));
  }, []);

  const handleEvent = useCallback(
    (event: AgentEvent) => {
      const eventTaskId = typeof event.task_id === "string" ? event.task_id : null;
      const actorLabel = resolveActorLabel(event);

      if (event.event_type === "run_started") {
        setTaskStatus(eventTaskId, "active");
      } else if (event.event_type === "run_completed") {
        setTaskStatus(eventTaskId, "completed");
      } else if (event.event_type === "run_failed") {
        setTaskStatus(eventTaskId, "failed");
      } else if (event.event_type === "run_cancelled") {
        setTaskStatus(eventTaskId, "cancelled");
      } else if (
        event.event_type === "task_activity_waiting" ||
        event.event_type === "task_signal_waiting"
      ) {
        setTaskStatus(eventTaskId, "waiting");
      } else if (event.event_type === "task_signal_wait_satisfied") {
        setTaskStatus(eventTaskId, "active");
      }

      if (event.event_type === "progress_update_tool_action_started") {
        const toolCall = event.data?.args?.[0];
        const toolName = toolCall?.function?.name as string | undefined;
        if (toolName) {
          const toolArgs = parseToolArgs(toolCall?.function?.arguments);
          pushActivity(
            `${actorLabel} started \`${toolName}\`.`,
            event.timestamp,
            {
              kind: "tool_started",
              actorLabel,
              toolName,
              detail: summarizeToolAction(toolName, toolArgs),
            }
          );
        }
      }

      if (event.event_type === "progress_update_tool_action_completed") {
        const completion = event.data?.result;
        const toolCall = completion?.tool_call;
        const toolName = toolCall?.function?.name as string | undefined;
        const output = (completion?.client_output ?? {}) as Record<string, any>;
        if (toolName) {
          const toolArgs = parseToolArgs(toolCall?.function?.arguments);
          pushActivity(
            `${actorLabel} completed \`${toolName}\`.`,
            event.timestamp,
            {
              kind: "tool_completed",
              actorLabel,
              toolName,
              detail: summarizeToolAction(toolName, toolArgs),
            }
          );
        }

        const maybeState = output?.game_state;
        if (maybeState && typeof maybeState === "object") {
          setGameState(maybeState as GameStateSnapshot);
        }

        if (toolName === "setup_game") {
          const fromStateHumanId =
            (maybeState?.human_player_id as string | undefined) ?? null;
          setHumanPlayerId((prev) => prev ?? fromStateHumanId);
        }

        if (toolName === "submit_day_vote" && eventTaskId) {
          const currentRound = gameStateRef.current?.round_no;
          const actorPlayerId = gameStateRef.current?.players_omniscient.find(
            (player) => player.task_id === eventTaskId
          )?.player_id;
          if (typeof currentRound === "number" && actorPlayerId) {
            setVotedRoundByPlayerId((prev) => ({
              ...prev,
              [actorPlayerId]: currentRound,
            }));
          }
        }

        const outputChannel = normalizeChannel(output?.channel);
        if (outputChannel && typeof output?.message === "string" && output.message.trim()) {
          appendThreadMessage(
            outputChannel,
            actorLabel,
            output.message,
            outputChannel === "system" ? "system" : "chat",
            event.timestamp
          );
        }

        if (typeof output?.summary === "string" && output.summary.trim()) {
          pushActivity(output.summary.trim(), event.timestamp, {
            kind: "info",
            actorLabel,
            toolName,
          });
        }
      }

      if (event.event_type === "progress_update_tool_action_failed") {
        const toolCall = event.data?.args?.[0];
        const toolName = toolCall?.function?.name as string | undefined;
        pushActivity(
          toolName
            ? `${actorLabel} failed tool \`${toolName}\`: ${event.error ?? "unknown error"}`
            : `${actorLabel} failed a tool call: ${event.error ?? "unknown error"}`,
          event.timestamp,
          {
            kind: "tool_failed",
            actorLabel,
            toolName,
            detail: event.error ?? "unknown error",
          }
        );
      }

      if (event.event_type === "task_signal_waiting" && eventTaskId) {
        const signalId = event.data?.signal_id;
        if (typeof signalId === "string") {
          pushActivity(`${actorLabel} waiting for signal ${signalId}.`, event.timestamp, {
            kind: "wait",
            actorLabel,
            detail: signalId,
          });
        }
      }

      if (event.event_type === "task_signal_wait_satisfied" && eventTaskId) {
        const signalId = event.data?.signal_id;
        if (typeof signalId === "string") {
          pushActivity(`${actorLabel} resumed on signal ${signalId}.`, event.timestamp, {
            kind: "resume",
            actorLabel,
            detail: signalId,
          });
        }
      }

      if (
        event.event_type === "agent_output" &&
        event.agent_name === "mafia_game_master"
      ) {
        const text = extractFinalOutput(event.data);
        if (text) {
          setFinalReport(text);
          appendThreadMessage(
            "system",
            "Game Master",
            "Final game report is available.",
            "system",
            event.timestamp
          );
        }
      }

      if (
        event.event_type === "run_completed" &&
        event.agent_name === "mafia_game_master" &&
        eventTaskId &&
        taskIdRef.current === eventTaskId
      ) {
        setStarting(false);
        setCancelling(false);
      }

      if (
        ["run_failed", "run_cancelled"].includes(event.event_type) &&
        event.agent_name === "mafia_game_master" &&
        eventTaskId &&
        taskIdRef.current === eventTaskId
      ) {
        setStarting(false);
        setCancelling(false);
        setTaskId(null);
      }
    },
    [appendThreadMessage, pushActivity, resolveActorLabel, setTaskStatus]
  );

  useEffect(() => {
    const stream = new EventSource(`${SSE_BASE_URL}/${userId}`);
    stream.onmessage = (evt) => {
      try {
        const parsed = JSON.parse(evt.data) as AgentEvent;
        handleEvent(parsed);
      } catch (error) {
        console.error("Failed to parse SSE event", error);
      }
    };
    stream.onerror = (error) => {
      console.error("SSE stream error", error);
    };
    return () => {
      stream.close();
    };
  }, [handleEvent, userId]);

  const startGame = useCallback(async () => {
    if (starting || cancelling || (!!taskId && !gameState?.winner)) {
      return;
    }
    setStarting(true);
    resetView();
    try {
      const response = await fetch(`${API_BASE_URL}/enqueue`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: userId,
          game_name: gameName.trim() || "Mafia in nfactorial",
          include_human: includeHuman,
          human_name: humanName.trim() || "You",
          human_role_preference: humanRolePreference,
          ai_player_count: aiPlayerCount,
          day_discussion_seconds: dayDiscussionSeconds,
          day_vote_seconds: dayVoteSeconds,
          night_seconds: nightSeconds,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const payload = (await response.json()) as {
        task_id: string;
        human_player_id?: string | null;
      };
      setTaskId(payload.task_id);
      setHumanPlayerId(payload.human_player_id ?? null);
      setSetupModalOpen(false);
      pushActivity("Game task enqueued.", new Date().toISOString());
    } catch (error) {
      setStarting(false);
      setErrorText(
        error instanceof Error ? error.message : "Failed to start game."
      );
    }
  }, [
    aiPlayerCount,
    cancelling,
    dayDiscussionSeconds,
    dayVoteSeconds,
    gameName,
    humanName,
    humanRolePreference,
    includeHuman,
    nightSeconds,
    pushActivity,
    resetView,
    starting,
    taskId,
    userId,
    gameState?.winner,
  ]);

  const cancelGame = useCallback(async () => {
    if (!taskId || cancelling) {
      return;
    }
    setCancelling(true);
    try {
      const response = await fetch(`${API_BASE_URL}/cancel`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: userId,
          task_id: taskId,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      pushActivity("Cancellation requested.", new Date().toISOString());
    } catch (error) {
      setCancelling(false);
      setErrorText(
        error instanceof Error ? error.message : "Failed to cancel game."
      );
    }
  }, [cancelling, pushActivity, taskId, userId]);

  const sendHumanChat = useCallback(async () => {
    if (
      !taskId ||
      !includeHuman ||
      !humanIsAlive ||
      (gameStateRef.current?.phase ?? "").includes("night")
    ) {
      return;
    }
    const content = chatInput.trim();
    if (!content) {
      return;
    }
    const channel = selectedChannel === "wolf" ? "wolf" : "town";
    appendThreadMessage(channel, "You", content, "chat");
    setChatInput("");
    try {
      const response = await fetch(`${API_BASE_URL}/games/${taskId}/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: userId,
          channel,
          content,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      pushActivity(`Human sent ${channel} chat message.`, new Date().toISOString());
    } catch (error) {
      setErrorText(
        error instanceof Error ? error.message : "Failed to send chat message."
      );
    }
  }, [
    appendThreadMessage,
    chatInput,
    humanIsAlive,
    includeHuman,
    pushActivity,
    selectedChannel,
    taskId,
    userId,
  ]);

  const submitVote = useCallback(async () => {
    if (!taskId || !voteTarget || !includeHuman || !humanIsAlive || submittingVote) {
      return;
    }
    setSubmittingVote(true);
    const selectedTarget =
      gameStateRef.current?.players_omniscient.find(
        (player) => player.player_id === voteTarget
      )?.display_name ??
      gameStateRef.current?.players_public.find(
        (player) => player.player_id === voteTarget
      )?.display_name ??
      voteTarget;
    try {
      const response = await fetch(`${API_BASE_URL}/games/${taskId}/vote`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: userId,
          target_player_id: voteTarget,
          round_no: gameState?.round_no ?? null,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      if (humanPlayerId && typeof gameState?.round_no === "number") {
        setVotedRoundByPlayerId((prev) => ({
          ...prev,
          [humanPlayerId]: gameState.round_no,
        }));
      }
      appendThreadMessage(
        "system",
        "System",
        `You voted for ${selectedTarget}.`,
        "system",
      );
      pushActivity(`Human vote submitted for ${selectedTarget}.`, new Date().toISOString());
    } catch (error) {
      setErrorText(
        error instanceof Error ? error.message : "Failed to submit vote."
      );
    } finally {
      setSubmittingVote(false);
    }
  }, [
    appendThreadMessage,
    gameState?.round_no,
    humanIsAlive,
    humanPlayerId,
    includeHuman,
    pushActivity,
    submittingVote,
    taskId,
    userId,
    voteTarget,
  ]);

  const submitNightAction = useCallback(async () => {
    if (!taskId || !nightTarget || !includeHuman || !humanIsAlive) {
      return;
    }
    try {
      const response = await fetch(`${API_BASE_URL}/games/${taskId}/night_action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: userId,
          target_player_id: nightTarget,
          round_no: gameState?.round_no ?? null,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      pushActivity(
        `Human night action submitted for ${nightTarget}.`,
        new Date().toISOString()
      );
    } catch (error) {
      setErrorText(
        error instanceof Error ? error.message : "Failed to submit night action."
      );
    }
  }, [
    gameState?.round_no,
    humanIsAlive,
    includeHuman,
    nightTarget,
    pushActivity,
    taskId,
    userId,
  ]);

  const canViewWolfThread = useMemo(() => {
    const humanRole = gameState?.human_private_role;
    return showOmniscient || humanRole === "werewolf";
  }, [gameState?.human_private_role, showOmniscient]);

  useEffect(() => {
    if (selectedChannel === "wolf" && !canViewWolfThread) {
      setSelectedChannel("town");
    }
  }, [canViewWolfThread, selectedChannel]);

  const visiblePlayers = useMemo(() => {
    if (!gameState) {
      return [];
    }
    return showOmniscient ? gameState.players_omniscient : gameState.players_public;
  }, [gameState, showOmniscient]);

  const aliveVoteTargets = useMemo(() => {
    return visiblePlayers.filter(
      (player) => player.alive && player.player_id !== humanPlayerId
    );
  }, [humanPlayerId, visiblePlayers]);

  const aliveNightTargets = useMemo(() => {
    return visiblePlayers.filter(
      (player) =>
        player.alive &&
        player.player_id !== humanPlayerId &&
        (player.role ?? "villager") !== "werewolf"
    );
  }, [humanPlayerId, visiblePlayers]);

  const dayVoteOpen = useMemo(() => {
    if (!gameState?.phase) {
      return false;
    }
    return gameState.phase === "open_day_vote" || gameState.phase === "collect_day_votes";
  }, [gameState?.phase]);

  const nightActionOpen = useMemo(() => {
    if (!gameState?.phase) {
      return false;
    }
    return (
      gameState.phase === "open_night_action" ||
      gameState.phase === "collect_night_actions"
    );
  }, [gameState?.phase]);

  const isNightPhase = useMemo(() => {
    if (!gameState?.phase) {
      return false;
    }
    return gameState.phase.includes("night");
  }, [gameState?.phase]);

  const phaseSecondsRemaining = useMemo(() => {
    const deadline = gameState?.phase_deadline_ts;
    if (typeof deadline !== "number" || !Number.isFinite(deadline)) {
      return null;
    }
    return Math.max(0, Math.ceil(deadline - nowEpochMs / 1000));
  }, [gameState?.phase_deadline_ts, nowEpochMs]);
  const phaseTimerLabel =
    phaseSecondsRemaining === null
      ? "No timer"
      : formatCountdown(phaseSecondsRemaining);
  const isPhaseTimerCritical =
    phaseSecondsRemaining !== null && phaseSecondsRemaining <= 5;

  const canSubmitNightAction = useMemo(() => {
    return (
      !!humanPlayerId &&
      gameState?.human_private_role === "werewolf" &&
      nightActionOpen &&
      humanIsAlive &&
      !gameState?.winner
    );
  }, [
    gameState?.human_private_role,
    gameState?.winner,
    humanIsAlive,
    humanPlayerId,
    nightActionOpen,
  ]);
  const humanHasVotedThisRound = useMemo(() => {
    if (!humanPlayerId || !gameState?.round_no) {
      return false;
    }
    return votedRoundByPlayerId[humanPlayerId] === gameState.round_no;
  }, [gameState?.round_no, humanPlayerId, votedRoundByPlayerId]);

  const isGameRunning = !!taskId && !gameState?.winner;
  const canUseHumanActions = isGameRunning && includeHuman && humanIsAlive;

  const activeAgentCount = useMemo(
    () => Object.values(agentStatus).filter((status) => status === "active").length,
    [agentStatus]
  );
  const waitingAgentCount = useMemo(
    () => Object.values(agentStatus).filter((status) => status === "waiting").length,
    [agentStatus]
  );
  const completedAgentCount = useMemo(
    () => Object.values(agentStatus).filter((status) => status === "completed").length,
    [agentStatus]
  );
  const failedAgentCount = useMemo(
    () => Object.values(agentStatus).filter((status) => status === "failed").length,
    [agentStatus]
  );

  const sortedVisiblePlayers = useMemo(() => {
    return [...visiblePlayers].sort((left, right) => Number(right.alive) - Number(left.alive));
  }, [visiblePlayers]);

  const totalMessageCount = useMemo(() => {
    return Object.values(threads).reduce((sum, entries) => sum + entries.length, 0);
  }, [threads]);

  const gameStatus = useMemo(() => {
    if (starting) {
      return {
        label: "Starting",
        className: "border-blue-200 bg-blue-50 text-blue-700",
      };
    }
    if (cancelling) {
      return {
        label: "Cancelling",
        className: "border-amber-200 bg-amber-50 text-amber-700",
      };
    }
    if (gameState?.winner) {
      return {
        label: `${formatRoleLabel(gameState.winner)} Win`,
        className: "border-emerald-200 bg-emerald-50 text-emerald-700",
      };
    }
    if (isGameRunning) {
      return {
        label: "Live",
        className: "border-violet-200 bg-violet-50 text-violet-700",
      };
    }
    return {
      label: "Idle",
      className: "border-slate-200 bg-slate-100 text-slate-700",
    };
  }, [cancelling, gameState?.winner, isGameRunning, starting]);

  const currentThreadMessages = threads[selectedChannel];
  const guidanceText = useMemo(() => {
    if (starting) {
      return "Creating the game session and spawning player agents.";
    }
    if (cancelling) {
      return "Cancellation in progress. Waiting for game master shutdown.";
    }
    if (!taskId) {
      return "Open setup and start a game to begin the simulation.";
    }
    if (gameState?.winner) {
      if (gameState.winner_reason) {
        return `Game finished. ${gameState.winner_reason}`;
      }
      return "Game finished. Review final report and activity timeline.";
    }
    if (includeHuman && !humanIsAlive) {
      return "You are eliminated and now spectating. Follow chat and activity to watch the endgame.";
    }
    if (dayVoteOpen && canUseHumanActions) {
      if (humanHasVotedThisRound) {
        return "Vote submitted for this round. Waiting for other players to vote.";
      }
      return voteTarget
        ? "Day vote is open. Submit your chosen target."
        : "Day vote is open. Choose a player and submit your vote.";
    }
    if (canSubmitNightAction && canUseHumanActions) {
      return nightTarget
        ? "Night action target selected. Submit to lock in the move. Chat is locked at night."
        : "Night action is open. Pick a villager target. Chat is locked at night.";
    }
    if (isNightPhase) {
      return "Night phase is active. Chat is locked while agents resolve actions.";
    }
    if (canUseHumanActions && selectedChannel !== "system") {
      return `You can talk in ${CHANNEL_LABEL[selectedChannel]}.`;
    }
    return "Agents are coordinating. Follow thread updates and runtime activity.";
  }, [
    canSubmitNightAction,
    canUseHumanActions,
    cancelling,
    dayVoteOpen,
    gameState?.winner,
    gameState?.winner_reason,
    humanHasVotedThisRound,
    humanIsAlive,
    isNightPhase,
    includeHuman,
    nightTarget,
    selectedChannel,
    starting,
    taskId,
    voteTarget,
  ]);

  const cardClass =
    "rounded-2xl border border-slate-200/90 bg-white shadow-[0_1px_2px_rgba(15,23,42,0.05)]";
  const boardCardClass = isNightPhase
    ? "rounded-2xl border border-slate-700/80 bg-slate-900 shadow-[0_1px_2px_rgba(2,6,23,0.5)]"
    : cardClass;
  const inputClass =
    "mt-1 w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 outline-none transition focus:border-slate-500 focus:ring-2 focus:ring-slate-200 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-400";
  const subtleButtonClass =
    "inline-flex items-center gap-1.5 rounded-xl border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:bg-slate-50";

  return (
    <div className="min-h-full bg-[radial-gradient(circle_at_top,#ffffff_0%,#f8fafc_48%,#eef2ff_100%)] text-slate-900">
      <div className="mx-auto flex h-full max-w-[1680px] flex-col gap-4 p-4">
        <header className={`${cardClass} px-5 py-4`}>
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h1 className="text-xl font-semibold tracking-tight text-slate-950">
                AI Mafia Control Room
              </h1>
              <p className="mt-1 text-sm text-slate-600">
                Follow agent state in real time with group chat on the right.
              </p>
              <div className="mt-3 flex flex-wrap items-center gap-2 text-xs">
                <span className={`rounded-full border px-2.5 py-1 font-medium ${gameStatus.className}`}>
                  {gameStatus.label}
                </span>
                <span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-600">
                  Phase {formatPhaseLabel(gameState?.phase)}
                </span>
                {gameState && (
                  <span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-600">
                    Round {gameState.round_no}
                  </span>
                )}
                <span
                  className={[
                    "rounded-full border px-2.5 py-1",
                    phaseSecondsRemaining === null
                      ? "border-slate-200 bg-slate-50 text-slate-500"
                      : isPhaseTimerCritical
                        ? "border-rose-200 bg-rose-50 text-rose-700"
                        : "border-emerald-200 bg-emerald-50 text-emerald-700",
                  ].join(" ")}
                >
                  Timer {phaseTimerLabel}
                </span>
                <span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-slate-600">
                  {totalMessageCount} messages
                </span>
                {taskId && (
                  <span className="rounded-full border border-slate-200 bg-white px-2.5 py-1 font-mono text-slate-500">
                    task {shortTaskId(taskId)}
                  </span>
                )}
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-2 text-xs">
              <button
                type="button"
                onClick={() => setSetupModalOpen(true)}
                className={subtleButtonClass}
              >
                <Settings2 className="h-3.5 w-3.5" />
                Game Setup
              </button>
              <button
                type="button"
                onClick={() => setShowOmniscient((prev) => !prev)}
                className={[
                  subtleButtonClass,
                  showOmniscient
                    ? "border-violet-300 bg-violet-50 text-violet-700"
                    : "",
                ].join(" ")}
              >
                {showOmniscient ? <Eye className="h-3.5 w-3.5" /> : <EyeOff className="h-3.5 w-3.5" />}
                {showOmniscient ? "Omniscient On" : "Omniscient Off"}
              </button>
              <button
                type="button"
                onClick={() => setShowActivity((prev) => !prev)}
                className={[
                  subtleButtonClass,
                  showActivity ? "border-violet-300 bg-violet-50 text-violet-700" : "",
                ].join(" ")}
              >
                <MessageSquare className="h-3.5 w-3.5" />
                {showActivity ? "Activity On" : "Activity Off"}
              </button>
              <span className="rounded-full border border-slate-200 bg-white px-2.5 py-1 font-mono text-[11px] text-slate-500">
                session {shortTaskId(userId)}
              </span>
            </div>
          </div>
          <p className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-600">
            {guidanceText}
          </p>
          {errorText && (
            <div className="mt-3 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700">
              {errorText}
            </div>
          )}
        </header>

        <main className="grid min-h-0 flex-1 gap-4 lg:grid-cols-[minmax(0,1fr),420px]">
          <section className="hidden">
            <div className={`${cardClass} p-4`}>
              <div className="mb-3">
                <h2 className="text-sm font-semibold text-slate-900">Game Setup</h2>
                <p className="text-xs text-slate-500">
                  Configure roster and timings, then launch the match.
                </p>
              </div>

              <div className="space-y-3">
                <label className="block text-xs font-medium text-slate-600">
                  Game name
                  <input
                    value={gameName}
                    onChange={(event) => setGameName(event.target.value)}
                    disabled={starting || cancelling}
                    className={inputClass}
                    placeholder="Mafia in nfactorial"
                  />
                </label>

                <div className="grid gap-3 sm:grid-cols-2">
                  <label className="block text-xs font-medium text-slate-600">
                    AI players
                    <input
                      type="number"
                      min={3}
                      max={10}
                      value={aiPlayerCount}
                      disabled={starting || cancelling}
                      onChange={(event) => setAiPlayerCount(Number(event.target.value))}
                      className={inputClass}
                    />
                  </label>
                  <label className="block text-xs font-medium text-slate-600">
                    Human role
                    <select
                      value={humanRolePreference}
                      disabled={starting || cancelling}
                      onChange={(event) =>
                        setHumanRolePreference(event.target.value as HumanRolePreference)
                      }
                      className={inputClass}
                    >
                      <option value="random">Random</option>
                      <option value="villager">Force Villager</option>
                      <option value="werewolf">Force Werewolf</option>
                    </select>
                  </label>
                </div>

                <div className="grid gap-3 sm:grid-cols-3">
                  <label className="block text-xs font-medium text-slate-600">
                    Discuss (s)
                    <input
                      type="number"
                      min={10}
                      max={300}
                      value={dayDiscussionSeconds}
                      disabled={starting || cancelling}
                      onChange={(event) => setDayDiscussionSeconds(Number(event.target.value))}
                      className={inputClass}
                    />
                  </label>
                  <label className="block text-xs font-medium text-slate-600">
                    Vote (s)
                    <input
                      type="number"
                      min={10}
                      max={300}
                      value={dayVoteSeconds}
                      disabled={starting || cancelling}
                      onChange={(event) => setDayVoteSeconds(Number(event.target.value))}
                      className={inputClass}
                    />
                  </label>
                  <label className="block text-xs font-medium text-slate-600">
                    Night (s)
                    <input
                      type="number"
                      min={10}
                      max={300}
                      value={nightSeconds}
                      disabled={starting || cancelling}
                      onChange={(event) => setNightSeconds(Number(event.target.value))}
                      className={inputClass}
                    />
                  </label>
                </div>

                <div className="grid gap-3 sm:grid-cols-[auto,minmax(0,1fr)] sm:items-end">
                  <label className="inline-flex h-10 items-center gap-2 rounded-xl border border-slate-200 bg-slate-50 px-3 text-sm text-slate-700">
                    <input
                      type="checkbox"
                      checked={includeHuman}
                      disabled={starting || cancelling}
                      onChange={(event) => setIncludeHuman(event.target.checked)}
                      className="h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-300"
                    />
                    Include human
                  </label>
                  <label className="block text-xs font-medium text-slate-600">
                    Human display name
                    <input
                      value={humanName}
                      onChange={(event) => setHumanName(event.target.value)}
                      disabled={starting || cancelling || !includeHuman}
                      className={inputClass}
                      placeholder="You"
                    />
                  </label>
                </div>

                <div className="flex flex-wrap items-center gap-2 pt-1">
                  {!isGameRunning ? (
                    <button
                      type="button"
                      onClick={() => void startGame()}
                      disabled={starting}
                      className="inline-flex min-w-[136px] items-center justify-center gap-1.5 rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                    >
                      {starting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                      {starting ? "Starting..." : "Start Game"}
                    </button>
                  ) : (
                    <button
                      type="button"
                      onClick={() => void cancelGame()}
                      disabled={cancelling}
                      className="inline-flex min-w-[136px] items-center justify-center gap-1.5 rounded-xl bg-rose-600 px-4 py-2 text-sm font-medium text-white transition hover:bg-rose-500 disabled:cursor-not-allowed disabled:opacity-60"
                    >
                      {cancelling ? <Loader2 className="h-4 w-4 animate-spin" /> : <Square className="h-4 w-4" />}
                      {cancelling ? "Cancelling..." : "Cancel Game"}
                    </button>
                  )}
                  <span className="text-xs text-slate-500">
                    {isGameRunning ? "Match is in progress." : "Start when ready."}
                  </span>
                </div>

                {taskId && (
                  <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 font-mono text-[11px] text-slate-500">
                    {taskId}
                  </div>
                )}
                {errorText && (
                  <div className="rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700">
                    {errorText}
                  </div>
                )}
              </div>
            </div>
          </section>

          <section className="order-2 flex min-h-0 flex-col gap-4">
            <div className={`${cardClass} flex min-h-0 flex-1 flex-col p-4`}>
              <div className="mb-3 flex items-center justify-between gap-3">
                <div>
                  <h2 className="text-sm font-semibold text-slate-900">Group Chat</h2>
                  <p className="text-xs text-slate-500">Live thread for town, wolf, and system updates.</p>
                </div>
                <span className="rounded-full border border-slate-200 bg-slate-50 px-2.5 py-1 text-[11px] text-slate-500">
                  {CHANNEL_LABEL[selectedChannel]}
                </span>
              </div>

              <div className="mb-3 flex flex-wrap items-center gap-2">
                {(["town", "wolf", "system"] as const).map((channel) => {
                  if (channel === "wolf" && !canViewWolfThread) {
                    return null;
                  }
                  const selected = selectedChannel === channel;
                  const badgeCount = threads[channel].length;
                  return (
                    <button
                      key={channel}
                      type="button"
                      onClick={() => setSelectedChannel(channel)}
                      className={[
                        "rounded-xl border px-3 py-1.5 text-xs font-medium transition",
                        selected
                          ? "border-slate-900 bg-slate-900 text-white"
                          : "border-slate-300 bg-white text-slate-700 hover:bg-slate-50",
                      ].join(" ")}
                    >
                      {CHANNEL_LABEL[channel]} ({badgeCount})
                    </button>
                  );
                })}
              </div>

              <div className="min-h-0 flex-1 space-y-2 overflow-y-auto rounded-xl border border-slate-200 bg-slate-50 p-3">
                {currentThreadMessages.length === 0 ? (
                  <div className="flex h-full items-center justify-center text-sm text-slate-500">
                    No messages in this channel yet.
                  </div>
                ) : (
                  currentThreadMessages.map((message) => {
                    const fromHuman = message.fromLabel === "You";
                    const isSystem = message.channel === "system" || message.badge === "system";
                    return (
                      <div
                        key={message.id}
                        className={[
                          "rounded-xl border px-3 py-2",
                          fromHuman
                            ? "border-slate-900 bg-slate-900 text-white"
                            : isSystem
                              ? "border-indigo-200 bg-indigo-50"
                              : "border-slate-200 bg-white",
                        ].join(" ")}
                      >
                        <div
                          className={[
                            "mb-1 flex items-center justify-between text-[11px]",
                            fromHuman ? "text-slate-300" : "text-slate-500",
                          ].join(" ")}
                        >
                          <span className={fromHuman ? "font-medium text-white" : "font-medium text-slate-700"}>
                            {message.fromLabel}
                          </span>
                          <span>{formatTimestamp(message.timestamp)}</span>
                        </div>
                        <p className={["whitespace-pre-wrap text-sm", fromHuman ? "text-white" : "text-slate-800"].join(" ")}>
                          {message.content}
                        </p>
                      </div>
                    );
                  })
                )}
              </div>

              <div className="mt-3 flex items-center gap-2">
                <input
                  value={chatInput}
                  onChange={(event) => setChatInput(event.target.value)}
                  placeholder={
                    isNightPhase
                      ? "Night phase: chat is locked."
                      : selectedChannel === "wolf"
                      ? "Message the wolf den..."
                      : "Message the town square..."
                  }
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      event.preventDefault();
                      void sendHumanChat();
                    }
                  }}
                  disabled={
                    !canUseHumanActions || selectedChannel === "system" || isNightPhase
                  }
                  className="flex-1 rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 outline-none transition focus:border-slate-500 focus:ring-2 focus:ring-slate-200 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-400"
                />
                <button
                  type="button"
                  onClick={() => void sendHumanChat()}
                  disabled={
                    !canUseHumanActions || selectedChannel === "system" || isNightPhase
                  }
                  className="inline-flex items-center gap-1.5 rounded-xl bg-slate-900 px-3 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  <Send className="h-4 w-4" />
                  Send
                </button>
              </div>
            </div>

          </section>

          <section className="order-1 min-h-0 flex flex-col gap-4">
            <div className={`${boardCardClass} flex min-h-[380px] flex-col p-4`}>
              <div className="mb-3">
                <h2
                  className={[
                    "text-sm font-semibold",
                    isNightPhase ? "text-slate-100" : "text-slate-900",
                  ].join(" ")}
                >
                  Agent Board {isNightPhase ? "· Night" : "· Day"}
                </h2>
                <p className={["text-xs", isNightPhase ? "text-slate-400" : "text-slate-500"].join(" ")}>
                  Robot view of all players. Eliminated agents are muted.
                </p>
              </div>

              {gameState && (
                <div className="mb-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
                  <div
                    className={[
                      "rounded-xl border px-3 py-2 text-xs",
                      isNightPhase
                        ? "border-slate-700 bg-slate-800 text-slate-300"
                        : "border-slate-200 bg-slate-50 text-slate-600",
                    ].join(" ")}
                  >
                    Phase{" "}
                    <span className={["font-semibold", isNightPhase ? "text-slate-100" : "text-slate-900"].join(" ")}>
                      {formatPhaseLabel(gameState.phase)}
                    </span>
                  </div>
                  <div
                    className={[
                      "rounded-xl border px-3 py-2 text-xs",
                      isNightPhase
                        ? "border-slate-700 bg-slate-800 text-slate-300"
                        : "border-slate-200 bg-slate-50 text-slate-600",
                    ].join(" ")}
                  >
                    Round{" "}
                    <span className={["font-semibold", isNightPhase ? "text-slate-100" : "text-slate-900"].join(" ")}>
                      {gameState.round_no}
                    </span>
                  </div>
                  <div
                    className={[
                      "rounded-xl border px-3 py-2 text-xs",
                      isNightPhase
                        ? "border-slate-700 bg-slate-800 text-slate-300"
                        : "border-slate-200 bg-slate-50 text-slate-600",
                    ].join(" ")}
                  >
                    Villagers{" "}
                    <span className={["font-semibold", isNightPhase ? "text-slate-100" : "text-slate-900"].join(" ")}>
                      {gameState.alive_villagers}
                    </span>
                  </div>
                  <div
                    className={[
                      "rounded-xl border px-3 py-2 text-xs",
                      isNightPhase
                        ? "border-slate-700 bg-slate-800 text-slate-300"
                        : "border-slate-200 bg-slate-50 text-slate-600",
                    ].join(" ")}
                  >
                    Werewolves{" "}
                    <span className={["font-semibold", isNightPhase ? "text-slate-100" : "text-slate-900"].join(" ")}>
                      {gameState.alive_werewolves}
                    </span>
                  </div>
                </div>
              )}

              {includeHuman && gameState?.human_private_role && (
                <div
                  className={[
                    "mb-3 rounded-xl border px-3 py-2 text-xs",
                    gameState.human_private_role === "werewolf"
                      ? "border-rose-200 bg-rose-50 text-rose-700"
                      : "border-emerald-200 bg-emerald-50 text-emerald-700",
                  ].join(" ")}
                >
                  Your role <span className="font-semibold">{formatRoleLabel(gameState.human_private_role)}</span>
                </div>
              )}
              {includeHuman && humanPlayerId && !humanIsAlive && (
                <div className="mb-3 rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
                  You are eliminated and now spectating.
                </div>
              )}
              {gameState?.winner && (
                <div className="mb-3 rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-700">
                  Winner <span className="font-semibold">{formatRoleLabel(gameState.winner)}</span>
                  {gameState.winner_reason ? ` — ${gameState.winner_reason}` : ""}
                </div>
              )}

              {!gameState ? (
                <div
                  className={[
                    "flex flex-1 items-center justify-center rounded-xl border border-dashed text-sm",
                    isNightPhase
                      ? "border-slate-700 bg-slate-800 text-slate-400"
                      : "border-slate-300 bg-slate-50 text-slate-500",
                  ].join(" ")}
                >
                  Start a game from setup to see the agent board.
                </div>
              ) : (
                <div className="min-h-0 flex-1 overflow-y-auto pr-1">
                  <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                    {sortedVisiblePlayers.map((player) => {
                      const votedThisRound = Boolean(
                        gameState?.round_no &&
                          votedRoundByPlayerId[player.player_id] === gameState.round_no
                      );
                      const statusLabel = !player.alive
                        ? "Eliminated"
                        : votedThisRound
                          ? "Voted"
                          : "Online";
                      const statusDotClass = !player.alive
                        ? "bg-slate-400"
                        : votedThisRound
                          ? "bg-amber-400"
                          : "bg-emerald-400";
                      const avatar = !player.alive
                        ? "💀"
                        : player.is_human
                          ? "🧑"
                          : showOmniscient && player.role === "werewolf"
                            ? "🐺"
                            : "🤖";
                      const showSleepingIcon = isNightPhase && player.alive && !player.is_human;
                      return (
                        <div
                          key={player.player_id}
                          className={[
                            "rounded-2xl border p-3 transition",
                            player.alive
                              ? isNightPhase
                                ? "border-slate-700 bg-slate-800"
                                : "border-slate-200 bg-white"
                              : isNightPhase
                                ? "border-slate-700 bg-slate-900 opacity-55 grayscale"
                                : "border-slate-200 bg-slate-100 opacity-55 grayscale",
                          ].join(" ")}
                        >
                          <div className="mb-2 flex items-center justify-between">
                            <span className="text-2xl leading-none">{showSleepingIcon ? "😴" : avatar}</span>
                            <span
                              className={[
                                "inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[11px] font-medium",
                                isNightPhase
                                  ? "bg-slate-700 text-slate-100"
                                  : "bg-slate-100 text-slate-700",
                              ].join(" ")}
                            >
                              <span className={["h-2 w-2 rounded-full", statusDotClass].join(" ")} />
                              {statusLabel}
                            </span>
                          </div>
                          <div className={["text-sm font-semibold", isNightPhase ? "text-slate-100" : "text-slate-900"].join(" ")}>
                            {player.display_name}
                          </div>
                          <div className={["mt-0.5 text-xs", isNightPhase ? "text-slate-400" : "text-slate-500"].join(" ")}>
                            {showOmniscient && player.role
                              ? formatRoleLabel(player.role)
                              : player.is_human
                                ? "Human player"
                                : "AI agent"}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <div className={`${cardClass} p-4`}>
                <div className="mb-2 flex items-center justify-between gap-2">
                  <h3 className="flex items-center gap-1.5 text-sm font-semibold text-slate-900">
                    <Vote className="h-4 w-4 text-indigo-600" />
                    Day Vote
                  </h3>
                  <span
                    className={[
                      "rounded-full px-2 py-0.5 text-[11px] font-medium",
                      humanHasVotedThisRound
                        ? "bg-amber-100 text-amber-700"
                        : dayVoteOpen
                          ? "bg-emerald-100 text-emerald-700"
                          : "bg-slate-100 text-slate-500",
                    ].join(" ")}
                  >
                    {humanHasVotedThisRound ? "Voted" : dayVoteOpen ? "Open" : "Closed"}
                  </span>
                </div>
                <select
                  value={voteTarget}
                  onChange={(event) => setVoteTarget(event.target.value)}
                  disabled={!dayVoteOpen || !canUseHumanActions || humanHasVotedThisRound}
                  className="w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 outline-none transition focus:border-slate-500 focus:ring-2 focus:ring-slate-200 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-400"
                >
                  <option value="">Select target</option>
                  {aliveVoteTargets.map((player) => (
                    <option key={player.player_id} value={player.player_id}>
                      {player.display_name}
                    </option>
                  ))}
                </select>
                <button
                  type="button"
                  onClick={() => void submitVote()}
                  disabled={
                    !dayVoteOpen ||
                    !voteTarget ||
                    !canUseHumanActions ||
                    humanHasVotedThisRound ||
                    submittingVote
                  }
                  className="mt-2 w-full rounded-xl bg-slate-900 px-3 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {submittingVote ? (
                    <span className="inline-flex items-center gap-2">
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Submitting...
                    </span>
                  ) : humanHasVotedThisRound ? (
                    "Vote Submitted"
                  ) : (
                    "Submit Vote"
                  )}
                </button>
              </div>

              <div className={`${cardClass} p-4`}>
                <div className="mb-2 flex items-center justify-between gap-2">
                  <h3 className="flex items-center gap-1.5 text-sm font-semibold text-slate-900">
                    <MoonStar className="h-4 w-4 text-rose-600" />
                    Night Action
                  </h3>
                  <span
                    className={[
                      "rounded-full px-2 py-0.5 text-[11px] font-medium",
                      canSubmitNightAction
                        ? "bg-emerald-100 text-emerald-700"
                        : "bg-slate-100 text-slate-500",
                    ].join(" ")}
                  >
                    {canSubmitNightAction ? "Open" : "Closed"}
                  </span>
                </div>
                <select
                  value={nightTarget}
                  onChange={(event) => setNightTarget(event.target.value)}
                  disabled={!canSubmitNightAction || !canUseHumanActions}
                  className="w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 outline-none transition focus:border-slate-500 focus:ring-2 focus:ring-slate-200 disabled:cursor-not-allowed disabled:bg-slate-50 disabled:text-slate-400"
                >
                  <option value="">Select target</option>
                  {aliveNightTargets.map((player) => (
                    <option key={player.player_id} value={player.player_id}>
                      {player.display_name}
                    </option>
                  ))}
                </select>
                <button
                  type="button"
                  onClick={() => void submitNightAction()}
                  disabled={!canSubmitNightAction || !nightTarget || !canUseHumanActions}
                  className="mt-2 w-full rounded-xl bg-slate-900 px-3 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  Submit Night Action
                </button>
              </div>
            </div>

            {showActivity && (
              <div className={`${cardClass} p-4`}>
                <div className="mb-3 flex items-center justify-between gap-2">
                  <h3 className="text-sm font-semibold text-slate-900">Runtime Activity</h3>
                  <div className="flex items-center gap-2 text-[11px] text-slate-500">
                    <span>Active {activeAgentCount}</span>
                    <span>Waiting {waitingAgentCount}</span>
                    <span>Done {completedAgentCount}</span>
                    <span>Failed {failedAgentCount}</span>
                  </div>
                </div>
                <div className="max-h-64 space-y-2 overflow-y-auto pr-1">
                  {activity.length === 0 ? (
                    <p className="text-sm text-slate-500">No activity yet.</p>
                  ) : (
                    activity.slice(-40).reverse().map((entry) => {
                      const styleByKind: Record<
                        ActivityKind,
                        { card: string; meta: string; text: string }
                      > = {
                        info: {
                          card: "border-slate-200 bg-slate-50",
                          meta: "text-slate-500",
                          text: "text-slate-700",
                        },
                        tool_started: {
                          card: "border-blue-200 bg-blue-50",
                          meta: "text-blue-700",
                          text: "text-blue-900",
                        },
                        tool_completed: {
                          card: "border-emerald-200 bg-emerald-50",
                          meta: "text-emerald-700",
                          text: "text-emerald-900",
                        },
                        tool_failed: {
                          card: "border-rose-200 bg-rose-50",
                          meta: "text-rose-700",
                          text: "text-rose-900",
                        },
                        wait: {
                          card: "border-amber-200 bg-amber-50",
                          meta: "text-amber-700",
                          text: "text-amber-900",
                        },
                        resume: {
                          card: "border-violet-200 bg-violet-50",
                          meta: "text-violet-700",
                          text: "text-violet-900",
                        },
                      };
                      const style = styleByKind[entry.kind];
                      const iconByKind = {
                        info: <MessageSquare className="h-3.5 w-3.5" />,
                        tool_started: <Hammer className="h-3.5 w-3.5" />,
                        tool_completed: <CheckCircle2 className="h-3.5 w-3.5" />,
                        tool_failed: <AlertTriangle className="h-3.5 w-3.5" />,
                        wait: <Clock3 className="h-3.5 w-3.5" />,
                        resume: <Play className="h-3.5 w-3.5" />,
                      } as const;
                      return (
                        <div
                          key={entry.id}
                          className={`rounded-xl border px-3 py-2 ${style.card}`}
                        >
                          <div className="flex items-start gap-2">
                            <span className={`mt-0.5 ${style.meta}`}>{iconByKind[entry.kind]}</span>
                            <div className="min-w-0 flex-1">
                              <div className={`mb-0.5 flex flex-wrap items-center gap-1 text-[11px] ${style.meta}`}>
                                <span>{formatTimestamp(entry.timestamp)}</span>
                                {entry.actorLabel && (
                                  <span className="rounded-full border border-current/30 px-1.5 py-0.5">
                                    {entry.actorLabel}
                                  </span>
                                )}
                                {entry.toolName && (
                                  <span className="rounded-full border border-current/30 px-1.5 py-0.5 font-mono">
                                    {entry.toolName}
                                  </span>
                                )}
                              </div>
                              <div className={`text-xs ${style.text}`}>{entry.text}</div>
                              {entry.detail && (
                                <div className={`mt-0.5 text-[11px] ${style.meta}`}>
                                  {entry.detail}
                                </div>
                              )}
                            </div>
                          </div>
                        </div>
                      );
                    })
                  )}
                </div>
              </div>
            )}

            {finalReport && (
              <div className="rounded-2xl border border-emerald-200 bg-emerald-50 p-4 shadow-[0_1px_2px_rgba(15,23,42,0.05)]">
                <h3 className="mb-2 flex items-center gap-1.5 text-sm font-semibold text-emerald-900">
                  <Shield className="h-4 w-4" />
                  Final Report
                </h3>
                <pre className="max-h-[180px] overflow-auto whitespace-pre-wrap text-xs text-emerald-900">
                  {finalReport}
                </pre>
              </div>
            )}
          </section>
        </main>
      </div>

      {setupModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
          <div
            className="absolute inset-0 bg-slate-950/35 backdrop-blur-[2px]"
            onClick={() => {
              if (!starting) {
                setSetupModalOpen(false);
              }
            }}
          />
          <div className="relative z-10 w-full max-w-2xl rounded-2xl border border-slate-200 bg-white p-5 shadow-[0_20px_50px_rgba(15,23,42,0.22)]">
            <div className="mb-4 flex items-start justify-between gap-3">
              <div>
                <h2 className="text-base font-semibold text-slate-950">Game Setup</h2>
                <p className="mt-1 text-sm text-slate-600">
                  Configure this run, then launch into the live board.
                </p>
              </div>
              <button
                type="button"
                onClick={() => setSetupModalOpen(false)}
                disabled={starting}
                className="rounded-xl border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
              >
                Close
              </button>
            </div>

            <div className="space-y-3">
              <label className="block text-xs font-medium text-slate-600">
                Game name
                <input
                  value={gameName}
                  onChange={(event) => setGameName(event.target.value)}
                  disabled={starting || cancelling}
                  className={inputClass}
                  placeholder="Mafia in nfactorial"
                />
              </label>

              <div className="grid gap-3 sm:grid-cols-2">
                <label className="block text-xs font-medium text-slate-600">
                  AI players
                  <input
                    type="number"
                    min={3}
                    max={10}
                    value={aiPlayerCount}
                    disabled={starting || cancelling}
                    onChange={(event) => setAiPlayerCount(Number(event.target.value))}
                    className={inputClass}
                  />
                </label>
                <label className="block text-xs font-medium text-slate-600">
                  Human role
                  <select
                    value={humanRolePreference}
                    disabled={starting || cancelling}
                    onChange={(event) =>
                      setHumanRolePreference(event.target.value as HumanRolePreference)
                    }
                    className={inputClass}
                  >
                    <option value="random">Random</option>
                    <option value="villager">Force Villager</option>
                    <option value="werewolf">Force Werewolf</option>
                  </select>
                </label>
              </div>

              <div className="grid gap-3 sm:grid-cols-3">
                <label className="block text-xs font-medium text-slate-600">
                  Discuss (s)
                  <input
                    type="number"
                    min={10}
                    max={300}
                    value={dayDiscussionSeconds}
                    disabled={starting || cancelling}
                    onChange={(event) => setDayDiscussionSeconds(Number(event.target.value))}
                    className={inputClass}
                  />
                </label>
                <label className="block text-xs font-medium text-slate-600">
                  Vote (s)
                  <input
                    type="number"
                    min={10}
                    max={300}
                    value={dayVoteSeconds}
                    disabled={starting || cancelling}
                    onChange={(event) => setDayVoteSeconds(Number(event.target.value))}
                    className={inputClass}
                  />
                </label>
                <label className="block text-xs font-medium text-slate-600">
                  Night (s)
                  <input
                    type="number"
                    min={10}
                    max={300}
                    value={nightSeconds}
                    disabled={starting || cancelling}
                    onChange={(event) => setNightSeconds(Number(event.target.value))}
                    className={inputClass}
                  />
                </label>
              </div>

              <div className="grid gap-3 sm:grid-cols-[auto,minmax(0,1fr)] sm:items-end">
                <label className="inline-flex h-10 items-center gap-2 rounded-xl border border-slate-200 bg-slate-50 px-3 text-sm text-slate-700">
                  <input
                    type="checkbox"
                    checked={includeHuman}
                    disabled={starting || cancelling}
                    onChange={(event) => setIncludeHuman(event.target.checked)}
                    className="h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-300"
                  />
                  Include human
                </label>
                <label className="block text-xs font-medium text-slate-600">
                  Human display name
                  <input
                    value={humanName}
                    onChange={(event) => setHumanName(event.target.value)}
                    disabled={starting || cancelling || !includeHuman}
                    className={inputClass}
                    placeholder="You"
                  />
                </label>
              </div>

              {errorText && (
                <div className="rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700">
                  {errorText}
                </div>
              )}
            </div>

            <div className="mt-4 flex flex-wrap items-center justify-between gap-2">
              <div className="text-xs text-slate-500">
                {isGameRunning
                  ? "A game is already running. Cancel it before starting another."
                  : "Ready to launch the simulation."}
              </div>
              <button
                type="button"
                onClick={() => void startGame()}
                disabled={starting || cancelling || isGameRunning}
                className="inline-flex min-w-[148px] items-center justify-center gap-1.5 rounded-xl bg-slate-900 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {starting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                {starting ? "Starting..." : "Start Game"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
