import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { API_BASE_URL, SSE_BASE_URL } from "../constants";
import type {
  ActivityEntry,
  ActivityKind,
  AgentEvent,
  AgentStatus,
  Channel,
  GameStateSnapshot,
  HumanRolePreference,
  PlayerStateView,
  ThoughtEntry,
  UiMessage,
} from "../types";
import {
  CHANNEL_LABEL,
  createUserId,
  extractFinalOutput,
  formatCountdown,
  normalizeChannel,
  parseToolArgs,
  shortTaskId,
  summarizeToolAction,
} from "../utils";

export function useGameEngine() {
  const [userId] = useState(createUserId);

  /* -- Configuration ------------------------------------------------- */

  const [gameName, setGameName] = useState("Mafia in nfactorial");
  const [includeHuman, setIncludeHuman] = useState(true);
  const [humanName, setHumanName] = useState("You");
  const [humanRolePreference, setHumanRolePreference] =
    useState<HumanRolePreference>("random");
  const [aiPlayerCount, setAiPlayerCount] = useState(7);
  const [dayDiscussionSeconds, setDayDiscussionSeconds] = useState(90);
  const [dayVoteSeconds, setDayVoteSeconds] = useState(35);
  const [nightSeconds, setNightSeconds] = useState(25);

  /* -- UI toggles ---------------------------------------------------- */

  const [showOmniscient, setShowOmniscient] = useState(false);
  const [showActivity, setShowActivity] = useState(false);

  /* -- Core game state ----------------------------------------------- */

  const [taskId, setTaskId] = useState<string | null>(null);
  const [humanPlayerId, setHumanPlayerId] = useState<string | null>(null);
  const [setupModalOpen, setSetupModalOpen] = useState(true);
  const [starting, setStarting] = useState(false);
  const [cancelling, setCancelling] = useState(false);
  const [errorText, setErrorText] = useState<string | null>(null);

  const [gameState, setGameState] = useState<GameStateSnapshot | null>(null);
  const [finalReport, setFinalReport] = useState<string | null>(null);
  const [showFinalReport, setShowFinalReport] = useState(false);
  const [agentStatus, setAgentStatus] = useState<Record<string, AgentStatus>>(
    {},
  );

  /* -- Chat & interaction state -------------------------------------- */

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
  const [thoughtsByPlayerId, setThoughtsByPlayerId] = useState<
    Record<string, ThoughtEntry[]>
  >({});
  const [selectedAgentPlayerId, setSelectedAgentPlayerId] = useState<
    string | null
  >(null);
  const [votedRoundByPlayerId, setVotedRoundByPlayerId] = useState<
    Record<string, number>
  >({});
  const [submittingVote, setSubmittingVote] = useState(false);
  const [nightActionSubmittedRound, setNightActionSubmittedRound] = useState<
    number | null
  >(null);
  const [submittingNightAction, setSubmittingNightAction] = useState(false);
  const [calledVoteRound, setCalledVoteRound] = useState<number | null>(null);
  const [submittingCallVote, setSubmittingCallVote] = useState(false);

  /* -- Refs & timers ------------------------------------------------- */

  const taskIdRef = useRef<string | null>(taskId);
  const gameStateRef = useRef<GameStateSnapshot | null>(gameState);
  const [nowEpochMs, setNowEpochMs] = useState(() => Date.now());

  useEffect(() => {
    taskIdRef.current = taskId;
  }, [taskId]);
  useEffect(() => {
    gameStateRef.current = gameState;
  }, [gameState]);

  useEffect(() => {
    const id = window.setInterval(() => setNowEpochMs(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, []);

  /* -- Derived: human alive ------------------------------------------ */

  const humanIsAlive = useMemo(() => {
    if (!includeHuman || !gameState || !humanPlayerId) return false;
    const humanPlayer =
      gameState.players_omniscient.find(
        (p) => p.player_id === humanPlayerId,
      ) ??
      gameState.players_public.find((p) => p.player_id === humanPlayerId);
    return humanPlayer?.alive ?? false;
  }, [gameState, humanPlayerId, includeHuman]);

  /* -- Internal helpers ---------------------------------------------- */

  const resetView = useCallback(() => {
    setGameState(null);
    setFinalReport(null);
    setShowFinalReport(false);
    setThreads({ town: [], wolf: [], system: [] });
    setActivity([]);
    setThoughtsByPlayerId({});
    setSelectedAgentPlayerId(null);
    setAgentStatus({});
    setVotedRoundByPlayerId({});
    setSubmittingVote(false);
    setNightActionSubmittedRound(null);
    setSubmittingNightAction(false);
    setCalledVoteRound(null);
    setSubmittingCallVote(false);
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
      },
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
        return merged.length > 500
          ? merged.slice(merged.length - 500)
          : merged;
      });
    },
    [],
  );

  const appendThreadMessage = useCallback(
    (
      channel: Channel,
      fromLabel: string,
      content: string,
      badge: UiMessage["badge"],
      timestamp?: string,
    ) => {
      const normalized = content.trim();
      if (!normalized) return;
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
    [],
  );

  const resolveActorLabel = useCallback((event: AgentEvent): string => {
    const eventTaskId =
      typeof event.task_id === "string" ? event.task_id : null;
    const state = gameStateRef.current;
    if (eventTaskId && state) {
      for (const player of state.players_omniscient) {
        if (player.task_id === eventTaskId) return player.display_name;
      }
    }
    if (event.agent_name === "mafia_game_master") return "Game Master";
    if (eventTaskId) return shortTaskId(eventTaskId);
    return event.agent_name ?? "system";
  }, []);

  const resolveActorPlayerId = useCallback(
    (
      eventTaskId: string | null,
      stateOverride?: GameStateSnapshot | null,
    ): string | null => {
      if (!eventTaskId) return null;
      const state = stateOverride ?? gameStateRef.current;
      if (!state) return null;
      return (
        state.players_omniscient.find((p) => p.task_id === eventTaskId)
          ?.player_id ?? null
      );
    },
    [],
  );

  const setTaskStatus = useCallback(
    (eventTaskId: string | null, status: AgentStatus) => {
      if (!eventTaskId) return;
      setAgentStatus((prev) => ({ ...prev, [eventTaskId]: status }));
    },
    [],
  );

  /* -- SSE event handler --------------------------------------------- */

  const handleEvent = useCallback(
    (event: AgentEvent) => {
      const eventTaskId =
        typeof event.task_id === "string" ? event.task_id : null;
      const actorLabel = resolveActorLabel(event);

      /* Status transitions */
      if (event.event_type === "run_started")
        setTaskStatus(eventTaskId, "active");
      else if (event.event_type === "run_completed")
        setTaskStatus(eventTaskId, "completed");
      else if (event.event_type === "run_failed")
        setTaskStatus(eventTaskId, "failed");
      else if (event.event_type === "run_cancelled")
        setTaskStatus(eventTaskId, "cancelled");
      else if (
        event.event_type === "task_activity_waiting" ||
        event.event_type === "task_signal_waiting"
      )
        setTaskStatus(eventTaskId, "waiting");
      else if (event.event_type === "task_signal_wait_satisfied")
        setTaskStatus(eventTaskId, "active");

      /* Tool started */
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
            },
          );
        }
      }

      /* Tool completed */
      if (event.event_type === "progress_update_tool_action_completed") {
        const completion = event.data?.result;
        const toolCall = completion?.tool_call;
        const toolName = toolCall?.function?.name as string | undefined;
        const output = (completion?.client_output ?? {}) as Record<
          string,
          any
        >;
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
            },
          );
        }

        const maybeState = output?.game_state;
        if (maybeState && typeof maybeState === "object") {
          setGameState(maybeState as GameStateSnapshot);
        }
        const stateForLookup =
          maybeState && typeof maybeState === "object"
            ? (maybeState as GameStateSnapshot)
            : gameStateRef.current;
        const actorPlayerId = resolveActorPlayerId(
          eventTaskId,
          stateForLookup,
        );

        if (toolName === "setup_game") {
          const fromStateHumanId =
            (maybeState?.human_player_id as string | undefined) ?? null;
          setHumanPlayerId((prev) => prev ?? fromStateHumanId);
        }

        if (toolName === "vote" && actorPlayerId) {
          const currentRound = gameStateRef.current?.round_no;
          if (typeof currentRound === "number") {
            setVotedRoundByPlayerId((prev) => ({
              ...prev,
              [actorPlayerId]: currentRound,
            }));
          }
        }

        if (
          output?.channel === "thought" &&
          actorPlayerId &&
          typeof output?.message === "string" &&
          output.message.trim()
        ) {
          const normalizedThought = output.message.trim();
          setThoughtsByPlayerId((prev) => {
            const currentThoughts = prev[actorPlayerId] ?? [];
            const nextThoughts = [
              ...currentThoughts,
              {
                id: `${actorPlayerId}:${event.timestamp}:${currentThoughts.length}`,
                timestamp: event.timestamp,
                content: normalizedThought,
              },
            ];
            return { ...prev, [actorPlayerId]: nextThoughts.slice(-200) };
          });
        }

        const outputChannel = normalizeChannel(output?.channel);
        if (
          outputChannel &&
          typeof output?.message === "string" &&
          output.message.trim()
        ) {
          appendThreadMessage(
            outputChannel,
            actorLabel,
            output.message,
            outputChannel === "system" ? "system" : "chat",
            event.timestamp,
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

      /* Tool failed */
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
          },
        );
      }

      /* Signal waiting / satisfied */
      if (event.event_type === "task_signal_waiting" && eventTaskId) {
        const signalId = event.data?.signal_id;
        if (typeof signalId === "string") {
          pushActivity(
            `${actorLabel} waiting for signal ${signalId}.`,
            event.timestamp,
            { kind: "wait", actorLabel, detail: signalId },
          );
        }
      }

      if (event.event_type === "task_signal_wait_satisfied" && eventTaskId) {
        const signalId = event.data?.signal_id;
        if (typeof signalId === "string") {
          pushActivity(
            `${actorLabel} resumed on signal ${signalId}.`,
            event.timestamp,
            { kind: "resume", actorLabel, detail: signalId },
          );
        }
      }

      /* Game master final output */
      if (
        event.event_type === "agent_output" &&
        event.agent_name === "mafia_game_master"
      ) {
        const text = extractFinalOutput(event.data);
        if (text) {
          setFinalReport(text);
          setShowFinalReport(true);
          appendThreadMessage(
            "system",
            "Game Master",
            "Final game report is available.",
            "system",
            event.timestamp,
          );
        }
      }

      /* Game master run completed */
      if (
        event.event_type === "run_completed" &&
        event.agent_name === "mafia_game_master" &&
        eventTaskId &&
        taskIdRef.current === eventTaskId
      ) {
        setStarting(false);
        setCancelling(false);
      }

      /* Game master run failed / cancelled */
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
    [
      appendThreadMessage,
      pushActivity,
      resolveActorLabel,
      resolveActorPlayerId,
      setTaskStatus,
    ],
  );

  /* -- SSE connection ------------------------------------------------ */

  useEffect(() => {
    const stream = new EventSource(`${SSE_BASE_URL}/${userId}`);
    stream.onmessage = (evt) => {
      try {
        handleEvent(JSON.parse(evt.data) as AgentEvent);
      } catch (error) {
        console.error("Failed to parse SSE event", error);
      }
    };
    stream.onerror = (error) => console.error("SSE stream error", error);
    return () => stream.close();
  }, [handleEvent, userId]);

  /* -- API actions --------------------------------------------------- */

  const startGame = useCallback(async () => {
    if (starting || cancelling || (!!taskId && !gameState?.winner)) return;
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
      if (!response.ok) throw new Error(await response.text());
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
        error instanceof Error ? error.message : "Failed to start game.",
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
    if (!taskId || cancelling) return;
    setCancelling(true);
    try {
      const response = await fetch(`${API_BASE_URL}/cancel`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: userId, task_id: taskId }),
      });
      if (!response.ok) throw new Error(await response.text());
      pushActivity("Cancellation requested.", new Date().toISOString());
    } catch (error) {
      setCancelling(false);
      setErrorText(
        error instanceof Error ? error.message : "Failed to cancel game.",
      );
    }
  }, [cancelling, pushActivity, taskId, userId]);

  const sendHumanChat = useCallback(async () => {
    if (
      !taskId ||
      !includeHuman ||
      !humanIsAlive ||
      (gameStateRef.current?.phase ?? "").includes("night")
    )
      return;
    const content = chatInput.trim();
    if (!content) return;
    const channel = selectedChannel === "wolf" ? "wolf" : "town";
    appendThreadMessage(channel, "You", content, "chat");
    setChatInput("");
    try {
      const response = await fetch(
        `${API_BASE_URL}/games/${taskId}/chat`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ user_id: userId, channel, content }),
        },
      );
      if (!response.ok) throw new Error(await response.text());
      pushActivity(
        `Human sent ${channel} chat message.`,
        new Date().toISOString(),
      );
    } catch (error) {
      setErrorText(
        error instanceof Error
          ? error.message
          : "Failed to send chat message.",
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
    if (
      !taskId ||
      !voteTarget ||
      !includeHuman ||
      !humanIsAlive ||
      submittingVote
    )
      return;
    setSubmittingVote(true);
    const selectedTarget =
      gameStateRef.current?.players_omniscient.find(
        (p) => p.player_id === voteTarget,
      )?.display_name ??
      gameStateRef.current?.players_public.find(
        (p) => p.player_id === voteTarget,
      )?.display_name ??
      voteTarget;
    try {
      const response = await fetch(
        `${API_BASE_URL}/games/${taskId}/vote`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            user_id: userId,
            target_player_id: voteTarget,
            round_no: gameState?.round_no ?? null,
          }),
        },
      );
      if (!response.ok) throw new Error(await response.text());
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
      pushActivity(
        `Human vote submitted for ${selectedTarget}.`,
        new Date().toISOString(),
      );
    } catch (error) {
      setErrorText(
        error instanceof Error ? error.message : "Failed to submit vote.",
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
    if (
      !taskId ||
      !nightTarget ||
      !includeHuman ||
      !humanIsAlive ||
      submittingNightAction
    )
      return;
    setSubmittingNightAction(true);
    const selectedTarget =
      gameStateRef.current?.players_omniscient.find(
        (p) => p.player_id === nightTarget,
      )?.display_name ??
      gameStateRef.current?.players_public.find(
        (p) => p.player_id === nightTarget,
      )?.display_name ??
      nightTarget;
    try {
      const response = await fetch(
        `${API_BASE_URL}/games/${taskId}/night_action`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            user_id: userId,
            target_player_id: nightTarget,
            round_no: gameState?.round_no ?? null,
          }),
        },
      );
      if (!response.ok) throw new Error(await response.text());
      if (typeof gameState?.round_no === "number")
        setNightActionSubmittedRound(gameState.round_no);
      appendThreadMessage(
        "system",
        "System",
        `Night action submitted for ${selectedTarget}.`,
        "system",
      );
      pushActivity(
        `Human night action submitted for ${selectedTarget}.`,
        new Date().toISOString(),
      );
    } catch (error) {
      setErrorText(
        error instanceof Error
          ? error.message
          : "Failed to submit night action.",
      );
    } finally {
      setSubmittingNightAction(false);
    }
  }, [
    appendThreadMessage,
    gameState?.round_no,
    humanIsAlive,
    includeHuman,
    nightTarget,
    pushActivity,
    submittingNightAction,
    taskId,
    userId,
  ]);

  const submitCallVote = useCallback(async () => {
    if (!taskId || !includeHuman || !humanIsAlive || submittingCallVote) return;
    setSubmittingCallVote(true);
    try {
      const response = await fetch(
        `${API_BASE_URL}/games/${taskId}/call_vote`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            user_id: userId,
            round_no: gameState?.round_no ?? null,
          }),
        },
      );
      if (!response.ok) throw new Error(await response.text());
      if (typeof gameState?.round_no === "number")
        setCalledVoteRound(gameState.round_no);
      appendThreadMessage(
        "system",
        "System",
        "You called for a vote.",
        "system",
      );
      pushActivity("Human called for a vote.", new Date().toISOString());
    } catch (error) {
      setErrorText(
        error instanceof Error
          ? error.message
          : "Failed to call vote.",
      );
    } finally {
      setSubmittingCallVote(false);
    }
  }, [
    appendThreadMessage,
    gameState?.round_no,
    humanIsAlive,
    includeHuman,
    pushActivity,
    submittingCallVote,
    taskId,
    userId,
  ]);

  /* -- Derived state ------------------------------------------------- */

  const canViewWolfThread = useMemo(() => {
    return showOmniscient || gameState?.human_private_role === "werewolf";
  }, [gameState?.human_private_role, showOmniscient]);

  useEffect(() => {
    if (selectedChannel === "wolf" && !canViewWolfThread)
      setSelectedChannel("town");
  }, [canViewWolfThread, selectedChannel]);

  useEffect(() => {
    if (!showOmniscient) {
      setSelectedAgentPlayerId(null);
      return;
    }
    if (!selectedAgentPlayerId || !gameState) return;
    const stillVisible = gameState.players_omniscient.some(
      (p) => p.player_id === selectedAgentPlayerId && !p.is_human,
    );
    if (!stillVisible) setSelectedAgentPlayerId(null);
  }, [gameState, selectedAgentPlayerId, showOmniscient]);

  const visiblePlayers = useMemo(() => {
    if (!gameState) return [];
    return showOmniscient
      ? gameState.players_omniscient
      : gameState.players_public;
  }, [gameState, showOmniscient]);

  const aliveVoteTargets = useMemo(
    () =>
      visiblePlayers.filter(
        (p) => p.alive && p.player_id !== humanPlayerId,
      ),
    [humanPlayerId, visiblePlayers],
  );

  const aliveNightTargets = useMemo(
    () =>
      visiblePlayers.filter(
        (p) =>
          p.alive &&
          p.player_id !== humanPlayerId &&
          (p.role ?? "villager") !== "werewolf",
      ),
    [humanPlayerId, visiblePlayers],
  );

  const dayVoteOpen = useMemo(() => {
    if (!gameState?.phase) return false;
    return (
      gameState.phase === "open_day_vote" ||
      gameState.phase === "collect_day_votes"
    );
  }, [gameState?.phase]);

  const nightActionOpen = useMemo(() => {
    if (!gameState?.phase) return false;
    return (
      gameState.phase === "open_night_action" ||
      gameState.phase === "collect_night_actions"
    );
  }, [gameState?.phase]);

  const isNightPhase = useMemo(() => {
    if (!gameState?.phase) return false;
    return gameState.phase.includes("night");
  }, [gameState?.phase]);

  const phaseSecondsRemaining = useMemo(() => {
    const deadline = gameState?.phase_deadline_ts;
    if (typeof deadline !== "number" || !Number.isFinite(deadline))
      return null;
    return Math.max(0, Math.ceil(deadline - nowEpochMs / 1000));
  }, [gameState?.phase_deadline_ts, nowEpochMs]);

  const phaseTimerLabel =
    phaseSecondsRemaining === null
      ? "--:--"
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
    if (!humanPlayerId || !gameState?.round_no) return false;
    return votedRoundByPlayerId[humanPlayerId] === gameState.round_no;
  }, [gameState?.round_no, humanPlayerId, votedRoundByPlayerId]);

  const humanHasSubmittedNightActionThisRound = useMemo(() => {
    if (typeof gameState?.round_no !== "number") return false;
    return nightActionSubmittedRound === gameState.round_no;
  }, [gameState?.round_no, nightActionSubmittedRound]);

  const dayDiscussionOpen = useMemo(() => {
    if (!gameState?.phase) return false;
    return (
      gameState.phase === "open_day_discussion" ||
      gameState.phase === "collect_vote_calls"
    );
  }, [gameState?.phase]);

  const humanHasCalledVoteThisRound = useMemo(() => {
    if (typeof gameState?.round_no !== "number") return false;
    return calledVoteRound === gameState.round_no;
  }, [calledVoteRound, gameState?.round_no]);

  const isGameRunning = !!taskId && !gameState?.winner;
  const canUseHumanActions = isGameRunning && includeHuman && humanIsAlive;

  const activeAgentCount = useMemo(
    () => Object.values(agentStatus).filter((s) => s === "active").length,
    [agentStatus],
  );
  const waitingAgentCount = useMemo(
    () => Object.values(agentStatus).filter((s) => s === "waiting").length,
    [agentStatus],
  );

  const sortedVisiblePlayers = useMemo(
    () =>
      [...visiblePlayers].sort((a, b) => Number(b.alive) - Number(a.alive)),
    [visiblePlayers],
  );

  const selectedAgent = useMemo(() => {
    if (!showOmniscient || !selectedAgentPlayerId || !gameState) return null;
    return (
      gameState.players_omniscient.find(
        (p) => p.player_id === selectedAgentPlayerId && !p.is_human,
      ) ?? null
    );
  }, [gameState, selectedAgentPlayerId, showOmniscient]);

  const selectedAgentThoughts = useMemo(() => {
    if (!selectedAgentPlayerId) return [];
    return thoughtsByPlayerId[selectedAgentPlayerId] ?? [];
  }, [selectedAgentPlayerId, thoughtsByPlayerId]);

  const guidanceText = useMemo(() => {
    if (starting)
      return "Creating the game session and spawning player agents...";
    if (cancelling)
      return "Cancellation in progress. Waiting for game master shutdown.";
    if (!taskId) return "Configure and start a new game to begin.";
    if (gameState?.winner) {
      return gameState.winner_reason
        ? `Game over. ${gameState.winner_reason}`
        : "Game over. Check the final report for details.";
    }
    if (includeHuman && !humanIsAlive)
      return "You have been eliminated. Spectating the remaining game.";
    if (dayVoteOpen && canUseHumanActions) {
      if (humanHasVotedThisRound)
        return "Vote locked in. Waiting for other players...";
      return "Day vote is open. Choose a player to eliminate.";
    }
    if (canSubmitNightAction && canUseHumanActions) {
      if (humanHasSubmittedNightActionThisRound)
        return "Night action submitted. Waiting for resolution...";
      return "Night phase. Select a villager target for the kill.";
    }
    if (isNightPhase)
      return "Night has fallen. Chat is locked while agents resolve actions.";
    if (dayDiscussionOpen && canUseHumanActions) {
      const received = gameState?.vote_calls_received ?? 0;
      const threshold = gameState?.vote_calls_threshold ?? 0;
      if (humanHasCalledVoteThisRound) {
        return `Vote called (${received}/${threshold}). Waiting for others to call or timer to expire.`;
      }
      return `Discuss freely, or call a vote when ready (${received}/${threshold} needed).`;
    }
    if (canUseHumanActions && selectedChannel !== "system") {
      return `Speak freely in ${CHANNEL_LABEL[selectedChannel]}. Convince others or coordinate.`;
    }
    return "Agents are deliberating. Follow the thread and activity updates.";
  }, [
    canSubmitNightAction,
    canUseHumanActions,
    cancelling,
    dayDiscussionOpen,
    dayVoteOpen,
    gameState?.vote_calls_received,
    gameState?.vote_calls_threshold,
    gameState?.winner,
    gameState?.winner_reason,
    humanHasCalledVoteThisRound,
    humanHasSubmittedNightActionThisRound,
    humanHasVotedThisRound,
    humanIsAlive,
    isNightPhase,
    includeHuman,
    selectedChannel,
    starting,
    taskId,
  ]);

  /* ------------------------------------------------------------------ */

  return {
    gameName,
    setGameName,
    includeHuman,
    setIncludeHuman,
    humanName,
    setHumanName,
    humanRolePreference,
    setHumanRolePreference,
    aiPlayerCount,
    setAiPlayerCount,
    dayDiscussionSeconds,
    setDayDiscussionSeconds,
    dayVoteSeconds,
    setDayVoteSeconds,
    nightSeconds,
    setNightSeconds,

    taskId,
    humanPlayerId,
    gameState,
    finalReport,
    humanIsAlive,
    threads,
    activity,
    agentStatus,
    thoughtsByPlayerId,
    votedRoundByPlayerId,

    starting,
    cancelling,
    errorText,
    setErrorText,
    setupModalOpen,
    setSetupModalOpen,
    showFinalReport,
    setShowFinalReport,
    showOmniscient,
    setShowOmniscient,
    showActivity,
    setShowActivity,
    selectedChannel,
    setSelectedChannel,
    chatInput,
    setChatInput,
    voteTarget,
    setVoteTarget,
    nightTarget,
    setNightTarget,
    selectedAgentPlayerId,
    setSelectedAgentPlayerId,
    submittingVote,
    submittingNightAction,
    submittingCallVote,

    canViewWolfThread,
    visiblePlayers,
    sortedVisiblePlayers,
    aliveVoteTargets,
    aliveNightTargets,
    dayDiscussionOpen,
    dayVoteOpen,
    isNightPhase,
    humanHasCalledVoteThisRound,
    phaseTimerLabel,
    isPhaseTimerCritical,
    canSubmitNightAction,
    humanHasVotedThisRound,
    humanHasSubmittedNightActionThisRound,
    isGameRunning,
    canUseHumanActions,
    activeAgentCount,
    waitingAgentCount,
    selectedAgent,
    selectedAgentThoughts,
    guidanceText,

    startGame,
    cancelGame,
    sendHumanChat,
    submitVote,
    submitNightAction,
    submitCallVote,
  };
}
