import { ActivityPanel } from "./components/ActivityPanel";
import { ChatPanel } from "./components/ChatPanel";
import { FinalReportModal } from "./components/FinalReportModal";
import { Header } from "./components/Header";
import { SetupModal } from "./components/SetupModal";
import { Sidebar } from "./components/Sidebar";
import { useGameEngine } from "./hooks/useGameEngine";

export default function App() {
  const g = useGameEngine();

  const bgClass = !g.isGameRunning
    ? "bg-[#09090b]"
    : g.isNightPhase
      ? "bg-linear-to-b from-violet-950/35 via-[#09090b] to-[#09090b]"
      : "bg-linear-to-b from-amber-950/20 via-[#09090b] to-[#09090b]";

  return (
    <div
      className={`flex h-screen flex-col overflow-hidden text-neutral-100 ${bgClass} transition-colors duration-700`}
    >
      <Header
        gameName={g.gameName}
        gameState={g.gameState}
        isNightPhase={g.isNightPhase}
        starting={g.starting}
        cancelling={g.cancelling}
        isGameRunning={g.isGameRunning}
        phaseTimerLabel={g.phaseTimerLabel}
        isPhaseTimerCritical={g.isPhaseTimerCritical}
        showOmniscient={g.showOmniscient}
        showActivity={g.showActivity}
        guidanceText={g.guidanceText}
        errorText={g.errorText}
        onToggleOmniscient={() => g.setShowOmniscient((v) => !v)}
        onToggleActivity={() => g.setShowActivity((v) => !v)}
        onOpenSetup={() => g.setSetupModalOpen(true)}
        onDismissError={() => g.setErrorText(null)}
        onCancel={() => void g.cancelGame()}
      />

      <div className="flex min-h-0 flex-1 flex-col">
        <div className="flex min-h-0 flex-1">
          <Sidebar
            includeHuman={g.includeHuman}
            gameState={g.gameState}
            humanPlayerId={g.humanPlayerId}
            humanIsAlive={g.humanIsAlive}
            showOmniscient={g.showOmniscient}
            isNightPhase={g.isNightPhase}
            sortedVisiblePlayers={g.sortedVisiblePlayers}
            selectedAgentPlayerId={g.selectedAgentPlayerId}
            votedRoundByPlayerId={g.votedRoundByPlayerId}
            selectedAgent={g.selectedAgent}
            selectedAgentThoughts={g.selectedAgentThoughts}
            finalReport={g.finalReport}
            onSelectAgent={g.setSelectedAgentPlayerId}
            onShowFinalReport={() => g.setShowFinalReport(true)}
          />

          <ChatPanel
            selectedChannel={g.selectedChannel}
            threads={g.threads}
            canViewWolfThread={g.canViewWolfThread}
            activeAgentCount={g.activeAgentCount}
            waitingAgentCount={g.waitingAgentCount}
            isNightPhase={g.isNightPhase}
            canUseHumanActions={g.canUseHumanActions}
            chatInput={g.chatInput}
            dayVoteOpen={g.dayVoteOpen}
            humanHasVotedThisRound={g.humanHasVotedThisRound}
            voteTarget={g.voteTarget}
            aliveVoteTargets={g.aliveVoteTargets}
            submittingVote={g.submittingVote}
            dayDiscussionOpen={g.dayDiscussionOpen}
            humanHasCalledVoteThisRound={g.humanHasCalledVoteThisRound}
            submittingCallVote={g.submittingCallVote}
            voteCallsReceived={g.gameState?.vote_calls_received ?? 0}
            voteCallsThreshold={g.gameState?.vote_calls_threshold ?? 0}
            canSubmitNightAction={g.canSubmitNightAction}
            humanHasSubmittedNightActionThisRound={
              g.humanHasSubmittedNightActionThisRound
            }
            nightTarget={g.nightTarget}
            aliveNightTargets={g.aliveNightTargets}
            submittingNightAction={g.submittingNightAction}
            onSelectChannel={g.setSelectedChannel}
            onChatInputChange={g.setChatInput}
            onSendChat={() => void g.sendHumanChat()}
            onVoteTargetChange={g.setVoteTarget}
            onSubmitVote={() => void g.submitVote()}
            onNightTargetChange={g.setNightTarget}
            onSubmitNightAction={() => void g.submitNightAction()}
            onCallVote={() => void g.submitCallVote()}
          />
        </div>

        {g.showActivity && <ActivityPanel activity={g.activity} />}
      </div>

      {g.showFinalReport && g.finalReport && (
        <FinalReportModal
          finalReport={g.finalReport}
          onClose={() => g.setShowFinalReport(false)}
          onNewGame={() => {
            g.setShowFinalReport(false);
            g.setSetupModalOpen(true);
          }}
        />
      )}

      {g.setupModalOpen && (
        <SetupModal
          gameName={g.gameName}
          includeHuman={g.includeHuman}
          humanName={g.humanName}
          humanRolePreference={g.humanRolePreference}
          aiPlayerCount={g.aiPlayerCount}
          dayDiscussionSeconds={g.dayDiscussionSeconds}
          dayVoteSeconds={g.dayVoteSeconds}
          nightSeconds={g.nightSeconds}
          starting={g.starting}
          cancelling={g.cancelling}
          isGameRunning={g.isGameRunning}
          errorText={g.errorText}
          onGameNameChange={g.setGameName}
          onIncludeHumanChange={g.setIncludeHuman}
          onHumanNameChange={g.setHumanName}
          onHumanRolePreferenceChange={g.setHumanRolePreference}
          onAiPlayerCountChange={g.setAiPlayerCount}
          onDayDiscussionSecondsChange={g.setDayDiscussionSeconds}
          onDayVoteSecondsChange={g.setDayVoteSeconds}
          onNightSecondsChange={g.setNightSeconds}
          onClose={() => g.setSetupModalOpen(false)}
          onStart={() => void g.startGame()}
        />
      )}
    </div>
  );
}
