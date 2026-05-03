import { Play, Shield, X } from "lucide-react";

interface FinalReportModalProps {
  finalReport: string;
  onClose: () => void;
  onNewGame: () => void;
}

export function FinalReportModal({
  finalReport,
  onClose,
  onNewGame,
}: FinalReportModalProps) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-6">
      <div
        className="absolute inset-0 bg-black/60 backdrop-blur-sm"
        onClick={onClose}
      />
      <div className="animate-scale-in relative z-10 w-full max-w-2xl rounded-xl border border-neutral-800 bg-neutral-950 p-6 shadow-2xl">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="flex items-center gap-2 text-base font-semibold text-white">
            <Shield className="h-5 w-5 text-emerald-400" />
            Game Report
          </h2>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg p-1.5 text-neutral-500 transition hover:bg-neutral-800 hover:text-white"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        <pre className="max-h-[60vh] overflow-auto whitespace-pre-wrap rounded-lg bg-neutral-900 p-4 text-sm text-neutral-300">
          {finalReport}
        </pre>
        <div className="mt-4 flex justify-end">
          <button
            type="button"
            onClick={onNewGame}
            className="inline-flex items-center gap-2 rounded-lg bg-neutral-800 px-4 py-2 text-sm font-medium text-white transition hover:bg-neutral-700"
          >
            <Play className="h-4 w-4" />
            New Game
          </button>
        </div>
      </div>
    </div>
  );
}
