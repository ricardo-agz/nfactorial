import React, { useState, useEffect, useRef } from 'react';
import { ChevronDown, ChevronUp } from 'lucide-react';
import { ThinkingProgress } from '../types';
import { ToolIcon } from './ToolResult';
import { ToolCall } from '../types';

interface SubAgentCarouselProps {
  taskIds: string[];
  progressMap: Record<string, ThinkingProgress>;
  progressPercent?: number;
}

// New: Animated dot-grid progress bar
const DotProgressBar: React.FC<{ percentage: number; isComplete: boolean }> = ({ percentage, isComplete }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const ROWS = 4; // exactly 5 rows as requested
  const DOT_SIZE = 3; // px
  const GAP = 2; // px (x == y spacing)
  const [cols, setCols] = useState(50);
  const totalDots = ROWS * cols;

  useEffect(() => {
    const updateDots = () => {
      const width = containerRef.current?.clientWidth ?? 300;
      const colWidth = DOT_SIZE + GAP;
      // +GAP so last gap fits nicely
      const newCols = Math.max(10, Math.floor((width + GAP) / colWidth));
      setCols(newCols);
    };

    updateDots();
    window.addEventListener('resize', updateDots);
    return () => window.removeEventListener('resize', updateDots);
  }, []);

  // Internal state used for animation – start at the current percentage so
  // the bar doesn't reset when the component remounts (e.g. after final
  // output arrives)
  const [filled, setFilled] = useState<number>(() => Math.round((percentage / 100) * totalDots));

  // Smoothly animate `filled` towards the latest target.
  // We also speed up the fill by moving several dots per tick based on the
  // remaining distance.
  useEffect(() => {
    const target = Math.round((percentage / 100) * totalDots);

    // Early-exit if we're already at the target
    if (filled === target) return;

    const interval = setInterval(() => {
      setFilled(prev => {
        if (prev === target) {
          clearInterval(interval);
          return prev;
        }

        const diff  = target - prev;
        const step  = Math.sign(diff) * Math.max(1, Math.ceil(Math.abs(diff) / 5)); // move 1-20%/tick
        const next  = prev + step;
        const clamped = diff > 0 ? Math.min(next, target) : Math.max(next, target);
        return clamped;
      });
    }, 14); // faster updates (~70fps)

    return () => clearInterval(interval);
  }, [percentage, totalDots, filled]);

  const dots = Array.from({ length: totalDots }, (_, idx) => {
    const isFilled = idx < filled;
    return (
      <div
        key={idx}
        style={{
          width : `${DOT_SIZE}px`,
          height: `${DOT_SIZE}px`,
          borderRadius: '50%',
          transition: 'background-color 0.2s',
          backgroundColor: isFilled ? (isComplete ? '#22c55e' /* green-500 */ : '#f97316' /* orange-400 */) : 'rgba(75,85,99,0.4)', // gray-600/40
        }}
      />
    );
  });

  return (
    <div className="mb-2 w-full">
      <div
        ref={containerRef}
        style={{
          display: 'grid',
          columnGap: `${GAP}px`,
          rowGap: `${GAP}px`,
          gridTemplateColumns: `repeat(${cols}, ${DOT_SIZE}px)`,
          gridTemplateRows: `repeat(${ROWS}, ${DOT_SIZE}px)`,
          gridAutoFlow: 'column',
        }}
      >
        {dots}
      </div>
      <div className="text-[10px] text-gray-500 mt-1 text-right select-none">
        {isComplete ? 'Completed' : `${percentage}%`}
      </div>
    </div>
  );
};

// Renders a horizontal carousel of square cards showing the thinking progress of
// sub-agent tasks spawned by the `research` tool.
export const SubAgentCarousel: React.FC<SubAgentCarouselProps> = ({ taskIds, progressMap, progressPercent }) => {
  if (!taskIds.length) return null;

  // Calculate overall progress based on how many sub-agent tasks have finished
  const total      = taskIds.length;
  const completed  = taskIds.filter(id => progressMap[id]?.is_complete).length;
  const computedPct = total ? Math.round((completed / total) * 100) : 0;

  const pct = progressPercent !== undefined ? Math.round(progressPercent) : computedPct;
  const isComplete = pct === 100;

  // Visual progress bar using dots
  const progressBar = <DotProgressBar percentage={pct} isComplete={isComplete} />;

  // Collect and deduplicate sources across tasks by URL
  const sourceMap = new Map<string, any>();

  taskIds.forEach(id => {
    const prog = progressMap[id];
    if (!prog) return;

    const ingest = (resArr?: any[]) => {
      if (!resArr) return;
      resArr.forEach(item => {
        if (item && item.url && !sourceMap.has(item.url)) {
          sourceMap.set(item.url, item);
        }
      });
    };

    // Collect from final output (when task finished)
    if (prog.final_output) {
      const { results } = parseFinalOutput(prog.final_output);
      ingest(results);
    }

    // Collect from each completed search call along the way
    Object.values(prog.tool_calls).forEach(call => {
      if (call.tool_name === 'search' && call.status === 'completed' && call.result) {
        const { results } = parseFinalOutput(call.result);
        ingest(results);
      }
    });
  });

  const aggregatedResults = Array.from(sourceMap.values());

  return (
    <div className="mb-2">
      {progressBar}
      <div className="max-h-64 overflow-y-auto py-2 space-y-1 subagent-scrollbar">
        {taskIds.map(id => (
          <TaskItem key={id} id={id} progress={progressMap[id]} />
        ))}
      </div>
      <AggregatedSourcesBar results={aggregatedResults} />
    </div>
  );
};


interface SubAgentCardProps {
  progress?: ThinkingProgress;
  className?: string;
}

const SubAgentCard: React.FC<SubAgentCardProps> = ({ progress, className = '' }) => {
  return (
    <div
      className={`w-56 min-w-[14rem] h-56 bg-white border border-gray-200 rounded-lg shadow-none px-3 py-1 flex flex-col ${className}`}
    >
      {progress ? (
        <div className="flex-1 overflow-y-auto space-y-2 subagent-scrollbar">
          {/* Search query (if available) */}
          {(() => {
            const searchCall = Object.values(progress.tool_calls)
              .filter(c => c.tool_name === 'search')
              .find(c => c.status === 'completed' && c.result);
            if (!searchCall) return null;

            // Parse arguments which may come as JSON string
            let q: string | undefined;
            try {
              const args = typeof searchCall.arguments === 'string' ? JSON.parse(searchCall.arguments) : searchCall.arguments;
              q = args?.query;
            } catch {
              /* noop */
            }

            if (!q) return null;

            return (
              <div className="mb-3">
                <div className="text-xs font-semibold text-gray-800 truncate" title={q}>
                  {q}
                </div>
                {/* Favicons */}
                {renderFaviconsRow(searchCall?.result ?? progress.final_output)}
              </div>
            );
          })()}

          {/* Tool call status list */}
          {Object.values(progress.tool_calls).map(call => (
            <ToolCallStatus key={call.id} call={call} />
          ))}

          {/* Compact search results preview & findings when complete */}
          {progress.is_complete && progress.final_output && (
            <div className="space-y-1">
              <SearchResultsPreview finalOutput={progress.final_output} />
              <FindingsDisplay finalOutput={progress.final_output} />
            </div>
          )}

          {/* Error */}
          {progress.error && (
            <div className="text-xs text-red-600">{progress.error}</div>
          )}
        </div>
      ) : (
        <div className="flex items-center justify-center h-full text-xs text-gray-500">
          Waiting for updates…
        </div>
      )}
    </div>
  );
};

// NEW: Component to render individual tool call with status dot & icon
interface ToolCallStatusProps {
  call: ToolCall;
}

const ToolCallStatus: React.FC<ToolCallStatusProps> = ({ call }) => {
  // Extract search query snippet if relevant
  let querySnippet: string | undefined;
  if (call.tool_name === 'search' && call.arguments) {
    try {
      const parsed = typeof call.arguments === 'string' ? JSON.parse(call.arguments) : call.arguments;
      if (parsed?.query) {
        querySnippet = parsed.query as string;
      }
    } catch {
      /* ignore */
    }
  }

  return (
    <div className="flex items-center gap-1 text-xs overflow-hidden">
      {/* Status dot */}
      <span
        className={`inline-block w-1 h-1 mr-1 rounded-full flex-shrink-0 ${
          call.status === 'completed'
            ? 'bg-green-500'
            : call.status === 'failed'
            ? 'bg-red-500'
            : 'bg-yellow-400'
        }`}
      />

      {/* Tool icon */}
      <div className="flex-shrink-0 w-4 h-4 flex items-center justify-center">
        <ToolIcon name={call.tool_name} />
      </div>

      {/* Name + optional query */}
      <div className="min-w-0 truncate text-gray-800">
        <span className="capitalize">
          {call.tool_name}
          {querySnippet ? ': ' : ''}
        </span>
        {querySnippet && (
          <span className="truncate" title={querySnippet}>
            {querySnippet}
          </span>
        )}
      </div>
    </div>
  );
};

// NEW: centralized parser to avoid duplicate logic across helpers
const parseFinalOutput = (finalOutput: any): { results?: any[]; findings?: string[] } => {
  if (!finalOutput) return {};

  let parsed: any = finalOutput;
  if (typeof parsed === 'string') {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      return {};
    }
  }

  // unwrap nested final_output field if present
  if (!Array.isArray(parsed) && parsed?.final_output !== undefined) {
    parsed = parsed.final_output;
    if (typeof parsed === 'string') {
      try {
        parsed = JSON.parse(parsed);
      } catch {
        return {};
      }
    }
  }

  const results = Array.isArray(parsed)
    ? parsed
    : Array.isArray(parsed?.results)
    ? parsed.results
    : undefined;

  const findings = Array.isArray(parsed?.findings)
    ? (parsed.findings as string[])
    : Array.isArray(parsed)
    ? (parsed as string[])
    : undefined;

  return { results, findings };
};

// Extract favicons row now reusing parseFinalOutput helper
const renderFaviconsRow = (finalOutput: any): React.ReactElement | null => {
  const { results } = parseFinalOutput(finalOutput);
  if (!results) return null;

  return (
    <div className="flex gap-1 flex-wrap mt-1 mb-1">
      {results.slice(0, 6).map((item: any, idx: number) => {
        try {
          const host = new URL(item.url).hostname;
          return (
            <img
              key={idx}
              src={`https://www.google.com/s2/favicons?domain=${host}&sz=64`}
              alt=""
              className="w-4 h-4"
            />
          );
        } catch {
          return null;
        }
      })}
    </div>
  );
};

// Update SearchResultsPreview to leverage parser
interface PreviewProps {
  finalOutput: any;
}

const SearchResultsPreview: React.FC<PreviewProps> = ({ finalOutput }) => {
  const { results } = parseFinalOutput(finalOutput);
  if (!results) return null;

  return (
    <div className="text-[10px] text-gray-500 mt-1">{results.length} results found</div>
  );
};

// Update FindingsDisplay to leverage parser
const FindingsDisplay: React.FC<PreviewProps> = ({ finalOutput }) => {
  const { findings } = parseFinalOutput(finalOutput);
  if (!findings || findings.length === 0) return null;

  return (
    <ul className="list-disc list-inside text-[10px] text-gray-700 space-y-0.5 mt-1">
      {findings.slice(0, 3).map((f, idx) => (
        <li key={idx} className="truncate" title={f}>{f}</li>
      ))}
      {findings.length > 3 && (
        <li className="text-gray-500">…and {findings.length - 3} more</li>
      )}
    </ul>
  );
};

// Replace horizontal carousel with vertical list and aggregated sources display.
// 1. Add utility to aggregate results across tasks
// 2. Introduce ThinTaskCard component for compact vertical layout
// 3. Render AggregatedSourcesBar at the bottom of the carousel
//
// === 1. Utility to aggregate results ===
interface AggregatedSourcesProps {
  results: any[];
}

function AggregatedSourcesBar({ results }: AggregatedSourcesProps) {
  if (!results || results.length === 0) return null;

  // Collect unique hostnames for favicons (up to 3)
  const uniqueHosts: string[] = [];
  for (const item of results) {
    try {
      const host = new URL(item.url).hostname;
      if (!uniqueHosts.includes(host)) uniqueHosts.push(host);
    } catch {
      /* ignore bad urls */
    }
    if (uniqueHosts.length >= 3) break;
  }

  const count = results.length;

  return (
    <div className="flex items-center gap-1 mt-1 text-[10px] text-gray-700 select-none ml-1">
      {uniqueHosts.map((host, idx) => (
        <img
          key={idx}
          src={`https://www.google.com/s2/favicons?domain=${host}&sz=64`}
          alt=""
          className="w-4 h-4"
        />
      ))}
      <span className="ml-1">{count} sources</span>
    </div>
  );
}

// === 2. Compact vertical card component ===
interface ThinTaskCardProps {
  progress?: ThinkingProgress;
  rightElement?: React.ReactNode;
  containerClassName?: string;
}

function ThinTaskCard({ progress, rightElement, containerClassName = '' }: ThinTaskCardProps) {
  // Determine current activity & status label
  let label = 'Thinking…';

  // Track call references outside conditional scope
  let ongoing: ToolCall | undefined;
  let failed: ToolCall | undefined;
  // completedCalls no longer used but kept for potential future metrics

  if (progress) {
    const calls = Object.values(progress.tool_calls);
    ongoing = calls.find(c => c.status === 'started');
    failed = calls.find(c => c.status === 'failed');
    // completedCalls = calls.filter(c => c.status === 'completed'); // This line was removed

    if (failed) {
      label = `Error: ${failed.tool_name}`;
    } else if (ongoing) {
      label = 'Searching';
      // show query snippet for search
      if (ongoing.tool_name === 'search' && ongoing.arguments) {
        try {
          const args = typeof ongoing.arguments === 'string' ? JSON.parse(ongoing.arguments) : ongoing.arguments;
          if (args?.query) {
            label += `: ${args.query}`;
          }
        } catch {
          /* ignore */
        }
      }
    } else if (progress.is_complete) {
      label = 'Agent completed';
    } else {
      // No ongoing work yet (or between calls) – thinking
      label = 'Thinking…';
    }
  }

  // limit label length to prevent overflow
  const displayLabel = label.length > 60 ? `${label.slice(0, 57)}…` : label;

  // Tool icon determination
  const iconName = (() => {
    if (failed) return 'search'; // fallback icon for failed tool
    if (ongoing) {
      // Only show the tool icon in the label area when it is *not* already
      // rendered as the left-hand indicator.
      return ongoing.tool_name === 'search' ? '' : ongoing.tool_name;
    }
    return ''; // thinking / complete states rely solely on 4-dot indicator
  })();

  /* === New 4-dot indicator component === */
  const FourDotIndicator: React.FC<{ animating: boolean }> = ({ animating }) => {
    const DOT       = 3; // px (match progress bar)
    const GAP       = 2; // px (match progress bar)
    const delayStep = 0.15; // s

    return (
      <div
        className="grid grid-cols-2 grid-rows-2 flex-shrink-0"
        style={{ gap: `${GAP}px`, width: `${DOT * 2 + GAP}px`, height: `${DOT * 2 + GAP}px` }}
      >
        {Array.from({ length: 4 }).map((_, idx) => (
          <div
            key={idx}
            className={`${animating ? 'spinner-dot' : 'bg-gray-800'} rounded-full`}
            style={{
              width : `${DOT}px`,
              height: `${DOT}px`,
              animationDelay: animating ? `${idx * delayStep}s` : undefined,
            }}
          />
        ))}
      </div>
    );
  };

  return (
    <div className={`w-full bg-white border border-gray-200 rounded-md px-2 py-1 flex items-center gap-2 text-xs min-h-[2rem] ${containerClassName}`}>
      {/* Left status indicator (fixed width for consistent spacing) */}
      <div className="w-4 h-4 flex items-center justify-center flex-shrink-0">
        {failed ? (
          <span className="inline-block w-2 h-2 rounded-full bg-red-500" />
        ) : ongoing && ongoing.tool_name === 'search' ? (
          <ToolIcon name="search" />
        ) : progress?.is_complete ? (
          <FourDotIndicator animating={false} />
        ) : (
          <FourDotIndicator animating={true} />
        )}
      </div>

      {/* Optional icon label when not showing search indicator */}
      {iconName && (
        <div className="w-4 h-4 flex-shrink-0">
          <ToolIcon name={iconName} />
        </div>
      )}

      <span className="truncate min-w-0 flex-1 text-gray-800" title={label}>{displayLabel}</span>
      {rightElement && (
        <div className="flex-shrink-0 ml-2">
          {rightElement}
        </div>
      )}
    </div>
  );
}

// === 3. TaskItem accordion component ===
interface TaskItemProps {
  id: string;
  progress?: ThinkingProgress;
}

function TaskItem({ id, progress }: TaskItemProps) {
  const [open, setOpen] = useState(false);

  return (
    <div key={id} className="bg-white border border-gray-200 rounded-md overflow-hidden">
      <ThinTaskCard
        progress={progress}
        containerClassName="border-none rounded-none"
        rightElement={
          <button onClick={() => setOpen(prev => !prev)} aria-label={open ? 'Collapse details' : 'Expand details'} className="cursor-pointer">
            {open ? <ChevronUp className="w-3 h-3 text-gray-500" /> : <ChevronDown className="w-3 h-3 text-gray-500" />}
          </button>
        }
      />
      {open && (
        <div className="border-t border-gray-200 p-2">
          <SubAgentCard progress={progress} className="w-full min-w-0 h-auto border-none shadow-none rounded-none p-0" />
        </div>
      )}
    </div>
  );
} 