import React from 'react';
import { ThinkingProgress } from '../types';
import { ToolIcon } from './ToolResult';
import { ToolCall } from '../types';

interface SubAgentCarouselProps {
  taskIds: string[];
  progressMap: Record<string, ThinkingProgress>;
}

// Renders a horizontal carousel of square cards showing the thinking progress of
// sub-agent tasks spawned by the `research` tool.
export const SubAgentCarousel: React.FC<SubAgentCarouselProps> = ({ taskIds, progressMap }) => {
  if (!taskIds.length) return null;

  return (
    <div className="overflow-x-auto py-2 subagent-scrollbar">
      <div className="flex gap-3">
        {taskIds.map(id => (
          <SubAgentCard key={id} progress={progressMap[id]} />
        ))}
      </div>
    </div>
  );
};


interface SubAgentCardProps {
  progress?: ThinkingProgress;
}

const SubAgentCard: React.FC<SubAgentCardProps> = ({ progress }) => {
  return (
    <div
      className="w-56 min-w-[14rem] h-56 bg-white border border-gray-200 rounded-lg shadow-sm p-2 flex flex-col"
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
              <div>
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
      <div className="flex-shrink-0 w-4 h-4">
        <ToolIcon name={call.tool_name} />
      </div>

      {/* Name + optional query */}
      <div className="min-w-0 truncate">
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