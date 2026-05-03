import React, { useState } from 'react';
import { ChevronDown, ChevronRight } from 'lucide-react';
import { ThinkingProgress } from '../types';
import { ToolIcon, ToolArguments, ToolResultDisplay } from './ToolResult';
import { SubAgentCarousel } from './SubAgentCarousel';

interface ThinkingDropdownProps {
  thinking: ThinkingProgress;
  subAgentProgress?: Record<string, ThinkingProgress>;
  researchProgress?: number;
}

export const ThinkingDropdown: React.FC<ThinkingDropdownProps> = ({
  thinking,
  subAgentProgress,
  researchProgress,
}) => {
  const [open, setOpen] = useState(true);
  const calls = Object.values(thinking.tool_calls);
  const hasActive = calls.some(c => c.status === 'started');

  // Helper to render bullet-point representation for reflect / plan
  const renderBulletRow = (text: string, key?: React.Key, indent: number = 0) => {
    // Tailwind needs literal class names; map indent -> margin class explicitly
    const mlClass = indent === 0 ? 'ml-3' : indent === 1 ? 'ml-6' : `ml-9`;
    return (
      <div key={key} className={`flex gap-2 items-baseline ${mlClass}`}>
        <div className="w-1 h-1 rounded-full bg-gray-600 mt-0.5 flex-shrink-0 mr-1" />
        <span className="text-xs text-gray-700 truncate" title={text}>{text}</span>
      </div>
    );
  };

  const renderBulletForCall = (call: typeof calls[number]) => {
    if (call.tool_name === 'reflect') {
      const text = typeof call.result === 'string' ? call.result : JSON.stringify(call.result);
      return renderBulletRow(text, call.id);
    }

    if (call.tool_name === 'plan') {
      let overview: string | undefined;
      const steps: string[] = [];

      if (typeof call.result === 'object' && call.result) {
        const obj: any = call.result;
        if (typeof obj.overview === 'string') overview = obj.overview;
        if (Array.isArray(obj.steps)) steps.push(...obj.steps.map((s: any) => String(s)));
      }

      // Render overview (indent 0) and each step (indent 1). If overview/steps not available, fall back to entire result.
      return (
        <div key={call.id} className="space-y-1">
          {overview && renderBulletRow(overview, 'overview', 0)}
          {steps.map((s, idx) => renderBulletRow(s, `step-${idx}`, 1))}
          {!overview && !steps.length && (
            renderBulletRow(
              typeof call.result === 'string' ? call.result : JSON.stringify(call.result),
              'fallback',
              0,
            )
          )}
        </div>
      );
    }

    return null;
  };

  return (
    <div className="bg-gray-50 rounded-lg p-3 mb-3 border border-gray-200">
      <button
        className="flex items-center gap-2 w-full text-left group cursor-pointer"
        onClick={() => setOpen(!open)}
      >
        {open ? (
          <ChevronDown className="w-4 h-4 text-gray-400" />
        ) : (
          <ChevronRight className="w-4 h-4 text-gray-400" />
        )}
        <span className="text-xs font-normal text-gray-700">
          {thinking.is_complete ? 'Thinking complete' : 'Thinking...'}
        </span>
        {hasActive && (
          <div className="w-1.5 h-1.5 bg-blue-500 rounded-full animate-pulse" />
        )}
      </button>

      {open && (
        <div className="mt-3 space-y-2">

          {calls.map(call => (
            ['reflect', 'plan'].includes(call.tool_name) && call.status === 'completed' ? (
              renderBulletForCall(call)
            ) : (
            <div
              key={call.id}
              className={`rounded p-2 ${call.tool_name === 'search' ? '' : 'bg-white border border-gray-200'}`}
            >
              <div className={`flex gap-2 ${call.tool_name === 'search' ? 'items-baseline' : 'items-start'}`}>
                <div className={`flex-shrink-0 ${call.tool_name === 'search' ? '' : 'mt-1'}`}>
                  <ToolIcon name={call.tool_name} />
                </div>
                <div className="flex-1 min-w-0">
                  {call.tool_name === 'search' ? (
                    (() => {
                      let querySnippet: string | undefined;
                      try {
                        const parsed = typeof call.arguments === 'string' ? JSON.parse(call.arguments) : call.arguments;
                        if (call.tool_name === 'search' && parsed?.query) querySnippet = parsed.query as string;
                      } catch {/* ignore */}

                      if (call.tool_name === 'search') {
                        const labelBase = call.status === 'completed'
                          ? 'Searched'
                          : call.status === 'failed'
                          ? 'Search failed'
                          : 'Searching';

                        return (
                          <div className="text-xs font-medium text-gray-700 truncate leading-none" title={querySnippet ? `${labelBase}: ${querySnippet}` : labelBase}>
                            {labelBase}
                            {querySnippet ? `: "${querySnippet}"` : ''}
                          </div>
                        );
                      }

                      // Research label logic
                      if (call.tool_name === 'research') {
                        // Parse queries to build label
                        let queries: string[] = [];
                        try {
                          const parsed = typeof call.arguments === 'string' ? JSON.parse(call.arguments) : call.arguments;
                          queries = Array.isArray(parsed)
                            ? parsed
                            : Array.isArray(parsed?.queries)
                              ? parsed.queries as string[]
                              : [];
                        } catch {/* ignore */}

                        const count     = queries.length || 1;
                        const preview   = queries[0] ?? '';
                        const remaining = count - 1;

                        const labelBase = call.status === 'completed'
                          ? 'Researched'
                          : call.status === 'failed'
                          ? 'Research failed'
                          : 'Researching';

                        const label = `${labelBase} ${count} topic${count === 1 ? '' : 's'}`;

                        return (
                          <div className="text-xs font-medium text-gray-700 truncate leading-none" title={preview ? `${label}: ${preview}` : label}>
                            {label}
                            {preview && `: "${preview}"`}
                            {remaining > 0 && ` + ${remaining} more`}
                          </div>
                        );
                      }

                      return null; // fallback shouldn't occur
                    })()
                  ) : (
                    <>
                      <div className="flex items-center gap-2 mb-1">
                        <span className="text-xs font-medium capitalize text-gray-700">
                          {call.tool_name}
                        </span>
                        {call.status !== 'completed' && call.tool_name !== 'research' && (
                          <span
                            className={`text-xs px-1.5 py-0.5 rounded font-medium ${
                              call.status === 'failed' ? 'bg-red-100 text-red-700' : 'bg-blue-100 text-blue-700'
                            }`}
                          >
                            {call.status}
                          </span>
                        )}
                      </div>

                      <div className="text-xs text-gray-500 mb-2">
                        {call.tool_name === 'research' && call.status === 'completed' ? (
                          (() => {
                            let queries: string[] = [];
                            try {
                              const parsed = typeof call.arguments === 'string' ? JSON.parse(call.arguments) : call.arguments;
                              queries = Array.isArray(parsed)
                                ? parsed
                                : Array.isArray(parsed?.queries)
                                  ? parsed.queries as string[]
                                  : [];
                            } catch {/* ignore */}

                            const count     = queries.length || 1;
                            const preview   = queries[0] ?? '';
                            const remaining = count - 1;

                            return (
                              <span>
                                {`Researched ${count} topic${count === 1 ? '' : 's'}`}
                                {preview && `: "${preview}"`}
                                {remaining > 0 && ` + ${remaining} more`}
                              </span>
                            );
                          })()
                        ) : (
                          <ToolArguments name={call.tool_name} args={call.arguments} />
                        )}
                      </div>
                    </>
                  )}

                  {call.status === 'completed' && call.result && (
                    <div>
                      {call.tool_name === 'research' && Array.isArray(call.result) && subAgentProgress ? (
                        <SubAgentCarousel
                          taskIds={call.result as string[]}
                          progressMap={subAgentProgress}
                          progressPercent={researchProgress}
                        />
                      ) : (
                        <ToolResultDisplay name={call.tool_name} result={call.result} />
                      )}
                    </div>
                  )}

                  {call.status === 'failed' && call.error && (
                    <div className="text-xs text-red-600 p-1 bg-red-50 border border-red-200 rounded">
                      {call.error}
                    </div>
                  )}
                </div>
              </div>
            </div>
            )
          ))}

          {thinking.error && (
            <div className="p-2 bg-red-50 border border-red-200 rounded text-xs text-red-700">
              {thinking.error}
            </div>
          )}
        </div>
      )}
    </div>
  );
}; 