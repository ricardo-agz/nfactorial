import React, { useState } from 'react';
import { ChevronDown, ChevronRight } from 'lucide-react';
import { ThinkingProgress } from '../types';
import { ToolIcon, ToolArguments, ToolResultDisplay } from './ToolResult';
import { SubAgentCarousel } from './SubAgentCarousel';

interface ThinkingDropdownProps {
  thinking: ThinkingProgress;
  subAgentProgress?: Record<string, ThinkingProgress>;
}

export const ThinkingDropdown: React.FC<ThinkingDropdownProps> = ({
  thinking,
  subAgentProgress,
}) => {
  const [open, setOpen] = useState(true);
  const calls = Object.values(thinking.tool_calls);
  const hasActive = calls.some(c => c.status === 'started');

  return (
    <div className="bg-gray-50 rounded-lg p-3 mb-3 border border-gray-200">
      <button
        className="flex items-center gap-2 w-full text-left group"
        onClick={() => setOpen(!open)}
      >
        {open ? (
          <ChevronDown className="w-4 h-4 text-gray-400" />
        ) : (
          <ChevronRight className="w-4 h-4 text-gray-400" />
        )}
        <span className="text-sm font-medium text-gray-700">
          {thinking.is_complete ? 'Thinking complete' : 'Thinking...'}
        </span>
        {hasActive && (
          <div className="w-1.5 h-1.5 bg-blue-500 rounded-full animate-pulse" />
        )}
      </button>

      {open && (
        <div className="mt-3 space-y-2">
          {calls.map(call => (
            <div
              key={call.id}
              className="bg-white rounded border border-gray-200 p-2"
            >
              <div className="flex items-start gap-2">
                <div className="flex-shrink-0 mt-1">
                  <ToolIcon name={call.tool_name} />
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-xs font-medium capitalize text-gray-700">
                      {call.tool_name}
                    </span>
                    <span
                      className={`text-xs px-1.5 py-0.5 rounded font-medium ${
                        call.status === 'completed'
                          ? 'bg-green-100 text-green-700'
                          : call.status === 'failed'
                          ? 'bg-red-100 text-red-700'
                          : 'bg-blue-100 text-blue-700'
                      }`}
                    >
                      {call.status}
                    </span>
                  </div>

                  <div className="text-xs text-gray-500 mb-2">
                    <ToolArguments name={call.tool_name} args={call.arguments} />
                  </div>

                  {call.status === 'completed' && call.result && (
                    <div>
                      {call.tool_name === 'research' && Array.isArray(call.result) && subAgentProgress ? (
                        <SubAgentCarousel taskIds={call.result as string[]} progressMap={subAgentProgress} />
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