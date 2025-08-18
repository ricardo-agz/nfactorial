type AnyRecord = Record<string, unknown>;

export const isRecord = (value: unknown): value is AnyRecord => typeof value === 'object' && value !== null;

export type ToolCallFunction = { name: string; arguments?: unknown };
export type ToolCallEvent = { id: string; function: ToolCallFunction };
export type ToolCompletedResult = { tool_call: ToolCallEvent; output_data?: unknown };

/**
 * Extracts the tool call object from a tool action Started or Failed event payload (event.data).
 */
export const extractToolCallFromActionStartedOrFailed = (eventData: unknown): ToolCallEvent | null => {
  if (!isRecord(eventData)) return null;
  const args = Array.isArray(eventData.args) ? eventData.args : null;
  const tc = args && isRecord(args[0]) ? args[0] : null;
  if (!tc || typeof tc.id !== 'string' || !isRecord(tc.function) || typeof tc.function.name !== 'string') {
    return null;
  }
  return { id: tc.id, function: { name: tc.function.name as string, arguments: (tc.function as AnyRecord).arguments } };
};

/**
 * Extracts the completed tool action result payload from a Completed event payload (event.data).
 */
export const extractToolActionCompletedResult = (eventData: unknown): ToolCompletedResult | null => {
  if (!isRecord(eventData)) return null;
  const result = isRecord(eventData.result) ? (eventData.result as AnyRecord) : null;
  if (!result) return null;
  const toolCallRaw = isRecord(result.tool_call) ? (result.tool_call as AnyRecord) : null;
  if (!toolCallRaw || typeof toolCallRaw.id !== 'string') return null;
  const fn = isRecord(toolCallRaw.function) ? (toolCallRaw.function as AnyRecord) : null;
  if (!fn || typeof fn.name !== 'string') return null;
  return {
    tool_call: { id: toolCallRaw.id as string, function: { name: fn.name as string, arguments: fn.arguments } },
    output_data: result.output_data,
  };
};


