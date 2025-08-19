<example>
# EXAMPLE: useAgentEvents (frontend)
A custom hook that makes it easy to subscribe to websocket agent events and add handlers for different events. Unless the user has
specifically requested an alternative way to subscribe to the realtime agent events, you should implement this hook to subscribe 
to the agent realtime websocket events. Unless you have a reason to deviate from it, it is best to copy this file line by line.

e.g. /ui/hooks/useAgentEvents.tsx
```typescript
import { useEffect, useRef, useCallback, useMemo, useState } from 'react';
import type { DefaultEventMap } from '../types/events';

export type EventHandler<Evt> = (event: Evt) => void | Promise<void>;

export interface EventSubscription<EventsMap> {
  eventType: keyof EventsMap | Array<keyof EventsMap> | '*';
  agentName?: string | string[];
  handler: EventHandler<EventsMap[keyof EventsMap]>;
}

export interface UseAgentEventsConfig {
  websocketUrl: string;
  token?: string;
  enabled?: boolean;
  reconnectAttempts?: number;
  reconnectDelay?: number;
  onConnect?: () => void;
  onDisconnect?: () => void;
  onError?: (error: Event) => void;
}

export interface AgentEventsApi<EventsMap> {
  subscribe: (subscription: EventSubscription<EventsMap>) => () => void;
  unsubscribe: (eventType: keyof EventsMap | '*', agentName?: string) => void;
  isConnected: boolean;
  reconnect: () => void;
  disconnect: () => void;
}

/**
 * Core hook for managing WebSocket connections and event subscriptions
 * Provides a clean API for subscribing to specific agent events.
 * Designed to be reusable across different agent applications.
 */
export const useAgentEvents = <EventsMap extends Record<string, unknown> = DefaultEventMap>(
  config: UseAgentEventsConfig
): AgentEventsApi<EventsMap> => {
  const {
    websocketUrl,
    token,
    enabled = true,
    reconnectAttempts = 3,
    reconnectDelay = 1000,
    onConnect,
    onDisconnect,
    onError,
  } = config;

  const wsRef = useRef<WebSocket | null>(null);
  const subscriptionsRef = useRef<Map<string, EventSubscription<EventsMap>[]>>(new Map());
  const [isConnected, setIsConnected] = useState(false);
  const reconnectCountRef = useRef(0);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const getSubscriptionKey = (eventType: string, agentName?: string): string => {
    return agentName ? `${agentName}:${eventType}` : eventType;
  };

  const handleMessage = useCallback((event: MessageEvent) => {
    try {
      const rawEvent = JSON.parse(event.data) as unknown;
      const base = rawEvent as { event_type: string; agent_name?: string };
      const event_type = base.event_type;
      const agent_name = base.agent_name;

      // Exact agent:event match
      if (agent_name) {
        const exactKey = getSubscriptionKey(event_type, agent_name);
        const exactSubs = subscriptionsRef.current.get(exactKey) || [];
        exactSubs.forEach(sub => {
          Promise.resolve(sub.handler(rawEvent as EventsMap[keyof EventsMap])).catch(err => {
            console.error('Agent event handler error (exact):', err);
          });
        });
      }

      // Any agent with this event type
      const eventKey = getSubscriptionKey(event_type);
      const eventSubs = subscriptionsRef.current.get(eventKey) || [];
      eventSubs.forEach(sub => {
        Promise.resolve(sub.handler(rawEvent as EventsMap[keyof EventsMap])).catch(err => {
          console.error('Agent event handler error (event):', err);
        });
      });

      // Wildcard subscriptions for this agent
      if (agent_name) {
        const wildcardKey = getSubscriptionKey('*', agent_name);
        const wildcardSubs = subscriptionsRef.current.get(wildcardKey) || [];
        wildcardSubs.forEach(sub => {
          Promise.resolve(sub.handler(rawEvent as EventsMap[keyof EventsMap])).catch(err => {
            console.error('Agent event handler error (agent *):', err);
          });
        });
      }

      // Global wildcard subscriptions
      const globalKey = getSubscriptionKey('*');
      const globalSubs = subscriptionsRef.current.get(globalKey) || [];
      globalSubs.forEach(sub => {
        Promise.resolve(sub.handler(rawEvent as EventsMap[keyof EventsMap])).catch(err => {
          console.error('Agent event handler error (global *):', err);
        });
      });
    } catch (error) {
      console.error('Failed to parse agent event:', error);
    }
  }, []);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const url = token ? `${websocketUrl}?token=${encodeURIComponent(token)}` : websocketUrl;
    const ws = new WebSocket(url);

    ws.onopen = () => {
      setIsConnected(true);
      reconnectCountRef.current = 0;
      onConnect?.();
    };

    ws.onmessage = handleMessage;

    ws.onerror = (error: Event) => {
      onError?.(error);
    };

    ws.onclose = () => {
      setIsConnected(false);
      onDisconnect?.();

      // Attempt reconnection if enabled
      if (enabled && reconnectCountRef.current < reconnectAttempts) {
        reconnectCountRef.current++;
        reconnectTimeoutRef.current = setTimeout(() => {
          connect();
        }, reconnectDelay * reconnectCountRef.current);
      }
    };

    wsRef.current = ws;
  }, [websocketUrl, token, enabled, reconnectAttempts, reconnectDelay, onConnect, onDisconnect, onError, handleMessage]);

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
    }
    reconnectCountRef.current = reconnectAttempts; // Prevent reconnection
    wsRef.current?.close();
    wsRef.current = null;
  }, [reconnectAttempts]);

  const subscribe = useCallback((subscription: EventSubscription<EventsMap>): (() => void) => {
    const { eventType, agentName, handler } = subscription;
    const eventTypes = eventType === '*' ? ['*'] : (Array.isArray(eventType) ? eventType : [eventType]);
    const agentNames = agentName ? (Array.isArray(agentName) ? agentName : [agentName]) : [undefined];

    const keys: string[] = [];

    // Register all combinations
    eventTypes.forEach(et => {
      agentNames.forEach(an => {
        const key = getSubscriptionKey(String(et), an);
        keys.push(key);

        const existing = subscriptionsRef.current.get(key) || [];
        subscriptionsRef.current.set(key, [...existing, subscription]);
      });
    });

    // Return unsubscribe function
    return () => {
      keys.forEach(key => {
        const subs = subscriptionsRef.current.get(key) || [];
        const filtered = subs.filter(s => s.handler !== handler);
        if (filtered.length > 0) {
          subscriptionsRef.current.set(key, filtered);
        } else {
          subscriptionsRef.current.delete(key);
        }
      });
    };
  }, []);

  const unsubscribe = useCallback((eventType: keyof EventsMap | '*', agentName?: string) => {
    const key = getSubscriptionKey(String(eventType), agentName);
    subscriptionsRef.current.delete(key);
  }, []);

  const reconnect = useCallback(() => {
    reconnectCountRef.current = 0;
    disconnect();
    connect();
  }, [connect, disconnect]);

  // Auto-connect on mount and handle cleanup
  useEffect(() => {
    if (enabled) {
      connect();
    }
    return () => {
      disconnect();
    };
  }, [enabled, connect, disconnect]);

  const api = useMemo<AgentEventsApi<EventsMap>>(() => ({
    subscribe,
    unsubscribe,
    isConnected,
    reconnect,
    disconnect,
  }), [subscribe, unsubscribe, isConnected, reconnect, disconnect]);

  return api;
};

/**
 * Helper hook for subscribing to specific events with automatic cleanup
 */
type EventFromKey<EM extends Record<string, unknown>, K> =
  K extends '*' ? EM[keyof EM]
    : K extends ReadonlyArray<infer U>
      ? U extends keyof EM
        ? EM[U]
        : never
      : K extends keyof EM
        ? EM[K]
        : never;

export const useEventSubscription = <
  EventsMap extends Record<string, unknown> = DefaultEventMap,
  K extends keyof EventsMap | '*' | Array<keyof EventsMap> = keyof EventsMap
>(
  events: AgentEventsApi<EventsMap>,
  eventType: K,
  handler: EventHandler<EventFromKey<EventsMap, K>>,
  agentName?: string | string[],
) => {
  const handlerRef = useRef<EventHandler<EventFromKey<EventsMap, K>>>(handler);
  useEffect(() => {
    handlerRef.current = handler;
  }, [handler]);

  useEffect(() => {
    const wrappedHandler = (evt: unknown) => (handlerRef.current as (e: unknown) => void)(evt);
    const unsubscribe = events.subscribe({
      eventType,
      agentName,
      handler: wrappedHandler as unknown as EventHandler<EventsMap[keyof EventsMap]>,
    });
    return unsubscribe;
  }, [events, eventType, agentName]);
};
```

## /ui/types/events.ts

```typescript
export interface BaseEvent {
  event_type: string;
  task_id: string;
  agent_name?: string;
  timestamp: string;
  error?: string;
}

export type ToolFunctionPayload = {
  name: string;
  arguments?: Record<string, unknown>;
};

export type ToolCallPayload = {
  id: string;
  function: ToolFunctionPayload;
};

export interface ToolActionStartedEvent extends BaseEvent {
  event_type: 'progress_update_tool_action_started';
  data: { args: [ToolCallPayload] };
}

export interface ToolActionCompletedEvent extends BaseEvent {
  event_type: 'progress_update_tool_action_completed';
  data: { result: { tool_call: ToolCallPayload; output_data?: Record<string, unknown> } };
}

export interface ToolActionFailedEvent extends BaseEvent {
  event_type: 'progress_update_tool_action_failed';
  data: { args: [ToolCallPayload] };
}

export interface AgentOutputEvent extends BaseEvent {
  event_type: 'agent_output';
  data: string; // or assuming your agent had a final output type pydantic model, this would match that
}

export interface RunCancelledEvent extends BaseEvent {
  event_type: 'run_cancelled';
}

export interface RunFailedEvent extends BaseEvent {
  event_type: 'run_failed';
}

export type DefaultAgentEvent =
  | ToolActionStartedEvent
  | ToolActionCompletedEvent
  | ToolActionFailedEvent
  | AgentOutputEvent
  | RunCancelledEvent
  | RunFailedEvent;

export type DefaultEventMap = {
  progress_update_tool_action_started: ToolActionStartedEvent;
  progress_update_tool_action_completed: ToolActionCompletedEvent;
  progress_update_tool_action_failed: ToolActionFailedEvent;
  agent_output: AgentOutputEvent;
  run_cancelled: RunCancelledEvent;
  run_failed: RunFailedEvent;
};
```


## Usage Examples:
```typescript
import { useState } from 'react';
import { useAgentEvents, useEventSubscription } from '../hooks/useAgentEvents';
import type {
  DefaultEventMap,
  ToolActionStartedEvent,
  ToolActionCompletedEvent,
  ToolActionFailedEvent,
  AgentOutputEvent,
} from '../types/events';

export const TypedSubscriptionsExample = () => {
  const userId = `user_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
  const [logs, setLogs] = useState<string[]>([]);

  const events = useAgentEvents<DefaultEventMap>({
    websocketUrl: `ws://localhost:8000/ws/${userId}`,
    enabled: true,
  });

  // Tool action started
  useEventSubscription(
    events,
    'progress_update_tool_action_started',
    (event: ToolActionStartedEvent) => {
      const toolCall = event.data.args[0];
      setLogs(prev => [...prev, `Started: ${toolCall.function.name}`]);
    }
  );

  // Tool action completed
  useEventSubscription(
    events,
    'progress_update_tool_action_completed',
    (event: ToolActionCompletedEvent) => {
      const { tool_call, output_data } = event.data.result;
      setLogs(prev => [...prev, `Completed: ${tool_call.function.name} (${typeof output_data === 'string' ? output_data : JSON.stringify(output_data)})`]);
    }
  );

  // Tool action failed
  useEventSubscription(
    events,
    'progress_update_tool_action_failed',
    (event: ToolActionFailedEvent) => {
      const toolCall = event.data.args[0];
      setLogs(prev => [...prev, `Failed: ${toolCall.function.name}`]);
    }
  );

  // Agent final output
  useEventSubscription(
    events,
    'agent_output',
    (event: AgentOutputEvent) => {
      setLogs(prev => [...prev, `Answer: ${event.data}`]);
    }
  );

  // Optional: wildcard subscription for a specific agent
  useEventSubscription(events, '*', (event) => {
    // Observe everything from this agent
    console.debug(`[${event.agent_name}] ${event.event_type}`);
  }, 'chat_agent');

  return (
    <div>
      <h3>Event Logs</h3>
      <ul>
        {logs.map((log, i) => <li key={i}>{log}</li>)}
      </ul>
    </div>
  );
};
```
</example>