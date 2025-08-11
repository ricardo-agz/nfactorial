# useAgentEvents
A custom hook that makes it easy to subscribe to websocket agent events and add handlers for different events.

e.g. /ui/hooks/useAgentEvents.tsx
```typescript
import { useEffect, useRef, useCallback, useMemo } from 'react';
import { AgentEvent } from '../types';

export type EventHandler<T = any> = (event: AgentEvent<T>) => void | Promise<void>;

export interface EventSubscription {
  eventType: string | string[];
  agentName?: string | string[];
  handler: EventHandler;
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

export interface AgentEventsApi {
  subscribe: (subscription: EventSubscription) => () => void;
  unsubscribe: (eventType: string, agentName?: string) => void;
  isConnected: boolean;
  reconnect: () => void;
  disconnect: () => void;
}

/**
 * Core hook for managing WebSocket connections and event subscriptions
 * Provides a clean API for subscribing to specific agent events
 */
export const useAgentEvents = (config: UseAgentEventsConfig): AgentEventsApi => {
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
  const subscriptionsRef = useRef<Map<string, EventSubscription[]>>(new Map());
  const isConnectedRef = useRef(false);
  const reconnectCountRef = useRef(0);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | undefined>(undefined);

  const getSubscriptionKey = (eventType: string, agentName?: string): string => {
    return agentName ? `${agentName}:${eventType}` : eventType;
  };

  const handleMessage = useCallback((event: MessageEvent) => {
    try {
      const agentEvent: AgentEvent = JSON.parse(event.data);
      const { event_type, agent_name } = agentEvent;

      // Check subscriptions for exact agent:event match
      if (agent_name) {
        const exactKey = getSubscriptionKey(event_type, agent_name);
        const exactSubs = subscriptionsRef.current.get(exactKey) || [];
        exactSubs.forEach(sub => sub.handler(agentEvent));
      }

      // Check subscriptions for any agent with this event type
      const eventKey = getSubscriptionKey(event_type);
      const eventSubs = subscriptionsRef.current.get(eventKey) || [];
      eventSubs.forEach(sub => sub.handler(agentEvent));

      // Check wildcard subscriptions for this agent
      if (agent_name) {
        const wildcardKey = getSubscriptionKey('*', agent_name);
        const wildcardSubs = subscriptionsRef.current.get(wildcardKey) || [];
        wildcardSubs.forEach(sub => sub.handler(agentEvent));
      }

      // Check global wildcard subscriptions
      const globalKey = getSubscriptionKey('*');
      const globalSubs = subscriptionsRef.current.get(globalKey) || [];
      globalSubs.forEach(sub => sub.handler(agentEvent));
    } catch (error) {
      console.error('Failed to parse agent event:', error);
    }
  }, []);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const url = token ? `${websocketUrl}?token=${token}` : websocketUrl;
    const ws = new WebSocket(url);

    ws.onopen = () => {
      isConnectedRef.current = true;
      reconnectCountRef.current = 0;
      onConnect?.();
    };

    ws.onmessage = handleMessage;

    ws.onerror = (error: Event) => {
      onError?.(error);
    };

    ws.onclose = () => {
      isConnectedRef.current = false;
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

  const subscribe = useCallback((subscription: EventSubscription): (() => void) => {
    const { eventType, agentName, handler } = subscription;
    const eventTypes = Array.isArray(eventType) ? eventType : [eventType];
    const agentNames = agentName ? (Array.isArray(agentName) ? agentName : [agentName]) : [undefined];

    const keys: string[] = [];

    // Register all combinations
    eventTypes.forEach(et => {
      agentNames.forEach(an => {
        const key = getSubscriptionKey(et, an);
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

  const unsubscribe = useCallback((eventType: string, agentName?: string) => {
    const key = getSubscriptionKey(eventType, agentName);
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

  const api = useMemo<AgentEventsApi>(() => ({
    subscribe,
    unsubscribe,
    isConnected: isConnectedRef.current,
    reconnect,
    disconnect,
  }), [subscribe, unsubscribe, reconnect, disconnect]);

  return api;
};

/**
 * Helper hook for subscribing to specific events with automatic cleanup
 */
export const useEventSubscription = (
  events: AgentEventsApi,
  eventType: string | string[],
  handler: EventHandler,
  agentName?: string | string[],
  deps: React.DependencyList = []
) => {
  useEffect(() => {
    const unsubscribe = events.subscribe({
      eventType,
      agentName,
      handler,
    });
    return unsubscribe;
  }, [events, eventType, agentName, ...deps]);
};

```


## Usage Examples:
```typescript
export const BasicEventExample = () => {
  const [logs, setLogs] = useState<string[]>([]);

  const events = useAgentEvents({
    websocketUrl: 'ws://localhost:8000/ws',
    enabled: true,
  });

  // Subscribe to all events from chat_agent
  useEventSubscription(
    events,
    '*', // wildcard for all event types
    (event) => {
      setLogs(prev => [...prev, `[${event.agent_name}] ${event.event_type}: ${JSON.stringify(event.data)}`]);
    },
    'chat_agent'
  );

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