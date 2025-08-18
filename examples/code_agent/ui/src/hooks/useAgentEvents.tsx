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


