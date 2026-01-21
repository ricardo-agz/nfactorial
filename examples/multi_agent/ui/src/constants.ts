function trimTrailingSlash(value: string): string {
  return value.replace(/\/+$/, '');
}

function ensureEndsWith(value: string, suffix: string): string {
  return value.endsWith(suffix) ? value : `${value}${suffix}`;
}

function defaultApiBaseUrl(): string {
  // Fallback that works for both local runs and docker-compose (browser hits host ports).
  if (typeof window === 'undefined') return 'http://localhost:8000/api';
  return `${window.location.protocol}//${window.location.hostname}:8000/api`;
}

function toWsUrl(url: string): string {
  if (url.startsWith('https://')) return `wss://${url.slice('https://'.length)}`;
  if (url.startsWith('http://')) return `ws://${url.slice('http://'.length)}`;
  return url;
}

function defaultWsBaseUrl(): string {
  // Fallback that works for both local runs and docker-compose (browser hits host ports).
  if (typeof window === 'undefined') return 'ws://localhost:8000/ws';
  const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${wsProtocol}//${window.location.hostname}:8000/ws`;
}

const rawApiBase = (import.meta.env.VITE_API_BASE_URL ?? '').trim();
const rawWsBase  = (import.meta.env.VITE_WS_BASE_URL ?? '').trim();

// Server endpoints live under `/api/*`
export const API_BASE_URL = ensureEndsWith(
  trimTrailingSlash(rawApiBase || defaultApiBaseUrl()),
  '/api',
);

// Websocket endpoint lives under `/ws/{user_id}`
export const WS_BASE_URL = ensureEndsWith(
  trimTrailingSlash(toWsUrl(rawWsBase || defaultWsBaseUrl())),
  '/ws',
);
