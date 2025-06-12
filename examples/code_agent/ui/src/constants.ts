const env = import.meta.env;

export const API_BASE = env.VITE_API_BASE_URL || 'http://localhost:8000/api';
export const WS_BASE = env.VITE_WS_BASE_URL || 'ws://localhost:8000/ws';