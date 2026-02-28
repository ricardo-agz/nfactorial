const apiBaseUrl = import.meta.env.VITE_API_BASE_URL as string | undefined;
const wsBaseUrl = import.meta.env.VITE_WS_BASE_URL as string | undefined;

export const API_BASE_URL = apiBaseUrl ?? "http://localhost:8000/api";
export const WS_BASE_URL = wsBaseUrl ?? "ws://localhost:8000/ws";
