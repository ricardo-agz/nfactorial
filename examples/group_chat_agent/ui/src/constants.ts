const env = import.meta.env;

const isLocalhost =
  typeof window !== "undefined" &&
  (window.location.hostname === "localhost" ||
    window.location.hostname === "127.0.0.1");

export const API_BASE_URL =
  env.VITE_API_BASE_URL ||
  env.VITE_API_URL ||
  (isLocalhost ? "http://localhost:8000/api" : "/api");

export const SSE_BASE_URL =
  env.VITE_SSE_BASE_URL || `${API_BASE_URL}/events`;
