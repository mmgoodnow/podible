import type { AppSettings } from "./types";

function isLocalhost(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1";
}

export function authorizeRequest(request: Request, settings: AppSettings): boolean {
  const url = new URL(request.url);
  if (settings.auth.mode === "local" && isLocalhost(url.hostname)) {
    return true;
  }

  const queryApiKey = url.searchParams.get("api_key")?.trim();
  if (queryApiKey && queryApiKey === settings.auth.key) {
    return true;
  }

  const bearer = request.headers.get("authorization");
  if (bearer?.toLowerCase().startsWith("bearer ")) {
    const token = bearer.slice("bearer ".length).trim();
    if (token && token === settings.auth.key) return true;
  }

  const apiKey = request.headers.get("x-api-key")?.trim();
  if (apiKey && apiKey === settings.auth.key) return true;

  return false;
}
