import { createHash, randomBytes } from "node:crypto";

import type { AppSettings, SessionWithUserRow } from "./types";

export const SESSION_COOKIE_NAME = "podible_session";
const SESSION_DURATION_MS = 1000 * 60 * 60 * 24 * 30;

function isLocalhost(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1";
}

function parseCookies(request: Request): Map<string, string> {
  const header = request.headers.get("cookie") ?? "";
  const out = new Map<string, string>();
  for (const part of header.split(";")) {
    const [rawKey, ...rawValue] = part.split("=");
    const key = rawKey?.trim();
    if (!key) continue;
    out.set(key, decodeURIComponent(rawValue.join("=").trim()));
  }
  return out;
}

export function hashSessionToken(token: string): string {
  return createHash("sha256").update(token).digest("hex");
}

export function createSessionToken(): string {
  return randomBytes(32).toString("base64url");
}

export function resolveSessionFromRequest(
  request: Request,
  resolveSession?: (tokenHash: string) => SessionWithUserRow | null
): SessionWithUserRow | null {
  if (!resolveSession) return null;
  const sessionToken = parseCookies(request).get(SESSION_COOKIE_NAME)?.trim();
  if (!sessionToken) return null;
  const session = resolveSession(hashSessionToken(sessionToken));
  if (!session) return null;
  if (new Date(session.expires_at).getTime() <= Date.now()) {
    return null;
  }
  return session;
}

export function buildSessionCookie(token: string, request: Request): string {
  const url = new URL(request.url);
  const secure = url.protocol === "https:";
  const parts = [
    `${SESSION_COOKIE_NAME}=${encodeURIComponent(token)}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Lax",
    `Max-Age=${Math.floor(SESSION_DURATION_MS / 1000)}`,
  ];
  if (secure) {
    parts.push("Secure");
  }
  return parts.join("; ");
}

export function clearSessionCookie(request: Request): string {
  const url = new URL(request.url);
  const secure = url.protocol === "https:";
  const parts = [`${SESSION_COOKIE_NAME}=`, "Path=/", "HttpOnly", "SameSite=Lax", "Max-Age=0", "Expires=Thu, 01 Jan 1970 00:00:00 GMT"];
  if (secure) {
    parts.push("Secure");
  }
  return parts.join("; ");
}

export function sessionExpiresAt(): string {
  return new Date(Date.now() + SESSION_DURATION_MS).toISOString();
}

export function isApiKeyAuthorized(request: Request, settings: AppSettings): boolean {
  const url = new URL(request.url);
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

export function authorizeRequest(
  request: Request,
  settings: AppSettings,
  resolveSession?: (tokenHash: string) => SessionWithUserRow | null
): boolean {
  const url = new URL(request.url);
  if (process.env.NODE_ENV !== "production" && isLocalhost(url.hostname)) {
    return true;
  }
  if (settings.auth.mode === "local" && isLocalhost(url.hostname)) {
    return true;
  }

  if (isApiKeyAuthorized(request, settings)) return true;

  if (resolveSessionFromRequest(request, resolveSession)) return true;

  return false;
}
