import { createHash, randomBytes } from "node:crypto";

import type { SessionWithUserRow } from "./app-types";

export const SESSION_COOKIE_NAME = "podible_session";
const SESSION_DURATION_MS = 1000 * 60 * 60 * 24 * 30;

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

function isSecureRequest(request: Request): boolean {
  const url = new URL(request.url);
  if (url.protocol === "https:") return true;
  const forwardedProto = request.headers.get("x-forwarded-proto");
  if (!forwardedProto) return false;
  return forwardedProto
    .split(",")
    .some((value) => value.trim().toLowerCase() === "https");
}

function resolveSessionFromToken(
  token: string | null | undefined,
  resolveSession?: (tokenHash: string) => SessionWithUserRow | null
): SessionWithUserRow | null {
  if (!resolveSession) return null;
  const trimmed = token?.trim();
  if (!trimmed) return null;
  const session = resolveSession(hashSessionToken(trimmed));
  if (!session) return null;
  if (new Date(session.expires_at).getTime() <= Date.now()) {
    return null;
  }
  return session;
}

export function resolveBrowserSessionFromRequest(
  request: Request,
  resolveSession?: (tokenHash: string) => SessionWithUserRow | null
): SessionWithUserRow | null {
  return resolveSessionFromToken(parseCookies(request).get(SESSION_COOKIE_NAME), resolveSession);
}

export function resolveBearerSessionFromRequest(
  request: Request,
  resolveSession?: (tokenHash: string) => SessionWithUserRow | null
): SessionWithUserRow | null {
  const authorization = request.headers.get("authorization");
  if (!authorization?.toLowerCase().startsWith("bearer ")) return null;
  return resolveSessionFromToken(authorization.slice("bearer ".length), resolveSession);
}

export function resolveSessionFromRequest(
  request: Request,
  resolveSession?: (tokenHash: string) => SessionWithUserRow | null
): SessionWithUserRow | null {
  return resolveBearerSessionFromRequest(request, resolveSession) ?? resolveBrowserSessionFromRequest(request, resolveSession);
}

export function buildSessionCookie(token: string, request: Request): string {
  const secure = isSecureRequest(request);
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
  const secure = isSecureRequest(request);
  const parts = [`${SESSION_COOKIE_NAME}=`, "Path=/", "HttpOnly", "SameSite=Lax", "Max-Age=0", "Expires=Thu, 01 Jan 1970 00:00:00 GMT"];
  if (secure) {
    parts.push("Secure");
  }
  return parts.join("; ");
}

export function sessionExpiresAt(): string {
  return new Date(Date.now() + SESSION_DURATION_MS).toISOString();
}
