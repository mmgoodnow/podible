import type { MiddlewareHandler } from "hono";

import { browserSessionTokenFromRequest, buildSessionCookie, resolveSessionFromRequest } from "../auth";
import { BooksRepo } from "../repo";
import type { SessionWithUserRow } from "../app-types";

export type HttpEnv = {
  Variables: {
    session: SessionWithUserRow | null;
    logSuffix: string;
  };
};

export function getCurrentSession(c: { get(key: "session"): SessionWithUserRow | null }): SessionWithUserRow | null {
  return c.get("session");
}

export function setLogSuffix(c: { set(key: "logSuffix", value: string): void }, suffix: string): void {
  c.set("logSuffix", suffix);
}

function loginRedirectPath(url: URL): string {
  const nextPath = `${url.pathname}${url.search}`;
  return `/login?redirectTo=${encodeURIComponent(nextPath)}`;
}

const SESSION_ACTIVITY_INTERVAL_MS = 60_000;

function isWriteContention(error: unknown): boolean {
  return error instanceof Error && "code" in error &&
    typeof error.code === "string" && /^(?:SQLITE_BUSY|SQLITE_LOCKED)(?:_|$)/.test(error.code);
}

export function createRequestContextMiddleware(repo: BooksRepo): MiddlewareHandler<HttpEnv> {
  const activityAttempts = new Map<number, number>();
  return async (c, next) => {
    const startedAt = Date.now();
    c.set("logSuffix", "");

    let session = resolveSessionFromRequest(c.req.raw, (tokenHash) => repo.getSessionByTokenHash(tokenHash));
    const now = Date.now();
    const lastSeen = session ? Date.parse(session.last_seen_at) : now;
    if (session && (!Number.isFinite(lastSeen) || now - lastSeen >= SESSION_ACTIVITY_INTERVAL_MS) &&
        now - (activityAttempts.get(session.id) ?? -Infinity) >= SESSION_ACTIVITY_INTERVAL_MS) {
      // Expiry/revocation were checked above. Activity bookkeeping is best-effort,
      // not a reason to fail an otherwise authenticated read during worker writes.
      try {
        session = repo.touchSessionWithoutWaiting(session.id) ?? session;
      } catch (error) {
        if (!isWriteContention(error)) throw error;
      }
      activityAttempts.set(session.id, now);
      if (activityAttempts.size > 1024) activityAttempts.delete(activityAttempts.keys().next().value!);
    }
    c.set("session", session);

    await next();

    const browserToken = session?.kind === "browser" ? browserSessionTokenFromRequest(c.req.raw) : null;
    if (browserToken && !c.res.headers.has("set-cookie")) {
      c.header("Set-Cookie", buildSessionCookie(browserToken, c.req.raw));
    }

    const elapsedMs = Date.now() - startedAt;
    const suffix = c.get("logSuffix");
    const logSuffix = suffix ? ` ${suffix}` : "";
    console.log(`[http] ${c.req.method} ${new URL(c.req.url).pathname} status=${c.res.status} ms=${elapsedMs}${logSuffix}`);
  };
}

export const requireAuthenticatedPageSession: MiddlewareHandler<HttpEnv> = async (c, next) => {
  if (getCurrentSession(c)) {
    await next();
    return;
  }
  return c.redirect(loginRedirectPath(new URL(c.req.url)), 303);
};

export const requireAuthenticatedRequest: MiddlewareHandler<HttpEnv> = async (c, next) => {
  if (getCurrentSession(c)) {
    await next();
    return;
  }
  return c.text("Unauthorized", 401, { "WWW-Authenticate": 'Bearer realm="podible"' });
};

export const requireAdminSession: MiddlewareHandler<HttpEnv> = async (c, next) => {
  const session = getCurrentSession(c);
  if (!session) {
    if (c.req.method === "GET") {
      return c.redirect(loginRedirectPath(new URL(c.req.url)), 303);
    }
    return c.text("Unauthorized", 401, { "WWW-Authenticate": 'Bearer realm="podible"' });
  }
  if (session.is_admin !== 1) {
    return c.text("Forbidden", 403);
  }
  await next();
};
