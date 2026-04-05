import type { MiddlewareHandler } from "hono";

import { resolveSessionFromRequest } from "../auth";
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

export function createRequestContextMiddleware(repo: BooksRepo): MiddlewareHandler<HttpEnv> {
  return async (c, next) => {
    const startedAt = Date.now();
    c.set("logSuffix", "");

    let session = resolveSessionFromRequest(c.req.raw, (tokenHash) => repo.getSessionByTokenHash(tokenHash));
    if (session) {
      session = repo.touchSession(session.id) ?? session;
    }
    c.set("session", session);

    await next();

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
