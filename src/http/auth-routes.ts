import { Hono, type Context } from "hono";

import {
  buildSessionCookie,
  clearSessionCookie,
  createSessionToken,
  hashSessionToken,
} from "../auth";
import { buildPlexAuthUrl, createEphemeralPlexIdentity, createPlexPin } from "../plex";
import { BooksRepo } from "../repo";
import { sanitizeRedirectPath } from "./common";
import { getCurrentSession, type HttpEnv } from "./middleware";
import { waitForPlexLoginResult } from "./support";

function plexResultRedirect(c: Context<HttpEnv>, result: { ok: boolean; redirectTo: string | null; error?: string | null }): Response {
  const query = new URLSearchParams({ ok: result.ok ? "1" : "0", redirectTo: sanitizeRedirectPath(result.redirectTo) ?? "/" });
  if (!result.ok) query.set("error", result.error || "Plex sign-in failed.");
  return c.redirect(`/login/plex/result?${query}`, 303);
}

export function createLoginRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", async (c, next) => {
    c.header("Cache-Control", "no-store");
    await next();
  });

  app.post("/plex/start", async (c) => {
    const settings = repo.getSettings();
    const redirectTo = sanitizeRedirectPath(c.req.query("redirectTo"));
    if (settings.auth.mode !== "plex") {
      return c.json({ error: "Plex sign-in is not enabled." }, 403);
    }
    try {
      repo.deleteExpiredPlexLoginAttempts(new Date(Date.now() - 15 * 60_000).toISOString());
      const identity = createEphemeralPlexIdentity(settings.auth.plex.productName);
      const pin = await createPlexPin(identity);
      repo.createPlexLoginAttempt({
        pinId: pin.id,
        clientIdentifier: identity.clientIdentifier,
        publicJwkJson: JSON.stringify(identity.publicJwk),
        privateKeyPkcs8: identity.privateKeyPkcs8,
      });
      console.log(`[plex] created pin id=${pin.id} clientId=${identity.clientIdentifier}`);
      const forwardUrl = new URL(c.req.url);
      forwardUrl.pathname = "/login/plex/complete";
      forwardUrl.search = "";
      forwardUrl.searchParams.set("pinId", String(pin.id));
      if (redirectTo) {
        forwardUrl.searchParams.set("redirectTo", redirectTo);
      }
      return c.json({
        pinId: pin.id,
        authUrl: buildPlexAuthUrl(identity, pin.code, forwardUrl.toString()),
      });
    } catch (error) {
      return c.json({ error: (error as Error).message || "Unable to start Plex sign-in." }, 502);
    }
  });

  app.get("/plex/complete", async (c) => {
    const settings = repo.getSettings();
    const redirectTo = sanitizeRedirectPath(c.req.query("redirectTo"));
    if (settings.auth.mode !== "plex") {
      return plexResultRedirect(c, { ok: false, redirectTo: "/login", error: "Plex sign-in is not enabled." });
    }
    const rawPinId = c.req.query("pinId") ?? "";
    const pinId = Number(rawPinId);
    if (!/^[1-9]\d*$/.test(rawPinId) || !Number.isSafeInteger(pinId)) {
      return plexResultRedirect(c, { ok: false, redirectTo: "/login", error: "Missing or invalid Plex PIN id." });
    }
    const result = await waitForPlexLoginResult(repo, settings, pinId, null, redirectTo);
    const response = plexResultRedirect(c, {
      ok: result.kind === "success",
      redirectTo: result.redirectTo,
      error: result.kind === "error" ? result.error : null,
    });
    if (result.kind === "success") {
      response.headers.append("Set-Cookie", buildSessionCookie(result.sessionToken, c.req.raw));
    }
    return response;
  });

  return app;
}

export function createLogoutRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.post("/", (c) => {
    const currentSession = getCurrentSession(c);
    if (currentSession) {
      repo.deleteSession(currentSession.id);
    }
    const response = c.redirect("/login?notice=Signed%20out.", 303);
    response.headers.append("Set-Cookie", clearSessionCookie(c.req.raw));
    return response;
  });

  return app;
}

export function createAppAuthRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", async (c, next) => {
    c.header("Cache-Control", "no-store");
    await next();
  });
  app.get("/:attemptId", (c) => handleAppLogin(repo, c, false));
  app.get("/:attemptId/complete", (c) => handleAppLogin(repo, c, true));
  return app;
}

function handleAppLogin(repo: BooksRepo, c: Context<HttpEnv>, isComplete: boolean): Response {
  repo.deleteExpiredAppLoginAttempts(new Date().toISOString());
  const attemptId = c.req.param("attemptId");
  if (!attemptId) {
    return c.redirect(`/login?error=${encodeURIComponent("This app sign-in attempt is missing or has expired.")}`, 303);
  }
  const attempt = repo.getAppLoginAttempt(attemptId);
  if (!attempt) {
    return c.redirect(`/login?error=${encodeURIComponent("This app sign-in attempt is missing or has expired.")}`, 303);
  }

  const currentSession = getCurrentSession(c);
  const attemptPath = `/auth/app/${encodeURIComponent(attempt.id)}`;
  if (!currentSession) {
    const query = new URLSearchParams({ inline: "1", redirectTo: `${attemptPath}/complete` });
    return c.redirect(`/login?${query}`, 303);
  }
  if (isComplete) {
    const code = createSessionToken();
    repo.createAuthCode({
      codeHash: hashSessionToken(code),
      userId: currentSession.user_id,
      attemptId: attempt.id,
      expiresAt: new Date(Date.now() + 5 * 60_000).toISOString(),
    });
    const callbackUrl = new URL(attempt.redirect_uri);
    callbackUrl.searchParams.set("code", code);
    callbackUrl.searchParams.set("state", attempt.state);
    return c.redirect(callbackUrl.toString(), 302);
  }

  return c.redirect(`${attemptPath}/complete`, 303);
}
