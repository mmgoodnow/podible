import { Hono, type Context } from "hono";

import {
  buildSessionCookie,
  clearSessionCookie,
  createSessionToken,
  hashSessionToken,
} from "../auth";
import { buildPlexAuthUrl, createEphemeralPlexIdentity, createPlexPin } from "../plex";
import { BooksRepo } from "../repo";
import type { AppSettings } from "../types";
import { renderLoginPage } from "./login-page";
import { renderAppAuthErrorPage, sanitizeRedirectPath } from "./common";
import { getCurrentSession, type HttpEnv } from "./middleware";
import { json } from "./route-helpers";
import { renderPlexImmediateResultPage, renderPlexLoadingPage, waitForPlexLoginResult } from "./support";

export function createAuthRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();

  app.get("/login", (c) => {
    const settings = repo.getSettings();
    return renderLoginPage(settings, {
      notice: c.req.query("notice"),
      error: c.req.query("error"),
      currentUser: getCurrentSession(c),
      redirectTo: sanitizeRedirectPath(c.req.query("redirectTo")),
    });
  });

  app.post("/login/plex/start", async (c) => {
    const settings = repo.getSettings();
    const redirectTo = sanitizeRedirectPath(c.req.query("redirectTo"));
    if (settings.auth.mode !== "plex") {
      return json({ error: "Plex sign-in is not enabled." }, 403);
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
      return json({
        pinId: pin.id,
        authUrl: buildPlexAuthUrl(identity, pin.code, forwardUrl.toString()),
      });
    } catch (error) {
      return json({ error: (error as Error).message || "Unable to start Plex sign-in." }, 502);
    }
  });

  app.get("/login/plex/loading", (c) => renderPlexLoadingPage(repo.getSettings()));

  app.get("/login/plex/complete", async (c) => {
    let settings = repo.getSettings();
    const redirectTo = sanitizeRedirectPath(c.req.query("redirectTo"));
    if (settings.auth.mode !== "plex") {
      return renderPlexImmediateResultPage({ ok: false, redirectTo: "/", error: "Plex sign-in is not enabled." });
    }
    const pinId = Number.parseInt(c.req.query("pinId") ?? "", 10);
    if (!Number.isInteger(pinId) || pinId <= 0) {
      return renderPlexImmediateResultPage({ ok: false, redirectTo: "/login", error: "Missing or invalid Plex PIN id." });
    }
    const result = await waitForPlexLoginResult(repo, settings, pinId, null, redirectTo);
    settings = result.settings;
    const response = renderPlexImmediateResultPage({
      ok: result.kind === "success",
      redirectTo: result.redirectTo,
      error: result.kind === "error" ? result.error : null,
    });
    if (result.kind === "success") {
      response.headers.append("Set-Cookie", buildSessionCookie(result.sessionToken, c.req.raw));
    }
    return response;
  });

  app.post("/logout", (c) => {
    const currentSession = getCurrentSession(c);
    if (currentSession) {
      repo.deleteSession(currentSession.id);
    }
    const response = c.redirect("/login?notice=Signed%20out.", 303);
    response.headers.append("Set-Cookie", clearSessionCookie(c.req.raw));
    return response;
  });

  app.get("/auth/app/:attemptId", (c) => renderAppLogin(repo, c, false));
  app.get("/auth/app/:attemptId/complete", (c) => renderAppLogin(repo, c, true));

  return app;
}

function renderAppLogin(repo: BooksRepo, c: Context<HttpEnv>, isComplete: boolean): Response {
  const settings = repo.getSettings();
  repo.deleteExpiredAppLoginAttempts(new Date().toISOString());
  const attemptId = c.req.param("attemptId");
  if (!attemptId) {
    return renderAppAuthErrorPage(settings, "This app sign-in attempt is missing or has expired.");
  }
  const attempt = repo.getAppLoginAttempt(attemptId);
  if (!attempt) {
    return renderAppAuthErrorPage(settings, "This app sign-in attempt is missing or has expired.");
  }

  const currentSession = getCurrentSession(c);
  const attemptPath = `/auth/app/${encodeURIComponent(attempt.id)}`;
  if (isComplete) {
    if (!currentSession) {
      return c.redirect(attemptPath, 303);
    }
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

  if (currentSession) {
    return c.redirect(`${attemptPath}/complete`, 303);
  }

  return renderLoginPage(settings, {
    currentUser: currentSession,
    redirectTo: `${attemptPath}/complete`,
    inlinePlexLogin: true,
  });
}
