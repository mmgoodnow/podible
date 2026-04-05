import {
  buildSessionCookie,
  clearSessionCookie,
  createSessionToken,
  hashSessionToken,
} from "../auth";
import { buildPlexAuthUrl, createEphemeralPlexIdentity, createPlexPin } from "../plex";
import { BooksRepo } from "../repo";
import { renderLoginPage } from "./login-page";
import { renderAppAuthErrorPage } from "./common";
import { renderPlexImmediateResultPage, renderPlexLoadingPage, waitForPlexLoginResult } from "./support";
import { json, redirect } from "./route-helpers";
import type { AppSettings, SessionWithUserRow } from "../types";

type AppLoginPath = { attemptId: string; isComplete: boolean } | null;

export function isPublicRoute(pathname: string, appLoginPath: AppLoginPath): boolean {
  return (
    pathname === "/" ||
    pathname === "/login" ||
    pathname === "/logout" ||
    pathname === "/login/plex/start" ||
    pathname === "/login/plex/loading" ||
    pathname === "/login/plex/complete" ||
    appLoginPath !== null
  );
}

export async function handleAuthRoute(input: {
  repo: BooksRepo;
  request: Request;
  settings: AppSettings;
  currentSession: SessionWithUserRow | null;
  pathname: string;
  method: string;
  redirectTo: string | null;
  appLoginPath: AppLoginPath;
}): Promise<{ response: Response; settings: AppSettings } | null> {
  const { repo, request, currentSession, pathname, method, redirectTo, appLoginPath } = input;
  let { settings } = input;

  if (pathname === "/login" && method === "GET") {
    return {
      response: renderLoginPage(settings, {
        notice: new URL(request.url).searchParams.get("notice"),
        error: new URL(request.url).searchParams.get("error"),
        currentUser: currentSession,
        redirectTo,
      }),
      settings,
    };
  }

  if (pathname === "/login/plex/start" && method === "POST") {
    if (settings.auth.mode !== "plex") {
      return { response: json({ error: "Plex sign-in is not enabled." }, 403), settings };
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
      const forwardUrl = new URL(request.url);
      forwardUrl.pathname = "/login/plex/complete";
      forwardUrl.search = "";
      forwardUrl.searchParams.set("pinId", String(pin.id));
      if (redirectTo) {
        forwardUrl.searchParams.set("redirectTo", redirectTo);
      }
      return {
        response: json({
          pinId: pin.id,
          authUrl: buildPlexAuthUrl(identity, pin.code, forwardUrl.toString()),
        }),
        settings,
      };
    } catch (error) {
      return { response: json({ error: (error as Error).message || "Unable to start Plex sign-in." }, 502), settings };
    }
  }

  if (pathname === "/login/plex/loading" && method === "GET") {
    return { response: renderPlexLoadingPage(settings), settings };
  }

  if (pathname === "/login/plex/complete" && method === "GET") {
    if (settings.auth.mode !== "plex") {
      return {
        response: renderPlexImmediateResultPage({ ok: false, redirectTo: "/", error: "Plex sign-in is not enabled." }),
        settings,
      };
    }
    const pinId = Number.parseInt(new URL(request.url).searchParams.get("pinId") ?? "", 10);
    if (!Number.isInteger(pinId) || pinId <= 0) {
      return {
        response: renderPlexImmediateResultPage({ ok: false, redirectTo: "/login", error: "Missing or invalid Plex PIN id." }),
        settings,
      };
    }
    const result = await waitForPlexLoginResult(repo, settings, pinId, null, redirectTo);
    settings = result.settings;
    const response = renderPlexImmediateResultPage({
      ok: result.kind === "success",
      redirectTo: result.redirectTo,
      error: result.kind === "error" ? result.error : null,
    });
    if (result.kind === "success") {
      response.headers.append("Set-Cookie", buildSessionCookie(result.sessionToken, request));
    }
    return { response, settings };
  }

  if (pathname === "/logout" && method === "POST") {
    if (currentSession) {
      repo.deleteSession(currentSession.id);
    }
    const response = redirect("/login?notice=Signed%20out.");
    response.headers.append("Set-Cookie", clearSessionCookie(request));
    return { response, settings };
  }

  if (appLoginPath && method === "GET") {
    repo.deleteExpiredAppLoginAttempts(new Date().toISOString());
    const attempt = repo.getAppLoginAttempt(appLoginPath.attemptId);
    if (!attempt) {
      return { response: renderAppAuthErrorPage(settings, "This app sign-in attempt is missing or has expired."), settings };
    }
    const attemptPath = `/auth/app/${encodeURIComponent(attempt.id)}`;
    if (appLoginPath.isComplete) {
      if (!currentSession) {
        return { response: redirect(attemptPath), settings };
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
      return { response: redirect(callbackUrl.toString(), 302), settings };
    }
    if (currentSession) {
      return { response: redirect(`${attemptPath}/complete`), settings };
    }
    return {
      response: renderLoginPage(settings, {
        currentUser: currentSession,
        redirectTo: `${attemptPath}/complete`,
        inlinePlexLogin: true,
      }),
      settings,
    };
  }

  return null;
}
