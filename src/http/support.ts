import { buildSessionCookie, createSessionToken, hashSessionToken, sessionExpiresAt } from "../auth";
import {
  checkPlexUserAccess,
  exchangePlexPinForToken,
  fetchPlexUser,
} from "../plex";

import { BooksRepo } from "../repo";
import type { AppSettings, PlexJwk, SessionWithUserRow } from "../app-types";

import {
  addApiKey,
  escapeHtml,
  renderAppPage,
  sanitizeRedirectPath,
} from "./common";

function renderPlexLoadingPage(settings: AppSettings, apiKey: string | null = null): Response {
  const body = `
    <section class="hero">
      <h1>Plex sign-in</h1>
      <p>Finish the Plex sign-in flow in this window. Podible will continue automatically when Plex redirects back.</p>
    </section>`;
  return renderAppPage("Plex sign in", body, settings, null, "", apiKey);
}

async function waitForPlexLoginResult(
  repo: BooksRepo,
  settings: AppSettings,
  pinId: number,
  apiKey: string | null,
  redirectTo: string | null
): Promise<
  | { kind: "error"; settings: AppSettings; error: string; redirectTo: string }
  | { kind: "success"; settings: AppSettings; redirectTo: string; sessionToken: string }
> {
  let currentSettings = settings;
  for (let attempt = 0; attempt < 20; attempt += 1) {
    const result = await resolvePlexLoginStatus(repo, currentSettings, pinId, apiKey, redirectTo);
    currentSettings = result.settings;
    if (result.kind === "success" || result.kind === "error") {
      return result;
    }
    await Bun.sleep(3000);
  }
  return {
    kind: "error",
    settings: currentSettings,
    error: "Timed out waiting for Plex sign-in.",
    redirectTo: "/login",
  };
}

function renderPlexImmediateResultPage(
  result: { ok: boolean; redirectTo: string; error?: string | null }
): Response {
  const payload = JSON.stringify({
    type: "podible-plex-login",
    ok: result.ok,
    redirectTo: result.redirectTo,
    error: result.error ?? null,
  });
  return new Response(
    `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Plex sign-in complete</title>
  </head>
  <body style="font-family: ui-sans-serif, system-ui, sans-serif; padding: 24px;">
    <p>${escapeHtml(result.ok ? "Sign-in complete. You can close this window." : result.error ?? "Plex sign-in failed.")}</p>
    <script>
      (function () {
        var payload = ${payload};
        if (window.opener && !window.opener.closed) {
          window.opener.postMessage(payload, window.location.origin);
          ${result.ok ? "window.close();" : ""}
        } else if (payload.ok && payload.redirectTo) {
          window.location.href = payload.redirectTo;
        }
      })();
    </script>
  </body>
</html>`,
    {
      status: result.ok ? 200 : 400,
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store",
      },
    }
  );
}

async function resolvePlexLoginStatus(
  repo: BooksRepo,
  settings: AppSettings,
  pinId: number,
  apiKey: string | null = null,
  redirectTo: string | null = null
): Promise<
  | { kind: "pending"; settings: AppSettings; message: string }
  | { kind: "error"; settings: AppSettings; error: string; redirectTo: string }
  | { kind: "success"; settings: AppSettings; redirectTo: string; sessionToken: string }
> {
  const attempt = repo.getPlexLoginAttempt(pinId);
  if (!attempt) {
    return {
      kind: "error",
      settings,
      error: "This Plex login attempt is missing or has expired.",
      redirectTo: "/login",
    };
  }
  const ageMs = Date.now() - Date.parse(attempt.created_at);
  if (!Number.isFinite(ageMs) || ageMs > 15 * 60_000) {
    repo.deletePlexLoginAttempt(pinId);
    return {
      kind: "error",
      settings,
      error: "This Plex login attempt has expired. Please try again.",
      redirectTo: "/login",
    };
  }
  const identity = {
    productName: settings.auth.plex.productName,
    clientIdentifier: attempt.client_identifier,
    publicJwk: JSON.parse(attempt.public_jwk_json) as PlexJwk,
    privateKeyPkcs8: attempt.private_key_pkcs8,
  };

  console.log(
    `[plex] checking login status pinId=${pinId} ownerToken=${settings.auth.plex.ownerToken ? "present" : "missing"} machineId=${settings.auth.plex.machineId || "missing"} clientId=${attempt.client_identifier}`
  );

  let plexToken: string;
  try {
    plexToken = await exchangePlexPinForToken(identity, pinId);
  } catch (error) {
    const message = (error as Error).message || "";
    if (message.includes("has not been claimed yet")) {
      return {
        kind: "pending",
        settings,
        message: "Waiting for Plex to finish sign-in…",
      };
    }
    if (message.includes("rate limited")) {
      return {
        kind: "pending",
        settings,
        message: "Plex is asking Podible to slow down. Retrying…",
      };
    }
    console.log(`[plex] login failed pinId=${pinId} error=${message || "unknown"}`);
    return {
      kind: "error",
      settings,
      error: message || "Plex sign-in failed.",
      redirectTo: "/login",
    };
  }

  console.log(`[plex] pin claimed pinId=${pinId}`);

  try {
    const plexUser = await fetchPlexUser(settings, plexToken, attempt.client_identifier);
    console.log(`[plex] fetched user id=${plexUser.id} username=${plexUser.username}`);
    const existingPlexUsers = repo.listUsers("plex");
    const existingPlexUser = existingPlexUsers.find((user) => user.provider_user_id === plexUser.id) ?? null;
    const existingUsers = repo.listUsers();
    const isBootstrap = existingPlexUsers.length === 0;
    let allowed = false;
    let denialReason = "This Plex user is not allowed on this Podible instance.";

    if (isBootstrap) {
      allowed = true;
    } else if (!settings.auth.plex.ownerToken || !settings.auth.plex.machineId) {
      allowed = Boolean(existingPlexUser);
      if (!allowed) {
        denialReason = "An admin needs to choose which Plex server controls Podible access first.";
      }
    } else {
      const hasServerAccess =
        plexUser.id === (existingPlexUsers.find((user) => user.is_admin === 1)?.provider_user_id ?? "")
          ? true
          : await checkPlexUserAccess(settings, plexUser.id);
      allowed = hasServerAccess;
    }

    if (!allowed) {
      repo.deletePlexLoginAttempt(pinId);
      return {
        kind: "error",
        settings,
        error: denialReason,
        redirectTo: "/login",
      };
    }

    const hasPlexAdminUser = existingPlexUsers.some((user) => user.is_admin === 1);
    if (!settings.auth.plex.ownerToken && (!hasPlexAdminUser || existingPlexUser?.is_admin === 1)) {
      settings = repo.updateSettings({
        ...settings,
        auth: {
          ...settings.auth,
          plex: {
            ...settings.auth.plex,
            ownerToken: plexToken,
          },
        },
      });
      console.log(`[plex] captured owner token from user id=${plexUser.id}`);
    }

    const user = repo.upsertUser({
      provider: "plex",
      providerUserId: plexUser.id,
      username: plexUser.username,
      displayName: plexUser.displayName,
      thumbUrl: plexUser.thumbUrl,
      isAdmin: existingPlexUser ? existingPlexUser.is_admin === 1 : !hasPlexAdminUser,
    });
    const sessionToken = createSessionToken();
    repo.createSession(user.id, hashSessionToken(sessionToken), sessionExpiresAt());
    repo.deletePlexLoginAttempt(pinId);
    return {
      kind: "success",
      settings,
      redirectTo: addApiKey(sanitizeRedirectPath(redirectTo) ?? "/", apiKey),
      sessionToken,
    };
  } catch (error) {
    const message = (error as Error).message || "Plex sign-in failed.";
    console.log(`[plex] login failed pinId=${pinId} error=${message || "unknown"}`);
    repo.deletePlexLoginAttempt(pinId);
    return {
      kind: "error",
      settings,
      error: message,
      redirectTo: "/login",
    };
  }
}

export {
  renderPlexImmediateResultPage,
  renderPlexLoadingPage,
  waitForPlexLoginResult,
};
