import type { AppSettings, SessionWithUserRow } from "../app-types";

import { addApiKey, messageMarkup, renderAppPage, sanitizeRedirectPath } from "./common";

export function renderLoginPage(
  settings: AppSettings,
  options: {
    notice?: string | null;
    error?: string | null;
    currentUser?: SessionWithUserRow | null;
    apiKey?: string | null;
    redirectTo?: string | null;
    inlinePlexLogin?: boolean;
  } = {}
): Response {
  const apiKey = options.apiKey ?? null;
  const redirectTo = sanitizeRedirectPath(options.redirectTo) ?? "/";
  const inlinePlexLogin = options.inlinePlexLogin === true;
  const plexStartPath = addApiKey(`/login/plex/start?redirectTo=${encodeURIComponent(redirectTo)}`, apiKey);
  const plexLoadingPath = addApiKey(`/login/plex/loading?redirectTo=${encodeURIComponent(redirectTo)}`, apiKey);
  const body = `
    <style>
      .login-shell {
        min-height: calc(100vh - 36px);
        display: grid;
        place-items: center;
      }
      .login-hero {
        max-width: 560px;
        margin: 0 auto;
        padding: 28px;
        text-align: center;
        background:
          radial-gradient(circle at top, rgba(40, 89, 67, 0.08), transparent 55%),
          var(--paper);
      }
      .login-hero p {
        margin-left: auto;
        margin-right: auto;
      }
      .login-hero .actions {
        justify-content: center;
      }
      button.login-cta {
        min-width: 220px;
        justify-content: center;
        font-weight: 600;
        background: #e5a00d;
        border-color: #e5a00d;
        color: #1f261c;
      }
      button.login-cta:hover {
        background: #d09108;
        border-color: #d09108;
        color: #1f261c;
      }
    </style>
    <div class="login-shell">
    <section class="hero login-hero">
      <h1>Sign in to Podible</h1>
      <p>Use Plex to sign in and open your library.</p>
      ${messageMarkup(options.notice, options.error)}
      <div class="actions" style="margin-top: 14px;">
        <button id="plex-login-btn" type="button" class="login-cta">Continue with Plex</button>
      </div>
      <p id="plex-login-status" class="muted" style="margin-top: 10px;"></p>
    </section>
    </div>
    <script>
      (() => {
        const button = document.getElementById("plex-login-btn");
        const status = document.getElementById("plex-login-status");
        const startUrl = ${JSON.stringify(plexStartPath)};
        const loadingUrl = ${JSON.stringify(plexLoadingPath)};
        const successPath = ${JSON.stringify(addApiKey(redirectTo, apiKey))};
        const inlineLogin = ${inlinePlexLogin ? "true" : "false"};
        function setStatus(message) {
          if (status) status.textContent = message || "";
        }
        function openCenteredPopup(url, name, width, height) {
          const dualScreenLeft = window.screenLeft ?? window.screenX ?? 0;
          const dualScreenTop = window.screenTop ?? window.screenY ?? 0;
          const viewportWidth = window.innerWidth || document.documentElement.clientWidth || screen.width;
          const viewportHeight = window.innerHeight || document.documentElement.clientHeight || screen.height;
          const left = Math.max(0, Math.round(dualScreenLeft + (viewportWidth - width) / 2));
          const top = Math.max(0, Math.round(dualScreenTop + (viewportHeight - height) / 2));
          const features = [
            "popup=yes",
            "width=" + width,
            "height=" + height,
            "left=" + left,
            "top=" + top,
            "resizable=yes",
            "scrollbars=yes",
          ].join(",");
          return window.open(url, name, features);
        }
        if (!inlineLogin) {
          window.addEventListener("message", (event) => {
            if (event.origin !== window.location.origin || !event.data || event.data.type !== "podible-plex-login") {
              return;
            }
            if (event.data.ok) {
              window.location.href = event.data.redirectTo || successPath;
              return;
            }
            setStatus(event.data.error || "Plex sign-in failed.");
            if (button) button.disabled = false;
          });
        }
        button?.addEventListener("click", async () => {
          button.disabled = true;
          setStatus("Opening Plex sign-in…");
          const popup = inlineLogin ? null : openCenteredPopup(loadingUrl, "podible-plex-login", 520, 680);
          if (!inlineLogin && !popup) {
            setStatus("Popup blocked. Please allow popups for this site.");
            button.disabled = false;
            return;
          }
          try {
            const response = await fetch(startUrl, { method: "POST" });
            const payload = await response.json();
            if (!response.ok || !payload.authUrl) {
              throw new Error(payload.error || "Unable to start Plex sign-in.");
            }
            if (inlineLogin) {
              window.location.href = payload.authUrl;
              return;
            }
            popup.location.href = payload.authUrl;
            setStatus("Finish sign-in in the Plex window…");
          } catch (error) {
            if (popup) popup.close();
            setStatus(error && error.message ? error.message : "Unable to start Plex sign-in.");
            button.disabled = false;
          }
        });
      })();
    </script>`;
  return renderAppPage("Sign in", body, settings, options.currentUser ?? null, "", apiKey, { showNav: false });
}
