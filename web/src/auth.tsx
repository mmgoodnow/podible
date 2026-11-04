import { useEffect, useEffectEvent, useRef, useState } from "react";
import { safeReturnPath } from "../../src/redirects";

export function LoginScreen({ mode = "login" }: { mode?: "login" | "loading" | "result" }) {
  const params = new URLSearchParams(location.search);
  const destination = safeReturnPath(params.get("redirectTo")) ?? "/";
  const inline = params.get("inline") === "1";
  const ok = params.get("ok") === "1";
  const error = params.get("error");
  const [status, setStatus] = useState("");
  const [pending, setPending] = useState(false);
  const popup = useRef<Window | null>(null);
  const onMessage = useEffectEvent((event: MessageEvent) => {
    if (event.origin !== location.origin || event.source !== popup.current || !event.data || event.data.type !== "podible-plex-login") return;
    if (event.data.ok === true) location.assign(safeReturnPath(event.data.redirectTo) ?? destination);
    else { setStatus(typeof event.data.error === "string" ? event.data.error : "Plex sign-in failed."); setPending(false); }
  });
  useEffect(() => {
    const handler = (event: MessageEvent) => onMessage(event);
    window.addEventListener("message", handler);
    return () => window.removeEventListener("message", handler);
  }, []);
  useEffect(() => {
    if (mode !== "result") return;
    const payload = { type: "podible-plex-login", ok, redirectTo: destination, error };
    if (window.opener && !window.opener.closed) {
      window.opener.postMessage(payload, location.origin);
      if (ok) window.close();
    } else if (ok) location.replace(destination);
  }, [mode, ok, destination, error]);
  async function startLogin() {
    setPending(true); setStatus("Opening Plex sign-in...");
    if (!inline) {
      const left = Math.max(0, Math.round(window.screenX + (window.innerWidth - 520) / 2));
      const top = Math.max(0, Math.round(window.screenY + (window.innerHeight - 680) / 2));
      popup.current = window.open(`/login/plex/loading?redirectTo=${encodeURIComponent(destination)}`, "podible-plex-login",
        `popup=yes,width=520,height=680,left=${left},top=${top},resizable=yes,scrollbars=yes`);
      if (!popup.current) { setStatus("Popup blocked. Allow popups or continue in this window."); setPending(false); return; }
    }
    try {
      const response = await fetch(`/login/plex/start?redirectTo=${encodeURIComponent(destination)}`, {
        method: "POST", credentials: "same-origin", signal: AbortSignal.timeout(30_000),
      });
      const payload = await response.json() as { authUrl?: unknown; error?: unknown };
      if (!response.ok || typeof payload.authUrl !== "string")
        throw new Error(typeof payload.error === "string" ? payload.error : "Unable to start Plex sign-in.");
      const authUrl = new URL(payload.authUrl);
      if (authUrl.origin !== "https://app.plex.tv") throw new Error("Unexpected Plex sign-in destination.");
      if (inline) location.assign(authUrl.href);
      else if (popup.current && !popup.current.closed) { popup.current.location.href = authUrl.href; setStatus("Finish sign-in in the Plex window."); }
      else throw new Error("The Plex window was closed. Try again.");
    } catch (cause) {
      popup.current?.close(); setStatus(cause instanceof Error ? cause.message : "Unable to start Plex sign-in."); setPending(false);
    }
  }
  return <main className="login-page" style={{ maxWidth: 520, margin: "12vh auto", padding: "1.5rem" }}>
    <a className="wordmark" href="/">podible</a>
    <div className="library-toolbar" style={{ marginTop: "2rem" }}><h1>{mode === "loading" ? "Plex sign-in" : mode === "result" ? ok ? "Sign-in complete" : "Sign-in failed" : "Sign in"}</h1></div>
    {mode === "loading" ? <p role="status">Opening the Plex sign-in window...</p> : mode === "result" ? <>
      <p role={ok ? "status" : "alert"}>{ok ? "You can close this window." : error || "Plex sign-in failed. Please try again."}</p>
      <a className="button" href={ok ? destination : `/login?redirectTo=${encodeURIComponent(destination)}`}>{ok ? "Continue" : "Try again"}</a>
    </> : <>
      <p>Use Plex to open your library.</p>
      {params.get("notice") && <p role="status">{params.get("notice")}</p>}
      {error && <p role="alert">{error}</p>}
      <button type="button" disabled={pending} onClick={() => void startLogin()}>{pending ? "Connecting to Plex..." : "Continue with Plex"}</button>
      {status && <p role="status">{status}</p>}
      {!inline && <p><a href={`/login?inline=1&redirectTo=${encodeURIComponent(destination)}`}>Continue in this window instead</a></p>}
    </>}
  </main>;
}
