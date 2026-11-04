import { useEffect, useState } from "react";
import { safeReturnPath } from "../../src/redirects";
import "./auth.css";

export function LoginScreen({ mode = "login" }: { mode?: "login" | "loading" | "result" }) {
  const params = new URLSearchParams(location.search);
  const destination = safeReturnPath(params.get("redirectTo")) ?? "/";
  const ok = params.get("ok") === "1";
  const error = params.get("error");
  const [status, setStatus] = useState("");
  const [pending, setPending] = useState(false);
  useEffect(() => {
    if (mode !== "result") return;
    const payload = { type: "podible-plex-login", ok, redirectTo: destination, error };
    if (window.opener && !window.opener.closed) {
      window.opener.postMessage(payload, location.origin);
      if (ok) window.close();
    } else if (ok) location.replace(destination);
  }, [mode, ok, destination, error]);
  async function startLogin() {
    setPending(true); setStatus("Connecting to Plex...");
    try {
      const response = await fetch(`/login/plex/start?redirectTo=${encodeURIComponent(destination)}`, {
        method: "POST", credentials: "same-origin", signal: AbortSignal.timeout(30_000),
      });
      const payload = await response.json() as { authUrl?: unknown; error?: unknown };
      if (!response.ok || typeof payload.authUrl !== "string")
        throw new Error(typeof payload.error === "string" ? payload.error : "Unable to start Plex sign-in.");
      const authUrl = new URL(payload.authUrl);
      if (authUrl.origin !== "https://app.plex.tv" || authUrl.username || authUrl.password)
        throw new Error("Unexpected Plex sign-in destination.");
      location.assign(authUrl.href);
    } catch (cause) {
      setStatus(cause instanceof Error && cause.name === "TimeoutError" ? "Plex sign-in timed out. Please try again."
        : cause instanceof Error ? cause.message : "Unable to start Plex sign-in.");
      setPending(false);
    }
  }
  return <main className="login-page">
    <a className="wordmark" href="/">podible</a>
    <div className="library-toolbar"><h1>{mode === "loading" ? "Plex sign-in" : mode === "result" ? ok ? "Sign-in complete" : "Sign-in failed" : "Sign in"}</h1></div>
    {mode === "loading" ? <p role="status">Connecting to Plex...</p> : mode === "result" ? <>
      <p role={ok ? "status" : "alert"}>{ok ? "Returning to your library..." : error || "Plex sign-in failed. Please try again."}</p>
      <a className="button" href={ok ? destination : `/login?redirectTo=${encodeURIComponent(destination)}`}>{ok ? "Continue" : "Try again"}</a>
    </> : <>
      <p>Use Plex to open your library.</p>
      {params.get("notice") && <p role="status">{params.get("notice")}</p>}
      {error && <p role="alert">{error}</p>}
      <button type="button" disabled={pending} onClick={() => void startLogin()}>{pending ? "Connecting to Plex..." : "Continue with Plex"}</button>
      {status && <p role={pending ? "status" : "alert"}>{status}</p>}
    </>}
  </main>;
}
