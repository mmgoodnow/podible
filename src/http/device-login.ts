import { Hono } from "hono";
import { bodyLimit } from "hono/body-limit";
import { getCookie, setCookie } from "hono/cookie";
import { html } from "hono/html";
import { hashSessionToken } from "../auth";
import { deviceLoginIdSchema, deviceLoginSecretSchema } from "../rpc/auth-router";
import type { BooksRepo } from "../repo";
import { getCurrentSession, type HttpEnv } from "./middleware";

function page(title: string, body: ReturnType<typeof html>) {
  return html`<!doctype html><html lang="en"><meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>${title} | Podible</title>
    <style>
    body{color:#e8eae8;background:#151817;font:1.125rem/1.5 ui-sans-serif,sans-serif;margin:0}
    main{max-width:32rem;padding:2rem;margin:8vh auto}
    label{display:block} input,button{font:inherit;padding:.65rem;margin:.5rem 0;border-radius:.4rem}
    input{width:9rem} button{display:block;background:#b7e8cc;color:#152319;border:0}
    </style>
    <main><h1>${title}</h1>${body}</main></html>`;
}

export function createDeviceLoginRoutes(repo: BooksRepo) {
  const app = new Hono<HttpEnv>();
  const unavailable = page("Sign-in unavailable", html`<p>Scan a new QR code on the TV to start again.</p>`);
  const approvalCookie = (id: string) => `podible_tv_approval_${id}`;
  app.use("*", async (c, next) => {
    c.header("Cache-Control", "no-store");
    c.header("Referrer-Policy", "no-referrer");
    c.header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'");
    await next();
  });
  app.get("/", (c) => {
    const id = deviceLoginIdSchema.safeParse(c.req.query("id"));
    if (!id.success) return c.html(unavailable, 404);
    const grant = repo.getPendingDeviceLogin(id.data);
    if (!grant) return c.html(unavailable, 404);
    const path = `/auth/device?id=${id.data}`;
    if (grant.qrOnly) {
      const scanned = c.req.query("approval");
      const secret = deviceLoginSecretSchema.safeParse(scanned ?? getCookie(c, approvalCookie(id.data)));
      if (!secret.success || !repo.deviceLoginAcceptsQr(id.data, hashSessionToken(secret.data)))
        return c.html(unavailable, 404);
      if (scanned !== undefined) {
        // Keep the approval secret out of the Plex redirect and subsequent browser URLs.
        setCookie(c, approvalCookie(id.data), secret.data, {
          httpOnly: true, sameSite: "Lax", path: "/auth/device", maxAge: 300,
          secure: new URL(c.req.url).protocol === "https:" || c.req.header("x-forwarded-proto") === "https",
        });
        return c.redirect(path, 303);
      }
    }
    const session = getCurrentSession(c);
    const signIn = `/login?${new URLSearchParams({ redirectTo: path })}`;
    if (session?.kind !== "browser") return c.redirect(signIn, 303);
    const machineId = repo.getSettings().auth.plex.machineId;
    const linked = repo.deviceLoginHasPlexAccess(id.data, session.id, grant.qrOnly ? machineId : undefined);
    // no-referrer makes browsers serialize Origin as null for a native form POST.
    c.header("Referrer-Policy", "same-origin");
    if (grant.qrOnly) return c.html(page("Connect Apple TV", html`
      <p>You are signed in as <strong>${session.display_name || session.username}</strong>.</p>
      ${machineId && !linked ? html`<p>To connect this TV, sign in with a Plex account that has access to this server.</p>
        <a style="color:#b7e8cc" href="${signIn}">Sign in with Plex</a>` : html`
        <p>Only approve if you scanned this QR code on your TV.</p>
        <form method="post" action="${path}">
          <button type="submit">Approve sign-in</button>
        </form>`}`));
    return c.html(page("Connect Apple TV", html`
      <p>You are signed in as <strong>${session.display_name || session.username}</strong>. Enter the code shown on your TV. Do not enter a code sent to you by someone else.</p>
      <form method="post" action="${path}">
        <label for="code">TV code</label>
        <input id="code" name="code" type="text" inputmode="numeric" pattern="[0-9]{6}" maxlength="6" autocomplete="off" required>
        <button type="submit">Approve sign-in</button>
      </form>
      ${linked ? html`<p>This TV will use your Plex access for playback.</p>` : html`
        <p><a style="color:#b7e8cc" href="${signIn}">Sign in with Plex to enable playback</a>, or approve now to browse and request.</p>`}`));
  });
  app.post("/", bodyLimit({ maxSize: 256 }), async (c) => {
    const session = getCurrentSession(c);
    const external = new URL(c.req.url);
    const proto = c.req.header("x-forwarded-proto");
    if (proto === "https" || proto === "http") external.protocol = `${proto}:`;
    if (session?.kind !== "browser" || c.req.header("origin") !== external.origin || c.req.header("sec-fetch-site") === "cross-site")
      return c.text("Sign-in approval requires a same-origin browser session.", 403);
    if (!c.req.header("content-type")?.startsWith("application/x-www-form-urlencoded")) return c.text("Invalid form", 400);
    const id = deviceLoginIdSchema.safeParse(c.req.query("id"));
    if (!id.success) return c.html(unavailable, 404);
    const grant = repo.getPendingDeviceLogin(id.data);
    if (!grant) return c.html(unavailable, 404);
    if (grant.qrOnly) {
      const secret = deviceLoginSecretSchema.safeParse(getCookie(c, approvalCookie(id.data)));
      const machineId = repo.getSettings().auth.plex.machineId || null;
      if (!secret.success || !repo.deviceLoginAcceptsQr(id.data, hashSessionToken(secret.data)))
        return c.html(unavailable, 404);
      if (!repo.approveDeviceLoginQr(id.data, hashSessionToken(secret.data), session.user_id, session.id, machineId))
        return c.html(page("Could not approve sign-in", html`<p>Sign in with a Plex account that can play from this server, then try again.</p>
          <a style="color:#b7e8cc" href="${`/auth/device?id=${id.data}`}">Back to approval</a>`), 403);
      return c.html(page("Apple TV connected", html`<p>Return to the TV. You can close this page.</p>`));
    }
    const form = await c.req.parseBody();
    if (typeof form.code !== "string" || !/^\d{6}$/.test(form.code))
      return c.text("Invalid code", 400);
    if (!repo.approveDeviceLogin(id.data, form.code, session.user_id, session.id))
      return c.html(page("Could not approve sign-in", html`<p>The code is incorrect or expired. Return to the previous page to try again, or start a new sign-in on the TV.</p>`), 400);
    return c.html(page("Apple TV connected", html`<p>Return to the TV. You can close this page.</p>`));
  });
  return app;
}
