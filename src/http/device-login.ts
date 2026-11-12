import { Hono } from "hono";
import { bodyLimit } from "hono/body-limit";
import { html } from "hono/html";
import { deviceLoginIdSchema } from "../rpc/auth-router";
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
  app.use("*", async (c, next) => {
    c.header("Cache-Control", "no-store");
    c.header("Referrer-Policy", "no-referrer");
    c.header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'");
    await next();
  });
  app.get("/", (c) => {
    const id = deviceLoginIdSchema.safeParse(c.req.query("id"));
    if (!id.success || !repo.deviceLoginNeedsApproval(id.data))
      return c.html(page("Sign-in unavailable", html`<p>Start a new sign-in on the TV.</p>`), 404);
    const session = getCurrentSession(c);
    const path = `/auth/device?id=${id.data}`;
    if (session?.kind !== "browser") return c.redirect(`/login?${new URLSearchParams({ redirectTo: path })}`, 303);
    return c.html(page("Connect Apple TV", html`
      <p>You are signed in as <strong>${session.display_name || session.username}</strong>. Enter the code shown on your TV. Do not enter a code sent to you by someone else.</p>
      <form method="post" action="${path}">
        <label for="code">TV code</label>
        <input id="code" name="code" type="text" inputmode="numeric" pattern="[0-9]{6}" maxlength="6" autocomplete="off" required>
        <button type="submit">Approve sign-in</button>
      </form>`));
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
    const form = await c.req.parseBody();
    if (!id.success || typeof form.code !== "string" || !/^\d{6}$/.test(form.code))
      return c.text("Invalid code", 400);
    if (!repo.approveDeviceLogin(id.data, form.code, session.user_id))
      return c.html(page("Could not approve sign-in", html`<p>The code is incorrect or expired. Return to the previous page to try again, or start a new sign-in on the TV.</p>`), 400);
    return c.html(page("Apple TV connected", html`<p>Return to the TV. You can close this page.</p>`));
  });
  return app;
}
