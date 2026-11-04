import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium, type BrowserContext, type Page } from "playwright";
import { capturePage } from "./capture";

const server = Bun.spawn([process.execPath, "run", "test/fixtures/spa-server.ts"], {
  stdout: "pipe", stderr: "inherit",
  env: { ...process.env, PORT: "0", AUTH_FIXTURE: "1", ADMIN_FIXTURE: "", TV_FIXTURE: "", SHADOW_FIXTURE: "" },
});
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
try {
  const reader = server.stdout.getReader();
  let output = "", origin = "";
  const deadline = Date.now() + 15_000;
  while (!origin) {
    const part = await Promise.race([reader.read(), Bun.sleep(Math.max(1, deadline - Date.now())).then(() => { throw new Error("Auth fixture startup timed out"); })]);
    output += new TextDecoder().decode(part.value);
    origin = /SPA_FIXTURE_URL=(\S+)/.exec(output)?.[1] ?? "";
    if (part.done && !origin) throw new Error("Auth fixture exited without a URL");
  }
  reader.releaseLock();
  browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
  const pageErrors: string[] = [], networkErrors: string[] = [];
  const networkChecks: Promise<void>[] = [];
  let popups = 0;
  const artifacts = path.resolve("tmp/movies-lab");
  await mkdir(artifacts, { recursive: true });
  const link = (value: string) => new URL(value, origin).href;
  const monitor = (page: Page, mode: string) => {
    page.on("popup", () => popups++);
    page.on("pageerror", error => pageErrors.push(`${mode}: ${error.message}`));
    page.on("requestfailed", request => {
      if (!request.failure()?.errorText.includes("ERR_ABORTED")) networkErrors.push(`${mode}: ${request.url()} ${request.failure()?.errorText}`);
    });
    page.on("response", response => {
      if (!response.url().includes("/api/trpc/") && !response.url().includes("/login/plex/start")) return;
      if (response.status() < 400) return;
      if (response.status() === 401 && ["/api/trpc/auth.me", "/api/trpc/library.bootstrap"].includes(new URL(response.url()).pathname)) {
        networkChecks.push(response.request().allHeaders().then(headers => {
          if (/(?:^|;\s*)podible_session=/.test(headers.cookie ?? "")) networkErrors.push(`${mode}: Authenticated request returned HTTP 401 ${response.url()}`);
        }));
        return;
      }
      if (mode === "start-error" && response.status() === 502 && response.url().includes("/login/plex/start")) return;
      networkErrors.push(`${mode}: HTTP ${response.status()} ${response.url()}`);
    });
  };
  const fixtureContext = async (mode: string, mobile = false) => {
    const context = await browser!.newContext({ viewport: mobile ? { width: 390, height: 844 } : { width: 1280, height: 900 } });
    context.setDefaultTimeout(15_000);
    context.setDefaultNavigationTimeout(15_000);
    context.on("page", page => monitor(page, mode));
    await context.addInitScript("window.open = () => { throw new Error('Login must not attempt a popup'); };");
    // Only the provider page is mocked; Podible login/result/cookie/RPC are real DOM/HTTP.
    await context.route("https://app.plex.tv/**", route => route.fulfill({
      status: 200, contentType: "text/html",
      body: `<!doctype html><html><body><h1>Fixture Plex sign-in</h1><a id="finish" href="#">Finish fixture sign-in</a><script>
        document.getElementById('finish').href = new URLSearchParams(location.hash.replace(/^#\\??/, '')).get('forwardUrl');
      </script></body></html>`,
    }));
    const response = await context.request.post(link("/__fixture/auth-mode"), { data: { mode } });
    assert.equal(response.status(), 200);
    return context;
  };
  const rpcMe = async (context: BrowserContext) => {
    const response = await context.request.post(link("/rpc"), { data: { jsonrpc: "2.0", id: 1, method: "auth.me", params: {} } });
    assert.equal(response.status(), 200);
    return response.json() as Promise<{ result?: { user: { username: string } }; error?: { code: number } }>;
  };
  const startSameWindow = async (page: Page) => {
    await page.getByRole("button", { name: "Continue with Plex", exact: true }).click();
    await page.waitForURL(url => url.origin === "https://app.plex.tv");
    await page.getByRole("heading", { name: "Fixture Plex sign-in", exact: true }).waitFor();
    assert.equal(page.context().pages().length, 1, "Default login created another page");
    assert.equal(await page.opener(), null);
    const callback = await page.getByRole("link", { name: "Finish fixture sign-in", exact: true }).getAttribute("href");
    assert.equal(new URL(callback!).origin, new URL(origin).origin);
    return callback!;
  };
  const verifyLoginBackground = async (page: Page) => {
    await page.evaluate("Promise.all(document.getAnimations().filter(animation => animation.effect?.getTiming().iterations !== Infinity).map(animation => animation.finished.catch(() => {})))");
    const layout = await page.evaluate<{ bodyTop: number; rootTop: number; mainTop: number; rootHeight: number;
      viewport: number; mainMargin: string; bodyBackground: string; rootBackground: string; overflow: boolean }>(`(() => {
      const root = document.getElementById("root"), main = document.querySelector("main.login-page");
      return { bodyTop: document.body.getBoundingClientRect().top, rootTop: root.getBoundingClientRect().top,
        mainTop: main.getBoundingClientRect().top, rootHeight: root.getBoundingClientRect().height,
        viewport: innerHeight, mainMargin: getComputedStyle(main).marginTop,
        bodyBackground: getComputedStyle(document.body).backgroundImage,
        rootBackground: getComputedStyle(root).backgroundColor,
        overflow: document.documentElement.scrollWidth > innerWidth };
    })()`);
    assert.equal(layout.bodyTop, 0, "Login margin moved the body background below the viewport");
    assert.equal(layout.rootTop, 0, "Login margin escaped the root and exposed a top strip");
    assert.equal(layout.mainTop, 0);
    assert.equal(layout.mainMargin, "0px");
    assert.ok(layout.rootHeight >= layout.viewport);
    assert.ok(layout.bodyBackground.includes("radial-gradient"), "Existing body atmosphere was removed");
    assert.equal(layout.rootBackground, "rgba(0, 0, 0, 0)");
    assert.equal(layout.overflow, false);
  };

  const success = await fixtureContext("success"), page = await success.newPage();
  const loginEntry = await page.goto(link("/login?redirectTo=%2Flibrary%3Ffrom%3Dsame-window"));
  assert.equal(loginEntry?.headers()["cache-control"], "no-store");
  await page.getByRole("heading", { name: "Sign in", exact: true }).waitFor();
  assert.equal(await page.getByRole("link", { name: "Continue in this window instead", exact: true }).count(), 0);
  await verifyLoginBackground(page);
  await capturePage(page, path.join(artifacts, "auth-login-desktop.png"));
  await page.setViewportSize({ width: 390, height: 844 });
  await verifyLoginBackground(page);
  await capturePage(page, path.join(artifacts, "auth-login-mobile.png"));
  await startSameWindow(page);
  const callbackResponse = page.waitForResponse(response => new URL(response.url()).pathname === "/__fixture/auth-callback");
  await page.getByRole("link", { name: "Finish fixture sign-in", exact: true }).click();
  const callback = await callbackResponse;
  assert.equal(callback.status(), 303);
  assert.ok((await callback.allHeaders())["set-cookie"]?.includes("HttpOnly"));
  await page.waitForURL(link("/library?from=same-window"));
  await page.getByRole("heading", { name: "Library", exact: true }).waitFor();
  await page.getByRole("heading", { name: "Piranesi", exact: true }).waitFor();
  assert.equal((await rpcMe(success)).result?.user.username, "reader");
  const cookie = (await success.cookies()).find(item => item.name === "podible_session");
  assert.ok(cookie?.value); assert.equal(cookie?.httpOnly, true); assert.equal(cookie?.sameSite, "Lax");
  const signOutResponse = page.waitForResponse(response => new URL(response.url()).pathname === "/logout" && response.request().method() === "POST");
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  assert.equal((await signOutResponse).status(), 303);
  await page.getByRole("heading", { name: "Sign in", exact: true }).waitFor();
  await page.getByRole("status").getByText("Signed out.", { exact: true }).waitFor();
  assert.equal((await success.cookies()).some(item => item.name === "podible_session"), false);
  assert.equal((await rpcMe(success)).error?.code, -32001);
  assert.equal(success.pages().length, 1);
  await success.close();
  console.log("Auth browser: default same-window mobile login, callback cookie, safe redirect and POST logout passed");

  const denied = await fixtureContext("error"), deniedPage = await denied.newPage();
  await deniedPage.goto(link("/login?redirectTo=%2Flibrary"));
  await startSameWindow(deniedPage);
  await deniedPage.getByRole("link", { name: "Finish fixture sign-in", exact: true }).click();
  await deniedPage.getByRole("heading", { name: "Sign-in failed", exact: true }).waitFor();
  await deniedPage.getByRole("alert").getByText("Fixture Plex sign-in denied", { exact: true }).waitFor();
  assert.equal((await rpcMe(denied)).error?.code, -32001);
  await deniedPage.getByRole("link", { name: "Try again", exact: true }).click();
  await deniedPage.getByRole("button", { name: "Continue with Plex", exact: true }).waitFor();
  assert.equal(denied.pages().length, 1);
  await denied.close();

  const noCookie = await fixtureContext("no-cookie"), noCookiePage = await noCookie.newPage();
  await noCookiePage.goto(link("/login?redirectTo=%2Flibrary"));
  await startSameWindow(noCookiePage);
  await noCookiePage.getByRole("link", { name: "Finish fixture sign-in", exact: true }).click();
  await noCookiePage.getByRole("link", { name: "Sign in", exact: true }).waitFor();
  assert.equal((await noCookie.cookies()).some(item => item.name === "podible_session"), false);
  assert.equal((await rpcMe(noCookie)).error?.code, -32001);
  assert.equal(await noCookiePage.getByRole("heading", { name: "Library", exact: true }).count(), 0);
  assert.equal(noCookie.pages().length, 1);
  await noCookie.close();

  for (const [mode, message] of [["start-error", "Fixture could not start Plex sign-in"], ["invalid-origin", "Unexpected Plex sign-in destination."]] as const) {
    const context = await fixtureContext(mode), page = await context.newPage();
    const loginUrl = link("/login"); await page.goto(loginUrl);
    await page.getByRole("button", { name: "Continue with Plex", exact: true }).click();
    await page.getByRole("alert").getByText(message, { exact: true }).waitFor();
    assert.equal(await page.getByRole("button", { name: "Continue with Plex", exact: true }).isEnabled(), true);
    assert.equal(page.url(), loginUrl); assert.equal(context.pages().length, 1);
    assert.equal((await rpcMe(context)).error?.code, -32001);
    await context.close();
  }

  const safe = await fixtureContext("success"), safePage = await safe.newPage();
  await safePage.goto(link("/login?redirectTo=%2F%2Fevil.invalid%2Fsteal"));
  const originalUrl = safePage.url();
  await safePage.evaluate("window.postMessage({ type: 'podible-plex-login', ok: true, redirectTo: '/users' }, location.origin)");
  await safePage.waitForTimeout(100);
  assert.equal(safePage.url(), originalUrl, "Untrusted message changed the same-window login flow");
  const safeCallback = await startSameWindow(safePage);
  assert.equal(new URL(safeCallback).searchParams.get("redirectTo"), "/");
  await safePage.getByRole("link", { name: "Finish fixture sign-in", exact: true }).click();
  await safePage.waitForURL(link("/"));
  await safePage.getByRole("heading", { name: "Library", exact: true }).waitFor();
  await safe.close();

  const timed = await fixtureContext("success"), timedPage = await timed.newPage();
  await timedPage.goto(link("/login"));
  const stalled = Promise.withResolvers<void>();
  await timedPage.route(url => url.pathname === "/login/plex/start", async route => {
    await stalled.promise;
    await route.abort().catch(() => {});
  });
  try {
    const start = Date.now();
    await timedPage.getByRole("button", { name: "Continue with Plex", exact: true }).click();
    await timedPage.getByRole("alert").getByText("Plex sign-in timed out. Please try again.", { exact: true }).waitFor({ timeout: 40_000 });
    assert.ok(Date.now() - start >= 29_000, "Login timeout fired before its 30-second bound");
    assert.equal(await timedPage.getByRole("button", { name: "Continue with Plex", exact: true }).isEnabled(), true);
    assert.equal(timedPage.url(), link("/login")); assert.equal(timed.pages().length, 1);
    assert.equal((await rpcMe(timed)).error?.code, -32001);
  } finally { stalled.resolve(); await timed.unrouteAll({ behavior: "wait" }); await timed.close(); }

  const trailing = await fixtureContext("success"), trailingPage = await trailing.newPage();
  const trailingEntry = await trailingPage.goto(link("/login/?redirectTo=%2Flibrary"));
  assert.equal(trailingEntry?.status(), 200); assert.equal(trailingEntry?.headers()["cache-control"], "no-store");
  await trailingPage.getByRole("heading", { name: "Sign in", exact: true }).waitFor();
  await trailingPage.goto(link("/login/plex/loading"));
  await trailingPage.getByRole("status").getByText("Connecting to Plex...", { exact: true }).waitFor();
  await trailing.close();
  await Promise.all(networkChecks);
  assert.equal(popups, 0, "Authentication opened a popup");
  assert.deepEqual(pageErrors, [], "Auth UI page errors");
  assert.deepEqual(networkErrors, [], "Unexpected auth UI request/HTTP failures");
  console.log("Auth Chrome acceptance passed: same-page URL/callback cookie/redirect/logout, denial/no-cookie/start errors, foreign URL and return-path rejection, ignored messages, real 30-second timeout, mobile root/body background origin, screenshots and no popups. No real Plex login.");
} finally {
  await browser?.close();
  server.kill("SIGTERM");
  await server.exited;
}
