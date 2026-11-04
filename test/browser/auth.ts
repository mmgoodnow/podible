import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium, type BrowserContext, type Page } from "playwright";

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
  const artifacts = path.resolve("tmp/movies-lab");
  await mkdir(artifacts, { recursive: true });
  const link = (value: string) => new URL(value, origin).href;
  const monitor = (page: Page, mode: string) => {
    page.on("pageerror", (error) => pageErrors.push(`${mode}: ${error.message}`));
    page.on("requestfailed", (request) => {
      if (!request.failure()?.errorText.includes("ERR_ABORTED")) networkErrors.push(`${mode}: ${request.url()} ${request.failure()?.errorText}`);
    });
    page.on("response", (response) => {
      if (!response.url().includes("/api/trpc/") && !response.url().includes("/login/plex/start")) return;
      if (response.status() < 400) return;
      if (response.status() === 401 && ["/api/trpc/auth.me", "/api/trpc/library.bootstrap"].includes(new URL(response.url()).pathname)) {
        // The SPA checks its actor before sign-in and after logout; a session-bearing 401 is unexpected.
        networkChecks.push(response.request().allHeaders().then((headers) => {
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
    context.on("page", (page) => monitor(page, mode));
    // Only the provider page is mocked; Podible login/result/cookie/RPC are real DOM/HTTP.
    await context.route("https://app.plex.tv/**", (route) => route.fulfill({
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
  const startPopup = async (page: Page) => {
    const opened = page.waitForEvent("popup");
    await page.getByRole("button", { name: "Continue with Plex", exact: true }).click();
    const popup = await opened;
    await popup.getByRole("heading", { name: "Fixture Plex sign-in", exact: true }).waitFor();
    const href = await popup.getByRole("link", { name: "Finish fixture sign-in", exact: true }).getAttribute("href");
    assert.equal(new URL(href!).origin, new URL(origin).origin);
    return { popup, callback: href! };
  };

  const success = await fixtureContext("success"), page = await success.newPage();
  const loginEntry = await page.goto(link("/login?redirectTo=%2Flibrary%3Ffrom%3Dpopup"));
  assert.equal(loginEntry?.headers()["cache-control"], "no-store", "Login entry must not cache authentication state");
  await page.getByRole("heading", { name: "Sign in", exact: true }).waitFor().catch(async (error) => {
    console.error(JSON.stringify({ route: page.url(), status: loginEntry?.status(), body: await page.locator("body").innerText(), pageErrors, networkErrors }));
    throw error;
  });
  await page.screenshot({ path: path.join(artifacts, "auth-login-desktop.png"), animations: "disabled" });
  const loading = await success.newPage();
  await loading.goto(link("/login/plex/loading"));
  await loading.getByRole("heading", { name: "Plex sign-in", exact: true }).waitFor();
  await loading.getByRole("status").getByText("Opening the Plex sign-in window...", { exact: true }).waitFor();
  await loading.close();
  const opened = await startPopup(page);
  const callbackResponse = opened.popup.waitForResponse((response) => new URL(response.url()).pathname === "/__fixture/auth-callback");
  const popupClosed = opened.popup.waitForEvent("close");
  await opened.popup.getByRole("link", { name: "Finish fixture sign-in", exact: true }).click({ noWaitAfter: true });
  const callback = await callbackResponse;
  assert.equal(callback.status(), 303);
  assert.ok((await callback.allHeaders())["set-cookie"]?.includes("HttpOnly"));
  await popupClosed;
  await page.waitForURL(link("/library?from=popup"));
  await page.getByRole("heading", { name: "Library", exact: true }).waitFor();
  await page.getByRole("heading", { name: "Piranesi", exact: true }).waitFor();
  assert.equal((await rpcMe(success)).result?.user.username, "reader");
  const cookie = (await success.cookies()).find((item) => item.name === "podible_session");
  assert.ok(cookie?.value);
  assert.equal(cookie?.httpOnly, true);
  assert.equal(cookie?.sameSite, "Lax");
  const signOutResponse = page.waitForResponse((response) => new URL(response.url()).pathname === "/logout" && response.request().method() === "POST");
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  const signOut = await signOutResponse;
  assert.equal(signOut.status(), 303);
  assert.ok((await signOut.allHeaders()).location.startsWith("/login"));
  await page.getByRole("heading", { name: "Sign in", exact: true }).waitFor();
  await page.getByRole("status").getByText("Signed out.", { exact: true }).waitFor();
  assert.equal((await success.cookies()).some((item) => item.name === "podible_session"), false);
  assert.equal((await rpcMe(success)).error?.code, -32001, "POST logout did not revoke the session");
  console.log("Auth browser: popup callback, shared cookie and postMessage return passed");
  await success.close();

  const denied = await fixtureContext("error"), deniedPage = await denied.newPage();
  await deniedPage.goto(link("/login?redirectTo=%2Flibrary"));
  const deniedPopup = await startPopup(deniedPage);
  await deniedPopup.popup.getByRole("link", { name: "Finish fixture sign-in", exact: true }).click({ noWaitAfter: true });
  await deniedPopup.popup.getByRole("heading", { name: "Sign-in failed", exact: true }).waitFor();
  await deniedPopup.popup.getByRole("alert").getByText("Fixture Plex sign-in denied", { exact: true }).waitFor();
  await deniedPage.getByText("Fixture Plex sign-in denied", { exact: true }).waitFor();
  assert.equal(await deniedPage.getByRole("button", { name: "Continue with Plex", exact: true }).isEnabled(), true);
  assert.equal((await rpcMe(denied)).error?.code, -32001);
  await deniedPopup.popup.close();
  await denied.close();

  const noCookie = await fixtureContext("no-cookie"), noCookiePage = await noCookie.newPage();
  await noCookiePage.goto(link("/login?redirectTo=%2Flibrary"));
  const cookieLessPopup = await startPopup(noCookiePage);
  await cookieLessPopup.popup.getByRole("link", { name: "Finish fixture sign-in", exact: true }).click({ noWaitAfter: true });
  await noCookiePage.getByRole("link", { name: "Sign in", exact: true }).waitFor();
  assert.equal((await noCookie.cookies()).some((item) => item.name === "podible_session"), false);
  assert.equal((await rpcMe(noCookie)).error?.code, -32001, "A success message without a cookie granted access");
  assert.equal(await noCookiePage.getByRole("heading", { name: "Library", exact: true }).count(), 0);
  await noCookie.close();
  console.log("Auth browser: denied and cookie-less callbacks remain unauthenticated");

  const messages = await fixtureContext("success"), messagePage = await messages.newPage();
  await messagePage.goto(link("/login?redirectTo=%2Flibrary"));
  const messagePopup = await startPopup(messagePage), loginUrl = messagePage.url();
  assert.equal(await messagePopup.popup.evaluate("Boolean(window.opener)"), true, "Provider popup lost its opener");
  await messagePopup.popup.evaluate("window.opener.postMessage({type:'podible-plex-login',ok:true,redirectTo:'/users'}, '*')");
  await messagePage.evaluate("window.postMessage({type:'podible-plex-login',ok:true,redirectTo:'/ops'}, location.origin)");
  await messagePage.waitForTimeout(150);
  assert.equal(messagePage.url(), loginUrl, "Wrong-origin/source login message redirected the page");
  assert.equal((await rpcMe(messages)).error?.code, -32001);
  // Browser-initiated page.goto may sever the opener across origins; use in-page navigation like Plex does.
  await messagePopup.popup.evaluate(`location.assign(${JSON.stringify(link("/login/plex/loading"))})`);
  await messagePopup.popup.getByRole("heading", { name: "Plex sign-in", exact: true }).waitFor();
  assert.equal(await messagePopup.popup.evaluate("Boolean(window.opener)"), true, "Returning popup lost its opener");
  await messagePopup.popup.evaluate("window.opener.postMessage({type:'not-a-login',ok:true,redirectTo:'/users'}, location.origin)");
  await messagePage.waitForTimeout(100);
  assert.equal(messagePage.url(), loginUrl, "Unrelated same-origin message redirected the page");
  await messagePopup.popup.evaluate(`location.assign(${JSON.stringify(messagePopup.callback)})`).catch((error: Error) => { if (!/closed/i.test(error.message)) throw error; });
  await messagePage.waitForURL(link("/library"));
  await messagePage.getByRole("heading", { name: "Library", exact: true }).waitFor();
  await messages.close();
  console.log("Auth browser: external-origin, wrong-source and unrelated messages ignored");

  const blocked = await fixtureContext("success", true);
  await blocked.addInitScript("window.open = () => null;");
  const blockedPage = await blocked.newPage();
  await blockedPage.goto(link("/login?redirectTo=%2Flibrary%3Ffrom%3Dinline"));
  const startsBefore = (await (await blocked.request.get(link("/__fixture/auth-state"))).json()).starts;
  await blockedPage.getByRole("button", { name: "Continue with Plex", exact: true }).click();
  await blockedPage.getByText("Popup blocked. Allow popups or continue in this window.", { exact: true }).waitFor();
  assert.equal((await (await blocked.request.get(link("/__fixture/auth-state"))).json()).starts, startsBefore, "Blocked popup still started provider work");
  assert.equal(await blockedPage.getByRole("button", { name: "Continue with Plex", exact: true }).isEnabled(), true);
  assert.equal(await blockedPage.evaluate("document.documentElement.scrollWidth > innerWidth"), false);
  await blockedPage.screenshot({ path: path.join(artifacts, "auth-blocked-mobile.png"), animations: "disabled" });
  await blockedPage.getByRole("link", { name: "Continue in this window instead", exact: true }).click();
  await blockedPage.waitForURL((url) => url.pathname === "/login" && url.searchParams.get("inline") === "1");
  await blockedPage.getByRole("button", { name: "Continue with Plex", exact: true }).click();
  await blockedPage.getByRole("heading", { name: "Fixture Plex sign-in", exact: true }).waitFor();
  assert.equal(blocked.pages().length, 1, "Inline sign-in opened a popup");
  await blockedPage.getByRole("link", { name: "Finish fixture sign-in", exact: true }).click();
  await blockedPage.waitForURL(link("/library?from=inline"));
  await blockedPage.getByRole("heading", { name: "Library", exact: true }).waitFor();
  assert.equal((await rpcMe(blocked)).result?.user.username, "reader");
  await blocked.close();
  console.log("Auth browser: blocked-popup recovery and inline sign-in passed");

  for (const [mode, message] of [["start-error", "Fixture could not start Plex sign-in"], ["invalid-origin", "Unexpected Plex sign-in destination."]] as const) {
    const context = await fixtureContext(mode), page = await context.newPage();
    await page.goto(link("/login"));
    await page.getByRole("button", { name: "Continue with Plex", exact: true }).click();
    await page.getByText(message, { exact: true }).waitFor();
    assert.equal(await page.getByRole("button", { name: "Continue with Plex", exact: true }).isEnabled(), true);
    assert.equal((await rpcMe(context)).error?.code, -32001);
    await context.close();
  }
  const safe = await fixtureContext("success"), safePage = await safe.newPage();
  await safePage.goto(link("/login?redirectTo=%2F%2Fevil.invalid%2Fsteal&inline=1"));
  await safePage.getByRole("button", { name: "Continue with Plex", exact: true }).click();
  await safePage.getByRole("heading", { name: "Fixture Plex sign-in", exact: true }).waitFor();
  const safeCallback = await safePage.getByRole("link", { name: "Finish fixture sign-in", exact: true }).getAttribute("href");
  assert.equal(new URL(safeCallback!).searchParams.get("redirectTo"), "/");
  await safePage.getByRole("link", { name: "Finish fixture sign-in", exact: true }).click();
  await safePage.waitForURL(link("/"));
  await safePage.getByRole("heading", { name: "Library", exact: true }).waitFor();
  await safe.close();
  await Promise.all(networkChecks);
  assert.deepEqual(pageErrors, [], "Auth UI page errors");
  assert.deepEqual(networkErrors, [], "Unexpected auth UI request/HTTP failures");
  console.log("Auth browser: canonical login/logout, errors, redirect safety and clean runtime/network passed");
  const trailing = await fixtureContext("success"), trailingPage = await trailing.newPage();
  const trailingEntry = await trailingPage.goto(link("/login/?redirectTo=%2Flibrary"));
  assert.equal(trailingEntry?.status(), 200, "Trailing-slash login must reach the SPA, not HTTP 404");
  assert.equal(trailingEntry?.headers()["cache-control"], "no-store");
  await trailingPage.getByRole("heading", { name: "Sign in", exact: true }).waitFor();
  await trailing.close();
  await Promise.all(networkChecks);
  assert.deepEqual(pageErrors, [], "Auth UI page errors");
  assert.deepEqual(networkErrors, [], "Unexpected auth UI request/HTTP failures");
  console.log("Auth Chrome acceptance passed: SPA login/loading/result, fixture popup/callback cookie/postMessage, POST sign-out/session revocation, errors/no-cookie, ignored untrusted messages, blocked popup, inline flow, redirect safety, mobile and clean runtime/network. No real Plex login.");
} finally {
  await browser?.close();
  server.kill("SIGTERM");
  await server.exited;
}
