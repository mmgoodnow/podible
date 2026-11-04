import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium } from "playwright";

const server = Bun.spawn([process.execPath, "run", "test/fixtures/spa-server.ts"], {
  stdout: "pipe", stderr: "inherit", env: { ...process.env, PORT: "0", TV_FIXTURE: "1", SHADOW_FIXTURE: "", ADMIN_FIXTURE: "", AUTH_FIXTURE: "" },
});
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
try {
  const reader = server.stdout.getReader();
  let output = "", url = "";
  const deadline = Date.now() + 15_000;
  while (!url) {
    const part = await Promise.race([reader.read(), Bun.sleep(Math.max(1, deadline - Date.now())).then(() => { throw new Error("TV fixture startup timed out"); })]);
    output += new TextDecoder().decode(part.value);
    url = /SPA_FIXTURE_URL=(\S+)/.exec(output)?.[1] ?? "";
    if (part.done && !url) throw new Error("TV fixture exited without a URL");
  }
  reader.releaseLock();
  browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  context.setDefaultTimeout(15_000);
  context.setDefaultNavigationTimeout(15_000);
  await context.addCookies([{ name: "podible_session", value: "spa-browser-fixture", url, httpOnly: true }]);
  const rpc = async (method: string, params: Record<string, unknown> = {}, request = context.request) => {
    const response = await request.post(new URL("/rpc", url).href, { data: { jsonrpc: "2.0", id: 1, method, params } });
    assert.equal(response.status(), 200);
    return response.json() as Promise<{ result?: any; error?: { code: number } }>;
  };
  const anonymous = await browser.newContext();
  const readerContext = await browser.newContext();
  await readerContext.addCookies([{ name: "podible_session", value: "tv-reader-fixture", url, httpOnly: true }]);
  for (const method of ["tv.status", "tv.list", "tv.get", "tv.sync", "tv.backfill", "tv.retry", "tvShadow.status", "tvShadow.list", "tvShadow.sync"]) {
    assert.equal((await rpc(method, {}, anonymous.request)).error?.code, -32001);
    assert.equal((await rpc(method, {}, readerContext.request)).error?.code, -32003);
  }
  assert.equal((await anonymous.request.get(new URL("/api/trpc/tv.status?input=%7B%7D", url).href)).status(), 401);
  assert.equal((await readerContext.request.get(new URL("/api/trpc/tv.status?input=%7B%7D", url).href)).status(), 403);
  const readerPage = await readerContext.newPage();
  await readerPage.goto(new URL("/tv-sync", url).href);
  await readerPage.getByRole("alert").getByText("Administrator access required.").waitFor();
  assert.equal(await readerPage.getByRole("heading", { name: "Independent TV acquisition", exact: true }).count(), 0);

  const page = await context.newPage(), pageErrors: string[] = [], networkErrors: string[] = [], detailRequests: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("requestfailed", (request) => networkErrors.push(`${request.method()} ${request.url()}: ${request.failure()?.errorText}`));
  page.on("response", (response) => { if (response.url().includes("/api/trpc/") && response.status() >= 400) networkErrors.push(`${response.status()} ${response.url()}`); });
  page.on("request", (request) => { if (new URL(request.url()).pathname.split("/").at(-1)?.split(",").includes("tv.get")) detailRequests.push(request.url()); });
  await page.goto(new URL("/tv-sync", url).href);
  await page.getByRole("heading", { name: "TV sync", exact: true }).waitFor();
  await page.getByRole("heading", { name: "Independent TV acquisition", exact: true }).waitFor();
  await page.getByRole("heading", { name: "Sonarr comparison audit", exact: true }).waitFor();
  const primaryNavigation = page.getByRole("navigation", { name: "Main navigation" });
  assert.equal(await primaryNavigation.getByRole("link", { name: "Activity", exact: true }).getAttribute("href"), "/ops");
  assert.equal(await primaryNavigation.getByRole("link", { name: "TV sync", exact: true }).count(), 0);
  const native = page.getByRole("region", { name: "Native TV acquisition", exact: true });
  const audit = page.getByRole("region", { name: "TV comparison cases", exact: true });
  await native.locator("summary").filter({ hasText: "1/2 episodes available" }).waitFor();
  await audit.locator("summary").filter({ hasText: "Would import" }).waitFor();
  assert.equal(detailRequests.length, 0, "Collapsed native rows requested per-request details");
  assert.equal(await native.getByText("Would import", { exact: false }).count(), 0, "Audit verdict became native coverage");
  await native.locator("summary").filter({ hasText: "1/2 episodes available" }).click();
  await native.getByText("1 of 2 requested episodes available", { exact: true }).waitFor();
  await native.getByText("Acquisition #1 selection and episode decisions", { exact: true }).click();
  await native.getByText("native-selected-file.mkv", { exact: false }).waitFor();
  await native.getByText("Acquisition events (2)", { exact: true }).click();
  await native.getByText("One native episode available; future episode intent retained", { exact: true }).waitFor();
  await audit.locator("summary").filter({ hasText: "Would import" }).click();
  await audit.getByText("Entry relationships and provider numbering", { exact: true }).click();
  await audit.getByRole("listitem").filter({ hasText: "Fixture Prequel" }).waitFor();
  await audit.getByRole("cell").filter({ hasText: "tvdb:aired" }).waitFor();
  await audit.getByRole("cell").filter({ hasText: /^anidb/ }).waitFor();
  await audit.getByText("sonarr:fixture:queue:1", { exact: true }).waitFor();
  await audit.getByText("Captured file maps and probe evidence", { exact: true }).click();
  await audit.getByText('"codec_name": "h264"', { exact: false }).waitFor();
  for (const secret of ["tv-fixture-seerr-private", "tv-fixture-agent-private", "tv-fixture-sonarr-private"])
    assert.ok(!(await page.locator("body").innerText()).includes(secret), "Server credentials reached the page");

  await native.getByText("Approved-season backfill", { exact: true }).click();
  const ids = native.getByLabel("Seerr TV request IDs", { exact: true });
  const acquire = native.getByRole("button", { name: "Acquire approved seasons", exact: true });
  assert.equal(await native.getByLabel("Episode limit per request", { exact: true }).count(), 0, "Episode-level scope is still offered");
  assert.equal(await native.getByRole("spinbutton").count(), 0, "Episode cap was replaced by another numeric control");
  await native.getByText("Each includes all episodes in its explicitly approved seasons", { exact: false }).waitFor();
  await native.getByText("Approval is season-level only", { exact: false }).waitFor();
  assert.equal(await acquire.isDisabled(), true);
  for (const value of ["726,724,723,721,720,719", "726,726", "726, episodeLimit:1", "726, /etc/passwd"]) {
    await ids.fill(value);
    assert.equal(await acquire.isDisabled(), true, `Untrusted request scope accepted: ${value}`);
  }
  await ids.fill("726");
  assert.equal(await acquire.isEnabled(), true);
  const mutation = async (method: string, action: () => Promise<unknown>) => {
    const response = page.waitForResponse((item) => new URL(item.url()).pathname.endsWith(method) && item.request().method() === "POST");
    await action();
    const result = await response;
    assert.equal(result.status(), 200);
    const body = await result.json(), value = (Array.isArray(body) ? body[0] : body).result.data;
    assert.ok(Number.isSafeInteger(value.jobId));
    if (method === "tv.retry") {
      const request = (await rpc("tv.list")).result.items[0];
      const acquisition = request.acquisitions.find((item: { jobId: number }) => item.jobId === value.jobId);
      assert.equal(acquisition?.status, "queued", "Fixture must not start real native recovery");
    } else {
      const status = (await rpc("tv.status")).result;
      assert.equal(status.lastSync.jobId, value.jobId);
      assert.equal(status.lastSync.status, "queued", "Fixture must not start real native sync");
    }
    const posted = JSON.parse(result.request().postData()!);
    return { id: value.jobId, input: posted[0] ?? posted };
  };
  const backfill = await mutation("tv.backfill", () => acquire.click());
  assert.deepEqual(backfill.input, { backfillRequestIds: [726] });
  await native.getByText(`Explicit native backfill queued as job #${backfill.id}`, { exact: false }).waitFor();
  const sync = await mutation("tv.sync", () => native.getByRole("button", { name: "Sync new TV requests", exact: true }).click());
  await native.getByText(`Native request sync queued as job #${sync.id}`, { exact: false }).waitFor();
  const recovery = await mutation("tv.retry", () => native.getByRole("button", { name: "Re-request seasons", exact: true }).click());
  await native.getByText(`Native recovery queued as job #${recovery.id}`, { exact: false }).waitFor();
  assert.equal((await rpc("tv.list")).result.items[0].availableEpisodes, 1, "Queued work changed episode availability");
  await native.getByText("1 of 2 requested episodes available", { exact: true }).waitFor();

  const lastSync = (await rpc("tv.status")).result.lastSync;
  for (const invalid of [{ backfillRequestIds: [726], episodeLimit: 1 }, { backfillRequestIds: [726], episodeIds: [1] },
    { backfillRequestIds: [726], path: "/etc/passwd" }]) {
    assert.equal((await rpc("tv.backfill", invalid)).error?.code, -32602);
    const rejected = await context.request.post(new URL("/api/trpc/tv.backfill", url).href, { data: invalid });
    assert.equal(rejected.status(), 400);
  }
  assert.deepEqual((await rpc("tv.status")).result.lastSync, lastSync, "Rejected episode/path scope still queued work");
  assert.equal((await rpc("tvShadow.sync", { sourcePaths: ["/etc/passwd"] })).error?.code, -32602);
  assert.equal((await rpc("tv.readPath", { path: "/etc/passwd" })).error?.code, -32601);
  for (const pathname of ["/api/trpc/tv.missing", "/tv-sync/arbitrary-path", "/web-assets/tv-missing.js"])
    assert.equal((await context.request.get(new URL(pathname, url).href)).status(), 404, `${pathname} became an arbitrary route or SPA success`);
  await audit.getByText("Captured file maps and probe evidence", { exact: true }).click();
  const artifacts = path.resolve("tmp/movies-lab");
  await mkdir(artifacts, { recursive: true });
  await page.screenshot({ path: path.join(artifacts, "tv-sync-desktop.png"), fullPage: true, animations: "disabled" });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"), false, "TV page overflows mobile viewport");
  await ids.scrollIntoViewIfNeeded();
  assert.equal(await acquire.isVisible(), true, "Mobile approved-season action is inaccessible");
  assert.equal(await native.getByRole("spinbutton").count(), 0, "Mobile UI still offers an episode cap");
  await page.screenshot({ path: path.join(artifacts, "tv-sync-mobile.png"), animations: "disabled" });
  await audit.getByText("Entry relationships and provider numbering", { exact: true }).scrollIntoViewIfNeeded();
  await page.screenshot({ path: path.join(artifacts, "tv-sync-mobile-audit.png"), animations: "disabled" });
  assert.deepEqual(pageErrors, [], "TV UI runtime errors");
  assert.deepEqual(networkErrors, [], "TV UI request/HTTP errors");
  console.log("TV browser acceptance passed: real auth/RPC, native coverage versus audit, explicit anime maps, complete approved-season backfill with no episode cap, rejected episode-level API/UI input, queued sync/retry, safe routes, desktop/mobile and clean runtime/network.");
} finally {
  await browser?.close();
  server.kill("SIGTERM");
  await server.exited;
}
