import { chromium } from "playwright";
import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";

// The diagnostic APIs are covered by diagnostic-api.test.ts; this suite covers their retired URLs.
const server = Bun.spawn([process.execPath, "run", "test/fixtures/spa-server.ts"], {
  stdout: "pipe", stderr: "inherit", env: { ...process.env, PORT: "0", SHADOW_FIXTURE: "1", TV_FIXTURE: "1",
    ADMIN_FIXTURE: "", AUTH_FIXTURE: "", FIXTURE_TLS_KEY: "", FIXTURE_TLS_CERT: "" },
});
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
try {
  const reader = server.stdout.getReader();
  let output = "", url = "";
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const timeout = new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("Retired routes fixture startup timed out")),15_000); });
    while (!url) {
      const part = await Promise.race([reader.read(),timeout]);
      output += new TextDecoder().decode(part.value);
      url = /SPA_FIXTURE_URL=(\S+)/.exec(output)?.[1] ?? "";
      if (part.done && !url) throw new Error("Retired routes fixture exited without a URL");
    }
  } finally { clearTimeout(timer); reader.releaseLock(); }
  browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  context.setDefaultTimeout(15_000);
  await context.addCookies([{ name: "podible_session", value: "spa-browser-fixture", url, httpOnly: true }]);
  const page = await context.newPage(), errors: string[] = [], diagnostics: string[] = [], networkErrors: string[] = [];
  page.on("pageerror", error => errors.push(error.message));
  page.on("request", request => {
    if (/(?:movieShadow|movieDualWrite|tvShadow)\./.test(new URL(request.url()).pathname)) diagnostics.push(request.url());
  });
  page.on("requestfailed", request => networkErrors.push(`${request.method()} ${request.url()}: ${request.failure()?.errorText}`));
  page.on("response", response => { if (response.url().includes("/api/trpc/") && response.status() >= 400) networkErrors.push(`${response.status()} ${response.url()}`); });
  // Keep the dashboard request behind the shell to exercise navigation-before-load races.
  await page.route("**/api/trpc/**", async route => {
    if (/transferMetrics\.(status|library)/.test(new URL(route.request().url()).pathname)) await Bun.sleep(150);
    await route.continue();
  });
  for (const retired of ["/movie-dual-write", "/movie-shadow?history=1", "/movie-shadow/1", "/tv-sync?targetId=1",
    "/app/movie-dual-write/", "/app/movie-shadow/1/", "/app/tv-sync"]) {
    const redirect = await context.request.get(new URL(retired,url).href,{ maxRedirects: 0 });
    assert.equal(redirect.status(),308);
    assert.equal(redirect.headers().location,"/ops");
    assert.ok(!(await redirect.text()).includes('<div id="root">'),"Retired diagnostic URL rendered a page");
    const [metricsResponse, inventoryResponse] = await Promise.all([
      page.waitForResponse(response => new URL(response.url()).pathname.includes("/api/trpc/") &&
        new URL(response.url()).pathname.includes("transferMetrics.status")),
      page.waitForResponse(response => new URL(response.url()).pathname.includes("/api/trpc/") &&
        new URL(response.url()).pathname.includes("transferMetrics.library")),
      page.goto(new URL(retired,url).href),
    ]);
    assert.equal(metricsResponse.status(),200,"Activity metrics request failed");
    assert.equal(await metricsResponse.finished(),null,"Activity metrics response did not finish");
    const payload = await metricsResponse.json();
    assert.ok((Array.isArray(payload) ? payload : [payload]).every(item => item.result && !item.error),
      "Activity metrics returned an RPC error");
    assert.equal(inventoryResponse.status(),200,"Library inventory request failed");
    assert.equal(await inventoryResponse.finished(),null,"Library inventory response did not finish");
    const inventoryPayload = await inventoryResponse.json();
    assert.ok((Array.isArray(inventoryPayload) ? inventoryPayload : [inventoryPayload]).every(item => item.result && !item.error),
      "Library inventory returned an RPC error");
    assert.equal(new URL(page.url()).pathname,"/ops");
    assert.equal(new URL(page.url()).search,"");
    await page.getByRole("heading",{ name: "Activity", exact: true }).waitFor();
    const dashboard = page.getByRole("region",{ name: "Sharing and transfers" });
    await dashboard.waitFor();
    await dashboard.getByText("Loading transfer metrics...",{ exact: true }).waitFor({ state: "hidden" });
    assert.equal(await dashboard.getByRole("alert").count(),0,"Activity metrics failed to render");
    for (const label of ["Movie sync","TV sync","Radarr comparison audit","Sonarr comparison audit","Seerr dual-write"])
      assert.equal(await page.getByText(label,{ exact: true }).count(),0,`${label} remains in the Activity UI`);
  }
  await page.evaluate("history.pushState(null, '', '/tv-sync?targetId=1'); window.dispatchEvent(new PopStateEvent('popstate'))");
  await page.waitForURL(new URL("/ops",url).href);
  await page.getByRole("heading",{ name: "Activity", exact: true }).waitFor();
  const artifacts = path.resolve("tmp/movies-lab");
  await mkdir(artifacts,{ recursive: true });
  await page.screenshot({ path: path.join(artifacts,"retired-diagnostics-desktop.png"), fullPage: true, animations: "disabled" });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"),false,"Activity overflows mobile viewport");
  await page.screenshot({ path: path.join(artifacts,"retired-diagnostics-mobile.png"), fullPage: true, animations: "disabled" });
  await page.waitForTimeout(5500);
  assert.deepEqual(diagnostics,[],"Retired diagnostics are still fetched or polled");
  assert.deepEqual(errors,[],"Retired-route browser runtime errors");
  assert.deepEqual(networkErrors,[],"Retired-route browser request errors");
  console.log("Retired diagnostics browser acceptance passed: HTTP/app aliases and stale client links redirect to Activity, no raw pages or diagnostic polling, desktop/mobile screenshots and clean runtime/network.");
} finally {
  await browser?.close();
  server.kill("SIGTERM");
  const timer = setTimeout(() => server.kill("SIGKILL"),5000);
  try { await server.exited; } finally { clearTimeout(timer); }
}
