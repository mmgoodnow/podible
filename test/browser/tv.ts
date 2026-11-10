import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium, type Page } from "playwright";
import { createTRPCClient, httpLink } from "@trpc/client";
import type { AppRouter } from "../../src/trpc";

const server = Bun.spawn([process.execPath,"run","test/fixtures/spa-server.ts"],{
  stdout: "pipe", stderr: "inherit", env: { ...process.env, PORT: "0", TV_FIXTURE: "1", TV_SEARCH_FIXTURE: "1", SHADOW_FIXTURE: "",
    ADMIN_FIXTURE: "", AUTH_FIXTURE: "", FIXTURE_TLS_KEY: "", FIXTURE_TLS_CERT: "" },
});
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
let activePage: Page | undefined;
try {
  const reader = server.stdout.getReader();
  let output = "", url = "";
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const timeout = new Promise<never>((_,reject) => { timer = setTimeout(() => reject(new Error("TV fixture startup timed out")),15_000); });
    while (!url) {
      const part = await Promise.race([reader.read(),timeout]);
      output += new TextDecoder().decode(part.value);
      url = /SPA_FIXTURE_URL=(\S+)/.exec(output)?.[1] ?? "";
      if (part.done && !url) throw new Error("TV fixture exited without a URL");
    }
  } finally { clearTimeout(timer); reader.releaseLock(); }
  browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  context.setDefaultTimeout(15_000);
  await context.addCookies([{ name: "podible_session", value: "tv-reader-fixture", url, httpOnly: true }]);
  const api = createTRPCClient<AppRouter>({ links: [httpLink({ url: new URL("/api/trpc",url).href,
    headers: { cookie: "podible_session=tv-reader-fixture", origin: new URL(url).origin } })] });
  const { requests } = await api.tv.myRequests.query({});
  const request = requests.find(r => r.entry.title === "Fixture Sequel")!;
  const withGaps = requests.find(r => r.entry.title === "Fixture with gaps")!;
  const page = await context.newPage(), errors: string[] = [], networkErrors: string[] = [], diagnostics: string[] = [];
  activePage = page;
  page.on("pageerror",error => errors.push(error.message));
  page.on("requestfailed",item => {
    const error = item.failure()?.errorText;
    if (new URL(item.url()).pathname === "/api/trpc/acquisition.changes" && error === "net::ERR_ABORTED") return;
    networkErrors.push(`${item.method()} ${item.url()}: ${error}`);
  });
  page.on("response",response => { if (response.url().includes("/api/trpc/") && response.status() >= 400) networkErrors.push(`${response.status()} ${response.url()}`); });
  page.on("request",item => { if (/(?:tvShadow|movieShadow|movieDualWrite)\.|\/tv\.(?:get|list|status)(?:$|,)/.test(new URL(item.url()).pathname)) diagnostics.push(item.url()); });
  await page.goto(new URL(`/tv/requests/${request.requestId}`,url).href);
  await page.getByRole("heading",{ name: "Fixture Sequel", exact: true }).waitFor();
  await page.getByRole("heading",{ name: "Available in Plex", exact: true }).waitFor();
  assert.equal(new URL(page.url()).pathname,`/tv/library/${request.entryId}`);
  await page.getByRole("heading",{ name: "Season 2", exact: true }).waitFor();
  await page.getByRole("button", { name: "Season 2", exact: true }).click();
  const episodes = page.getByRole("list", { name: "Season 2 episodes" });
  await episodes.getByRole("listitem").filter({ hasText: "Fixture episode 1" }).getByText("Ready in Plex").waitFor();
  await episodes.getByRole("listitem").filter({ hasText: "Fixture episode 2" }).getByText("Upcoming").waitFor();
  await page.getByText("1 of 2 episodes verified in Plex.",{ exact: true }).waitFor();
  assert.equal(await page.getByRole("progressbar",{ name: "Available TV episodes" }).getAttribute("value"),"1");
  assert.equal(await page.getByRole("heading",{ name: "Activity", exact: true }).count(),0,"Request page displays a raw execution history");
  assert.equal(await page.getByRole("button",{ name: "Search missing episodes" }).count(),0,"Future-only intent should not invite searches");
  await page.getByText("Fixture Prequel · PREQUEL",{ exact: true }).waitFor();
  for (const hidden of ["Would import","native-selected-file.mkv","sonarr:fixture:queue:1","h264","tv-fixture-seerr-private","tv-fixture-agent-private","tv-fixture-sonarr-private"])
    assert.ok(!(await page.locator("body").innerText()).includes(hidden),`${hidden} leaked into a user-safe request`);
  assert.equal(await page.getByRole("button",{ name: "Acquire approved seasons", exact: true }).count(),0);
  assert.equal(await page.getByRole("spinbutton").count(),0,"Individual episode request scope is exposed");
  await page.reload();
  await page.getByText("1 of 2 episodes verified in Plex.",{ exact: true }).waitFor();
  assert.equal(await page.locator("header").count(),1,"Native TV request rendered duplicate app shells");
  const artifacts = path.resolve("tmp/movies-lab");
  await mkdir(artifacts,{ recursive: true });
  await page.screenshot({ path: path.join(artifacts,"tv-request-desktop.png"), fullPage: true, animations: "disabled" });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.reload();
  await page.getByText("1 of 2 episodes verified in Plex.",{ exact: true }).waitFor();
  assert.equal(await page.locator("header").count(),1,"Mobile TV request rendered duplicate app shells");
  assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"),false,"Native TV request overflows mobile viewport");
  assert.equal(await page.getByRole("button",{ name: "Withdraw request", exact: true }).isVisible(),true);
  await page.screenshot({ path: path.join(artifacts,"tv-request-mobile.png"), animations: "disabled" });
  console.log("Native TV browser: persisted coverage and desktop/mobile screenshots verified");

  const stream = page.waitForResponse(response => new URL(response.url()).pathname === "/api/trpc/acquisition.changes" && response.ok());
  await page.goto(new URL(`/tv/requests/${withGaps.requestId}`,url).href);
  await stream;
  await page.getByRole("heading", { name: "1 episode batch in progress" }).waitFor();
  const stages = page.getByRole("list", { name: "Acquisition stages" });
  await stages.getByRole("listitem").filter({ hasText: "Download" }).getByText("1 season", { exact: true }).waitFor();
  assert.equal(await stages.getByRole("listitem").count(), 5);
  await page.getByRole("progressbar", { name: "Active downloads progress" }).waitFor();
  assert.equal(await page.getByRole("progressbar", { name: "Season 1 downloads progress" }).getAttribute("value"),String(3 * 1024 ** 3));
  assert.equal(await page.getByRole("progressbar", { name: "Season 2 downloads progress" }).count(),0);
  await page.getByText("3 GiB of 8 GiB · 2 MiB/s", { exact: true }).first().waitFor();
  assert.equal((await context.request.post(new URL("/__fixture/tv-transfer", url).href)).ok(), true);
  await page.getByText("5 GiB of 8 GiB · 2 MiB/s", { exact: true }).first().waitFor({ timeout: 1500 });
  assert.equal((await context.request.post(new URL("/__fixture/tv-transfer?reconnect", url).href)).ok(), true);
  await page.getByText("6 GiB of 8 GiB · 2 MiB/s", { exact: true }).first().waitFor({ timeout: 10_000 });
  await page.getByRole("button", { name: "Season 1", exact: true }).press("Enter");
  await page.getByRole("list", { name: "Season 1 episodes" }).getByText("Downloading", { exact: true }).waitFor();
  await page.getByRole("button", { name: "Season 2", exact: true }).press("Enter");
  await page.getByRole("list", { name: "Season 2 episodes" }).getByText("Not in Plex", { exact: true }).waitFor();
  assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"),false,"Expanded episode list overflows mobile viewport");
  await page.screenshot({ path: path.join(artifacts,"tv-request-transfer-mobile.png"), fullPage: true, animations: "disabled" });
  await page.getByRole("button", { name: "Search missing episodes", exact: true }).click();
  await page.getByRole("heading", { name: "2 episode batches in progress" }).waitFor();
  assert.equal(await page.getByRole("button", { name: "Search missing episodes" }).count(), 0);
  const afterSearch = (await api.tv.myRequest.query({ requestId: withGaps.requestId })).request;
  assert.equal(afterSearch.acquisitions.length, 2, "Retry did not share the already queued batch");
  assert.equal(afterSearch.progress.searchableAiredEpisodes, 0);
  await page.goto(new URL(`/tv/requests/${request.requestId}`,url).href);

  const admin = await browser.newContext();
  admin.setDefaultTimeout(15_000);
  await admin.addCookies([{ name: "podible_session", value: "spa-browser-fixture", url, httpOnly: true }]);
  const adminPage = await admin.newPage();
  activePage = adminPage;
  await adminPage.goto(new URL(`/tv/requests/${request.requestId}`,url).href);
  await adminPage.getByRole("alert").filter({ hasText: "TV request not found" }).waitFor();
  assert.equal(await adminPage.getByRole("heading",{ name: "Fixture Sequel", exact: true }).count(),0,"Admin opened another user's private request");
  const entry = (await api.catalog.available.query({ query: "Fixture Sequel" })).tv.find(entry => entry.key === request.entry.key)!;
  await adminPage.goto(new URL(`/tv/library/${entry.entryId}`,url).href);
  await adminPage.getByText("1 of 2 episodes verified in Plex.",{ exact: true }).waitFor();
  assert.equal(await adminPage.getByRole("button", { name: "Withdraw request", exact: true }).count(),0);
  let retentionAudits = 0;
  adminPage.on("request",item => { if (item.url().includes("tvRetention.audit")) retentionAudits++; });
  await adminPage.goto(new URL("/tv-retention",url).href);
  const seasonPicker = adminPage.getByRole("combobox",{ name: "TV title and season", exact: true });
  await seasonPicker.getByRole("option",{ name: "Fixture Sequel (2020) / Season 2", exact: true }).waitFor({ state: "attached" });
  assert.deepEqual(await seasonPicker.getByRole("option").allTextContents(), ["Choose a title and season", "Fixture Sequel (2020) / Season 2",
    "Fixture with gaps (2020) / Season 1", "Fixture with gaps (2020) / Season 2"]);
  await seasonPicker.selectOption({ label: "Fixture Sequel (2020) / Season 2" });
  assert.equal(retentionAudits,0,"Selecting a published TV title implicitly started retention work");
  assert.equal(await adminPage.getByRole("spinbutton").count(),0,"Retention still exposes numeric target IDs");
  await admin.close();
  console.log("Native TV browser: private request denied to foreign admin, published library remains accessible");

  activePage = page;
  page.once("dialog",dialog => dialog.accept());
  await page.getByRole("button",{ name: "Withdraw request", exact: true }).click();
  await page.getByRole("button",{ name: "Withdraw request", exact: true }).waitFor({ state: "detached" });
  await page.reload();
  await page.getByRole("heading",{ name: "Available in Plex", exact: true }).waitFor();
  assert.equal((await api.tv.myRequest.query({ requestId: request.requestId })).request.withdrawn,true);
  assert.equal((await api.catalog.tvEntry.query({ entryId: entry.entryId })).availableEpisodes,1,"Withdrawal removed existing published coverage");
  console.log("Native TV browser: withdrawal persisted without deleting published episodes");
  await context.clearCookies();
  await page.reload();
  await page.getByRole("link",{ name: "Sign in", exact: true }).waitFor();
  // The expired-session request is expected; earlier authenticated views must stay error-free.
  assert.deepEqual(networkErrors.filter(error => !error.startsWith("401 ")),[],"Native TV browser request errors");
  assert.deepEqual(diagnostics,[],"Native TV screens fetched admin diagnostics");
  assert.deepEqual(errors,[],"Native TV browser runtime errors");
  console.log("Native TV browser acceptance passed: owned request deep link/reload, actual coverage versus audit, future season intent, private/admin separation, published library route, withdrawal persistence/no media deletion, expired session and desktop/mobile screenshots.");
} catch (error) {
  if (activePage && !activePage.isClosed()) {
    console.error(await activePage.locator("body").innerText());
    await mkdir("tmp/movies-lab",{ recursive: true });
    await activePage.screenshot({ path: "tmp/movies-lab/tv-request-failure.png", fullPage: true, animations: "disabled" });
  }
  throw error;
} finally {
  await browser?.close();
  server.kill("SIGTERM");
  const timer = setTimeout(() => server.kill("SIGKILL"),5000);
  try { await server.exited; } finally { clearTimeout(timer); }
}
