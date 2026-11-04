import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { chromium } from "playwright";

const server = Bun.spawn([process.execPath, "run", "test/fixtures/spa-server.ts"], {
  stdout: "pipe", stderr: "inherit",
  env: { ...process.env, PORT: "0", ADMIN_FIXTURE: "1", TV_FIXTURE: "", SHADOW_FIXTURE: "", AUTH_FIXTURE: "" },
});
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
try {
  const reader = server.stdout.getReader();
  let output = "", url = "";
  const deadline = Date.now() + 15_000;
  while (!url) {
    const part = await Promise.race([reader.read(), Bun.sleep(Math.max(1, deadline - Date.now())).then(() => { throw new Error("Admin fixture startup timed out"); })]);
    output += new TextDecoder().decode(part.value);
    url = /SPA_FIXTURE_URL=(\S+)/.exec(output)?.[1] ?? "";
    if (part.done && !url) throw new Error("Admin fixture exited without a URL");
  }
  reader.releaseLock();
  browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  await context.addCookies([{ name: "podible_session", value: "spa-browser-fixture", url, httpOnly: true }]);
  const rpc = async (method: string, params: Record<string, unknown> = {}, request = context.request) => {
    const response = await request.post(new URL("/rpc", url).href, { data: { jsonrpc: "2.0", id: 1, method, params } });
    assert.equal(response.status(), 200);
    return response.json() as Promise<{ result?: any; error?: { code: number; message: string } }>;
  };
  const anonymous = await browser.newContext(), readerContext = await browser.newContext();
  await readerContext.addCookies([{ name: "podible_session", value: "admin-reader-fixture", url, httpOnly: true }]);
  for (const method of ["settings.get", "settings.update", "adminWeb.users", "adminWeb.ops", "adminWeb.plexServers", "adminWeb.selectPlexServer", "adminWeb.refresh", "jobs.retry"]) {
    assert.equal((await rpc(method, {}, anonymous.request)).error?.code, -32001);
    assert.equal((await rpc(method, {}, readerContext.request)).error?.code, -32003);
  }
  assert.equal((await readerContext.request.get(new URL("/api/trpc/adminWeb.users?input=%7B%7D", url).href)).status(), 403);
  const readerPage = await readerContext.newPage();
  for (const pathname of ["/settings", "/users", "/ops"]) {
    await readerPage.goto(new URL(pathname, url).href);
    await readerPage.getByRole("alert").getByText("Administrator access required.").waitFor();
    assert.equal(await readerPage.getByLabel("Settings JSON", { exact: true }).count(), 0);
  }
  for (const page of ["settings", "users", "ops"]) {
    const legacy = await context.request.get(new URL(`/admin/${page}`, url).href, { maxRedirects: 0 });
    assert.equal(legacy.status(), 303);
    assert.equal(legacy.headers().location, `/${page}`);
  }
  for (const removed of ["/admin/db", "/admin/curation", "/admin/curation/api/runs", "/admin/curation/api/trace", "/db", "/curation"])
    assert.equal((await context.request.get(new URL(removed, url).href)).status(), 404, `Removed surface still exists: ${removed}`);

  const page = await context.newPage();
  const pageErrors: string[] = [], networkErrors: string[] = [];
  let expectedSettingsError = false, settingsPosts = 0, diagnosticsRequests = 0;
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("requestfailed", (request) => networkErrors.push(`${request.url()}: ${request.failure()?.errorText}`));
  page.on("response", (response) => {
    if (response.url().includes("/api/trpc/") && response.status() >= 400 &&
        !(expectedSettingsError && response.url().includes("settings.update"))) networkErrors.push(`${response.status()} ${response.url()}`);
  });
  page.on("request", (request) => {
    const resource = new URL(request.url());
    if (resource.pathname.includes("settings.update") && request.method() === "POST") settingsPosts++;
    if (resource.pathname.includes("adminWeb.ops")) {
      const inputs = JSON.parse(request.postData() || resource.searchParams.get("input") || "{}");
      if (Object.values(inputs).some((value: any) => (value?.json ?? value)?.includeJobs === true) || inputs.includeJobs === true) diagnosticsRequests++;
    }
  });
  await page.goto(new URL("/settings", url).href);
  await page.getByRole("heading", { name: "Settings", exact: true }).waitFor();
  console.log("Admin browser: settings route loaded");
  const adminNavigation = page.getByRole("navigation", { name: "Administrator pages", exact: true });
  assert.equal(await adminNavigation.getByRole("link").count(), 3);
  const editor = page.getByLabel("Settings JSON", { exact: true });
  await editor.waitFor({ timeout: 5000 }).catch(async (error) => {
    console.error(JSON.stringify({ route: page.url(), labels: await page.locator("label").count(),
      textareas: await page.locator("textarea").count(), alerts: await page.getByRole("alert").allTextContents(), pageErrors }));
    throw error;
  });
  await page.waitForFunction('document.querySelector(\'textarea[aria-describedby="settings-json-note"]\')?.value.includes("Fixture library")');
  const initial = JSON.parse(await editor.inputValue());
  assert.equal(initial.auth.plex.machineId, "fixture-mendel");
  assert.equal(await page.getByLabel("Plex access server", { exact: true }).inputValue(), "fixture-mendel");
  const edited = { ...initial, feed: { ...initial.feed, title: "Unsaved administrator draft" } };
  const editedText = JSON.stringify(edited, null, 2);
  await editor.fill(editedText);
  await page.getByRole("status").getByText("Unsaved changes", { exact: true }).waitFor();
  await page.getByRole("button", { name: "Reload server settings", exact: true }).click();
  await page.getByRole("button", { name: "Reload server settings", exact: true }).waitFor({ state: "visible" });
  assert.equal(await editor.inputValue(), editedText, "Refetch overwrote an unsaved settings draft");
  await adminNavigation.getByRole("link", { name: "Users", exact: true }).click();
  console.log("Admin browser: checking users and draft navigation");
  await page.getByRole("heading", { name: "Users", exact: true }).waitFor();
  await page.getByRole("cell").filter({ hasText: "Actual Reader" }).waitFor();
  const users = (await rpc("adminWeb.users")).result.users;
  assert.ok(users.some((user: { displayName: string }) => user.displayName === "Actual Reader"));
  for (const user of users) assert.deepEqual(Object.keys(user).sort(), ["id", "provider", "username", "displayName", "isAdmin", "createdAt", "updatedAt"].sort());
  assert.ok(!(await page.locator("body").innerText()).includes("admin-fixture-owner-secret"));
  await adminNavigation.getByRole("link", { name: "Settings", exact: true }).click();
  await editor.waitFor();
  assert.equal(await editor.inputValue(), editedText, "SPA navigation lost the unsaved draft");
  await page.getByLabel("Plex access server", { exact: true }).selectOption("fixture-cyprus");
  await page.getByRole("button", { name: "Use selected Plex server", exact: true }).click();
  console.log("Admin browser: checking fresh Plex selection");
  await page.getByText("Plex access server selected. Other unsaved settings are preserved.", { exact: true }).waitFor();
  const rebased = JSON.parse(await editor.inputValue());
  assert.equal(rebased.auth.plex.machineId, "fixture-cyprus");
  assert.equal(rebased.feed.title, edited.feed.title);
  assert.equal(rebased.agents.apiKey, initial.agents.apiKey);
  const afterSelection = (await rpc("settings.get")).result;
  assert.equal(afterSelection.auth.plex.machineId, "fixture-cyprus");
  assert.equal(afterSelection.feed.title, initial.feed.title, "Plex selection saved unrelated draft settings");
  const servers = (await rpc("adminWeb.plexServers")).result;
  assert.ok(!JSON.stringify(servers).includes("admin-fixture-server-secret"));
  assert.equal((await rpc("adminWeb.selectPlexServer", { machineId: "arbitrary-unlisted-server" })).error?.code, -32602);

  let releaseSave!: () => void;
  const saveGate = new Promise<void>((resolve) => { releaseSave = resolve; });
  await page.route("**/api/trpc/settings.update**", async (route) => { await saveGate; await route.continue(); });
  const pendingSave = page.waitForRequest((request) => request.url().includes("settings.update") && request.method() === "POST");
  await page.getByRole("button", { name: "Save settings", exact: true }).click();
  await pendingSave;
  console.log("Admin browser: checking edits during an in-flight save");
  const newer = { ...rebased, feed: { ...rebased.feed, title: "Newer edits while saving" } };
  await editor.fill(JSON.stringify(newer, null, 2));
  releaseSave();
  await page.getByText("Settings saved; newer edits remain unsaved.", { exact: true }).waitFor();
  await page.unroute("**/api/trpc/settings.update**");
  assert.equal(JSON.parse(await editor.inputValue()).feed.title, newer.feed.title, "Successful save erased newer edits");
  assert.equal((await rpc("settings.get")).result.feed.title, edited.feed.title);
  await page.getByRole("button", { name: "Save settings", exact: true }).click();
  await page.getByText("Settings saved.", { exact: true }).waitFor();
  assert.equal((await rpc("settings.get")).result.auth.plex.machineId, "fixture-cyprus");
  const saved = JSON.parse(await editor.inputValue());
  const invalid = { ...saved, agents: { ...saved.agents, timeoutMs: "not-a-number" } };
  const invalidText = JSON.stringify(invalid, null, 2);
  await editor.fill(invalidText);
  expectedSettingsError = true;
  await page.getByRole("button", { name: "Save settings", exact: true }).click();
  await page.getByRole("alert").filter({ hasText: "Settings were not saved" }).waitFor();
  expectedSettingsError = false;
  assert.equal(await editor.inputValue(), invalidText, "Server validation erased the draft");
  assert.equal((await rpc("settings.get")).result.agents.timeoutMs, saved.agents.timeoutMs);
  const postsBeforeParseError = settingsPosts;
  await editor.fill("{ broken JSON");
  await page.getByRole("alert").filter({ hasText: "Invalid JSON" }).waitFor();
  assert.equal(await page.getByRole("button", { name: "Save settings", exact: true }).isDisabled(), true);
  assert.equal(settingsPosts, postsBeforeParseError, "Invalid JSON was submitted");
  await page.getByRole("button", { name: "Discard draft", exact: true }).click();

  assert.equal((await rpc("jobs.list", { type: "full_library_refresh" })).result.jobs.length, 0, "Opening pages queued a refresh");
  await adminNavigation.getByRole("link", { name: "Ops", exact: true }).click();
  console.log("Admin browser: checking operational triage");
  await page.getByRole("heading", { name: "Ops", exact: true }).waitFor();
  const active = page.getByRole("region", { name: "Active work", exact: true });
  const attention = page.getByRole("region", { name: "Needs attention", exact: true });
  const providers = page.getByRole("region", { name: "Provider state", exact: true });
  const storage = page.getByRole("region", { name: "Storage and links", exact: true });
  await active.getByRole("link", { name: "Fixture Active Book", exact: true }).waitFor();
  await active.getByRole("link", { name: "Fixture Active Download", exact: true }).waitFor();
  await active.getByText("Telemetry: unknown.", { exact: false }).waitFor();
  await active.getByText("Will retry automatically", { exact: false }).waitFor();
  await attention.getByRole("link", { name: "Fixture Import Failure", exact: true }).waitFor();
  assert.equal(await attention.getByText("Fixture Waiting For Offers", { exact: false }).count(), 0);
  await page.getByText("1 acquisitions waiting for suitable releases or future episodes (informational).", { exact: true }).waitFor();
  await providers.getByText("Fixture provider", { exact: true }).waitFor();
  await providers.getByText("cooldown", { exact: true }).waitFor();
  await storage.getByText("Published link presence is not verified by this snapshot", { exact: false }).waitFor();
  await storage.getByText("unknown", { exact: true }).first().waitFor();
  assert.equal(diagnosticsRequests, 0, "Primary Ops view loaded the raw job history");
  const baselineOps = (await rpc("adminWeb.ops", { includeJobs: false })).result;
  assert.deepEqual(baselineOps.diagnostics.recentJobs, []);
  assert.equal(baselineOps.summary.waiting, 1);
  assert.equal(baselineOps.downloads.length, 1);
  assert.ok(baselineOps.downloads.every((download: { telemetry: string; bytesDone: number | null; bytesPerSecond: number | null; problem: unknown }) =>
    download.telemetry === "unknown" && download.bytesDone === null && download.bytesPerSecond === null && download.problem === null),
  "The fixture has execution demand but no verified private engine; it must not invent live measurements or tracker errors");
  assert.ok(!baselineOps.incidents.some((incident: { key: string }) => incident.key.startsWith("torrent:")));
  assert.ok(!JSON.stringify(baselineOps).includes("admin-fixture-provider-secret"));
  assert.ok(!(await page.locator("body").innerText()).includes("provider.invalid"));
  const diagnostics = await rpc("adminWeb.ops", { includeJobs: true });
  const failedImport = diagnostics.result.diagnostics.recentJobs.find((job: { type: string; status: string }) => job.type === "import" && job.status === "failed");
  assert.ok(failedImport);
  await page.getByText("Execution details", { exact: true }).click();
  await page.locator("summary").filter({ hasText: `Job #${failedImport.id} / import / failed` }).click();
  const retryResponse = page.waitForResponse((response) => response.url().includes("jobs.retry") && response.request().method() === "POST");
  await page.getByRole("button", { name: `Retry job #${failedImport.id}`, exact: true }).click();
  assert.equal((await retryResponse).status(), 200);
  await page.getByText(`Retry requested for job #${failedImport.id}. Follow its actual execution state above.`, { exact: true }).waitFor();
  await page.getByText("Execution details", { exact: true }).click();
  const afterClose = diagnosticsRequests;
  await page.waitForTimeout(5300);
  assert.equal(diagnosticsRequests, afterClose, "Collapsed execution history kept polling");
  await page.getByRole("button", { name: "Queue library refresh", exact: true }).click();
  await page.getByRole("status").filter({ hasText: "Library refresh queued as job" }).waitFor();
  const firstRefresh = (await rpc("jobs.list", { type: "full_library_refresh" })).result.jobs;
  assert.equal(firstRefresh.length, 1);
  assert.equal(firstRefresh[0].status, "queued");
  assert.equal(firstRefresh[0].max_attempts, 2);
  await page.getByRole("button", { name: "Queue library refresh", exact: true }).click();
  await page.getByRole("status").filter({ hasText: "Library refresh already queued or running" }).waitFor();
  assert.equal((await rpc("jobs.list", { type: "full_library_refresh" })).result.jobs.length, 1);
  const artifacts = path.resolve("tmp/movies-lab");
  await mkdir(artifacts, { recursive: true });
  await page.screenshot({ path: path.join(artifacts, "admin-ops-desktop.png"), fullPage: true, animations: "disabled" });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"), false, "Ops overflows mobile viewport");
  await active.scrollIntoViewIfNeeded();
  await page.screenshot({ path: path.join(artifacts, "admin-ops-mobile.png"), animations: "disabled" });
  await adminNavigation.getByRole("link", { name: "Settings", exact: true }).click();
  await editor.waitFor();
  assert.equal(JSON.parse(await editor.inputValue()).auth.plex.machineId, "fixture-cyprus");
  assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"), false, "Settings overflows mobile viewport");
  await page.getByLabel("Plex access server", { exact: true }).scrollIntoViewIfNeeded();
  await page.screenshot({ path: path.join(artifacts, "admin-settings-mobile.png"), animations: "disabled" });
  assert.deepEqual(pageErrors, [], "Admin UI page errors");
  assert.deepEqual(networkErrors, [], "Unexpected admin UI request/HTTP errors");
  console.log("Admin Chrome acceptance passed: canonical routes/auth, removed DB/curation, safe users, preserved JSON drafts/refetch/navigation/in-flight edits, validated saves and Plex rebase, operational triage/retry/provider/storage, lazy diagnostics, bounded refresh and mobile layout.");
} finally {
  await browser?.close();
  server.kill("SIGTERM");
  await server.exited;
}
