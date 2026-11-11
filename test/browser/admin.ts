import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { chromium } from "playwright";

const tlsRoot = await mkdtemp(path.join(os.tmpdir(), "podible-proxy-browser-"));
const key = path.join(tlsRoot, "key.pem"), cert = path.join(tlsRoot, "cert.pem");
const certificate = Bun.spawnSync(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key,
  "-out", cert, "-days", "1", "-subj", "/CN=localhost"]);
assert.equal(certificate.exitCode, 0, "Unable to generate local TLS fixture certificate");
const server = Bun.spawn([process.execPath, "run", "test/fixtures/spa-server.ts"], {
  stdout: "pipe", stderr: "inherit",
  env: { ...process.env, PORT: "0", ADMIN_FIXTURE: "1", TV_FIXTURE: "", SHADOW_FIXTURE: "", AUTH_FIXTURE: "",
    FIXTURE_TLS_KEY: key, FIXTURE_TLS_CERT: cert },
});
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
async function verify() {
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
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 }, ignoreHTTPSErrors: true });
  await context.addCookies([{ name: "podible_session", value: "spa-browser-fixture", url, httpOnly: true }]);
  const rpc = async (method: string, params: Record<string, unknown> = {}, request = context.request) => {
    const response = await request.post(new URL("/rpc", url).href, { data: { jsonrpc: "2.0", id: 1, method, params } });
    assert.equal(response.status(), 200);
    return response.json() as Promise<{ result?: any; error?: { code: number; message: string } }>;
  };
  const anonymous = await browser.newContext({ ignoreHTTPSErrors: true }), readerContext = await browser.newContext({ ignoreHTTPSErrors: true });
  await readerContext.addCookies([{ name: "podible_session", value: "admin-reader-fixture", url, httpOnly: true }]);
  for (const method of ["settings.get", "settings.update", "settings.indexers", "adminWeb.migrationPreview", "adminWeb.users", "adminWeb.ops", "adminWeb.plexServers", "adminWeb.selectPlexServer", "adminWeb.refresh", "jobs.retry", "retention.preview", "retention.evict", "transferMetrics.status", "transferMetrics.library",
    "crossSeed.startBackfill", "crossSeed.backfillLatest", "crossSeed.backfillStatus", "crossSeed.cancelBackfill"]) {
    assert.equal((await rpc(method, {}, anonymous.request)).error?.code, -32001);
    assert.equal((await rpc(method, {}, readerContext.request)).error?.code, -32003);
  }
  assert.equal((await readerContext.request.get(new URL("/api/trpc/adminWeb.users?input=%7B%7D", url).href)).status(), 403);
  const readerPage = await readerContext.newPage();
  for (const pathname of ["/settings", "/users", "/ops", "/retention"]) {
    await readerPage.goto(new URL(pathname, url).href);
    await readerPage.getByRole("alert").getByText("Administrator access required.").waitFor();
    const links = readerPage.getByRole("navigation", { name: "Main navigation" }).getByRole("link");
    assert.deepEqual(await links.evaluateAll(items => items.map(link => [link.textContent, link.getAttribute("href")])),
      [["Library", "/library"], ["Notifications", "/notifications"]], "A reader must not see administrator destinations");
    assert.equal(await readerPage.getByRole("navigation", { name: "Settings sections" }).count(), 0);
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
  let expectedSettingsError = false, expectedRetryRejection = false, settingsPosts = 0, diagnosticsRequests = 0;
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("requestfailed", (request) => networkErrors.push(`${request.url()}: ${request.failure()?.errorText}`));
  page.on("response", (response) => {
    if (response.url().includes("/api/trpc/") && response.status() >= 400 &&
        !(expectedSettingsError && response.url().includes("settings.update")) &&
        !(expectedRetryRejection && response.url().includes("jobs.retry"))) networkErrors.push(`${response.status()} ${response.url()}`);
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
  const sectionNav = page.getByRole("navigation", { name: "Settings sections", exact: true });
  const openSettings = (name: string) => sectionNav.getByRole("button", { name, exact: true }).click();
  const title = page.getByLabel("Feed title", { exact: true });
  const openAi = page.getByLabel("OpenAI API key", { exact: true });
  const saveSettings = page.getByRole("button", { name: "Save settings", exact: true });
  const reloadSettings = page.getByRole("button", { name: "Reload server settings", exact: true });
  const discard = page.getByRole("button", { name: "Discard draft", exact: true });
  const artifacts = path.resolve("tmp/movies-lab");
  await mkdir(artifacts, { recursive: true });
  await title.waitFor();
  assert.equal(await title.inputValue(), "Fixture library");
  assert.equal(await openAi.count(), 0);
  await openSettings("AI");
  assert.equal(await openAi.getAttribute("type"), "password");
  assert.equal(await openAi.getAttribute("placeholder"), "\u2022".repeat(8));
  assert.equal(await openAi.inputValue(), "", "An existing credential was put into an input");
  assert.equal(await page.getByLabel("Settings JSON", { exact: true }).count(), 0);
  assert.equal(await saveSettings.isDisabled(), true);
  await page.screenshot({ path: path.join(artifacts, "admin-settings-desktop.png"), animations: "disabled" });
  await openSettings("Storage");
  const migration = page.getByRole("region", { name: "Existing library preview" });
  await migration.getByRole("button", { name: "Preview library", exact: true }).click();
  await migration.getByText("A library worth keeping (2025)", { exact: true }).waitFor();
  await migration.getByText(/1 titles need a catalog identity/).waitFor();
  assert.equal(await migration.getByRole("button", { name: /Import|Adopt|Download/ }).count(), 0, "Preview must not expose a mutation");
  const adminNavigation = page.getByRole("navigation", { name: "Administrator pages", exact: true });
  assert.equal(await adminNavigation.getByRole("link").count(), 3);
  const initial = (await rpc("settings.get")).result;
  assert.equal(initial.auth.plex.machineId, "fixture-mendel");
  await openSettings("Books & feed");
  const editedTitle = "Unsaved administrator draft";
  await title.fill(editedTitle);
  await page.getByRole("status").getByText("Unsaved changes", { exact: true }).waitFor();
  const outsideSettings = { ...initial, feed: { ...initial.feed, author: "Updated elsewhere" },
    agents: { ...initial.agents, apiKey: "rotated-fixture-secret" } };
  assert.ok((await rpc("settings.update", { settings: outsideSettings })).result);
  await reloadSettings.click();
  await page.getByLabel("Feed author", { exact: true }).and(page.locator('[value="Updated elsewhere"]')).waitFor();
  assert.equal(await title.inputValue(), editedTitle, "Refetch overwrote an unsaved settings draft");
  assert.equal(await page.getByLabel("Feed author", { exact: true }).inputValue(), outsideSettings.feed.author);
  assert.equal(settingsPosts, 0, "Opening sections and editing fields must not save");
  await adminNavigation.getByRole("link", { name: "Users", exact: true }).click();
  console.log("Admin browser: checking users and draft navigation");
  await page.getByRole("heading", { name: "Users", exact: true }).waitFor();
  await page.getByRole("cell").filter({ hasText: "Actual Reader" }).waitFor();
  const users = (await rpc("adminWeb.users")).result.users;
  assert.ok(users.some((user: { displayName: string }) => user.displayName === "Actual Reader"));
  for (const user of users) assert.deepEqual(Object.keys(user).sort(), ["id", "provider", "username", "displayName", "isAdmin", "createdAt", "updatedAt"].sort());
  assert.ok(!(await page.locator("body").innerText()).includes("admin-fixture-owner-secret"));
  await adminNavigation.getByRole("link", { name: "Settings", exact: true }).click();
  await title.waitFor();
  assert.equal(await title.inputValue(), editedTitle, "SPA navigation lost the unsaved draft");
  await openSettings("Plex access");
  const plexPicker = page.getByLabel("Plex access server", { exact: true });
  await plexPicker.waitFor();
  assert.equal(await plexPicker.inputValue(), "fixture-mendel");
  await plexPicker.selectOption("fixture-cyprus");
  assert.equal((await rpc("settings.get")).result.auth.plex.machineId, "fixture-mendel", "Picker choice was applied before confirmation");
  await page.getByRole("button", { name: "Use selected Plex server", exact: true }).click();
  console.log("Admin browser: checking fresh Plex selection");
  await page.getByText("Plex access server selected. Other unsaved settings are preserved.", { exact: true }).waitFor();
  const afterSelection = (await rpc("settings.get")).result;
  assert.equal(afterSelection.auth.plex.machineId, "fixture-cyprus");
  assert.equal(afterSelection.feed.title, initial.feed.title, "Plex selection saved unrelated draft settings");
  assert.equal(settingsPosts, 0, "Verified Plex selection must not save the settings draft");
  await page.getByText("Advanced Plex identity settings", { exact: true }).click();
  assert.equal(await page.getByLabel("Plex owner token", { exact: true }).inputValue(), "");
  await openSettings("Books & feed");
  assert.equal(await title.inputValue(), editedTitle);
  const rotatedAgain = { ...afterSelection, agents: { ...afterSelection.agents, apiKey: "latest-fixture-secret" } };
  assert.ok((await rpc("settings.update", { settings: rotatedAgain })).result);
  const servers = (await rpc("adminWeb.plexServers")).result;
  assert.ok(!JSON.stringify(servers).includes("admin-fixture-server-secret"));
  assert.equal((await rpc("adminWeb.selectPlexServer", { machineId: "arbitrary-unlisted-server" })).error?.code, -32602);

  let releaseSave!: () => void;
  const saveGate = new Promise<void>((resolve) => { releaseSave = resolve; });
  await page.route("**/api/trpc/settings.update**", async (route) => { await saveGate; await route.continue(); });
  const pendingSave = page.waitForRequest((request) => request.url().includes("settings.update") && request.method() === "POST");
  await saveSettings.click();
  await pendingSave;
  console.log("Admin browser: checking edits during an in-flight save");
  const newerTitle = "Newer edits while saving";
  await title.fill(newerTitle);
  releaseSave();
  await page.getByText("Settings saved; newer edits remain unsaved.", { exact: true }).waitFor();
  await page.unroute("**/api/trpc/settings.update**");
  assert.equal(await title.inputValue(), newerTitle, "Successful save erased newer edits");
  assert.equal((await rpc("settings.get")).result.feed.title, editedTitle);
  await saveSettings.click();
  await page.getByText("Settings saved.", { exact: true }).waitFor();
  const saved = (await rpc("settings.get")).result;
  assert.deepEqual(saved, { ...rotatedAgain, feed: { ...rotatedAgain.feed, title: newerTitle } });
  assert.equal(saved.agents.apiKey, rotatedAgain.agents.apiKey, "An untouched credential rotation was overwritten");

  await openSettings("AI");
  const timeout = page.getByLabel("Agent timeout (ms)", { exact: true });
  await timeout.fill("45000");
  await page.route("**/api/trpc/settings.update**", async route => {
    const input = JSON.parse(route.request().postData()!);
    (input["0"] ?? input).settings.agents.timeoutMs = "force-server-rejection";
    await route.continue({ postData: JSON.stringify(input) });
  });
  expectedSettingsError = true;
  await saveSettings.click();
  await page.getByRole("alert").filter({ hasText: "Settings were not saved" }).waitFor();
  expectedSettingsError = false;
  await page.unroute("**/api/trpc/settings.update**");
  assert.equal(await timeout.inputValue(), "45000", "Server validation erased the draft");
  assert.equal((await rpc("settings.get")).result.agents.timeoutMs, saved.agents.timeoutMs);
  const postsBeforeInvalid = settingsPosts;
  await timeout.fill("not-a-number");
  await page.getByRole("alert").filter({ hasText: "Check 1 invalid setting" }).waitFor();
  assert.equal(await timeout.getAttribute("aria-invalid"), "true");
  assert.equal(await saveSettings.isDisabled(), true);
  assert.equal(settingsPosts, postsBeforeInvalid, "Invalid input was submitted");
  await discard.click();

  await timeout.fill("45000");
  await openSettings("Books & feed");
  await title.fill("A conflicting draft");
  const otherEdit = { ...saved, feed: { ...saved.feed, title: "A competing server edit" } };
  assert.ok((await rpc("settings.update", { settings: otherEdit })).result);
  await saveSettings.click();
  await page.getByRole("alert").filter({ hasText: "Server settings changed while you were editing: Feed title" }).waitFor();
  assert.equal(await saveSettings.isDisabled(), true);
  assert.equal(await title.inputValue(), "A conflicting draft");
  assert.equal(settingsPosts, postsBeforeInvalid, "Save must detect conflicts before writing");
  await page.getByRole("button", { name: "Use server values", exact: true }).click();
  assert.equal(await title.inputValue(), otherEdit.feed.title);
  await openSettings("AI");
  assert.equal(await timeout.inputValue(), "45000", "Accepting a conflicting server value discarded an unrelated draft edit");
  assert.equal(await saveSettings.isEnabled(), true);
  await discard.click();

  await page.getByRole("button", { name: "Clear OpenAI API key", exact: true }).click();
  await openSettings("Books & feed");
  const editionPreference = "Prefer full-cast audio.\nKeep unabridged narration when available.";
  await page.locator("textarea").and(page.getByLabel("Edition preference", { exact: true })).fill(editionPreference);
  await openSettings("Indexers");
  const indexerTable = page.getByRole("table", { name: "Discovered indexers" });
  const discoveredRow = indexerTable.getByRole("row").filter({ has: page.getByRole("rowheader", { name: "Fixture Prowlarr indexer", exact: true }) });
  await discoveredRow.waitFor();
  assert.deepEqual(await indexerTable.getByRole("columnheader").allTextContents(), ["Indexer", "Search", "Feeds"]);
  assert.deepEqual(await discoveredRow.getByRole("cell").allTextContents(), ["Audiobooks, Ebooks", "Audiobooks, Ebooks"]);
  await page.getByRole("button", { name: "Add torznab source", exact: true }).click();
  await page.getByLabel("Source name", { exact: true }).fill("Fixture Torznab");
  const torznabUrl = `${saved.prowlarr.url}/17?apikey=embedded-fixture-token`;
  await page.getByLabel("Torznab URL", { exact: true }).fill(torznabUrl);
  await page.getByLabel("Torznab API key", { exact: true }).fill("torznab-replacement-fixture");
  await page.getByLabel("Audiobook categories", { exact: true }).fill("3030,3040");
  await openSettings("TV");
  assert.equal(await page.getByRole("button", { name: "Add path mapping", exact: true }).count(), 0);
  await openSettings("Movies");
  await page.getByRole("button", { name: "Move tier 2 up", exact: true }).click();
  await openSettings("Storage");
  await page.getByLabel("Minimum free space (GiB)", { exact: true }).fill("2.5");
  const protectedTv = page.getByLabel("Protected TV target IDs", { exact: true });
  await protectedTv.fill("42");
  await protectedTv.press("End");
  await protectedTv.press("Enter");
  await protectedTv.pressSequentially("123");
  assert.equal(await protectedTv.inputValue(), "42\n123", "Typing a newline must not collapse the list");
  assert.equal(settingsPosts, postsBeforeInvalid, "Structured changes must not auto-save");
  assert.deepEqual((await rpc("settings.get")).result, otherEdit);
  await saveSettings.click();
  await page.getByText("Settings saved.", { exact: true }).waitFor();
  const grouped = (await rpc("settings.get")).result;
  const [firstTier, secondTier, ...otherTiers] = saved.movies.qualityPolicy.tiers;
  assert.deepEqual(grouped, { ...otherEdit, agents: { ...saved.agents, apiKey: "", editionPreference },
    torznab: [{ name: "Fixture Torznab", baseUrl: torznabUrl, apiKey: "torznab-replacement-fixture", categories: { audio: "3030,3040" } }],
    movies: { ...saved.movies, qualityPolicy: { ...saved.movies.qualityPolicy, tiers: [secondTier, firstTier, ...otherTiers] } },
    retention: { ...saved.retention, minimumFreeBytes: 2.5 * 1024 ** 3, protectedTvTargetIds: [42, 123] },
  });
  await openSettings("Indexers");
  await page.getByText("Already managed by Prowlarr. You can remove this manual entry.", { exact: true }).waitFor();
  assert.equal(await page.getByLabel("Torznab API key", { exact: true }).inputValue(), "");
  assert.equal(await page.getByLabel("Torznab URL", { exact: true }).inputValue(), "", "A URL with embedded credentials was exposed");
  await page.getByRole("button", { name: "Remove torznab source 1", exact: true }).click();
  await saveSettings.click();
  await page.getByText("Settings saved.", { exact: true }).waitFor();
  assert.deepEqual((await rpc("settings.get")).result, { ...grouped, torznab: [] });
  await discoveredRow.waitFor();

  await page.setViewportSize({ width: 390, height: 844 });
  await openSettings("Books & feed");
  await title.waitFor();
  assert.equal(await page.getByLabel("Edition preference", { exact: true }).inputValue(), editionPreference);
  await page.evaluate("scrollTo(0, 0)");
  await page.screenshot({ path: path.join(artifacts, "admin-settings-mobile.png"), animations: "disabled" });
  for (const section of ["Books & feed", "AI", "Indexers", "Downloads", "Movies", "TV", "Storage", "Notifications", "Plex access"]) {
    await openSettings(section);
    await page.getByRole("heading", { name: section, exact: true }).waitFor();
    assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"), false, `${section} settings overflow mobile viewport`);
  }
  await plexPicker.waitFor();
  assert.equal(await plexPicker.inputValue(), "fixture-cyprus");
  await plexPicker.scrollIntoViewIfNeeded();
  await page.screenshot({ path: path.join(artifacts, "admin-settings-plex-mobile.png"), animations: "disabled" });
  assert.equal((await rpc("jobs.list", { type: "full_library_refresh" })).result.jobs.length, 0, "Opening pages queued a refresh");
  if (process.env.SETTINGS_ONLY === "1") {
    assert.deepEqual(pageErrors, [], "Settings UI page errors");
    assert.deepEqual(networkErrors, [], "Unexpected Settings UI request/HTTP errors");
    console.log("Settings Chrome acceptance passed at 1280x900 and 390x844.");
    return;
  }
  await page.setViewportSize({ width: 1280, height: 900 });
  await adminNavigation.getByRole("link", { name: "Activity", exact: true }).click();
  console.log("Admin browser: checking operational triage");
  await page.getByRole("heading", { name: "Activity", exact: true }).waitFor();
  const sharing = page.getByRole("region", { name: "Sharing and transfers", exact: true });
  await sharing.getByText("200 GiB", { exact: true }).waitFor();
  const published = page.getByRole("region", { name: "Published library", exact: true });
  await published.getByText("1 ebook · 1 audiobook", { exact: true }).waitFor();
  assert.ok(await page.evaluate("Boolean(document.getElementById('activity-library').compareDocumentPosition(document.getElementById('activity-work')) & Node.DOCUMENT_POSITION_FOLLOWING)"));
  const history = page.getByRole("region", { name: "Sharing history", exact: true });
  await sharing.getByRole("link", { name: "View history", exact: true }).click();
  assert.equal(new URL(page.url()).hash, "#activity-sharing-history");
  await history.getByText("Tracker totals and cross-seed outcomes", { exact: true }).click();
  const reuse = history.getByRole("region", { name: "Cross-seeding", exact: true });
  for (const label of ["Seeds created", "Pending", "Skipped"]) await reuse.getByText(label, { exact: true }).waitFor();
  assert.ok(!(await reuse.innerText()).includes("Success rate"));
  assert.ok(!(await reuse.innerText()).includes("seed time"), "All-torrent seed time was attributed to cross-seeding");
  const backfill = page.getByRole("region", { name: "System", exact: true }).getByRole("region", { name: "Find more seeds", exact: true });
  await backfill.getByRole("button", { name: "Search library", exact: true }).click();
  await backfill.getByRole("status").getByText("Preparing library search", { exact: true }).waitFor();
  await page.reload();
  await backfill.getByRole("button", { name: "Stop search", exact: true }).waitFor();
  await backfill.getByRole("button", { name: "Stop search", exact: true }).click();
  await backfill.getByRole("status").getByText("Library search stopped", { exact: true }).waitFor();
  assert.ok((await backfill.innerText()).includes("count as seeds only after verification"));
  await history.getByText("Tracker totals and cross-seed outcomes", { exact: true }).click();
  await history.getByText(/combined seed time across all managed torrents/).waitFor();
  await published.getByText("Disk usage by media type", { exact: true }).click();
  const inventory = published.getByRole("region", { name: "Library inventory", exact: true });
  for (const name of ["Books", "eBooks", "Audiobooks"]) {
    const row = inventory.getByRole("rowheader", { name, exact: true }).locator("..");
    assert.equal(await row.getByRole("cell").first().innerText(), "1");
  }
  await published.getByText("Disk usage by media type", { exact: true }).click();
  const chart = history.getByRole("region", { name: "Tracker history", exact: true });
  await chart.getByRole("img", { name: "Seed size by tracker; missing readings appear as gaps" }).waitFor();
  assert.equal(await chart.locator("svg path").count(), 2, "Both tracker series must be visible together");
  assert.equal(await history.getByLabel("Chart tracker").count(), 0);
  assert.equal(await history.getByRole("button", { name: "Edit tracker names", exact: true }).count(), 0);
  const bookSeries = chart.getByRole("button", { name: /Book tracker/ });
  await bookSeries.click();
  assert.equal(await chart.locator("svg path").count(), 1);
  await bookSeries.click();
  await chart.getByLabel("Chart measure").selectOption("uploadedBytes");
  await bookSeries.click();
  const weekResponse = page.waitForResponse(response => response.url().includes("transferMetrics.status") && new URL(response.url()).searchParams.get("input")?.includes("168") === true);
  await chart.getByRole("button", { name: "7d", exact: true }).click();
  assert.equal((await weekResponse).status(), 200);
  assert.equal(await chart.getByLabel("Chart measure").inputValue(), "uploadedBytes", "Changing the time range reset the selected measure");
  assert.equal(await bookSeries.getAttribute("aria-pressed"), "false", "Changing the time range reset tracker visibility");
  await bookSeries.click();
  await chart.getByRole("button", { name: "24h", exact: true }).click();
  assert.equal(await chart.locator("svg path").count(), 2);
  const paths = await chart.locator("svg path").evaluateAll(nodes => nodes.map(node => node.getAttribute("d")));
  assert.ok(paths.every(path => path && new Set([...path.matchAll(/,([\d.]+)/g)].map(match => match[1])).size > 1), "Growing upload counters must not be drawn as flat lines");
  assert.equal(await chart.getByRole("slider").count(), 0, "Chart inspection must not look like playback or progress");
  const plot = chart.getByRole("img"), inspected = chart.getByLabel("Inspected sample", { exact: true });
  await plot.hover({ position: { x: 30, y: 40 } });
  await chart.locator(".transfer-crosshair").waitFor({ state: "attached" });
  assert.notEqual(await inspected.innerText(), "Latest sample");
  assert.deepEqual(await chart.locator("svg path").evaluateAll(nodes => nodes.map(node => node.getAttribute("d"))), paths, "Hover must not alter the plotted series");
  await chart.getByRole("heading", { name: "Tracker history", exact: true }).hover();
  await chart.locator(".transfer-crosshair").waitFor({ state: "detached" });
  assert.equal(await inspected.innerText(), "Latest sample");
  await plot.focus();
  await plot.press("Home");
  const firstSample = await inspected.innerText();
  assert.notEqual(firstSample, "Latest sample");
  await plot.press("ArrowRight");
  assert.notEqual(await inspected.innerText(), firstSample, "Keyboard navigation must inspect another sample");
  await plot.press("End");
  await plot.press("Escape");
  assert.equal(await inspected.innerText(), "Latest sample");
  await chart.getByLabel("Zoom vertical scale", { exact: true }).check();
  await chart.getByLabel("Chart measure").selectOption("seedBytes");
  await chart.getByLabel("Chart measure").selectOption("seedingTorrents");
  await chart.getByText("No change in this window", { exact: true }).waitFor();
  await chart.getByLabel("Chart measure").selectOption("uploadedBytes");
  const scrape = await context.request.get(new URL("/metrics", url).href);
  assert.equal(scrape.status(), 200);
  assert.ok((await scrape.text()).includes("podible_seeding_torrents 2"));
  const mainLinks = page.getByRole("navigation", { name: "Main navigation" }).getByRole("link");
  assert.deepEqual(await mainLinks.evaluateAll(items => items.map(link => [link.textContent, link.getAttribute("href")])),
    [["Library", "/library"], ["Notifications", "/notifications"], ["Activity", "/ops"], ["Settings", "/settings"]]);
  const active = page.getByRole("region", { name: "Active work", exact: true });
  const waiting = page.getByRole("region", { name: "Waiting work", exact: true });
  const attention = page.getByRole("region", { name: "Needs attention", exact: true });
  const mapping = attention.getByRole("article", { name: "Tom and Jerry", exact: true });
  await mapping.getByText("Episode mapping needed", { exact: true }).waitFor();
  assert.ok((await mapping.innerText()).includes("1940, 1950, 1960"));
  assert.equal(await mapping.getByRole("button", { name: "Retry", exact: true }).count(), 0);
  assert.equal(await mapping.getByText("Recovery state unknown", { exact: true }).count(), 0);
  const providers = page.getByRole("region", { name: "Connections", exact: true });
  const storage = page.getByRole("region", { name: "Storage and links", exact: true });
  await waiting.getByRole("link", { name: "Fixture Active Book", exact: true }).waitFor();
  await waiting.getByRole("link", { name: "Fixture Active Download", exact: true }).waitFor();
  await waiting.getByText("Waiting for download status.", { exact: true }).waitFor();
  assert.equal(await waiting.getByText("Telemetry: unknown.", { exact: false }).first().isVisible(), false, "Raw telemetry should not lead the activity view");
  await page.getByRole("region", { name: "Waiting work", exact: true }).getByText("Retry scheduled", { exact: true }).waitFor();
  assert.equal(await active.getByText("Fixture Automatic Retry", { exact: true }).count(), 0, "Sleeping work must not be presented as running");
  await waiting.getByText("Finding audiobook editions", { exact: true }).first().waitFor();
  await active.getByText("No downloads or imports in progress.", { exact: true }).waitFor();
  await attention.getByRole("link", { name: "Fixture Import Failure", exact: true }).waitFor();
  assert.equal(await attention.getByText("Fixture Waiting For Offers", { exact: false }).count(), 0);
  assert.equal(await attention.getByText("Fixture Automatic Retry", { exact: true }).count(), 0, "Automatic recovery is not a human-action alert");
  await page.getByText("1 request is waiting for a release or an unaired episode.", { exact: false }).waitFor();
  const activityPreviews = path.resolve("tmp/movies-lab");
  await mkdir(activityPreviews, { recursive: true });
  await page.screenshot({ path: path.join(activityPreviews, "admin-activity-initial.png"), fullPage: true, animations: "disabled" });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"), false, "Initial Activity overflows mobile viewport");
  await page.screenshot({ path: path.join(activityPreviews, "admin-activity-initial-mobile.png"), fullPage: true, animations: "disabled" });
  await page.setViewportSize({ width: 1280, height: 900 });
  await providers.getByText("Fixture provider", { exact: true }).waitFor();
  await providers.getByText("2 providers", { exact: true }).waitFor();
  await providers.getByText("Mixed reports", { exact: true }).waitFor();
  const historyCard = providers.locator("details.connections-provider").filter({ has: page.getByText("History provider", { exact: true }) });
  await historyCard.getByText("Success recorded", { exact: true }).waitFor();
  await historyCard.locator("summary").click();
  assert.equal(await providers.locator(".connections-gap, [data-state='gap']").count(), 0);
  assert.doesNotMatch(await providers.innerText(), /feed gaps?|history gap|complete feed history|could not be verified/i);
  await historyCard.locator("summary").click();
  const providerCard = providers.locator("details.connections-provider").filter({ has: page.getByText("Fixture provider", { exact: true }) });
  assert.equal(await providerCard.count(), 1, "One provider should not repeat for each media type");
  assert.equal(await providerCard.getByRole("article", { name: "Movies feed checkpoints" }).isVisible(), false);
  await providerCard.locator("summary").focus();
  await page.keyboard.press("Enter");
  await providerCard.getByRole("article", { name: "Movies feed checkpoints" }).waitFor();
  await providerCard.getByRole("article", { name: "TV feed checkpoints" }).waitFor();
  await providerCard.getByText("A cooldown was reported.", { exact: false }).waitFor();
  assert.doesNotMatch(await providerCard.innerText(), /feed gaps?|history gap|complete feed history/i);
  assert.equal(await providerCard.locator("time[datetime]").count() > 0, true);
  await providerCard.locator("summary").focus();
  await page.keyboard.press("Enter");
  assert.equal(await providerCard.getByRole("article", { name: "Movies feed checkpoints" }).isVisible(), false);
  await storage.getByText("Capacity unknown", { exact: true }).first().waitFor();
  assert.equal(await storage.locator("code:visible").count(), 0, "Filesystem paths belong in storage details, not the overview");
  assert.equal(diagnosticsRequests, 0, "Primary Ops view loaded the raw job history");
  const baselineOps = (await rpc("adminWeb.ops", { includeJobs: false })).result;
  assert.equal(baselineOps.feeds.find((feed: { provider: string }) => feed.provider === "History provider")?.gap, true,
    "Hiding historical UI noise must not erase saved backend evidence");
  const feedDiagnostics = (await rpc("indexerFeeds.status")).result;
  const historyFeed = feedDiagnostics.feeds.find((feed: { provider: string }) => feed.provider === "History provider");
  assert.ok(historyFeed?.gapSince);
  assert.equal(historyFeed.gapNote, "Earlier history could not be verified");
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
  expectedRetryRejection = true;
  await page.route("**/api/trpc/jobs.retry*", async route => {
    const response = await route.fetch({ headers: { ...route.request().headers(), origin: "https://foreign.invalid" } });
    await route.fulfill({ response });
  });
  const rejectedRetry = page.waitForResponse(response => response.url().includes("jobs.retry") && response.request().method() === "POST");
  const retryButton = attention.getByRole("article", { name: "Fixture Import Failure", exact: true }).getByRole("button", { name: "Retry", exact: true });
  await retryButton.click();
  assert.equal((await rejectedRetry).status(), 403);
  await attention.getByRole("alert").filter({ hasText: "Cross-origin mutation rejected" }).waitFor();
  assert.ok(!(await attention.innerText()).includes("Unable to transform response"));
  assert.ok(!(await attention.innerText()).includes("Retry was not queued"));
  assert.equal((await rpc("jobs.get", { jobId: failedImport.id })).result.job.status, "failed");
  await page.unroute("**/api/trpc/jobs.retry*");
  expectedRetryRejection = false;
  const retryResponse = page.waitForResponse((response) => response.url().includes("jobs.retry") && response.request().method() === "POST");
  await attention.getByRole("article", { name: "Fixture Import Failure", exact: true }).getByRole("button", { name: "Retry", exact: true }).click();
  assert.equal((await retryResponse).status(), 200);
  await page.getByText("Retry requested for Fixture Import Failure. Its current progress appears below.", { exact: true }).waitFor();
  assert.equal(diagnosticsRequests, 0, "Retrying a specific problem must not require opening global execution history");
  assert.equal(await page.getByText("Execution details", { exact: true }).count(), 0);
  assert.equal(await page.locator("summary").filter({ hasText: /Job #/ }).count(), 0);
  assert.equal(await page.getByText("Integration comparisons", { exact: true }).count(), 0);
  assert.equal(await page.getByText("About these reports", { exact: true }).count(), 0);
  await page.waitForTimeout(5300);
  assert.equal(diagnosticsRequests, 0, "Activity must not fetch the removed worker history");
  await page.getByRole("button", { name: "Refresh library metadata", exact: true }).click();
  await page.getByRole("status").getByText("Library refresh queued.", { exact: true }).waitFor();
  const firstRefresh = (await rpc("jobs.list", { type: "full_library_refresh" })).result.jobs;
  assert.equal(firstRefresh.length, 1);
  assert.equal(firstRefresh[0].status, "queued");
  assert.equal(firstRefresh[0].max_attempts, 2);
  await page.getByRole("button", { name: "Refresh library metadata", exact: true }).click();
  await page.getByRole("status").filter({ hasText: "Library refresh already queued or running" }).waitFor();
  assert.equal((await rpc("jobs.list", { type: "full_library_refresh" })).result.jobs.length, 1);
  await sharing.screenshot({ path: path.join(artifacts, "transfer-metrics-desktop.png"), animations: "disabled" });
  await page.screenshot({ path: path.join(artifacts, "admin-ops-desktop.png"), fullPage: true, animations: "disabled" });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"), false, "Ops overflows mobile viewport");
  await sharing.screenshot({ path: path.join(artifacts, "transfer-metrics-mobile.png"), animations: "disabled" });
  await waiting.scrollIntoViewIfNeeded();
  await page.screenshot({ path: path.join(artifacts, "admin-ops-mobile.png"), animations: "disabled" });
  const cleanupBefore = (await rpc("jobs.list", { type: "retention_cleanup" })).result.jobs;
  const movieStorage = page.getByRole("region", { name: "Storage and links" }).getByRole("article").filter({ has: page.getByRole("heading", { name: "Movies", exact: true }) });
  await movieStorage.getByText("Storage details", { exact: true }).click();
  await movieStorage.getByRole("link", { name: "Review removable titles", exact: true }).click();
  await page.getByRole("heading", { name: "Free up space", exact: true }).waitFor();
  await page.getByRole("button", { name: "Check storage", exact: true }).click();
  await page.getByText("No disk pressure", { exact: false }).waitFor();
  const waitingAudit = page.locator("details.panel").filter({ has: page.locator("summary").filter({ hasText: "Fixture Waiting For Offers" }) }).first();
  await waitingAudit.locator("summary").first().click();
  await waitingAudit.getByRole("button", { name: "Check removal eligibility", exact: true }).click();
  await waitingAudit.getByText("No selected canonical movie version can be evaluated for this target.", { exact: true }).waitFor();
  assert.equal(await page.getByRole("button", { name: "Remove library copy", exact: true }).count(), 0);
  assert.deepEqual((await rpc("jobs.list", { type: "retention_cleanup" })).result.jobs, cleanupBefore, "Preview queued a destructive job");
  assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"), false, "Retention overflows mobile viewport");
  await page.screenshot({ path: path.join(artifacts, "admin-retention-mobile.png"), animations: "disabled" });
  assert.deepEqual(pageErrors, [], "Admin UI page errors");
  assert.deepEqual(networkErrors, [], "Unexpected admin UI request/HTTP errors");
  console.log("Admin Chrome acceptance passed: authorization, grouped Settings, secret preservation, draft conflicts, explicit saves and Plex selection, Activity, and mobile layout.");
}

try {
  await verify();
} finally {
  await browser?.close();
  server.kill("SIGTERM");
  await server.exited;
  await rm(tlsRoot, { recursive: true, force: true });
}
