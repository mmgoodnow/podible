import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import { chromium } from "playwright";
import { randomUUID } from "node:crypto";
import { createPodibleFetchHandler } from "../../src/http";
import { createBookSpaData } from "../fixtures/book-spa-data";
import { fixtureMovie, fixtureFacts } from "../fixtures/movie-services";
import { fixtureTvEntry } from "../fixtures/tv-catalog";

// Real repository publications and private requests, separate from the upstream catalog fixture.
async function checkPosterShelves(browser: Awaited<ReturnType<typeof chromium.launch>>) {
  const data = await createBookSpaData();
  const { repo, admin, reader, db } = data;
  const policy = repo.getSettings().movies.qualityPolicy;
  const art = "https://artworks.thetvdb.com/banners/";
  const movieTitles = ["The Last Coast", "A Room in Winter", "After the Rain", "The Quiet Atlas", "A Very Long Movie Title About Finding Our Way Back Home", "Night Orchard"];
  for (const [index, title] of movieTitles.entries()) {
    const requested = repo.movies.request(index === 0 ? reader.id : admin.id, {
      ...fixtureMovie, tmdbId: 500 + index, title, year: 2020 + index,
      posterUrl: index === 3 ? null : `${art}${index === 4 ? "broken" : index}.svg`,
    }, policy);
    const acquisition = repo.movies.startAcquisition(requested.targetId, "initial", () => repo.createJob({ type: "metadata_hydration" }).id).acquisition;
    const id = randomUUID();
    const version = repo.movies.recordVersion(acquisition.id, fixtureFacts(), [{ id, relativePath: `files/${id.slice(0, 2)}/${id}.mkv`, sizeBytes: 10, probe: {} }]);
    repo.movies.selectVersion(requested.targetId, version, 0);
    repo.movies.finishAcquisition(acquisition.id);
    const key = `published:${requested.targetId}:${version}`;
    repo.movies.event(requested.targetId, null, key, "available", "Verified in Plex and ready to watch");
    db.query("UPDATE movie_events SET created_at=? WHERE event_key=?").run(new Date(Date.now() - index * 86_400_000).toISOString(), key);
  }
  repo.movies.request(reader.id, { ...fixtureMovie, tmdbId: 600, title: "Tomorrow's Tide", posterUrl: `${art}6.svg` }, policy);
  repo.movies.request(admin.id, { ...fixtureMovie, tmdbId: 601, title: "Private unreleased movie" }, policy);
  const tvTitles = ["The Glasshouse", "North of Here", "Small Hours"];
  for (const [index, title] of tvTitles.entries()) {
    const entry = fixtureTvEntry();
    entry.episodes = entry.episodes.map(ep => ({ ...ep, key: ep.key.replace("700", String(800 + index)), airDate: new Date(Date.now() - 86_400_000).toISOString().slice(0, 10) }));
    const requested = repo.tv.requestSeasons(index === 0 ? reader.id : admin.id, {
      ...entry, key: `tvdb:${800 + index}`, tvdbId: 800 + index, tmdbId: 800 + index, title,
      posterUrl: `${art}${index + 7}.svg`,
    }, policy, [1]);
    const acquisition = repo.tv.startAcquisition(requested.targetId, "initial", () => repo.createJob({ type: "metadata_hydration" }).id, requested.requestId).acquisition;
    const id = randomUUID();
    const version = repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [{ id, relativePath: `files/${id.slice(0, 2)}/${id}.mkv`, sizeBytes: 10, probe: {}, episodeIds: [requested.episodeIds[0]!] }]);
    repo.tv.stagePublication(requested.targetId, version, 0);
    repo.tv.promotePublication(requested.targetId, version, 0);
    repo.tv.clearPublication(requested.targetId, version);
    repo.tv.finishAcquisition(acquisition.id);
    repo.tv.event(requested.targetId, null, `tv-published:${requested.targetId}:${version}`, "available", "Episode identity and file paths verified in Plex");
  }
  repo.tv.requestSeasons(reader.id, { ...fixtureTvEntry(), key: "tvdb:900", tvdbId: 900, tmdbId: 900, title: "The Next Chapter" }, policy, [2]);
  repo.tv.requestSeasons(admin.id, { ...fixtureTvEntry(), key: "tvdb:901", tvdbId: 901, tmdbId: 901, title: "Private unreleased TV" }, policy, [2]);
  const handler = createPodibleFetchHandler(repo, Date.now());
  const fixture = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: handler });
  const origin = fixture.url.origin;
  const context = await browser.newContext({ viewport: { width: 1440, height: 1100 }, reducedMotion: "reduce" });
  context.setDefaultTimeout(15000);
  const external: string[] = [], errors: string[] = [];
  await context.route("**/*", async route => {
    const url = new URL(route.request().url());
    if (url.origin === origin) return route.continue();
    if (url.href.startsWith(art)) {
      if (url.pathname.endsWith("/broken.svg")) return route.fulfill({ status: 404, body: "" });
      const index = Number(url.pathname.split("/").pop()?.split(".")[0]) || 0;
      const colors = [["#183e3d", "#a9c0a1"], ["#704638", "#d0af8d"], ["#4a5667", "#c8c7b3"]][index % 3]!;
      return route.fulfill({ contentType: "image/svg+xml", body: `<svg xmlns="http://www.w3.org/2000/svg" width="300" height="450" viewBox="0 0 300 450"><rect width="300" height="450" fill="${colors[0]}"/><circle cx="215" cy="135" r="75" fill="${colors[1]}"/><path d="M0 250L100 160L220 280L300 230V450H0Z" fill="#142c2b"/><path d="M0 330Q130 250 300 370V450H0Z" fill="${colors[1]}" opacity=".55"/><text x="25" y="48" fill="#f4f1e9" font-family="Georgia" font-size="14" letter-spacing="4">THE COLLECTION</text></svg>` });
    }
    if (url.hostname === "covers.openlibrary.org") return route.fulfill({ status: 404, body: "" });
    external.push(url.origin);
    return route.abort();
  });
  await context.addCookies([{ name: "podible_session", value: "book-spa-reader", url: origin, httpOnly: true }]);
  const page = await context.newPage();
  page.on("pageerror", error => errors.push(error.message));
  let releaseRequestCards = () => {};
  const requestCardsGate = new Promise<void>(resolve => { releaseRequestCards = resolve; });
  await page.route(/\/api\/trpc\/(?:requests\.list|tv\.requestCards)\?/, async route => {
    await requestCardsGate;
    await route.continue();
  });
  try {
    await page.goto(origin);
    const movieRail = page.getByRole("region", { name: "Newly added movies", exact: true });
    await movieRail.locator(".screen-card").nth(5).waitFor();
    assert.deepEqual(await movieRail.locator("h3").allTextContents(), movieTitles, "Newly published movies must sort by publication date, not IDs");
    const before = await movieRail.boundingBox();
    const requestRail = page.getByRole("region", { name: "Your requests", exact: true });
    const requestPosition = await requestRail.boundingBox();
    assert.ok(before && requestPosition && requestPosition.y < before.y, "Requests must be the first home rail");
    assert.equal(await requestRail.getAttribute("aria-busy"), "true");
    releaseRequestCards();
    await page.getByRole("region", { name: "Your requests", exact: true }).getByRole("link", { name: /Tomorrow's Tide/ }).waitFor();
    await requestRail.getByRole("link", { name: /The Next Chapter/ }).waitFor();
    assert.equal((await movieRail.boundingBox())?.y, before?.y, "Late request cards must not move published rails");
    await page.getByRole("region", { name: "Recently aired episodes", exact: true }).locator(".screen-card").nth(2).waitFor();
    assert.equal(await page.getByText(/Private unreleased/).count(), 0);
    await page.screenshot({ path: path.join(os.tmpdir(), "podible-home-rails-desktop.png"), fullPage: true, animations: "disabled" });
    await page.setViewportSize({ width: 390, height: 844 });
    const track = movieRail.getByRole("group", { name: "Newly added movies cards" });
    await track.focus(); await page.keyboard.press("ArrowRight");
    assert.ok(await track.evaluate(el => el.scrollLeft > 0), "ArrowRight must scroll a focused rail");
    await page.keyboard.press("Home");
    assert.ok(await track.evaluate(el => el.scrollLeft <= 3), "Home must return to the first card (allowing track focus padding)");
    await page.keyboard.press("End");
    assert.ok(await track.evaluate(el => el.scrollLeft > 0));
    assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"), false, "Home rails overflow the mobile page");
    await page.screenshot({ path: path.join(os.tmpdir(), "podible-home-rails-mobile.png"), fullPage: true, animations: "disabled" });
    await movieRail.getByRole("link", { name: /Night Orchard/ }).click();
    await page.getByRole("heading", { name: "Night Orchard", exact: true }).waitFor();
    await page.setViewportSize({ width: 1440, height: 1100 });
    await page.goto(`${origin}/?media=movies`);
    const movies = page.getByRole("region", { name: "Your movies" });
    await movies.getByText("6 available", { exact: true }).waitFor();
    assert.equal(await movies.getByText("Shared library", { exact: false }).count(), 0);
    assert.equal(await page.getByRole("region", { name: "Your books" }).count(), 0);
    const poster = movies.getByRole("img", { name: "The Last Coast movie poster", exact: true });
    await poster.scrollIntoViewIfNeeded();
    await page.waitForFunction("[...document.querySelectorAll('.screen-poster > img')].some(img => img.alt === 'The Last Coast movie poster' && img.complete && img.naturalWidth === 300)");
    const box = await poster.boundingBox();
    assert.ok(box && Math.abs(box.width / box.height - 2 / 3) < 0.01, "Poster must remain 2:3");
    await movies.getByRole("img", { name: "No poster available for The Quiet Atlas", exact: true }).waitFor();
    await movies.getByRole("img", { name: `No poster available for ${movieTitles[4]}`, exact: true }).waitFor();
    await movies.getByRole("link", { name: /Tomorrow's Tide/ }).waitFor();
    assert.equal(await movies.getByRole("region", { name: "Movie requests", exact: true }).locator("summary").count(), 0, "A single waiting request must not need disclosure");
    const ownMovie = movies.getByRole("link", { name: /The Last Coast/ });
    assert.match((await ownMovie.getAttribute("href"))!, /^\/requests\//);
    await ownMovie.click();
    await page.getByRole("heading", { name: "The Last Coast", exact: true }).waitFor();
    await page.goto(`${origin}/?media=movies`);
    await page.getByRole("region", { name: "Your movies" }).getByRole("link", { name: /A Room in Winter/ }).click();
    assert.ok(page.url().endsWith("/movies/501"));
    await page.getByRole("heading", { name: "A Room in Winter", exact: true }).waitFor();
    await page.getByText("Available in Plex", { exact: true }).waitFor();
    assert.equal(await page.getByRole("button", { name: "Add to my library", exact: true }).count(), 0);
    assert.equal(await page.getByRole("button", { name: "Follow movie", exact: true }).count(), 0);
    await page.goto(`${origin}/?media=tv`);
    const tv = page.getByRole("region", { name: "TV", exact: true });
    await tv.getByText("3 available", { exact: true }).waitFor();
    assert.equal(await tv.getByText("Shared library", { exact: false }).count(), 0);
    await page.waitForFunction("['The Glasshouse tv series poster', 'North of Here tv series poster'].every(alt => [...document.querySelectorAll('.screen-poster > img')].some(img => img.alt === alt && img.complete && img.naturalWidth === 300))");
    const progress = tv.getByRole("progressbar", { name: "The Glasshouse available episodes" });
    assert.equal(await progress.getAttribute("value"), "1");
    assert.equal(await progress.getAttribute("max"), "3");
    await tv.getByRole("link", { name: /The Next Chapter/ }).waitFor();
    assert.equal(await tv.getByRole("region", { name: "TV requests", exact: true }).locator("summary").count(), 0, "A single waiting TV request must not need disclosure");
    await tv.getByRole("link", { name: /North of Here/ }).click();
    assert.match(page.url(), /\/tv\/library\/\d+$/);
    await page.getByRole("heading", { name: "North of Here", exact: true }).waitFor();
    await page.goto(`${origin}/?media=movies`);
    await page.getByRole("region", { name: "Your movies" }).getByText("6 available", { exact: true }).waitFor();
    await page.screenshot({ path: path.join(os.tmpdir(), "podible-unified-posters-desktop.png"), fullPage: true, animations: "disabled" });
    await page.setViewportSize({ width: 390, height: 844 });
    for (const media of ["movies", "tv"] as const) {
      await page.goto(`${origin}/?media=${media}`);
      const shelf = page.getByRole("region", { name: media === "movies" ? "Your movies" : "TV", exact: true });
      await shelf.getByText(media === "movies" ? "6 available" : "3 available", { exact: true }).waitFor();
      const cards = shelf.locator(":scope > .screen-grid > .screen-card");
      const first = await cards.nth(0).boundingBox(), second = await cards.nth(1).boundingBox();
      assert.ok(first && second && Math.abs(first.y - second.y) < 1 && second.x > first.x, "Mobile shelf must have two columns");
      assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"), false, `${media} shelf overflows mobile`);
      assert.equal(await page.getByText(/Private unreleased/).count(), 0, "Another member's pending requests must stay private");
      await page.screenshot({ path: path.join(os.tmpdir(), `podible-unified-posters-${media}-mobile.png`), fullPage: true, animations: "disabled" });
    }
    // Bound only the extra rows: the first six requests remain visible to ordinary members.
    for (const media of ["movies", "tv"] as const) {
      const addPending = (index: number) => {
        if (media === "movies") repo.movies.request(reader.id, {
          ...fixtureMovie, tmdbId: 610 + index, title: `Waiting movie ${index}`, posterUrl: `${art}6.svg`,
        }, policy);
        else repo.tv.requestSeasons(reader.id, {
          ...fixtureTvEntry(), key: `tvdb:${910 + index}`, tvdbId: 910 + index, tmdbId: 910 + index, title: `Waiting TV ${index}`,
        }, policy, [2]);
      };
      for (let index = 0; index < 5; index++) addPending(index);
      await page.goto(`${origin}/?media=${media}`);
      const requests = page.getByRole("region", { name: media === "movies" ? "Movie requests" : "TV requests", exact: true });
      await requests.locator(":scope > .screen-grid > .screen-card").nth(5).waitFor();
      assert.equal(await requests.locator(".screen-card:visible").count(), 6);
      assert.equal(await requests.locator("summary").count(), 0, "Exactly six requests need no disclosure");
      addPending(5); addPending(6);
      await page.reload();
      const summary = requests.locator("summary");
      await summary.getByText("2 more", { exact: true }).waitFor();
      assert.equal(await requests.locator(".screen-card:visible").count(), 6, "The first six requests must always remain visible");
      assert.equal(await requests.locator("details .screen-card").count(), 2, "Only remaining requests belong behind disclosure");
      await summary.focus();
      await page.keyboard.press("Enter");
      await requests.locator("details .screen-card").nth(1).waitFor();
      assert.equal(await requests.locator(".screen-card:visible").count(), 8);
      assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"), false, "Expanded request rows overflow mobile");
      await page.keyboard.press("Enter");
      assert.equal(await requests.locator(".screen-card:visible").count(), 6);
    }
    const signedOut = await browser.newContext();
    try {
      const response = await signedOut.request.get(`${origin}/api/trpc/catalog.available?input=${encodeURIComponent(JSON.stringify({ query: "", limit: 50 }))}`);
      assert.equal(response.status(), 401, "Shared catalog must still require authentication");
    } finally { await signedOut.close(); }
    await context.addCookies([{ name: "podible_session", value: "book-spa-admin", url: origin, httpOnly: true }]);
    await page.goto(`${origin}/?media=movies`);
    await page.getByRole("button", { name: "Sign out" }).waitFor();
    const nav = page.getByRole("navigation", { name: "Main navigation" });
    assert.deepEqual(await nav.getByRole("link").allTextContents(), ["Library", "Activity", "Settings"]);
    assert.deepEqual(await nav.getByRole("link").evaluateAll(links => links.map(link => link.getAttribute("href"))), ["/library", "/ops", "/settings"]);
    assert.deepEqual(external, [], "Unexpected external network request");
    assert.deepEqual(errors, []);
  } finally { releaseRequestCards(); await context.close(); fixture.stop(true); await data.close(); }
}
const server = Bun.spawn([process.execPath, "run", "test/fixtures/unified-spa-server.ts"], { stdout: "pipe", stderr: "inherit" });
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
let releaseBooks = () => {};
let releaseDiscovery = () => {};
try {
  const reader = server.stdout.getReader();
  let output = "", origin = "";
  const deadline = Date.now() + 20000;
  while (!origin) {
    const part = await Promise.race([reader.read(), Bun.sleep(Math.max(1, deadline - Date.now())).then(() => { throw new Error("Unified fixture startup timed out"); })]);
    output += new TextDecoder().decode(part.value);
    const value = /UNIFIED_FIXTURE=(.+)/.exec(output)?.[1];
    if (value) origin = JSON.parse(value).url;
    if (part.done && !origin) throw new Error("Unified fixture exited");
  }
  reader.releaseLock();
  void (async () => { const drain = server.stdout.getReader(); while (!(await drain.read()).done) {} })();
  browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  context.setDefaultTimeout(15000);
  await context.addCookies([{ name: "podible_session", value: "book-spa-reader", url: origin, httpOnly: true }]);
  const page = await context.newPage();
  const errors: string[] = [];
  page.on("pageerror", e => errors.push(e.message));
  const gate = new Promise<void>(resolve => { releaseBooks = resolve; });
  const discoveryGate = new Promise<void>(resolve => { releaseDiscovery = resolve; });
  await page.route("**/api/trpc/catalog.discovery**", async route => { await discoveryGate; await route.continue(); });
  await context.route("**/*", async route => {
    const url = new URL(route.request().url());
    if (url.origin === origin) return route.continue();
    if (url.hostname === "image.tmdb.org") return route.fulfill({ contentType: "image/svg+xml", body: '<svg xmlns="http://www.w3.org/2000/svg" width="300" height="450"><rect width="300" height="450" fill="#284e42"/><circle cx="170" cy="160" r="95" fill="#d7c5a0"/><path d="M0 260L140 150L300 340V450H0Z" fill="#577063"/></svg>' });
    return route.fulfill({ status: 404, body: "" });
  });
  await page.route("**/api/trpc/openlibrary.search**", async route => { await gate; await route.continue(); });
  await page.goto(origin);
  await page.getByRole("region", { name: "Newly added books", exact: true }).getByRole("link", { name: /Piranesi/ }).waitFor();
  await page.getByText("Loading trending titles...", { exact: true }).waitFor();
  releaseDiscovery();
  await page.getByRole("region", { name: "Trending movies", exact: true }).locator(".screen-card").nth(13).waitFor();
  await page.getByRole("region", { name: "Trending TV", exact: true }).locator(".screen-card").nth(13).waitFor();
  await page.waitForFunction("[...document.querySelectorAll('.home-rails .screen-poster > img')].some(img => img.complete && img.naturalWidth === 300)");
  await page.getByRole("region", { name: "Trending TV", exact: true }).locator(".screen-card").first().scrollIntoViewIfNeeded();
  await page.screenshot({ path: path.join(os.tmpdir(), "podible-home-discovery-desktop.png"), fullPage: true, animations: "disabled" });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"), false, "Discovery rails overflow mobile");
  await page.screenshot({ path: path.join(os.tmpdir(), "podible-home-discovery-mobile.png"), fullPage: true, animations: "disabled" });
  await page.setViewportSize({ width: 1280, height: 900 });
  assert.equal(await page.getByRole("region", { name: "Your books", exact: true }).count(), 0, "All home must use rails, not the full books grid");
  await page.getByRole("button", { name: "Books", exact: true }).click();
  await page.locator(".book-card").nth(47).waitFor();
  await page.getByRole("button", { name: "All", exact: true }).click();
  assert.equal(await page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: /Discover|Movies|Requests/ }).count(), 0);
  await page.getByRole("searchbox", { name: "Search library and catalogs" }).fill("Piranesi");
  assert.equal(await page.locator(".home-rails").count(), 0, "Search must replace home rails immediately");
  await page.getByRole("region", { name: "Your books" }).getByRole("link", { name: /Piranesi Susanna Clarke/ }).waitFor();
  await page.getByRole("region", { name: "Movie catalog" }).getByRole("link", { name: /Piranesi on Screen/ }).waitFor();
  await page.getByRole("region", { name: "TV catalog" }).getByRole("link", { name: /Piranesi Series/ }).waitFor();
  await page.getByRole("region", { name: "Book catalog" }).getByRole("status").waitFor();
  assert.equal(await page.getByRole("heading", { name: "Piranesi Companion" }).count(), 0, "Slow book provider unexpectedly finished");
  releaseBooks();
  await page.getByRole("heading", { name: "Piranesi Companion" }).waitFor();
  await page.screenshot({ path: path.join(os.tmpdir(), "podible-unified-desktop.png"), fullPage: true, animations: "disabled" });
  await page.getByRole("region", { name: "TV catalog" }).getByRole("link", { name: /Piranesi Series/ }).click();
  await page.getByRole("heading", { name: "Choose seasons" }).waitFor();
  const checkboxes = page.getByRole("group", { name: "Seasons", exact: true }).getByRole("checkbox");
  assert.equal(await checkboxes.count(), 2);
  await checkboxes.first().check();
  await page.getByRole("button", { name: "Request season", exact: true }).click();
  await page.getByRole("heading", { name: "Waiting for upcoming episodes" }).waitFor();
  await page.getByText("0 of 3 episodes verified in Plex.", { exact: true }).waitFor();
  const requestUrl = page.url();
  await page.reload();
  await page.getByText("0 of 3 episodes verified in Plex.", { exact: true }).waitFor();
  await page.getByRole("link", { name: "Back to library" }).click();
  await page.getByRole("region", { name: "TV", exact: true }).getByRole("link", { name: /Piranesi Series/ }).waitFor();
  await page.goto(origin);
  await page.getByRole("region", { name: "Your requests", exact: true }).getByRole("link", { name: /Piranesi Series/ }).waitFor();
  await page.goto(requestUrl);
  page.once("dialog", dialog => dialog.accept());
  await page.getByRole("button", { name: "Withdraw request" }).click();
  await page.getByRole("button", { name: "Withdraw request" }).waitFor({ state: "detached" });
  await page.reload();
  await page.getByRole("heading", { name: "Piranesi Series" }).waitFor();
  assert.equal(page.url(), requestUrl, "Withdrawing a request must not change the show's page");
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(origin);
  await page.getByRole("searchbox").waitFor();
  await page.screenshot({ path: path.join(os.tmpdir(), "podible-unified-mobile.png"), fullPage: false, animations: "disabled" });
  assert.equal(await page.evaluate("document.documentElement.scrollWidth > innerWidth"), false);
  assert.deepEqual(errors, []);
  await checkPosterShelves(browser);
  console.log("Unified Library Chrome acceptance passed: real RPC publications/private requests, poster images and 2:3 geometry, broken/missing artwork, request disclosure, actual detail navigation, TV progress, auth/privacy, two-column mobile/no overflow, independent delayed providers and durable season request/withdrawal.");
} finally { releaseBooks(); releaseDiscovery(); await browser?.close(); server.kill("SIGTERM"); await server.exited; }
