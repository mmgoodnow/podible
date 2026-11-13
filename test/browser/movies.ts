import { chromium } from "playwright";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
const server = Bun.spawn(
  [process.execPath, "run", "test/fixtures/spa-server.ts"],
  { stdout: "pipe", stderr: "inherit", env: { ...process.env, PORT: "0" } },
);
const reader = server.stdout.getReader();
let output = "";
let url: string | undefined;
const deadline = Date.now() + 10_000;
while (!url) {
  const data = await reader.read();
  output += new TextDecoder().decode(data.value);
  url = /SPA_FIXTURE_URL=(\S+)/.exec(output)?.[1];
  if (data.done || Date.now() > deadline)
    throw new Error("Movie browser fixture failed to start");
}
reader.releaseLock();
const browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
let diagnosticPage: Awaited<ReturnType<typeof browser.newPage>> | undefined;
try {
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1000 },
  });
  const externalRequests: string[] = [];
  await context.route("**/*", async (route) => {
    if (new URL(route.request().url()).origin !== new URL(url!).origin) {
      externalRequests.push(route.request().url());
      await route.abort();
    } else await route.continue();
  });
  await context.addCookies([
    {
      name: "podible_session",
      value: "spa-browser-fixture",
      url,
      httpOnly: true,
    },
  ]);
  const page = await context.newPage();
  diagnosticPage = page;
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto(new URL("/movies", url).href);
  await page
    .getByRole("heading", { name: "Movies", exact: true })
    .waitFor();
  if (new URL(page.url()).pathname !== "/movies") throw new Error("Movie navigation still uses /app");
  if (await page.getByRole("navigation").locator('a[href^="/app"]').count())
    throw new Error("Navigation contains noncanonical /app links");
  const idleSearch = page.getByRole("button", { name: "Search", exact: true });
  if (!await idleSearch.isDisabled() || await page.evaluate(
    "getComputedStyle(document.querySelector('button:disabled')).cursor",
  ) !== "default")
    throw new Error("Disabled search shows a busy cursor before a search starts");
  await page.getByText("Movie data & credits", { exact: true }).click();
  const logo = page.getByRole("img", { name: "TMDB", exact: true });
  await logo.evaluate((image) => (image as unknown as { decode(): Promise<void> }).decode());
  const logoState = await logo.evaluate((image) => {
    const img = image as unknown as { naturalWidth: number; currentSrc: string };
    return { width: img.naturalWidth, src: img.currentSrc };
  });
  if (!logoState.width || new URL(logoState.src).origin !== new URL(url!).origin)
    throw new Error("TMDB attribution logo did not decode as a local asset");
  await page.evaluate("window.__spaNavigationProof = 42");
  await page.getByRole("searchbox", { name: "Search movies" }).fill("fixture");
  let finishSearch!: () => void;
  const searchGate = new Promise<void>((resolve) => { finishSearch = resolve; });
  await page.route("**/api/trpc/movies.search**", async (route) => {
    await searchGate;
    await route.continue();
  });
  await page.getByRole("button", { name: "Search", exact: true }).click();
  const searching = page.getByRole("button", { name: "Searching...", exact: true });
  await searching.waitFor();
  await searching.hover();
  if (await searching.getAttribute("aria-busy") !== "true" ||
      await page.evaluate("getComputedStyle(document.querySelector('button[aria-busy=true]')).cursor") !== "default")
    throw new Error("Pending search lacks a busy label or shows a beachball cursor");
  finishSearch();
  let releaseCredits!: () => void;
  const creditsGate = new Promise<void>(resolve => { releaseCredits = resolve; });
  await page.route("**/api/trpc/movies.pageExtras**", async route => {
    await creditsGate;
    await route.continue();
  });
  await page
    .getByRole("heading", { name: "A Fixture Film", exact: true })
    .click();
  await page.unroute("**/api/trpc/movies.search**");
  await page
    .getByRole("button", { name: "Request movie", exact: true })
    .click();
  console.log("Movie browser: submitted request");
  await page
    .getByRole("heading", { name: "Watching for a release", exact: true })
    .waitFor();
  releaseCredits();
  await page.getByText("Directed by Fixture Director", { exact: true }).waitFor();
  await page.getByText("97 min · Drama · Mystery", { exact: true }).waitFor();
  const cast = page.getByRole("region", { name: "Cast", exact: true });
  await cast.getByRole("heading", { name: "Fixture Actor" }).waitFor();
  await cast.getByText("Detective", { exact: true }).waitFor();
  const related = page.getByRole("region", { name: "Related movies", exact: true }).getByRole("link", { name: /A Related Film/ });
  if (await related.getAttribute("href") !== "/movies/102") throw new Error("Related title does not link to its movie page");
  await page.unroute("**/api/trpc/movies.pageExtras**");
  if (!/^\/movies\/\d+$/.test(new URL(page.url()).pathname)) throw new Error("Request changed the canonical movie URL");
  if (await page.getByText(/\/ Request \d+/).count()) throw new Error("Movie request headline exposes a primary request ID");
  await context.request.post(new URL("/__fixture/add-release", url).href);
  await page.getByText("Choose a release manually", { exact: true }).click();
  await page
    .getByRole("button", { name: "Search releases", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Choose release", exact: true })
    .click();
  console.log("Movie browser: selected manual release");
  await page
    .getByRole("heading", { name: "Ready to watch in Plex", exact: true })
    .waitFor();
  const detailUrl = page.url();
  await context.request.post(new URL("/__fixture/add-upgrade", url).href);
  await page
    .getByRole("button", { name: "Check for upgrade", exact: true })
    .click();
  console.log("Movie browser: requested upgrade");
  await page.getByText(/Quality tier: 4/).waitFor();
  await page
    .getByRole("heading", { name: "Ready to watch in Plex", exact: true })
    .waitFor();
  await page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "Library", exact: true }).click();
  await page.getByRole("group", { name: "Media type" }).getByRole("button", { name: "Movies", exact: true }).click();
  await page.getByRole("region", { name: "Your movies" }).locator(".screen-card").first().waitFor();
  if ((await page.evaluate("window.__spaNavigationProof")) !== 42)
    throw new Error("SPA navigation reloaded the document");
  await page.goto(detailUrl);
  await page
    .getByRole("heading", { name: "Ready to watch in Plex", exact: true })
    .waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("region", { name: "Related movies", exact: true }).waitFor();
  if (await page.locator("header").count() !== 1)
    throw new Error("Movie page rendered duplicate app shells");
  if (await page.evaluate("document.documentElement.scrollWidth > innerWidth"))
    throw new Error("Movie request detail overflows on mobile");
  const artifacts = path.resolve("tmp/movies-lab");
  await fs.mkdir(artifacts, { recursive: true });
  await page.screenshot({
    path: path.join(artifacts, "movie-request-mobile.png"),
    fullPage: false,
    animations: "disabled",
  });
  await page.getByText("Movie data & credits", { exact: true }).click();
  await logo.evaluate((image) => (image as unknown as { decode(): Promise<void> }).decode());
  await page.locator(".credits").screenshot({
    path: path.join(artifacts, "movie-credits-mobile.png"),
    animations: "disabled",
  });
  await page
    .getByRole("button", { name: "Withdraw my request", exact: true })
    .click();
  await page
    .getByRole("heading", { name: "Request withdrawn", exact: true })
    .waitFor();
  await page.getByRole("button", { name: "Request a specific version", exact: true }).click();
  await page.getByLabel("Resolution", { exact: true }).selectOption("2160");
  await page.getByRole("button", { name: "Request version", exact: true }).click();
  await page.getByRole("region", { name: "4K version", exact: true }).waitFor();
  await page.getByRole("region", { name: "4K version", exact: true }).getByRole("list", { name: "Acquisition stages" }).waitFor();
  assert.equal(await page.getByText("4K requested. Its progress is shown above.", { exact: true }).count(), 0);
  if (await page.evaluate("document.documentElement.scrollWidth > innerWidth"))
    throw new Error("Version request form overflows on mobile");
  // An in-flight authenticated response can restore a cleared rolling cookie.
  const revoked = await fetch(new URL("/rpc", url), {
    method: "POST",
    headers: { "content-type": "application/json", cookie: "podible_session=spa-browser-fixture" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "auth.logout", params: {} }),
  });
  assert.equal(revoked.status, 200);
  assert.deepEqual((await revoked.json() as { result?: unknown }).result, { ok: true });
  await page.reload();
  await page.getByRole("link", { name: "Sign in", exact: true }).waitFor();
  if (errors.length) throw new Error(errors.join("\n"));
  if (externalRequests.length)
    throw new Error("Fixture UI unexpectedly depends on external assets: " + externalRequests.join(", "));
  console.log(
    "Movie SPA acceptance passed: catalog, request, blocked/manual recovery, upgrade, availability, client routing, deep link, mobile, withdrawal, expired session, offline attribution logo and normal pending-search cursor.",
  );
} catch (error) {
  if (diagnosticPage) {
    await fs.mkdir("tmp/movies-lab", { recursive: true });
    const body = await diagnosticPage.locator("body").innerText();
    await fs.writeFile("tmp/movies-lab/movie-browser-failure.txt", body);
    await diagnosticPage.screenshot({
      path: "tmp/movies-lab/movie-browser-failure.png",
      fullPage: true,
    });
    console.error(body);
  }
  throw error;
} finally {
  await browser.close();
  server.kill();
  await server.exited;
}
