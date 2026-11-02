import { chromium } from "playwright";
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
  await page.goto(new URL("/app/movies", url).href);
  await page
    .getByRole("heading", { name: "Find your next feature." })
    .waitFor();
  await page.evaluate("window.__spaNavigationProof = 42");
  await page.getByRole("searchbox", { name: "Search movies" }).fill("fixture");
  await page.getByRole("button", { name: "Search", exact: true }).click();
  await page
    .getByRole("heading", { name: "A Fixture Film", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Request movie", exact: true })
    .click();
  console.log("Movie browser: submitted request");
  await page
    .getByRole("heading", { name: "Needs attention", exact: true })
    .waitFor();
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
  await page.getByRole("link", { name: "Requests", exact: true }).click();
  await page
    .getByRole("heading", { name: "Your requests.", exact: true })
    .waitFor();
  if ((await page.evaluate("window.__spaNavigationProof")) !== 42)
    throw new Error("SPA navigation reloaded the document");
  await page.goto(detailUrl);
  await page
    .getByRole("heading", { name: "Ready to watch in Plex", exact: true })
    .waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
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
  await page
    .getByRole("button", { name: "Withdraw my request", exact: true })
    .click();
  await page
    .getByRole("heading", { name: "Request withdrawn", exact: true })
    .waitFor();
  await context.clearCookies();
  await page.reload();
  await page.getByRole("link", { name: "Sign in", exact: true }).waitFor();
  if (errors.length) throw new Error(errors.join("\n"));
  console.log(
    "Movie SPA acceptance passed: catalog, request, blocked/manual recovery, upgrade, availability, client routing, deep link, mobile, withdrawal and expired session.",
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
