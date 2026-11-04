import { chromium } from "playwright";
import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";
const server = Bun.spawn(
  [process.execPath, "run", "test/fixtures/spa-server.ts"],
  {
    stdout: "pipe",
    stderr: "inherit",
    env: { ...process.env, PORT: "0", SHADOW_FIXTURE: "1" },
  },
);
const reader = server.stdout.getReader();
let output = "";
let url = "";
while (!url) {
  const data = await reader.read();
  output += new TextDecoder().decode(data.value);
  url = /SPA_FIXTURE_URL=(\S+)/.exec(output)?.[1] ?? "";
  if (data.done) throw new Error("Shadow fixture failed");
}
reader.releaseLock();
const browser = await chromium.launch({
  ...(process.env.CI ? {} : { channel: "chrome" }),
  headless: true,
});
try {
  const context = await browser.newContext({
    viewport: { width: 1280, height: 900 },
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
  const errors: string[] = [];
  const auditRequests: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("request", (request) => {
    if (new URL(request.url()).pathname.includes("movieShadow."))
      auditRequests.push(request.url());
  });
  await page.goto(new URL("/movie-dual-write", url).href);
  await page.getByRole("heading", { name: "Movie sync", exact: true }).waitFor();
  await page.getByRole("button", { name: "Sync new requests" }).waitFor();
  await page.getByRole("heading",{name:"Indexer feeds",exact:true}).waitFor();
  await page.locator("summary").filter({hasText:"Fixture indexer"}).click();
  await page.getByText("Earlier releases are outside the available feed history.",{exact:true}).waitFor();
  await page.getByRole("button",{name:"Poll feeds now",exact:true}).click();
  await page.getByText("Feed polling queued.",{exact:true}).waitFor();
  assert.equal(await page.getByRole("heading", { name: "Seerr dual-write" }).count(), 1);
  const primaryNavigation = page.getByRole("navigation", { name: "Main navigation" });
  assert.equal(await primaryNavigation.getByRole("link", { name: "Activity", exact: true }).getAttribute("href"), "/ops");
  assert.equal(await primaryNavigation.getByRole("link", { name: "Movie sync", exact: true }).count(), 0);
  assert.equal(await page.getByRole("navigation").getByRole("link", { name: "Shadow audit" }).count(), 0);
  assert.equal(await page.locator("#radarr-audit").getAttribute("open"), null);
  assert.equal(auditRequests.length, 0);
  await page.getByText("Radarr comparison audit", { exact: true }).click();
  await page.getByText("#1 A Fixture Film", { exact: false }).click();
  await page
    .getByText("Podible: unobservable. No source files remain", { exact: true })
    .waitFor();
  await page
    .getByText("Optional audit feedback (never blocks acquisition)", {
      exact: true,
    })
    .click();
  await page.getByLabel("Human verdict").selectOption("uncertain");
  await page
    .getByLabel("Review notes")
    .fill("Missing source is not a wrong release.");
  await page.getByRole("button", { name: "Save review" }).click();
  await page.getByText("Review saved.", { exact: true }).waitFor();
  await page.reload();
  await page.getByText("Radarr comparison audit", { exact: true }).click();
  await page.getByText("#1 A Fixture Film", { exact: false }).click();
  await page
    .getByText("Optional audit feedback (never blocks acquisition)", {
      exact: true,
    })
    .click();
  if (
    (await page.getByLabel("Review notes").inputValue()) !==
    "Missing source is not a wrong release."
  )
    throw new Error("Review did not persist");
  await page.getByRole("button", { name: "Observe Radarr now" }).click();
  await page.getByText("Observation queued.").waitFor();
  const artifacts = path.resolve("tmp/movies-lab");
  await mkdir(artifacts, { recursive: true });
  await page.screenshot({ path: path.join(artifacts, "movie-sync-desktop.png"), fullPage: true, animations: "disabled" });
  await page.getByText("Radarr comparison audit", { exact: true }).click();
  await page.getByRole("button", { name: "Observe Radarr now" }).waitFor({ state: "detached" });
  const requestsWhenClosed = auditRequests.length;
  await page.waitForTimeout(5500);
  assert.equal(auditRequests.length, requestsWhenClosed, "Collapsed audit continued polling");
  await page.goto(new URL("/movie-shadow?history=1", url).href);
  assert.equal(new URL(page.url()).pathname, "/movie-dual-write");
  assert.equal(new URL(page.url()).search, "?history=1");
  assert.equal(new URL(page.url()).hash, "#radarr-audit");
  await page.getByRole("button", { name: "Observe Radarr now" }).waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: path.join(artifacts, "movie-sync-mobile.png"), fullPage: true, animations: "disabled" });
  if (
    await page.evaluate(
      "document.documentElement.scrollWidth > window.innerWidth",
    )
  )
    throw new Error("Shadow page overflows mobile viewport");
  if (errors.length) throw new Error(errors.join("\n"));
  console.log(
    "Movie sync browser: single admin page, lazy audit, stopped polling, legacy redirect, persistent review, sync, mobile passed",
  );
} finally {
  await browser.close();
  server.kill("SIGTERM");
  await server.exited;
}
