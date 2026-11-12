import { chromium } from "playwright";
import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";

const server = Bun.spawn(
  [process.execPath, "run", "test/fixtures/spa-server.ts"],
  { stdout: "pipe", stderr: "inherit", env: { ...process.env, PORT: "0" } },
);
const reader = server.stdout.getReader();
const first = await Promise.race([
  reader.read(),
  Bun.sleep(10_000).then(() => {
    throw new Error("Fixture server startup timed out");
  }),
]);
reader.releaseLock();
const url = new TextDecoder()
  .decode(first.value)
  .match(/SPA_FIXTURE_URL=(\S+)/)?.[1];
if (!url) {
  server.kill();
  throw new Error("Missing fixture URL");
}
const browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
try {
  const context = await browser.newContext({
    viewport: { width: 1440, height: 960 },
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
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto(new URL("/library?media=books", url).href);
  await page
    .getByRole("heading", { name: "The Odyssey", exact: true })
    .waitFor();
  const mainLinks = page.getByRole("navigation", { name: "Main navigation" }).getByRole("link");
  assert.deepEqual(await mainLinks.evaluateAll(items => items.map(link => [link.textContent, link.getAttribute("href")])),
    [["Library", "/library"]], "A reader must not see administrator destinations");
  await page.goto(new URL("/movie-dual-write", url).href);
  assert.equal(new URL(page.url()).pathname, "/ops");
  await page.getByRole("alert").getByText("Administrator access required.").waitFor();
  await page.getByRole("navigation").getByRole("link", { name: "Library", exact: true }).click();
  await page.getByRole("group", { name: "Media type" }).getByRole("button", { name: "Books", exact: true }).click();
  await page.getByRole("heading", { name: "The Odyssey", exact: true }).waitFor();
  await page.getByRole("searchbox").fill("Piranesi");
  await page
    .getByRole("heading", { name: "The Odyssey", exact: true })
    .waitFor({ state: "detached" });
  await page.getByRole("heading", { name: "Piranesi", exact: true }).waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  if (await page.evaluate("document.documentElement.scrollWidth > innerWidth"))
    throw new Error("Mobile page overflows horizontally");
  const artifacts = path.resolve("tmp/movies-lab");
  await mkdir(artifacts, { recursive: true });
  await page.screenshot({
    path: path.join(artifacts, "spa-mobile.png"),
    fullPage: true,
    animations: "disabled",
  });
  if (
    (
      await context.request.get(new URL("/web-assets/missing.js", url).href)
    ).status() !== 404
  )
    throw new Error("Missing asset became SPA HTML");
  if (
    (
      await context.request.get(new URL("/api/trpc/missing", url).href)
    ).status() !== 404
  )
    throw new Error("Missing API procedure became SPA HTML");
  await context.clearCookies();
  await page.reload();
  await page.getByRole("link", { name: "Sign in", exact: true }).waitFor();
  assert.deepEqual(await mainLinks.evaluateAll(items => items.map(link => [link.textContent, link.getAttribute("href")])),
    [["Library", "/library"]], "Signed-out navigation must not expose account destinations");
  if (errors.length) throw new Error(errors.join("\n"));
  console.log(
    "SPA browser acceptance passed: typed API, live filtering, mobile layout, deep link, missing resources and expired session.",
  );
} finally {
  await browser.close();
  server.kill();
  await server.exited;
}
