import assert from "node:assert/strict";
import { chromium } from "playwright";

const server = Bun.spawn([process.execPath, "run", "test/fixtures/book-spa-server.ts"], {
  stdout: "pipe", stderr: "inherit", env: process.env,
});
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
let releaseIdentity = () => {};
try {
  const reader = server.stdout.getReader();
  let output = "", origin = "";
  const deadline = Date.now() + 15_000;
  while (!origin) {
    const part = await Promise.race([reader.read(), Bun.sleep(Math.max(1, deadline - Date.now())).then(() => { throw new Error("Library performance fixture startup timed out"); })]);
    output += new TextDecoder().decode(part.value);
    const fixture = /BOOK_FIXTURE=(.+)/.exec(output)?.[1];
    if (fixture) origin = JSON.parse(fixture).url;
    if (part.done && !origin) throw new Error("Library performance fixture exited without a URL");
  }
  reader.releaseLock();
  void (async () => { const drain = server.stdout.getReader(); while (!(await drain.read()).done) {} })();
  browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
  const context = await browser.newContext({ viewport: { width: 1280, height: 720 } });
  context.setDefaultTimeout(15_000);
  await context.addCookies([{ name: "podible_session", value: "book-spa-admin", url: origin, httpOnly: true }]);
  const page = await context.newPage();
  const errors: string[] = [], requests: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("response", (response) => { if (response.status() >= 500) errors.push(`HTTP ${response.status()} ${new URL(response.url()).pathname}`); });
  page.on("request", (request) => { const path = new URL(request.url()).pathname; if (path.startsWith("/api/trpc/")) requests.push(path); });
  const identityGate = new Promise<void>((resolve) => { releaseIdentity = resolve; });
  await page.route("**/api/trpc/auth.me**", async (route) => { await identityGate; await route.continue(); });
  await page.goto(new URL("/library?media=books", origin).href);
  await page.locator(".book-card").nth(47).waitFor();
  assert.equal(await page.locator(".book-card").count(), 48);
  assert.equal(requests.filter((path) => path.includes("library.bootstrap")).length, 1);
  assert.equal(requests.some((path) => path.includes("auth.me")), false, "Initial Library still waits for a separate auth query");
  assert.equal(requests.some((path) => path.includes("library.list")), false, "Bootstrap immediately duplicated the first-page request");
  await page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "Settings", exact: true }).waitFor();
  releaseIdentity();
  const refresh = await page.waitForResponse(response => new URL(response.url()).pathname.includes("library.list"), { timeout: 25_000 });
  const refreshInput = JSON.parse(new URL(refresh.url()).searchParams.get("input")!);
  console.log("Library background refresh input:", JSON.stringify(refreshInput));
  assert.equal(refresh.status(), 200, "The first seeded-page background refresh must succeed");
  assert.equal(Object.values(refreshInput).some(input => (input as { cursor?: unknown }).cursor === null), false,
    "The initial-page cursor must be omitted, not serialized as null");
  assert.equal(await page.locator(".book-card").count(), 48);
  assert.equal(await page.getByRole("alert").count(), 0);
  await page.getByRole("button", { name: "Load more", exact: true }).click();
  await page.locator(".book-card").nth(51).waitFor();
  await page.getByRole("searchbox").fill("Piranesi");
  await page.getByRole("link", { name: /Piranesi Susanna Clarke/ }).waitFor();
  await page.waitForFunction("document.querySelectorAll('.book-card').length === 1");
  assert.equal(requests.some((path) => path.includes("library.list")), true, "Filtering and pagination must keep using their own query keys");
  requests.length = 0;
  await page.goto(new URL("/library?media=books&q=Piranesi", origin).href);
  await page.getByRole("link", { name: /Piranesi Susanna Clarke/ }).waitFor();
  assert.equal(await page.locator(".book-card").count(), 1, "Filtered bootstrap was seeded with the unfiltered page");
  assert.equal(requests.filter((path) => path.includes("library.bootstrap")).length, 1);
  assert.equal(requests.some((path) => /auth.me|library.list/.test(path)), false);

  const readerContext = await browser.newContext();
  readerContext.setDefaultTimeout(15_000);
  await readerContext.addCookies([{ name: "podible_session", value: "book-spa-reader", url: origin, httpOnly: true }]);
  const readerPage = await readerContext.newPage();
  await readerPage.goto(new URL("/library?media=books", origin).href);
  await readerPage.locator(".book-card").nth(47).waitFor();
  assert.equal(await readerPage.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "Settings", exact: true }).count(), 0);
  const anonymous = await browser.newContext();
  anonymous.setDefaultTimeout(15_000);
  const anonymousPage = await anonymous.newPage();
  await anonymousPage.goto(origin);
  await anonymousPage.getByRole("heading", { name: "Sign in", exact: true }).waitFor();
  assert.equal(await anonymousPage.locator(".book-card").count(), 0, "Anonymous navigation displayed another actor's seeded Library");
  assert.deepEqual(errors, []);
  console.log("Library performance Chrome acceptance passed: one authenticated bootstrap before cards, no auth waterfall or duplicate page fetch, exact filter/pagination keys, and reader/anonymous isolation.");
} finally {
  releaseIdentity();
  await browser?.close();
  server.kill("SIGTERM");
  await server.exited;
}
