import assert from "node:assert/strict";
import { chromium } from "playwright";

const server = Bun.spawn([process.execPath, "run", "test/fixtures/spa-server.ts"], {
  stdout: "pipe", stderr: "inherit", env: { ...process.env, PORT: "0", ADMIN_FIXTURE: "1", OPS_SCALE_FIXTURE: "1",
    SHADOW_FIXTURE: "", TV_FIXTURE: "", AUTH_FIXTURE: "", FIXTURE_TLS_KEY: "", FIXTURE_TLS_CERT: "" },
});
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
const inventory = Promise.withResolvers<void>();
let startup: ReturnType<typeof setTimeout> | undefined;
try {
  const reader = server.stdout.getReader();
  let output = "", url = "";
  try {
    const timeout = new Promise<never>((_, reject) => {
      startup = setTimeout(() => reject(new Error("Activity fixture startup timed out")), 30_000);
    });
    while (!url) {
      const part = await Promise.race([reader.read(), timeout]);
      output += new TextDecoder().decode(part.value);
      url = /SPA_FIXTURE_URL=(\S+)/.exec(output)?.[1] ?? "";
      if (part.done && !url) throw new Error("Activity fixture exited without a URL");
    }
  } finally { clearTimeout(startup); reader.releaseLock(); }
  browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
  const context = await browser.newContext();
  await context.addCookies([{ name: "podible_session", value: "spa-browser-fixture", url, httpOnly: true }]);
  const page = await context.newPage(), errors: string[] = [];
  page.setDefaultTimeout(10_000);
  page.on("pageerror", error => errors.push(error.message));
  page.on("requestfailed", request => errors.push(request.failure()?.errorText ?? request.url()));
  page.on("response", response => { if (response.status() >= 400) errors.push(`${response.status()} ${response.url()}`); });
  let measurements = 0;
  await page.route("**/api/trpc/**", async route => {
    if (new URL(route.request().url()).pathname.includes("transferMetrics.library")) {
      measurements++;
      await inventory.promise;
    }
    await route.continue();
  });
  await page.goto(new URL("/ops", url).href);
  await page.getByRole("region", { name: "Published library" }).getByText("1 ebook · 1 audiobook", { exact: true }).waitFor();
  await page.getByRole("region", { name: "Waiting work" }).getByText("Fixture Active Book").waitFor();
  await page.getByRole("region", { name: "Tracker history" }).getByRole("img").waitFor();
  assert.equal(measurements, 0, "The overview should not measure files");
  const requestedInventory = page.waitForRequest(request => request.url().includes("transferMetrics.library"));
  await page.getByText("Disk usage by media type", { exact: true }).click();
  await requestedInventory;
  const library = page.getByRole("region", { name: "Library inventory" });
  assert.equal(await library.getByRole("status").textContent(), "Measuring library...");
  inventory.resolve();
  await library.getByRole("table").waitFor();
  assert.deepEqual(await page.getByRole("alert").allTextContents(), []);
  assert.deepEqual(errors, []);
  console.log("Activity acceptance passed: published totals, work and history render with 40,000 jobs; file measurement is on demand.");
} finally {
  inventory.resolve();
  await browser?.close();
  server.kill("SIGTERM");
  const timer = setTimeout(() => server.kill("SIGKILL"), 5000);
  try { await server.exited; } finally { clearTimeout(timer); }
}
