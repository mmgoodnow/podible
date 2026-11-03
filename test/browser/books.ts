import assert from "node:assert/strict";
import { mkdir, readFile } from "node:fs/promises";
import path from "node:path";
import { chromium } from "playwright";
import { createTRPCClient, httpBatchLink } from "@trpc/client";
import type { AppRouter } from "../../src/trpc";

const server = Bun.spawn(
  [process.execPath, "run", "test/fixtures/book-spa-server.ts"],
  { stdout: "pipe", stderr: "inherit", env: process.env },
);
const reader = server.stdout.getReader();
let output = "";
let fixture: {
  url: string;
  bookId: number;
  failedBookId: number;
  manifestationIds: number[];
  importPath: string;
};
try {
  const deadline = Date.now() + 15_000;
  while (!output.includes("BOOK_FIXTURE=")) {
    const next = await Promise.race([
      reader.read(),
      Bun.sleep(Math.max(1, deadline - Date.now())).then(() => {
        throw new Error("Book fixture startup timed out");
      }),
    ]);
    if (next.done) throw new Error(`Book fixture exited: ${output}`);
    output += new TextDecoder().decode(next.value);
  }
  fixture = JSON.parse(output.match(/BOOK_FIXTURE=(.+)/)![1]!);
} catch (error) {
  server.kill();
  throw error;
} finally {
  reader.releaseLock();
}
// Drain logs so a long browser run cannot block the server on a full pipe.
void (async () => {
  const drain = server.stdout.getReader();
  while (!(await drain.read()).done) {}
})();
const browser = await chromium.launch({
  ...(process.env.CI ? {} : { channel: "chrome" }),
  headless: true,
});
const artifacts = path.resolve("tmp/books-spa");
await mkdir(artifacts, { recursive: true });
try {
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1000 },
  });
  await context.addCookies([
    {
      name: "podible_session",
      value: "book-spa-admin",
      url: fixture.url,
      httpOnly: true,
    },
  ]);
  const client = createTRPCClient<AppRouter>({
    links: [
      httpBatchLink({
        url: new URL("/api/trpc", fixture.url).href,
        headers: { cookie: "podible_session=book-spa-admin" },
      }),
    ],
  });
  const page = await context.newPage();
  page.setDefaultTimeout(15_000);
  const errors: string[] = [];
  const badResponses: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("response", (response) => {
    if (response.status() >= 500)
      badResponses.push(`${response.status()} ${response.url()}`);
  });
  await page.route("https://covers.openlibrary.org/**", (route) =>
    route.fulfill({
      contentType: "image/svg+xml",
      body: '<svg xmlns="http://www.w3.org/2000/svg" width="200" height="300"><rect width="200" height="300" fill="#294333"/><text x="30" y="150" fill="#f4f1e9" font-family="Georgia" font-size="28">Piranesi</text></svg>',
    }),
  );

  await page.goto(fixture.url);
  await page.getByRole("heading", { name: "A good story awaits." }).waitFor();
  await page.locator(".book-card").nth(47).waitFor();
  assert.equal(await page.locator(".book-card").count(), 48);
  await page.getByRole("button", { name: "More from your shelves" }).click();
  await page.locator(".book-card").nth(51).waitFor();
  const documentId = await page.evaluate(() => performance.timeOrigin);
  await page.getByRole("searchbox").fill("Piranesi");
  await page.getByRole("link", { name: /Piranesi Susanna Clarke/ }).click();
  await page.getByRole("heading", { name: "Piranesi", exact: true }).waitFor();
  assert.equal(
    await page.evaluate(() => performance.timeOrigin),
    documentId,
    "Book link must stay inside the SPA",
  );
  await page
    .getByLabel("Audio edition")
    .selectOption(String(fixture.manifestationIds[1]));
  await page
    .getByRole("button", { name: "Alternate narration: The House" })
    .waitFor();
  await page.reload();
  await page
    .getByRole("button", { name: "Alternate narration: The House" })
    .waitFor();
  const detail = await client.library.get.query({ bookId: fixture.bookId });
  const selectedAudio = detail.book.playback.audioOptions.find(
    (audio) => audio.manifestationId === fixture.manifestationIds[1],
  )!;
  assert.equal(
    await page.locator("audio").getAttribute("src"),
    selectedAudio.streamUrl,
  );
  assert.equal(
    await page
      .getByRole("link", { name: "Download audio", exact: true })
      .getAttribute("href"),
    selectedAudio.streamUrl,
  );
  const stream = await context.request.get(selectedAudio.streamUrl, {
    headers: { range: "bytes=0-127" },
  });
  assert.equal(
    stream.status(),
    206,
    "Actual audio range streaming must remain available",
  );
  assert.equal((await stream.body()).length, 128);
  await page
    .locator("audio")
    .evaluate((node) => (node as unknown as { play(): Promise<void> }).play());
  assert.ok(
    await page
      .locator("audio")
      .evaluate(
        (node) => (node as unknown as { readyState: number }).readyState >= 2,
      ),
  );
  await page
    .locator("audio")
    .evaluate((node) => (node as unknown as { pause(): void }).pause());
  const editions = await client.book.editions.query({ bookId: fixture.bookId });
  const pdf = editions.ebooks.find(
    (ebook) => ebook.mimeType === "application/pdf",
  )!;
  await page.getByLabel("Ebook edition").selectOption(String(pdf.assetId));
  assert.equal(
    await page
      .getByRole("link", { name: "Download PDF", exact: true })
      .getAttribute("href"),
    pdf.downloadUrl,
  );
  const downloadEvent = page.waitForEvent("download");
  await page.getByRole("link", { name: "Download PDF", exact: true }).click();
  const download = await downloadEvent;
  assert.equal(download.suggestedFilename(), "Piranesi.pdf");
  assert.match(await readFile((await download.path())!, "utf8"), /^%PDF/);

  await page.screenshot({
    path: path.join(artifacts, "book-desktop.png"),
    fullPage: true,
    animations: "disabled",
  });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(
    await page.evaluate("document.documentElement.scrollWidth > innerWidth"),
    false,
    "Book detail must not overflow on mobile",
  );
  await page.screenshot({
    path: path.join(artifacts, "book-mobile.png"),
    fullPage: true,
    animations: "disabled",
  });
  await page.setViewportSize({ width: 1440, height: 1000 });

  await page
    .getByRole("button", { name: "Report wrong audio", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Confirm report", exact: true })
    .click();
  await page.getByRole("status").filter({ hasText: "Review queued" }).waitFor();
  await page
    .getByRole("button", { name: "Report wrong ebook", exact: true })
    .click();
  const reportResponse = page.waitForResponse(
    (response) =>
      response.url().includes("library.reportImportIssue") &&
      response.request().method() === "POST",
  );
  await page
    .getByRole("button", { name: "Confirm report", exact: true })
    .click();
  await reportResponse;
  assert.equal(
    (await client.library.get.query({ bookId: fixture.bookId })).assets.length,
    4,
    "Reporting must preserve current media",
  );
  await page
    .getByRole("button", { name: "Force regeneration", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Generation in progress", exact: true })
    .waitFor();
  assert.equal(
    (
      await client.library.transcriptStatus.query({
        bookId: fixture.bookId,
        manifestationId: fixture.manifestationIds[1],
      })
    ).status,
    "pending",
  );
  await page
    .getByLabel("Audio edition")
    .selectOption(String(fixture.manifestationIds[0]));
  await page
    .getByRole("button", {
      name: "Generate chapters & transcript",
      exact: true,
    })
    .click();
  await page
    .getByRole("button", { name: "Generation in progress", exact: true })
    .waitFor();
  await page
    .getByRole("button", { name: "Choose artwork", exact: true })
    .click();
  await page.getByRole("button", { name: /Use cover 12345/ }).click();
  await page
    .getByRole("status")
    .filter({ hasText: "Cover updated." })
    .waitFor();
  const cover = (await client.library.get.query({ bookId: fixture.bookId }))
    .book.coverUrl;
  assert.ok(cover);
  await page.reload();
  assert.equal(
    await page
      .getByRole("img", { name: "Cover of Piranesi" })
      .getAttribute("src"),
    cover,
  );
  await page
    .getByRole("button", { name: "Choose artwork", exact: true })
    .click();
  await page.getByRole("button", { name: /Use cover 12346/ }).click();
  await page
    .getByRole("status")
    .filter({ hasText: "Cover updated." })
    .waitFor();
  const replacementCover = (
    await client.library.get.query({ bookId: fixture.bookId })
  ).book.coverUrl;
  assert.notEqual(
    replacementCover,
    cover,
    "Replacing an existing cover needs a fresh server-owned URL",
  );
  assert.equal(
    await page
      .getByRole("img", { name: "Cover of Piranesi" })
      .getAttribute("src"),
    replacementCover,
  );

  await page.getByText("Find a specific edition", { exact: true }).click();
  await page
    .getByRole("button", { name: "Search releases", exact: true })
    .click();
  await page
    .getByRole("checkbox", { name: /Piranesi - Fixture audiobook MP3/ })
    .check();
  await page
    .getByLabel("Edition label", { exact: true })
    .fill("Browser selected edition");
  await page
    .getByRole("button", {
      name: "Acquire selected as new edition",
      exact: true,
    })
    .click();
  await page
    .getByRole("status")
    .filter({ hasText: "New edition queued" })
    .waitFor();
  await page.getByText("Import local files", { exact: true }).click();
  await page
    .getByLabel("Server path", { exact: true })
    .fill(fixture.importPath);
  await page
    .getByRole("button", { name: "Inspect files", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Import selected files", exact: true })
    .click();
  await page
    .getByRole("status")
    .filter({ hasText: "Imported 1 file" })
    .waitFor();

  await page.getByRole("link", { name: "Discover", exact: true }).click();
  await page
    .getByRole("searchbox", { name: "Search the book catalog" })
    .fill("provider-error");
  await page.getByRole("button", { name: "Search books", exact: true }).click();
  await page.getByRole("alert").waitFor();
  assert.equal(
    await page.getByText("No books found.", { exact: true }).count(),
    0,
    "A failed catalog search must not look empty",
  );
  await page
    .getByRole("searchbox", { name: "Search the book catalog" })
    .fill("Susanna Clarke");
  await page.getByRole("button", { name: "Search books", exact: true }).click();
  await page
    .getByRole("heading", {
      name: "Jonathan Strange & Mr Norrell",
      exact: true,
    })
    .waitFor();
  await page.screenshot({
    path: path.join(artifacts, "catalog-desktop.png"),
    fullPage: true,
    animations: "disabled",
  });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(
    await page.evaluate("document.documentElement.scrollWidth > innerWidth"),
    false,
  );
  await page.screenshot({
    path: path.join(artifacts, "catalog-mobile.png"),
    fullPage: true,
    animations: "disabled",
  });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page
    .locator(".catalog-result")
    .filter({
      has: page.getByRole("heading", {
        name: "Jonathan Strange & Mr Norrell",
        exact: true,
      }),
    })
    .getByRole("button", { name: "Add to library" })
    .click();
  await page.waitForURL(/\/book\/\d+$/);
  await page
    .getByRole("heading", {
      name: "Jonathan Strange & Mr Norrell",
      exact: true,
    })
    .waitFor();
  assert.match(page.url(), /\/book\/\d+$/);
  await page.getByText("Manage this book", { exact: true }).click();
  await page
    .getByRole("button", { name: "Remove from library", exact: true })
    .click();
  await page.getByRole("button", { name: "Keep book", exact: true }).click();
  await page
    .getByRole("button", { name: "Remove from library", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Permanently remove book", exact: true })
    .click();
  await page.waitForURL(/\/library$/);
  await page.goto(new URL(`/book/${fixture.bookId}`, fixture.url).href);
  await page.getByRole("link", { name: "Susanna Clarke", exact: true }).click();
  await page
    .getByRole("heading", { name: "Susanna Clarke", exact: true })
    .waitFor();
  await page.getByRole("heading", { name: "Piranesi", exact: true }).waitFor();
  await page.goto(new URL(`/book/${fixture.bookId}`, fixture.url).href);
  await page
    .getByRole("link", { name: "The House · Book 1", exact: true })
    .click();
  await page.getByRole("heading", { name: "The House", exact: true }).waitFor();
  await page.getByRole("heading", { name: "Piranesi", exact: true }).waitFor();

  await page.goto(new URL(`/book/${fixture.failedBookId}`, fixture.url).href);
  await page
    .getByRole("heading", { name: "Waiting for the provider.", exact: true })
    .waitFor();
  assert.equal(
    (await page.locator("body").innerText()).includes("NEVER-EXPOSE"),
    false,
  );
  await page
    .getByRole("button", { name: "Retry acquisition", exact: true })
    .click();
  await page
    .getByRole("status")
    .filter({ hasText: "Acquisition queued" })
    .waitFor();
  await page
    .getByRole("button", { name: "Acquisition in progress", exact: true })
    .waitFor();
  assert.equal(
    await page
      .getByRole("button", { name: "Acquisition in progress", exact: true })
      .isDisabled(),
    true,
  );

  await context.clearCookies();
  await context.addCookies([
    {
      name: "podible_session",
      value: "book-spa-reader",
      url: fixture.url,
      httpOnly: true,
    },
  ]);
  await page.goto(new URL(`/book/${fixture.bookId}`, fixture.url).href);
  await page.getByRole("heading", { name: "Piranesi", exact: true }).waitFor();
  assert.equal(
    await page.getByText("Find a specific edition", { exact: true }).count(),
    0,
  );
  await context.clearCookies();
  await page.reload();
  await page.getByRole("link", { name: "Sign in", exact: true }).waitFor();
  assert.deepEqual(errors, [], "No uncaught browser errors");
  const expectedOutages = badResponses.filter(
    (response) =>
      response.includes("/openlibrary.search?") &&
      response.includes("provider-error"),
  );
  assert.equal(
    expectedOutages.length,
    1,
    "The deliberate catalog outage must be observed in the actual browser network",
  );
  assert.deepEqual(
    badResponses.filter((response) => !expectedOutages.includes(response)),
    [],
    "No unexpected server failures during book workflows",
  );
  console.log(
    "Book SPA Chrome acceptance passed: pagination, client navigation, deep edition links, real range/audio playback, PDF download, per-edition generation/reporting, cover persistence, manual release acquisition/import, catalog add, series, provider failure/retry, mobile, and session/admin boundaries.",
  );
} catch (error) {
  const page = browser.contexts()[0]?.pages()[0];
  if (page) {
    console.error(
      `Book browser failure at ${page.url()}:\n${await page.locator("body").innerText()}`,
    );
    await page
      .screenshot({ path: path.join(artifacts, "failure.png"), fullPage: true })
      .catch(() => {});
  }
  throw error;
} finally {
  await browser.close();
  server.kill();
  await server.exited;
}
