import { chromium } from "playwright";
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
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto(new URL("/movie-shadow", url).href);
  await page.getByRole("heading", { name: "Movie shadow audit" }).waitFor();
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
  await page.setViewportSize({ width: 390, height: 844 });
  if (
    await page.evaluate(
      "document.documentElement.scrollWidth > window.innerWidth",
    )
  )
    throw new Error("Shadow page overflows mobile viewport");
  if (errors.length) throw new Error(errors.join("\n"));
  console.log(
    "Shadow browser: admin evidence, persistent review, sync, mobile passed",
  );
} finally {
  await browser.close();
  server.kill("SIGTERM");
  await server.exited;
}
