import assert from "node:assert/strict";
import { createTRPCClient, httpLink } from "@trpc/client";
import { chromium, type Page } from "playwright";
import type { AppRouter } from "../../src/trpc";
import { fixtureMovie } from "../fixtures/movie-services";

const fixture = Bun.spawn([process.execPath, "run", "test/fixtures/spa-server.ts"], {
  stdout: "pipe", stderr: "inherit", env: { ...process.env, PORT: "0", TV_FIXTURE: "1", TV_SEARCH_FIXTURE: "",
    SHADOW_FIXTURE: "", ADMIN_FIXTURE: "", AUTH_FIXTURE: "", FIXTURE_TLS_KEY: "", FIXTURE_TLS_CERT: "" },
});
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined;
try {
  const reader = fixture.stdout.getReader();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let output = "", url: string | undefined;
  try {
    const timeout = new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("SPA fixture startup timed out")), 15_000); });
    while (!url) {
      const item = await Promise.race([reader.read(), timeout]);
      output += new TextDecoder().decode(item.value);
      url = /SPA_FIXTURE_URL=(\S+)/.exec(output)?.[1];
      if (item.done && !url) throw new Error(`SPA fixture exited: ${output}`);
    }
  } finally { clearTimeout(timer); reader.releaseLock(); }
  const origin = new URL(url).origin;
  const api = (session: string) => createTRPCClient<AppRouter>({ links: [httpLink({ url: `${origin}/api/trpc`,
    headers: { cookie: `podible_session=${session}`, origin } })] });
  const movieSession = "spa-browser-fixture", tvSession = "tv-reader-fixture";
  await api(movieSession).movies.request.mutate({ tmdbId: fixtureMovie.tmdbId });
  const { requests } = await api(tvSession).tv.myRequests.query({});
  const { entryId } = requests.find(row => row.entry.title === "Fixture Sequel")!;

  browser = await chromium.launch({ ...(process.env.CI ? {} : { channel: "chrome" }), headless: true });
  const errors: string[] = [];
  const openAs = async (session: string, width: number) => {
    const context = await browser!.newContext({ viewport: { width, height: 860 } });
    context.setDefaultTimeout(10_000);
    await context.route("**/*", route => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
    await context.addCookies([{ name: "podible_session", value: session, url: origin, httpOnly: true }]);
    const page = await context.newPage();
    page.on("pageerror", error => errors.push(error.message));
    page.on("response", response => {
      if (response.url().includes("/api/trpc/issues.") && !response.ok()) errors.push(`${response.status()} ${response.url()}`);
    });
    return page;
  };
  const openPanel = (page: Page) => page.getByRole("button", { name: "Report a problem", exact: true }).click();
  const file = async (page: Page, description: string) => {
    await page.getByRole("textbox", { name: "Description", exact: true }).fill(description);
    await page.getByRole("button", { name: "Send report", exact: true }).click();
    await page.getByRole("status").filter({ hasText: /Report #\d+ saved. No repair was attempted./ }).waitFor();
    await page.getByText(description, { exact: true }).waitFor();
  };
  const movie = await openAs(movieSession, 1280);
  await movie.goto(`${origin}/movies/${fixtureMovie.tmdbId}`);
  await openPanel(movie);
  assert.equal(await movie.getByLabel("Episode (optional)").count(), 0);
  await file(movie, "Movie playback stops at 00:42.");
  await movie.reload();
  await openPanel(movie);
  await movie.getByText("Movie playback stops at 00:42.", { exact: true }).waitFor();

  const tv = await openAs(tvSession, 390);
  await tv.goto(`${origin}/tv/library/${entryId}`);
  await openPanel(tv);
  await tv.getByLabel("Episode (optional)").selectOption({ label: "1. Fixture episode 1" });
  await tv.getByLabel("Issue type").selectOption("audio-subtitles");
  await file(tv, "<script>this is plain text</script>");
  await tv.getByText("Audio or subtitles: Fixture episode 1", { exact: false }).waitFor();
  assert.equal(await tv.locator(".screen-issue-description script").count(), 0);
  assert.equal(await tv.evaluate("document.documentElement.scrollWidth > innerWidth"), false);

  await movie.goto(`${origin}/tv/library/${entryId}`);
  await openPanel(movie);
  await movie.getByText("No reports filed.", { exact: true }).waitFor();
  assert.deepEqual(errors, []);
  console.log("Screen issues: movie reload, exact TV episode, safe text, mobile layout, and account isolation passed in Chrome.");
} finally {
  await browser?.close();
  fixture.kill();
  await fixture.exited;
}
