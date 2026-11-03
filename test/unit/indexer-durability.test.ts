import { describe, expect, test } from "bun:test";
import { LeaseExpiredError, RunCancelledError } from "reflow-ts";
import { IndexerSearchError, parseIndexerRetryAfter } from "../../src/library/indexer-errors";
import { parseTorznabSearch, TorznabSearchClient } from "../../src/library/torznab";
import { runSearch, runSearchBatch } from "../../src/library/service";
import { defaultSettings } from "../../src/settings";
import { selectWithIndexerRecovery } from "../../src/worker/indexer-search";
import type { WorkerContext } from "../../src/worker/context";

const EMPTY = "<rss><channel></channel></rss>";
const RSS = `<rss><channel><item><title>Dune [M4B EPUB]</title><enclosure url="https://downloads.example/dune.torrent" length="100" /></item></channel></rss>`;
const source = { name: "MAM", baseUrl: "https://indexer.example/1", apiKey: "private-api-key" };

describe("safe indexer failures and backoff", () => {
  test("parses Retry-After seconds and HTTP dates without shortening long waits", () => {
    const now = Date.parse("2026-11-02T12:00:00Z");
    expect(parseIndexerRetryAfter("120", now)).toBe(120_000);
    expect(parseIndexerRetryAfter("Mon, 02 Nov 2026 12:10:00 GMT", now)).toBe(600_000);
    expect(parseIndexerRetryAfter("86400", now)).toBe(86_400_000);
    expect(parseIndexerRetryAfter("Mon, 02 Nov 2026 11:00:00 GMT", now)).toBe(0);
    expect(parseIndexerRetryAfter("garbage", now)).toBeNull();
    expect(parseIndexerRetryAfter(null, now)).toBeNull();
  });

  for (const header of ["300", "Mon, 02 Nov 2026 12:05:00 GMT"]) {
    test(`honors HTTP Retry-After ${header} across queries and media`, async () => {
      let now = Date.parse("2026-11-02T12:00:00Z");
      let calls = 0;
      const client = new TorznabSearchClient(async () => {
        calls++;
        return new Response("upstream mam_id expired; private-api-key", { status: 429, headers: { "Retry-After": header } });
      }, () => now);
      const first = await client.search([source], "Dune", "audio");
      expect(first.failures[0]).toMatchObject({ kind: "unavailable", status: 429, retryAt: now + 300_000 });
      expect(first.failures[0]?.message).toContain("not rate limiting");
      expect(JSON.stringify(first)).not.toContain("private-api-key");
      expect(JSON.stringify(first)).not.toContain("indexer.example");
      expect(JSON.stringify(first)).not.toContain("mam_id");
      await client.search([source], "Another book", "ebook");
      now += 299_999;
      await client.search([source], "Yet another book", "audio");
      expect(calls).toBe(1);
      now++;
      await client.search([source], "Dune", "audio");
      expect(calls).toBe(2);
    });
  }

  test("repeated 429s increase conservative provider backoff, then recover", async () => {
    let now = 1_000_000;
    let calls = 0;
    const client = new TorznabSearchClient(async () => ++calls < 3 ? new Response("", { status: 429 }) : new Response(RSS), () => now);
    expect((await client.search([source], "Dune", "audio")).failures[0]?.retryAt).toBe(now + 60_000);
    now += 60_000;
    expect((await client.search([source], "Dune", "audio")).failures[0]?.retryAt).toBe(now + 120_000);
    now += 60_000;
    await client.search([source], "Other query", "ebook");
    expect(calls).toBe(2);
    now += 60_000;
    expect((await client.search([source], "Dune", "audio")).results).toHaveLength(1);
    expect(calls).toBe(3);
  });

  for (const status of [401, 403]) {
    test(`HTTP ${status} is configuration failure, not no-results or a guessed cooldown`, async () => {
      const client = new TorznabSearchClient(async () => new Response("private upstream details", { status }));
      const result = await client.search([source], "Dune", "audio");
      expect(result.failures[0]).toMatchObject({ kind: "configuration", status, retryAt: null });
      expect(result.failures[0]?.message).toContain("credentials");
      expect(result.failures[0]?.message).not.toContain("private upstream details");
    });
  }

  test("Torznab XML authentication errors and malformed successes cannot become no-results", async () => {
    const auth = new TorznabSearchClient(async () => new Response('<error code="100" description="https://secret.example/?apikey=secret" />'));
    expect((await auth.search([source], "Dune", "audio")).failures[0]).toMatchObject({ kind: "configuration", status: 401 });
    const html = new TorznabSearchClient(async () => new Response("<html>upstream offline</html>"));
    expect((await html.search([source], "Dune", "audio")).failures[0]?.kind).toBe("unavailable");
    const empty = new TorznabSearchClient(async () => new Response(EMPTY));
    expect(await empty.search([source], "Dune", "audio")).toEqual({ results: [], failures: [] });
  });

  test("fetch errors are sanitized, but Reflow ownership errors are not swallowed", async () => {
    const broken = new TorznabSearchClient(async (url) => { throw new Error(`fetch failed ${url.href}`); });
    const batch = await broken.search([source], "Dune", "audio");
    expect(batch.failures[0]?.kind).toBe("unavailable");
    expect(JSON.stringify(batch)).not.toContain("private-api-key");
    expect(JSON.stringify(batch)).not.toContain("indexer.example");
    for (const error of [new LeaseExpiredError("fixture"), new RunCancelledError("fixture")]) {
      const controlled = new TorznabSearchClient(async () => { throw error; });
      await expect(controlled.search([source], "Dune", "audio")).rejects.toBe(error);
    }
  });
});

describe("bounded coalescing and partial providers", () => {
  test("coalesces category-less URLs, re-tags each media, and expires the success cache", async () => {
    let calls = 0;
    let now = 1_000_000;
    const client = new TorznabSearchClient(async () => { calls++; await Bun.sleep(5); return new Response(RSS); }, () => now);
    const [audio, ebook] = await Promise.all([client.search([source], "Dune", "audio"), client.search([source], "Dune", "ebook")]);
    expect(calls).toBe(1);
    expect(audio.results[0]?.mediaType).toBe("audio");
    expect(ebook.results[0]?.mediaType).toBe("ebook");
    await client.search([source], "Dune", "ebook");
    expect(calls).toBe(1);
    now += 30_001;
    await client.search([source], "Dune", "ebook");
    expect(calls).toBe(2);
  });

  test("different categories and credentials do not share responses", async () => {
    const urls: URL[] = [];
    const client = new TorznabSearchClient(async (url) => { urls.push(url); return new Response(RSS); });
    const scoped = { ...source, categories: { audio: "3030", ebook: "7020" } };
    await Promise.all([client.search([scoped], "Dune", "audio"), client.search([scoped], "Dune", "ebook")]);
    expect(urls.map((url) => url.searchParams.get("cat")).sort()).toEqual(["3030", "7020"]);
    await client.search([{ ...scoped, apiKey: "changed" }], "Dune", "audio");
    expect(urls).toHaveLength(3);
  });

  test("a slower successful category cannot clear a newer provider cooldown", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    let calls = 0;
    const client = new TorznabSearchClient(async (url) => {
      calls++;
      if (url.searchParams.get("cat") === "3030") { await gate; return new Response(RSS); }
      return new Response("", { status: 429 });
    });
    const scoped = { ...source, categories: { audio: "3030", ebook: "7020" } };
    const audio = client.search([scoped], "Dune", "audio");
    try {
      await client.search([scoped], "Dune", "ebook");
      release();
      expect((await audio).results).toHaveLength(1);
      expect((await client.search([scoped], "Another book", "audio")).failures[0]?.status).toBe(429);
      expect(calls).toBe(2);
    } finally { release(); }
  });

  test("a healthy source progresses while blocked applicable sources remain planned", async () => {
    let now = 1_000_000;
    const urls: string[] = [];
    const client = new TorznabSearchClient(async (url) => { urls.push(url.href); return new Response(RSS); }, () => now);
    const sources = [source, { name: "Healthy", baseUrl: "https://healthy.example" }];
    let batch = await client.search([{ ...source, availableAfter: now + 60_000 }, sources[1]!], "Dune", "audio");
    expect(batch.results[0]?.provider).toBe("Healthy");
    expect(batch.failures[0]).toMatchObject({ provider: "MAM", kind: "unavailable", retryAt: now + 60_000 });
    expect(urls).toHaveLength(1);
    now += 60_000;
    batch = await client.search(sources, "Dune", "audio");
    expect(batch.failures).toHaveLength(0);
    expect(batch.results).toHaveLength(2);
  });

  test("bounds success cache and unique in-flight requests", async () => {
    let calls = 0;
    const client = new TorznabSearchClient(async () => { calls++; return new Response(EMPTY); });
    for (let i = 0; i < 130; i++) await client.search([source], `Book ${i}`, "audio");
    await client.search([source], "Book 0", "audio");
    expect(calls).toBe(131);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    calls = 0;
    const busy = new TorznabSearchClient(async () => { calls++; await gate; return new Response(EMPTY); });
    try {
      const searches = Array.from({ length: 130 }, (_, i) => busy.search([source], `Book ${i}`, "audio"));
      const extra = await busy.search([source], "Extra", "ebook");
      expect(calls).toBe(128);
      expect(extra.failures[0]?.kind).toBe("unavailable");
      release();
      expect((await Promise.all(searches)).filter((batch) => batch.failures.length)).toHaveLength(2);
    } finally {
      release();
    }
  });
});

describe("book discovery and durable search phases", () => {
  test("discovers applicable book indexers and coalesces audio/ebook discovery", async () => {
    const routes: string[] = [];
    const server = Bun.serve({ port: 0, fetch(request) {
      const url = new URL(request.url);
      routes.push(url.pathname + url.searchParams.get("cat"));
      if (url.pathname === "/api/v1/indexer") {
        expect(request.headers.get("X-Api-Key")).toBe("discovery-key");
        return Response.json([
          { id: 1, name: "Books", enable: true, protocol: "torrent", capabilities: { categories: [{ id: 3030 }, { id: 7020 }] } },
          { id: 2, name: "Movies", enable: true, protocol: "torrent", capabilities: { categories: [{ id: 2000 }] } },
        ]);
      }
      if (url.pathname === "/api/v1/indexerstatus") return Response.json([]);
      if (url.pathname === "/1/api") return new Response(RSS);
      return new Response("Unexpected endpoint", { status: 500 });
    } });
    try {
      const settings = defaultSettings({ prowlarr: { url: server.url.origin, apiKey: "discovery-key" }, torznab: [{ name: "Legacy", baseUrl: "http://unused.example" }] });
      const [audio, ebook] = await Promise.all([runSearch(settings, { query: "Dune", media: "audio" }), runSearch(settings, { query: "Dune", media: "ebook" })]);
      expect(Array.isArray(audio)).toBe(true);
      expect(audio[0]?.provider).toBe("Books");
      expect(ebook[0]?.provider).toBe("Books");
      expect(routes.filter((route) => route.startsWith("/api/v1/indexerstatus"))).toHaveLength(1);
      expect(routes.filter((route) => route === "/api/v1/indexernull")).toHaveLength(1);
      expect(routes).toContain("/1/api3030");
      expect(routes).toContain("/1/api7020");
      expect(routes.some((route) => route.startsWith("/2"))).toBe(false);
    } finally { await server.stop(true); }
  });

  test("legacy fallback and public array API distinguish an outage from zero hits", async () => {
    const server = Bun.serve({ port: 0, fetch() { return new Response("blocked", { status: 429 }); } });
    try {
      const settings = defaultSettings({ torznab: [{ name: "MAM", baseUrl: server.url.origin }] });
      const batch = await runSearchBatch(settings, { query: "Dune", media: "audio" });
      expect(batch.results).toHaveLength(0);
      expect(batch.failures[0]?.status).toBe(429);
      await expect(runSearch(settings, { query: "Dune", media: "audio" })).rejects.toBeInstanceOf(IndexerSearchError);
    } finally { await server.stop(true); }
  });

  test("dynamic discovery retains temporarily blocked applicable indexers alongside healthy providers", async () => {
    const calls: string[] = [];
    const blockedUntil = Date.now() + 300_000;
    const server = Bun.serve({ port: 0, fetch(request) {
      const url = new URL(request.url);
      calls.push(url.pathname);
      if (url.pathname === "/api/v1/indexer") return Response.json([1, 2].map((id) => ({ id, name: id === 1 ? "Blocked" : "Healthy", enable: true, protocol: "torrent", capabilities: { categories: [{ id: 3030 }] } })));
      if (url.pathname === "/api/v1/indexerstatus") return Response.json([{ indexerId: 1, disabledTill: new Date(blockedUntil).toISOString() }]);
      return new Response(RSS);
    } });
    try {
      const settings = defaultSettings({ prowlarr: { url: server.url.origin, apiKey: "discovery-key" } });
      const batch = await runSearchBatch(settings, { query: "Dune", media: "audio" });
      expect(batch.results[0]?.provider).toBe("Healthy");
      expect(batch.failures[0]).toMatchObject({ provider: "Blocked", kind: "unavailable", retryAt: blockedUntil });
      expect(batch.failures[0]?.message).toContain("Prowlarr reports a cooldown");
      expect(batch.failures[0]?.message).toContain("does not identify the upstream cause");
      expect(calls).not.toContain("/1/api");
      expect(calls).toContain("/2/api");
    } finally { await server.stop(true); }
  });

  test("discovery authentication failures stay configuration errors and never silently use legacy sources", async () => {
    let calls = 0;
    const server = Bun.serve({ port: 0, fetch() { calls++; return new Response("secret", { status: 401 }); } });
    try {
      const settings = defaultSettings({ prowlarr: { url: server.url.origin, apiKey: "private-key" }, torznab: [{ name: "Legacy", baseUrl: "https://unused.example" }] });
      const batch = await runSearchBatch(settings, { query: "Dune", media: "audio" });
      expect(batch.results).toHaveLength(0);
      expect(batch.failures[0]).toMatchObject({ kind: "configuration", status: 401 });
      await runSearchBatch(settings, { query: "Other book", media: "ebook" });
      expect(calls).toBe(1);
      expect(JSON.stringify(batch)).not.toContain("private-key");
      expect(JSON.stringify(batch)).not.toContain(server.url.origin);
    } finally { await server.stop(true); }
  });

  test("legacy array checkpoint replays without fetching or changing the selection name", async () => {
    const steps: string[] = [];
    const ctx = { getSettings() { throw new Error("must replay"); }, onLog() {}, execution: {
      key: "fixture", assertActive() {}, async sleep() { throw new Error("must not wait"); },
      async run<T>(name: string, handler: () => T | Promise<T>): Promise<T> {
        steps.push(name);
        return name === "audio:search" ? [] as T : handler();
      },
    } } as unknown as WorkerContext;
    const decision = await selectWithIndexerRecovery(ctx, { query: "Dune", media: "audio", jobId: 1, bookId: 1 }, async (_results, name) => {
      expect(name).toBe("audio:selection");
      return { selections: [] };
    });
    expect(decision.selections).toHaveLength(0);
    expect(steps).toEqual(["audio:search"]);
  });

  test("deferred checkpoints wait, use fresh retry phases, and allow completed replay", async () => {
    const checkpoints = new Map<string, unknown>([["audio:search", { kind: "indexer-search-deferred", results: [], failures: [{ provider: "MAM", kind: "unavailable", status: 429, retryAt: Date.now() + 60_000, message: "MAM unavailable" }] }], ["audio:search:retry:1", []]]);
    const waits: string[] = [];
    const names: string[] = [];
    let selections = 0;
    const ctx = { getSettings() { throw new Error("must replay"); }, onLog() {}, execution: {
      key: "fixture", assertActive() {}, async sleep(name: string, ms: number) { expect(ms).toBeGreaterThan(59_000); waits.push(name); },
      async run<T>(name: string, handler: () => T | Promise<T>): Promise<T> {
        names.push(name);
        if (!checkpoints.has(name)) checkpoints.set(name, await handler());
        return checkpoints.get(name) as T;
      },
    } } as unknown as WorkerContext;
    const request = { query: "Dune", media: "audio" as const, jobId: 1, bookId: 1 };
    const select = async (_results: unknown, name: string) => ctx.execution!.run(name, () => { selections++; return { selections: [] }; });
    await selectWithIndexerRecovery(ctx, request, select);
    await selectWithIndexerRecovery(ctx, request, select);
    expect(waits).toEqual(["audio:search:wait:0", "audio:search:wait:0"]);
    expect(names).toContain("audio:search:retry:1");
    expect(names).toContain("audio:selection:retry:1");
    expect(selections).toBe(1);
  });

  test("rejected healthy results do not permanently finalize a partial provider outage", async () => {
    const healthy = parseTorznabSearch(RSS, "Healthy", "audio");
    const recovered = parseTorznabSearch(RSS, "Recovered", "audio");
    const checkpoints = new Map<string, unknown>([
      ["audio:search", { kind: "indexer-search-deferred", results: healthy, failures: [{ provider: "Missing", kind: "unavailable", status: 429, retryAt: Date.now() + 60_000, message: "Missing unavailable" }] }],
      ["audio:search:retry:1", recovered],
    ]);
    const selections: string[] = [];
    const waits: string[] = [];
    const ctx = { onLog() {}, execution: {
      key: "fixture", assertActive() {}, async sleep(name: string) { waits.push(name); },
      async run<T>(name: string, handler: () => T | Promise<T>): Promise<T> {
        if (!checkpoints.has(name)) checkpoints.set(name, await handler());
        return checkpoints.get(name) as T;
      },
    } } as unknown as WorkerContext;
    const decision = await selectWithIndexerRecovery(ctx, { query: "Dune", media: "audio", jobId: 1, bookId: 1 }, async (results, name) => ctx.execution!.run(name, () => {
      selections.push(name);
      return { selections: results[0]?.provider === "Healthy" ? [] : [results[0]!] };
    }));
    expect(decision.selections[0]?.provider).toBe("Recovered");
    expect(selections).toEqual(["audio:selection", "audio:selection:retry:1"]);
    expect(waits).toEqual(["audio:search:wait:0"]);
  });

  test("configuration failure and cancellation escape without no-result selection or generic attempts", async () => {
    const server = Bun.serve({ port: 0, fetch() { return new Response("private", { status: 403 }); } });
    try {
      for (const kind of ["configuration", "unavailable"] as const) {
        const cancelled = new RunCancelledError("fixture");
        const ctx = { onLog() {}, getSettings: () => defaultSettings({ torznab: [{ name: "MAM", baseUrl: server.url.origin }] }), execution: {
          key: "fixture", assertActive() {}, async sleep() { throw cancelled; },
          async run<T>(_name: string, handler: () => T | Promise<T>) {
            if (kind === "configuration") return handler();
            return { kind: "indexer-search-deferred", results: [], failures: [{ provider: "MAM", kind, status: 429, retryAt: Date.now() + 60_000, message: "Safe failure" }] } as T;
          },
        } } as unknown as WorkerContext;
        const pending = selectWithIndexerRecovery(ctx, { query: "Dune", media: "audio", jobId: 1, bookId: 1 }, async () => { throw new Error("must not select no-results"); });
        if (kind === "configuration") await expect(pending).rejects.toBeInstanceOf(IndexerSearchError);
        else await expect(pending).rejects.toBe(cancelled);
      }
    } finally { await server.stop(true); }
  });
});
