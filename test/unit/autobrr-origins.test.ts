import { expect, spyOn, test } from "bun:test";
import { parseAutobrrRelease, readAutobrrConfig, refreshAutobrrDownloadOrigins } from "../../src/media/autobrr";
import { ProwlarrDiscovery } from "../../src/media/prowlarr";

const prowlarr = { url: "https://prowlarr.invalid/base/", apiKey: "private-fixture" };
const config = () => readAutobrrConfig({
  AUTOBRR_URL: "http://autobrr.invalid",
  AUTOBRR_API_KEY: "autobrr-fixture",
  AUTOBRR_PODIBLE_URL: "http://podible.invalid",
  AUTOBRR_DOWNLOAD_ORIGINS: "http://prowlarr.invalid",
})!;
const indexer = (id = 6, name = "FileList", value: unknown = "https://filelist.io/", overrides = {}) => ({
  id, name, enable: true, protocol: "torrent", fields: [{ name: "baseUrl", value }], ...overrides,
});
const event = (name = "FileList", url = "https://filelist.io/download?id=42") => ({
  event: "release.new", data: {
    indexer: { name, identifier: "fl" },
    release: { name: "Fixture.S01E01", protocol: "torrent", type: "series", download_url: url },
  },
});

test("Prowlarr origin discovery shares the bounded configuration cache and rejects ambiguous or unsafe origins", async () => {
  const calls: string[] = [];
  const discovery = new ProwlarrDiscovery(prowlarr, (async (input, init) => {
    const url = new URL(String(input));
    calls.push(url.pathname);
    expect(new Headers(init?.headers).get("X-Api-Key")).toBe(prowlarr.apiKey);
    expect(init?.redirect).toBe("manual");
    return Response.json(url.pathname.endsWith("/indexerstatus") ? [] : [
      indexer(6, "FileList Custom", "https://filelist.io/path", { definitionName: "FileList" }),
      indexer(7, "Disabled", "https://disabled.invalid", { enable: false }),
      indexer(8, "Usenet", "https://usenet.invalid", { protocol: "usenet" }),
      ...["file:///etc/passwd", "https://user:pass@private.invalid", "https://private.invalid/#fragment", "https://private.invalid/?key=secret", ["https://private.invalid"]]
        .map((value, n) => indexer(10 + n, "Unsafe " + n, value)),
      indexer(20, "Ambiguous", "https://one.invalid"),
      indexer(21, "Ambiguous", "https://two.invalid"),
      indexer(21, "Ambiguous", "https://three.invalid"),
      indexer(22, "Suggestions Only", null, { indexerUrls: ["https://suggested.invalid"], baseUrl: "https://not-a-field.invalid" }),
      indexer(23, "Duplicate Field", "https://one.invalid", { fields: [{ name: "baseUrl", value: "https://one.invalid" }, { name: "baseUrl", value: "https://two.invalid" }] }),
      indexer(24, "Aliased", "https://one.invalid", { definitionName: "FileList Custom" }),
      indexer(25, "Same ID", "https://one.invalid"),
      indexer(25, "Same ID", "https://two.invalid"),
    ]);
  }) as typeof fetch);
  const [origins] = await Promise.all([discovery.downloadOrigins(), discovery.sources("tv")]);
  expect([...origins.keys()].sort()).toEqual(["aliased", "filelist"]);
  expect([...origins.get("filelist")!]).toEqual(["https://filelist.io"]);
  await discovery.downloadOrigins();
  expect(calls).toEqual(["/base/api/v1/indexer", "/base/api/v1/indexerstatus"]);
});

test("cached refreshes coalesce without extending the original trust deadline", async () => {
  let now = Date.now();
  const clock = spyOn(Date, "now").mockImplementation(() => now);
  const settings = config();
  let calls = 0;
  const fetcher = (async input => {
    calls++;
    return Response.json(String(input).endsWith("/indexerstatus") ? [] : [indexer()]);
  }) as typeof fetch;
  try {
    await Promise.all(Array.from({ length: 8 }, () => refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher)));
    expect(calls).toBe(2);
    now += 59_000;
    await refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher);
    expect(calls).toBe(2);
    now += 6 * 60_000 - 59_000;
    expect(parseAutobrrRelease(settings, event())).toBeNull();
  } finally {
    clock.mockRestore();
  }
});

test("auto origins expire, refresh membership, fail closed with cooldown, and preserve explicit origins", async () => {
  let now = Date.now();
  const clock = spyOn(Date, "now").mockImplementation(() => now);
  const settings = config();
  let enabled = true, unavailable = false, calls = 0;
  const fetcher = (async input => {
    calls++;
    if (unavailable) return new Response("private response must not escape", { status: 403 });
    return Response.json(String(input).endsWith("/indexerstatus") ? [] : [indexer(6, "FileList", "https://filelist.io", { enable: enabled })]);
  }) as typeof fetch;
  try {
    await refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher);
    expect(parseAutobrrRelease(settings, event())).not.toBeNull();
    now += 6 * 60_000 + 1;
    expect(parseAutobrrRelease(settings, event())).toBeNull();
    await refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher);
    expect(parseAutobrrRelease(settings, event())).not.toBeNull();
    enabled = false;
    now += 61_000;
    await refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher);
    expect(parseAutobrrRelease(settings, event())).toBeNull();
    enabled = true;
    now += 61_000;
    await refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher);
    expect(parseAutobrrRelease(settings, event())).not.toBeNull();
    unavailable = true;
    now += 61_000;
    await expect(refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher)).rejects.toThrow("Autobrr Prowlarr download origin discovery is unavailable");
    expect(parseAutobrrRelease(settings, event())).toBeNull();
    const beforeRetry = calls;
    await expect(refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher)).rejects.not.toThrow("private");
    expect(calls).toBe(beforeRetry);
    expect(parseAutobrrRelease(settings, event("Other", "http://prowlarr.invalid/1/download"))).not.toBeNull();
    unavailable = false;
    now += 61_000;
    await refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher);
    expect(parseAutobrrRelease(settings, event())).not.toBeNull();
    await refreshAutobrrDownloadOrigins(settings, { ...prowlarr, url: "" }, fetcher);
    expect(parseAutobrrRelease(settings, event())).toBeNull();
  } finally {
    clock.mockRestore();
  }
});

test("a settings rotation discards old auto trust before discovery and cannot be overwritten by old inflight work", async () => {
  const settings = config();
  let finishOld!: (response: Response) => void;
  const old = refreshAutobrrDownloadOrigins(settings, prowlarr, (async input =>
    String(input).endsWith("/indexerstatus") ? Response.json([]) : new Promise<Response>(resolve => { finishOld = resolve; })) as typeof fetch);
  await refreshAutobrrDownloadOrigins(settings, { ...prowlarr, apiKey: "rotated" }, (async input =>
    Response.json(String(input).endsWith("/indexerstatus") ? [] : [indexer(6, "FileList", "https://replacement.invalid")])) as typeof fetch);
  finishOld(Response.json([indexer()]));
  await old;
  expect(parseAutobrrRelease(settings, event())).toBeNull();
  expect(parseAutobrrRelease(settings, event("FileList", "https://replacement.invalid/download"))).not.toBeNull();
});

test("rotating or removing Prowlarr settings revokes old trust before pending discovery completes", async () => {
  const settings = config();
  await refreshAutobrrDownloadOrigins(settings, prowlarr, (async input =>
    Response.json(String(input).endsWith("/indexerstatus") ? [] : [indexer()])) as typeof fetch);
  expect(parseAutobrrRelease(settings, event())).not.toBeNull();
  let finish!: (response: Response) => void;
  const pending = refreshAutobrrDownloadOrigins(settings, { ...prowlarr, apiKey: "rotated" }, (async input =>
    String(input).endsWith("/indexerstatus") ? Response.json([]) : new Promise<Response>(resolve => { finish = resolve; })) as typeof fetch);
  expect(parseAutobrrRelease(settings, event())).toBeNull();
  await refreshAutobrrDownloadOrigins(settings, { ...prowlarr, apiKey: "" });
  finish(Response.json([indexer()]));
  await pending;
  expect(parseAutobrrRelease(settings, event())).toBeNull();
});

test("concurrent discovery failures share one request and a bounded retry cooldown", async () => {
  const settings = config();
  let calls = 0;
  const fetcher = (async (_input: Parameters<typeof fetch>[0]): Promise<Response> => {
    calls++;
    throw new Error("private upstream URL and API key");
  }) as typeof fetch;
  const results = await Promise.allSettled(Array.from({ length: 8 }, () => refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher)));
  expect(calls).toBe(1);
  for (const result of results) {
    expect(result.status).toBe("rejected");
    if (result.status === "rejected") expect(result.reason.message).toBe("Autobrr Prowlarr download origin discovery is unavailable");
  }
  await expect(refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher)).rejects.toThrow("Autobrr Prowlarr download origin discovery is unavailable");
  expect(calls).toBe(1);
  expect(parseAutobrrRelease(settings, event())).toBeNull();
});

test("Prowlarr redirects, malformed configuration and oversized responses never grant download trust", async () => {
  for (const [endpoint, response] of [
    ["indexer", () => Response.redirect("https://unrelated.invalid/private")],
    ["indexerstatus", () => Response.redirect("https://unrelated.invalid/private")],
    ["indexerstatus", () => new Response("private failure", { status: 403 })],
    ["indexer", () => new Response(" ".repeat(8 * 1024 * 1024 + 1))],
    ["indexerstatus", () => new Response(" ".repeat(8 * 1024 * 1024 + 1))],
    ["indexer", () => new Response("private invalid JSON")],
    ["indexer", () => Response.json(Array.from({ length: 1025 }, (_, n) => indexer(n + 1)))],
    ["indexerstatus", () => Response.json(Array.from({ length: 1025 }, (_, n) => ({ indexerId: n + 1 })))],
    ["indexer", () => Response.json([indexer(6, "FileList", "https://filelist.io", { fields: Array.from({ length: 129 }, () => ({ name: "baseUrl", value: "https://filelist.io" })) })])],
  ] as const) {
    const settings = config();
    let calls = 0;
    const fetcher = (async (input, init) => {
      calls++;
      expect(init?.redirect).toBe("manual");
      const url = new URL(String(input));
      expect(url.origin).toBe("https://prowlarr.invalid");
      return url.pathname.endsWith("/" + endpoint) ? response() : Response.json([indexer()]);
    }) as typeof fetch;
    await expect(refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher)).rejects.toThrow("Autobrr Prowlarr download origin discovery is unavailable");
    expect(calls).toBe(endpoint === "indexer" ? 1 : 2);
    expect(parseAutobrrRelease(settings, event())).toBeNull();
  }
});

test("invalid Prowlarr URLs cannot receive credentials or grant trust", async () => {
  for (const url of [
    "file:///etc/passwd", "https://user:pass@prowlarr.invalid", "https://prowlarr.invalid/?apiKey=private", "https://prowlarr.invalid/#private",
  ]) {
    const settings = config();
    let calls = 0;
    const fetcher = (async (_input: Parameters<typeof fetch>[0]) => {
      calls++;
      return Response.json([indexer()]);
    }) as typeof fetch;
    await expect(refreshAutobrrDownloadOrigins(settings, { ...prowlarr, url }, fetcher)).rejects.toThrow("Autobrr Prowlarr download origin discovery is unavailable");
    expect(calls).toBe(0);
    expect(parseAutobrrRelease(settings, event())).toBeNull();
  }
});

test("shutdown aborts stalled discovery bodies, cancels the stream and grants no trust", async () => {
  for (const endpoint of ["indexer", "indexerstatus"]) {
    const settings = config();
    const lifetime = new AbortController();
    let reading!: () => void;
    const started = new Promise<void>(resolve => { reading = resolve; });
    let cancelled = false;
    let requestSignal: AbortSignal | undefined;
    const fetcher = (async (input, init) => {
      expect(init?.signal).toBeDefined();
      if (String(input).endsWith("/" + endpoint)) {
        requestSignal = init!.signal!;
        return new Response(new ReadableStream({
          pull() { reading(); },
          cancel() { cancelled = true; },
        }, { highWaterMark: 0 }));
      }
      return Response.json([indexer()]);
    }) as typeof fetch;
    const pending = refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher, lifetime.signal);
    await started;
    lifetime.abort();
    await expect(pending).rejects.toThrow("Autobrr Prowlarr download origin discovery is unavailable");
    expect(requestSignal?.aborted).toBe(true);
    expect(cancelled).toBe(true);
    expect(parseAutobrrRelease(settings, event())).toBeNull();
    expect(parseAutobrrRelease(settings, event("Other", "http://prowlarr.invalid/download"))).not.toBeNull();
  }
});

test("discovery snapshots server settings before asynchronous requests", async () => {
  const settings = config();
  const mutable = { ...prowlarr };
  let finish!: (response: Response) => void;
  const fetcher = (async (input, init) => {
    expect(new URL(String(input)).origin).toBe("https://prowlarr.invalid");
    expect(new Headers(init?.headers).get("X-Api-Key")).toBe(prowlarr.apiKey);
    return String(input).endsWith("/indexerstatus") ? Response.json([]) : new Promise<Response>(resolve => { finish = resolve; });
  }) as typeof fetch;
  const pending = refreshAutobrrDownloadOrigins(settings, mutable, fetcher);
  mutable.url = "https://replacement.invalid";
  mutable.apiKey = "rotated";
  finish(Response.json([indexer()]));
  await pending;
  expect(parseAutobrrRelease(settings, event())).not.toBeNull();
});

test("chunked Prowlarr bodies stop at the byte limit and cancel the stream", async () => {
  const settings = config();
  let cancelled = false, calls = 0;
  const fetcher = (async (_input: Parameters<typeof fetch>[0]) => {
    calls++;
    return new Response(new ReadableStream({
      pull(controller) { controller.enqueue(new Uint8Array(1024 * 1024)); },
      cancel() { cancelled = true; },
    }));
  }) as typeof fetch;
  await expect(refreshAutobrrDownloadOrigins(settings, prowlarr, fetcher)).rejects.toThrow("Autobrr Prowlarr download origin discovery is unavailable");
  expect(calls).toBe(1);
  expect(cancelled).toBe(true);
  expect(parseAutobrrRelease(settings, event())).toBeNull();
});

test("Autobrr can run with only discovered origins, but an empty trust cache authorizes nothing", () => {
  const settings = readAutobrrConfig({
    AUTOBRR_URL: "http://autobrr.invalid", AUTOBRR_API_KEY: "fixture", AUTOBRR_PODIBLE_URL: "http://podible.invalid",
  })!;
  expect(settings.downloadOrigins.size).toBe(0);
  expect(parseAutobrrRelease(settings, event())).toBeNull();
});
