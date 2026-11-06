import { afterAll, beforeAll, expect, test } from "bun:test";
import { createTRPCClient, httpLink } from "@trpc/client";
import type { AppRouter } from "../../src/trpc";

function spawnFixture() {
  return Bun.spawn([process.execPath, "run", "test/fixtures/spa-server.ts"], {
    stdout: "pipe", stderr: "inherit",
    env: { ...process.env, PORT: "0", SHADOW_FIXTURE: "1", TV_FIXTURE: "1", ADMIN_FIXTURE: "", AUTH_FIXTURE: "",
      FIXTURE_TLS_KEY: "", FIXTURE_TLS_CERT: "" },
  });
}
let server: ReturnType<typeof spawnFixture> | undefined;
let origin = "";
const adminToken = "spa-browser-fixture", readerToken = "tv-reader-fixture";
const client = (token = adminToken) => createTRPCClient<AppRouter>({ links: [httpLink({
  url: `${origin}/api/trpc`, headers: { cookie: `podible_session=${token}`, origin },
})] });
async function rpc(method: string, params: Record<string, unknown> = {}, token = adminToken) {
  const response = await fetch(`${origin}/rpc`, { method: "POST", headers: { "Content-Type": "application/json",
    cookie: `podible_session=${token}`, origin }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) });
  expect(response.status).toBe(200);
  return response.json() as Promise<{ result?: unknown; error?: { code: number } }>;
}

beforeAll(async () => {
  server = spawnFixture();
  const reader = server.stdout.getReader();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const timeout = new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("Diagnostic API fixture startup timed out")),15_000); });
    let output = "";
    while (!origin) {
      const part = await Promise.race([reader.read(),timeout]);
      output += new TextDecoder().decode(part.value);
      origin = /SPA_FIXTURE_URL=(\S+)/.exec(output)?.[1]?.replace(/\/$/,"") ?? "";
      if (part.done && !origin) throw new Error("Diagnostic API fixture exited without a URL");
      if (output.length > 16_384) throw new Error("Diagnostic API fixture startup exceeded output budget");
    }
  } finally { clearTimeout(timer); reader.releaseLock(); }
},20_000);
afterAll(async () => {
  const child = server;
  if (!child) return;
  child.kill("SIGTERM");
  const timer = setTimeout(() => child.kill("SIGKILL"),3000);
  try { await child.exited; } finally { clearTimeout(timer); }
});

test("retired diagnostic pages leave the movie/TV/admin RPC authorization boundaries intact", async () => {
  const queries = ["tv.status", "tv.list", "tv.get", "tvShadow.status", "tvShadow.list", "tvShadow.get",
    "movieShadow.status", "movieShadow.list", "movieShadow.get", "movieDualWrite.status", "movieDualWrite.list", "indexerFeeds.status"];
  const mutations = ["tv.sync", "tv.backfill", "tv.retry", "tvShadow.sync", "tvShadow.retry", "movieShadow.sync",
    "movieShadow.retry", "movieShadow.review", "movieDualWrite.sync", "movieDualWrite.backfill", "indexerFeeds.sync"];
  for (const [token,code,status] of [["",-32001,401],[readerToken,-32003,403]] as const) {
    for (const method of [...queries,...mutations]) {
      const params = method === "tv.get" || method === "tv.retry" ? { requestId: 1 }
        : method === "movieShadow.review" ? { caseId: 1, verdict: "uncertain", notes: "" }
        : /Shadow\.(get|retry)$/.test(method) ? { caseId: 1 }
        : method.endsWith(".backfill") ? { backfillRequestIds: [726] } : {};
      expect((await rpc(method,params,token)).error?.code).toBe(code);
      const headers = { cookie: `podible_session=${token}`, "Content-Type": "application/json", origin };
      const response = await fetch(`${origin}/api/trpc/${method}${queries.includes(method) ? "?input=" + encodeURIComponent(JSON.stringify(params)) : ""}`,
        queries.includes(method) ? { headers } : { method: "POST", headers, body: JSON.stringify(params) });
      expect(response.status).toBe(status);
    }
  }
});

test("movie audit review persists across HTTP reads and observation/feed actions still enqueue real jobs", async () => {
  const api = client();
  expect((await api.movieDualWrite.status.query({})).keyConfigured).toBe(true);
  const first = (await api.movieShadow.list.query({})).items[0]!;
  expect(first.input.observation.movie.title).toBe("A Fixture Film");
  expect(first.result).toMatchObject({ decision: "unobservable", reason: "No source files remain" });
  const notes = "Missing source is not a wrong release.";
  const reviewed = await api.movieShadow.review.mutate({ caseId: first.id, verdict: "uncertain", notes });
  expect(reviewed.case.review).toMatchObject({ verdict: "uncertain", notes });
  expect((await api.movieShadow.get.query({ caseId: first.id })).case.review).toEqual(reviewed.case.review);
  const legacy = await rpc("movieShadow.get",{ caseId: first.id });
  expect(legacy.result).toEqual({ case: await api.movieShadow.get.query({ caseId: first.id }).then(value => value.case) });
  const observation = await api.movieShadow.sync.mutate({});
  expect((await api.jobs.get.query({ jobId: observation.jobId })).job).toMatchObject({ type: "movie_shadow_sync", status: "queued" });
  expect((await api.movieShadow.status.query({})).lastSync?.jobId).toBe(observation.jobId);
  const feeds = await api.indexerFeeds.status.query({});
  expect(feeds.feeds.find(feed => feed.provider === "Fixture indexer")?.gapNote).toBe("Earlier releases are outside the available feed history.");
  const poll = await api.indexerFeeds.sync.mutate({});
  expect((await api.jobs.get.query({ jobId: poll.jobId })).job).toMatchObject({ type: "indexer_feed_sync", status: "queued" });
  const sync = await api.movieDualWrite.sync.mutate({});
  expect((await api.jobs.get.query({ jobId: sync.jobId })).job).toMatchObject({ type: "movie_dual_write_sync", status: "queued" });
});

test("TV audit evidence stays distinct from native published coverage, including private user-safe reads", async () => {
  const api = client(), reader = client(readerToken);
  const native = (await api.tv.list.query({})).items[0]!;
  expect(native).toMatchObject({ requestedEpisodes: 2, availableEpisodes: 1, futureEpisodes: 1, seasonNumbers: [2] });
  expect(native.acquisitions[0]?.decision).toMatchObject({ plan: { files: [{ name: "native-selected-file.mkv" }] } });
  expect(native.events.some(event => event.message === "One native episode available; future episode intent retained")).toBe(true);
  const audit = (await api.tvShadow.list.query({})).items[0]!;
  const reloaded = (await api.tvShadow.get.query({ caseId: audit.id })).case;
  expect(reloaded).toEqual(audit);
  expect(audit.input.observation).toMatchObject({ sourceKey: "sonarr:fixture:queue:1", sonarr: { outcome: "blocked" } });
  expect(audit.input.entry.relations[0]).toMatchObject({ title: "Fixture Prequel" });
  expect(audit.input.entry.episodes[0]?.numbering.map(number => number.scheme).sort()).toEqual(["anidb","tvdb:aired"]);
  expect(audit.input.entry.episodes[0]?.numbering.find(number => number.scheme === "tvdb:aired"))
    .toMatchObject({ season: 2, episode: 1, absolute: null, source: "fixture:sonarr:aired" });
  expect(audit.input.entry.episodes[0]?.numbering.find(number => number.scheme === "anidb"))
    .toMatchObject({ season: 1, episode: 1, absolute: 1, source: "fixture:explicit-anime-map" });
  expect(audit.result).toMatchObject({ decision: "import", probes: [{ probe: { streams: [{ codec_type: "video", codec_name: "h264" },{ codec_type: "audio" }] } }] });
  expect((await api.tv.get.query({ requestId: native.requestId })).availableEpisodes).toBe(1);
  const safe = await reader.tv.myRequest.query({ requestId: native.requestId });
  expect(safe.request).toMatchObject({ availableEpisodes: 1, futureEpisodes: 1, externalRequestId: null });
  expect(safe.request.acquisitions.every(acquisition => acquisition.decision === null)).toBe(true);
  expect(safe.request.events.every(event => event.message === `TV acquisition: ${event.stage}`)).toBe(true);
  await expect(api.tv.myRequest.query({ requestId: native.requestId })).rejects.toMatchObject({ data: { code: "FORBIDDEN" } });
  for (const secret of ["tv-fixture-seerr-private", "tv-fixture-agent-private", "tv-fixture-sonarr-private"])
    expect(JSON.stringify([native,audit,safe,await api.tv.status.query({}),await api.tvShadow.status.query({})])).not.toContain(secret);
});

test("TV approved-season backfill, sync and retry queue without manufacturing episode availability", async () => {
  const api = client(), before = (await api.tv.list.query({})).items[0]!;
  for (const backfillRequestIds of [[726],[726,724,723],[1,2,3,4,5]]) {
    const queued = await api.tv.backfill.mutate({ backfillRequestIds });
    const job = (await api.jobs.get.query({ jobId: queued.jobId })).job;
    expect(job).toMatchObject({ type: "tv_sync", status: "queued" });
    expect(JSON.parse(job.payload_json!)).toEqual({ backfillRequestIds });
    expect((await api.tv.status.query({})).lastSync).toEqual({ jobId: queued.jobId, status: "queued" });
  }
  const sync = await api.tv.sync.mutate({});
  expect((await api.tv.status.query({})).lastSync).toEqual({ jobId: sync.jobId, status: "queued" });
  await expect(api.tv.retry.mutate({ requestId: before.requestId })).rejects.toMatchObject({data:{code:"BAD_REQUEST"}});
  const after = await api.tv.get.query({ requestId: before.requestId });
  expect(after.acquisitions).toHaveLength(before.acquisitions.length); // Only an unaired episode remains; no pointless search.
  expect(after.availableEpisodes).toBe(before.availableEpisodes);
  expect(after.futureEpisodes).toBe(before.futureEpisodes);
  const observation = await api.tvShadow.sync.mutate({});
  expect((await api.jobs.get.query({ jobId: observation.jobId })).job).toMatchObject({ type: "tv_shadow_sync", status: "queued" });
});

test("retained TV API rejects episode/path scope and malformed backfill before enqueuing anything", async () => {
  const api = client(), before = await api.tv.status.query({});
  const invalid = [
    { backfillRequestIds: [] }, { backfillRequestIds: [0] }, { backfillRequestIds: [-1] }, { backfillRequestIds: [1.5] },
    { backfillRequestIds: [Number.MAX_SAFE_INTEGER + 1] }, { backfillRequestIds: [1,2,3,4,5,6] },
    { backfillRequestIds: ["all"] }, { backfillRequestIds: ["/etc/passwd"] },
    { backfillRequestIds: [726], episodeLimit: 1 }, { backfillRequestIds: [726], episodeIds: [1] },
    { backfillRequestIds: [726], path: "/etc/passwd" },
  ];
  for (const params of invalid) {
    expect((await rpc("tv.backfill",params)).error?.code).toBe(-32602);
    const response = await fetch(`${origin}/api/trpc/tv.backfill`, { method: "POST", headers: {
      "Content-Type": "application/json", cookie: `podible_session=${adminToken}`, origin }, body: JSON.stringify(params) });
    expect(response.status).toBe(400);
  }
  expect(await api.tv.status.query({})).toEqual(before);
  expect((await rpc("tvShadow.sync",{ sourcePaths: ["/etc/passwd"] })).error?.code).toBe(-32602);
  expect((await rpc("tv.readPath",{ path: "/etc/passwd" })).error?.code).toBe(-32601);
  for (const pathname of ["/api/trpc/tv.missing", "/tv-sync/arbitrary-path", "/web-assets/tv-missing.js"])
    expect((await fetch(origin + pathname,{ headers: { cookie: `podible_session=${adminToken}` } })).status).toBe(404);
});
