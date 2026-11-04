import { expect, test } from "bun:test";
import {
  ANIME_LINKS_URL, ANIME_MAPPING_URL, ANILIST_URL, AnimeMetadataUnavailableError,
  createAnimeMetadataAdapter, enrichAnimeEntry, parseAniListMedia, parseAnimeLinksJson,
  parseAnimeListXml, projectAnimeEntries, type AnimeTvdbEpisode,
} from "../../src/tv/anime";
import { tvCatalogEntrySchema, type TvCatalogEntry } from "../../src/tv/types";

const xml = (...rows: string[]) => `<anime-list>${rows.join("")}</anime-list>`;
const row = (id: number, attrs = 'defaulttvdbseason="1"', rules = "", tvdbId = 10) =>
  `<anime anidbid="${id}" tvdbid="${tvdbId}" ${attrs}><name>Work ${id}</name>${rules ? `<mapping-list>${rules}</mapping-list>` : ""}</anime>`;
const episode = (season: number, number: number, key = `tvdb:${season * 100 + number}`): AnimeTvdbEpisode =>
  ({ key, season, episode: number, title: "Provider title", airDate: "2020-01-01", runtimeMinutes: 24, absolute: 30 });
const project = (data: string, episodes: AnimeTvdbEpisode[]) =>
  projectAnimeEntries({ tvdbId: 10, episodes, mappings: parseAnimeListXml(data).records, links: [] });
const payload = (id: number, episodes: number | null = 12, relations: unknown[] = []) =>
  ({ data: { Media: { id, type: "ANIME", episodes, title: { romaji: `Exact work ${id}` }, relations: { edges: relations } } } });
const edge = (id: number, relationType = "SEQUEL", type = "ANIME") =>
  ({ relationType, node: { id, type, title: { romaji: `Related ${id}` } } });

test("simple defaults split TVDB seasons into independent anime IDs and retain source IDs/numbering", () => {
  const result = project(xml(row(1), row(2, 'defaulttvdbseason="2" episodeoffset="12"')),
    [episode(1, 1), episode(2, 13)]);
  expect(result.entries.map(e => e.key)).toEqual(["anidb:1", "anidb:2"]);
  expect(result.entries.every(e => e.tvdbId === 10 && e.anime)).toBe(true);
  const ep = result.entries[1]!.episodes[0]!;
  expect(ep.key).toBe("tvdb:213");
  expect(ep.sourceEpisodeKey).toBe(ep.key);
  expect(ep.numbering.map(n => [n.scheme, n.season, n.episode])).toEqual([
    ["tvdb:aired", 2, 13], ["anidb", 1, 1],
  ]);
  expect(ep.numbering[0]!.absolute).toBe(30);
  expect(ep.runtimeMinutes).toBe(24);
  expect(result.warnings.some(w => w.code === "missing-relations")).toBe(true);
});

test("finite explicit ranges, pair overrides, negatives, and sentinel zero are inverse-mapped", () => {
  const data = xml(row(1, 'defaulttvdbseason="1"',
    '<mapping anidbseason="1" tvdbseason="2" start="13" end="24" offset="-12">;14-9;15-0;</mapping>' +
    '<mapping anidbseason="0" tvdbseason="0">;1-4;</mapping>'));
  const result = project(data, [episode(2, 1), episode(2, 9), episode(0, 4), episode(2, 3), episode(1, 15)]);
  // TVDB S2E9 maps to both native 14 (pair) and 21 (range), so it is genuinely ambiguous.
  expect(result.entries[0]!.episodes.map(e => e.numbering[1]!.episode)).toEqual([13, 1]);
  expect(result.unmappedEpisodeKeys).toEqual(["tvdb:209", "tvdb:203", "tvdb:115"]);
  expect(result.warnings.some(w => w.code === "ambiguous-mapping")).toBe(true);
});

test("individual mappings override range/default for the same native episode across seasons", () => {
  const result = project(xml(row(1, 'defaulttvdbseason="1"',
    '<mapping anidbseason="1" tvdbseason="2">;1-4;2-0;</mapping>')),
  [episode(1, 1), episode(1, 2), episode(1, 3), episode(2, 4)]);
  expect(result.unmappedEpisodeKeys).toEqual(["tvdb:101", "tvdb:102"]);
  expect(result.entries[0]!.episodes.map(e => e.numbering[1]!.episode)).toEqual([3, 1]);
});

test("special-season one-offs with explicit regular lists do not claim unrelated specials", () => {
  const result = project(xml(row(1, 'defaulttvdbseason="0"',
    '<mapping anidbseason="1" tvdbseason="0">;1-5;</mapping>')),
  [episode(0, 1), episode(0, 5), episode(0, 25)]);
  expect(result.entries[0]!.episodes.map(e => e.key)).toEqual(["tvdb:5"]);
  expect(result.unmappedEpisodeKeys).toEqual(["tvdb:1", "tvdb:25"]);
});

test("unsupported ranges, combined episodes, encoded trailers, absolute defaults never guess", () => {
  for (const rules of [
    '<mapping anidbseason="1" tvdbseason="1" start="1" offset="0"/>',
    '<mapping anidbseason="1" tvdbseason="1" start="9" end="2"/>',
    '<mapping anidbseason="1" tvdbseason="1">;1-1+2;</mapping>',
    '<mapping anidbseason="0" tvdbseason="0">;201-1;</mapping>',
    '<mapping anidbseason="1" tvdbseason="1"/>',
  ]) {
    const parsed = parseAnimeListXml(xml(row(1, 'defaulttvdbseason="1"', rules)));
    expect(parsed.records[0]!.unsupported).toBe(true);
    expect(parsed.warnings.some(w => w.code === "unsupported-mapping")).toBe(true);
    expect(project(xml(row(1, 'defaulttvdbseason="1"', rules)), [episode(1, 1)]).entries).toEqual([]);
  }
  const result = project(xml(row(1, 'defaulttvdbseason="a"')), [episode(1, 1)]);
  expect(result.entries).toEqual([]);
  expect(result.warnings.some(w => w.code === "unsupported-mapping")).toBe(true);
});

test("unsupported competitors cannot silently turn a different default into a unique match", () => {
  const result = project(xml(row(1), row(2, 'defaulttvdbseason="1"',
    '<mapping anidbseason="1" tvdbseason="1">;1-1+2;</mapping>')), [episode(1, 1)]);
  expect(result.entries).toEqual([]);
  expect(result.warnings.some(w => w.episodeKey === "tvdb:101" && w.code === "unsupported-mapping")).toBe(true);
});

test("explicit coordinates can still be used when the default is absolute/letter-based", () => {
  const result = project(xml(row(1, 'defaulttvdbseason="a"',
    '<mapping anidbseason="1" tvdbseason="2" start="13" end="24" offset="-12"/>')),
  [episode(2, 1), episode(1, 1)]);
  expect(result.entries[0]!.episodes[0]!.numbering[1]!.episode).toBe(13);
  expect(result.unmappedEpisodeKeys).toEqual(["tvdb:101"]);
  expect(result.warnings.some(w => w.code === "unsupported-mapping")).toBe(true);
});

test("TMDB-only rules cannot alter a TVDB mapping; unknown TVDB IDs and title matches are ignored", () => {
  const data = xml(row(1, 'defaulttvdbseason="1"',
    '<mapping anidbseason="1" tmdbseason="1">;1-7;</mapping>'),
  '<anime anidbid="2" tvdbid="unknown" defaulttvdbseason="1"><name>Provider title</name></anime>');
  expect(parseAnimeListXml(data).records).toHaveLength(1);
  expect(project(data, [episode(1, 1)]).entries[0]!.key).toBe("anidb:1");
});

test("overlapping defaults stay uncertain until exact-ID episode counts supply finite bounds", () => {
  const mappings = parseAnimeListXml(xml(row(1), row(2, 'defaulttvdbseason="1" episodeoffset="13"'))).records;
  const links = [{ anidbId: 1, anilistId: 101 }, { anidbId: 2, anilistId: 102 }];
  const input = { tvdbId: 10, episodes: [episode(1, 14)], mappings, links };
  expect(projectAnimeEntries(input).entries).toEqual([]);
  const result = projectAnimeEntries({ ...input, media: [parseAniListMedia(payload(101, 13), 101)] });
  expect(result.entries[0]!.key).toBe("anidb:2");
  expect(result.entries[0]!.episodes[0]!.numbering[1]!.episode).toBe(1);
});

test("many native episodes for one TVDB episode remain ambiguous, not first-match", () => {
  const result = project(xml(row(1, 'defaulttvdbseason="0"',
    '<mapping anidbseason="1" tvdbseason="0">;1-7;2-7;</mapping>')), [episode(0, 7)]);
  expect(result.entries).toEqual([]);
  expect(result.warnings.some(w => w.code === "ambiguous-mapping")).toBe(true);
});

test("crosswalk rejects conflicting links both directions while allowing duplicate identical rows", () => {
  const parsed = parseAnimeLinksJson(JSON.stringify([
    { anidb_id: 1, anilist_id: 11 }, { anidb_id: 1, anilist_id: 12 },
    { anidb_id: 2, anilist_id: 20 }, { anidb_id: 3, anilist_id: 20 },
    { anidb_id: "4", anilist_id: "40" }, { anidb_id: 4, anilist_id: 40 },
    { title: "Work 5", anilist_id: 50 }, { anidb_id: 6, anilist_id: [60, 61] },
  ]));
  expect(parsed.links).toEqual([{ anidbId: 4, anilistId: 40 }]);
  expect(parsed.warnings).toHaveLength(3);
});

test("exact AniList relations use node IDs, retain source/type, exclude manga, and never invent links", () => {
  const media = parseAniListMedia(payload(11, 12, [edge(22), edge(99, "SIDE_STORY"), edge(33, "SOURCE", "MANGA")]), 11);
  const result = projectAnimeEntries({ tvdbId: 10, episodes: [episode(1, 1)],
    mappings: parseAnimeListXml(xml(row(1))).records,
    links: [{ anidbId: 1, anilistId: 11 }, { anidbId: 2, anilistId: 22 }], media: [media] });
  expect(result.entries[0]!.relations).toEqual([
    { key: "anidb:2", title: "Related 22", relation: "SEQUEL", source: `${ANILIST_URL}#Media:11` },
    { key: "anilist:99", title: "Related 99", relation: "SIDE_STORY", source: `${ANILIST_URL}#Media:11` },
  ]);
  expect(() => parseAniListMedia(payload(12), 11)).toThrow("different ID");
  expect(() => parseAniListMedia({ ...payload(11), errors: [{ message: "partial" }] }, 11)).toThrow("errors");
  expect(() => parseAniListMedia({ data: { Media: { ...payload(11).data.Media, type: "MANGA" } } }, 11)).toThrow();
});

test("XML rejects entities/malformed roots, duplicate IDs, oversized text/rules; projection is bounded", () => {
  for (const data of ['<!DOCTYPE anime-list [<!ENTITY x "bad">]><anime-list/>', "<anime-list>", "<wrong/>"])
    expect(() => parseAnimeListXml(data)).toThrow();
  expect(() => parseAnimeListXml("x".repeat(16 * 1024 * 1024 + 1))).toThrow("byte limit");
  const duplicate = parseAnimeListXml(xml(row(1), row(1)));
  expect(duplicate.records.every(r => r.unsupported)).toBe(true);
  expect(() => project(xml(row(1)), [episode(1, 1), episode(1, 1)])).toThrow("duplicate");
  expect(() => project(xml(row(1)), Array.from({ length: 2_001 }, (_, i) => episode(1, i + 1)))).toThrow("oversized");
});

// Facts copied from public snapshots downloaded during implementation, not title matching.
const codeGeassXml = xml(
  '<anime anidbid="4521" tvdbid="79525" defaulttvdbseason="1" episodeoffset=""><name>Code Geass: Hangyaku no Lelouch</name></anime>',
  '<anime anidbid="5373" tvdbid="79525" defaulttvdbseason="2" episodeoffset=""><name>Code Geass: Hangyaku no Lelouch R2</name></anime>',
);
const codeGeassLinks = JSON.stringify([{ anidb_id: 4521, anilist_id: 1575 }, { anidb_id: 5373, anilist_id: 2904 }]);
const baseEntry = (): TvCatalogEntry => ({ key: "tvdb:79525", tvdbId: 79525, tmdbId: null,
  title: "Code Geass", aliases: [], year: 2006, anime: false, refs: [],
  source: "sonarr:test:api/v3/series", relations: [], episodes: [1, 2].map(season => ({
    key: `tvdb:${100 + season}`, title: `Provider ${season}`, airDate: season === 1 ? "2006-10-06" : "2008-04-06",
    runtimeMinutes: 24, refs: [{ provider: "tvdb", id: String(100 + season), instance: null, source: "sonarr:test" }],
    identity: { stability: "provider", source: "sonarr:test" }, numbering: [
      { scheme: "tvdb:aired", season, episode: 1, absolute: null, source: "sonarr:test:api/v3/episode" },
      { scheme: "sonarr:test:scene", season, episode: 1, absolute: null, source: "sonarr:test:api/v3/episode" },
    ],
  })) });

test("real Code Geass canary produces schema-valid independent catalog entries with exact-ID graphs", async () => {
  const calls: { url: string; init?: RequestInit }[] = [];
  const adapter = createAnimeMetadataAdapter({ minRequestIntervalMs: 0, fetch: async (url, init) => {
    calls.push({ url, init });
    expect(init?.redirect).toBe("error");
    expect(new Headers(init?.headers).has("Authorization")).toBe(false);
    if (url === ANIME_MAPPING_URL) return new Response(codeGeassXml);
    if (url === ANIME_LINKS_URL) return new Response(codeGeassLinks);
    const id = JSON.parse(String(init?.body)).variables.id;
    return Response.json(payload(id, 25, [edge(id === 1575 ? 2904 : 1575, id === 1575 ? "SEQUEL" : "PREQUEL")]));
  } });
  const base = baseEntry();
  const before = structuredClone(base);
  const result = await enrichAnimeEntry(base, adapter);
  expect(result.unmappedEpisodeKeys).toEqual([]);
  expect(result.entries.map(e => e.key)).toEqual(["anidb:4521", "anidb:5373"]);
  for (const entry of result.entries) expect(tvCatalogEntrySchema.safeParse(entry).success).toBe(true);
  expect(result.entries[0]!.relations[0]!.key).toBe("anidb:5373");
  expect(result.entries[1]!.relations[0]!.relation).toBe("PREQUEL");
  expect(result.entries[1]!.year).toBeNull();
  expect(result.entries[1]!.episodes[0]!.numbering.slice(0, 2)).toEqual(base.episodes[1]!.numbering);
  expect(result.entries[1]!.episodes[0]!.numbering[2]!.scheme).toBe("anidb");
  expect(result.entries[1]!.episodes[0]!.refs).toEqual(base.episodes[1]!.refs);
  expect(base).toEqual(before);
  expect(calls.map(c => c.url)).toEqual([ANIME_MAPPING_URL, ANIME_LINKS_URL, ANILIST_URL, ANILIST_URL]);
  expect(String(calls[2]!.init?.body)).not.toContain('"search"');
  await adapter.enrichAnimeEntry(base);
  expect(calls).toHaveLength(4);
});

test("real Fate/Zero same-season offset canary splits after exact work count bounds, never before", () => {
  const mappings = parseAnimeListXml(xml(
    '<anime anidbid="8160" tvdbid="275798" defaulttvdbseason="1" episodeoffset=""><name>Fate/Zero</name></anime>',
    '<anime anidbid="8880" tvdbid="275798" defaulttvdbseason="1" episodeoffset="13"><name>Fate/Zero (2012)</name></anime>',
  )).records;
  const input = { tvdbId: 275798, mappings, links: [{ anidbId: 8160, anilistId: 10087 }, { anidbId: 8880, anilistId: 11741 }],
    episodes: [episode(1, 1), episode(1, 14)] };
  expect(projectAnimeEntries(input).unmappedEpisodeKeys).toEqual(["tvdb:114"]);
  const result = projectAnimeEntries({ ...input, media: [parseAniListMedia(payload(10087, 13), 10087),
    parseAniListMedia(payload(11741, 12), 11741)] });
  expect(result.entries.map(e => e.key)).toEqual(["anidb:8160", "anidb:8880"]);
  expect(result.entries[1]!.episodes[0]!.numbering[1]!.episode).toBe(1);
});

test("catalog wrapper preserves uncertain/missing coordinates in the base entry; graph absence is explicit", async () => {
  const adapter = createAnimeMetadataAdapter({ maxMediaRequests: 0, fetch: async url =>
    new Response(url === ANIME_MAPPING_URL ? codeGeassXml : codeGeassLinks) });
  const base = baseEntry();
  base.episodes[1]!.numbering = [{ scheme: "sonarr:test:scene", season: 2, episode: 1, absolute: null }];
  const result = await adapter.enrichAnimeEntry(base);
  expect(result.entries.map(e => e.key)).toEqual(["anidb:4521", "tvdb:79525"]);
  expect(result.entries[1]!.episodes).toEqual([base.episodes[1]!]);
  expect(result.warnings.some(w => w.code === "missing-relations")).toBe(true);
  expect(result.warnings.some(w => w.code === "request-limit")).toBe(true);
  const unavailable = createAnimeMetadataAdapter({ fetch: async () => { throw new Error("offline"); } });
  expect((await unavailable.enrichAnimeEntry(base)).entries).toEqual([base]);
});

test("missing TVDB identity and zero/conflicting aired coordinates preserve the base without network", async () => {
  let calls = 0;
  const adapter = createAnimeMetadataAdapter({ fetch: async () => { calls++; return new Response(""); } });
  const missingId = { ...baseEntry(), tvdbId: null };
  expect((await adapter.enrichAnimeEntry(missingId)).entries).toEqual([missingId]);
  const base = baseEntry();
  base.episodes[0]!.numbering[0]!.episode = 0;
  base.episodes[1]!.numbering.push({ scheme: "tvdb:aired", season: 3, episode: 2, absolute: null });
  const result = await adapter.enrichAnimeEntry(base);
  expect(result.entries).toEqual([base]);
  expect(result.unmappedEpisodeKeys).toEqual(base.episodes.map(e => e.key));
  expect(calls).toBe(0);
});

test("adapter TTL, inflight coalescing, mutation isolation, and LRU cap are deterministic", async () => {
  let clock = 1_000;
  const calls: string[] = [];
  const adapter = createAnimeMetadataAdapter({ now: () => clock, minRequestIntervalMs: 0,
    datasetTtlMs: 100, mediaTtlMs: 50, maxCacheEntries: 1, fetch: async (url, init) => {
      calls.push(url);
      if (url === ANIME_MAPPING_URL) return new Response(codeGeassXml);
      if (url === ANIME_LINKS_URL) return new Response(codeGeassLinks);
      return Response.json(payload(JSON.parse(String(init?.body)).variables.id));
    } });
  const [a, b] = await Promise.all([adapter.loadDatasets(), adapter.loadDatasets()]);
  a.mappings[0]!.title = "mutated";
  expect(b.mappings[0]!.title).not.toBe("mutated");
  expect(calls).toHaveLength(2);
  const [m, n] = await Promise.all([adapter.getMedia(1), adapter.getMedia(1)]);
  m.title = "mutated";
  expect(n.title).not.toBe("mutated");
  expect(calls).toHaveLength(3);
  await adapter.getMedia(2);
  await adapter.getMedia(1); // Entry 1 was evicted, although not expired.
  expect(calls).toHaveLength(5);
  clock += 51;
  await adapter.getMedia(1);
  expect(calls).toHaveLength(6);
  clock += 50;
  await adapter.loadDatasets();
  expect(calls).toHaveLength(8);
});

test("429 Retry-After seconds/date and reset apply across IDs without automatic retries", async () => {
  for (const headers of [{ "retry-after": "120" },
    { "retry-after": new Date(120_000).toUTCString() }, { "x-ratelimit-reset": "120" }]) {
    let clock = 0;
    let calls = 0;
    const adapter = createAnimeMetadataAdapter({ now: () => clock, minRequestIntervalMs: 0, fetch: async () => {
      calls++;
      return calls === 1 ? new Response("rate limited", { status: 429, headers }) : Response.json(payload(2));
    } });
    try { await adapter.getMedia(1); throw new Error("expected 429"); }
    catch (error) { expect(error).toBeInstanceOf(AnimeMetadataUnavailableError);
      expect((error as AnimeMetadataUnavailableError).retryAt).toBe(120_000); }
    await expect(adapter.getMedia(2)).rejects.toThrow("cooling down");
    expect(calls).toBe(1);
    clock = 120_000;
    expect((await adapter.getMedia(2)).id).toBe(2);
    expect(calls).toBe(2);
  }
});

test("successful exhausted rate-limit response is usable but blocks another network request", async () => {
  let calls = 0;
  const adapter = createAnimeMetadataAdapter({ now: () => 0, minRequestIntervalMs: 0,
    fetch: async () => { calls++; return Response.json(payload(1), { headers: { "x-ratelimit-remaining": "0" } }); } });
  expect((await adapter.getMedia(1)).id).toBe(1);
  expect((await adapter.getMedia(1)).id).toBe(1);
  await expect(adapter.getMedia(2)).rejects.toThrow("cooling down");
  expect(calls).toBe(1);
});

test("GraphQL 429 errors also cool down the transport, while invalid JSON never becomes a cached success", async () => {
  let clock = 0;
  let calls = 0;
  const adapter = createAnimeMetadataAdapter({ now: () => clock, minRequestIntervalMs: 0,
    fetch: async () => { calls++; return calls === 1
      ? Response.json({ errors: [{ status: 429, message: "rate limited" }] }) : new Response("not JSON"); } });
  await expect(adapter.getMedia(1)).rejects.toThrow("rate limited");
  await expect(adapter.getMedia(2)).rejects.toThrow("cooling down");
  expect(calls).toBe(1);
  clock = 60_000;
  await expect(adapter.getMedia(2)).rejects.toThrow();
  await expect(adapter.getMedia(2)).rejects.toThrow("cooling down");
  expect(calls).toBe(2);
});

test("request pacing is injected and concurrent different IDs fail fast rather than creating a queue", async () => {
  let clock = 0;
  const delays: number[] = [];
  const adapter = createAnimeMetadataAdapter({ now: () => clock,
    sleep: async ms => { delays.push(ms); clock += ms; }, fetch: async (_url, init) =>
      Response.json(payload(JSON.parse(String(init?.body)).variables.id)) });
  await adapter.getMedia(1);
  await adapter.getMedia(2);
  expect(delays).toEqual([2_100]);
  let finish: ((response: Response) => void) | undefined;
  const pending = createAnimeMetadataAdapter({ minRequestIntervalMs: 0,
    fetch: () => new Promise(resolve => { finish = resolve; }) });
  const first = pending.getMedia(1);
  await expect(pending.getMedia(2)).rejects.toThrow("busy");
  finish!(Response.json(payload(1)));
  expect((await first).id).toBe(1);
});

test("failed dataset refresh does not publish half a new snapshot or serve expired metadata silently", async () => {
  let clock = 0;
  let calls = 0;
  const adapter = createAnimeMetadataAdapter({ now: () => clock, datasetTtlMs: 1,
    fetch: async url => {
      calls++;
      if (calls === 4) return new Response("invalid JSON");
      return new Response(url === ANIME_MAPPING_URL ? codeGeassXml : codeGeassLinks);
    } });
  await adapter.loadDatasets();
  clock = 2;
  const result = await adapter.enrichAnimeEntry(baseEntry());
  expect(result.entries).toEqual([baseEntry()]);
  expect(result.warnings.some(w => w.code === "metadata-unavailable")).toBe(true);
  await expect(adapter.loadDatasets()).rejects.toThrow("cooling down");
  expect(calls).toBe(4);
});

test("streamed/declared oversized bodies and stalled streams fail within bounds and cancel", async () => {
  const declared = createAnimeMetadataAdapter({ maxMediaBytes: 16,
    fetch: async () => new Response("{}", { headers: { "content-length": "1000" } }) });
  await expect(declared.getMedia(1)).rejects.toThrow("byte limit");
  let cancelled = false;
  const streamed = createAnimeMetadataAdapter({ maxMediaBytes: 16, fetch: async () =>
    new Response(new ReadableStream({ start(c) { c.enqueue(new Uint8Array(17)); }, cancel() { cancelled = true; } })) });
  await expect(streamed.getMedia(1)).rejects.toThrow("byte limit");
  expect(cancelled).toBe(true);
  cancelled = false;
  const stalled = createAnimeMetadataAdapter({ timeoutMs: 5, fetch: async () =>
    new Response(new ReadableStream({ cancel() { cancelled = true; } })) });
  await expect(stalled.getMedia(1)).rejects.toThrow("timed out");
  expect(cancelled).toBe(true);
});

test("invalid settings, oversized requests, and missing IDs are rejected before fetching", async () => {
  expect(() => createAnimeMetadataAdapter({ maxCacheEntries: Infinity })).toThrow("limit");
  expect(() => createAnimeMetadataAdapter({ maxDatasetBytes: 32 * 1024 * 1024 })).toThrow("limit");
  let calls = 0;
  const adapter = createAnimeMetadataAdapter({ fetch: async () => { calls++; return new Response(""); } });
  await expect(adapter.enrich({ tvdbId: 0, episodes: [] })).rejects.toThrow("Invalid");
  await expect(adapter.getMedia(0)).rejects.toThrow("Invalid");
  expect(calls).toBe(0);
});

test("oversized aggregate work sets return a warning/base fallback before any AniList fanout", async () => {
  const calls: string[] = [];
  const adapter = createAnimeMetadataAdapter({ fetch: async url => {
    calls.push(url);
    return new Response(url === ANIME_MAPPING_URL
      ? xml(...Array.from({ length: 201 }, (_, i) => row(i + 1, 'defaulttvdbseason="1"', "", 79525)))
      : "[]");
  } });
  const base = baseEntry();
  const result = await adapter.enrichAnimeEntry(base);
  expect(result.entries).toEqual([base]);
  expect(result.warnings.some(w => w.code === "request-limit")).toBe(true);
  expect(calls).toEqual([ANIME_MAPPING_URL, ANIME_LINKS_URL]);
});
