import { expect, test } from "bun:test";
import { Usage, type ModelProvider, type ModelRequest } from "@openai/agents-core";
import { createTvDecider, validateTvImportPlan } from "../../src/tv/agents";
import { defaultSettings } from "../../src/settings";
import { TEXT_AGENT_MODEL } from "../../src/ai-models";
import { createTvFeedMatcher } from "../../src/tv/feed-agent";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import type { TvEntry, TvEpisode } from "../../src/tv/types";
import bencode from "bencode";
import { createHash } from "node:crypto";
import { compactTvImportEvidence, compactTvImportLegend, evidencePage, inlineTvImportEvidence } from "../../src/tv/agent-evidence";

const episode: TvEpisode = { id: 12, key: "tvdb:1001", title: "First of a sequel", airDate: "2020-01-01", runtimeMinutes: 24,
  refs: [], identity: { stability: "provider", source: "fixture" },
  numbering: [{ scheme: "tvdb-aired", season: 2, episode: 1, absolute: null, source: "fixture" },
    { scheme: "anidb", season: 1, episode: 1, absolute: null, source: "fixture" }] };
const entry: TvEntry = { id: 3, key: "anidb:200", title: "A Sequel", aliases: ["An Aggregate Series"], year: 2020,
  posterUrl: null, anime: true, tvdbId: 100, tmdbId: null, episodes: [episode], refs: [], relations: [
    { key: "anidb:199", title: "A Prequel", relation: "PREQUEL", source: "fixture" }] };
const offer = { key: "release", title: "An Aggregate Series 02x01", provider: "Fixture", sizeBytes: 1234,
  seeders: 3, infoHash: null, url: "https://private.invalid/torrent?passkey=hidden", attributes: { secret: "hidden" } };
const files = [{ index: 0, name: "A Sequel - 01.mkv", sizeBytes: 1234,
  probe: { format: { duration: "1440" }, streams: [{ codec_type: "video", codec_name: "h264", width: 1920, height: 1080 }, { codec_type: "audio", tags: { language: "jpn" } }] } }];
const claims = { identityVerified: true, cut: null, source: "web" as const, proper: null };
const plan = { files: [{ fileIndex: 0, episodeIds: [episode.id], facts: claims, reason: "Anime-local 1 maps to provider season 2 episode 1." }], reason: "One whole requested episode." };
function provider(outputs: unknown[]) {
  const calls: ModelRequest[] = [];
  let selectedModel: string | undefined;
  const model: ModelProvider = { getModel(name) { selectedModel = name; return {
    async getResponse(request) {
      calls.push(request);
      const value = outputs[calls.length - 1];
      return { usage: new Usage(), output: Array.isArray(value) ? value : [{ type: "message" as const, role: "assistant" as const,
        status: "completed" as const, content: [{ type: "output_text" as const, text: JSON.stringify(value) }] }] };
    }, async *getStreamedResponse() { throw new Error("unused"); },
  }; } };
  return { model, calls, selectedModel: () => selectedModel };
}

test("TV selector researches an empty initial search without leaking server-owned URLs", async () => {
  const fake = provider([[{ type: "function_call", name: "searchReleases", callId: "search", arguments: JSON.stringify({ query: "An Aggregate Series S02" }), status: "completed" }],
    { index: 0, reason: "Provider season 2 contains the independently modeled sequel." }]);
  const searches: string[] = [];
  const result = await createTvDecider(defaultSettings(), fake.model, async (query) => {
    searches.push(query); return { offers: [offer], warnings: [] };
  }).select({ entry, episodes: [episode], policy: defaultMovieQualityPolicy, offers: [] });
  expect(searches).toEqual(["An Aggregate Series S02"]);
  expect(result.offer?.key).toBe(offer.key);
  expect(result.matchingOffers).toEqual([]);
  expect(fake.selectedModel()).toBe(TEXT_AGENT_MODEL);
  expect(JSON.stringify(fake.calls)).not.toContain("hidden");
  expect(JSON.stringify(fake.calls)).toContain("anidb");
});

test("TV inspection failures never send credential-bearing errors to the model", async () => {
  const fake = provider([[{ type: "function_call", name: "inspectRelease", callId: "inspect",
    arguments: '{"index":0,"offset":0,"limit":5}', status: "completed" }],
    { index: null, reason: "Could not inspect candidate." }]);
  const decider = createTvDecider(defaultSettings(), fake.model, undefined, async () => {
    throw new Error("Network failure for https://tracker.invalid/fixture-tv-secret/announce");
  });
  const result = await decider.select({ entry, episodes: [episode], policy: defaultMovieQualityPolicy, offers: [offer] });
  expect(result.offer).toBeNull();
  expect(fake.calls).toHaveLength(2);
  expect(JSON.stringify(fake.calls[1]?.input)).toContain("Could not inspect this release's torrent file.");
  expect(JSON.stringify(fake.calls)).not.toContain("fixture-tv-secret");
});

test("TV verifier uses probe policy evidence and preserves episode IDs across numbering schemes", async () => {
  const fake = provider([[{ type: "function_call", name: "assessCandidate", callId: "assess", arguments: JSON.stringify({ fileIndex: 0, facts: claims }), status: "completed" }], plan]);
  const result = await createTvDecider(defaultSettings(), fake.model).inspect({ entry, episodes: [episode], policy: defaultMovieQualityPolicy, offer, files });
  expect(result.files[0]?.episodeIds).toEqual([12]);
  expect(JSON.stringify(fake.calls[1]?.input)).toContain("eligible");
  expect(JSON.stringify(fake.calls)).not.toContain("hidden");
});

test("a small TV pack has all requested episodes and sanitized stream evidence in the first model call", async () => {
  const episodes = Array.from({ length: 12 }, (_, i) => ({ ...episode, id: 200 + i, title: `Requested member ${i + 1}` }));
  const candidates = episodes.map((_, i) => ({ ...files[0]!, index: i, name: `Downloaded member ${i + 1}.mkv`,
    probe: { ...files[0]!.probe, format: { duration: "1440", secret: "private-probe-tag" } },
  }));
  const expected = { ...plan, files: episodes.map(({ id }, i) => ({ ...plan.files[0]!, fileIndex: i, episodeIds: [id] })) };
  const fake = provider([expected]);
  expect(await createTvDecider(defaultSettings(), fake.model).inspect({ entry, episodes, policy: defaultMovieQualityPolicy,
    offer, files: candidates })).toEqual(expected);
  expect(fake.calls).toHaveLength(1);
  const first = JSON.stringify(fake.calls[0]!.input);
  expect(first).toContain("inlineEvidence");
  expect(first).toContain("streamProfiles");
  expect(first).toContain("Requested member 12");
  expect(first).toContain("Downloaded member 12.mkv");
  expect(first).toContain("1440");
  expect(first).toContain("jpn");
  expect(first).not.toContain("private-probe-tag");
  expect(first).not.toContain("passkey");
  expect(fake.calls[0]!.tools.map(tool => tool.name)).toContain("getFileProbes");
});

test("compact TV evidence round trips exactly, preserving stream order, paging and distinct profiles", () => {
  const streams = [
    ...files[0]!.probe.streams,
    { codec_type: "video", codec_name: "mjpeg", disposition: { attached_pic: 1 } },
    ...Array.from({ length: 10 }, () => ({ codec_type: "subtitle", tags: { language: "eng" } })),
  ];
  const probe = { format: { duration: "1440.001" }, streams };
  const same = { format: { duration: "1452.002" }, streams };
  const different = { format: {}, streams: [streams[1]!, streams[0]!, ...streams.slice(2)] };
  const episodes = [episode, { ...episode, id: 13, runtimeMinutes: null, identity: { ...episode.identity, stability: "ephemeral" as const },
    numbering: Array.from({ length: 31 }, (_, i) => ({ scheme: "anidb", source: "mapping", season: null, episode: null, absolute: i + 1 })) }];
  const evidence = inlineTvImportEvidence([probe, same, different].map((probe, i) => ({ ...files[0]!, index: 4 + i, probe })), episodes)!;
  const packed = compactTvImportEvidence(evidence);
  expect(packed.streamProfiles).toHaveLength(2);
  expect(packed.files.map(file => file[6])).toEqual([0, 0, 1]);
  expect(packed.numberingSources).toHaveLength(3);
  const absent = <T>(v: T | null) => v ?? undefined;
  const decoded = {
    files: packed.files.map(([index, name, sizeBytes, duration, streamCount, nextOffset, profile]) => ({
      index, name, sizeBytes, format: { duration: absent(duration) }, streamCount, nextOffset,
      streams: packed.streamProfiles[profile]!.map(([type, codec, width, height, transfer, picture, language]) => ({
        codec_type: absent(type), codec_name: absent(codec), width: absent(width), height: absent(height),
        color_transfer: absent(transfer), disposition: { attached_pic: absent(picture) }, tags: { language: absent(language) },
      })),
    })),
    episodes: packed.episodes.map(([id, key, title, runtimeMinutes, numbering, stability, moreNumbering]) => ({
      id, key, title, runtimeMinutes, numbering: numbering.map(([coordinate, season, episode, absolute]) => {
        const [scheme, source] = packed.numberingSources[coordinate]!;
        return { scheme, season, episode, absolute, source };
      }), identity: { stability }, moreNumbering,
    })),
  };
  expect(decoded).toStrictEqual(evidence);
  expect(JSON.stringify(packed).length + compactTvImportLegend.length).toBeLessThan(JSON.stringify(evidence).length);
});

test("inline compaction does not change the overview and paging for large TV packs", async () => {
  const fake = provider([plan]);
  const candidates = Array.from({ length: 17 }, (_, index) => ({ ...files[0]!, index }));
  await createTvDecider(defaultSettings(), fake.model).inspect({ entry, episodes: [episode], policy: defaultMovieQualityPolicy, offer, files: candidates });
  expect(JSON.stringify(fake.calls[0]?.input)).not.toContain("inlineEvidence");
  expect(fake.calls[0]!.tools.map(tool => tool.name)).toContain("listFiles");
});

test("TV import validation rejects invented files, ordinal IDs, and duplicate episode assignments", () => {
  expect(validateTvImportPlan(plan, files, [episode])).toEqual(plan);
  expect(() => validateTvImportPlan({ ...plan, files: [{ ...plan.files[0], fileIndex: 1 }] }, files, [episode])).toThrow("unknown");
  expect(() => validateTvImportPlan({ ...plan, files: [{ ...plan.files[0], episodeIds: [1] }] }, files, [episode])).toThrow("unknown");
  expect(() => validateTvImportPlan({ ...plan, files: [{ ...plan.files[0], episodeIds: [12, 12] }] }, files, [episode])).toThrow("duplicate");
  expect(validateTvImportPlan({ files: [], reason: "No complete requested episode." }, files, [episode]).files).toEqual([]);
});

test("pack coverage only accepts requested episode IDs backed by known playable file indexes", async () => {
  const fake = provider([
    { files: [{ fileIndex: 1, episodeIds: [episode.id] }] },
    { files: [{ fileIndex: 0, episodeIds: [999] }] },
    { files: [{ fileIndex: 0, episodeIds: [episode.id] }] },
  ]);
  const result = await createTvDecider(defaultSettings(), fake.model).matchPack!({ entry, episodes: [episode], title: offer.title,
    files: [{ path: "A Sequel - 01.mkv", size: 1234 }, { path: "description.nfo", size: 10 }] });
  expect(result).toEqual([episode.id]);
  expect(fake.calls).toHaveLength(3);
  expect(fake.calls[0]!.tools.map(tool => tool.name)).toContain("getRequestedEpisodes");
});

test("production TV feed matcher uses the shared agent and receives no private offer URLs", async () => {
  const fake = provider([{ matches: [{ targetId: 3, releaseIndexes: [0], reason: "Explicit provider season maps to the requested sequel." }] }]);
  const matches = await createTvFeedMatcher(defaultSettings(), fake.model)({
    targets: [{ targetId: 3, entry, episodes: [episode] }],
    offers: [{ title: offer.title, provider: offer.provider, sizeBytes: offer.sizeBytes, seeders: offer.seeders, attributes: [] }],
  });
  expect(matches[0]?.releaseIndexes).toEqual([0]);
  expect(fake.selectedModel()).toBe(TEXT_AGENT_MODEL);
  expect(JSON.stringify(fake.calls)).not.toContain("hidden");
  expect(JSON.stringify(fake.calls)).toContain("live-action");
});

for (const mode of ["acquire", "reuse"] as const) test(`TV feed ${mode} defaults to one Responses pass without Decisions`, async () => {
  const fake = provider([{ matches: [{ targetId: 3, releaseIndexes: [0], reason: "Plausible work identity." }] }]);
  let decisionsCalls = 0;
  const matcher = createTvFeedMatcher(defaultSettings({ agents: { apiKey: "fixture" } }), fake.model, mode, async () => {
    decisionsCalls++;
    throw new Error("The production default must bypass Decisions");
  });
  const input = { targets: [{ targetId: 3, entry, episodes: [episode] }], offers: [
    { title: offer.title, provider: offer.provider, sizeBytes: offer.sizeBytes, seeders: offer.seeders, attributes: [] },
  ] };
  expect((await matcher(input))[0]?.releaseIndexes).toEqual([0]);
  expect(fake.calls).toHaveLength(1);
  expect(decisionsCalls).toBe(0);
  expect(await matcher({ ...input, offers: [] })).toEqual([]);
  expect(await matcher({ ...input, targets: [] })).toEqual([]);
  expect(fake.calls).toHaveLength(1);
});

test("explicit TV feed replay policy uses Decisions first and sends only unresolved offers to Responses", async () => {
  const fake = provider([{ matches: [{ targetId: 3, releaseIndexes: [0], reason: "Plausible alternate numbering." }] }]);
  const settings = defaultSettings();
  settings.agents.apiKey = "fixture-key";
  const matcher = createTvFeedMatcher(settings, fake.model, "acquire", async (_url, init) => {
    const body = JSON.parse(String(init.body));
    expect(body.questions.map((q: { name: string }) => q.name)).toEqual(["release_0", "release_1", "release_2"]);
    expect(JSON.stringify(body)).toContain("An Aggregate Series");
    expect(JSON.stringify(body)).not.toContain("fixture-secret");
    return Response.json({ model: TEXT_AGENT_MODEL, answers: [
      { type: "choice", name: "release_2", choice: "review", confidence: 1 },
      { type: "choice", name: "release_0", choice: "t0", confidence: 0.98 },
      { type: "choice", name: "release_1", choice: "no_match", confidence: 0.99 },
    ] });
  }, {});
  const candidate = { title: offer.title, provider: offer.provider, sizeBytes: offer.sizeBytes, seeders: offer.seeders, attributes: [] };
  const matches = await matcher({ targets: [{ targetId: 3, entry, episodes: [episode] }], offers: [
    { ...candidate, title: "An Aggregate Series 02x01 1080p WEB-DL DD+ H.264-GROUP https://fixture.invalid/fixture-secret" },
    { ...candidate, title: "Unrelated programme" },
    { ...candidate, title: "Ambiguous release" },
  ] });
  expect(matches.map(({ releaseIndexes }) => releaseIndexes)).toEqual([[0], [2]]);
  expect(fake.calls).toHaveLength(1);
  expect(JSON.stringify(fake.calls)).toContain("Ambiguous release");
  expect(JSON.stringify(fake.calls)).not.toContain("Unrelated programme");
  expect(JSON.stringify(fake.calls)).not.toContain("fixture-secret");
});

test("a failed Decisions request is not treated as an empty TV feed", async () => {
  const fake = provider([{ matches: [{ targetId: 3, releaseIndexes: [0], reason: "Alternate numbering." }] }]);
  const settings = defaultSettings();
  settings.agents.apiKey = "fixture-key";
  const matcher = createTvFeedMatcher(settings, fake.model, "reuse", async () => new Response(null, { status: 503 }), {});
  const matches = await matcher({ targets: [{ targetId: 3, entry, episodes: [episode] }], offers: [
    { title: offer.title, provider: offer.provider, sizeBytes: offer.sizeBytes, seeders: offer.seeders, attributes: [] },
  ] });
  expect(matches[0]?.releaseIndexes).toEqual([0]);
  expect(fake.calls).toHaveLength(1);
});

test("batched Responses-only TV routing preserves release offsets and does not call Decisions", async () => {
  const fake = provider([
    { matches: [{ targetId: 3, releaseIndexes: [1], reason: "First cohort and page." }] },
    { matches: [{ targetId: 3, releaseIndexes: [0], reason: "First cohort, second page." }] },
    { matches: [] },
    { matches: [{ targetId: 15, releaseIndexes: [1], reason: "Second cohort and page." }] },
  ]);
  const settings = defaultSettings({ agents: { apiKey: "fixture" } });
  const matcher = createTvFeedMatcher(settings, fake.model, "acquire", async () => {
    throw new Error("Responses-only must not call Decisions");
  });
  const targets = Array.from({ length: 13 }, (_, i) => ({ targetId: i + 3, entry, episodes: [episode] }));
  const offers = Array.from({ length: 52 }, (_, i) => ({ title: `Offer ${i}`, provider: "Fixture", sizeBytes: 10,
    seeders: 1, attributes: [] }));
  const results = await matcher({ targets, offers });
  expect(results.map(({ targetId, releaseIndexes }) => [targetId, releaseIndexes])).toEqual([[3, [1]], [3, [50]], [15, [51]]]);
  expect(fake.calls).toHaveLength(4);
  expect(JSON.stringify(fake.calls[0]!.input)).toContain("numbering");
  expect(JSON.stringify(fake.calls[0]!.input)).not.toContain("Offer 50");
});

test("TV selector research cannot reintroduce excluded or oversized releases", async () => {
  const fake = provider([[{ type: "function_call", name: "searchReleases", callId: "search", arguments: '{"query":"A Sequel"}', status: "completed" }],
    { index: null, reason: "Only excluded or oversized sources remain." }]);
  const result = await createTvDecider(defaultSettings(), fake.model, async () => ({
    offers: [offer, { ...offer, key: "oversized", sizeBytes: 999_999 }], warnings: [],
    deferred: [{ provider: "Offline", retryAt: "2030-01-01T00:00:00.000Z" }],
  })).select({ entry, episodes: [episode], policy: defaultMovieQualityPolicy, offers: [], excludedOfferKeys: [offer.key], maximumReleaseBytes: 2000 });
  expect(result.offer).toBeNull();
  expect(result.deferred?.[0]?.provider).toBe("Offline");
  expect(JSON.stringify(fake.calls[1]?.input)).toContain("previously-attempted");
  expect(JSON.stringify(fake.calls[1]?.input)).toContain("release-size-limit");
});

test("TV agents receive the current episode scope once, not a duplicated whole-series catalog", async () => {
  const full = { ...entry, episodes: Array.from({ length: 1200 }, (_, index) => ({
    ...episode, id: 1000 + index, title: `Unrequested catalog member ${index}`,
  })) };
  const selector = provider([{ index: 0, reason: "The requested episode is present." }]);
  await createTvDecider(defaultSettings(), selector.model).select({ entry: full, episodes: [episode], policy: defaultMovieQualityPolicy, offers: [offer] });
  const verifier = provider([plan]);
  await createTvDecider(defaultSettings(), verifier.model).inspect({ entry: full, episodes: [episode], policy: defaultMovieQualityPolicy, offer, files });
  for (const fake of [selector, verifier]) {
    const text = JSON.stringify(fake.calls[0]?.input);
    expect(text).not.toContain("Unrequested catalog member");
    expect(text.match(/First of a sequel/g)).toHaveLength(1);
    expect(text).toContain("A Prequel");
    expect(text).toContain("anidb");
  }
});

test("TV expanded research candidates participate in the shared evidence tool and final semantic group", async () => {
  const offers = [{ ...offer, provider: "Tracker A" }, { ...offer, key: "same-tracker-copy", provider: " TRACKER A " }];
  const expanded = { ...offer, key: "new-tracker-copy", provider: "Tracker B", infoHash: "c".repeat(40),
    attributes: { attr: [{ name: "downloadvolumefactor", value: "0.25" }, { name: "secret", value: "hidden" }] } };
  const fake = provider([
    [{ type: "function_call", name: "searchReleases", callId: "search", arguments: '{"query":"A Sequel S02"}', status: "completed" }],
    [{ type: "function_call", name: "assessCrossSeedability", callId: "coverage", arguments: '{"indexes":[0,1,2]}', status: "completed" }],
    { index: 2, matchingIndexes: [0, 1, 2], reason: "Same episode coverage and encode; source has discounted download volume." },
  ]);
  const queries: string[] = [];
  const result = await createTvDecider(defaultSettings(), fake.model, async query => {
    queries.push(query); return { offers: [offers[0]!, expanded], warnings: [] };
  }).select({ entry, episodes: [episode], policy: defaultMovieQualityPolicy, offers });
  expect(queries).toEqual(["A Sequel S02"]);
  expect(result.offer).toBe(expanded);
  expect(result.matchingOffers).toEqual(offers);
  expect(fake.calls).toHaveLength(3);
  const input = fake.calls[2]!.input;
  if (!Array.isArray(input)) throw new Error("Expected SDK tool history");
  const assessment = input.find(item => item.type === "function_call_result" && item.name === "assessCrossSeedability");
  if (assessment?.type !== "function_call_result") throw new Error("Missing tool evidence");
  const output = typeof assessment.output === "string" ? assessment.output
    : !Array.isArray(assessment.output) && assessment.output.type === "text" ? assessment.output.text : "null";
  expect(JSON.parse(output)).toMatchObject({ distinctTrackerCount: 2,
    offers: [{ index: 0 }, { index: 1 }, { index: 2, downloadVolumeFactor: 0.25 }] });
  expect(JSON.stringify(fake.calls)).not.toContain("hidden");
  expect(fake.calls[0]!.systemInstructions).toContain("only to break ties among viable releases with equal quality and episode coverage");
});

test("ineligible optional copies cannot poison a valid TV primary selection", async () => {
  const copies = [{ ...offer, key: "excluded" }, { ...offer, key: "oversized", sizeBytes: 5000 },
    { ...offer, key: "eligible", provider: "Other" }];
  const fake = provider([{ index: 0, matchingIndexes: [0, 0, 1, 2, 3, 3, 999], reason: "Copies of the same episode release." }]);
  const result = await createTvDecider(defaultSettings(), fake.model).select({ entry, episodes: [episode],
    policy: defaultMovieQualityPolicy, offers: [offer, ...copies], excludedOfferKeys: ["excluded"], maximumReleaseBytes: 2000 });
  expect(result.offer).toBe(offer);
  expect(result.matchingOffers).toEqual([copies[2]!]);
  expect(fake.calls).toHaveLength(1);
});

test("TV primary rejection returns feedback and preserves tool research for correction", async () => {
  const fake = provider([
    [{ type: "function_call", name: "searchReleases", callId: "search", arguments: '{"query":"A Sequel"}', status: "completed" }],
    { index: 0, reason: "Attempted source." },
    { index: 999, reason: "Invented index." },
    { index: 1, matchingIndexes: [0], reason: "Eligible alternate discovered by research." },
  ]);
  let searches = 0;
  const alternate = { ...offer, key: "alternate" };
  const result = await createTvDecider(defaultSettings(), fake.model, async () => {
    searches++; return { offers: [alternate], warnings: [] };
  }).select({ entry, episodes: [episode], policy: defaultMovieQualityPolicy, offers: [offer], excludedOfferKeys: [offer.key] });
  expect(result.offer).toEqual(alternate);
  expect(result.matchingOffers).toEqual([]);
  expect(searches).toBe(1);
  expect(fake.calls).toHaveLength(4);
  const history = JSON.stringify(fake.calls[3]!.input);
  expect(history).toContain("Selection rejected");
  expect(history).toContain("searchReleases");
  expect(history).not.toContain("hidden");
});

test("TV invalid primary correction is bounded and cannot bypass policy", async () => {
  const fake = provider(Array.from({ length: 3 }, () => ({ index: 0, reason: "Still blocked." })));
  await expect(createTvDecider(defaultSettings(), fake.model).select({ entry, episodes: [episode],
    policy: defaultMovieQualityPolicy, offers: [offer], maximumReleaseBytes: 100 })).rejects.toThrow("policy-blocked");
  expect(fake.calls).toHaveLength(3);
});

test("TV selector exposes archive-only metainfo and corrects blocked primary and optional copies", async () => {
  const bytes = Buffer.from("synthetic archive bytes");
  const metainfo = bencode.encode({ info: { name: "scene.rar", length: bytes.length,
    "piece length": 16384, pieces: createHash("sha1").update(bytes).digest() } });
  const alternate = { ...offer, key: "playable-alternate" };
  const fake = provider([
    [{ type: "function_call", name: "inspectRelease", callId: "archive", arguments: '{"index":0}', status: "completed" }],
    { index: 0, reason: "Attempted archive-only primary." },
    [{ type: "function_call", name: "listReleases", callId: "blocked", arguments: '{}', status: "completed" }],
    { index: 1, matchingIndexes: [0], reason: "Choose the playable alternate instead." },
  ]);
  let requests = 0;
  const result = await createTvDecider(defaultSettings(), fake.model, undefined, async () => {
    requests++; return metainfo;
  }).select({ entry, episodes: [episode], policy: defaultMovieQualityPolicy, offers: [offer, alternate] });
  expect(result.offer).toEqual(alternate);
  expect(result.matchingOffers).toEqual([]);
  expect(requests).toBe(1);
  expect(JSON.stringify(fake.calls[1]!.input)).toContain("no-playable-video");
  expect(JSON.stringify(fake.calls[2]!.input)).toContain("Selection rejected");
  expect(JSON.stringify(fake.calls[3]!.input)).toContain("no-playable-video");
  expect(JSON.stringify(fake.calls)).not.toContain("hidden");
});

test("TV selector keeps mixed playable packs eligible despite archive sidecars", async () => {
  const bytes = Buffer.from("ab");
  const metainfo = bencode.encode({ info: { name: "mixed", files: [
    { length: 1, path: ["episode.MKV"] }, { length: 1, path: ["extras.rar"] },
  ], "piece length": 16384, pieces: createHash("sha1").update(bytes).digest() } });
  const fake = provider([
    [{ type: "function_call", name: "inspectRelease", callId: "mixed", arguments: '{"index":0}', status: "completed" }],
    { index: 0, reason: "The actual episode is a directly playable file." },
  ]);
  const result = await createTvDecider(defaultSettings(), fake.model, undefined, async () => metainfo)
    .select({ entry, episodes: [episode], policy: defaultMovieQualityPolicy, offers: [offer] });
  expect(result.offer).toEqual(offer);
  expect(JSON.stringify(fake.calls[1]!.input)).not.toContain("no-playable-video");
});

test("TV no-match selection ignores optional copies rather than failing the search", async () => {
  const fake = provider([{ index: null, matchingIndexes: [0, 999], reason: "No suitable primary." }]);
  const result = await createTvDecider(defaultSettings(), fake.model).select({ entry, episodes: [episode],
    policy: defaultMovieQualityPolicy, offers: [offer] });
  expect(result.offer).toBeNull();
  expect(result.matchingOffers).toEqual([]);
});

test("selector retrieves late candidates and episode evidence in pages and caches metainfo across pages", async () => {
  const bytes=Buffer.from("0123456789"), metainfo=bencode.encode({ announce:"https://tracker.invalid/private-passkey", info:{name:"pack",
    files:Array.from({length:10},(_,i)=>({length:1,path:[`episode-${i+1}.mkv`]})),"piece length":16384,
    pieces:createHash("sha1").update(bytes).digest()}});
  let requests=0;
  const candidates=Array.from({length:50},(_,i)=>({...offer,key:`candidate-${i}`,title:`Candidate ${i}`,
    sizeBytes:i===1?5000:1234,url:"https://fixture.invalid/private?secret=hidden"}));
  const episodes=Array.from({length:64},(_,i)=>({...episode,id:100+i,title:`Requested episode ${i}`}));
  const call=(name:string,args:unknown)=>[{type:"function_call",name,callId:name+JSON.stringify(args),arguments:JSON.stringify(args),status:"completed"}];
  const fake=provider([
    call("listReleases",{offset:10,limit:20,query:null}),
    call("getRequestedEpisodes",{offset:4,limit:10}),
    call("inspectRelease",{index:15,offset:0,limit:5}),
    call("inspectRelease",{index:15,offset:5,limit:5}),
    {index:15,reason:"Late candidate's actual file list matches requested identities."},
  ]);
  const result=await createTvDecider(defaultSettings(),fake.model,undefined,async (selected) => {
    expect(selected).toBe(candidates[15]); requests++; return metainfo;
  }).select({entry:{...entry,episodes},episodes,policy:defaultMovieQualityPolicy,
    offers:candidates,maximumReleaseBytes:2000,excludedOfferKeys:[candidates[2]!.key]});
  expect(result.offer?.key).toBe("candidate-15"); expect(requests).toBe(1);
  const initial=JSON.stringify(fake.calls[0]!.input);
  expect(initial).not.toContain('"title":"Candidate 15"');
  expect(initial).not.toContain("Requested episode 63");
  expect(initial).toContain("release-size-limit"); expect(initial).toContain("previously-attempted");
  expect(initial.length).toBeLessThan(10_000);
  expect(JSON.stringify(fake.calls[1]!.input)).toContain("Candidate 15");
  expect(JSON.stringify(fake.calls[2]!.input)).toContain("Requested episode 13");
  expect(JSON.stringify(fake.calls[3]!.input)).toContain("episode-5.mkv");
  expect(JSON.stringify(fake.calls[3]!.input)).not.toContain("episode-6.mkv");
  expect(JSON.stringify(fake.calls[4]!.input)).toContain("episode-10.mkv");
  expect(JSON.stringify(fake.calls)).not.toContain("hidden");
  expect(JSON.stringify(fake.calls)).not.toContain("private-passkey");
});

test("import verifier retrieves file/probe evidence without receiving a whole manifest or arbitrary probe tags upfront",async()=>{
  const candidates=Array.from({length:50},(_,i)=>({...files[0]!,index:i,name:`Playable file ${i}.mkv`,
    probe:{...files[0]!.probe,format:{duration:"1440",irrelevant:"x".repeat(100_000)}}}));
  const fake=provider([
    [{type:"function_call",name:"listFiles",callId:"files",arguments:'{"offset":10,"limit":10}',status:"completed"}],
    [{type:"function_call",name:"getFileProbe",callId:"probe",arguments:'{"fileIndex":15,"offset":0,"limit":10}',status:"completed"}],
    [{type:"function_call",name:"assessCandidate",callId:"quality",arguments:JSON.stringify({fileIndex:15,facts:claims}),status:"completed"}],
    {...plan,files:[{...plan.files[0]!,fileIndex:15}]},
  ]);
  await createTvDecider(defaultSettings(),fake.model).inspect({entry,episodes:[episode],policy:defaultMovieQualityPolicy,offer,files:candidates});
  expect(JSON.stringify(fake.calls[0]!.input)).not.toContain("Playable file 15");
  expect(JSON.stringify(fake.calls[1]!.input)).toContain("Playable file 15");
  const history=fake.calls[2]!.input;
  if(!Array.isArray(history)) throw new Error("Expected probe tool history");
  const probe=history.find(item=>item.type==="function_call_result"&&item.name==="getFileProbe");
  if(probe?.type!=="function_call_result"||Array.isArray(probe.output)||typeof probe.output==="string"||probe.output.type!=="text") throw new Error("Missing probe evidence");
  expect(JSON.parse(probe.output.text).format.duration).toBe("1440");
  expect(JSON.stringify(fake.calls)).not.toContain("irrelevant");
});

test("size exclusions are visible but neither selected nor retained as usable copies",async()=>{
  const fake=provider(Array.from({length:3},()=>({index:0,reason:"Attempt to ignore configured admission limit"})));
  await expect(createTvDecider(defaultSettings(),fake.model).select({entry,episodes:[episode],policy:defaultMovieQualityPolicy,
    offers:[offer],maximumReleaseBytes:1000})).rejects.toThrow("policy-blocked");
  expect(JSON.stringify(fake.calls[0]!.input)).toContain("release-size-limit");
});

test("evidence pages honor a character budget without silently dropping records",()=>{
  const values=Array.from({length:10},(_,i)=>({i,text:"x".repeat(4000)}));
  const first=evidencePage(values,0,10,value=>value);
  expect(first.items).toHaveLength(2);expect(first.total).toBe(10);expect(first.nextOffset).toBe(2);
  expect(evidencePage(values,first.nextOffset!,10,value=>value).items[0]).toEqual(values[2]);
});
