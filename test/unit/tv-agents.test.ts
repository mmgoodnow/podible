import { expect, test } from "bun:test";
import { Usage, type ModelProvider, type ModelRequest } from "@openai/agents-core";
import { createTvDecider, validateTvImportPlan } from "../../src/tv/agents";
import { defaultSettings } from "../../src/settings";
import { TEXT_AGENT_MODEL } from "../../src/ai-models";
import { createTvFeedMatcher } from "../../src/tv/feed-agent";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import type { TvEntry, TvEpisode } from "../../src/tv/types";

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

test("TV verifier uses probe policy evidence and preserves episode IDs across numbering schemes", async () => {
  const fake = provider([[{ type: "function_call", name: "assessCandidate", callId: "assess", arguments: JSON.stringify({ fileIndex: 0, facts: claims }), status: "completed" }], plan]);
  const result = await createTvDecider(defaultSettings(), fake.model).inspect({ entry, episodes: [episode], policy: defaultMovieQualityPolicy, offer, files });
  expect(result.files[0]?.episodeIds).toEqual([12]);
  expect(JSON.stringify(fake.calls[1]?.input)).toContain("eligible");
  expect(JSON.stringify(fake.calls)).not.toContain("hidden");
});

test("TV import validation rejects invented files, ordinal IDs, and duplicate episode assignments", () => {
  expect(validateTvImportPlan(plan, files, [episode])).toEqual(plan);
  expect(() => validateTvImportPlan({ ...plan, files: [{ ...plan.files[0], fileIndex: 1 }] }, files, [episode])).toThrow("unknown");
  expect(() => validateTvImportPlan({ ...plan, files: [{ ...plan.files[0], episodeIds: [1] }] }, files, [episode])).toThrow("unknown");
  expect(() => validateTvImportPlan({ ...plan, files: [{ ...plan.files[0], episodeIds: [12, 12] }] }, files, [episode])).toThrow("duplicate");
  expect(validateTvImportPlan({ files: [], reason: "No complete requested episode." }, files, [episode]).files).toEqual([]);
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

test("TV selector research cannot reintroduce excluded or oversized releases", async () => {
  const fake = provider([[{ type: "function_call", name: "searchReleases", callId: "search", arguments: '{"query":"A Sequel"}', status: "completed" }],
    { index: null, reason: "Only excluded or oversized sources remain." }]);
  const result = await createTvDecider(defaultSettings(), fake.model, async () => ({
    offers: [offer, { ...offer, key: "oversized", sizeBytes: 999_999 }], warnings: [],
    deferred: [{ provider: "Offline", retryAt: "2030-01-01T00:00:00.000Z" }],
  })).select({ entry, episodes: [episode], policy: defaultMovieQualityPolicy, offers: [], excludedOfferKeys: [offer.key], maximumReleaseBytes: 2000 });
  expect(result.offer).toBeNull();
  expect(result.deferred?.[0]?.provider).toBe("Offline");
  expect(JSON.stringify(fake.calls[1]?.input)).not.toContain('"index":0');
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
  expect(JSON.stringify(fake.calls[0]!.systemInstructions)).toContain("episode coverage always win");
});

test("TV off records semantic copies but cannot return unknown or filtered matching indexes", async () => {
  const other = { ...offer, key: "other", provider: "Other" };
  const fake = provider([
    [{ type: "function_call", name: "assessCrossSeedability", callId: "coverage", arguments: '{"indexes":[0,1]}', status: "completed" }],
    { index: 0, matchingIndexes: [1], reason: "Keep source selection independent of tracker counts." },
  ]);
  const settings = defaultSettings();
  settings.movies.crossSeedPreference = "off";
  const selected = await createTvDecider(settings, fake.model).select({ entry, episodes: [episode],
    policy: defaultMovieQualityPolicy, offers: [offer, other] });
  expect(selected.offer).toBe(offer);
  expect(selected.matchingOffers).toEqual([other]);
  expect(JSON.stringify(fake.calls[1]?.input)).toContain("off");
  for (const matchingIndexes of [[1], [0, 0]]) {
    const bad = provider([{ index: 0, matchingIndexes, reason: "Invalid matching group." }]);
    await expect(createTvDecider(defaultSettings(), bad.model).select({ entry, episodes: [episode],
      policy: defaultMovieQualityPolicy, offers: [offer, other], excludedOfferKeys: [other.key] })).rejects.toThrow();
  }
});
