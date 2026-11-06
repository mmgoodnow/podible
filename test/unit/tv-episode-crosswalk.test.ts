import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { Usage, type ModelProvider, type ModelRequest } from "@openai/agents-core";
import { BooksRepo } from "../../src/repo";
import { runMigrations } from "../../src/db";
import { mapRequestedEpisodes, validateEpisodeMappingDecisions, type EpisodeMappingResearch } from "../../src/tv/episode-crosswalk";
import { createEpisodeMappingResearch } from "../../src/tv/episode-mapping-agent";
import { defaultSettings } from "../../src/settings";
import { TEXT_AGENT_MODEL } from "../../src/ai-models";
import type { TvCatalogEntry } from "../../src/tv/types";
import type { SeerrTvSeason } from "../../src/tv/upstream";

function fixture() {
  const entry: TvCatalogEntry = { key: "tvdb:72860", tmdbId: 340782, tvdbId: 72860, title: "Tom and Jerry", aliases: [], year: 1940,
    anime: false, source: "fixture", relations: [], episodes: [
      { key: "tvdb:1", title: "Puss Gets the Boot", airDate: "1940-02-10", runtimeMinutes: 9, numbering: [{ scheme: "tvdb:aired", season: 1940, episode: 1, absolute: null }] },
      { key: "tvdb:2", title: "The Yankee Doodle Mouse", airDate: "1943-06-26", runtimeMinutes: 7, numbering: [{ scheme: "tvdb:aired", season: 1940, episode: 2, absolute: null }] },
      { key: "tvdb:3", title: "The Hollywood Bowl", airDate: "1950-09-16", runtimeMinutes: 7, numbering: [{ scheme: "tvdb:aired", season: 1950, episode: 1, absolute: null }] },
      { key: "tvdb:4", title: "Special", airDate: "1940-02-10", runtimeMinutes: 9, numbering: [{ scheme: "tvdb:aired", season: 0, episode: 1, absolute: null }] },
    ] };
  const metadata: SeerrTvSeason[] = [{ tmdbId: 340782, seasonNumber: 1, episodes: entry.episodes.slice(0, 3).map((ep, i) => ({
    id: i + 101, title: i === 1 ? "Yankee Doodle Mouse" : i === 2 ? "Tom and Jerry in the Hollywood Bowl" : ep.title,
    airDate: ep.airDate, runtimeMinutes: ep.runtimeMinutes, seasonNumber: 1, episodeNumber: i + 1, overview: "", tvdbId: null,
  })) }];
  const calls: Parameters<EpisodeMappingResearch>[0][] = [];
  const research: EpisodeMappingResearch = async input => { calls.push(input); return input.sources.map(ep => ({ sourceId: ep.id,
    targetKey: `tvdb:${ep.id - 100}`, reason: "Distinct title variant with matching original-release date within this work." })); };
  return { entry, metadata, calls, research, tmdbId: 340782, seasons: [1] };
}

test("many-to-many season membership maps actual episode identities and persists evidence without changing canonical publication numbers", async () => {
  const f = fixture(), result = await mapRequestedEpisodes(f);
  expect(result.selectedKeys).toEqual(["tvdb:1", "tvdb:2", "tvdb:3"]);
  expect(f.calls[0]!.sources.map(ep => ep.id)).toEqual([102, 103]);
  expect(result.entry.episodes[0]!.correspondences![0]!.method).toBe("title_date");
  expect(result.entry.episodes[1]!.correspondences![0]!.method).toBe("agent");
  expect(result.entry.episodes[1]!.correspondences![0]!.model).toBe(TEXT_AGENT_MODEL);
  expect(result.entry.episodes[2]!.numbering).toMatchObject([
    { scheme: "tvdb:aired", season: 1950, episode: 1 }, { scheme: "tmdb:aired", season: 1, episode: 3 },
  ]);
  const db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db);
  try {
    const saved = repo.tv.upsertEntry(result.entry);
    repo.tv.upsertEntry(f.entry); // A raw provider refresh must not erase semantic evidence.
    const previous = repo.tv.entry(saved.id)!.episodes;
    expect(previous[1]!.correspondences).toEqual(result.entry.episodes[1]!.correspondences);
    expect(previous[0]!.airDate).toBe("1940-02-10");
    f.calls.length = 0;
    expect((await mapRequestedEpisodes({ ...f, previous })).selectedKeys).toEqual(result.selectedKeys);
    expect(f.calls).toHaveLength(0);
    f.metadata[0]!.episodes[1]!.airDate = "1943-05-01";
    await mapRequestedEpisodes({ ...f, previous });
    expect(f.calls[0]!.sources.map(ep => ep.id)).toEqual([102]);
    const blank = { ...f.entry, episodes: [{ ...f.entry.episodes[0]!, airDate: null, runtimeMinutes: null }] };
    expect(repo.tv.upsertEntry(blank).episodes[0]!.airDate).toBeNull();
    expect(repo.tv.upsertEntry(blank).episodes[0]!.runtimeMinutes).toBeNull();
  } finally { db.close(); }
});

test("date conflicts and duplicate titles go to research, never ordinal or equal-count matching", async () => {
  const f = fixture();
  f.entry.episodes[0]!.airDate = "1940-03-10";
  await mapRequestedEpisodes(f);
  expect(f.calls[0]!.sources).toHaveLength(3);
  f.entry.episodes[0]!.airDate = "1940-02-10";
  f.entry.episodes[3]!.title = "Puss Gets the Boot";
  f.calls.length = 0;
  await mapRequestedEpisodes(f);
  expect(f.calls[0]!.sources.map(ep => ep.id)).toContain(101);
  const initial = fixture(), saved = await mapRequestedEpisodes(initial);
  initial.calls.length = 0;
  initial.entry.episodes.push({ ...initial.entry.episodes[0]!, key: "tvdb:55" });
  await mapRequestedEpisodes({ ...initial, previous: saved.entry.episodes });
  expect(initial.calls[0]!.sources.map(ep => ep.id)).toEqual([101, 102, 103]);
});

test("mapping rejects incomplete approvals, wrong work IDs, explicit-ID contradictions and duplicate assignments", async () => {
  const f = fixture();
  await expect(mapRequestedEpisodes({ ...f, seasons: [1, 2] })).rejects.toThrow("missing or conflicting");
  await expect(mapRequestedEpisodes({ ...f, tmdbId: 99 })).rejects.toThrow("work identity");
  await expect(mapRequestedEpisodes({ ...f, metadata: [{ ...f.metadata[0]!, tmdbId: 99 }] })).rejects.toThrow("missing or conflicting");
  const sources = f.metadata[0]!.episodes;
  expect(() => validateEpisodeMappingDecisions([{ sourceId: 101, targetKey: "tvdb:1", reason: "test" }], sources, f.entry.episodes)).toThrow("omitted");
  expect(() => validateEpisodeMappingDecisions(sources.map(ep => ({ sourceId: ep.id, targetKey: "tvdb:1", reason: "test" })), sources, f.entry.episodes)).toThrow("duplicate");
  sources[0]!.tvdbId = 99;
  await expect(mapRequestedEpisodes(f)).rejects.toThrow("absent from canonical");
  expect(() => validateEpisodeMappingDecisions([{ sourceId: 101, targetKey: "tvdb:1", reason: "test" }], [sources[0]!], f.entry.episodes)).toThrow("contradicts");
  sources[0]!.tvdbId = null;
  await expect(mapRequestedEpisodes({ ...f, research: async input => input.sources.map(ep => ({ sourceId: ep.id, targetKey: null, reason: "Metadata is ambiguous." })) })).rejects.toThrow("could not resolve");
});

test("research is bounded to twelve unresolved episodes and sees candidates through tools rather than initial context", async () => {
  const f = fixture();
  f.entry.episodes = Array.from({ length: 31 }, (_, i) => ({ ...f.entry.episodes[0]!, key: `tvdb:${i + 1}`, title: `Candidate ${i + 1}` }));
  f.metadata[0]!.episodes = Array.from({ length: 31 }, (_, i) => ({ ...f.metadata[0]!.episodes[0]!, id: i + 101, episodeNumber: i + 1, title: `Source ${i + 1}` }));
  const result = await mapRequestedEpisodes(f);
  expect(f.calls.map(call => call.sources.length)).toEqual([12, 12, 7]);
  expect(result.selectedKeys).toHaveLength(31);
});

test("the real SDK loop returns rejected submissions for correction and requires inspected pairs before finishing", async () => {
  const f = fixture(), requests: ModelRequest[] = [];
  const call = (name: string, args: unknown) => [{ type: "function_call" as const, name, callId: `call-${requests.length}`, arguments: JSON.stringify(args), status: "completed" as const }];
  const decisions = [{ sourceId: 102, targetKey: "tvdb:2", reason: "Article variant, same distinctive title and date." }];
  const model: ModelProvider = { getModel(name) { expect(name).toBe(TEXT_AGENT_MODEL); return {
    async getResponse(request) {
      requests.push(request);
      const output = requests.length === 1 ? call("submitEpisodeMappings", { decisions })
        : requests.length === 2 ? call("inspectEpisodePair", { sourceId: 102, targetKey: "tvdb:2" })
        : call("submitEpisodeMappings", { decisions });
      return { usage: new Usage(), output };
    }, async *getStreamedResponse() { throw new Error("unused"); },
  }; } };
  expect(await createEpisodeMappingResearch(defaultSettings(), model)({ entry: f.entry, sources: [f.metadata[0]!.episodes[1]!], candidates: f.entry.episodes })).toEqual(decisions);
  expect(requests).toHaveLength(3);
  expect(JSON.stringify(requests[0]!.input)).not.toContain("Hollywood");
  expect(JSON.stringify(requests[1]!.input)).toContain("Inspect each proposed episode pair");
});
