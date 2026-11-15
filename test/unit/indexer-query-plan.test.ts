import { expect, test } from "bun:test";
import { Usage, type ModelProvider } from "@openai/agents-core";
import { planIndexerQuery, tvSearchEvidence } from "../../src/media/indexer-query-plan";
import type { TvEpisode } from "../../src/tv/types";
import { defaultSettings } from "../../src/settings";

const evidence = { media: "tv" as const, query: "Example S02", title: "Example", aliases: ["Rei"],
  numbering: [["tvdb:aired", 2, null, null]] as [string, number, null, null][] };
function fixture(output: unknown) {
  let calls = 0;
  const provider: ModelProvider = {
    getModel: () => ({
      async getResponse(request) {
        calls++;
        expect(JSON.stringify(request.input)).toContain("Rei");
        expect(JSON.stringify(request.input)).toContain("tvdb:aired");
        expect(request.systemInstructions).toContain("[scheme, season, episode, absolute]");
        return { usage: new Usage(), output: [{ type: "message", role: "assistant", status: "completed",
          content: [{ type: "output_text", text: JSON.stringify(output) }] }] };
      },
      async *getStreamedResponse() { throw new Error("Unexpected streaming response"); },
    }),
  };
  return { provider, calls: () => calls };
}

test("numbering rows retain distinct alternatives and leave provider ID coordinates intact", () => {
  const episode = { numbering: [
    { scheme: "tvdb:aired", season: 2, episode: 1, absolute: null },
    { scheme: "tmdb:aired", season: 1, episode: 13, absolute: null },
    { scheme: "anidb:absolute", season: null, episode: null, absolute: 13 },
  ] } as TvEpisode;
  const { evidence, options } = tvSearchEvidence({ title: "Example", tvdbId: 42, tmdbId: 8 }, [episode]);
  expect(evidence.numbering).toEqual([["tvdb:aired", 2, 1, null], ["tmdb:aired", 1, 13, null], ["anidb:absolute", null, null, 13]]);
  expect(options).toMatchObject({ ids: { tvdbid: "42", tmdbid: "8" },
    coordinates: { tvdbid: { season: 2, episode: 1 }, tmdbid: { season: 1, episode: 13 } } });
  expect(tvSearchEvidence({ title: "Example" }, [episode, episode]).evidence.numbering).toEqual(evidence.numbering!);
});

test("query planner shares a bounded text-only plan across provider searches", async () => {
  const expected = { primaryQuery: "Rei S02", alternateQuery: "Example S02" };
  const f = fixture(expected);
  const plan = () => planIndexerQuery(defaultSettings(), evidence, f.provider);
  expect(await Promise.all([plan(), plan()])).toEqual([expected, expected]);
  expect(f.calls()).toBe(1);
});

test("query planner cannot supply an invented ID or prevent a fallback search", async () => {
  const f = fixture({ primaryQuery: "Rei S02", alternateQuery: null, tvdbid: "123" });
  expect(await planIndexerQuery(defaultSettings(), evidence, f.provider)).toEqual({ primaryQuery: evidence.query, alternateQuery: null });
  expect(f.calls()).toBe(1);
});
