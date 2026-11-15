import { Database } from "bun:sqlite";
import { agentUsagePrometheus, withAgentUsageAccounting } from "../../src/agent-usage";
import { runMigrations } from "../../src/db";
import { factsFromMovieProbe } from "../../src/movies/probe";
import { defaultMovieQualityPolicy, evaluateMovieQuality } from "../../src/movies/quality";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { createTvDecider, type TvDecider, type TvFileCandidate } from "../../src/tv/agents";
import { compactTvImportEvidence, compactTvImportLegend, inlineTvImportEvidence } from "../../src/tv/agent-evidence";
import type { TvEntry, TvEpisode } from "../../src/tv/types";

type Input = Parameters<TvDecider["inspect"]>[0];
type Fixture = { id: string; input: Input; expected: [number, number][] };
type Member = { name: string; minutes?: number; ids: number[]; audio?: string };
const ep = (n: number, title = `Episode ${n}`, season = 1): TvEpisode => ({
  id: 110 + n, key: `tvdb:${210 + n}`, title, runtimeMinutes: 24, airDate: null, refs: [],
  identity: { stability: "provider", source: "tvdb" },
  numbering: [{ scheme: "tvdb:aired", season, episode: n, absolute: null, source: "tvdb" }],
});
const member = (name: string, ...numbers: number[]): Member => ({ name, ids: numbers.map(n => 110 + n) });
const show = "Beacon Harbor";
function fixture(id: string, episodes: TvEpisode[], members: Member[], overrides: Partial<TvEntry> = {}, title = show): Fixture {
  const entry: TvEntry = { id: 7, key: "tvdb:100", title, aliases: [], year: 2021, anime: false,
    tvdbId: 100, tmdbId: 300, posterUrl: null, episodes, relations: [], refs: [], ...overrides };
  const files: TvFileCandidate[] = members.map(({ name, minutes = 24, audio = "eng" }, i) => ({
    index: 2 * i + 1, name: `${name}.1080p.WEB-DL.H264.mkv`, sizeBytes: minutes * 45_000_000,
    probe: { format: { duration: String(60 * minutes) }, streams: [
      { codec_type: "video", codec_name: "h264", width: 1920, height: 1080, color_transfer: "bt709" },
      { codec_type: "audio", codec_name: "eac3", tags: { language: audio } },
      { codec_type: "subtitle", codec_name: "subrip", tags: { language: "eng" } },
      { codec_type: "subtitle", codec_name: "subrip", tags: { language: "spa" } },
    ] },
  }));
  return { id, expected: members.flatMap(({ ids }, i) => ids.map(episode => [files[i]!.index, episode])),
    input: { entry, episodes, files, offer: { title: `${title} 1080p WEB-DL H264`, key: "fixture", provider: "fixture" },
      policy: defaultMovieQualityPolicy, allowDecisions: false } };
}
const ordinary = Array.from({ length: 8 }, (_, i) => ep(i + 1));
const numbered = (n: number) => `${show}.S01E${String(n).padStart(2, "0")}`;
const anime = [ep(1, "The Red Tower", 2), ep(2, "Silver Clouds", 2), ep(3, "Homeward", 2)].map((episode, i) => ({
  ...episode, numbering: [...episode.numbering,
    { scheme: "anidb:local", season: 1, episode: i + 1, absolute: null, source: "anime-map" },
    { scheme: "tvdb:absolute", season: null, episode: null, absolute: 25 + i, source: "tvdb" }],
}));
const fixtures = [
  fixture("ordinary-shuffled-pack", ordinary, [4, 1, 8, 6, 2, 7, 5, 3].map(n => member(numbered(n), n))),
  fixture("anime-mixed-numbering", anime, [
    { ...member("Hoshi no Minato 26 Silver Clouds", 2), audio: "jpn" },
    { ...member("Hoshi no Minato S01E03 Homeward", 3), audio: "jpn" },
    { ...member("Beacon Harbor S02E01 The Red Tower", 1), audio: "jpn" },
    { ...member("Beacon Harbor 25 The Red Tower SAMPLE"), minutes: 1, audio: "jpn" },
  ], { anime: true, aliases: ["Hoshi no Minato"], relations: [{ key: "anidb:200", title: "The First Harbor", relation: "PREQUEL", source: "fixture" }] }),
  fixture("combined-and-unrequested", ordinary.slice(0, 5), [
    { ...member(`${show}.S01E01-E02`, 1, 2), minutes: 48 },
    member(numbered(3), 3),
    { ...member(`${show}.S01E05-E06`), minutes: 48 },
    member(`${show}.Behind.The.Scenes.Featurette`),
    member(numbered(4), 4),
  ]),
  fixture("partial-pack-with-sample", ordinary.slice(0, 5), [
    { ...member(`${numbered(1)}.SAMPLE`), minutes: 1 },
    member(numbered(2), 2), member(numbered(4), 4),
    { ...member(`${numbered(5)}.First.Three.Minutes`), minutes: 3 },
  ]),
  fixture("wrong-series", ordinary.slice(0, 4), [
    ...[1, 2, 3, 4].map(n => member(`Rocket Town.S01E0${n}`)),
  ]),
];

async function replay(fixture: Fixture, format: "compact" | "objects", apiKey: string) {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const verifier = createTvDecider(defaultSettings({ agents: { apiKey } }), undefined, undefined, undefined, undefined, format);
  const start = performance.now();
  const plan = await withAgentUsageAccounting(repo, () => verifier.inspect(fixture.input));
  const ms = Math.round(performance.now() - start);
  const metrics = agentUsagePrometheus(repo).split("\n");
  const count = (name: string) => metrics.filter(line => line.startsWith(`podible_agent_${name}{workflow="tv",api="responses",`))
    .reduce((sum, line) => sum + Number(line.split(" ").at(-1)), 0);
  const actual = plan.files.flatMap(file => {
    const probe = fixture.input.files.find(candidate => candidate.index === file.fileIndex)!.probe;
    if (evaluateMovieQuality(fixture.input.policy, factsFromMovieProbe(probe, file.facts)).eligibility !== "eligible") return [];
    return file.episodeIds.map(id => `${file.fileIndex}:${id}`);
  });
  const expected = fixture.expected.map(([file, episode]) => `${file}:${episode}`);
  return { ms, expected: expected.length, misses: expected.filter(pair => !actual.includes(pair)),
    extras: actual.filter(pair => !expected.includes(pair)),
    unexpectedAssignments: plan.files.flatMap(file => file.episodeIds.map(id => `${file.fileIndex}:${id}`)).filter(pair => !expected.includes(pair)),
    input: count("input_tokens_total"), output: count("output_tokens_total"), calls: count("model_calls_total"),
    missingUsage: count("usage_missing_total"), cached: count("cached_input_tokens_total") };
}

async function main() {
  const apiKey = process.env.OPENAI_API_KEY?.trim();
  if (!apiKey) throw new Error("OPENAI_API_KEY is required for live replay");
  const repeats = Number(process.argv[2] ?? 1);
  if (!Number.isInteger(repeats) || repeats < 1 || repeats > 5) throw new Error("Expected repeat count 1-5");
  let failed = false;
  for (let run = 0; run < repeats; run++) {
    let next = 0;
    await Promise.all(Array.from({ length: 2 }, async () => {
      while (next < fixtures.length) {
        const index = next++, fixture = fixtures[index]!;
        const evidence = inlineTvImportEvidence(fixture.input.files, fixture.input.episodes)!;
        const delta = Buffer.byteLength(JSON.stringify(evidence)) -
          Buffer.byteLength(JSON.stringify(compactTvImportEvidence(evidence))) - Buffer.byteLength(compactTvImportLegend);
        if (delta <= 0) throw new Error("Fixture does not exercise compact evidence");
        const order = (index + run) % 2 === 0 ? ["objects", "compact"] as const : ["compact", "objects"] as const;
        for (const format of order) {
          const label = { case: fixture.id, run: run + 1, format, inlineBytesSaved: format === "compact" ? delta : 0 };
          try { console.log(JSON.stringify({ ...label, ...await replay(fixture, format, apiKey) })); }
          catch { failed = true; console.log(JSON.stringify({ ...label, error: "replay_failed" })); }
        }
      }
    }));
  }
  process.exitCode = failed ? 1 : 0;
}

if (import.meta.main) await main();
