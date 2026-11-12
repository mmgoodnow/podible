import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { Usage, type ModelProvider, type ModelRequest } from "@openai/agents-core";
import { agentUsagePrometheus, withAgentUsageAccounting } from "../../src/agent-usage";
import { runMigrations } from "../../src/db";
import { selectManualImportPaths, selectSearchCandidates } from "../../src/library/agents";
import type { DecisionsTransport } from "../../src/library/decisions";
import { createMovieDecider } from "../../src/movies/agents";
import { defaultMovieQualityPolicy as policy } from "../../src/movies/quality";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { createTvDecider } from "../../src/tv/agents";
import type { TvEntry, TvEpisode } from "../../src/tv/types";
import { processImportJob } from "../../src/worker/imports";
import { fixtureFacts, fixtureMovie } from "../fixtures/movie-services";

const settings = defaultSettings({ agents: { apiKey: "test-only-key" } });
const forbidden = /private-evidence|test-only-key|https?:\/\/|magnet:/;
const offer = { key: "private-evidence-id", provider: "Fixture", url: "https://private.invalid/?passkey=private-evidence", attributes: { secret: "private-evidence" },
  seeders: 3, infoHash: null, sizeBytes: 12345, title: "A.Fixture.Film.2024.1080p.WEB-DL.H264" };
const movie = { ...fixtureMovie, runtimeMinutes: 100, overview: "private-evidence overview", posterUrl: offer.url };
const episode: TvEpisode = { id: 20, key: "tvdb:220", title: "The premiere", airDate: "2024-01-01", runtimeMinutes: 24,
  refs: [], identity: { stability: "provider", source: "fixture" },
  numbering: [{ scheme: "tvdb:aired", season: 1, episode: 1, absolute: null, source: "fixture" }] };
const entry: TvEntry = { id: 3, key: "tvdb:200", title: "A Fixture Series", aliases: [], year: 2024, posterUrl: null,
  anime: false, tvdbId: 200, tmdbId: null, episodes: [episode], refs: [], relations: [] };
const tvOffer = { ...offer, title: "A.Fixture.Series.2024.S01E01.1080p.WEB-DL.H264" };
const importClaims = { identityVerified: true, cut: null, source: "web", proper: false };
const file = (title: string, duration: number) => ({ index: 7, name: `${title}.mkv`, sizeBytes: 12345, probe: {
  format: { duration: String(duration), arbitrary: "private-evidence" },
  streams: [{ codec_type: "video", codec_name: "h264", width: 1920, height: 1080 },
    { codec_type: "audio", tags: { language: "eng", arbitrary: "private-evidence" } }],
} });
const selection = { offerKey: offer.key, facts: fixtureFacts(1920), reason: "private-evidence" };
const movieSearch = { movie, offers: [offer], policy, current: null };
const movieImport = { movie, offer, files: [file(offer.title, 6000)], selection, policy };
const tvSearch = { entry, episodes: [episode], policy, offers: [tvOffer] };
const tvImport = { entry, episodes: [episode], policy, offer: tvOffer, files: [file(tvOffer.title, 1440)] };

function decisions(answers: string[] = []) {
  const questions: string[] = [];
  const choicesSeen: { id: string; title: string; preferred?: boolean }[][] = [];
  const fetcher: DecisionsTransport = async (url, init) => {
    expect(url).toBe("https://api.openai.com/v1/decisions");
    expect(new Headers(init.headers).get("authorization")).toBe("Bearer test-only-key");
    expect(String(init.body)).not.toMatch(forbidden);
    const body = JSON.parse(String(init.body));
    const [{ name, choices }] = body.questions;
    const candidates = body.input[0].content[0].data.candidates;
    expect(choices.map((choice: { value: string }) => choice.value)).toEqual([...candidates.map(({ id }: { id: string }) => id), "abstain"]);
    expect(candidates[0].id).toBe("c1");
    choicesSeen.push(candidates);
    questions.push(name);
    const choice = answers.shift() ?? "c1";
    return Response.json({ model: "gpt-6-luna", answers: [{ type: "choice", name, choice, confidence: 0.99,
      probabilities: choices.map(({ value }: { value: string }) => ({ value, probability: value === choice ? 0.99 : 0.01 / (choices.length - 1) })) }],
      usage: { input_tokens: 200, output_tokens: 0, input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 } } });
  };
  return { fetcher, questions, choicesSeen };
}

function responses(toolName: string, args: unknown, answer: unknown) {
  const requests: ModelRequest[] = [];
  const provider: ModelProvider = { getModel: () => ({
    async getResponse(request) {
      requests.push(request);
      expect(JSON.stringify(request.input)).not.toMatch(forbidden);
      if (requests.length === 1) {
        expect(request.tools.some(tool => tool.name === toolName)).toBe(true);
        return { usage: new Usage(), output: [{ type: "function_call", name: toolName, callId: "fixture-tool",
          arguments: JSON.stringify(args), status: "completed" }] };
      }
      expect(requests.length).toBe(2);
      expect(JSON.stringify(request.input)).toContain("function_call_result");
      return { usage: new Usage(), output: [{ type: "message", role: "assistant", status: "completed",
        content: [{ type: "output_text", text: JSON.stringify(answer) }] }] };
    },
    async *getStreamedResponse() { throw new Error("Unexpected streaming model"); },
  }) };
  return { provider, requests };
}

const noResponses: ModelProvider = { getModel() { throw new Error("Unexpected Responses fallback"); } };
const noMetainfo = async () => { throw new Error("Unexpected torrent inspection"); };

test("all six acquisition questions accept a screened local candidate without running Responses", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db), book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
  const mock = decisions();
  const audioOffer = { ...offer, title: "Dune Frank Herbert Audiobook M4B", mediaType: "audio" as const, guid: "private-evidence", leechers: 0, raw: {} };
  await withAgentUsageAccounting(repo, async () => {
    const searched = await selectSearchCandidates(settings, { media: "audio", book, query: "Dune Frank Herbert", results: [audioOffer] }, {
      repo, modelProvider: noResponses, preferSearchAgent: true, freshAcquisitionDecisions: { fetch: mock.fetcher, minConfidence: 0.95, minMargin: 0.5 },
    });
    expect(searched.selections[0]?.parts).toEqual([audioOffer]);
    const bookFile = { sourcePath: "/private-evidence/Dune Frank Herbert.m4b", relativePath: "Dune Frank Herbert.m4b", ext: ".m4b",
      size: 123, mtimeMs: 0, supportedAudio: true, supportedEbook: false };
    const imported = await selectManualImportPaths(settings, { mediaType: "audio", book, files: [bookFile], releaseTitle: audioOffer.title }, {
      modelProvider: noResponses, freshImportDecisions: mock.fetcher,
    });
    expect(imported.selectedPaths).toEqual([bookFile.sourcePath]);
    const movies = createMovieDecider(settings, noResponses, noMetainfo, mock.fetcher);
    expect(await movies.select(movieSearch)).toMatchObject({ offerKey: offer.key, facts: { source: "web", height: 1080 }, matchingOffers: [] });
    expect(await movies.inspect(movieImport)).toMatchObject({ fileIndex: 7, facts: { identityVerified: true, source: "web", cut: null } });
    const tv = createTvDecider(settings, noResponses, undefined, noMetainfo, mock.fetcher);
    expect((await tv.select(tvSearch)).offer).toBe(tvOffer);
    expect((await tv.inspect(tvImport)).files[0]).toMatchObject({ fileIndex: 7, episodeIds: [episode.id], facts: { identityVerified: true } });
  });
  expect(mock.questions).toEqual(["book_release", "book_import", "movie_release", "movie_import", "tv_release", "tv_import"]);
  const metrics = agentUsagePrometheus(repo);
  for (const question of mock.questions) expect(metrics).toContain(`podible_agent_acquisition_decisions_total{question="${question}",outcome="accepted"} 1`);
  expect(metrics).toMatch(/podible_agent_model_call_seconds_total\{workflow="tv",api="decisions"/);
  expect(metrics).not.toContain("private-evidence");
});

test("abstention escalates to real local tool execution in each new review path", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const mock = decisions(["abstain", "abstain", "abstain", "abstain", "abstain"]);
  const book = { id: 4, title: "Dune", author: "Frank Herbert" };
  const bookFiles = [{ sourcePath: "/private-evidence/Dune Frank Herbert.epub", relativePath: "Dune Frank Herbert.epub", ext: ".epub",
    size: 123, mtimeMs: 0, supportedAudio: false, supportedEbook: true }];
  const bookAgent = responses("listImportFiles", { offset: 0, limit: 1 }, { selectedIndices: [], confidence: 0.8, reason: "Do not import" });
  await withAgentUsageAccounting(repo, async () => {
    expect((await selectManualImportPaths(settings, { mediaType: "ebook", book, files: bookFiles },
      { modelProvider: bookAgent.provider, freshImportDecisions: mock.fetcher })).selectedPaths).toEqual([]);
    const movieSelector = responses("listReleases", { offset: 0, limit: 1 }, { index: null, facts: fixtureFacts(), reason: "No eligible release" });
    const movieVerifier = responses("getFileProbe", { fileIndex: 7, offset: 0, limit: 2 }, { fileIndex: null, facts: importClaims, reason: "No eligible feature" });
    expect((await createMovieDecider(settings, movieSelector.provider, noMetainfo, mock.fetcher).select(movieSearch)).offerKey).toBeNull();
    expect((await createMovieDecider(settings, movieVerifier.provider, noMetainfo, mock.fetcher).inspect(movieImport)).fileIndex).toBeNull();
    const tvSelector = responses("getRequestedEpisodes", { offset: 0, limit: 1 }, { index: null, reason: "No eligible release" });
    const tvVerifier = responses("getFileProbe", { fileIndex: 7, offset: 0, limit: 2 }, { files: [], reason: "No safe episode mapping" });
    expect((await createTvDecider(settings, tvSelector.provider, undefined, noMetainfo, mock.fetcher).select(tvSearch)).offer).toBeNull();
    expect((await createTvDecider(settings, tvVerifier.provider, undefined, noMetainfo, mock.fetcher).inspect(tvImport)).files).toEqual([]);
  });
  expect(mock.questions).toEqual(["book_import", "movie_release", "movie_import", "tv_release", "tv_import"]);
  for (const question of mock.questions) expect(agentUsagePrometheus(repo)).toContain(`podible_agent_acquisition_decisions_total{question="${question}",outcome="escalated"} 1`);
});

test("hard gates keep ambiguous or incompatible inputs out of the Decisions fast path", async () => {
  const mock = decisions();
  for (const input of [
    { ...movieSearch, current: fixtureFacts() }, { ...movieSearch, allowDecisions: false },
    { ...movieSearch, offers: [offer, { ...offer, key: "another" }] }, { ...movieSearch, offers: [{ ...offer, seeders: 0 }] },
    { ...movieSearch, policy: { ...policy, cut: "theatrical" } }, { ...movieSearch, offers: [{ ...offer, title: `${offer.title}.Sample` }] },
  ]) {
    const agent = responses("listReleases", { offset: 0, limit: 1 }, { index: null, facts: fixtureFacts(), reason: "Rejected" });
    expect((await createMovieDecider(settings, agent.provider, noMetainfo, mock.fetcher).select(input)).offerKey).toBeNull();
  }
  for (const input of [
    { ...movieImport, files: [file(offer.title, 360)] }, { ...movieImport, files: [file(offer.title, 6000), { ...file(offer.title, 6000), index: 8 }] },
    { ...movieImport, policy: { ...policy, audioLanguages: ["ja"] } },
    { ...movieImport, files: [file(offer.title.replace("H264", "HEVC"), 6000)] },
  ]) {
    const agent = responses("listFiles", { offset: 0, limit: 2 }, { fileIndex: null, facts: importClaims, reason: "Rejected" });
    expect((await createMovieDecider(settings, agent.provider, noMetainfo, mock.fetcher).inspect(input)).fileIndex).toBeNull();
  }
  for (const input of [
    { ...tvSearch, maximumReleaseBytes: 1 }, { ...tvSearch, excludedOfferKeys: [tvOffer.key] },
    { ...tvSearch, entry: { ...entry, anime: true } },
    { ...tvSearch, offers: [{ ...tvOffer, title: tvOffer.title.replace("S01E01", "S01E02") }] },
    { ...tvSearch, episodes: [episode, { ...episode, id: 21 }] },
  ]) {
    const agent = responses("getRequestedEpisodes", { offset: 0, limit: 2 }, { index: null, reason: "Rejected" });
    expect((await createTvDecider(settings, agent.provider, undefined, noMetainfo, mock.fetcher).select(input)).offer).toBeNull();
  }
  expect(mock.questions).toEqual([]);
});

test("multiple screened video releases use Decisions without letting it override tier or preferences", async () => {
  const movieLow = { ...offer, key: "movie-low", title: offer.title.replace("1080p", "720p") };
  const tvLow = { ...tvOffer, key: "tv-low", title: tvOffer.title.replace("1080p", "720p") };
  const movieInput = { ...movieSearch, offers: [movieLow, offer] };
  const tvInput = { ...tvSearch, offers: [tvOffer, tvLow] };
  const mock = decisions(["c2", "c1", "c1"]);
  expect((await createMovieDecider(settings, noResponses, noMetainfo, mock.fetcher).select(movieInput)).offerKey).toBe(offer.key);
  expect((await createTvDecider(settings, noResponses, undefined, noMetainfo, mock.fetcher).select(tvInput)).offer).toBe(tvOffer);
  expect(mock.choicesSeen.map(set => set.map(candidate => candidate.preferred))).toEqual([[false, true], [true, false]]);
  const escalation = responses("listReleases", { offset: 0, limit: 2 }, { index: 1, facts: fixtureFacts(1920), reason: "Higher policy tier" });
  expect((await createMovieDecider(settings, escalation.provider, noMetainfo, mock.fetcher).select(movieInput)).offerKey).toBe(offer.key);
  expect(escalation.requests).toHaveLength(2);
  expect(mock.questions).toEqual(["movie_release", "tv_release", "movie_release"]);
});

test("the bounded video cohort accepts twelve screened choices, but not thirteen", async () => {
  const offers = Array.from({ length: 11 }, (_, index) => ({ ...offer, key: `normal-${index}`, sizeBytes: 12345 + index }));
  const proper = { ...offer, key: "proper", title: `${offer.title}.PROPER` };
  const mock = decisions(["c12"]);
  const decider = createMovieDecider(settings, noResponses, noMetainfo, mock.fetcher);
  expect((await decider.select({ ...movieSearch, offers: [...offers, proper] })).offerKey).toBe(proper.key);
  expect(mock.choicesSeen[0]).toHaveLength(12);
  expect(mock.choicesSeen[0]?.filter(candidate => candidate.preferred)).toHaveLength(1);
  const agent = responses("listReleases", { offset: 0, limit: 2 }, { index: 11, facts: fixtureFacts(1920), reason: "Proper release" });
  expect((await createMovieDecider(settings, agent.provider, noMetainfo, mock.fetcher).select({ ...movieSearch,
    offers: [...offers, proper, { ...offer, key: "thirteenth" }] })).offerKey).toBe(proper.key);
  expect(mock.questions).toEqual(["movie_release"]);
});

test("a reviewed managed book import cannot fall through to deterministic publish or replay an unowned path", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db), book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
  const release = repo.createRelease({ bookId: book.id, provider: "fixture", title: "Dune Frank Herbert EPUB", mediaType: "ebook",
    url: offer.url, infoHash: "a".repeat(40) });
  const job = repo.createJob({ type: "import", bookId: book.id, releaseId: release.id, payload: { preferAgentFirst: true } });
  for (const [selectedPaths, reason] of [[[], "did not confirm a safe file"], [["/unowned/book.epub"], "outside the owned torrent"]] as const) {
    const steps: string[] = [];
    await expect(processImportJob({ repo, getSettings: () => settings, modelProvider: noResponses, onLog() {}, execution: {
      key: "fixture", assertActive() {}, async sleep() { throw new Error("Unexpected wait"); }, async run<T>(name: string) {
        steps.push(name);
        if (name === "import:source") return { freshDecisionReview: true, basePath: "/owned", selectedPaths: ["/owned/book.epub"] } as T;
        if (name === "import:agent-first:inspect") return [] as T;
        if (name === "import:agent-first:select") return { selectedPaths, mode: "agent", reason: "Fixture decision" } as T;
        throw new Error(`Unexpected effect: ${name}`);
      },
    } }, job)).rejects.toThrow(reason);
    expect(steps).toEqual(["import:source", "import:agent-first:inspect", "import:agent-first:select"]);
  }
  expect(repo.findAssetByImportKey(`import:${job.id}`)).toBeNull();
});
