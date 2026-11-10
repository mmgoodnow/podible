import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import bencode from "bencode";
import type { Model, ModelProvider, ModelRequest, ModelResponse } from "@openai/agents-core";
import { Usage } from "@openai/agents-core";

import { selectManualImportPaths, selectSearchCandidates } from "../../src/library/agents";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings, parseSettings } from "../../src/settings";
import { torrentCacheKeyFor } from "../../src/library/torrent-cache";
import type { TorznabResult } from "../../src/library/torznab";

function makeMultiFileTorrentBytes(rootName: string, files: Array<{ path: string[]; length: number }>): Uint8Array {
  return bencode.encode({
    announce: new TextEncoder().encode("http://tracker/announce"),
    info: {
      name: new TextEncoder().encode(rootName),
      "piece length": 16384,
      pieces: new Uint8Array(20),
      files: files.map((file) => ({
        length: file.length,
        path: file.path.map((part) => new TextEncoder().encode(part)),
      })),
    },
  });
}

function agentSettings(overrides?: Partial<ReturnType<typeof defaultSettings>["agents"]>) {
  return {
    ...defaultSettings().agents,
    apiKey: "test-key",
    ...overrides,
  };
}

/** Build an internal ModelResponse with a plain text assistant message. */
function textModelResponse(text: string): ModelResponse {
  return {
    usage: new Usage(),
    output: [
      {
        type: "message",
        role: "assistant",
        status: "completed",
        content: [{ type: "output_text", text }],
      },
    ],
  };
}

/** Build an internal ModelResponse with function_call outputs (no text). */
function functionCallModelResponse(calls: Array<{ callId: string; name: string; arguments: string }>): ModelResponse {
  return {
    usage: new Usage(),
    output: calls.map((c) => ({
      type: "function_call" as const,
      callId: c.callId,
      name: c.name,
      arguments: c.arguments,
      status: "completed" as const,
    })),
  };
}

/**
 * A fake ModelProvider that drives tests with a scripted sequence of ModelResponses.
 * Each call to getResponse() pops the next response from the queue.
 * Captures each ModelRequest for assertion.
 */
class FakeModelProvider implements ModelProvider {
  readonly requests: ModelRequest[] = [];
  private readonly responses: ModelResponse[];

  constructor(responses: ModelResponse[]) {
    this.responses = [...responses];
  }

  getModel(_modelName?: string): Model {
    return {
      getResponse: async (request: ModelRequest): Promise<ModelResponse> => {
        this.requests.push(request);
        const response = this.responses.shift();
        if (!response) throw new Error("FakeModelProvider: no more scripted responses");
        return response;
      },
      getStreamedResponse: async function* () {
        throw new Error("FakeModelProvider: streaming not supported");
      },
    };
  }
}

describe("agent decisions", () => {
  test("book selector resolves equivalent copies per ordered part from server evidence", async () => {
    const results: TorznabResult[] = [0, 1, 2, 3].map(index => ({
      title: "Dune Frank Herbert [MP3]", provider: `provider-${index}`, mediaType: "audio",
      sizeBytes: 1000, url: `https://example.com/${index}`, guid: `g${index}`, infoHash: null,
      seeders: 10 - index, leechers: 0, raw: {},
    }));
    const provider = new FakeModelProvider([textModelResponse(JSON.stringify({
      selections: [{ parts: [2, 0], matchingIndexes: [[3], [1]] }], confidence: 0.9, reason: "Exact copies of each ordered part",
    }))]);
    const decision = await selectSearchCandidates(defaultSettings({ agents: agentSettings() }), {
      query: "Dune Frank Herbert", media: "audio", results, forceAgent: true,
    }, { modelProvider: provider });
    expect(decision.selections[0]!.parts).toEqual([results[2]!, results[0]!]);
    expect(decision.selections[0]!.matchingCopies).toEqual([[results[3]!], [results[1]!]]);
    expect(JSON.stringify(provider.requests[0])).toContain("same text/translation");
  });

  test("book selector rejects fabricated, selected, repeated or misaligned copy indexes", async () => {
    const results: TorznabResult[] = [0, 1, 2].map(index => ({
      title: "Dune Frank Herbert [MP3]", provider: `provider-${index}`, mediaType: "audio",
      sizeBytes: 1000, url: `https://example.com/${index}`, guid: `g${index}`, infoHash: null,
      seeders: 10 - index, leechers: 0, raw: {},
    }));
    for (const matchingIndexes of [[[99], []], [[0], []], [[1], [1]], [[1]], [["https://evil.invalid/torrent"], []]]) {
      const provider = new FakeModelProvider([textModelResponse(JSON.stringify({
        selections: [{ parts: [0, 2], matchingIndexes }], confidence: 0.9, reason: "Invalid evidence",
      }))]);
      await expect(selectSearchCandidates(defaultSettings({ agents: agentSettings() }), {
        query: "Dune Frank Herbert", media: "audio", results, forceAgent: true,
      }, { modelProvider: provider })).rejects.toThrow("Agent search selection failed");
    }
  });

  test("search selection is deterministic by default", async () => {
    const settings = defaultSettings();

    const decision = await selectSearchCandidates(settings, {
      query: "Dune Frank Herbert",
      media: "audio",
      results: [
        {
          title: "Dune Frank Herbert [ENG / M4B]",
          provider: "mock",
          mediaType: "audio",
          sizeBytes: 1000,
          url: "https://example.com/one.torrent",
          guid: "g1",
          infoHash: null,
          seeders: 3,
          leechers: 0,
          raw: {},
        },
        {
          title: "Dune Complete Box Set [ENG / MP3]",
          provider: "mock",
          mediaType: "audio",
          sizeBytes: 1000,
          url: "https://example.com/two.torrent",
          guid: "g2",
          infoHash: null,
          seeders: 100,
          leechers: 0,
          raw: {},
        },
      ],
    });

    expect(decision.mode).toBe("deterministic");
    expect(decision.selections[0]?.parts[0]?.url).toBe("https://example.com/one.torrent");
    expect(decision.error).toBeNull();
  });

  test("search selection can use responses api when forced", async () => {
    const fakeProvider = new FakeModelProvider([
      textModelResponse(JSON.stringify({
        selections: [{ manifestation: { label: null, editionNote: null }, parts: [1] }],
        confidence: 0.73,
        reason: "Candidate two better matches requested edition",
      })),
    ]);
    const settings = defaultSettings({
      agents: agentSettings({ lowConfidenceThreshold: 0.99 }),
    });
    const decision = await selectSearchCandidates(
      settings,
      {
        query: "Dune Frank Herbert",
        media: "audio",
        forceAgent: true,
        results: [
          {
            title: "Dune Frank Herbert [ENG / M4B]",
            provider: "mock",
            mediaType: "audio",
            sizeBytes: 1000,
            url: "https://example.com/one.torrent",
            guid: "g1",
            infoHash: null,
            seeders: 3,
            leechers: 0,
            raw: {},
          },
          {
            title: "Dune Frank Herbert [ENG / MP3]",
            provider: "mock",
            mediaType: "audio",
            sizeBytes: 1000,
            url: "https://example.com/two.torrent",
            guid: "g2",
            infoHash: null,
            seeders: 1,
            leechers: 0,
            raw: {},
          },
        ],
      },
      { modelProvider: fakeProvider }
    );

    expect(decision.mode).toBe("agent");
    expect(decision.trigger).toBe("forced");
    expect(decision.selections[0]?.parts[0]?.url).toBe("https://example.com/two.torrent");
    expect(decision.confidence).toBe(0.73);
    expect(decision.error).toBeNull();
  });

  test("search agent can select ordered multipart manifestation releases", async () => {
    const fakeProvider = new FakeModelProvider([
      textModelResponse(JSON.stringify({
        selections: [
          {
            manifestation: { label: "GraphicAudio dramatization", editionNote: "full cast" },
            parts: [1, 2],
          },
        ],
        confidence: 0.82,
        reason: "GraphicAudio is split across two releases and matches the global preference.",
      })),
    ]);
    const settings = defaultSettings({
      agents: agentSettings({ lowConfidenceThreshold: 0.99, editionPreference: "prefer GraphicAudio dramatizations" }),
    });
    const decision = await selectSearchCandidates(
      settings,
      {
        query: "Red Rising Pierce Brown",
        media: "audio",
        forceAgent: true,
        editionPreference: settings.agents.editionPreference,
        results: [
          {
            title: "Red Rising GraphicAudio Part 1 [MP3]",
            provider: "mock",
            mediaType: "audio",
            sizeBytes: 1000,
            url: "https://example.com/ga-part-1.torrent",
            guid: "ga1",
            infoHash: null,
            seeders: 3,
            leechers: 0,
            raw: {},
          },
          {
            title: "Red Rising GraphicAudio Part 2 [MP3]",
            provider: "mock",
            mediaType: "audio",
            sizeBytes: 1000,
            url: "https://example.com/ga-part-2.torrent",
            guid: "ga2",
            infoHash: null,
            seeders: 3,
            leechers: 0,
            raw: {},
          },
          {
            title: "Red Rising Tim Gerard Reynolds [M4B]",
            provider: "mock",
            mediaType: "audio",
            sizeBytes: 1000,
            url: "https://example.com/classic.torrent",
            guid: "classic",
            infoHash: null,
            seeders: 10,
            leechers: 0,
            raw: {},
          },
        ],
      },
      { modelProvider: fakeProvider }
    );

    expect(decision.mode).toBe("agent");
    expect(decision.selections).toHaveLength(1);
    expect(decision.selections[0]?.manifestation).toEqual({
      label: "GraphicAudio dramatization",
      editionNote: "full cast",
      language: null,
    });
    expect(decision.selections[0]?.parts.map((part) => part.url)).toEqual([
      "https://example.com/ga-part-1.torrent",
      "https://example.com/ga-part-2.torrent",
    ]);
    // Verify the edition preference was included in the prompt sent to the model
    expect(JSON.stringify(fakeProvider.requests[0]?.input)).toContain("prefer GraphicAudio");
  });

  test("search agent inspect tool can inspect torrent file list before selecting", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const torrentUrl = "https://example.com/twilight-saga.torrent";
    const torrentBytes = makeMultiFileTorrentBytes("Twilight Saga", [
      { path: ["Twilight", "Twilight-01.mp3"], length: 1234 },
      { path: ["Twilight", "Twilight-02.mp3"], length: 2345 },
      { path: ["New Moon", "NewMoon-01.mp3"], length: 3456 },
    ]);
    let torrentFetchCount = 0;

    // The FakeModelProvider drives the AI turns; the real torrent fetch still hits our mock.
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (input: unknown) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input instanceof Request
              ? input.url
              : String(input);
      if (url === torrentUrl) {
        torrentFetchCount += 1;
        return new Response(torrentBytes, { status: 200, headers: { "Content-Type": "application/x-bittorrent" } });
      }
      throw new Error(`Unexpected url: ${url}`);
    }) as typeof fetch;

    // Turn 1: agent calls inspect(1). Turn 2: agent returns final JSON.
    const fakeProvider = new FakeModelProvider([
      functionCallModelResponse([
        { callId: "call_1", name: "inspect", arguments: JSON.stringify({ index: 1 }) },
      ]),
      textModelResponse(JSON.stringify({
        selections: [{ manifestation: { label: null, editionNote: null }, parts: [1] }],
        confidence: 0.71,
        reason: "Inspected candidate 1 and verified the torrent contains Twilight files.",
      })),
    ]);

    try {
      const settings = defaultSettings({ agents: agentSettings() });
      const decision = await selectSearchCandidates(
        settings,
        {
          query: "Twilight Stephenie Meyer",
          media: "audio",
          forceAgent: true,
          results: [
            {
              title: "Midnight Sun by Stephenie Meyer [ENG / MP3]",
              provider: "mock",
              mediaType: "audio",
              sizeBytes: 1000,
              url: "https://example.com/midnight-sun.torrent",
              guid: "g0",
              infoHash: null,
              seeders: 50,
              leechers: 0,
              raw: {},
            },
            {
              title: "Twilight Saga by Stephenie Meyer [ENG / MP3]",
              provider: "mock",
              mediaType: "audio",
              sizeBytes: 1000,
              url: torrentUrl,
              guid: "g1",
              infoHash: null,
              seeders: 10,
              leechers: 0,
              raw: {},
            },
          ],
          book: { id: 1, title: "Twilight", author: "Stephenie Meyer" },
        },
        { repo, modelProvider: fakeProvider }
      );

      expect(decision.mode).toBe("agent");
      expect(decision.selections[0]?.parts[0]?.url).toBe(torrentUrl);
      expect(torrentFetchCount).toBe(1);
      // Two model turns: first returned a tool call, second returned the selection
      expect(fakeProvider.requests).toHaveLength(2);
      // Second request should include the tool result with torrent file paths
      expect(JSON.stringify(fakeProvider.requests[1]?.input)).toContain("Twilight/Twilight-01.mp3");
    } finally {
      globalThis.fetch = originalFetch;
      db.close();
    }
  });

  test("book inspection failures never send credential-bearing errors to the model", async () => {
    using db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const url = "https://indexer.invalid/download?apikey=fixture-book-secret";
    const originalFetch = globalThis.fetch;
    let fetches = 0;
    globalThis.fetch = Object.assign(async () => {
      fetches++;
      throw new Error(`Network failure for ${url}`);
    }, { preconnect: originalFetch.preconnect });
    const fake = new FakeModelProvider([
      functionCallModelResponse([{ callId: "inspect_failure", name: "inspect", arguments: '{"index":0}' }]),
      textModelResponse(JSON.stringify({ selections: [], confidence: 0, reason: "Could not inspect candidate." })),
    ]);
    try {
      const decision = await selectSearchCandidates(defaultSettings({ agents: agentSettings() }), {
        query: "Twilight Stephenie Meyer", media: "audio", forceAgent: true,
        book: { id: 1, title: "Twilight", author: "Stephenie Meyer" },
        results: [{ title: "Twilight Stephenie Meyer MP3", provider: "fixture", mediaType: "audio",
          url, guid: "fixture", infoHash: null, sizeBytes: 1000, seeders: 1, leechers: 0, raw: {} }],
      }, { repo, modelProvider: fake });
      expect(decision.mode).toBe("agent");
      expect(fetches).toBe(1);
      expect(fake.requests).toHaveLength(2);
      expect(JSON.stringify(fake.requests[1]?.input)).toContain("Could not inspect this release's torrent file.");
      expect(JSON.stringify(fake.requests)).not.toContain("fixture-book-secret");
    } finally { globalThis.fetch = originalFetch; }
  });

  test("forced/configured search agents inspect all oppositely tagged candidates even when fallback selects nothing", async () => {
    for (const media of ["audio", "ebook"] as const) {
      const db = new Database(":memory:");
      runMigrations(db);
      const repo = new BooksRepo(db);
      const wrongTag = media === "audio" ? "EPUB" : "MP3";
      const correctFile = media === "audio" ? "Foundation.m4b" : "Foundation.epub";
      const candidates: TorznabResult[] = ["decoy", "complete", "rejected"].map((name) => ({
        title: `Foundation Isaac Asimov [${wrongTag}] ${name}`,
        provider: "mislabeled-provider", mediaType: media, guid: name, infoHash: null,
        url: `https://example.invalid/${media}/${name}.torrent`, sizeBytes: 1000,
        seeders: 1, leechers: 0, raw: {},
      }));
      for (const [index, candidate] of candidates.entries()) {
        const torrentBytes = makeMultiFileTorrentBytes("Release", [
          { path: [index === 1 ? correctFile : `Another-Book.${media === "audio" ? "epub" : "mp3"}`], length: 900 },
          { path: ["release.nfo"], length: 100 },
        ]);
        repo.putTorrentCache({
          key: torrentCacheKeyFor({ provider: candidate.provider, providerGuid: candidate.guid, url: candidate.url }),
          provider: candidate.provider, providerGuid: candidate.guid, url: candidate.url, torrentBytes,
        });
      }
      const fakeProvider = new FakeModelProvider([
        functionCallModelResponse([
          { callId: "inspect_decoy", name: "inspect", arguments: JSON.stringify({ index: 0 }) },
          { callId: "inspect_complete", name: "inspect", arguments: JSON.stringify({ index: 1 }) },
        ]),
        textModelResponse(JSON.stringify({
          selections: [{ manifestation: { label: "Verified contents", editionNote: "Incorrect provider format tag" }, parts: [1] }],
          confidence: 0.95, reason: `The inspected file list contains ${correctFile}, despite the ${wrongTag} title tag`,
        })),
      ]);
      try {
        // One media exercises explicit forcing, the other configured-agent
        // recovery even when the normal confidence threshold disables review.
        const settings = defaultSettings({ agents: agentSettings({ lowConfidenceThreshold: 0 }) });
        const input = {
          query: "Foundation Isaac Asimov", media, results: candidates,
          rejectedGuids: ["rejected"], forceAgent: media === "audio",
          book: { id: 1, title: "Foundation", author: "Isaac Asimov" },
        };
        const fallback = await selectSearchCandidates(defaultSettings(), { ...input, forceAgent: false }, { repo });
        expect(fallback.mode).toBe("deterministic");
        expect(fallback.selections).toEqual([]);
        const decision = await selectSearchCandidates(settings, input, { repo, modelProvider: fakeProvider });
        expect(decision.mode).toBe("agent");
        expect(decision.selections[0].parts).toEqual([candidates[1]]);
        expect(fakeProvider.requests).toHaveLength(2);
        expect(JSON.stringify(fakeProvider.requests[0])).toContain("Never knowingly select an incomplete multipart edition");
        const prompt = JSON.stringify(fakeProvider.requests[0].input);
        expect(prompt).toContain(candidates[0].title);
        expect(prompt).toContain(candidates[1].title);
        expect(prompt).not.toContain(candidates[2].title);
        const evidence = JSON.stringify(fakeProvider.requests[1].input);
        expect(evidence).toContain(correctFile);
        expect(evidence).toContain("Another-Book.");
        expect(evidence).toContain("release.nfo");
      } finally { db.close(); }
    }
  });

  test("empty or entirely rejected search evidence never invokes the agent", async () => {
    const settings = defaultSettings({ agents: agentSettings() });
    const candidate: TorznabResult = {
      title: "Foundation Isaac Asimov [EPUB]", mediaType: "audio", provider: "mock",
      url: "https://example.invalid/rejected.torrent", guid: "rejected", infoHash: null,
      sizeBytes: 1000, seeders: 1, leechers: 0, raw: {},
    };
    const fakeProvider = new FakeModelProvider([]);
    for (const results of [[], [candidate]]) {
      const decision = await selectSearchCandidates(settings, {
        query: "Foundation Isaac Asimov", media: "audio", results,
        forceAgent: true, rejectedGuids: ["rejected"],
      }, { modelProvider: fakeProvider });
      expect(decision.selections).toEqual([]);
    }
    expect(fakeProvider.requests).toHaveLength(0);
  });

  test("deterministic media hints do not mistake Isaac for AAC", async () => {
    const candidate: TorznabResult = {
      title: "Foundation Isaac Asimov", mediaType: "ebook", provider: "mock",
      url: "https://example.invalid/foundation.torrent", guid: "foundation", infoHash: null,
      sizeBytes: 1000, seeders: 1, leechers: 0, raw: {},
    };
    const decision = await selectSearchCandidates(defaultSettings(), {
      query: "Foundation Isaac Asimov", media: "ebook", results: [candidate],
    });
    expect(decision.mode).toBe("deterministic");
    expect(decision.selections[0].parts).toEqual([candidate]);
  });

  test("failed GUID and URL may identify new content, but unknown and explicitly failed hashes stay excluded", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);
    const book = repo.createBook({ title: "Foundation", author: "Isaac Asimov" });
    const url = "https://example.invalid/mutable.torrent";
    const failed = repo.createRelease({
      bookId: book.id, provider: "mock", providerGuid: "mutable", title: "Foundation old content",
      mediaType: "audio", infoHash: "a".repeat(40), url,
    });
    repo.setReleaseStatus(failed.id, "failed", "Wrong content");
    const candidates: TorznabResult[] = [
      { name: "unknown", hash: null }, { name: "old-content", hash: "a".repeat(40) },
      { name: "new-content", hash: "b".repeat(40) }, { name: "other-rejected-content", hash: "c".repeat(40) },
    ].map(({ name, hash }) => ({
      title: `Foundation Isaac Asimov [M4B] ${name}`, provider: "mock", mediaType: "audio",
      guid: "mutable", url, infoHash: hash, sizeBytes: 1000, seeders: 1, leechers: 0, raw: {},
    }));
    const fakeProvider = new FakeModelProvider([textModelResponse(JSON.stringify({
      selections: [{ manifestation: { label: null, editionNote: null }, parts: [0] }],
      confidence: 0.95, reason: "Known new torrent identity, not the previously failed content",
    }))]);
    try {
      const decision = await selectSearchCandidates(defaultSettings({ agents: agentSettings() }), {
        query: "Foundation Isaac Asimov", media: "audio", results: candidates, forceAgent: true,
        book: { id: book.id, title: book.title, author: book.author },
        rejectedUrls: [url], rejectedGuids: ["mutable"], rejectedInfoHashes: ["a".repeat(40), "c".repeat(40)],
      }, { repo, modelProvider: fakeProvider });
      expect(decision.selections[0].parts).toEqual([candidates[2]]);
      const prompt = JSON.stringify(fakeProvider.requests[0].input);
      expect(prompt).toContain(candidates[2].title);
      for (const index of [0, 1, 3]) expect(prompt).not.toContain(candidates[index].title);
    } finally { db.close(); }
  });

  test("search throws when agent call fails", async () => {
    const fakeProvider = new FakeModelProvider([
      // Simulates a model failure by throwing from getModel/getResponse
    ]);
    // Override getModel to throw
    const failingProvider = {
      getModel: () => ({
        getResponse: async () => { throw new Error("model request failed: 500 Internal Server Error"); },
        getStreamedResponse: async function* () { throw new Error("not supported"); },
      }),
    };
    const settings = defaultSettings({
      agents: agentSettings({ lowConfidenceThreshold: 0.99 }),
    });
    await expect(
      selectSearchCandidates(
        settings,
        {
          query: "Dune Frank Herbert",
          media: "audio",
          forceAgent: true,
          results: [
            {
              title: "Dune Frank Herbert [ENG / M4B]",
              provider: "mock",
              mediaType: "audio",
              sizeBytes: 1000,
              url: "https://example.com/one.torrent",
              guid: "g1",
              infoHash: null,
              seeders: 3,
              leechers: 0,
              raw: {},
            },
            {
              title: "Dune Complete Box Set [ENG / MP3]",
              provider: "mock",
              mediaType: "audio",
              sizeBytes: 1000,
              url: "https://example.com/two.torrent",
              guid: "g2",
              infoHash: null,
              seeders: 100,
              leechers: 0,
              raw: {},
            },
          ],
        },
        { modelProvider: failingProvider }
      )
    ).rejects.toThrow("500");
  });

  test("legacy settings without nested agent domain config do not crash search selection", async () => {
    const legacySettings = parseSettings(
      JSON.stringify({
        ...defaultSettings(),
    agents: {
      provider: "openai-responses",
      model: "gpt-5-mini",
      apiKey: "test-key",
      lowConfidenceThreshold: 0.45,
      timeoutMs: 8000,
        },
      })
    );
    expect("model" in legacySettings.agents).toBe(false);

    const decision = await selectSearchCandidates(legacySettings, {
      query: "Dune Frank Herbert",
      media: "audio",
      results: [
        {
          title: "Dune Frank Herbert [ENG / M4B]",
          provider: "mock",
          mediaType: "audio",
          sizeBytes: 1000,
          url: "https://example.com/one.torrent",
          guid: "g1",
          infoHash: null,
          seeders: 3,
          leechers: 0,
          raw: {},
        },
      ],
    });

    expect(decision.mode).toBe("deterministic");
    expect(decision.selections[0]?.parts[0]?.url).toBe("https://example.com/one.torrent");
  });

  test("search selection excludes rejected guid and infohash", async () => {
    const settings = defaultSettings();

    const decision = await selectSearchCandidates(settings, {
      query: "Dune Frank Herbert",
      media: "audio",
      rejectedGuids: ["g1"],
      rejectedInfoHashes: ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
      results: [
        {
          title: "Dune Frank Herbert [ENG / M4B]",
          provider: "mock",
          mediaType: "audio",
          sizeBytes: 1000,
          url: "https://example.com/one.torrent",
          guid: "g1",
          infoHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          seeders: 3,
          leechers: 0,
          raw: {},
        },
        {
          title: "Dune Frank Herbert [ENG / MP3]",
          provider: "mock",
          mediaType: "audio",
          sizeBytes: 1200,
          url: "https://example.com/two.torrent",
          guid: "g2",
          infoHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
          seeders: 2,
          leechers: 0,
          raw: {},
        },
      ],
    });

    expect(decision.mode).toBe("deterministic");
    expect(decision.selections[0]?.parts[0]?.url).toBe("https://example.com/two.torrent");
  });

  test("manual import selection is deterministic by default", async () => {
    const settings = defaultSettings();

    const decision = await selectManualImportPaths(settings, {
      mediaType: "ebook",
      files: [
        {
          sourcePath: "/tmp/a.txt",
          relativePath: "a.txt",
          ext: ".txt",
          size: 10,
          mtimeMs: 1,
          supportedAudio: false,
          supportedEbook: false,
        },
        {
          sourcePath: "/tmp/b.epub",
          relativePath: "b.epub",
          ext: ".epub",
          size: 20,
          mtimeMs: 1,
          supportedAudio: false,
          supportedEbook: true,
        },
      ],
    });

    expect(decision.mode).toBe("deterministic");
    expect(decision.selectedPaths).toEqual(["/tmp/b.epub"]);
    expect(decision.error).toBeNull();
  });

  test("manual import does not call agent when no importable files exist", async () => {
    const fakeProvider = new FakeModelProvider([
      // Should never be called — no importable files, so deterministic path is taken
      textModelResponse(JSON.stringify({ selectedIndices: [], confidence: 0, reason: "should not be called" })),
    ]);
    const settings = defaultSettings({ agents: agentSettings() });
    const decision = await selectManualImportPaths(
      settings,
      {
        mediaType: "ebook",
        forceAgent: true,
        files: [
          {
            sourcePath: "/tmp/a.azw3",
            relativePath: "a.azw3",
            ext: ".azw3",
            size: 10,
            mtimeMs: 1,
            supportedAudio: false,
            supportedEbook: false,
          },
        ],
      },
      { modelProvider: fakeProvider }
    );

    expect(decision.mode).toBe("deterministic");
    expect(decision.selectedPaths).toEqual([]);
    expect(decision.reason).toBe("No supported files for media type");
    expect(fakeProvider.requests).toHaveLength(0);
  });

  test("manual import agent may return no alternative files", async () => {
    const fakeProvider = new FakeModelProvider([
      textModelResponse(JSON.stringify({
        selectedIndices: [],
        confidence: 0.9,
        reason: "Only previously rejected file is present; no alternative importable files.",
      })),
    ]);
    const settings = defaultSettings({ agents: agentSettings() });
    const decision = await selectManualImportPaths(
      settings,
      {
        mediaType: "ebook",
        forceAgent: true,
        files: [
          {
            sourcePath: "/tmp/twilight.epub",
            relativePath: "twilight.epub",
            ext: ".epub",
            size: 1234,
            mtimeMs: 1,
            supportedAudio: false,
            supportedEbook: true,
          },
        ],
        rejectedSourcePaths: ["/tmp/twilight.epub"],
      },
      { modelProvider: fakeProvider }
    );

    expect(decision.mode).toBe("agent");
    expect(decision.selectedPaths).toEqual([]);
    expect(decision.reason).toContain("no alternative");
  });

  test("manual import agent selecting previously rejected exact file set is treated as no alternative", async () => {
    const fakeProvider = new FakeModelProvider([
      textModelResponse(JSON.stringify({
        selectedIndices: [0],
        confidence: 0.6,
        reason: "Select the EPUB file.",
      })),
    ]);
    const settings = defaultSettings({ agents: agentSettings() });
    const decision = await selectManualImportPaths(
      settings,
      {
        mediaType: "ebook",
        forceAgent: true,
        files: [
          {
            sourcePath: "/tmp/twilight.epub",
            relativePath: "twilight.epub",
            ext: ".epub",
            size: 1234,
            mtimeMs: 1,
            supportedAudio: false,
            supportedEbook: true,
          },
        ],
        rejectedSourcePaths: ["/tmp/twilight.epub"],
      },
      { modelProvider: fakeProvider }
    );

    expect(decision.mode).toBe("agent");
    expect(decision.selectedPaths).toEqual([]);
  });
});
