import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import bencode from "bencode";
import type { Model, ModelProvider, ModelRequest, ModelResponse } from "@openai/agents-core";
import { Usage } from "@openai/agents-core";

import { selectManualImportPaths, selectSearchCandidates } from "../../src/library/agents";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings, parseSettings } from "../../src/settings";

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
