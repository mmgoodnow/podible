import { describe, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import bencode from "bencode";

import { selectManualImportPaths, selectSearchCandidate } from "../../src/books/agents";
import { runMigrations } from "../../src/books/db";
import { BooksRepo } from "../../src/books/repo";
import { defaultSettings, parseSettings } from "../../src/books/settings";

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

describe("agent decisions", () => {
  test("search selection is deterministic by default", async () => {
    const settings = defaultSettings();

    const decision = await selectSearchCandidate(settings, {
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
    expect(decision.candidate?.url).toBe("https://example.com/one.torrent");
    expect(decision.error).toBeNull();
  });

  test("search selection can use responses api when forced", async () => {
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
      if (url !== "https://api.openai.com/v1/responses") {
        throw new Error(`Unexpected url: ${url}`);
      }
      return new Response(
        JSON.stringify({
          output_text: JSON.stringify({
            selectedIndex: 1,
            confidence: 0.73,
            reason: "Candidate two better matches requested edition",
          }),
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }) as typeof fetch;

    try {
      const settings = defaultSettings({
        agents: agentSettings({ lowConfidenceThreshold: 0.99 }),
      });
      const decision = await selectSearchCandidate(settings, {
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
      });

      expect(decision.mode).toBe("agent");
      expect(decision.trigger).toBe("forced");
      expect(decision.candidate?.url).toBe("https://example.com/two.torrent");
      expect(decision.confidence).toBe(0.73);
      expect(decision.error).toBeNull();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("search agent inspect tool can inspect torrent file list before selecting", async () => {
    const db = new Database(":memory:");
    runMigrations(db);
    const repo = new BooksRepo(db);

    const originalFetch = globalThis.fetch;

    const torrentUrl = "https://example.com/twilight-saga.torrent";
    const torrentBytes = makeMultiFileTorrentBytes("Twilight Saga", [
      { path: ["Twilight", "Twilight-01.mp3"], length: 1234 },
      { path: ["Twilight", "Twilight-02.mp3"], length: 2345 },
      { path: ["New Moon", "NewMoon-01.mp3"], length: 3456 },
    ]);
    const openAiBodies: any[] = [];
    let torrentFetchCount = 0;

    globalThis.fetch = (async (input: unknown, init?: RequestInit) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input instanceof Request
              ? input.url
              : String(input);
      if (url === "https://api.openai.com/v1/responses") {
        const body = JSON.parse(String(init?.body ?? "{}"));
        openAiBodies.push(body);
        if (!body.previous_response_id) {
          return new Response(
            JSON.stringify({
              id: "resp_1",
              output: [
                {
                  type: "function_call",
                  id: "fc_1",
                  call_id: "call_1",
                  name: "inspect",
                  arguments: JSON.stringify({ index: 1 }),
                  status: "completed",
                },
              ],
              output_text: "",
            }),
            { status: 200, headers: { "Content-Type": "application/json" } }
          );
        }
        return new Response(
          JSON.stringify({
            id: "resp_2",
            output_text: JSON.stringify({
              selectedIndex: 1,
              confidence: 0.71,
              reason: "Inspected candidate 1 and verified the torrent contains Twilight files.",
            }),
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      if (url === torrentUrl) {
        torrentFetchCount += 1;
        return new Response(torrentBytes, {
          status: 200,
          headers: { "Content-Type": "application/x-bittorrent" },
        });
      }
      throw new Error(`Unexpected url: ${url}`);
    }) as typeof fetch;

    try {
      const settings = defaultSettings({
        agents: agentSettings(),
      });
      const decision = await selectSearchCandidate(
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
        { repo }
      );

      expect(decision.mode).toBe("agent");
      expect(decision.candidate?.url).toBe(torrentUrl);
      expect(torrentFetchCount).toBe(1);
      expect(openAiBodies).toHaveLength(2);
      expect(openAiBodies[0]?.tools?.[0]?.name).toBe("inspect");
      expect(JSON.stringify(openAiBodies[1])).toContain("function_call_output");
      expect(JSON.stringify(openAiBodies[1])).toContain("Twilight/Twilight-01.mp3");
    } finally {
      globalThis.fetch = originalFetch;
      db.close();
    }
  });

  test("search throws when agent call fails", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => new Response("bad", { status: 500 })) as unknown as typeof fetch;

    try {
      const settings = defaultSettings({
        agents: agentSettings({ lowConfidenceThreshold: 0.99 }),
      });
      await expect(
        selectSearchCandidate(settings, {
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
        })
      ).rejects.toThrow("500");
    } finally {
      globalThis.fetch = originalFetch;
    }
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

    const decision = await selectSearchCandidate(legacySettings, {
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
    expect(decision.candidate?.url).toBe("https://example.com/one.torrent");
  });

  test("search selection excludes rejected guid and infohash", async () => {
    const settings = defaultSettings();

    const decision = await selectSearchCandidate(settings, {
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
    expect(decision.candidate?.url).toBe("https://example.com/two.torrent");
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
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () => {
      throw new Error("should not call OpenAI");
    }) as unknown as typeof fetch;

    try {
      const settings = defaultSettings({
        agents: agentSettings(),
      });
      const decision = await selectManualImportPaths(settings, {
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
      });

      expect(decision.mode).toBe("deterministic");
      expect(decision.selectedPaths).toEqual([]);
      expect(decision.reason).toBe("No supported files for media type");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("manual import agent may return no alternative files", async () => {
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
      if (url !== "https://api.openai.com/v1/responses") {
        throw new Error(`Unexpected url: ${url}`);
      }
      return new Response(
        JSON.stringify({
          output_text: JSON.stringify({
            selectedIndices: [],
            confidence: 0.9,
            reason: "Only previously rejected file is present; no alternative importable files.",
          }),
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }) as typeof fetch;

    try {
      const settings = defaultSettings({
        agents: agentSettings(),
      });
      const decision = await selectManualImportPaths(settings, {
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
      });

      expect(decision.mode).toBe("agent");
      expect(decision.selectedPaths).toEqual([]);
      expect(decision.reason).toContain("no alternative");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  test("manual import agent selecting previously rejected exact file set is treated as no alternative", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () =>
      new Response(
        JSON.stringify({
          output_text: JSON.stringify({
            selectedIndices: [0],
            confidence: 0.6,
            reason: "Select the EPUB file.",
          }),
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      )) as unknown as typeof fetch;

    try {
      const settings = defaultSettings({
        agents: agentSettings(),
      });
      const decision = await selectManualImportPaths(settings, {
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
      });

      expect(decision.mode).toBe("agent");
      expect(decision.selectedPaths).toEqual([]);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
