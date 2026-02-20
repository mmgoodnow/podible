import { describe, expect, test } from "bun:test";

import { selectManualImportPaths, selectSearchCandidate } from "../../src/kindling/agents";
import { defaultSettings } from "../../src/kindling/settings";

describe("agent decisions", () => {
  test("search selection is deterministic by default", async () => {
    const settings = defaultSettings({
      agents: {
        ...defaultSettings().agents,
        enabled: false,
      },
    });

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
    const originalApiKey = process.env.OPENAI_API_KEY;
    process.env.OPENAI_API_KEY = "test-key";
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
        agents: {
          ...defaultSettings().agents,
          enabled: true,
          lowConfidenceThreshold: 0.99,
        },
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
      if (originalApiKey === undefined) {
        delete process.env.OPENAI_API_KEY;
      } else {
        process.env.OPENAI_API_KEY = originalApiKey;
      }
    }
  });

  test("search falls back to deterministic when agent call fails", async () => {
    const originalFetch = globalThis.fetch;
    const originalApiKey = process.env.OPENAI_API_KEY;
    process.env.OPENAI_API_KEY = "test-key";
    globalThis.fetch = (async () => new Response("bad", { status: 500 })) as unknown as typeof fetch;

    try {
      const settings = defaultSettings({
        agents: {
          ...defaultSettings().agents,
          enabled: true,
          lowConfidenceThreshold: 0.99,
        },
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
      expect(decision.error).toContain("500");
    } finally {
      globalThis.fetch = originalFetch;
      if (originalApiKey === undefined) {
        delete process.env.OPENAI_API_KEY;
      } else {
        process.env.OPENAI_API_KEY = originalApiKey;
      }
    }
  });

  test("search selection excludes rejected guid and infohash", async () => {
    const settings = defaultSettings({
      agents: {
        ...defaultSettings().agents,
        enabled: false,
      },
    });

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
    const settings = defaultSettings({
      agents: {
        ...defaultSettings().agents,
        enabled: false,
      },
    });

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
});
