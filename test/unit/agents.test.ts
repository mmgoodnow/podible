import { describe, expect, test } from "bun:test";

import { selectManualImportPaths, selectSearchCandidate } from "../../src/kindling/agents";
import { defaultSettings, parseSettings } from "../../src/kindling/settings";

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

  test("search throws when agent call fails", async () => {
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
      if (originalApiKey === undefined) {
        delete process.env.OPENAI_API_KEY;
      } else {
        process.env.OPENAI_API_KEY = originalApiKey;
      }
    }
  });

  test("legacy settings without nested agent domain config do not crash search selection", async () => {
    const legacySettings = parseSettings(
      JSON.stringify({
        ...defaultSettings(),
        agents: {
          enabled: true,
          provider: "openai-responses",
          model: "gpt-5-mini",
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

  test("manual import agent may return no alternative files", async () => {
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
        agents: {
          ...defaultSettings().agents,
          enabled: true,
        },
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
      if (originalApiKey === undefined) delete process.env.OPENAI_API_KEY;
      else process.env.OPENAI_API_KEY = originalApiKey;
    }
  });

  test("manual import agent selecting previously rejected exact file set is treated as no alternative", async () => {
    const originalFetch = globalThis.fetch;
    const originalApiKey = process.env.OPENAI_API_KEY;
    process.env.OPENAI_API_KEY = "test-key";
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
        agents: {
          ...defaultSettings().agents,
          enabled: true,
        },
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
      if (originalApiKey === undefined) delete process.env.OPENAI_API_KEY;
      else process.env.OPENAI_API_KEY = originalApiKey;
    }
  });
});
