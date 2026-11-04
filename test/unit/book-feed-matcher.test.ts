import { expect, test } from "bun:test";
import { Usage, type ModelProvider } from "@openai/agents-core";
import { TEXT_AGENT_MODEL } from "../../src/ai-models";
import {
  createBookFeedMatcher,
  type BookFeedTarget,
} from "../../src/library/feed-matcher";
import type { TorznabResult } from "../../src/library/torznab";
import { defaultSettings } from "../../src/settings";

type Projection = {
  targets: { index: number; title: string; author: string; media: string }[];
  releases: {
    index: number;
    title: string;
    media: string;
    attributes: unknown[];
  }[];
};

function fixture(reply: (input: Projection) => unknown) {
  const calls: { input: Projection; bytes: number; prompt: string }[] = [];
  const provider: ModelProvider = {
    getModel(model) {
      expect(model).toBe(TEXT_AGENT_MODEL);
      return {
        async getResponse(request) {
          const message =
            typeof request.input === "string"
              ? null
              : request.input.find(
                  (item) => item.type === "message" && item.role === "user",
                );
          const content = message?.type === "message" ? message.content : null;
          const text =
            typeof request.input === "string"
              ? request.input
              : typeof content === "string"
                ? content
                : Array.isArray(content)
                  ? content.find((item) => item.type === "input_text")?.text
                  : null;
          if (!text) throw new Error("Missing agent input");
          const input: Projection = JSON.parse(text);
          calls.push({
            input,
            bytes: Buffer.byteLength(text),
            prompt: JSON.stringify(request),
          });
          return {
            usage: new Usage(),
            output: [
              {
                type: "message",
                role: "assistant",
                status: "completed",
                content: [
                  { type: "output_text", text: JSON.stringify(reply(input)) },
                ],
              },
            ],
          };
        },
        getStreamedResponse: async function* () {
          throw new Error("unused");
        },
      };
    },
  };
  return { calls, matcher: createBookFeedMatcher(defaultSettings(), provider) };
}
const target = (
  bookId: number,
  media: "audio" | "ebook" = "audio",
): BookFeedTarget => ({
  bookId,
  media,
  title: "Foundation",
  author: "Isaac Asimov",
  language: "eng",
});
function release(
  title = "An untagged potential edition",
  media: "audio" | "ebook" = "audio",
): TorznabResult {
  return {
    title,
    mediaType: media,
    provider: "Private",
    url: "https://secret.invalid/download?apikey=private-key",
    guid: "opaque",
    infoHash: null,
    sizeBytes: 100,
    seeders: 1,
    leechers: 0,
    raw: {
      attr: [
        { name: "author", value: "Isaac Asimov" },
        { name: "description", value: "UNTRUSTED-SECRET-DESCRIPTION" },
      ],
    },
  };
}

test("book RSS agent uses the shared model and broad evidence, without credentials or raw descriptions", async () => {
  const f = fixture(() => ({
    matches: [
      {
        targetIndex: 0,
        releaseIndexes: [0, 0],
        reason: "Author and title are plausible",
      },
    ],
  }));
  expect(
    await f.matcher({ targets: [target(7)], releases: [release()] }),
  ).toEqual([
    {
      bookId: 7,
      media: "audio",
      releaseIndexes: [0],
      reason: "Author and title are plausible",
    },
  ]);
  expect(f.calls[0].input.releases).toHaveLength(1);
  const sent = f.calls[0].prompt;
  expect(sent).toContain("Isaac Asimov");
  expect(sent).toContain("broad semantic identity routing");
  expect(sent).not.toContain("private-key");
  expect(sent).not.toContain("secret.invalid");
  expect(sent).not.toContain("UNTRUSTED-SECRET-DESCRIPTION");
});

test("book RSS agent rejects invented indices, wrong media and empty evidence", async () => {
  for (const match of [
    { targetIndex: 2, releaseIndexes: [0], reason: "Unknown target" },
    { targetIndex: 0, releaseIndexes: [2], reason: "Unknown release" },
    { targetIndex: 0, releaseIndexes: [-1], reason: "Negative release" },
    { targetIndex: 0, releaseIndexes: [0], reason: " " },
  ]) {
    const f = fixture(() => ({ matches: [match] }));
    await expect(
      f.matcher({ targets: [target(7)], releases: [release()] }),
    ).rejects.toThrow();
  }
  const f = fixture(() => ({
    matches: [
      { targetIndex: 0, releaseIndexes: [0], reason: "Wrong category" },
    ],
  }));
  await expect(
    f.matcher({ targets: [target(7, "ebook")], releases: [release()] }),
  ).rejects.toThrow("crossed media");
});

test("book RSS batching bounds UTF-8 JSON bytes and remaps every target-release pair without loss", async () => {
  const padding = "\0".repeat(1000);
  const targets = Array.from({ length: 7 }, (_, i) => ({
    ...target(100 + i),
    title: `book-${i} ${padding}`,
    author: padding,
  }));
  const releases = Array.from({ length: 9 }, (_, i) => ({
    ...release(`release-${i} ${padding}`),
    raw: {
      attr: Array.from({ length: 20 }, () => ({
        name: "author",
        value: padding,
      })),
    },
  }));
  const f = fixture((input) => ({
    matches: input.targets.map((row) => ({
      targetIndex: row.index,
      releaseIndexes: input.releases.map((release) => release.index),
      reason: "Potential identity",
    })),
  }));
  const matches = await f.matcher({ targets, releases });
  expect(f.calls.length).toBeGreaterThan(1);
  expect(f.calls.every((call) => call.bytes <= 32 * 1024)).toBe(true);
  expect(matches.map((match) => match.bookId).sort()).toEqual(
    targets.map((row) => row.bookId).sort(),
  );
  for (const match of matches)
    expect([...match.releaseIndexes].sort((a, b) => a - b)).toEqual(
      releases.map((_, i) => i),
    );
});

test("empty book RSS batches do not invoke the model", async () => {
  const f = fixture(() => {
    throw new Error("Must not call");
  });
  expect(await f.matcher({ targets: [], releases: [release()] })).toEqual([]);
  expect(await f.matcher({ targets: [target(7)], releases: [] })).toEqual([]);
  expect(f.calls).toHaveLength(0);
});
