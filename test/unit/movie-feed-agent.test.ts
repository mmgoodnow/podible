import { expect, test } from "bun:test";
import type { ModelProvider, ModelRequest } from "@openai/agents-core";
import { Usage } from "@openai/agents-core";
import { createMovieFeedMatcher } from "../../src/movies/agents";
import { defaultSettings } from "../../src/settings";
import { fixtureMovie } from "../fixtures/movie-services";
import { TEXT_AGENT_MODEL } from "../../src/ai-models";
import type { MovieOffer } from "../../src/movies/offers";

type Projection = {
  targets: {
    index: number;
    title: string;
    originalTitle: string;
    tmdbId: number;
    imdbId: string | null;
  }[];
  releases: {
    index: number;
    title: string;
    provider: string;
    attributes: { name: string; value: string }[];
  }[];
};

function captureMatcher(reply: (input: Projection) => unknown) {
  const calls: { input: Projection; bytes: number }[] = [];
  const provider: ModelProvider = {
    getModel() {
      return {
        async getResponse(request) {
          let text: string;
          if (typeof request.input === "string") text = request.input;
          else {
            const message = request.input.find(
              (item) => item.type === "message" && item.role === "user",
            );
            if (!message || message.type !== "message")
              throw new Error("Missing model input");
            if (typeof message.content === "string") text = message.content;
            else {
              const content = message.content.find(
                (item) => item.type === "input_text",
              );
              if (!content || content.type !== "input_text")
                throw new Error("Missing model input text");
              text = content.text;
            }
          }
          const input: Projection = JSON.parse(text);
          calls.push({ input, bytes: Buffer.byteLength(text) });
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
  return {
    calls,
    matcher: createMovieFeedMatcher(defaultSettings(), provider),
  };
}

function offer(title = "An ambiguous edition"): MovieOffer {
  return {
    key: "opaque",
    provider: "Fixture",
    title,
    url: "https://private.invalid/?apikey=secret-key",
    infoHash: null,
    sizeBytes: null,
    seeders: null,
    attributes: {},
  };
}

function oversizedBatch() {
  // Escaping expands each NUL to six bytes; counts/character lengths alone are insufficient.
  const padding = "\0".repeat(1000);
  return {
    targets: Array.from({ length: 6 }, (_, index) => ({
      targetId: 100 + index,
      movie: {
        ...fixtureMovie,
        tmdbId: 1000 + index,
        title: padding,
        originalTitle: padding,
      },
    })),
    offers: Array.from({ length: 8 }, (_, index) => ({
      ...offer(`offer-${index} ${padding}`),
      provider: padding,
      attributes: {
        attr: Array.from({ length: 20 }, () => ({
          name: "tmdbid",
          value: padding,
        })),
      },
    })),
  };
}

test("RSS agent broadly routes known identities without exposing download URLs or arbitrary provider text", async () => {
  const requests: ModelRequest[] = [];
  let reply: unknown = {
    matches: [
      {
        targetIndex: 0,
        releaseIndexes: [0, 0],
        reason: "Potential identity match; quality remains unverified",
      },
    ],
  };
  const provider: ModelProvider = {
    getModel(name) {
      expect(name).toBe(TEXT_AGENT_MODEL);
      return {
        getResponse: async (request) => {
          requests.push(request);
          return {
            usage: new Usage(),
            output: [
              {
                type: "message",
                role: "assistant",
                status: "completed",
                content: [{ type: "output_text", text: JSON.stringify(reply) }],
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
  const matcher = createMovieFeedMatcher(defaultSettings(), provider);
  const input = {
    targets: [{ targetId: 7, movie: fixtureMovie }],
    offers: [
      {
        key: "opaque",
        provider: "Fixture",
        title: "An ambiguous edition",
        url: "https://private.invalid/?apikey=secret-key",
        infoHash: null,
        sizeBytes: null,
        seeders: null,
        attributes: {
          attr: [
            { name: "imdbid", value: "tt123" },
            { name: "description", value: "secret-prompt-body" },
          ],
        },
      },
    ],
  };
  expect(await matcher(input)).toEqual([
    {
      targetId: 7,
      releaseIndexes: [0],
      reason: "Potential identity match; quality remains unverified",
    },
  ]);
  const sent = JSON.stringify(requests);
  expect(sent).toContain("tt123");
  expect(sent).not.toContain("secret-key");
  expect(sent).not.toContain("secret-prompt-body");
  expect(sent).toContain("Include plausible matches");
  for (const match of [
    { targetIndex: 5, releaseIndexes: [0], reason: "Unknown target" },
    { targetIndex: 0, releaseIndexes: [5], reason: "Unknown release" },
  ]) {
    reply = { matches: [match] };
    await expect(matcher(input)).rejects.toThrow("unknown target or release");
  }
});

test("RSS matcher bounds oversized Unicode titles, provider/catalog strings and identity attributes without dropping candidates", async () => {
  const text = '\u{20bb7}"\n'.repeat(400_000);
  const candidate = {
    ...offer(text),
    provider: text,
    attributes: {
      attr: Array.from({ length: 30 }, () => ({ name: "imdbid", value: text })),
    },
  };
  const { matcher, calls } = captureMatcher(() => ({
    matches: [
      {
        targetIndex: 0,
        releaseIndexes: [0],
        reason: "Plausible partial identity",
      },
    ],
  }));
  expect(
    await matcher({
      targets: [
        {
          targetId: 7,
          movie: {
            ...fixtureMovie,
            title: text,
            originalTitle: text,
            imdbId: text,
          },
        },
      ],
      offers: [candidate],
    }),
  ).toEqual([
    { targetId: 7, releaseIndexes: [0], reason: "Plausible partial identity" },
  ]);
  expect(calls).toHaveLength(1);
  const { input, bytes } = calls[0];
  expect(bytes).toBeLessThanOrEqual(32 * 1024);
  expect(input.targets[0].title).toBe(text.slice(0, 512));
  expect(input.targets[0].originalTitle).toBe(text.slice(0, 512));
  expect(input.targets[0].imdbId).toBe(text.slice(0, 80));
  expect(input.releases[0].title).toBe(text.slice(0, 512));
  expect(input.releases[0].provider).toBe(text.slice(0, 120));
  expect(input.releases[0].attributes).toHaveLength(20);
  for (const attribute of input.releases[0].attributes) {
    expect(attribute.name.length).toBeLessThanOrEqual(80);
    expect(attribute.value).toBe(text.slice(0, 120));
  }
  expect(JSON.stringify(input)).not.toContain("secret-key");
  expect(candidate.title).toBe(text);
});

test("RSS matcher splits by serialized bytes across both axes, covers every pair and remaps local indices", async () => {
  const batch = oversizedBatch();
  const { matcher, calls } = captureMatcher((input) => ({
    matches: input.targets.map((target) => ({
      targetIndex: target.index,
      releaseIndexes: input.releases.map((release) => release.index),
      reason: "Possible identity",
    })),
  }));
  const matches = await matcher(batch);
  expect(calls.length).toBeGreaterThan(1);
  expect(
    calls.some((call) => call.input.targets.length < batch.targets.length),
  ).toBe(true);
  expect(
    calls.some((call) => call.input.releases.length < batch.offers.length),
  ).toBe(true);
  const pairs: string[] = [];
  for (const { input, bytes } of calls) {
    expect(bytes).toBeLessThanOrEqual(32 * 1024);
    expect(input.targets.map((target) => target.index)).toEqual(
      input.targets.map((_, index) => index),
    );
    expect(input.releases.map((release) => release.index)).toEqual(
      input.releases.map((_, index) => index),
    );
    for (const target of input.targets)
      for (const release of input.releases) {
        pairs.push(`${target.tmdbId}:${release.title.split(" ")[0]}`);
      }
  }
  expect(pairs).toHaveLength(batch.targets.length * batch.offers.length);
  expect(new Set(pairs).size).toBe(pairs.length);
  expect(matches).toHaveLength(batch.targets.length);
  for (const target of batch.targets) {
    const matched = matches.find(
      (match) => match.targetId === target.targetId,
    )!;
    expect([...matched.releaseIndexes].sort((a, b) => a - b)).toEqual(
      batch.offers.map((_, index) => index),
    );
    expect(matched.reason.length).toBeLessThanOrEqual(1000);
  }
});

for (const invalid of ["target", "release"] as const) {
  test(`RSS matcher rejects a ${invalid} index outside its sub-batch even if valid in the original batch`, async () => {
    const batch = oversizedBatch();
    const { matcher, calls } = captureMatcher((input) => ({
      matches: [
        {
          targetIndex: invalid === "target" ? input.targets.length : 0,
          releaseIndexes: [invalid === "release" ? input.releases.length : 0],
          reason: "Invalid index",
        },
      ],
    }));
    await expect(matcher(batch)).rejects.toThrow("unknown target or release");
    expect(calls).toHaveLength(1);
    expect(calls[0].input.targets.length).toBeLessThan(batch.targets.length);
    expect(calls[0].input.releases.length).toBeLessThan(batch.offers.length);
  });
}
