import { expect, test } from "bun:test";
import type { ModelProvider, ModelRequest } from "@openai/agents-core";
import { Usage } from "@openai/agents-core";
import { createMovieDecider } from "../../src/movies/agents";
import { defaultSettings } from "../../src/settings";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { fixtureFacts, fixtureMovie } from "../fixtures/movie-services";
import { TEXT_AGENT_MODEL } from "../../src/ai-models";
import type { MovieOffer } from "../../src/movies/offers";

test("movie agent uses the shared model, bounded structured selection and no provider URLs in prompts", async () => {
  const requests: ModelRequest[] = [];
  let modelName: string | undefined;
  const provider: ModelProvider = {
    getModel(name) {
      modelName = name;
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
                content: [
                  {
                    type: "output_text",
                    text: JSON.stringify({
                      index: 0,
                      facts: fixtureFacts(),
                      reason: "Matches target film and theatrical cut",
                    }),
                  },
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
  const decider = createMovieDecider(defaultSettings(), provider);
  const result = await decider.select({
    movie: fixtureMovie,
    policy: defaultMovieQualityPolicy,
    current: null,
    offers: [
      {
        key: "opaque",
        title: "Untagged Fixture Film",
        provider: "test",
        sizeBytes: 1000,
        seeders: 3,
        infoHash: null,
        url: "https://secret.invalid/?apikey=hidden",
        attributes: { private: "hidden" },
      },
    ],
  });
  expect(result.offerKey).toBe("opaque");
  expect(result.matchingOffers).toEqual([]);
  expect(modelName).toBe(TEXT_AGENT_MODEL);
  expect(JSON.stringify(requests)).not.toContain("hidden");
});

function selectionProvider(outputs: unknown[]) {
  const requests: ModelRequest[] = [];
  const model: ModelProvider = { getModel: () => ({
    async getResponse(request) {
      requests.push(request);
      const value = outputs[requests.length - 1];
      return { usage: new Usage(), output: Array.isArray(value) ? value : [{ type: "message" as const,
        role: "assistant" as const, status: "completed" as const,
        content: [{ type: "output_text" as const, text: JSON.stringify(value) }] }] };
    }, async *getStreamedResponse() { throw new Error("unused"); },
  }) };
  return { requests, model };
}
const selectionOffer: MovieOffer = { key: "base", title: "Film 1080p WEB encode A", provider: "Tracker A", sizeBytes: 1000,
  seeders: 3, url: "https://secret.invalid/?passkey=hidden", infoHash: "a".repeat(40),
  attributes: { attr: [{ name: "downloadvolumefactor", value: "1" }, { name: "private", value: "hidden" }], description: "hidden" } };
function assessment(indexes: number[]) {
  return [{ type: "function_call", name: "assessCrossSeedability", callId: "coverage",
    arguments: JSON.stringify({ indexes }), status: "completed" }];
}
function assessmentOutputs(request: ModelRequest) {
  if (!Array.isArray(request.input)) throw new Error("Expected agent tool history");
  return request.input.flatMap(item => item.type === "function_call_result" && item.name === "assessCrossSeedability"
    ? [JSON.parse(typeof item.output === "string" ? item.output : !Array.isArray(item.output) && item.output.type === "text"
      ? item.output.text : "null")] : []);
}

test("movie selector executes distinct-tracker evidence tool and retains all semantic copies, including same-tracker uploads", async () => {
  const offers = [selectionOffer, { ...selectionOffer, key: "second-upload", provider: " TRACKER A ", infoHash: "b".repeat(40) },
    { ...selectionOffer, key: "free-copy", provider: "Tracker B", infoHash: "c".repeat(40), attributes: { attr: { name: "freeleech", value: "true" }, secret: "hidden" } },
    { ...selectionOffer, key: "different-cut", title: "Film extended alternate encode" }];
  const fake = selectionProvider([assessment([0, 1, 2]), {
    index: 2, matchingIndexes: [0, 1, 2], facts: fixtureFacts(), reason: "Equal quality/coverage; two trackers and a viable freeleech source.",
  }]);
  const result = await createMovieDecider(defaultSettings(), fake.model).select({ movie: fixtureMovie,
    policy: defaultMovieQualityPolicy, current: null, offers });
  expect(result.offerKey).toBe("free-copy");
  expect(result.matchingOffers).toEqual([offers[0]!, offers[1]!]);
  expect(result.matchingOffers?.[0]).toBe(offers[0]!);
  expect(fake.requests).toHaveLength(2);
  expect(assessmentOutputs(fake.requests[1]!)[0]).toMatchObject({ preference: "tie-break", distinctTrackerCount: 2,
    sizeEvidence: { distinctCount: 1 }, hashEvidence: { distinctCount: 3 } });
  expect(assessmentOutputs(fake.requests[1]!)[0].offers[2].downloadVolumeFactor).toBe(0);
  expect(JSON.stringify(fake.requests)).not.toContain("hidden");
  const instructions = JSON.stringify(fake.requests[0]!.systemInstructions);
  expect(instructions).toContain("Hard constraints, quality tiers/preferences, identity and episode coverage always win");
  expect(instructions).toContain("semantically");
  expect(instructions).toContain("Do not perform extra searches solely");
  expect(instructions).toContain("not proof of additional torrent registrations or new seeding coverage");
  if (!Array.isArray(fake.requests[0]!.input)) throw new Error("Expected SDK input");
  const message = fake.requests[0]!.input.find(item => item.type === "message" && item.role === "user");
  expect(JSON.stringify(message)).toContain("selectionPreference");
});

test("off still exposes evidence and records matches without changing the agent's source selection", async () => {
  const offers = [selectionOffer, { ...selectionOffer, key: "free", provider: "B", attributes: { attr: { name: "freeleech", value: true } } }];
  const fake = selectionProvider([assessment([0, 1]), { index: 0, matchingIndexes: [1], facts: fixtureFacts(), reason: "Source chosen without coverage ranking." }]);
  const settings = defaultSettings();
  settings.movies.crossSeedPreference = "off";
  const result = await createMovieDecider(settings, fake.model).select({ movie: fixtureMovie,
    policy: defaultMovieQualityPolicy, current: null, offers });
  expect(result.offerKey).toBe(offers[0]!.key);
  expect(result.matchingOffers).toEqual([offers[1]!]);
  expect(assessmentOutputs(fake.requests[1]!)[0]).toMatchObject({ preference: "off", distinctTrackerCount: 2 });
  expect(JSON.stringify(fake.requests[0]!.systemInstructions)).toContain('do not rank by tracker coverage or download volume factors');
});

test("bad grouping tool indexes return feedback, while invalid final alternatives are rejected", async () => {
  const fake = selectionProvider([assessment([0, 0]), { index: 0, matchingIndexes: [], facts: fixtureFacts(), reason: "No proven alternatives." }]);
  await createMovieDecider(defaultSettings(), fake.model).select({ movie: fixtureMovie,
    policy: defaultMovieQualityPolicy, current: null, offers: [selectionOffer] });
  expect(assessmentOutputs(fake.requests[1]!)[0].error).toContain("unique known");
  for (const [index, matchingIndexes] of [[0, [1]], [0, [0, 0]], [null, [0]]] as const) {
    const bad = selectionProvider([{ index, matchingIndexes, facts: fixtureFacts(), reason: "Invalid output." }]);
    await expect(createMovieDecider(defaultSettings(), bad.model).select({ movie: fixtureMovie,
      policy: defaultMovieQualityPolicy, current: null, offers: [selectionOffer] })).rejects.toThrow();
  }
});

test("import agent checks real policy in its tool loop without escalating irrelevant unknown facts", async () => {
  const requests: ModelRequest[] = [];
  const decision = {
    fileIndex: 0,
    facts: { identityVerified: true, cut: null, source: "web", proper: null },
    reason:
      "Matching playable feature; no edition or language restriction was requested",
  };
  const provider: ModelProvider = {
    getModel: () => ({
      getResponse: async (request) => {
        requests.push(request);
        return {
          usage: new Usage(),
          output:
            requests.length === 1
              ? [
                  {
                    type: "function_call",
                    callId: "assess-1",
                    name: "assessCandidate",
                    arguments: JSON.stringify({
                      fileIndex: 0,
                      facts: decision.facts,
                    }),
                    status: "completed",
                  },
                ]
              : [
                  {
                    type: "message",
                    role: "assistant",
                    status: "completed",
                    content: [
                      { type: "output_text", text: JSON.stringify(decision) },
                    ],
                  },
                ],
        };
      },
      getStreamedResponse: async function* () {
        throw new Error("unused");
      },
    }),
  };
  const result = await createMovieDecider(defaultSettings(), provider).inspect({
    movie: fixtureMovie,
    policy: defaultMovieQualityPolicy,
    offer: {
      key: "test",
      title: "A Fixture Film",
      provider: "fixture",
      sizeBytes: null,
      seeders: null,
      url: "",
      infoHash: null,
      attributes: {},
    },
    selection: { offerKey: "test", facts: fixtureFacts(), reason: "Selected" },
    files: [
      {
        index: 0,
        name: "feature.mkv",
        sizeBytes: 1234,
        probe: {
          streams: [
            {
              codec_type: "video",
              codec_name: "h264",
              width: 1920,
              height: 1080,
            },
            { codec_type: "audio", tags: { language: "hin" } },
          ],
        },
      },
    ],
  });
  expect(result.fileIndex).toBe(0);
  expect(requests).toHaveLength(2);
  expect(JSON.stringify(requests[1]?.input)).toContain("eligible");
  expect(JSON.stringify(requests[1]?.input)).toContain("import");
});
