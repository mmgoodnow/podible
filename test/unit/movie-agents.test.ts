import { expect, test } from "bun:test";
import type { ModelProvider, ModelRequest } from "@openai/agents-core";
import { Usage } from "@openai/agents-core";
import { createMovieDecider } from "../../src/movies/agents";
import { defaultSettings } from "../../src/settings";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { fixtureFacts, fixtureMovie } from "../fixtures/movie-services";
import { TEXT_AGENT_MODEL } from "../../src/ai-models";

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
  expect(modelName).toBe(TEXT_AGENT_MODEL);
  expect(JSON.stringify(requests)).not.toContain("hidden");
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
