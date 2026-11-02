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
