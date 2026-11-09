import { expect, test } from "bun:test";
import { Usage, type ModelProvider } from "@openai/agents-core";
import { planIndexerQuery } from "../../src/media/indexer-query-plan";
import { defaultSettings } from "../../src/settings";

const evidence = { media: "tv" as const, query: "Example S02", title: "Example", aliases: ["Rei"], numbering: ["tvdb:aired season=2"] };
function fixture(output: unknown) {
  let calls = 0;
  const provider: ModelProvider = {
    getModel: () => ({
      async getResponse(request) {
        calls++;
        expect(JSON.stringify(request.input)).toContain("Rei");
        return { usage: new Usage(), output: [{ type: "message", role: "assistant", status: "completed",
          content: [{ type: "output_text", text: JSON.stringify(output) }] }] };
      },
      async *getStreamedResponse() { throw new Error("Unexpected streaming response"); },
    }),
  };
  return { provider, calls: () => calls };
}

test("query planner shares a bounded text-only plan across provider searches", async () => {
  const expected = { primaryQuery: "Rei S02", alternateQuery: "Example S02" };
  const f = fixture(expected);
  const plan = () => planIndexerQuery(defaultSettings(), evidence, f.provider);
  expect(await Promise.all([plan(), plan()])).toEqual([expected, expected]);
  expect(f.calls()).toBe(1);
});

test("query planner cannot supply an invented ID or prevent a fallback search", async () => {
  const f = fixture({ primaryQuery: "Rei S02", alternateQuery: null, tvdbid: "123" });
  expect(await planIndexerQuery(defaultSettings(), evidence, f.provider)).toEqual({ primaryQuery: evidence.query, alternateQuery: null });
  expect(f.calls()).toBe(1);
});
