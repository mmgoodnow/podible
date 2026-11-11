import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { Usage, type ModelRequest, type ModelResponse } from "@openai/agents-core";
import { agentUsagePrometheus, estimateAgentUsageUsd, meterAgentProvider, recordAgentUsage, withAgentUsageAccounting } from "../../src/agent-usage";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";

test("prices Responses and Decisions separately without double-counting cached tokens", () => {
  const usage = { input: 100_000, cached: 20_000, cacheWrite: 10_000, output: 2_000 };
  expect(estimateAgentUsageUsd("gpt-6-luna", "responses", usage)).toBeCloseTo(0.00945, 8);
  expect(estimateAgentUsageUsd("gpt-6-luna", "decisions", usage)).toBeCloseTo(0.00845, 8);
  const long = { input: 300_000, cached: 0, cacheWrite: 0, output: 2_000 };
  expect(estimateAgentUsageUsd("gpt-6-luna", "responses", long)).toBeCloseTo(0.0615, 8);
  expect(estimateAgentUsageUsd("gpt-6-luna", "responses", usage, "priority")).toBeNull();
  expect(estimateAgentUsageUsd("unknown", "responses", usage)).toBeNull();
});

test("persists each observed model turn, keeps failures separate, and exports no response data", async () => {
  using db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db);
  const label = '{workflow="book-selection",api="responses",model="gpt-6-luna"}';
  expect(agentUsagePrometheus(repo)).toContain(`podible_agent_model_calls_total${label} 0\n`);
  let calls = 0;
  const provider = meterAgentProvider({ getModel: () => ({
    async getResponse(): Promise<ModelResponse> {
      if (++calls === 3) throw new Error("SECRET_ERROR");
      return { output: [], responseId: "SECRET_RESPONSE", providerData: { model: "gpt-6-luna-2026-10-06", other: "SECRET_DATA" },
        usage: new Usage({ inputTokens: 100_000, inputTokensDetails: { cached_tokens: 20_000, cache_write_tokens: 10_000 }, outputTokens: 2_000 }) };
    },
    async *getStreamedResponse() { throw new Error("not used"); },
  }) }, "book-selection");
  await withAgentUsageAccounting(repo, async () => {
    const model = await provider.getModel("gpt-6-luna");
    const request = {} as ModelRequest;
    expect((await model.getResponse(request)).responseId).toBe("SECRET_RESPONSE");
    await model.getResponse(request);
    await expect(model.getResponse(request)).rejects.toThrow("SECRET_ERROR");
    recordAgentUsage("tv", "responses", "SECRET_MODEL", 50, { input: 100 });
  });
  const metrics = agentUsagePrometheus(new BooksRepo(db));
  expect(metrics).toContain(`podible_agent_model_calls_total${label} 3\n`);
  expect(metrics).toContain(`podible_agent_usage_missing_total${label} 1\n`);
  expect(metrics).toContain(`podible_agent_cached_input_tokens_total${label} 40000\n`);
  const cost = Number(metrics.split("\n").find(line => line.startsWith(`podible_agent_estimated_cost_usd_total${label}`))?.split(" ")[1]);
  expect(cost).toBeCloseTo(0.0189, 8);
  expect(metrics).toContain('podible_agent_usage_unpriced_total{workflow="tv",api="responses",model="unknown"} 1\n');
  expect(metrics).not.toContain("SECRET");
});
