import { expect, test } from "bun:test";
import { TEXT_AGENT_MODEL } from "../../src/ai-models";
import { createScreenIssueAssessor } from "../../src/issues/diagnosis";
import type { ScreenIssueEvidence } from "../../src/issues/contracts";

test("the screen issue agent receives a strict projection and runs once without tools or stored responses", async () => {
  const bodies: unknown[] = [];
  let status = 200;
  const assess = createScreenIssueAssessor({ apiKey: "fixture-api-key", timeoutMs: 2000 }, async (url, init) => {
    expect(new URL(String(url)).pathname).toEndWith("/responses");
    expect(init?.signal).toBeInstanceOf(AbortSignal);
    bodies.push(JSON.parse(String(init?.body)));
    return Response.json(status !== 200 ? { error: { message: "Fixture rate limit" } } : {
      status: "completed", output: [{ type: "message", role: "assistant", status: "completed",
        content: [{ type: "output_text", text: '{"focus":"recorded-content-loss"}' }] }],
    }, { status });
  });
  const evidence: ScreenIssueEvidence = {
    scope: "tv-show", category: "missing", filedFinding: "publication-recorded", currentFinding: "content-loss-recorded",
  };
  const invalid = { ...evidence, description: "private report text", title: "untrusted title" };
  await expect(assess(invalid)).rejects.toThrow();
  expect(bodies).toHaveLength(0);
  expect(await assess(evidence)).toEqual({ focus: "recorded-content-loss" });
  expect(bodies).toHaveLength(1);
  expect(bodies[0]).toMatchObject({ model: TEXT_AGENT_MODEL, input: JSON.stringify(evidence), tools: [], tool_choice: "none",
    store: false, max_output_tokens: 512, text: { format: { type: "json_schema", strict: true } } });
  expect(JSON.stringify(bodies[0])).not.toContain("fixture-api-key");
  status = 429;
  await expect(assess(evidence)).rejects.toThrow();
  expect(bodies).toHaveLength(2);
});
