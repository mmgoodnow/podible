import { describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import { logAgentUsageEvent } from "../../src/library/chapter-curation-debug";

function loggedUsage(inputTokensDetails: Record<string, number> | Array<Record<string, number>>) {
  const dir = mkdtempSync(path.join(tmpdir(), "podible-curation-usage-"));
  const debugEventLogPath = path.join(dir, "events.jsonl");
  logAgentUsageEvent(
    { manifestation: { id: 1 }, debugEventLogPath },
    {
      role: "curator",
      model: "gpt-5.6-luna",
      rawResponses: [
        {
          usage: {
            requests: 1,
            inputTokens: 2_006,
            outputTokens: 300,
            totalTokens: 2_306,
            inputTokensDetails,
            outputTokensDetails: [{ reasoning_tokens: 100 }],
          },
        },
      ],
    }
  );
  return JSON.parse(readFileSync(debugEventLogPath, "utf8").trim());
}

describe("chapter curation usage logging", () => {
  test("prices cached GPT-5.6 Luna input at the current rate", () => {
    const event = loggedUsage({ cached_tokens: 1_920, cache_write_tokens: 0 });

    expect(event.usage).toEqual({
      requests: 1,
      inputTokens: 2_006,
      cachedInputTokens: 1_920,
      cacheWriteInputTokens: 0,
      uncachedInputTokens: 86,
      outputTokens: 300,
      reasoningTokens: 100,
      totalTokens: 2_306,
    });
    expect(event.cost.amountUsd).toBe(0.000416);
  });

  test("records and prices GPT-5.6 cache writes separately", () => {
    const event = loggedUsage([{ cached_tokens: 0, cache_write_tokens: 1_920 }]);

    expect(event.usage.cacheWriteInputTokens).toBe(1_920);
    expect(event.usage.uncachedInputTokens).toBe(86);
    expect(event.cost.amountUsd).toBe(0.000857);
    expect(event.cost.price.cacheWriteInputUsdPerMillion).toBe(0.25);
  });
});
