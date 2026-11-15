import { Database } from "bun:sqlite";
import { agentUsagePrometheus, estimateAgentUsageUsd, withAgentUsageAccounting } from "../../src/agent-usage";
import { TEXT_AGENT_MODEL } from "../../src/ai-models";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { createTvFeedMatcher, type TvFeedRoutingPolicy } from "../../src/tv/feed-agent";
import type { TvFeedMatch } from "../../src/tv/feed";
import { sparseTvFeedCases, tvFeedCases, type TvFeedCase } from "../fixtures/tv-feed-eval";

export const policies: Record<string, TvFeedRoutingPolicy> = {
  baseline: {}, compact95: { compact: true }, compact90: { compact: true, noMatchConfidence: 0.9 },
  compact85: { compact: true, noMatchConfidence: 0.85 }, binary90: { compact: true, negativeOnly: true, noMatchConfidence: 0.9 },
  responses: { responsesOnly: true }, compactResponses: { responsesOnly: true, compact: true },
  gate95: { batchNegativeOnly: true }, gate90: { batchNegativeOnly: true, noMatchConfidence: 0.9 },
  gate85: { batchNegativeOnly: true, noMatchConfidence: 0.85 },
  workGate75: { batchNegativeOnly: true, workOnlyGate: true, noMatchConfidence: 0.75 },
  workGate60: { batchNegativeOnly: true, workOnlyGate: true, noMatchConfidence: 0.6 },
};

export function scoreRoutes(fixture: TvFeedCase, matches: TvFeedMatch[]) {
  const actual = fixture.expected.map(() => new Set<number>());
  const known = new Set(fixture.input.targets.map(x => x.targetId));
  for (const match of matches) {
    if (!known.has(match.targetId)) throw new Error("Unknown target");
    for (const index of match.releaseIndexes) {
      if (!Number.isInteger(index) || index < 0 || index >= actual.length) throw new Error("Unknown offer");
      actual[index]!.add(match.targetId);
    }
  }
  const missed: [number, number][] = [], extra: [number, number][] = [];
  let positives = 0;
  for (const [i, expected] of fixture.expected.entries()) {
    positives += expected.length;
    for (const target of expected) if (!actual[i]!.has(target)) missed.push([i, target]);
    for (const target of actual[i]!) if (!expected.includes(target)) extra.push([i, target]);
  }
  return { positives, missed, extra };
}

async function replay(fixture: TvFeedCase, policy: TvFeedRoutingPolicy, apiKey: string) {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const gateScores: { choice: string; confidence: number | null; probability: number | null }[] = [];
  const transport = async (url: string, init: RequestInit) => {
    const response = await fetch(url, init);
    if (!policy.batchNegativeOnly || !response.ok) return response;
    const raw = await response.clone().json().catch(() => null) as { answers?: { name?: unknown; choice?: unknown;
      confidence?: unknown; probabilities?: { value?: unknown; probability?: unknown }[] }[] } | null;
    const answer = Array.isArray(raw?.answers) ? raw.answers[0] : undefined;
    if (answer?.name === "batch" && (answer.choice === "no_match" || answer.choice === "review")) {
      const score = Array.isArray(answer.probabilities) && answer.probabilities.find(row => row?.value === answer.choice);
      gateScores.push({ choice: answer.choice, confidence: typeof answer.confidence === "number" ? answer.confidence : null,
        probability: score && typeof score.probability === "number" ? score.probability : null });
    }
    return response;
  };
  const matcher = createTvFeedMatcher(defaultSettings({ agents: { apiKey } }), undefined, fixture.mode, transport, policy);
  const started = performance.now();
  const matches = await withAgentUsageAccounting(repo, () => matcher(fixture.input));
  const ms = Math.round(performance.now() - started);
  const metrics = agentUsagePrometheus(repo).split("\n");
  const count = (name: string, api: string) => metrics.filter(line => line.startsWith(`podible_agent_${name}{`)
    && line.includes(`workflow="tv-feed",api="${api}"`)).reduce((n, line) => n + Number(line.split(" ").at(-1)), 0);
  const usage = Object.fromEntries((["decisions", "responses"] as const).map(api => {
    const input = count("input_tokens_total", api), output = count("output_tokens_total", api);
    return [api, {
      calls: count("model_calls_total", api), missing: count("usage_missing_total", api), unpriced: count("usage_unpriced_total", api),
      input, output, cached: count("cached_input_tokens_total", api), cacheWrite: count("cache_write_tokens_total", api),
      usd: count("estimated_cost_usd_total", api),
      // The fixtures are below the long-context rate boundary, including in aggregate.
      uncachedUsd: input <= 272_000 ? estimateAgentUsageUsd(TEXT_AGENT_MODEL, api, { input, output, cached: 0, cacheWrite: 0 }) : null,
    }];
  }));
  const reasons = Object.fromEntries(metrics.flatMap(line => {
    const match = /^podible_agent_feed_decisions_total\{question="tv_feed",reason="([a-z_]+)"\} (\d+)$/.exec(line);
    return match && Number(match[2]) ? [[match[1], Number(match[2])]] : [];
  }));
  return { ...scoreRoutes(fixture, matches), usage, reasons, ms, ...(policy.batchNegativeOnly ? { gateScores } : {}) };
}

async function main() {
  const args = new Map(process.argv.slice(2).map(arg => {
    const match = /^--(split|policies|repeats|case)=(.+)$/.exec(arg);
    if (!match) throw new Error("Expected --split=dev|holdout|sparse|all --policies=baseline,compact95,... --repeats=1..5 --case=fixture-id");
    return [match[1], match[2]];
  }));
  const apiKey = process.env.OPENAI_API_KEY?.trim();
  if (!apiKey) throw new Error("OPENAI_API_KEY is required for live replay");
  const split = args.get("split") ?? "dev", repeats = Number(args.get("repeats") ?? 1);
  const names = (args.get("policies") ?? "baseline,compact95").split(",");
  if (!["all", "dev", "holdout", "sparse"].includes(split) || !Number.isInteger(repeats) || repeats < 1 || repeats > 5 ||
    names.some(name => !Object.hasOwn(policies, name))) throw new Error("Invalid split, policy, or repeat count");
  const cases = [...tvFeedCases, ...sparseTvFeedCases].filter(fixture => (split === "all" || fixture.split === split) && (!args.has("case") || fixture.id === args.get("case")));
  if (!cases.length) throw new Error("No matching fixtures");
  const jobs = Array.from({ length: repeats }, (_, run) => cases.flatMap((fixture, i) => {
    const rotation = (run + i) % names.length;
    return [...names.slice(rotation), ...names.slice(0, rotation)].map(policy => ({ run: run + 1, fixture, policy }));
  })).flat();
  let next = 0, errors = 0;
  await Promise.all(Array.from({ length: 3 }, async () => {
    while (next < jobs.length) {
      const { fixture, policy, run } = jobs[next++]!;
      const key = { case: fixture.id, split: fixture.split, mode: fixture.mode, policy, run, offers: fixture.expected.length };
      try { console.log(JSON.stringify({ ...key, ...await replay(fixture, policies[policy]!, apiKey) })); }
      catch {
        console.log(JSON.stringify({ ...key, error: "replay_failed" }));
        errors++;
      }
    }
  }));
  process.exitCode = errors ? 1 : 0;
}

if (import.meta.main) await main();
