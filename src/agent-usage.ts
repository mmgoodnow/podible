import { AsyncLocalStorage } from "node:async_hooks";
import type { ModelProvider, ModelResponse } from "@openai/agents-core";
import { TEXT_AGENT_MODEL } from "./ai-models";
import type { BooksRepo } from "./repo";

const key = "agent-usage:v1";
const scope = new AsyncLocalStorage<BooksRepo>();
const workflows = ["book-selection", "book-import", "book-feed", "book-upgrade", "chapter-curation", "chapter-judge",
  "movie", "tv", "tv-feed", "tv-episode-mapping", "indexer-query"] as const;
type Workflow = typeof workflows[number];
type Api = "responses" | "decisions";
type TokenUsage = { input: number; cached: number; cacheWrite: number; output: number };
type Totals = TokenUsage & { calls: number; missing: number; unpriced: number; usd: number; seconds: number };
type State = Partial<Record<`${Workflow}:${Api}:${string}`, Totals>>;
const count = (value: unknown) => typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : 0;
const empty = (): Totals => ({ calls: 0, missing: 0, unpriced: 0, input: 0, output: 0, cached: 0, cacheWrite: 0, usd: 0, seconds: 0 });

export function withAgentUsageAccounting<T>(repo: BooksRepo, work: () => T): T {
  return scope.run(repo, work);
}

/** Standard global text rates; this is an estimate, not a billing API. */
export function estimateAgentUsageUsd(model: string, api: Api, usage: TokenUsage, tier?: string): number | null {
  if (model !== "gpt-6-luna" || (tier && tier !== "default" && tier !== "auto")) return null;
  const long = usage.input > 272_000;
  const inputCost = (usage.input - usage.cached - usage.cacheWrite) * 0.10 + usage.cached * 0.01 + usage.cacheWrite * 0.125;
  const outputCost = api === "decisions" ? 0 : usage.output * 0.50;
  return ((long ? 2 : 1) * inputCost + (long ? 1.5 : 1) * outputCost) / 1_000_000;
}

export function recordAgentUsage(workflow: Workflow, api: Api, model: string | undefined, elapsedMs: number,
  raw?: Partial<TokenUsage>, tier?: string): void {
  const repo = scope.getStore();
  if (!repo) return;
  const label = model === TEXT_AGENT_MODEL || model?.startsWith(`${TEXT_AGENT_MODEL}-`) ? TEXT_AGENT_MODEL : "unknown";
  const input = count(raw?.input), output = count(raw?.output);
  const cached = Math.min(input, count(raw?.cached)), cacheWrite = Math.min(input - cached, count(raw?.cacheWrite));
  const usage = { input, output, cached, cacheWrite };
  const available = input + output > 0;
  const cost = available ? estimateAgentUsageUsd(label, api, usage, tier) : null;
  try {
    repo.transaction(() => {
      const state = repo.getJsonState<State>(key) ?? {};
      const total = state[`${workflow}:${api}:${label}`] ??= empty();
      total.calls++;
      total.missing += Number(!available);
      total.unpriced += Number(available && cost === null);
      total.input += input; total.output += output; total.cached += cached; total.cacheWrite += cacheWrite;
      total.usd += cost ?? 0;
      total.seconds += Number.isFinite(elapsedMs) ? Math.max(0, elapsedMs) / 1000 : 0;
      repo.setJsonState(key, state);
    });
  } catch {
    // A metering failure must never cause a paid call to be retried.
    console.warn("[agent-usage] Could not save usage metrics");
  }
}

function observeResponse(workflow: Workflow, model: string | undefined, started: number, response?: ModelResponse) {
  const detail = response?.usage.inputTokensDetails;
  const sum = (field: string) => detail?.reduce((total, entry) => total + count(entry[field]), 0);
  const data = response?.providerData;
  recordAgentUsage(workflow, "responses", typeof data?.model === "string" ? data.model : model, performance.now() - started, {
    input: response?.usage.inputTokens, output: response?.usage.outputTokens,
    cached: sum("cached_tokens"), cacheWrite: sum("cache_write_tokens") ?? 0,
  }, typeof data?.service_tier === "string" ? data.service_tier : undefined);
}

/** Instrument the existing non-streaming SDK calls, including every tool-loop turn. */
export function meterAgentProvider(provider: ModelProvider, workflow: Workflow): ModelProvider {
  return { async getModel(name) {
    const model = await provider.getModel(name);
    return {
      async getResponse(request) {
        const started = performance.now();
        let response: ModelResponse | undefined;
        try { return response = await model.getResponse(request); }
        finally { observeResponse(workflow, name, started, response); }
      },
      getStreamedResponse: model.getStreamedResponse.bind(model),
      ...(model.getRetryAdvice ? { getRetryAdvice: model.getRetryAdvice.bind(model) } : {}),
    };
  } };
}

export function agentUsagePrometheus(repo: BooksRepo): string {
  const state = repo.getJsonState<State>(key) ?? {};
  // Give Prometheus a zero baseline before the first paid call. Don't announce unused APIs.
  for (const workflow of workflows) {
    state[`${workflow}:responses:${TEXT_AGENT_MODEL}`] ??= empty();
    state[`${workflow}:responses:unknown`] ??= empty();
  }
  const rows = Object.entries(state).flatMap(([id, total]) => {
    const [workflow, api, model] = id.split(":");
    if (!total || !workflows.some(value => value === workflow) || (api !== "responses" && api !== "decisions") ||
        (model !== TEXT_AGENT_MODEL && model !== "unknown")) return [];
    return [{ total, labels: `workflow="${workflow}",api="${api}",model="${model}"` }];
  });
  const metrics: [string, keyof Totals, string][] = [
    ["model_calls_total", "calls", "Observed SDK model calls, including tool-loop turns and errors."],
    ["usage_missing_total", "missing", "Observed model calls with no token counts."],
    ["usage_unpriced_total", "unpriced", "Calls with usage but no applicable price."],
    ["input_tokens_total", "input", "Observed input tokens, including cached input."],
    ["cached_input_tokens_total", "cached", "Observed cached input tokens."],
    ["cache_write_tokens_total", "cacheWrite", "Observed cache write input tokens."],
    ["output_tokens_total", "output", "Observed output tokens, including reasoning tokens."],
    ["estimated_cost_usd_total", "usd", "Estimated standard text token cost, not provider billing."],
    ["model_call_seconds_total", "seconds", "Total model call wall time, excluding agent tools between calls."],
  ];
  return metrics.map(([suffix, field, help]) => {
    const name = `podible_agent_${suffix}`;
    const samples = rows.flatMap(({ total, labels }) => Number.isFinite(total[field]) && total[field] >= 0
      ? [`${name}{${labels}} ${total[field]}\n`] : []);
    return `# HELP ${name} ${help}\n# TYPE ${name} counter\n${samples.join("")}`;
  }).join("");
}
