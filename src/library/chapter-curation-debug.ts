import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";

type DebugContext = {
  manifestation: { id: number };
  debugEventLogPath?: string;
  debugTraceDir?: string;
};

type AgentUsageSummary = {
  requests: number;
  inputTokens: number;
  cachedInputTokens: number;
  uncachedInputTokens: number;
  outputTokens: number;
  reasoningTokens: number;
  totalTokens: number;
};

type AgentUsagePrice = {
  inputUsdPerMillion: number;
  cachedInputUsdPerMillion: number;
  outputUsdPerMillion: number;
  source: string;
};

const openAiTokenPrices: Array<{ prefix: string; price: AgentUsagePrice }> = [
  {
    prefix: "gpt-5.4-mini",
    price: {
      inputUsdPerMillion: 0.75,
      cachedInputUsdPerMillion: 0.075,
      outputUsdPerMillion: 4.5,
      source: "OpenAI API pricing, standard text tokens, checked 2026-05-22",
    },
  },
  {
    prefix: "gpt-5.4-nano",
    price: {
      inputUsdPerMillion: 0.2,
      cachedInputUsdPerMillion: 0.02,
      outputUsdPerMillion: 1.25,
      source: "OpenAI API pricing, standard text tokens, checked 2026-05-22",
    },
  },
  {
    prefix: "gpt-5.4",
    price: {
      inputUsdPerMillion: 2.5,
      cachedInputUsdPerMillion: 0.25,
      outputUsdPerMillion: 15,
      source: "OpenAI API pricing, standard text tokens, checked 2026-05-22",
    },
  },
];

function normalizeEventText(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

function logChapterCurationProgress(ctx: Pick<DebugContext, "manifestation">, message: string): void {
  console.warn(`[chapter-curation] manifestation=${ctx.manifestation.id} ${message}`);
}

export function logChapterCurationEvent(ctx: Pick<DebugContext, "manifestation" | "debugEventLogPath">, event: Record<string, unknown>): void {
  const payload = {
    ts: new Date().toISOString(),
    manifestationId: ctx.manifestation.id,
    ...event,
  };
  if (ctx.debugEventLogPath) {
    try {
      appendFileSync(ctx.debugEventLogPath, `${JSON.stringify(payload)}\n`, "utf8");
    } catch (error) {
      console.warn(`[chapter-curation] manifestation=${ctx.manifestation.id} event_log_error=${JSON.stringify((error as Error).message)}`);
    }
  }
  if (typeof event.message === "string") logChapterCurationProgress(ctx, event.message);
}

function eventPreview(value: string, limit = 180): string {
  const text = normalizeEventText(value);
  return text.length > limit ? `${text.slice(0, limit)}...` : text;
}

function summarizeToolResultForEvent(result: unknown): unknown {
  if (!result || typeof result !== "object") return result;
  const record = result as Record<string, unknown>;
  if (Array.isArray(record.matches)) {
    return {
      matches: record.matches.length,
      firstMatches: record.matches.slice(0, 3).map((match) => {
        const item = match as Record<string, unknown>;
        return {
          epubNodeId: item.epubNodeId,
          title: item.title,
          startTime: item.startTime,
          wordOffset: item.wordOffset,
          targetWordOffset: item.targetWordOffset,
          relationToTarget: item.relationToTarget,
          text: typeof item.text === "string" ? eventPreview(item.text) : undefined,
        };
      }),
    };
  }
  if (Array.isArray(record.nodes)) {
    return {
      nodes: record.nodes.length,
      firstNodes: record.nodes.slice(0, 3).map((node) => {
        const item = node as Record<string, unknown>;
        return {
          id: item.id,
          title: item.title,
          matches: Array.isArray(item.matches) ? item.matches.length : undefined,
        };
      }),
    };
  }
  if (Array.isArray(record.chapters)) return { chapters: record.chapters.length, diagnostics: record.diagnostics };
  if (Array.isArray(record.utterances)) {
    return {
      startMs: record.startMs,
      endMs: record.endMs,
      utterances: record.utterances.length,
      text: typeof record.text === "string" ? eventPreview(record.text) : undefined,
    };
  }
  if (Array.isArray(record.phraseVariants)) {
    return {
      id: record.id,
      title: record.title,
      startWord: record.startWord,
      endWord: record.endWord,
      text: typeof record.text === "string" ? eventPreview(record.text) : undefined,
      phraseVariants: record.phraseVariants,
    };
  }
  if ("accepted" in record) return { accepted: record.accepted, kind: record.kind, errors: record.errors, warnings: record.warnings, instruction: record.instruction };
  if ("error" in record) return record;
  return record;
}

export function logSpanToolCall(ctx: DebugContext, span: unknown, toolName: string, input: unknown): void {
  const spanPath = typeof span === "object" && span && typeof (span as { path?: unknown }).path === "string" ? (span as { path: string }).path : "?";
  logChapterCurationEvent(ctx, {
    type: "span-tool-call",
    message: `recursive span=${spanPath} tool=${toolName} call`,
    span,
    toolName,
    input,
  });
}

export function logSpanToolResult(ctx: DebugContext, span: unknown, toolName: string, result: unknown): void {
  const spanPath = typeof span === "object" && span && typeof (span as { path?: unknown }).path === "string" ? (span as { path: string }).path : "?";
  logChapterCurationEvent(ctx, {
    type: "span-tool-result",
    message: `recursive span=${spanPath} tool=${toolName} result`,
    span,
    toolName,
    result: summarizeToolResultForEvent(result),
  });
}

export function logSpanToolError(ctx: DebugContext, span: unknown, toolName: string, error: unknown): void {
  const spanPath = typeof span === "object" && span && typeof (span as { path?: unknown }).path === "string" ? (span as { path: string }).path : "?";
  logChapterCurationEvent(ctx, {
    type: "span-tool-error",
    message: `recursive span=${spanPath} tool=${toolName} error=${JSON.stringify((error as Error).message ?? String(error))}`,
    span,
    toolName,
    error: {
      name: (error as Error).name,
      message: (error as Error).message,
    },
  });
}

export function writeChapterCurationTrace(ctx: Pick<DebugContext, "manifestation" | "debugTraceDir">, name: string, payload: unknown): string | undefined {
  if (!ctx.debugTraceDir) return undefined;
  try {
    mkdirSync(ctx.debugTraceDir, { recursive: true });
    const safeName = name.replace(/[^a-zA-Z0-9._-]+/g, "-").replace(/^-+|-+$/g, "");
    const tracePath = path.join(ctx.debugTraceDir, `${new Date().toISOString().replace(/[:.]/g, "-")}-${safeName || "trace"}.json`);
    writeFileSync(tracePath, JSON.stringify(payload, null, 2), "utf8");
    return tracePath;
  } catch (error) {
    console.warn(`[chapter-curation] manifestation=${ctx.manifestation.id} trace_write_error=${JSON.stringify((error as Error).message)}`);
    return undefined;
  }
}

function numberFromRecord(record: unknown, keys: string[]): number {
  if (!record || typeof record !== "object") return 0;
  const values = record as Record<string, unknown>;
  for (const key of keys) {
    const value = values[key];
    if (typeof value === "number" && Number.isFinite(value)) return value;
  }
  return 0;
}

function usageDetailsTotal(details: unknown, keys: string[]): number {
  if (!Array.isArray(details)) return 0;
  return details.reduce((sum, item) => sum + numberFromRecord(item, keys), 0);
}

function summarizeAgentRawResponseUsage(rawResponses: unknown[]): AgentUsageSummary {
  const summary: AgentUsageSummary = {
    requests: 0,
    inputTokens: 0,
    cachedInputTokens: 0,
    uncachedInputTokens: 0,
    outputTokens: 0,
    reasoningTokens: 0,
    totalTokens: 0,
  };
  for (const response of rawResponses) {
    if (!response || typeof response !== "object") continue;
    const usage = (response as Record<string, unknown>).usage;
    if (!usage || typeof usage !== "object") continue;
    const usageRecord = usage as Record<string, unknown>;
    summary.requests += numberFromRecord(usageRecord, ["requests"]);
    summary.inputTokens += numberFromRecord(usageRecord, ["inputTokens", "input_tokens"]);
    summary.outputTokens += numberFromRecord(usageRecord, ["outputTokens", "output_tokens"]);
    summary.totalTokens += numberFromRecord(usageRecord, ["totalTokens", "total_tokens"]);
    summary.cachedInputTokens += usageDetailsTotal(usageRecord.inputTokensDetails, ["cached_tokens", "cachedTokens"]);
    summary.reasoningTokens += usageDetailsTotal(usageRecord.outputTokensDetails, ["reasoning_tokens", "reasoningTokens"]);
  }
  summary.uncachedInputTokens = Math.max(0, summary.inputTokens - summary.cachedInputTokens);
  if (summary.requests === 0 && summary.totalTokens > 0) summary.requests = rawResponses.length;
  return summary;
}

function serializedErrorRawResponses(error: unknown): unknown[] {
  if (!error || typeof error !== "object") return [];
  const state = (error as Record<string, unknown>).state;
  if (!state || typeof state !== "object") return [];
  const modelResponses = (state as Record<string, unknown>).modelResponses;
  return Array.isArray(modelResponses) ? modelResponses : [];
}

function openAiPriceForModel(model: string): AgentUsagePrice | null {
  const normalized = model.trim();
  const match = openAiTokenPrices.find((entry) => normalized === entry.prefix || normalized.startsWith(`${entry.prefix}-`));
  return match?.price ?? null;
}

function estimateOpenAiUsageCostUsd(model: string, usage: AgentUsageSummary): { amountUsd: number; price: AgentUsagePrice } | null {
  const price = openAiPriceForModel(model);
  if (!price) return null;
  const amountUsd =
    (usage.uncachedInputTokens / 1_000_000) * price.inputUsdPerMillion +
    (usage.cachedInputTokens / 1_000_000) * price.cachedInputUsdPerMillion +
    (usage.outputTokens / 1_000_000) * price.outputUsdPerMillion;
  return {
    amountUsd: Math.round(amountUsd * 1_000_000) / 1_000_000,
    price,
  };
}

export function logAgentUsageEvent(
  ctx: DebugContext,
  input: {
    role: "audible-node-selection" | "curator" | "judge";
    model: string;
    rawResponses?: unknown[];
    serializedError?: unknown;
    span?: unknown;
  }
): void {
  const rawResponses = input.rawResponses ?? serializedErrorRawResponses(input.serializedError);
  const usage = summarizeAgentRawResponseUsage(rawResponses);
  if (usage.totalTokens <= 0 && usage.inputTokens <= 0 && usage.outputTokens <= 0) return;
  const cost = estimateOpenAiUsageCostUsd(input.model, usage);
  logChapterCurationEvent(ctx, {
    type: "agent-usage",
    message: `agent usage role=${input.role} model=${input.model} requests=${usage.requests} tokens=${usage.totalTokens}${
      cost ? ` cost_usd=${cost.amountUsd}` : ""
    }`,
    span: input.span,
    role: input.role,
    model: input.model,
    usage,
    cost,
  });
}
