#!/usr/bin/env bun
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

type JsonRecord = Record<string, any>;

type Summary = {
  eventLog: string;
  startedAt: string | null;
  endedAt: string | null;
  wallSeconds: number | null;
  accepted: boolean | null;
  chapters: number | null;
  models: {
    curator: string | null;
    judge: string | null;
    base: string | null;
  };
  usage: {
    requests: number;
    tokens: number;
    costUsd: number;
    byRole: Record<string, { requests: number; tokens: number; costUsd: number }>;
  };
  spans: {
    started: number;
    splitAccepted: number;
    autoLeaf: number;
    deterministicAccepted: number;
    errors: number;
  };
  tools: Record<string, number>;
  judges: {
    accepted: number;
    rejected: number;
  };
  failures: string[];
};

function usage(): never {
  console.error("Usage: bun run summarize-curation-run -- <agent-events.jsonl>");
  process.exit(1);
}

function readEvents(filePath: string): JsonRecord[] {
  return readFileSync(filePath, "utf8")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .flatMap((line) => {
      try {
        return [JSON.parse(line) as JsonRecord];
      } catch {
        return [];
      }
    });
}

function timestampSeconds(start: string | null, end: string | null): number | null {
  if (!start || !end) return null;
  const startMs = Date.parse(start);
  const endMs = Date.parse(end);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return null;
  return Math.max(0, Math.round((endMs - startMs) / 100) / 10);
}

function roundMoney(value: number): number {
  return Math.round(value * 1_000_000) / 1_000_000;
}

function summarize(events: JsonRecord[], eventLog: string): Summary {
  const firstTs = events.find((event) => typeof event.ts === "string")?.ts ?? null;
  const lastTs = [...events].reverse().find((event) => typeof event.ts === "string")?.ts ?? null;
  const runStart = events.find((event) => event.type === "recursive-run-start");
  const mergeAccepted = [...events].reverse().find((event) => event.type === "recursive-merge-accepted");
  const mergeRejected = [...events].reverse().find((event) => event.type === "recursive-merge-rejected");

  const summary: Summary = {
    eventLog,
    startedAt: firstTs,
    endedAt: lastTs,
    wallSeconds: timestampSeconds(firstTs, lastTs),
    accepted: mergeAccepted ? true : mergeRejected ? false : null,
    chapters: typeof mergeAccepted?.chapters === "number" ? mergeAccepted.chapters : typeof mergeRejected?.chapters === "number" ? mergeRejected.chapters : null,
    models: {
      curator: typeof runStart?.curatorModel === "string" ? runStart.curatorModel : null,
      judge: typeof runStart?.judgeModel === "string" ? runStart.judgeModel : null,
      base: typeof runStart?.model === "string" ? runStart.model : null,
    },
    usage: {
      requests: 0,
      tokens: 0,
      costUsd: 0,
      byRole: {},
    },
    spans: {
      started: 0,
      splitAccepted: 0,
      autoLeaf: 0,
      deterministicAccepted: 0,
      errors: 0,
    },
    tools: {},
    judges: {
      accepted: 0,
      rejected: 0,
    },
    failures: [],
  };

  for (const event of events) {
    if (event.type === "agent-usage") {
      const role = typeof event.role === "string" ? event.role : "unknown";
      const requests = typeof event.usage?.requests === "number" ? event.usage.requests : 0;
      const tokens = typeof event.usage?.totalTokens === "number" ? event.usage.totalTokens : 0;
      const costUsd = typeof event.cost?.amountUsd === "number" ? event.cost.amountUsd : typeof event.costUsd === "number" ? event.costUsd : 0;
      summary.usage.requests += requests;
      summary.usage.tokens += tokens;
      summary.usage.costUsd += costUsd;
      const roleSummary = summary.usage.byRole[role] ?? { requests: 0, tokens: 0, costUsd: 0 };
      roleSummary.requests += requests;
      roleSummary.tokens += tokens;
      roleSummary.costUsd += costUsd;
      summary.usage.byRole[role] = roleSummary;
    }

    if (event.type === "span-start") summary.spans.started += 1;
    if (event.type === "span-split-accepted") summary.spans.splitAccepted += 1;
    if (event.type === "span-auto-leaf") summary.spans.autoLeaf += 1;
    if (event.type === "deterministic-boundary-accepted") summary.spans.deterministicAccepted += 1;
    if (event.type === "span-agent-error") {
      summary.spans.errors += 1;
      if (typeof event.message === "string") summary.failures.push(event.message);
    }

    if (event.type === "span-tool-call" || event.type === "agent-tool-call") {
      const toolName = typeof event.toolName === "string" ? event.toolName : "unknown";
      summary.tools[toolName] = (summary.tools[toolName] ?? 0) + 1;
    }

    if (event.type === "fulcrum-judge-result") {
      const accepted = typeof event.judgment?.accepted === "boolean" ? event.judgment.accepted : event.accepted;
      if (accepted === true) summary.judges.accepted += 1;
      else if (accepted === false) summary.judges.rejected += 1;
    }
  }

  summary.usage.costUsd = roundMoney(summary.usage.costUsd);
  for (const roleSummary of Object.values(summary.usage.byRole)) roleSummary.costUsd = roundMoney(roleSummary.costUsd);
  return summary;
}

const input = process.argv[2];
if (!input) usage();

const eventLog = path.resolve(input);
if (!existsSync(eventLog)) {
  console.error(`Event log not found: ${eventLog}`);
  process.exit(1);
}

console.log(JSON.stringify(summarize(readEvents(eventLog), eventLog), null, 2));
