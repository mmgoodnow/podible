import { BroadcastChannel, isMainThread } from "node:worker_threads";
import { currentEffects } from "../runtime/execution-effects";
import type { BooksRepo } from "../repo";
import type { TorrentSnapshot } from "./torrent-client";

type Media = "movie" | "tv";
type Phase = "queued_to_ready" | "complete_to_ready" | "complete_to_publish" | "publish_to_ready" | "view_reconcile";
type Histogram = { buckets: number[]; count: number; sum: number };
type State = Partial<Record<Media, Partial<Record<Phase, Histogram>>>>;
const mediaTypes = ["movie", "tv"] as const;
const phaseNames = ["queued_to_ready", "complete_to_ready", "complete_to_publish", "publish_to_ready", "view_reconcile"] as const;
const key = "acquisition-latency:v1";
const buckets = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600, 1800, 3600, 7200, 21600, 86400];
const metric = "podible_acquisition_latency_seconds";
const finalizationMedia = ["tv", "movie", "other"] as const;
type FinalizationMedia = typeof finalizationMedia[number];
const finalizationPhases = ["read_wait", "sha256", "cooperative", "revalidate", "proof_persist"] as const;
type FinalizationTimes = Record<typeof finalizationPhases[number], number>;
type FinalizationSample = { media: FinalizationMedia; bytes: number; times: FinalizationTimes };
const finalizationBuckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 90, 120, 180, 300, 600];
const finalizationMetric = "podible_canonical_finalization_seconds";
const finalizationBytes = "podible_canonical_finalization_bytes_total";

function histogram(bounds: number[]): Histogram {
  return { buckets: bounds.map(() => 0), count: 0, sum: 0 };
}

function observeHistogram(sample: Histogram, bounds: number[], seconds: number) {
  sample.count++;
  sample.sum += seconds;
  for (const [i, bound] of bounds.entries()) if (seconds <= bound) sample.buckets[i]!++;
}

const finalizations = Object.fromEntries(finalizationMedia.map(media => [media, {
  bytes: 0,
  phases: Object.fromEntries(finalizationPhases.map(phase => [phase, histogram(finalizationBuckets)])),
}]));

function collectFinalization(sample: FinalizationSample) {
  if (!sample || !finalizationMedia.includes(sample.media) || !Number.isSafeInteger(sample.bytes) || sample.bytes < 0 ||
      !finalizationPhases.every(phase => Number.isFinite(sample.times?.[phase]) && sample.times[phase] >= 0)) return;
  const state = finalizations[sample.media]!;
  state.bytes += sample.bytes;
  for (const phase of finalizationPhases) observeHistogram(state.phases[phase]!, finalizationBuckets, sample.times[phase] / 1000);
}

// Process-scoped telemetry: the worker publishes only successful proof writes.
// Keep it off SQLite and out of the proof/replay protocol.
const finalizationChannel = new BroadcastChannel("podible:canonical-finalization:v1");
finalizationChannel.unref();
if (isMainThread) finalizationChannel.onmessage = event => collectFinalization(event.data);

export function recordCanonicalFinalization(bytes: number, times: FinalizationTimes) {
  const step = currentEffects()?.stepName;
  const media = step?.startsWith("tv:") ? "tv" : step?.startsWith("movie:") ? "movie" : "other";
  const sample = { media, bytes, times } satisfies FinalizationSample;
  if (isMainThread) collectFinalization(sample);
  else {
    try { finalizationChannel.postMessage(sample); }
    catch { /* Telemetry must not fail adoption after the durable write. */ }
  }
}

export function savedCompletionTime(observation: string | null | undefined): string | null {
  if (!observation) return null;
  try {
    const seen = JSON.parse(observation) as Partial<TorrentSnapshot>;
    return seen.complete && typeof seen.completedAt === "string" ? seen.completedAt : null;
  } catch { return null; }
}

function observe(state: State, media: Media, phase: Phase, seconds: number) {
  if (!Number.isFinite(seconds) || seconds < 0) return;
  const phases = state[media] ??= {};
  observeHistogram(phases[phase] ??= histogram(buckets), buckets, seconds);
}

export function recordPublicationLatency(repo: BooksRepo, input: {
  media: Media; acquisitionId: number; queuedAt: string; completeAt?: string | null;
  publishingEvent: string; publicationEvent: string;
}) {
  const { media, acquisitionId, queuedAt, completeAt, publishingEvent, publicationEvent } = input;
  try {
    repo.transaction(() => {
      const readyAt = Date.parse(repo.readAcquisitionEventTime(media, publicationEvent, "available") ?? "");
      const publishAt = Date.parse(repo.readAcquisitionEventTime(media, publishingEvent, "publishing") ?? "");
      const queued = Date.parse(queuedAt);
      if (!Number.isFinite(readyAt) || !Number.isFinite(queued) || readyAt < queued) return;
      if (!repo.claimAcquisitionLatency(media, acquisitionId)) return;
      const state = repo.getJsonState<State>(key) ?? {};
      const complete = Date.parse(completeAt ?? "");
      observe(state, media, "queued_to_ready", (readyAt - queued) / 1000);
      observe(state, media, "publish_to_ready", (readyAt - publishAt) / 1000);
      // A torrent that completed before this batch was queued is reuse, not a new download.
      if (complete >= queued && complete <= publishAt && publishAt <= readyAt) {
        observe(state, media, "complete_to_ready", (readyAt - complete) / 1000);
        observe(state, media, "complete_to_publish", (publishAt - complete) / 1000);
      }
      repo.setJsonState(key, state);
    });
  } catch {
    console.warn("[acquisition-latency] Could not save publication sample");
  }
}

export function recordViewLatency(repo: BooksRepo, media: Media, ms: number) {
  try {
    repo.transaction(() => {
      const state = repo.getJsonState<State>(key) ?? {};
      observe(state, media, "view_reconcile", ms / 1000);
      repo.setJsonState(key, state);
    });
  } catch {
    console.warn("[acquisition-latency] Could not save view sample");
  }
}

export async function measureView<T>(operation: () => Promise<T>, report?: (ms: number) => void): Promise<T> {
  const start = performance.now();
  const result = await operation();
  report?.(performance.now() - start);
  return result;
}

export function acquisitionLatencyPrometheus(repo: BooksRepo) {
  const state = repo.getJsonState<State>(key) ?? {};
  const lines = [`# HELP ${metric} Successful acquisition and filesystem reconciliation durations.`, `# TYPE ${metric} histogram`];
  for (const media of mediaTypes) {
    for (const phase of phaseNames) {
      const sample = state[media]?.[phase] ?? { buckets: buckets.map(() => 0), count: 0, sum: 0 };
      const labels = `media="${media}",phase="${phase}"`;
      for (const [i, bound] of buckets.entries()) lines.push(`${metric}_bucket{${labels},le="${bound}"} ${sample.buckets[i]}`);
      lines.push(`${metric}_bucket{${labels},le="+Inf"} ${sample.count}`, `${metric}_sum{${labels}} ${sample.sum}`,
        `${metric}_count{${labels}} ${sample.count}`);
    }
  }
  return lines.join("\n") + "\n";
}

export function canonicalFinalizationPrometheus() {
  const lines = [
    `# HELP ${finalizationMetric} Per-file canonical finalization phase duration for successful durable proof writes.`,
    `# TYPE ${finalizationMetric} histogram`,
    `# HELP ${finalizationBytes} Bytes hashed for successful durable canonical finalization proof writes.`,
    `# TYPE ${finalizationBytes} counter`,
  ];
  for (const media of finalizationMedia) {
    const state = finalizations[media]!;
    lines.push(`${finalizationBytes}{media="${media}"} ${state.bytes}`);
    for (const phase of finalizationPhases) {
      const sample = state.phases[phase]!, labels = `media="${media}",phase="${phase}"`;
      for (const [i, bound] of finalizationBuckets.entries())
        lines.push(`${finalizationMetric}_bucket{${labels},le="${bound}"} ${sample.buckets[i]}`);
      lines.push(`${finalizationMetric}_bucket{${labels},le="+Inf"} ${sample.count}`,
        `${finalizationMetric}_sum{${labels}} ${sample.sum}`, `${finalizationMetric}_count{${labels}} ${sample.count}`);
    }
  }
  return lines.join("\n") + "\n";
}
