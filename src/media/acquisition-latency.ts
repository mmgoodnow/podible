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
  const histogram = phases[phase] ??= { buckets: buckets.map(() => 0), count: 0, sum: 0 };
  histogram.count++;
  histogram.sum += seconds;
  for (const [i, bound] of buckets.entries()) if (seconds <= bound) histogram.buckets[i]!++;
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
