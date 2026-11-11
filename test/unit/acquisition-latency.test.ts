import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { acquisitionLatencyPrometheus, recordPublicationLatency, recordViewLatency, savedCompletionTime } from "../../src/media/acquisition-latency";

test("publication latency is persisted once and reused torrents do not count as new downloads", () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const readyAt = "2026-11-10T03:02:00.000Z";
  // The workflow reads its existing durable available event when replaying.
  const event = "tv-published:1:2";
  const time = repo.readAcquisitionEventTime.bind(repo);
  repo.readAcquisitionEventTime = (media, key, stage) => media === "tv"
    ? stage === "available" && key === event ? readyAt : stage === "publishing" && key === "publish" ? "2026-11-10T03:01:50.000Z" : null
    : time(media, key, stage);
  const input = {
    media: "tv" as const, acquisitionId: 1, queuedAt: "2026-11-10T03:00:00.000Z",
    completeAt: "2026-11-10T03:01:00.000Z", publishingEvent: "publish",
    publicationEvent: event,
  };
  recordPublicationLatency(repo, input);
  recordPublicationLatency(repo, input);
  recordPublicationLatency(repo, { ...input, acquisitionId: 2, completeAt: "2026-11-09T03:01:00.000Z" });
  recordViewLatency(repo, "tv", 24);
  const exposition = acquisitionLatencyPrometheus(new BooksRepo(db));
  const metric = "podible_acquisition_latency_seconds";
  expect(exposition).toContain(`${metric}_count{media="tv",phase="complete_to_ready"} 1\n`);
  expect(exposition).toContain(`${metric}_sum{media="tv",phase="complete_to_publish"} 50\n`);
  expect(exposition).toContain(`${metric}_sum{media="tv",phase="publish_to_ready"} 20\n`);
  expect(exposition).toContain(`${metric}_count{media="tv",phase="queued_to_ready"} 2\n`);
  expect(exposition).toContain(`${metric}_bucket{media="tv",phase="view_reconcile",le="0.025"} 1\n`);
  expect(exposition).toContain(`${metric}_count{media="movie",phase="complete_to_ready"} 0\n`);
  expect(savedCompletionTime(JSON.stringify({ complete: false, completedAt: readyAt }))).toBeNull();
  expect(savedCompletionTime(JSON.stringify({ complete: true, completedAt: readyAt }))).toBe(readyAt);
});
