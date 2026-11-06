import type { BooksRepo } from "../../src/repo";
import type { MetricRecord } from "../../src/media/transfer-metrics";
import fs from "node:fs";
import path from "node:path";

/** Synthetic persisted observations for UI acceptance, not a claim of daemon verification. */
export function seedTransferMetricsFixture(repo: BooksRepo, bookId: number) {
  const content = path.join(repo.getSettings().libraryRoot, ".podible", "content");
  fs.mkdirSync(content, { recursive: true });
  for (const [kind, size, extension, mime] of [["ebook", 1024, "epub", "application/epub+zip"], ["audio", 2048, "m4b", "audio/mp4"]] as const) {
    const file = path.join(content, `inventory-fixture.${extension}`);
    fs.writeFileSync(file, Buffer.alloc(size));
    const manifestation = repo.addManifestation({ bookId, kind });
    repo.addAsset({ bookId, manifestationId: manifestation.id, kind: "single", mime, totalSize: size,
      files: [{ path: file, size, start: 0, end: size, durationMs: kind === "audio" ? 1000 : 0 }] });
  }
  const clientId = "transmission:11111111-1111-4111-8111-111111111111";
  const rows = [1, 2].map(n => repo.reserveReuseTorrent({ clientId, nativeId: String(n).repeat(40) },
    { rootId: "books", relativePath: `reuse/metrics-${n}` }, `metrics-offer-${n}`, { media: "audio", targetId: bookId }, `metrics-operation-${n}`));
  repo.transferMetrics.inferTracker("books.tracker.example", "Book tracker", new Date().toISOString());
  repo.transferMetrics.inferTracker("screen.tracker.example", "Screen tracker", new Date().toISOString());
  const now = Date.now();
  for (let minute = 30; minute >= 0; minute--) {
    const at = new Date(now - minute * 60_000).toISOString();
    for (const [index, row] of rows.entries()) {
      const metric: MetricRecord = { id: row.id, observedAt: at, verified: true, state: "seeding", complete: true, crossSeed: index === 1,
        sizeBytes: (index ? 100 - 2 * minute : 100 - minute) * 1024 ** 3, bytesDone: (index ? 100 - 2 * minute : 100 - minute) * 1024 ** 3, uploadedEver: (31 - minute) * 1024 ** 3,
        downloadedEver: 10 * 1024 ** 3, uploadedBytes: (31 - minute) * 1024 ** 3, downloadedBytes: 10 * 1024 ** 3,
        uploadBytesPerSecond: Math.round((1 + Math.sin(minute * .4 + index)) * 1024 ** 2), downloadBytesPerSecond: 0,
        seedSeconds: (31 - minute) * 60, seedSecondsRaw: (31 - minute) * 60, resets: 0, attributionStable: true,
        contents: [{ media: "audio", targetId: bookId, title: "Fixture Active Book", href: `/book/${bookId}` }],
        trackers: [{ host: index ? "screen.tracker.example" : "books.tracker.example", state: "healthy" }] };
      const inventory = repo.transferMetrics.inventory(row.id - 1, 1)[0]!;
      repo.transferMetrics.writeRecord(inventory, metric);
    }
    repo.transferMetrics.finishCollection(at, null);
  }
}
