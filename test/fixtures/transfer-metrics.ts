import type { BooksRepo } from "../../src/repo";
import type { MetricRecord } from "../../src/media/transfer-metrics";

/** Synthetic persisted observations for UI acceptance, not a claim of daemon verification. */
export function seedTransferMetricsFixture(repo: BooksRepo, bookId: number) {
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
        sizeBytes: 100 * 1024 ** 3, bytesDone: 100 * 1024 ** 3, uploadedEver: (31 - minute) * 1024 ** 3,
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
