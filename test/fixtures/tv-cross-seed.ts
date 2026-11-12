import { createHash, randomUUID } from "node:crypto";
import { crossSeedCommandSchema, crossSeedStateKey } from "../../src/media/cross-seed-state";
import { planContentReuse } from "../../src/media/reuse-plan";
import type { BooksRepo } from "../../src/repo";

export function seedTvCrossSeed(repo: BooksRepo, targetId: number, title: string, fileIds: string[] = [], relativePath = "private-path") {
  const operationId = randomUUID(), selectionCopyId = randomUUID();
  const infoHash = createHash("sha1").update(operationId).digest("hex");
  const scope = { media: "tv", targetId } as const;
  const command = crossSeedCommandSchema.parse({ operationId, selectionCopyId, scope });
  const job = repo.createJob({ type: "torrent_reuse" });
  const ref = { clientId: "transmission:fixture", nativeId: infoHash };
  const location = { rootId: "tv", relativePath };
  const record = repo.reserveReuseTorrent(ref, location, "selected-copy", scope, operationId);
  repo.setJsonState(`cross-seed:selected-copy:${selectionCopyId}`, { offer: {
    title: title + " https://tracker.invalid/private-token", provider: "Fixture Indexer", url: "https://private.invalid/download",
  } });
  repo.setJsonState(crossSeedStateKey(operationId), { command, jobId: job.id, outcome: {
    kind: "seeding", torrentId: record.id, reusedBytes: 42, infoHash, reason: "private diagnostic",
  } });
  if (fileIds.length) {
    const plan = planContentReuse({
      candidate: { identity: { v1: infoHash }, files: fileIds.map((_, i) => ({ path: `episode-${i}.mkv`, size: 10 })) },
      sources: fileIds.map((id, i) => ({
        id, path: `/fixture/episode-${i}.mkv`, size: 10,
        root: { path: "/fixture", device: "1", inode: "1",
          marker: { name: "marker", ownerId: "fixture", sha256: "0".repeat(64), device: "1", inode: "2" } },
        identity: { device: "1", inode: String(i + 3), size: 10, mtimeNs: "0", ctimeNs: "0" },
      })),
      policy: { version: 1, minReuseRatio: 1, maxExtraDownloadBytes: 0 },
    });
    repo.crossSeed.recordOptimistic(operationId, { planId: plan.id, ref, location, plan });
  }
  return { infoHash, torrentId: record.id };
}
