import { randomUUID } from "node:crypto";
import type { BooksRepo } from "../../src/repo";
import { crossSeedCommandHash, crossSeedCommandSchema, crossSeedStateKey } from "../../src/media/cross-seed-state";

export function makeReleaseFeedFixture(repo: BooksRepo) {
  const stamp = "2026-11-14T20:00:00.000Z";
  const book = repo.createBook({ title: "Picked Fixture Book", author: "Fixture Author" }).id;
  const feedId = "autobrr:release-feed-ordinals";
  repo.releaseInbox.ensureFeed(feedId, "Fixture Indexer", "audio", stamp);
  const [first, , , second, pending, picked, ignored] = repo.releaseInbox.ingest(feedId,
    ["First cross-seed release", "Older skipped audiobook", "Newer skipped audiobook", "Second cross-seed release",
      "Pending cross-seed release", "Picked audiobook release", "Ignored audiobook release"]
      .map((title, i) => ({ title, mediaType: "audio", provider: "Fixture Indexer", guid: String(i), infoHash: null,
        seeders: 10, leechers: 1, sizeBytes: null, raw: {}, url: "https://provider.invalid/torrent" })), stamp);
  repo.releaseInbox.recordPick(picked, "audio", book, "initial");
  const recordSeed = (releaseId: number, torrentId: number, sourceIds: string[], at?: string) => {
    const command = crossSeedCommandSchema.parse({ operationId: randomUUID(), releaseId, scope: { media: "audio", targetId: book } });
    const receipt = { command, commandHash: crossSeedCommandHash(command), jobId: 1, createdAt: stamp };
    if (at) repo.crossSeed.recordOutcome(receipt, { kind: "seeding", reason: "Fixture seed", at, torrentId }, sourceIds);
    else repo.setJsonState(crossSeedStateKey(command.operationId), receipt);
    return receipt;
  };
  return { first, second, pending, picked, ignored, recordSeed, book };
}
