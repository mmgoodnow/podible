import { expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { createTvRetentionPlexReader } from "../../src/media/tv-retention-plex";
import { tvRetentionDigest } from "../../src/media/tv-retention-seeds";
import { TV_RETENTION_DEFAULT_HASH_BYTES, TV_RETENTION_DEFAULT_TIMEOUT_MS } from "../../src/media/tv-retention-types";

const input = { tvdbId: 700, episodes: [{ id: 1, key: "tvdb:7001", providerId: "7001" }],
  plexUrl: "http://plex.invalid", plexToken: "secret-token", plexSectionId: "3" };

test("owner-only history witness is unknown even with watched counters; sensitive bodies never escape", async () => {
  const reader = createTvRetentionPlexReader({ fetcher: (async (_url, init) => {
    expect(init?.method).toBe("GET");
    return Response.json({ MediaContainer: { size: 1, offset: 0, totalSize: 1, Metadata: [{
      historyKey: "/status/sessions/history/1", ratingKey: "101", librarySectionID: "3", type: "episode", accountID: "1", viewedAt: 1_700_000_000, viewCount: 999,
    }] } });
  }) as typeof fetch });
  expect(await reader(input)).toMatchObject({ state: "unknown", coverage: "unknown", activePlayback: "unknown" });
  const rejected = createTvRetentionPlexReader({ fetcher: async () => new Response("private mam_id secret-token", { status: 403 }) });
  const result = await rejected(input);
  expect(result.state).toBe("unknown"); expect(JSON.stringify(result)).not.toContain("secret-token"); expect(JSON.stringify(result)).not.toContain("mam_id");
});

test("uncooperative Plex transport still has a hard audit deadline", async () => {
  const reader = createTvRetentionPlexReader({ timeoutMs: 10, fetcher: () => new Promise<Response>(() => {}) });
  const start = performance.now(); expect((await reader(input)).state).toBe("unknown"); expect(performance.now() - start).toBeLessThan(500);
});

test("default hash budget accepts a sparse episode larger than the former 512 MiB limit with bounded reads", async () => {
  expect(TV_RETENTION_DEFAULT_HASH_BYTES).toBe(32 * 1024 ** 3); expect(TV_RETENTION_DEFAULT_TIMEOUT_MS).toBe(60_000);
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "tv-retention-proof-")), file = path.join(root, "episode.mkv");
  const size = 513 * 1024 ** 2;
  try {
    const handle = await fs.open(file, "wx"); try { await handle.truncate(size); } finally { await handle.close(); }
    let bytes = 0;
    const proof = await tvRetentionDigest(file, { remainingBytes: TV_RETENTION_DEFAULT_HASH_BYTES, deadline: performance.now() + TV_RETENTION_DEFAULT_TIMEOUT_MS,
      onBytesRead: (_path, count) => { expect(count).toBeLessThanOrEqual(1024 ** 2); bytes += count; } });
    expect(proof).toMatch(/^[a-f0-9]{64}$/); expect(bytes).toBe(size);
    await expect(tvRetentionDigest(file, { remainingBytes: size - 1, deadline: performance.now() + 1000 })).rejects.toThrow("budget");
  } finally { await fs.rm(root, { recursive: true, force: true }); }
}, 15_000);
