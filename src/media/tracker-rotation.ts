import type { Database } from "bun:sqlite";
import path from "node:path";
import { z } from "zod";
import { readTorrentAnnounces, rewriteTorrentAnnounces } from "../library/torrent";
import { torrentLocation, type TorrentRecord } from "./torrent-records-schema";
import type { TransmissionRpc } from "./transmission-client";

const ownershipTags = { books: "Podible Books", movies: "Podible Movies", tv: "Podible TV" };
type RootId = keyof typeof ownershipTags;
type CacheRow = { key: string; provider: string | null; torrent_bytes: Uint8Array; updated_at: string };
const torrentSchema = z.object({
  id: z.number(), hashString: z.string(), downloadDir: z.string(), labels: z.array(z.string()), trackerList: z.string(),
});
const fields = Object.keys(torrentSchema.shape);
export class TrackerRotationError extends Error {}
const trackerTiers = (value: string) => value.trim().split("\n\n").map(tier => tier.split("\n"));
const renderTiers = (tiers: string[][]) => tiers.map(tier => tier.join("\n")).join("\n\n");
const urls = ({ primary, tiers }: ReturnType<typeof readTorrentAnnounces>) => [...new Set([
  ...(primary === null ? [] : [primary]), ...tiers.flat(),
])];
const safeUrl = (value: string) => {
  try {
    const url = new URL(value);
    return !/[\s\x00-\x1f\x7f]/.test(value) && ["http:", "https:", "udp:"].includes(url.protocol) &&
      Boolean(url.hostname) && !url.username && !url.password && !url.hash;
  } catch { return false; }
};

export async function rotateTrackerAnnounces(input: {
  db: Database; rpc: Pick<TransmissionRpc, "call">; clientId: string; provider: string; apply: boolean;
  roots: Partial<Record<RootId, string>>;
}) {
  const { db, rpc, clientId, provider, apply, roots } = input;
  const newest = db.query<CacheRow, [string]>(
    "SELECT key,provider,torrent_bytes,updated_at FROM torrent_cache WHERE provider=? ORDER BY updated_at DESC,rowid DESC LIMIT 1",
  );
  const donor = newest.get(provider);
  if (!donor) throw new TrackerRotationError("No cached metainfo for that provider");
  const announces = readTorrentAnnounces(donor.torrent_bytes);
  const newUrls = urls(announces);
  const tiers = announces.tiers.length ? announces.tiers : announces.primary ? [[announces.primary]] : [];
  const target = renderTiers(tiers);
  if (!target || !newUrls.every(safeUrl) || new Set(tiers.flat()).size !== tiers.flat().length)
    throw new TrackerRotationError("Newest cached torrent has no usable announce list");

  const result = { donorFetchedAt: donor.updated_at, trackerUrls: newUrls.length,
    cached: 0, registered: 0, skippedCache: 0, skippedRegistrations: 0 };
  const cacheRows = db.query<CacheRow, []>("SELECT key,provider,torrent_bytes,updated_at FROM torrent_cache");
  // URLs are credential-bearing evidence, not hostname aliases. Never log them.
  const owned = new Set<string>(), other = new Set<string>();
  for (const row of cacheRows.iterate()) {
    try {
      for (const url of urls(readTorrentAnnounces(row.torrent_bytes))) (row.provider === provider ? owned : other).add(url);
    } catch {
      if (row.provider === provider) result.skippedCache++;
    }
  }
  if (newUrls.some(url => other.has(url)))
    throw new TrackerRotationError("Newest announce list is also attributed to another provider");
  const belongs = (list: string[]) => list.length > 0 && list.every(url => owned.has(url) && !other.has(url));

  const rows = z.array(torrentSchema).parse((await rpc.call("torrent-get", { fields })).torrents);
  const binding = db.query<TorrentRecord, [string, string]>("SELECT * FROM media_torrents WHERE client_id=? AND native_id=?");
  function isOwned(row: z.infer<typeof torrentSchema>) {
    const hash = row.hashString;
    if (!/^[a-f0-9]{40}$/.test(hash)) return false;
    const record = binding.get(clientId, hash);
    if (record?.backend !== "transmission") return false;
    const { rootId, relativePath } = torrentLocation(JSON.parse(record.location_json));
    if (!Object.hasOwn(ownershipTags, rootId)) return false;
    const tag = ownershipTags[rootId as RootId], root = roots[rootId as RootId];
    return root !== undefined && path.isAbsolute(root) && (!record.root_path || record.root_path === root) &&
      record.ownership_tag === tag && row.labels.includes(tag) &&
      !/[\\\x00-\x1f]/.test(relativePath) && relativePath.split("/").every(p => p && p !== "." && p !== "..") &&
      row.downloadDir === path.join(root, relativePath);
  }
  function assertDonorUnchanged() {
    const latest = newest.get(provider);
    if (!latest || latest.key !== donor!.key || latest.updated_at !== donor!.updated_at ||
        !Buffer.from(latest.torrent_bytes).equals(donor!.torrent_bytes))
      throw new TrackerRotationError("Newest metainfo changed during rotation; preview again");
  }
  const read = async (hash: string) => z.array(torrentSchema).max(1).parse(
    (await rpc.call("torrent-get", { ids: [hash], fields })).torrents,
  )[0];

  // Leave old cache provenance available for a retry until all client writes are acknowledged.
  for (const row of rows) {
    if (!row.trackerList.trim() || row.trackerList.trim() === target) continue;
    const current = trackerTiers(row.trackerList).flat();
    if (!current.some(url => owned.has(url))) continue;
    if (!belongs(current) || !isOwned(row)) { result.skippedRegistrations++; continue; }
    if (apply) {
      const fresh = await read(row.hashString);
      if (!fresh || fresh.hashString !== row.hashString || fresh.id !== row.id ||
          fresh.trackerList !== row.trackerList || !isOwned(fresh))
        throw new TrackerRotationError("Registration changed during rotation; preview again");
      assertDonorUnchanged();
      await rpc.call("torrent-set", { ids: [row.hashString], trackerList: target });
      const changed = await read(row.hashString);
      if (!changed || changed.hashString !== row.hashString || changed.id !== row.id ||
          changed.trackerList.trim() !== target || !isOwned(changed))
        throw new TrackerRotationError("Transmission did not confirm the tracker change; preview again");
    }
    result.registered++;
  }

  const byKey = db.query<CacheRow, [string, string]>(
    "SELECT key,provider,torrent_bytes,updated_at FROM torrent_cache WHERE key=? AND provider=?",
  );
  const update = db.query("UPDATE torrent_cache SET torrent_bytes=? WHERE key=? AND provider=? AND torrent_bytes=? AND updated_at=?");
  // Keep timestamps as fetch provenance. Only replace bytes for cache rows still matching the scan.
  for (const key of (db.query("SELECT key FROM torrent_cache WHERE provider=?").all(provider) as { key: string }[])) {
    const row = byKey.get(key.key, provider);
    if (!row) continue;
    let previous;
    try { previous = readTorrentAnnounces(row.torrent_bytes); } catch { continue; }
    if (JSON.stringify(previous) === JSON.stringify(announces)) continue;
    if (!belongs(urls(previous))) { result.skippedCache++; continue; }
    if (apply) {
      assertDonorUnchanged();
      if (update.run(rewriteTorrentAnnounces(row.torrent_bytes, announces), row.key, provider, row.torrent_bytes, row.updated_at).changes !== 1)
        throw new TrackerRotationError("Cached metainfo changed during rotation; preview again");
    }
    result.cached++;
  }
  return result;
}
