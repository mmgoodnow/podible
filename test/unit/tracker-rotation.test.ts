import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import bencode from "bencode";
import { infoHashFromTorrentBytes, readTorrentAnnounces } from "../../src/library/torrent";
import { rotateTrackerAnnounces } from "../../src/media/tracker-rotation";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { getOrFetchCachedTorrentBytes } from "../../src/library/torrent-cache";

const old = "https://old.invalid/key/announce";
const primary = "https://new.invalid/new-key/announce";
const alternate = "https://backup.invalid/new-key/announce";
const extra = "https://extra.invalid/new-key/announce";
const foreign = "https://old.invalid/another-account/announce";
const newTiers = [[primary, alternate], [extra]];
const newList = `${primary}\n${alternate}\n\n${extra}`;
function fixture() {
  const db = new Database(":memory:");
  runMigrations(db);
  const info = Buffer.from("d4:name7:old.mkv6:lengthi42ee");
  const webseed = "https://webseed.invalid/file";
  const previous = Buffer.concat([Buffer.from("d8:announce"), bencode.encode(old),
    Buffer.from("4:info"), info, Buffer.from("8:url-list"), bencode.encode(webseed), Buffer.from("e")]);
  const hash = infoHashFromTorrentBytes(previous);
  const cached = db.query("INSERT INTO torrent_cache VALUES(?,?,?,?,?,?,?,?)");
  const addCache = (key: string, provider: string, bytes: Uint8Array, day: number) =>
    cached.run(key, provider, key, "https://indexer.invalid/download", infoHashFromTorrentBytes(bytes), bytes,
      "2026-01-01", `2026-01-0${day}`);
  addCache("old", "tracker", previous, 1);
  addCache("new", "tracker", bencode.encode({ announce: primary, "announce-list": newTiers, info: { name: "new" } }), 2);
  addCache("foreign", "foreign", bencode.encode({ announce: foreign, info: { name: "foreign" } }), 3);
  const rows = [
    { id: 1, hashString: hash, downloadDir: "/managed/slot", labels: ["Podible TV"], trackerList: old },
    { id: 2, hashString: "b".repeat(40), downloadDir: "/managed/other", labels: [], trackerList: old },
    { id: 3, hashString: "c".repeat(40), downloadDir: "/managed/mixed", labels: ["Podible TV"], trackerList: `${old}\n\n${foreign}` },
  ];
  const bind = db.query(`INSERT INTO media_torrents (client_id,native_id,location_json,offer_key,backend,ownership_tag)
    VALUES('client',?,?,'','transmission','Podible TV')`);
  bind.run(hash, '{"rootId":"tv","relativePath":"slot"}');
  bind.run(rows[2]!.hashString, '{"rootId":"tv","relativePath":"mixed"}');
  const getCache = () => db.query<{ torrent_bytes: Uint8Array; updated_at: string }, []>(
    "SELECT torrent_bytes,updated_at FROM torrent_cache WHERE key='old'",
  ).get()!;
  return { db, rows, hash, info, webseed, getCache, addCache };
}

test("rotation uses the latest provider torrent, updates tiers in place, and preserves raw info", async () => {
  const { db, rows, hash, info, webseed, getCache } = fixture();
  const writes: unknown[] = [];
  const run = (apply: boolean) => rotateTrackerAnnounces({
    db, clientId: "client", provider: "tracker", apply, roots: { tv: "/managed" },
    rpc: { async call(method, args) {
      if (method === "torrent-get") return { torrents: rows.filter(row => !args?.ids || (args.ids as string[]).includes(row.hashString)) };
      if (method !== "torrent-set") throw new Error("Unexpected mutation");
      writes.push(args);
      rows[0]!.trackerList = String(args?.trackerList);
      return {};
    } },
  });
  try {
    expect(await run(false)).toMatchObject({ donorFetchedAt: "2026-01-02", cached: 1, registered: 1, skippedRegistrations: 2 });
    expect(writes).toEqual([]);
    expect(readTorrentAnnounces(getCache().torrent_bytes).primary).toBe(old);
    expect(await run(true)).toMatchObject({ cached: 1, registered: 1, trackerUrls: 3 });
    expect(writes).toEqual([{ ids: [hash], trackerList: newList }]);
    const { torrent_bytes: bytes, updated_at } = getCache();
    expect(updated_at).toBe("2026-01-01");
    expect(readTorrentAnnounces(bytes)).toEqual({ primary, tiers: newTiers });
    expect(infoHashFromTorrentBytes(bytes)).toBe(hash);
    expect(Buffer.from(bytes).includes(info)).toBe(true);
    expect(Buffer.from(bytes).includes(webseed)).toBe(true);
    expect(await run(true)).toMatchObject({ cached: 0, registered: 0 });
    expect(writes).toHaveLength(1);
  } finally { db.close(); }
});

test("keeps the cache intact when a Transmission write is not confirmed", async () => {
  const { db, rows, getCache } = fixture();
  try {
    await expect(rotateTrackerAnnounces({
      db, clientId: "client", provider: "tracker", apply: true, roots: { tv: "/managed" },
      rpc: { async call(method, args) {
        return method === "torrent-set" ? {} : { torrents: args?.ids ? rows.slice(0, 1) : rows };
      } },
    })).rejects.toThrow("did not confirm");
    expect(readTorrentAnnounces(getCache().torrent_bytes).primary).toBe(old);
  } finally { db.close(); }
});

test("refuses a donor shared by another provider before issuing any RPC", async () => {
  const { db, addCache } = fixture();
  try {
    addCache("shared", "another", bencode.encode({ announce: primary, info: { name: "shared" } }), 3);
    await expect(rotateTrackerAnnounces({
      db, clientId: "client", provider: "tracker", apply: true, roots: { tv: "/managed" },
      rpc: { async call() { throw new Error("RPC should not be called"); } },
    })).rejects.toThrow("attributed to another provider");
  } finally { db.close(); }
});

test("real fetches trigger durable reconciliation only after independent confirmation", async () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const provider = "automatic";
  const oldBytes = bencode.encode({ announce: old, info: { name: "first" } });
  const sample = (name: string) => bencode.encode({ announce: primary, "announce-list": newTiers, info: { name } });
  const hash = infoHashFromTorrentBytes(oldBytes);
  const download = async (key: string, bytes: Uint8Array) => {
    const repo = new BooksRepo(db);
    return (await getOrFetchCachedTorrentBytes(repo, { provider, providerGuid: key, url: `https://indexer.invalid/${key}` },
      { revalidate: true, fetchBytes: async () => bytes })).bytes;
  };
  // An existing cache row from before the feature is also valid baseline evidence.
  new BooksRepo(db).putTorrentCache({ key: "pg:automatic:original", provider, providerGuid: "original",
    url: "https://indexer.invalid/original", infoHash: hash, torrentBytes: oldBytes });
  await download("original", sample("first"));
  await download("mirror", sample("first"));
  expect(new BooksRepo(db).trackerAnnounces.pendingProvider()).toBeNull();
  await download("independent", sample("second"));
  const state = () => new BooksRepo(db).trackerAnnounces;
  expect(state().pendingProvider()).toBe(provider);
  expect(state().currentMetainfo(provider, oldBytes)).toEqual(oldBytes);
  db.query(`INSERT INTO media_torrents(client_id,native_id,location_json,offer_key,backend,ownership_tag)
    VALUES('client',?,'{"rootId":"tv","relativePath":"slot"}','','transmission','Podible TV')`).run(hash);
  const row = { id: 1, hashString: hash, downloadDir: "/managed/slot", labels: ["Podible TV"], trackerList: old };
  let unavailable = true;
  const reconcile = () => state().reconcile(provider, {
    clientId: "client", roots: { tv: "/managed" },
    rpc: { async call(method, args) {
      if (unavailable) throw new Error("daemon unavailable");
      if (method === "torrent-get") return { torrents: [row] };
      if (method !== "torrent-set") throw new Error("Unexpected mutation");
      row.trackerList = String(args?.trackerList);
      return {};
    } },
  });
  await expect(reconcile()).rejects.toThrow("daemon unavailable");
  expect(state().pendingProvider()).toBe(provider);
  unavailable = false;
  expect(await reconcile()).toMatchObject({ registered: 1, remaining: false });
  expect(row.trackerList).toBe(newList);
  expect(state().pendingProvider()).toBeNull();
  const replay = state().currentMetainfo(provider, oldBytes);
  expect(readTorrentAnnounces(replay).tiers).toEqual(newTiers);
  expect(infoHashFromTorrentBytes(replay)).toBe(hash);
  expect(state().currentMetainfo("another", oldBytes)).toEqual(oldBytes);
  expect(readTorrentAnnounces(await download("stale", oldBytes)).tiers).toEqual(newTiers);
  expect(state().pendingProvider()).toBeNull();
  const saved = db.query<{ value_json: string }, []>(
    "SELECT value_json FROM app_state WHERE key='tracker-announces:v1:automatic'",
  ).get()!.value_json;
  expect(saved).not.toContain("://");
  expect(saved).not.toContain("new-key");
});
