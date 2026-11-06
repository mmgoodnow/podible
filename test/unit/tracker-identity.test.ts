import { test, expect } from "bun:test";
import bencode from "bencode";
import { Database } from "bun:sqlite";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { metainfoTrackerHosts } from "../../src/media/tracker-identity";

test("automatic tracker identity reads announces, strips credentials and ignores web seeds", () => {
  const bytes = bencode.encode({ announce: "https://user:SECRET@tracker.example/private/PASSKEY",
    "announce-list": [["udp://TRACKER.EXAMPLE:80/announce", "https://backup.example/token"]],
    "url-list": ["https://unrelated.example/private"], comment: "Another tracker" });
  expect(metainfoTrackerHosts(bytes)).toEqual(["backup.example", "tracker.example"]);
  expect(metainfoTrackerHosts(new Uint8Array([1, 2, 3]))).toEqual([]);
  expect(metainfoTrackerHosts(bencode.encode({ announce: "file:///private/path" }))).toEqual([]);
});

test("metainfo cache learns tracker names automatically and conflicting evidence stays unknown", () => {
  const db = new Database(":memory:"); runMigrations(db); const repo = new BooksRepo(db);
  try {
    const bytes = bencode.encode({ announce: "https://a.example/PRIVATE", "announce-list": [["https://b.example/PASSKEY"]] });
    repo.putTorrentCache({ key: "test", provider: "Friendly tracker", url: "https://indexer.example/private", torrentBytes: bytes });
    expect(repo.transferMetrics.mappings()).toEqual([
      { host: "a.example", name: "Friendly tracker", source: "inferred" },
      { host: "b.example", name: "Friendly tracker", source: "inferred" },
    ]);
    db.query("DELETE FROM transfer_tracker_mappings").run();
    repo.transferMetrics.learnCachedTrackers(new Date().toISOString());
    expect(repo.transferMetrics.mappings()[0]?.name).toBe("Friendly tracker");
    repo.transferMetrics.learnMetainfo(bytes, "Conflicting tracker");
    repo.transferMetrics.learnMetainfo(bytes, "Friendly tracker");
    expect(repo.transferMetrics.mappings().every(row => row.source === "unknown")).toBe(true);
    expect(JSON.stringify(db.query("SELECT * FROM transfer_tracker_mappings").all())).not.toMatch(/PRIVATE|PASSKEY|https/);
  } finally { db.close(); }
});
