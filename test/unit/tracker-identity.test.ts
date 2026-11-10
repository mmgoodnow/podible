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
    const bytes = bencode.encode({ announce: "https://a.example/PRIVATE" });
    repo.putTorrentCache({ key: "test", provider: "Friendly tracker", url: "https://indexer.example/private", torrentBytes: bytes });
    expect(repo.transferMetrics.mappings()).toEqual([
      { host: "a.example", name: "Friendly tracker", source: "inferred" },
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

test("multi-tracker metainfo cannot name independent announces after its supplying indexer", () => {
  const db = new Database(":memory:"); runMigrations(db); const repo = new BooksRepo(db);
  try {
    repo.transferMetrics.learnMetainfo(bencode.encode({ announce: "https://a.example/SECRET", "announce-list": [["https://b.example/PRIVATE"]] }), "Alpha");
    expect(repo.transferMetrics.mappings()).toEqual([
      { host: "a.example", name: "Unknown tracker", source: "unknown" },
      { host: "b.example", name: "Unknown tracker", source: "unknown" },
    ]);
  } finally { db.close(); }
});

test("cached private metainfo assigns the source name to all announce aliases without exposing credentials", () => {
  const db = new Database(":memory:"); runMigrations(db); const repo = new BooksRepo(db);
  try {
    const bytes = bencode.encode({ announce: "https://reactor.filelist.io/private-secret/announce",
      "announce-list": [["https://reactor.filelist.io/private-secret/announce"], ["https://reactor.thefl.org/other-secret/announce"]],
      info: { private: 1 } });
    repo.putTorrentCache({ key: "filelist", provider: "FileList", url: "https://indexer.example/private", torrentBytes: bytes });
    db.query("DELETE FROM transfer_tracker_mappings").run();
    repo.transferMetrics.learnCachedTrackers(new Date().toISOString());
    expect(repo.transferMetrics.mappings()).toEqual([
      { host: "reactor.filelist.io", name: "FileList", source: "inferred" },
      { host: "reactor.thefl.org", name: "FileList", source: "inferred" },
    ]);
    expect(JSON.stringify(repo.transferMetrics.mappings())).not.toContain("secret");
  } finally { db.close(); }
});
