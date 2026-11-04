import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { BOOK_TORRENTS_SCHEMA_SQL } from "../../src/media/book-torrents-schema";
import type { BookTorrentBinding } from "../../src/media/book-torrents";
import { TorrentRecordsRepo } from "../../src/media/torrent-records";
import { stewardshipBinding } from "../../src/media/torrent-stewardship";
import { admitTorrentUse } from "../../src/media/torrent-retirement-admission";
import type { TorrentSnapshot } from "../../src/media/torrent-client";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";

const clientId = "transmission:11111111-1111-4111-8111-111111111111";
const hash = "a".repeat(40);
const rootToken = "22222222-2222-4222-8222-222222222222";

function fixture(legacy = false) {
  const db = new Database(":memory:");
  db.exec("PRAGMA foreign_keys=ON");
  runMigrations(db);
  if (legacy) {
    // Reconstruct the exact pre-consolidation tables without duplicating older migrations.
    db.exec(`DROP TABLE book_torrent_bindings;
      ALTER TABLE media_torrents DROP COLUMN backend;
      ALTER TABLE media_torrents DROP COLUMN root_path;
      ALTER TABLE media_torrents DROP COLUMN root_token;
      ALTER TABLE media_torrents DROP COLUMN ownership_tag;
      ALTER TABLE indexer_feeds DROP COLUMN gap_diagnostics_json;
      DELETE FROM schema_migrations WHERE id IN (53,54);`);
    db.exec(BOOK_TORRENTS_SCHEMA_SQL);
  }
  const repo = new BooksRepo(db);
  repo.ensureSettings();
  const book = repo.createBook({ title: "Consolidation", author: "Fixture" });
  const release = repo.createRelease({ bookId: book.id, provider: "fixture", title: "Consolidation", mediaType: "audio",
    infoHash: hash, url: "https://fixture.invalid", status: "downloading" });
  const reserve = { infoHash: hash, bookId: book.id, clientId, rootPath: "/library/.podible/downloads", rootToken };
  const binding: BookTorrentBinding = { info_hash: hash, book_id: book.id, release_id: release.id, backend: "transmission",
    client_id: clientId, root_path: reserve.rootPath, root_token: rootToken, root_id: "books", relative_path: hash,
    created_at: "2026-01-01T00:00:00.000Z" };
  const seedBook = (row = binding) => db.query(`INSERT INTO book_torrents
    (info_hash,book_id,release_id,backend,client_id,root_path,root_token,root_id,relative_path,created_at)
    VALUES(?,?,?,?,?,?,?,?,?,?)`).run(row.info_hash,row.book_id,row.release_id,row.backend,row.client_id,
      row.root_path,row.root_token,row.root_id,row.relative_path,row.created_at);
  const records = new TorrentRecordsRepo(db);
  return { db, repo, book, release, reserve, binding, seedBook, records };
}

function screenAcquisitions(repo: BooksRepo) {
  const user = repo.upsertUser({ provider: "local", providerUserId: "torrent-fixture", username: "fixture", isAdmin: false });
  const movie = repo.movies.request(user.id, { tmdbId: 1, title: "Movie", originalTitle: "Movie", year: 2026,
    overview: "", originalLanguage: "en", posterUrl: null, imdbId: null }, defaultMovieQualityPolicy);
  const tv = repo.tv.request(user.id, { key: "tvdb:1", tvdbId: 1, tmdbId: null, title: "TV", aliases: [], year: 2026,
    anime: false, source: "fixture", relations: [], episodes: [{ key: "tvdb:10", title: "Episode", airDate: null,
      runtimeMinutes: null, numbering: [{ scheme: "tvdb:aired", season: 1, episode: 1, absolute: null, source: "fixture" }] }] }, {}, ["tvdb:10"]);
  const enqueue = () => repo.createJob({ type: "reconcile" }).id;
  return { movie: repo.movies.startAcquisition(movie.targetId,"initial",enqueue).acquisition,
    tv: repo.tv.startAcquisition(tv.targetId,"initial",enqueue,tv.requestId).acquisition };
}

test("migration 53 preserves book bindings, media IDs, associations, health, evidence and tombstones", () => {
  const f = fixture(true);
  try {
    f.seedBook();
    f.seedBook({ ...f.binding, info_hash: "b".repeat(40), relative_path: "b".repeat(40), book_id: 999, release_id: null });
    const acq = screenAcquisitions(f.repo);
    const location = '{"relativePath":"existing","rootId":"movies"}';
    f.db.query(`INSERT INTO media_torrents(id,client_id,native_id,location_json,offer_key,observation_json)
      VALUES(91,?,?,?,?,?)`).run(clientId,"c".repeat(40),location,"original-offer",'{"health":"unchanged"}');
    f.db.query("INSERT INTO movie_acquisition_downloads VALUES(?,91)").run(acq.movie.id);
    f.db.query(`INSERT INTO media_torrents(id,client_id,native_id,location_json,offer_key,observation_json)
      VALUES(92,?,?,?,?,?)`).run(clientId,"d".repeat(40),'{"rootId":"tv","relativePath":"existing-tv"}',"tv-offer",null);
    f.db.query("INSERT INTO tv_acquisition_downloads VALUES(?,92)").run(acq.tv.id);
    const original = f.db.query("SELECT * FROM media_torrents ORDER BY id").all();
    const digest = stewardshipBinding(f.repo,"books",{ ...f.binding },clientId);
    expect(digest).not.toBeNull();
    f.repo.setJsonState("torrent-stewardship:history:" + digest!.key, { bindingDigest: digest!.bindingDigest, history: ["evidence"] });
    f.repo.setJsonState("torrent-retirement:preserved", { retired: true, claim: { id: randomUUID() } });
    const state = f.db.query("SELECT * FROM app_state ORDER BY key").all();

    runMigrations(f.db);
    expect(f.repo.bookTorrents.get(hash)).toEqual(f.binding);
    expect(stewardshipBinding(f.repo,"books",{ ...f.repo.bookTorrents.get(hash)! },clientId)).toEqual(digest);
    expect(f.db.query("SELECT * FROM app_state ORDER BY key").all()).toEqual(state);
    expect(f.db.query("SELECT id,client_id,native_id,location_json,offer_key,observation_json FROM media_torrents WHERE id<=92 ORDER BY id").all()).toEqual(original);
    expect(f.repo.movies.downloads(acq.movie.target_id)[0]?.id).toBe(91);
    expect(f.repo.tv.downloads(acq.tv.target_id)[0]?.id).toBe(92);
    expect(f.db.query("SELECT id FROM media_torrents WHERE native_id=?").get(hash)).toEqual({ id: 93 });
    expect(f.repo.bookTorrents.get("b".repeat(40))).toMatchObject({ book_id: 999, release_id: null });
    expect(f.db.query("SELECT name FROM sqlite_master WHERE name='book_torrents'").get()).toBeNull();
    expect(f.db.query("SELECT COUNT(*) AS n FROM media_torrents").get()).toEqual({ n: 4 });
    expect(f.db.query("PRAGMA foreign_key_check").all()).toEqual([]);
    runMigrations(f.db);
    expect(f.db.query("SELECT COUNT(*) AS n FROM media_torrents").get()).toEqual({ n: 4 });
    expect(f.db.query("SELECT COUNT(*) AS n FROM schema_migrations WHERE id=53").get()).toEqual({ n: 1 });
  } finally { f.db.close(); }
});

test("migration reuses an agreeing common record without replacing its ID, offer, layout bytes or observation", () => {
  const f = fixture(true);
  try {
    f.seedBook();
    const json = JSON.stringify({ relativePath: hash, rootId: "books" });
    f.db.query(`INSERT INTO media_torrents(id,client_id,native_id,location_json,offer_key,observation_json)
      VALUES(77,?,?,?,?,?)`).run(clientId,hash,json,"original",'{"observedAt":"before"}');
    runMigrations(f.db);
    expect(f.records.get(clientId,hash)).toMatchObject({ id: 77, location_json: json, offer_key: "original",
      observation_json: '{"observedAt":"before"}', backend: "transmission", root_path: f.binding.root_path,
      root_token: rootToken, ownership_tag: "Podible Books" });
    expect(f.db.query("SELECT * FROM book_torrent_bindings").all()).toEqual([{ torrent_id: 77, book_id: f.book.id,
      release_id: f.release.id, created_at: f.binding.created_at }]);
    f.repo.deleteBook(f.book.id);
    expect(f.repo.bookTorrents.get(hash)).toEqual({ ...f.binding, release_id: null });
    expect(f.records.get(clientId,hash)?.id).toBe(77);
  } finally { f.db.close(); }
});

test("conflicting legacy merge rolls back every schema/data change and migration ledger entry", () => {
  const f = fixture(true);
  try {
    f.seedBook();
    f.db.query("INSERT INTO media_torrents(client_id,native_id,location_json,offer_key) VALUES(?,?,?,?)")
      .run(clientId,hash,JSON.stringify({ rootId: "movies", relativePath: hash }),"keep");
    const before = f.db.query("SELECT * FROM media_torrents").all();
    f.repo.setJsonState("torrent-retirement:tombstone", { retired: true });
    expect(() => runMigrations(f.db)).toThrow("another layout");
    expect(f.db.query("SELECT * FROM media_torrents").all()).toEqual(before);
    expect(f.db.query("SELECT * FROM book_torrents").all()).toEqual([f.binding]);
    expect(f.db.query("SELECT name FROM sqlite_master WHERE name='book_torrent_bindings'").get()).toBeNull();
    expect(f.db.query("SELECT id FROM schema_migrations WHERE id IN (53,54)").all()).toEqual([]);
    expect(f.repo.getJsonState<{ retired: boolean }>("torrent-retirement:tombstone")).toEqual({ retired: true });
  } finally { f.db.close(); }
});

test("invalid legacy ownership fails closed and leaves the old binding available for explicit repair", () => {
  const f = fixture(true);
  try {
    f.seedBook();
    const badHash = "b".repeat(40);
    f.seedBook({ ...f.binding, info_hash: badHash, relative_path: badHash, release_id: null, root_path: "relative-root" });
    expect(() => runMigrations(f.db)).toThrow("invalid legacy");
    expect(f.db.query("SELECT root_path FROM book_torrents WHERE info_hash=?").get(badHash)).toEqual({ root_path: "relative-root" });
    expect(f.db.query("SELECT COUNT(*) AS n FROM book_torrents").get()).toEqual({ n: 2 });
    expect(f.db.query("SELECT COUNT(*) AS n FROM media_torrents").get()).toEqual({ n: 0 });
  } finally { f.db.close(); }
});

test("book facade writes only the common identity and association, preserving pinned roots on replay", () => {
  const f = fixture();
  try {
    const binding = f.repo.bookTorrents.reserve(f.reserve);
    f.repo.bookTorrents.attach(hash,f.release.id);
    const record = f.records.get(clientId,hash)!;
    expect(record).toMatchObject({ backend: "transmission", root_path: f.reserve.rootPath, root_token: rootToken,
      ownership_tag: "Podible Books", location_json: JSON.stringify({ rootId: "books", relativePath: hash }) });
    expect(f.repo.bookTorrents.reserve(f.reserve)).toEqual({ ...binding, release_id: f.release.id });
    expect(() => f.repo.bookTorrents.reserve({ ...f.reserve, rootPath: "/replacement" })).toThrow("root_path");
    expect(() => f.repo.bookTorrents.reserve({ ...f.reserve, rootToken: randomUUID() })).toThrow("root_token");
    expect(f.db.query("SELECT COUNT(*) AS n FROM media_torrents").get()).toEqual({ n: 1 });
    expect(f.db.query("SELECT COUNT(*) AS n FROM book_torrent_bindings").get()).toEqual({ n: 1 });
    f.repo.deleteBook(f.book.id);
    expect(f.repo.bookTorrents.get(hash)).toEqual({ ...binding, release_id: null });
    const next = f.repo.createBook({ title: "Reclaimed", author: "Fixture" });
    expect(f.repo.bookTorrents.reclaimOrphan(f.repo.bookTorrents.get(hash)!,next.id).book_id).toBe(next.id);
    expect(f.records.get(clientId,hash)).toEqual(record);
  } finally { f.db.close(); }
});

test("all domain writers share ownership checks and keep conflicting acquisition associations absent", () => {
  const f = fixture();
  try {
    const acq = screenAcquisitions(f.repo);
    f.repo.bookTorrents.reserve(f.reserve);
    expect(() => f.repo.movies.bindDownload(acq.movie.id,{ clientId, nativeId: hash },{ rootId: "movies", relativePath: hash },"offer")).toThrow("another layout");
    expect(() => f.repo.tv.bindDownload(acq.tv.id,{ clientId, nativeId: hash },{ rootId: "books", relativePath: hash },"offer")).toThrow("ownership_tag");
    expect(f.repo.movies.downloads(acq.movie.target_id)).toEqual([]);
    expect(f.repo.tv.downloads(acq.tv.target_id)).toEqual([]);
    const ref = { clientId, nativeId: "b".repeat(40) }, location = { rootId: "movies", relativePath: "pack" };
    const id = f.repo.movies.bindDownload(acq.movie.id,ref,location,"first");
    expect(f.repo.movies.bindDownload(acq.movie.id,ref,location,"retry")).toBe(id);
    expect(f.records.get(clientId,ref.nativeId)?.offer_key).toBe("first");
    expect(() => f.repo.tv.bindDownload(acq.tv.id,ref,location,"offer")).toThrow("ownership_tag");
    expect(() => f.repo.movies.bindDownload(999,ref,location,"offer")).toThrow("Unknown movie acquisition");
    expect(() => f.repo.tv.bindDownload(999,ref,location,"offer")).toThrow("Unknown TV acquisition");
    expect(f.db.query("SELECT COUNT(*) AS n FROM media_torrents").get()).toEqual({ n: 2 });
  } finally { f.db.close(); }
});

test("common health cannot be overwritten by a foreign identity or location", () => {
  const f = fixture();
  try {
    const ref = { clientId, nativeId: hash }, location = { rootId: "books", relativePath: hash };
    f.repo.bookTorrents.reserve(f.reserve);
    const id = f.records.get(clientId,hash)!.id;
    const snapshot: TorrentSnapshot = { ref, location, state: "seeding", complete: true, bytesDone: 4, sizeBytes: 4,
      downloadBytesPerSecond: 0, message: null, observedAt: "2026-01-01T00:00:00.000Z" };
    f.records.observe(id,snapshot);
    expect(JSON.parse(f.repo.movies.torrent(clientId,hash)!.observation_json!)).toEqual(snapshot);
    expect(f.repo.tv.torrent(clientId,hash)?.observation_json).toBe(JSON.stringify(snapshot));
    expect(() => f.records.observe(id,{ ...snapshot, ref: { ...ref, nativeId: "b".repeat(40) } })).toThrow("another identity");
    expect(() => f.records.observe(id,{ ...snapshot, location: { ...location, relativePath: "replacement" } })).toThrow("another layout");
    expect(f.records.get(clientId,hash)?.observation_json).toBe(JSON.stringify(snapshot));
  } finally { f.db.close(); }
});

test("retired tombstones and execution fencing protect every shared/domain binding writer", () => {
  const f = fixture();
  try {
    const acq = screenAcquisitions(f.repo), ref = { clientId, infoHash: hash };
    admitTorrentUse(f.repo,ref,randomUUID());
    const key = (f.db.query("SELECT key FROM app_state WHERE key LIKE 'torrent-retirement:admission:v1:%'").get() as { key: string }).key;
    const before = { ...f.repo.getJsonState<object>(key), users: [], retired: true };
    f.repo.setJsonState(key,before);
    expect(() => f.repo.bookTorrents.reserve(f.reserve)).toThrow(/retir/i);
    expect(() => f.repo.movies.bindDownload(acq.movie.id,{ clientId, nativeId: hash },{ rootId: "movies", relativePath: hash },"offer")).toThrow(/retir/i);
    expect(() => f.repo.tv.bindDownload(acq.tv.id,{ clientId, nativeId: hash },{ rootId: "tv", relativePath: hash },"offer")).toThrow(/retir/i);
    expect(f.repo.getJsonState<object>(key)).toEqual(before);
    const fenced = f.repo.forExecution(() => { throw new Error("stale execution"); });
    expect(() => fenced.bookTorrents.reserve({ ...f.reserve, infoHash: "b".repeat(40) })).toThrow("stale execution");
    expect(() => fenced.movies.bindDownload(acq.movie.id,{ clientId, nativeId: "b".repeat(40) },{ rootId: "movies", relativePath: hash },"offer")).toThrow("stale execution");
    expect(f.db.query("SELECT COUNT(*) AS n FROM media_torrents").get()).toEqual({ n: 0 });
    expect(f.db.query("SELECT COUNT(*) AS n FROM book_torrent_bindings").get()).toEqual({ n: 0 });
  } finally { f.db.close(); }
});

test("common identity is client-scoped; ambiguous hash-only book associations fail closed", () => {
  const f = fixture();
  try {
    f.repo.bookTorrents.reserve(f.reserve);
    const other = f.records.reserve({ clientId: "transmission:33333333-3333-4333-8333-333333333333", nativeId: hash },
      { rootId: "books", relativePath: hash },"",{ backend: "transmission", root_path: f.reserve.rootPath,
        root_token: rootToken, ownership_tag: "Podible Books" });
    f.db.query("INSERT INTO book_torrent_bindings(torrent_id,book_id,created_at) VALUES(?,?,?)")
      .run(other.id,f.book.id,f.binding.created_at);
    expect(() => f.repo.bookTorrents.get(hash)).toThrow("ambiguous client");
    expect(f.db.query("SELECT COUNT(*) AS n FROM media_torrents").get()).toEqual({ n: 2 });
  } finally { f.db.close(); }
});

test("migration 54 adds nullable feed gap diagnostics without rewriting existing feed progress", () => {
  const f = fixture(true);
  try {
    f.db.query(`INSERT INTO indexer_feeds(id,provider,media_type,head_key,updated_at,boundary_json)
      VALUES('feed','fixture','book','head','before','{"key":"boundary"}')`).run();
    const row = f.db.query("SELECT * FROM indexer_feeds").get() as Record<string, unknown>;
    runMigrations(f.db);
    expect(f.db.query("SELECT * FROM indexer_feeds").get()).toEqual({ ...row, gap_diagnostics_json: null });
    f.db.query("UPDATE indexer_feeds SET gap_diagnostics_json=? WHERE id='feed'").run('{"missing":3}');
    runMigrations(f.db);
    expect(f.db.query("SELECT gap_diagnostics_json FROM indexer_feeds").get()).toEqual({ gap_diagnostics_json: '{"missing":3}' });
    expect(f.db.query("SELECT COUNT(*) AS n FROM schema_migrations WHERE id=54").get()).toEqual({ n: 1 });
  } finally { f.db.close(); }
});
