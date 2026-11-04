import { Database } from "bun:sqlite";
import { expect, spyOn, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { TorrentStewardship, stewardshipBinding, stewardshipBindingPage } from "../../src/media/torrent-stewardship";
import { readStewardshipTitles, type StewardshipTitleInput } from "../../src/media/stewardship-titles";
import { readStewardshipInventory } from "../../src/media/stewardship-inventory";
import { fixtureMovie } from "../fixtures/movie-services";
import { fixtureTvEntry } from "../fixtures/tv-catalog";

const clientId = "transmission:11111111-1111-4111-8111-111111111111";
function fixture() {
  const db = new Database(":memory:"); runMigrations(db); const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.libraryRoot = "/fixture/books"; settings.movies.managedRoot = "/fixture/movies"; settings.tv.managedRoot = "/fixture/tv"; repo.updateSettings(settings);
  const calls: StewardshipTitleInput[] = [];
  Object.assign(repo, { readStewardshipTitles(input: StewardshipTitleInput) { calls.push(input); return readStewardshipTitles(db, input); },
    readStewardshipInventory(input: Parameters<typeof readStewardshipInventory>[1]) { return readStewardshipInventory(db, input); } });
  const service = new TorrentStewardship(repo, { connection: { clientId, async get() { throw new Error("Status must never collect"); } } });
  return { db, repo, settings, service, calls };
}

test("combined raw pagination crosses producers once without duplicate or skipped binding keys past 25/50", () => {
  const f = fixture(); const book = f.repo.createBook({ title: "Actual book title", author: "Author" });
  try {
    for (let n = 1; n <= 27; n++) f.repo.bookTorrents.reserve({ infoHash: n.toString(16).padStart(40, "0"), bookId: book.id,
      clientId, rootPath: "/fixture/books/.podible/downloads", rootToken: "11111111-1111-4111-8111-111111111111" });
    for (let n = 100; n < 131; n++) f.db.query("INSERT INTO media_torrents(client_id,native_id,location_json,offer_key) VALUES(?,?,?,?)")
      .run(clientId, n.toString(16).padStart(40, "0"), JSON.stringify({ rootId: "movies", relativePath: String(n) }), "fixture");
    const before = f.db.query("SELECT total_changes() AS n").get();
    const pages = [0, 25, 50].map(offset => f.service.status({ source: "all", offset, limit: 25 }));
    expect(pages.map(page => [page.scannedCount, page.nextOffset, page.sourceRows])).toEqual([[25, 25, 58], [25, 50, 58], [8, null, 58]]);
    expect(pages[1]!.items.filter(item => item.media === "books")).toHaveLength(2);
    const keys = pages.flatMap(page => page.items.map(item => item.key));
    expect(new Set(keys).size).toBe(58);
    expect(keys).toEqual([...stewardshipBindingPage(f.repo, "books", 0, 50, clientId).bindings,
      ...stewardshipBindingPage(f.repo, "screen", 0, 50, clientId).bindings].map(binding => binding.key));
    expect(pages[0]!.items.every(item => item.label === "Actual book title")).toBe(true);
    const legacy = stewardshipBinding(f.repo, "books", { ...f.repo.bookTorrents.get("1".padStart(40, "0"))! }, clientId)!;
    expect(stewardshipBindingPage(f.repo, "books", 0, 1, clientId).bindings[0]!.bindingDigest).toBe(legacy.bindingDigest);
    expect(f.calls[0]!.books).toEqual([book.id]);
    expect(f.calls.every(input => input.books.length + input.screen.length <= 50)).toBe(true);
    expect(f.db.query("SELECT total_changes() AS n").get()).toEqual(before);
    expect(f.service.status({ offset: 100 }).nextOffset).toBeNull();
  } finally { f.db.close(); }
});

test("title projection resolves book/movie/TV names with two bounded queries, not per-row catalog hydration", () => {
  const f = fixture();
  try {
    const user = f.repo.upsertUser({ provider: "local", providerUserId: "titles", username: "Reader", isAdmin: true });
    const book = f.repo.createBook({ title: "A named book", author: "Author" });
    const movie = f.repo.movies.request(user.id, { ...fixtureMovie, title: "A named movie" }, f.settings.movies.qualityPolicy);
    const a = f.repo.movies.startAcquisition(movie.targetId, "initial", () => f.repo.createJob({ type: "metadata_hydration" }).id).acquisition;
    const movieTorrent = f.repo.movies.bindDownload(a.id, { clientId, nativeId: "a".repeat(40) }, { rootId: "movies", relativePath: "movie" }, "fixture");
    const tv = f.repo.tv.requestSeasons(user.id, { ...fixtureTvEntry(), title: "A named series" }, f.settings.movies.qualityPolicy, [1]);
    const b = f.repo.tv.startAcquisition(tv.targetId, "initial", () => f.repo.createJob({ type: "metadata_hydration" }).id, tv.requestId).acquisition;
    const tvTorrent = f.repo.tv.bindDownload(b.id, { clientId, nativeId: "b".repeat(40) }, { rootId: "tv", relativePath: "tv" }, "fixture");
    const queries = spyOn(f.db, "query");
    try {
      const rows = readStewardshipTitles(f.db, { books: [book.id, book.id], screen: [movieTorrent, tvTorrent] });
      expect(queries).toHaveBeenCalledTimes(2);
      expect(rows.map(row => row.titles[0]!.title)).toEqual(["A named book", "A named movie", "A named series"]);
      expect(rows[0]!.titles[0]!.href).toBe(`/book/${book.id}`);
      expect(rows[1]!.titles[0]!.href).toBe(`/movies/${fixtureMovie.tmdbId}`);
      expect(rows[2]!.titles[0]!.href).toBe("/tv/70");
    } finally { queries.mockRestore(); }
    expect(() => readStewardshipTitles(f.db, { books: Array.from({ length: 51 }, (_, i) => i + 1), screen: [] })).toThrow();
  } finally { f.db.close(); }
});

test("the display reader remains compatible before ownership-table consolidation", () => {
  using db = new Database(":memory:");
  db.exec(`CREATE TABLE book_torrents(info_hash TEXT,client_id TEXT);
    CREATE TABLE media_torrents(id INTEGER,location_json TEXT);
    CREATE TABLE movie_acquisition_downloads(torrent_id INTEGER);
    CREATE TABLE tv_acquisition_downloads(torrent_id INTEGER);
    INSERT INTO book_torrents VALUES('a','private');
    INSERT INTO media_torrents VALUES(1,'{"rootId":"movies","relativePath":"one"}');`);
  const page = readStewardshipInventory(db, { source: "all", offset: 0, limit: 25 });
  expect(page.total).toBe(2); expect(page.rows.map(row => row.source)).toEqual(["books", "screen"]);
});
