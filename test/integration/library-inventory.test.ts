import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { BooksRepo } from "../../src/repo";
import { runMigrations } from "../../src/db";
import { libraryInventorySchema, readLibraryInventory } from "../../src/media/library-inventory";
import { defaultMovieQualityPolicy } from "../../src/movies/quality";
import { fixtureFacts, fixtureMovie } from "../fixtures/movie-services";
import type { TvCatalogEntry } from "../../src/tv/types";
import { createPodibleFetchHandler } from "../../src/http";
import { hashSessionToken } from "../../src/auth";

async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "library-inventory-")));
  const db = new Database(":memory:"); runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings();
  settings.libraryRoot = path.join(root, "books");
  settings.movies.managedRoot = path.join(root, "movies"); settings.tv.managedRoot = path.join(root, "tv");
  repo.updateSettings(settings);
  const roots = { books: path.join(settings.libraryRoot, ".podible/content"), movies: path.join(settings.movies.managedRoot, "objects"), tv: path.join(settings.tv.managedRoot, "objects") };
  await Promise.all(Object.values(roots).map(root => fs.mkdir(root, { recursive: true })));
  const user = repo.upsertUser({ provider: "local", providerUserId: "inventory", username: "Inventory", isAdmin: true });
  const file = async (domain: keyof typeof roots, bytes: number, from?: string) => {
    const name = `${randomUUID()}.media`, destination = path.join(roots[domain], name);
    if (from) await fs.link(from, destination); else await fs.writeFile(destination, Buffer.alloc(bytes, 42));
    return { path: destination, size: bytes, relativePath: name };
  };
  const book = (kind: "audio" | "ebook", files: { path: string; size: number }[], bookId?: number, score = 0) => {
    const id = bookId ?? repo.createBook({ title: randomUUID(), author: "Inventory author" }).id;
    const manifestation = repo.addManifestation({ bookId: id, kind, preferredScore: score });
    const asset = repo.addAsset({ bookId: id, manifestationId: manifestation.id, kind: files.length > 1 ? "multi" : "single",
      mime: kind === "audio" ? "audio/mp4" : "application/epub+zip", totalSize: files.reduce((n, f) => n + f.size, 0),
      files: files.map(f => ({ ...f, start: 0, end: 1, durationMs: 1000 })) });
    return { id, manifestation, asset };
  };
  const movie = (files: Awaited<ReturnType<typeof file>>[], active = true) => {
    const request = repo.movies.request(user.id, { ...fixtureMovie, tmdbId: Math.floor(Math.random() * 1_000_000) + 1 }, defaultMovieQualityPolicy);
    const acquisition = repo.movies.startAcquisition(request.targetId, "initial", id => repo.createJob({ type: "movie_acquire", payload: { acquisitionId: id } }).id).acquisition;
    const version = repo.movies.recordVersion(acquisition.id, fixtureFacts(), files.map(f => ({ id: randomUUID(), relativePath: f.relativePath, sizeBytes: f.size, probe: {} })));
    if (active) repo.movies.selectVersion(request.targetId, version, 0);
    return { request, version };
  };
  return { root, roots, db, repo, settings, user, file, book, movie,
    close: async () => { db.close(); await fs.rm(root, { recursive: true, force: true }); } };
}

test("library inventory counts only available publications, rolls up formats, and deduplicates hardlinks and multi-episode files", async () => {
  const f = await fixture();
  try {
    const ebook = await f.file("books", 1024), audio = await f.file("books", 2048);
    const first = f.book("ebook", [ebook]);
    f.book("audio", [audio], first.id);
    f.book("ebook", [ebook], first.id); // A second edition references the same physical object.
    f.book("ebook", [await f.file("books", 1024, ebook.path)]);
    const requested = f.repo.createBook({ title: "Requested only", author: "Nobody" });
    f.repo.addManifestation({ bookId: requested.id, kind: "audio" });
    f.book("ebook", [await f.file("books", 3000)], undefined, -1); // Staged upgrade is not available.
    f.movie([await f.file("movies", 1024, ebook.path)]);
    f.movie([await f.file("movies", 4096)], false); // Retained but unselected version.
    const tv = await f.file("tv", 8192);
    const catalog: TvCatalogEntry = { key: "tvdb:77", tvdbId: 77, tmdbId: 77, title: "Inventory show", aliases: [], year: 2020,
      anime: false, source: "fixture", relations: [], episodes: [1, 2, 3].map(n => ({ key: `tvdb:${770 + n}`, title: `Episode ${n}`,
        airDate: "2020-01-01", runtimeMinutes: 20, numbering: [{ scheme: "tvdb:aired", season: n === 3 ? 2 : 1, episode: n, absolute: n, source: "fixture" }] })) };
    const request = f.repo.tv.request(f.user.id, catalog, defaultMovieQualityPolicy, catalog.episodes.map(ep => ep.key));
    const acquisition = f.repo.tv.startAcquisition(request.targetId, "initial", id => f.repo.createJob({ type: "tv_acquire", payload: { acquisitionId: id } }).id, request.requestId).acquisition;
    const version = f.repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [{ id: randomUUID(), relativePath: tv.relativePath, sizeBytes: tv.size, probe: {}, episodeIds: request.episodeIds.slice(0, 2) }]);
    f.repo.tv.stagePublication(request.targetId, version);
    f.repo.tv.promotePublication(request.targetId, version, 0); f.repo.tv.clearPublication(request.targetId, version);
    await fs.symlink(tv.path, path.join(f.root, "plex-view.mkv")); // Derived views add no payload bytes.
    await fs.writeFile(path.join(f.roots.tv, "unregistered-download.mkv"), Buffer.alloc(9999));
    const inventory = libraryInventorySchema.parse(await readLibraryInventory(f.repo));
    expect(inventory.complete).toBe(true);
    expect(Object.fromEntries(Object.entries(inventory.categories).map(([key, value]) => [key, value.count])))
      .toEqual({ books: 2, ebooks: 2, audiobooks: 1, movies: 1, tvShows: 1, seasons: 1, episodes: 2 });
    expect(inventory.categories.books.logicalBytes).toBe(3072);
    expect(inventory.categories.episodes.logicalBytes).toBe(8192);
    expect(inventory.totals.logicalBytes).toBe(11264);
    expect(inventory.totals.physicalObjects).toBe(3);
    const allocated = await Promise.all([ebook, audio, tv].map(async file => Number((await fs.stat(file.path, { bigint: true })).blocks * 512n)));
    expect(inventory.totals.onDiskBytes).toBe(allocated.reduce((a, b) => a + b, 0));
  } finally { await f.close(); }
});

test("missing and partial registered files do not appear available or become a false zero byte total", async () => {
  const f = await fixture();
  try {
    const missing = await f.file("books", 100), partial = await f.file("movies", 200);
    f.book("audio", [missing]); f.movie([partial]);
    await fs.unlink(missing.path); await fs.truncate(partial.path, 37);
    const inventory = await readLibraryInventory(f.repo);
    expect(inventory.complete).toBe(false);
    expect(inventory.categories.audiobooks).toMatchObject({ count: 0, unavailableCount: 1, missingFiles: 1, logicalBytes: null, onDiskBytes: null });
    expect(inventory.categories.movies).toMatchObject({ count: 0, unavailableCount: 1, partialFiles: 1, knownLogicalBytes: 37, logicalBytes: null });
    expect(inventory.totals).toMatchObject({ missingFiles: 1, partialFiles: 1, physicalObjects: 1, knownLogicalBytes: 37, onDiskBytes: null });
  } finally { await f.close(); }
});

test("sparse allocation uses stat blocks; canonical symlink escapes are unknown rather than followed", async () => {
  const f = await fixture();
  try {
    const sparse = await f.file("books", 1);
    await fs.truncate(sparse.path, 16 * 1024 ** 2); sparse.size = 16 * 1024 ** 2;
    f.book("ebook", [sparse]);
    const actual = await fs.stat(sparse.path, { bigint: true });
    const measured = await readLibraryInventory(f.repo);
    expect(measured.totals.logicalBytes).toBe(sparse.size);
    expect(measured.totals.onDiskBytes).toBe(Number(actual.blocks * 512n));
    const foreign = path.join(f.root, "private-foreign-file"); await fs.writeFile(foreign, "private");
    const link = path.join(f.roots.books, "symlink.epub"); await fs.symlink(foreign, link);
    f.book("ebook", [{ path: link, size: 7 }]);
    const outside = path.join(f.root, "outside.epub"); await fs.writeFile(outside, "outside");
    f.book("ebook", [{ path: outside, size: 7 }]);
    const inventory = await readLibraryInventory(f.repo);
    expect(inventory.categories.ebooks).toMatchObject({ count: 1, unknownCount: 2, unreadableFiles: 2, onDiskBytes: null });
    expect(inventory.totals.knownLogicalBytes).toBe(sparse.size);
    expect(JSON.stringify(inventory)).not.toContain(f.root);
  } finally { await f.close(); }
});

test("multipart books require every part, but a complete alternative keeps the title available", async () => {
  const f = await fixture();
  try {
    const one = await f.file("books", 100), two = await f.file("books", 200);
    const book = f.book("audio", [one, two]);
    const ebook = await f.file("books", 50); f.book("ebook", [ebook], book.id);
    await fs.truncate(two.path, 10);
    const inventory = await readLibraryInventory(f.repo);
    expect(inventory.categories.books).toMatchObject({ count: 1, unavailableCount: 0, partialFiles: 1, knownLogicalBytes: 160 });
    expect(inventory.categories.audiobooks).toMatchObject({ count: 0, unavailableCount: 1 });
    expect(inventory.categories.ebooks).toMatchObject({ count: 1, logicalBytes: 50 });
    expect(inventory.complete).toBe(false);
  } finally { await f.close(); }
});

test("configured root aliases are allowed without following internal directory symlinks", async () => {
  const f = await fixture();
  try {
    const file = await f.file("books", 123); f.book("ebook", [file]);
    const alias = path.join(f.root, "books-alias"); await fs.symlink(f.settings.libraryRoot, alias);
    f.settings.libraryRoot = alias; f.repo.updateSettings(f.settings);
    expect((await readLibraryInventory(f.repo)).categories.ebooks.count).toBe(1);
    const nested = path.join(f.roots.books, "foreign"); await fs.symlink(f.roots.movies, nested);
    const foreign = await f.file("movies", 321);
    f.book("ebook", [{ path: path.join(nested, foreign.relativePath), size: foreign.size }]);
    const inventory = await readLibraryInventory(f.repo);
    expect(inventory.categories.ebooks).toMatchObject({ count: 1, unknownCount: 1, unreadableFiles: 1, knownLogicalBytes: 123 });
  } finally { await f.close(); }
});

test("inventory endpoint is admin-only on JSON RPC and tRPC, rejects extra parameters, and queues no work", async () => {
  const f = await fixture();
  try {
    const viewer = f.repo.upsertUser({ provider: "local", providerUserId: "inventory-reader", username: "Reader", isAdmin: false });
    f.repo.createSession(f.user.id, hashSessionToken("inventory-admin"), null, "app");
    f.repo.createSession(viewer.id, hashSessionToken("inventory-reader"), null, "app");
    const fetch = createPodibleFetchHandler(f.repo, Date.now());
    for (const [token, expected] of [[null, -32001], ["inventory-reader", -32003], ["inventory-admin", null]] as const) {
      const headers: Record<string, string> = token ? { Authorization: `Bearer ${token}` } : {};
      const response = await fetch(new Request("http://fixture.invalid/rpc", { method: "POST", headers: { ...headers, "Content-Type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "transferMetrics.library", params: {} }) }));
      const value = await response.json() as any;
      if (expected) expect(value.error.code).toBe(expected); else expect(libraryInventorySchema.parse(value.result).categories.books.count).toBe(0);
      const typed = await fetch(new Request("http://fixture.invalid/api/trpc/transferMetrics.library?input=%7B%7D", { headers }));
      expect(typed.status).toBe(expected === -32001 ? 401 : expected === -32003 ? 403 : 200);
    }
    expect(f.db.query("SELECT COUNT(*) AS n FROM jobs").get()).toEqual({ n: 0 });
    const invalid = await fetch(new Request("http://fixture.invalid/api/trpc/transferMetrics.library?input=%7B%22scan%22%3Atrue%7D", { headers: { Authorization: "Bearer inventory-admin" } }));
    expect(invalid.status).toBe(400);
  } finally { await f.close(); }
});
