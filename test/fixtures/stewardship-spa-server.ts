import { Database } from "bun:sqlite";
import { promises as fs } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { hashSessionToken } from "../../src/auth";
import { createPodibleFetchHandler } from "../../src/http";
import { prepareTransmission } from "../../src/media/managed-transmission";
import { TorrentStewardship, type StewardshipConnection } from "../../src/media/torrent-stewardship";
import { fixtureMovie } from "./movie-services";
import { fixtureTvEntry } from "./tv-catalog";

if (!process.env.CONFIG_DIR) throw new Error("Fixture needs an isolated CONFIG_DIR");
const root = await fs.realpath(process.env.CONFIG_DIR), identity = prepareTransmission(path.join(root, "transmission"));
const clientId = `transmission:${identity.id}`, db = new Database(":memory:"); runMigrations(db);
const repo = new BooksRepo(db);
const settings = repo.ensureSettings(); settings.libraryRoot = path.join(root, "library");
settings.movies.managedRoot = path.join(root, "movies"); settings.tv.managedRoot = path.join(root, "tv"); repo.updateSettings(settings);
const admin = repo.upsertUser({ provider: "local", providerUserId: "steward-admin", username: "Librarian", isAdmin: true });
const member = repo.upsertUser({ provider: "local", providerUserId: "steward-member", username: "Reader", isAdmin: false });
repo.createSession(admin.id, hashSessionToken("steward-admin"), null, "browser");
repo.createSession(member.id, hashSessionToken("steward-member"), null, "browser");
const now = Date.now(), rows: Record<string, unknown>[] = [], privateSecret = "fixture-private-passkey-never-show";
function daemonRow(hash: string, directory: string, media: string, override = {}) {
  return { hashString: hash, downloadDir: directory, labels: [`Podible ${media}`], status: 6, leftUntilDone: 0, secondsSeeding: 86400, error: 0,
    trackerStats: [{ id: 1, announce: `https://tracker.invalid/${privateSecret}/announce`, isBackup: false, hasAnnounced: true,
      lastAnnounceSucceeded: true, lastAnnounceTimedOut: false, lastAnnounceTime: Math.floor(now / 1000), lastAnnounceResult: "Success" }], ...override };
}
const bookRoot = path.join(settings.libraryRoot, ".podible", "downloads"), token = randomUUID();
await fs.mkdir(bookRoot, { recursive: true }); await fs.writeFile(path.join(bookRoot, ".podible-content.json"), JSON.stringify({ id: token }));
for (const [index, title] of ["Healthy library book", "The Fifth Season", "The Unseen Archive", "Old saved report"].entries()) {
  const book = repo.createBook({ title, author: "Fixture Author" }), hash = (index + 1).toString(16).padStart(40, "0");
  const directory = path.join(bookRoot, hash); await fs.mkdir(directory);
  await fs.writeFile(path.join(directory, ".podible-book-torrent.json"), JSON.stringify({ rootToken: token, clientId, infoHash: hash }));
  repo.bookTorrents.reserve({ infoHash: hash, bookId: book.id, clientId, rootPath: bookRoot, rootToken: token });
  rows.push(daemonRow(hash, directory, "Books", index === 1 ? { status: 0, leftUntilDone: 123 } : index === 2 ? { trackerStats: [], secondsSeeding: null } : {}));
}
const movie = repo.movies.request(member.id, { ...fixtureMovie, title: "Coast at Dusk" }, settings.movies.qualityPolicy);
const movieAcquisition = repo.movies.startAcquisition(movie.targetId, "initial", () => repo.createJob({ type: "metadata_hydration", status: "succeeded" }).id).acquisition;
repo.movies.bindDownload(movieAcquisition.id, { clientId, nativeId: "a".repeat(40) }, { rootId: "movies", relativePath: "movie" }, "fixture");
const movieDirectory = path.join(settings.movies.managedRoot, "downloads", "movie"); await fs.mkdir(movieDirectory, { recursive: true });
rows.push(daemonRow("a".repeat(40), movieDirectory, "Movies", { error: 3 }));
const tv = repo.tv.requestSeasons(member.id, { ...fixtureTvEntry(), title: "Northern Lights" }, settings.movies.qualityPolicy, [1]);
const tvAcquisition = repo.tv.startAcquisition(tv.targetId, "initial", () => repo.createJob({ type: "metadata_hydration", status: "succeeded" }).id, tv.requestId).acquisition;
repo.tv.bindDownload(tvAcquisition.id, { clientId, nativeId: "b".repeat(40) }, { rootId: "tv", relativePath: "tv" }, "fixture");
const tvDirectory = path.join(settings.tv.managedRoot, "downloads", "tv"); await fs.mkdir(tvDirectory, { recursive: true });
rows.push(daemonRow("b".repeat(40), tvDirectory, "TV"));
const connection: StewardshipConnection = { clientId, async get(ids) { return { torrents: rows.filter(row => ids.includes(String(row.hashString))) }; } };
const service = new TorrentStewardship(repo, { connection, now: () => now });
await service.collect({ operationId: randomUUID() }); await service.collect({ operationId: randomUUID() });
const stale = service.status().items.find(item => item.label === "Old saved report")!;
const saved = repo.getJsonState<{ latest: Record<string, unknown>; history: unknown[] }>(`torrent-stewardship:v1:observation:${stale.key}`)!;
saved.latest.observedAt = new Date(now - 2 * 3600_000).toISOString(); repo.setJsonState(`torrent-stewardship:v1:observation:${stale.key}`, saved);
const calls: string[] = [], originalFetch = globalThis.fetch;
globalThis.fetch = Object.assign(async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
  const url = new URL(input instanceof Request ? input.url : String(input));
  if (url.hostname !== "127.0.0.1" || url.port !== "9091") throw new Error("Fixture forbids provider network");
  const request = JSON.parse(String(init?.body)); calls.push(request.method);
  if (request.method !== "torrent-get") throw new Error("Fixture forbids torrent controls");
  return Response.json({ result: "success", arguments: { torrents: rows.filter(row => request.arguments.ids.includes(row.hashString)) }, tag: request.tag });
}, { preconnect: originalFetch.preconnect });
const handler = createPodibleFetchHandler(repo, Date.now());
const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: request => {
  if (new URL(request.url).pathname === "/__fixture/state") return Response.json({ calls, receipts: db.query("SELECT key FROM app_state WHERE key LIKE 'torrent-stewardship:v1:collection:%'").all().length });
  return handler(request);
} });
console.log(`STEWARDSHIP_FIXTURE=${JSON.stringify({ url: server.url.origin })}`);
process.on("SIGTERM", () => { server.stop(true); db.close(); process.exit(0); });
