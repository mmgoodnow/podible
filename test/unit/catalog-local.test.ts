import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { randomUUID } from "node:crypto";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { hashSessionToken } from "../../src/auth";
import { createPodibleFetchHandler } from "../../src/http";
import { fixtureMovie, fixtureFacts } from "../fixtures/movie-services";
import { fixtureTvEntry } from "../fixtures/tv-catalog";

test("shared local catalog is user-safe, published-only, bounded and independent of providers", async () => {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db), settings = repo.ensureSettings();
  const owner = repo.upsertUser({ provider: "local", providerUserId: "catalog-owner", username: "Private owner", isAdmin: false });
  const reader = repo.upsertUser({ provider: "local", providerUserId: "catalog-reader", username: "Reader", isAdmin: false });
  repo.createSession(reader.id, hashSessionToken("catalog-reader"), null, "browser");
  const addMovie = (tmdbId: number, title: string, publish: boolean) => {
    const requested = repo.movies.request(owner.id, { ...fixtureMovie, tmdbId, title }, settings.movies.qualityPolicy);
    if (publish) {
      const acquisition = repo.movies.startAcquisition(requested.targetId, "initial", () => repo.createJob({ type: "metadata_hydration", status: "succeeded" }).id).acquisition;
      const fileId = randomUUID();
      const version = repo.movies.recordVersion(acquisition.id, fixtureFacts(), [{ id: fileId, relativePath: `files/${fileId.slice(0, 2)}/${fileId}.mkv`, sizeBytes: 10, probe: {} }]);
      repo.movies.selectVersion(requested.targetId, version, 0);
      repo.movies.finishAcquisition(acquisition.id);
    }
  };
  addMovie(11, "Shared Movie", true);
  addMovie(12, "Private Pending Movie", false);
  addMovie(13, "Shared Second Movie", true);
  const tv = repo.tv.requestSeasons(owner.id, fixtureTvEntry(), settings.movies.qualityPolicy, [1]);
  const acquisition = repo.tv.startAcquisition(tv.targetId, "initial", () => repo.createJob({ type: "metadata_hydration", status: "succeeded" }).id, tv.requestId).acquisition;
  const fileId = randomUUID();
  const version = repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [{ id: fileId, relativePath: `files/${fileId.slice(0, 2)}/${fileId}.mkv`, sizeBytes: 10, probe: {}, episodeIds: [tv.episodeIds[0]!] }]);
  repo.tv.stagePublication(tv.targetId, version, 0);
  repo.tv.promotePublication(tv.targetId, version, 0);
  repo.tv.clearPublication(tv.targetId, version);
  repo.tv.finishAcquisition(acquisition.id);
  repo.tv.requestSeasons(owner.id, { ...fixtureTvEntry(), key: "tvdb:800", tvdbId: 800, tmdbId: 80, title: "Private pending TV" }, settings.movies.qualityPolicy, [2]);
  const handle = createPodibleFetchHandler(repo, Date.now());
  async function rpc(method: string, params = {}, signedIn = true) {
    return (await (await handle(new Request("http://localhost/rpc", { method: "POST", headers: {
      "content-type": "application/json", ...(signedIn ? { cookie: "podible_session=catalog-reader" } : {}),
    }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }))).json()) as any;
  }
  try {
    expect((await rpc("catalog.available", {}, false)).error.code).toBe(-32001);
    const limited = (await rpc("catalog.available", { query: "", limit: 1 })).result;
    expect(limited.movies).toHaveLength(1); expect(limited.movieTruncated).toBe(true);
    const all = (await rpc("catalog.available")).result;
    expect(all.movies.map((m: any) => m.title).sort()).toEqual(["Shared Movie", "Shared Second Movie"]);
    expect(all.tv).toHaveLength(1); expect(all.tv[0]).toMatchObject({ title: "A Work", availableEpisodes: 1 });
    expect((await rpc("catalog.available", { query: "Another Title" })).result.tv).toHaveLength(1);
    expect(JSON.stringify(all)).not.toContain("Private");
    expect(JSON.stringify(all)).not.toContain("requestId");
    expect((await rpc("catalog.available", { query: "SECOND" })).result.movies.map((m: any) => m.title)).toEqual(["Shared Second Movie"]);
    expect((await rpc("catalog.available", { query: "%" })).result.movies).toEqual([]);
    expect((await rpc("catalog.available", { limit: 101 })).error.code).toBe(-32602);
    const movie = await rpc("movies.get", { tmdbId: 11 });
    expect(movie.result).toMatchObject({ movie: { title: "Shared Movie" }, requests: [], availableInLibrary: true });
    const followed = await rpc("movies.request", { tmdbId: 11 });
    expect(followed.result.request.available).toBe(true);
    const detail = (await rpc("catalog.tvEntry", { entryId: tv.entryId })).result;
    expect(detail.availableEpisodes).toBe(1); expect(detail.entry.title).toBe("A Work");
    expect(JSON.stringify(detail)).not.toContain("Private owner");
    expect((await rpc("catalog.tvEntry", { entryId: 999 })).error.code).toBe(-32602);
    const routes = await handle(new Request("http://localhost/tv/library/1"));
    expect(routes.status).toBe(200);
  } finally { db.close(); }
});
