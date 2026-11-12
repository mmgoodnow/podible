import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { hashSessionToken } from "../../src/auth";
import { runMigrations } from "../../src/db";
import { createPodibleFetchHandler } from "../../src/http";
import { episodePlaybackFiles } from "../../src/media/playback-publication";
import { BooksRepo } from "../../src/repo";
import type { playbackRouter } from "../../src/rpc/playback-router";
import { fixtureFacts, fixtureMovie } from "../fixtures/movie-services";
import type { z } from "zod";

const machine = "a".repeat(40);
const origin = "https://podible.fixture.invalid";
const plexOrigin = "https://plex.fixture.invalid";
const viewerToken = "private-viewer-token";
const tmdbId = fixtureMovie.tmdbId;
const tvCatalog = { key: "tvdb:700", tvdbId: 700, tmdbId: 1700, title: "Fixture Show", aliases: [], year: 2020, anime: false,
  relations: [], episodes: [1, 2, 3, 4].map((index) => ({ key: `tvdb:${7000 + index}`, title: `Episode ${index}`,
    airDate: "2020-01-01", runtimeMinutes: 24,
    numbering: [{ scheme: "tvdb:aired", season: 1, episode: index, absolute: null }],
    refs: [{ provider: "tvdb" as const, id: String(7000 + index), source: "fixture" }] })) };

let sample: Uint8Array<ArrayBuffer> | undefined;
function mediaBytes() {
  if (!sample) {
    const ffmpeg = Bun.spawnSync(["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=s=32x32:r=10:d=0.3",
      "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000", "-shortest", "-c:v", "libx264", "-c:a", "aac",
      "-movflags", "frag_keyframe+empty_moov", "-f", "mp4", "pipe:1"]);
    if (ffmpeg.exitCode !== 0) throw new Error("Could not generate the playback MP4 fixture");
    sample = new Uint8Array(ffmpeg.stdout);
  }
  return sample;
}

function fixture() {
  const db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const settings = repo.ensureSettings();
  Object.assign(settings.movies, { plexUrl: plexOrigin, plexSectionId: "1", managedRoot: "/podible/movies", plexViewPath: "/plex/movies",
    plexToken: "private-configured-service-token" });
  Object.assign(settings.tv, { plexSectionId: "2", managedRoot: "/podible/tv", plexViewPath: "/plex/tv" });
  Object.assign(settings.auth.plex, { machineId: machine, ownerToken: "private-owner-token" });
  repo.updateSettings(settings);
  const user = repo.upsertUser({ provider: "plex", providerUserId: "fixture", username: "viewer" });
  const browser = repo.createSession(user.id, hashSessionToken("phone"));
  repo.createSession(user.id, hashSessionToken("other-tv"), null, "app");
  repo.createDeviceLoginGrant("grant", "verifier", "123456", new Date(Date.now() + 600_000).toISOString());
  repo.stageDeviceLoginPlexAccess("grant", browser.id, machine, viewerToken);
  repo.approveDeviceLogin("grant", "123456", user.id, browser.id);
  expect(repo.exchangeDeviceLogin("grant", "verifier", hashSessionToken("tv"))).toBe("approved");
  const handle = createPodibleFetchHandler(repo, Date.now());
  const rpc = async <M extends keyof typeof playbackRouter.routes>(method: M, params: object, token = "tv") => {
    const response = await handle(new Request(`${origin}/rpc`, { method: "POST", headers: {
      Authorization: `Bearer ${token}`, "Content-Type": "application/json",
    }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: `playback.${method}`, params }) }));
    expect(response.headers.get("cache-control")).toBe("no-store");
    return await response.json() as { result?: z.output<(typeof playbackRouter.routes)[M]["resultSchema"]>; error?: { code: number } };
  };
  const stream = (path: string, credential?: string, range?: string, method = "GET") => handle(new Request(`${origin}${path}`, {
    method, headers: { ...(credential ? { cookie: `podible_playback=${credential}` } : {}), ...(range ? { range } : {}) },
  }));
  const bytes = mediaBytes();
  const lists = new Map<string, object[]>();
  const authorizedParts = new Set<number>();
  const calls: { path: string; token: string | null; range: string | null }[] = [];
  let servedMachine = machine;
  const fetchOriginal = globalThis.fetch;
  globalThis.fetch = (async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
    const url = new URL(String(input));
    if (url.origin !== plexOrigin || init?.redirect !== "error" || url.searchParams.has("X-Plex-Token")) throw new Error("Unexpected Plex upstream");
    const headers = new Headers(init.headers);
    const token = headers.get("X-Plex-Token");
    const range = headers.get("range");
    calls.push({ path: url.pathname, token, range });
    if (url.pathname === "/identity") return Response.json({ MediaContainer: { machineIdentifier: servedMachine, claimed: true } });
    if (token !== viewerToken) return new Response("private upstream credential message", { status: 403 });
    const metadata = lists.get(url.pathname);
    if (metadata) return Response.json({ MediaContainer: { size: metadata.length, totalSize: metadata.length, Metadata: metadata } });
    const part = /^\/library\/parts\/(\d+)\/1\/file\.mp4$/.exec(url.pathname);
    if (!part || !authorizedParts.has(Number(part[1]))) return new Response(null, { status: 403 });
    const selected = range ? /^bytes=(\d+)-(\d+)$/.exec(range) : null;
    const start = selected ? Number(selected[1]) : 0;
    const end = selected ? Number(selected[2]) : bytes.length - 1;
    return new Response(bytes.slice(start, end + 1), { status: selected ? 206 : 200, headers: {
      "Content-Type": "video/mp4", "Content-Length": String(end - start + 1),
      ...(selected ? { "Content-Range": `bytes ${start}-${end}/${bytes.length}` } : {}),
    } });
  }) as typeof fetch;

  function media(id: number, file: string, container = "mp4", size = bytes.length) {
    return { container, videoCodec: "h264", audioCodec: "aac",
      Part: [{ id, key: `/library/parts/${id}/1/file.${container}`, file, size }] };
  }
  function plexMovie(ratingKey: number, parts: object[]) {
    const item = { type: "movie", ratingKey, Guid: [{ id: `tmdb://${tmdbId}` }], librarySectionID: 1 };
    lists.set("/library/sections/1/all", [...lists.get("/library/sections/1/all") ?? [], item]);
    lists.set(`/library/metadata/${ratingKey}`, [{ ...item, Media: parts }]);
  }
  const file = (ext: string) => ({ id: randomUUID(), relativePath: `files/${randomUUID()}.${ext}`, sizeBytes: bytes.length, probe: {} });
  function publishMovie(staged = false, width = 1280, policy = settings.movies.qualityPolicy) {
    const requested = repo.movies.request(user.id, fixtureMovie, policy);
    const acquisition = repo.movies.startAcquisition(requested.targetId, staged ? "upgrade" : "initial",
      () => repo.createJob({ type: "metadata_hydration" }).id).acquisition;
    const version = repo.movies.recordVersion(acquisition.id, fixtureFacts(width), [file("mp4")]);
    if (staged) repo.movies.stagePublication(requested.targetId, version);
    else repo.movies.selectVersion(requested.targetId, version, 0);
    repo.movies.finishAcquisition(acquisition.id);
    return { targetId: requested.targetId, version, file: `/plex/movies/A Fixture Film (2024) {tmdb-101}/A Fixture Film - t${requested.targetId}v${version}.mp4` };
  }
  function publishShow() {
    const requested = repo.tv.request(user.id, tvCatalog, settings.movies.qualityPolicy, tvCatalog.episodes.map((episode) => episode.key));
    const acquisition = repo.tv.startAcquisition(requested.targetId, "initial",
      () => repo.createJob({ type: "metadata_hydration" }).id, requested.requestId).acquisition;
    const [first, second, third, fourth] = requested.episodeIds;
    const version = repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [
      { ...file("mp4"), episodeIds: [first!] }, { ...file("mkv"), episodeIds: [second!] }, { ...file("mp4"), episodeIds: [third!, fourth!] },
    ]);
    repo.tv.stagePublication(requested.targetId, version, 0);
    repo.tv.promotePublication(requested.targetId, version, 0);
    repo.tv.clearPublication(requested.targetId, version);
    const name = `/plex/tv/Fixture Show (2020) {tvdb-700}/Season 01/Fixture Show - `;
    const episodePath = (tag: string, ext: string) => `${name}${tag} - t${requested.targetId}v${version}.${ext}`;
    expect([...episodePlaybackFiles(repo, settings, requested.entryId)].map(([id, files]) => [id, ...files.keys()])).toEqual([
      [first, episodePath("S01E01", "mp4")], [second, episodePath("S01E02", "mkv")],
    ]);
    lists.set("/library/sections/2/all", [{ ratingKey: "70", type: "show", Guid: [{ id: "tvdb://700" }] }]);
    const episode = (index: number, parent: number, part: object) => ({ ratingKey: 100 * parent + index, type: "episode",
      grandparentRatingKey: parent, Guid: [{ id: `tvdb://${7000 + index}` }], parentIndex: 1, index, Media: [part] });
    lists.set("/library/metadata/70/allLeaves", [episode(1, 70, media(10, episodePath("S01E01", "mp4"))),
      episode(1, 71, media(99, episodePath("S01E01", "mp4"))), episode(2, 70, media(11, episodePath("S01E02", "mkv"), "mkv")),
      episode(3, 70, media(12, episodePath("S01E03-E04", "mp4"))), episode(4, 70, media(12, episodePath("S01E03-E04", "mp4")))]);
    return requested;
  }
  return { repo, settings, handle, fetchOriginal, rpc, stream, bytes, calls, media, plexMovie, publishMovie, publishShow, authorizedParts,
    changeMachine() { servedMachine = "b".repeat(40); }, close() { globalThis.fetch = fetchOriginal; db.close(); } };
}

test("movie playback never falls back to configured Plex tokens and requires the exact session and machine", async () => {
  const f = fixture();
  try {
    const input = { kind: "movie", tmdbId };
    expect((await f.rpc("prepare", input, "phone")).error?.code).toBe(-32001);
    expect((await f.rpc("prepare", input, "other-tv")).error?.code).toBe(-32003);
    expect((await f.rpc("prepare", { ...input, path: "/library/parts/1" })).error?.code).toBe(-32602);
    expect(f.calls).toEqual([]);
    f.publishMovie();
    f.changeMachine();
    expect((await f.rpc("prepare", input)).error?.code).toBe(-32050);
    expect(f.calls).toEqual([{ path: "/identity", token: null, range: null }]);
  } finally { f.close(); }
});

test("movie grants use only an exact active path, support HEAD and range seek, and die on session revocation", async () => {
  const f = fixture();
  try {
    const selected = f.publishMovie();
    const staged = f.publishMovie(true, 1920);
    f.plexMovie(21, [f.media(1, "/unmanaged/A Fixture Film.mp4"), f.media(2, staged.file)]);
    const input = { kind: "movie", tmdbId };
    expect((await f.rpc("prepare", input)).error?.code).toBe(-32044);
    f.plexMovie(22, [f.media(3, selected.file, "mp4", f.bytes.length + 1)]);
    expect((await f.rpc("prepare", input)).error?.code).toBe(-32044);
    f.plexMovie(23, [f.media(4, selected.file)]);
    expect((await f.rpc("prepare", input)).error?.code).toBe(-32003);
    f.authorizedParts.add(4);
    const grant = (await f.rpc("prepare", input)).result!;
    expect(grant.path).toBe(`/api/playback/file/${grant.id}.mp4`);
    expect(JSON.stringify(grant)).not.toMatch(/private-|plex\.fixture|\/plex\/movies/);
    expect((await f.stream(grant.path)).status).toBe(401);
    const head = await f.stream(grant.path, grant.credential, undefined, "HEAD");
    expect(head.status).toBe(200);
    expect(head.headers.get("content-length")).toBe(String(f.bytes.length));
    expect((await head.arrayBuffer()).byteLength).toBe(0);
    const ranged = await f.stream(grant.path, grant.credential, "bytes=4-15");
    expect(ranged.status).toBe(206);
    expect(ranged.headers.get("cache-control")).toBe("private, no-store");
    expect(ranged.headers.get("content-range")).toBe(`bytes 4-15/${f.bytes.length}`);
    expect(new Uint8Array(await ranged.arrayBuffer())).toEqual(f.bytes.slice(4, 16));
    using server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: f.handle });
    const full = await f.fetchOriginal(new URL(grant.path, server.url), { headers: { cookie: `podible_playback=${grant.credential}` } });
    expect(full.status).toBe(200);
    expect(new Uint8Array(await full.arrayBuffer())).toEqual(f.bytes);
    expect((await f.stream(grant.path, grant.credential, "bytes=0-1,4-7")).status).toBe(416);
    expect((await f.rpc("close", { id: grant.id }, "other-tv")).result).toEqual({ ok: true });
    expect((await f.stream(grant.path, grant.credential, "bytes=-4")).status).toBe(206);
    f.repo.deleteSession(f.repo.getSessionByTokenHash(hashSessionToken("tv"))!.id);
    const calls = f.calls.length;
    expect((await f.stream(grant.path, grant.credential)).status).toBe(401);
    expect(f.calls).toHaveLength(calls);
    expect(f.calls.every(({ path, token }) => path === "/identity" ? token === null : token === viewerToken)).toBe(true);
    expect(f.calls.filter(({ path }) => path.startsWith("/library/parts/")).every(({ path }) => path.includes("/4/"))).toBe(true);
  } finally { f.close(); }
});

test("a changed active movie edition invalidates the old grant and ambiguous direct copies fail closed", async () => {
  const f = fixture();
  try {
    const selected = f.publishMovie();
    f.plexMovie(21, [f.media(1, selected.file)]);
    f.authorizedParts.add(1);
    const input = { kind: "movie", tmdbId };
    const old = (await f.rpc("prepare", input)).result!;
    const staged = f.publishMovie(true, 1920);
    f.repo.movies.selectVersion(staged.targetId, staged.version, 1);
    f.repo.movies.clearPublication(staged.targetId, staged.version);
    expect((await f.stream(old.path, old.credential)).status).toBe(404);
    const alternate = f.publishMovie(false, 1280, { ...f.settings.movies.qualityPolicy, preferredCodecs: ["h264"] });
    expect(alternate.targetId).not.toBe(selected.targetId);
    f.plexMovie(22, [f.media(2, staged.file), f.media(3, alternate.file)]);
    expect((await f.rpc("prepare", input)).error?.code).toBe(-32015);
  } finally { f.close(); }
});

test("episodes require the exact show, provider episode and active path; MKV and combined files are not direct played", async () => {
  const f = fixture();
  try {
    const show = f.publishShow();
    const [first, second, third] = show.episodeIds;
    expect((await f.rpc("showEpisodes", { source: "tmdb", id: 1700 })).result).toEqual({ entryId: show.entryId,
      episodes: [{ id: first!, title: "Episode 1", season: 1, episode: 1, playable: true },
        { id: second!, title: "Episode 2", season: 1, episode: 2, playable: false }] });
    f.authorizedParts.add(10);
    const input = { kind: "episode", entryId: show.entryId, episodeId: first! };
    const grant = (await f.rpc("prepare", input)).result!;
    expect((await f.stream(grant.path, grant.credential, "bytes=0-7")).status).toBe(206);
    expect((await f.rpc("prepare", { ...input, episodeId: second! })).error?.code).toBe(-32015);
    expect((await f.rpc("prepare", { ...input, episodeId: third! })).error?.code).toBe(-32015);
    expect((await f.rpc("prepare", { ...input, entryId: show.entryId + 1 })).error?.code).toBe(-32044);
    expect((await f.rpc("close", { id: grant.id })).result).toEqual({ ok: true });
    expect((await f.stream(grant.path, grant.credential)).status).toBe(401);
  } finally { f.close(); }
});
