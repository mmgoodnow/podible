import { Database } from "bun:sqlite";
import { expect, spyOn, test } from "bun:test";
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
const hlsRoot = "/video/:/transcode/universal";
const playlistType = "application/vnd.apple.mpegurl";
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

let segment: Uint8Array<ArrayBuffer> | undefined;
function segmentBytes() {
  if (!segment) {
    const ffmpeg = Bun.spawnSync(["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", "pipe:0", "-c", "copy", "-f", "mpegts", "pipe:1"],
      { stdin: mediaBytes() });
    if (ffmpeg.exitCode !== 0) throw new Error("Could not generate the playback HLS segment fixture");
    segment = new Uint8Array(ffmpeg.stdout);
  }
  return segment;
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
  const starts: { query: URLSearchParams; headers: Headers }[] = [];
  const stops: string[] = [];
  const hlsSessions = new Set<string>();
  const hls = {
    master: (id: string) => `#EXTM3U\n#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=296000,RESOLUTION=320x180,FRAME-RATE=24.000000\nsession/${id}/base/index.m3u8\n`,
    media: (_id: string) => `#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-START:TIME-OFFSET=-0.1,PRECISE=NO\n#EXTINF:0.3,${viewerToken}\n00000.ts\n#EXT-X-ENDLIST\n`,
    segmentStatus: 200,
    segment: segmentBytes,
  };
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
    if (url.pathname === `${hlsRoot}/start.m3u8`) {
      const id = url.searchParams.get("transcodeSessionId")!;
      starts.push({ query: url.searchParams, headers });
      if (id !== url.searchParams.get("session") || id !== headers.get("X-Plex-Session-Identifier")) return new Response(null, { status: 400 });
      hlsSessions.add(id);
      return new Response(hls.master(id), { headers: { "Content-Type": playlistType } });
    }
    if (url.pathname === `${hlsRoot}/stop`) {
      const id = url.searchParams.get("session")!;
      stops.push(id);
      hlsSessions.delete(id);
      return new Response(null);
    }
    if (url.pathname.startsWith(`${hlsRoot}/session/`)) {
      const [id, variant, resource, extra] = url.pathname.slice(`${hlsRoot}/session/`.length).split("/");
      if (!id || !hlsSessions.has(id) || id !== headers.get("X-Plex-Session-Identifier") || variant !== "base" || extra || url.search)
        return new Response(null, { status: 404 });
      if (resource === "index.m3u8") return new Response(hls.media(id), { headers: { "Content-Type": playlistType } });
      if (resource === "00000.ts") {
        const bytes = hls.segment();
        return new Response(bytes, { status: hls.segmentStatus, headers: {
          "Content-Type": "video/mp2t", "Content-Length": String(bytes.length), "Set-Cookie": "upstream=secret", Location: "https://elsewhere.invalid/",
        } });
      }
      return new Response(null, { status: 404 });
    }
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
  function publishMovie(staged = false, width = 1280, policy = settings.movies.qualityPolicy, ext = "mp4") {
    const requested = repo.movies.request(user.id, fixtureMovie, policy);
    const acquisition = repo.movies.startAcquisition(requested.targetId, staged ? "upgrade" : "initial",
      () => repo.createJob({ type: "metadata_hydration" }).id).acquisition;
    const version = repo.movies.recordVersion(acquisition.id, fixtureFacts(width), [file(ext)]);
    if (staged) repo.movies.stagePublication(requested.targetId, version);
    else repo.movies.selectVersion(requested.targetId, version, 0);
    repo.movies.finishAcquisition(acquisition.id);
    return { targetId: requested.targetId, version, file: `/plex/movies/A Fixture Film (2024) {tmdb-101}/A Fixture Film - t${requested.targetId}v${version}.${ext}` };
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
    const secondPart = media(11, episodePath("S01E02", "mkv"), "mkv");
    const secondEpisode = episode(2, 70, secondPart);
    lists.set("/library/metadata/70/allLeaves", [episode(1, 70, media(10, episodePath("S01E01", "mp4"))),
      episode(1, 71, media(99, episodePath("S01E01", "mp4"))), secondEpisode,
      episode(3, 70, media(12, episodePath("S01E03-E04", "mp4"))), episode(4, 70, media(12, episodePath("S01E03-E04", "mp4")))]);
    const wrongPart = media(13, "/unmanaged/missing.mkv", "mkv");
    lists.set(`/library/metadata/${secondEpisode.ratingKey}`, [{ ...secondEpisode, Media: [wrongPart,
      { ...secondPart, Part: [...wrongPart.Part, ...secondPart.Part] }] }]);
    return requested;
  }
  return { repo, settings, handle, fetchOriginal, rpc, stream, bytes, calls, media, plexMovie, publishMovie, publishShow, authorizedParts,
    starts, stops, hls,
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

test("episodes bind HLS to fresh exact episode metadata and reject combined episode files", async () => {
  const f = fixture();
  try {
    const show = f.publishShow();
    const [first, second, third] = show.episodeIds;
    expect((await f.rpc("showEpisodes", { source: "tmdb", id: 1700 })).result).toEqual({ entryId: show.entryId,
      episodes: [{ id: first!, title: "Episode 1", season: 1, episode: 1, playable: true },
        { id: second!, title: "Episode 2", season: 1, episode: 2, playable: true }] });
    f.authorizedParts.add(10);
    const input = { kind: "episode", entryId: show.entryId, episodeId: first! };
    const grant = (await f.rpc("prepare", input)).result!;
    expect((await f.stream(grant.path, grant.credential, "bytes=0-7")).status).toBe(206);
    const fallback = (await f.rpc("prepare", { ...input, episodeId: second! })).result!;
    expect(fallback.path).toBe(`/api/playback/hls/${fallback.id}/master.m3u8`);
    const start = f.starts[0]!.query;
    expect([start.get("path"), start.get("mediaIndex"), start.get("partIndex")]).toEqual(["/library/metadata/7002", "1", "1"]);
    expect((await f.rpc("prepare", { ...input, episodeId: third! })).error?.code).toBe(-32015);
    expect((await f.rpc("prepare", { ...input, entryId: show.entryId + 1 })).error?.code).toBe(-32044);
    expect((await f.rpc("close", { id: grant.id })).result).toEqual({ ok: true });
    expect((await f.stream(grant.path, grant.credential)).status).toBe(401);
    expect((await f.rpc("close", { id: fallback.id })).result).toEqual({ ok: true });
    expect(f.stops).toEqual([start.get("session")!]);
    expect((await f.stream(fallback.path, fallback.credential)).status).toBe(401);
  } finally { f.close(); }
});

test("movie MKV fallback exposes only same-grant playlists and listed segments, not Plex credentials or paths", async () => {
  const f = fixture();
  try {
    const selected = f.publishMovie(false, 1280, undefined, "mkv");
    const foreign = f.media(1, "/other/copy.mkv", "mkv");
    const exact = f.media(2, selected.file, "mkv");
    f.plexMovie(31, [foreign]);
    f.plexMovie(32, [foreign, { ...exact, Part: [...foreign.Part, ...exact.Part] }]);
    const input = { kind: "movie", tmdbId };
    const grant = (await f.rpc("prepare", input)).result!;
    const base = `/api/playback/hls/${grant.id}`;
    const { query, headers } = f.starts[0]!;
    const plexSession = query.get("transcodeSessionId")!;
    expect(plexSession).not.toBe(grant.id);
    expect(Object.fromEntries(query)).toMatchObject({ path: "/library/metadata/32", mediaIndex: "1", partIndex: "1",
      directPlay: "0", directStream: "1", protocol: "hls", session: plexSession });
    expect(headers.get("X-Plex-Client-Profile-Extra")).toContain("container=mpegts&videoCodec=h264&audioCodec=aac");
    expect(headers.get("X-Plex-Token")).toBe(viewerToken);
    expect(JSON.stringify(grant)).not.toMatch(/private-|\/video\/|plex\.fixture/);
    expect((await f.stream(grant.path)).status).toBe(401);
    const master = await f.stream(grant.path, grant.credential);
    expect(master.headers.get("cache-control")).toBe("private, no-store");
    expect(await master.text()).toBe(`#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=296000\n${base}/media.m3u8\n`);
    const media = await f.stream(`${base}/media.m3u8`, grant.credential);
    const playlist = await media.text();
    expect(playlist).toContain(`${base}/00000.ts`);
    expect(playlist).toContain("#EXT-X-START:TIME-OFFSET=-0.1,PRECISE=NO\n");
    expect(playlist).not.toMatch(/private-|\/video\/|plex\.fixture|token=/i);
    expect(playlist).not.toContain(plexSession);
    const calls = f.calls.length;
    expect((await f.stream(`${base}/00001.ts`, grant.credential)).status).toBe(404);
    expect((await f.stream(`${base}/00000.ts?path=https://other.invalid`, grant.credential)).status).toBe(404);
    expect((await f.stream(`${base}/%2Fidentity`, grant.credential)).status).toBe(404);
    expect(f.calls).toHaveLength(calls);
    using server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: f.handle });
    const response = await f.fetchOriginal(new URL(`${base}/00000.ts`, server.url), { headers: {
      cookie: `podible_playback=${grant.credential}`, range: "bytes=0-7",
    } });
    expect(response.status).toBe(200);
    expect(f.calls.at(-1)?.range).toBeNull();
    expect(response.headers.get("set-cookie")).toBeNull();
    expect(response.headers.get("location")).toBeNull();
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(segmentBytes());
    expect((await f.rpc("prepare", input)).error?.code).toBe(-32029);
    expect(f.starts).toHaveLength(1);
    f.repo.deleteSession(f.repo.getSessionByTokenHash(hashSessionToken("tv"))!.id);
    expect((await f.stream(`${base}/00000.ts`, grant.credential)).status).toBe(401);
    expect(f.calls.every(({ path, token }) => path === "/identity" ? token === null : token === viewerToken)).toBe(true);
  } finally { f.close(); }
});

test("unsupported codecs fail closed on foreign HLS references, credential-bearing URLs, and bad segment responses", async () => {
  const f = fixture();
  try {
    const selected = f.publishMovie();
    f.plexMovie(31, [{ ...f.media(4, selected.file), audioCodec: "dca" }]);
    const input = { kind: "movie", tmdbId };
    const { master, media } = f.hls;
    for (const target of ["https://elsewhere.invalid/index.m3u8", "session/some-other-session/base/index.m3u8", "../base/index.m3u8"]) {
      f.hls.master = (id) => master(id).replace(`session/${id}/base/index.m3u8`, target);
      expect((await f.rpc("prepare", input)).error?.code).toBe(-32015);
    }
    f.hls.master = master;
    for (const target of ["00000.ts?X-Plex-Token=secret", "../00000.ts", "//elsewhere.invalid/00000.ts",
      `${hlsRoot}/session/foreign/base/00000.ts`, '#EXT-X-KEY:METHOD=AES-128,URI="https://elsewhere.invalid/key"\n00000.ts']) {
      f.hls.media = (id) => media(id).replace("00000.ts", target);
      expect((await f.rpc("prepare", input)).error?.code).toBe(-32015);
    }
    f.hls.media = media;
    f.hls.segmentStatus = 302;
    expect((await f.rpc("prepare", input)).error?.code).toBe(-32050);
    f.hls.segmentStatus = 200;
    f.hls.segment = () => new Uint8Array(188);
    expect((await f.rpc("prepare", input)).error?.code).toBe(-32050);
    expect(f.stops).toEqual(f.starts.map(({ query }) => query.get("session")!));
    f.hls.segment = segmentBytes;
    expect((await f.rpc("prepare", input)).result?.path).toEndWith("/master.m3u8");
  } finally { f.close(); }
});

test("an abandoned HLS grant expires before playback but a started stream survives a pause", async () => {
  let time = Date.now();
  const clock = spyOn(Date, "now").mockImplementation(() => time);
  const f = fixture();
  try {
    const selected = f.publishMovie(false, 1280, undefined, "mkv");
    f.plexMovie(31, [f.media(2, selected.file, "mkv")]);
    const input = { kind: "movie", tmdbId };
    const grant = (await f.rpc("prepare", input)).result!;
    time += 4 * 60_000;
    expect((await f.stream(grant.path, grant.credential)).status).toBe(200);
    time += 2 * 60_000;
    expect((await f.stream(grant.path, grant.credential)).status).toBe(401);
    const restarted = (await f.rpc("prepare", input)).result!;
    expect(restarted.id).not.toBe(grant.id);
    const segment = restarted.path.replace("master.m3u8", "00000.ts");
    expect(await (await f.stream(segment, restarted.credential)).arrayBuffer()).toEqual(segmentBytes().buffer);
    time += 10 * 60_000;
    expect((await f.stream(segment, restarted.credential)).status).toBe(200);
    expect((await f.rpc("prepare", input)).error?.code).toBe(-32029);
  } finally { f.close(); clock.mockRestore(); }
});

test("a publication change while Plex loads a segment prevents delivery", async () => {
  const f = fixture();
  try {
    const old = f.publishMovie(false, 1280, undefined, "mkv");
    f.plexMovie(31, [f.media(2, old.file, "mkv")]);
    const grant = (await f.rpc("prepare", { kind: "movie", tmdbId })).result!;
    const next = f.publishMovie(true, 1920, undefined, "mkv");
    f.hls.segment = () => {
      f.repo.movies.selectVersion(old.targetId, next.version, 1);
      return segmentBytes();
    };
    const response = await f.stream(grant.path.replace("master.m3u8", "00000.ts"), grant.credential);
    expect(response.status).toBe(404);
    expect(await response.text()).toBe("");
  } finally { f.close(); }
});
