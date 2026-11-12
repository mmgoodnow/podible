import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { z } from "zod";
import type { AppSettings, SessionWithUserRow } from "../app-types";
import { resolveBearerSessionFromRequest } from "../auth";
import { directPart, episodeIdentity, hasPublishedPart, matchesPlexIdentity, PlaybackError, playbackConfig, PlexPlayback, type PlayablePart } from "./plex-playback";
import { episodePlaybackFiles, moviePlaybackFiles } from "./playback-publication";
import type { BooksRepo } from "../repo";

const numericId = z.number().int().positive();
export const playbackShowSchema = z.strictObject({ source: z.enum(["entry", "tmdb"]), id: numericId });
export const playbackTargetSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("movie"), tmdbId: numericId }),
  z.strictObject({ kind: z.literal("episode"), entryId: numericId, episodeId: numericId }),
]);
type Target = z.infer<typeof playbackTargetSchema>;
export const playbackIdSchema = z.uuidv4();
type PlaybackRepo = Pick<BooksRepo, "readSettings" | "getSessionByTokenHash" | "getViewerPlexTokenForSession" | "movies" | "tv" | "contentLoss">;
export type PlaybackDependencies = {
  /** A server-specific token from this viewer's Plex grant; never a configured service/owner fallback. */
  viewerPlexToken: (sessionId: number, machineId: string) => string | null | Promise<string | null>;
  fetcher?: typeof fetch;
  now?: () => number;
};
type Grant = {
  verifier: Buffer;
  expiresAt: number;
  sessionId: number;
  userId: number;
  sessionHash: string;
  tokenHash: string;
  config: ReturnType<typeof playbackConfig>;
  target: Target;
  part: PlayablePart;
};
const hash = (value: string) => createHash("sha256").update(value).digest();
export const playbackCookie = "podible_playback";
const ttlSeconds = 6 * 60 * 60;

function liveAppSession(session: SessionWithUserRow | null, now: number): session is SessionWithUserRow {
  return session?.kind === "app" && (session.expires_at === null || Date.parse(session.expires_at) > now);
}

function parseRange(header: string | null, size: number) {
  if (header === null) return null;
  const match = /^bytes=(\d{0,16})-(\d{0,16})$/.exec(header);
  if (!match || !(match[1] || match[2])) return false;
  const first = match[1] ? Number(match[1]) : null;
  const last = match[2] ? Number(match[2]) : null;
  if ((first !== null && !Number.isSafeInteger(first)) || (last !== null && !Number.isSafeInteger(last))) return false;
  const start = first ?? Math.max(0, size - last!);
  const end = first === null || last === null ? size - 1 : Math.min(last, size - 1);
  return start < size && start <= end ? { start, end } : false;
}
type ByteRange = Exclude<ReturnType<typeof parseRange>, false>;
const rangeHeader = (range: ByteRange) => range && `bytes=${range.start}-${range.end}`;
const contentRange = (range: NonNullable<ByteRange>, size: number) => `bytes ${range.start}-${range.end}/${size}`;

async function checkPartResponse(response: Response, part: PlayablePart, range: ByteRange) {
  const length = range ? range.end - range.start + 1 : part.size;
  if (response.status !== (range ? 206 : 200) || !response.body ||
      response.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase() !== "video/mp4" ||
      ![null, "identity"].includes(response.headers.get("content-encoding")) ||
      response.headers.get("content-length") !== String(length) ||
      response.headers.get("content-range") !== (range ? contentRange(range, part.size) : null)) {
    await response.body?.cancel();
    throw new PlaybackError(response.status === 401 || response.status === 403 ? 403 : response.status === 404 ? 404 : 502);
  }
  return response.body;
}

export function createPlaybackService(repo: PlaybackRepo, dependencies: PlaybackDependencies) {
  const grants = new Map<string, Grant>();
  const now = dependencies.now ?? Date.now;
  const fetcher = dependencies.fetcher ?? fetch;

  function authenticate(request: Request) {
    const session = resolveBearerSessionFromRequest(request, (digest) => repo.getSessionByTokenHash(digest));
    if (!liveAppSession(session, now())) throw new PlaybackError(401);
    return session;
  }

  async function source(session: SessionWithUserRow, kind: Target["kind"], signal: AbortSignal) {
    const settings = repo.readSettings();
    if (!settings) throw new PlaybackError(503);
    const config = playbackConfig(settings, kind);
    const token = await dependencies.viewerPlexToken(session.id, config.machineId);
    if (!token || !/^[\x21-\x7e]{1,1024}$/.test(token)) throw new PlaybackError(403);
    return { plex: new PlexPlayback(config, token, fetcher, signal), settings, config, tokenHash: hash(token).toString("hex") };
  }

  function tvEntry(entryId: number) {
    const published = new Set(repo.tv.publishedEpisodeIds(entryId));
    const entry = published.size ? repo.tv.entry(entryId) : null;
    if (!entry) throw new PlaybackError(404);
    const expected = { ...(entry.tvdbId ? { tvdb: String(entry.tvdbId) } : {}), ...(entry.tmdbId ? { tmdb: String(entry.tmdbId) } : {}) };
    if (!Object.keys(expected).length) throw new PlaybackError(404);
    return { entry, published, expected };
  }

  async function showEpisodes(request: Request, input: z.infer<typeof playbackShowSchema>) {
    const session = authenticate(request);
    const entryId = input.source === "entry" ? input.id : repo.tv.visibleShowId(input.id, session.user_id);
    if (entryId === null) throw new PlaybackError(404);
    const { entry, published, expected } = tvEntry(entryId);
    const { plex, settings } = await source(session, "episode", AbortSignal.any([request.signal, AbortSignal.timeout(20_000)]));
    const publishedFiles = episodePlaybackFiles(repo, settings, entryId);
    await plex.verifyServer();
    const rows = await plex.episodeRows(expected);
    const episodes = entry.episodes.flatMap((episode) => {
      const files = publishedFiles.get(episode.id);
      if (!published.has(episode.id) || !files?.size) return [];
      const identity = episodeIdentity(episode);
      const matches = rows.filter((row) => matchesPlexIdentity(row, identity) && hasPublishedPart(row, files));
      const match = matches[0];
      if (!match || matches.length !== 1) return [];
      return [{ id: episode.id, title: (episode.title || "Episode").slice(0, 300), season: match.parentIndex ?? null,
        episode: match.index ?? null, playable: directPart(matches, files) !== null }];
    }).sort((a, b) => (a.season ?? 0) - (b.season ?? 0) || (a.episode ?? 0) - (b.episode ?? 0) || a.id - b.id);
    return { entryId, episodes };
  }

  function partStillPublished(settings: AppSettings, target: Target, part: PlayablePart) {
    if (target.kind === "episode")
      return episodePlaybackFiles(repo, settings, target.entryId).get(target.episodeId)?.get(part.path) === part.size;
    const movie = repo.movies.movieByTmdb(target.tmdbId);
    return movie !== null && moviePlaybackFiles(repo, settings, movie).get(part.path) === part.size;
  }

  async function prepare(request: Request, target: Target) {
    const session = authenticate(request);
    const signal = AbortSignal.any([request.signal, AbortSignal.timeout(20_000)]);
    const { plex, settings, config, tokenHash } = await source(session, target.kind, signal);
    let part: PlayablePart | null;
    if (target.kind === "movie") {
      const movie = repo.movies.availableByTmdb(target.tmdbId) ? repo.movies.movieByTmdb(target.tmdbId) : null;
      if (!movie) throw new PlaybackError(404);
      const files = moviePlaybackFiles(repo, settings, movie);
      if (!files.size) throw new PlaybackError(415);
      await plex.verifyServer();
      part = await plex.moviePart({ tmdb: String(target.tmdbId), ...(movie.imdbId ? { imdb: movie.imdbId } : {}) }, files);
    } else {
      const { entry, published, expected } = tvEntry(target.entryId);
      const episode = published.has(target.episodeId) ? entry.episodes.find((episode) => episode.id === target.episodeId) : null;
      const identity = episode && episodeIdentity(episode);
      if (!identity || !Object.keys(identity).length) throw new PlaybackError(404);
      const files = episodePlaybackFiles(repo, settings, entry.id).get(target.episodeId);
      if (!files?.size) throw new PlaybackError(415);
      await plex.verifyServer();
      const matches = (await plex.episodeRows(expected)).filter((row) => matchesPlexIdentity(row, identity) && hasPublishedPart(row, files));
      if (matches.length !== 1) throw new PlaybackError(404);
      part = directPart(matches, files);
    }
    if (!part) throw new PlaybackError(415);
    const probe = { start: 0, end: 7 };
    const probeBody = await checkPartResponse(await plex.file(part, rangeHeader(probe)), part, probe);
    const bytes = await readProbe(probeBody);
    if (bytes.length !== 8 || bytes.subarray(4).toString("ascii") !== "ftyp") throw new PlaybackError(415);
    // Recheck revocation after asynchronous lookup and upstream authorization.
    if (authenticate(request).id !== session.id) throw new PlaybackError(401);
    if (!partStillPublished(settings, target, part)) throw new PlaybackError(404);
    for (const [id, grant] of grants) if (grant.expiresAt <= now()) grants.delete(id);
    if (grants.size >= 512 || [...grants.values()].filter((grant) => grant.sessionId === session.id).length >= 8)
      throw new PlaybackError(429);
    const id = randomUUID();
    const credential = randomBytes(32).toString("base64url");
    grants.set(id, { verifier: hash(credential), expiresAt: now() + ttlSeconds * 1000, sessionId: session.id,
      userId: session.user_id, sessionHash: session.token_hash, tokenHash, config, target, part });
    return { id, path: `/api/playback/file/${id}.mp4`, credential, expiresInSeconds: ttlSeconds };
  }

  function close(request: Request, id: string) {
    const session = authenticate(request);
    if (grants.get(id)?.sessionId === session.id) grants.delete(id);
    return { ok: true as const };
  }

  async function stream(request: Request, id: string, credential: string | undefined) {
    const grant = grants.get(id);
    if (!grant || grant.expiresAt <= now() || !credential || !/^[A-Za-z0-9_-]{43}$/.test(credential) ||
        !timingSafeEqual(grant.verifier, hash(credential))) throw new PlaybackError(401);
    const session = repo.getSessionByTokenHash(grant.sessionHash);
    if (!liveAppSession(session, now()) || session.id !== grant.sessionId || session.user_id !== grant.userId) throw new PlaybackError(401);
    const { target, part } = grant;
    if (target.kind === "movie" ? !repo.movies.availableByTmdb(target.tmdbId) :
        !repo.tv.publishedEpisodeIds(target.entryId).includes(target.episodeId)) throw new PlaybackError(404);
    const range = parseRange(request.headers.get("range"), part.size);
    if (range === false) return new Response(null, { status: 416, headers: { "Content-Range": `bytes */${part.size}` } });
    const { plex, settings, config, tokenHash } = await source(session, target.kind, request.signal);
    if (grant.tokenHash !== tokenHash || config.origin !== grant.config.origin || config.machineId !== grant.config.machineId ||
        config.section !== grant.config.section) throw new PlaybackError(403);
    if (!partStillPublished(settings, target, part)) throw new PlaybackError(404);
    await plex.verifyServer();
    const head = request.method === "HEAD";
    const upstreamRange = head ? { start: 0, end: 7 } : range;
    const body = await checkPartResponse(await plex.file(part, rangeHeader(upstreamRange)), part, upstreamRange);
    if (grants.get(id) !== grant || !liveAppSession(repo.getSessionByTokenHash(grant.sessionHash), now())) {
      await body.cancel();
      throw new PlaybackError(401);
    }
    if (head) await body.cancel();
    const headers = new Headers({ "Accept-Ranges": "bytes", "Content-Type": "video/mp4",
      "Content-Length": String(range ? range.end - range.start + 1 : part.size) });
    if (range) headers.set("Content-Range", contentRange(range, part.size));
    return new Response(head ? null : body, { status: range ? 206 : 200, headers });
  }
  return { showEpisodes, prepare, close, stream };
}

async function readProbe(body: ReadableStream<Uint8Array>) {
  const reader = body.getReader();
  const bytes = Buffer.alloc(8);
  let offset = 0;
  try {
    while (offset < bytes.length) {
      const { done, value } = await reader.read();
      if (done || value.length > bytes.length - offset) throw new PlaybackError(502);
      bytes.set(value, offset);
      offset += value.length;
    }
    return bytes;
  } finally { await reader.cancel(); }
}

const services = new WeakMap<PlaybackRepo, ReturnType<typeof createPlaybackService>>();
export function playbackService(repo: PlaybackRepo) {
  let service = services.get(repo);
  if (!service) {
    service = createPlaybackService(repo, { viewerPlexToken: (sessionId, machineId) =>
      repo.getViewerPlexTokenForSession(sessionId, machineId) });
    services.set(repo, service);
  }
  return service;
}
