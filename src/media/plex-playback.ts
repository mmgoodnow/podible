import { z } from "zod";
import type { AppSettings } from "../app-types";
import type { TvEpisode } from "../tv/types";
import type { PublicationFiles } from "./playback-publication";

export class PlaybackError extends Error {
  constructor(readonly status: 400 | 401 | 403 | 404 | 415 | 429 | 502 | 503) { super("Playback unavailable"); }
}

export function playbackConfig(settings: AppSettings | null, kind: "movie" | "episode") {
  const section = kind === "movie" ? settings?.movies.plexSectionId : settings?.tv.plexSectionId;
  const machineId = settings?.auth.plex.machineId;
  const origin = URL.parse(settings?.movies.plexUrl ?? "");
  if (!section || !/^[1-9]\d{0,15}$/.test(section) || !machineId || !/^[a-f0-9]{32,64}$/.test(machineId) ||
      !origin || !["http:", "https:"].includes(origin.protocol) || origin.username || origin.password ||
      origin.pathname !== "/" || origin.search || origin.hash) throw new PlaybackError(503);
  return { origin: origin.origin, machineId, section };
}

type Config = ReturnType<typeof playbackConfig>;
const plexId = z.union([z.string().regex(/^[1-9]\d{0,15}$/), z.number().int().positive().transform(String)]);
const count = z.union([z.number().int().nonnegative(), z.string().regex(/^\d{1,9}$/).transform(Number)]);
const row = z.object({
  ratingKey: plexId, type: z.string(), guid: z.string().optional(), Guid: z.array(z.object({ id: z.string() })).default([]),
  librarySectionID: plexId.optional(), grandparentRatingKey: plexId.optional(),
  parentIndex: count.optional(), index: count.optional(),
  Media: z.array(z.unknown()).default([]),
});
const listing = z.object({ MediaContainer: z.object({
  size: count, totalSize: count.optional(), offset: count.optional(), librarySectionID: plexId.optional(),
  Metadata: z.array(row).max(100).default([]),
}) });
const rawPart = z.object({ id: plexId, key: z.string(), file: z.string(), size: z.number().int().min(8) });
const partSchema = z.object({
  container: z.literal("mp4"), videoCodec: z.enum(["h264", "hevc"]), audioCodec: z.literal("aac"),
  Part: z.tuple([rawPart.extend({ container: z.literal("mp4").optional() })]),
});
type PlexRow = z.output<typeof row>;
export type PlayablePart = { key: string; size: number; path: string; ratingKey: string; mediaIndex: number; partIndex: number;
  mode: "direct" | "hls" };

export const plexHlsRoot = "/video/:/transcode/universal";
export const plexHlsPlaylistType = "application/vnd.apple.mpegurl";
export const plexHlsSegment = /^\d{1,9}\.ts$/;
const sessionIdPattern = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
const hlsProfile = "add-transcode-target(type=videoProfile&context=streaming&protocol=hls&container=mpegts&videoCodec=h264&audioCodec=aac&replace=true)";
export type PlexHlsScope = { id: string; clientId: string };

const mediaParts = z.object({ Part: z.array(rawPart) });
export function hasPublishedPart(item: PlexRow, files: PublicationFiles) {
  return item.Media.some((media) => mediaParts.safeParse(media).data?.Part.some((part) => files.get(part.file) === part.size));
}

export function playablePart(items: PlexRow[], files: PublicationFiles): PlayablePart | null {
  const parts: PlayablePart[] = [];
  for (const item of items) for (const [mediaIndex, media] of item.Media.entries()) {
    const parsed = mediaParts.safeParse(media);
    if (!parsed.success) continue;
    const supportsDirect = partSchema.safeParse(media).success;
    for (const [partIndex, part] of parsed.data.Part.entries()) {
      if (files.get(part.file) !== part.size ||
          !new RegExp(`^/library/parts/${part.id}/[0-9]+/[a-z0-9_.-]+\\.(mp4|mkv|m4v|avi|mov|ts)$`, "i").test(part.key)) continue;
      parts.push({ key: part.key, size: part.size, path: part.file, ratingKey: item.ratingKey, mediaIndex, partIndex,
        mode: supportsDirect && /\.mp4$/i.test(part.key) ? "direct" : "hls" });
    }
  }
  const direct = parts.filter(({ mode }) => mode === "direct");
  // Preserve direct play, but never ask Plex to break a tie or substitute a different edition.
  return direct.length === 1 ? direct[0]! : parts.length === 1 ? parts[0]! : null;
}

export function matchesPlexIdentity(item: PlexRow, expected: Record<string, string>): boolean {
  const guids = [...item.Guid.map(({ id }) => id), item.guid].filter(Boolean);
  let matched = false;
  for (const [provider, id] of Object.entries(expected)) {
    const known = guids.filter((guid) => guid?.startsWith(`${provider}://`));
    if (!known.length) continue;
    if (known.some((guid) => guid !== `${provider}://${id}`)) return false;
    matched = true;
  }
  return matched;
}

export function episodeIdentity(episode: TvEpisode): Record<string, string> {
  if (episode.identity.stability !== "provider") return {};
  const ids = new Map<string, Set<string>>();
  for (const ref of episode.refs) {
    if (!["tmdb", "tvdb"].includes(ref.provider) || !/^[1-9]\d*$/.test(ref.id)) continue;
    const existing = ids.get(ref.provider) ?? new Set<string>();
    existing.add(ref.id);
    ids.set(ref.provider, existing);
  }
  const key = /^(tmdb|tvdb):([1-9]\d*)$/.exec(episode.key);
  if (key) {
    const existing = ids.get(key[1]!) ?? new Set<string>();
    existing.add(key[2]!);
    ids.set(key[1]!, existing);
  }
  if ([...ids.values()].some((set) => set.size !== 1)) return {};
  return Object.fromEntries([...ids].map(([provider, set]) => [provider, [...set][0]!]));
}

async function readPlexJSON(response: Response): Promise<unknown> {
  const max = 4 * 1024 * 1024;
  if (response.status !== 200 || !response.body || Number(response.headers.get("content-length")) > max) {
    await response.body?.cancel();
    throw new PlaybackError(response.status === 401 || response.status === 403 ? 403 : response.status === 404 ? 404 : 502);
  }
  const reader = response.body.getReader();
  let size = 0;
  const chunks: Uint8Array[] = [];
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > max) { await reader.cancel(); throw new PlaybackError(502); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

export class PlexPlayback {
  constructor(private config: Config, private token: string, private fetcher: typeof fetch, private signal: AbortSignal) {}

  private async json(path: string, authenticated = true) {
    return readPlexJSON(await this.fetcher(new URL(path, this.config.origin), {
      headers: { Accept: "application/json", "Accept-Encoding": "identity", ...(authenticated ? { "X-Plex-Token": this.token } : {}) },
      redirect: "error", signal: AbortSignal.any([this.signal, AbortSignal.timeout(8_000)]),
    }));
  }

  async verifyServer() {
    const identity = z.object({ MediaContainer: z.object({ claimed: z.literal(true), machineIdentifier: z.string() }) })
      .parse(await this.json("/identity", false)).MediaContainer;
    if (identity.machineIdentifier !== this.config.machineId) throw new PlaybackError(503);
  }

  private async rows(path: string, type: string) {
    const result = new Map<string, PlexRow>();
    const size = 100;
    for (let offset = 0; offset < 5_000; offset += size) {
      const separator = path.includes("?") ? "&" : "?";
      const container = listing.parse(await this.json(
        `${path}${separator}includeGuids=1&X-Plex-Container-Start=${offset}&X-Plex-Container-Size=${size}`,
      )).MediaContainer;
      if (container.size !== container.Metadata.length || (container.offset !== undefined && container.offset !== offset) ||
          (container.librarySectionID && container.librarySectionID !== this.config.section)) throw new PlaybackError(502);
      for (const item of container.Metadata) {
        if (item.type !== type || (item.librarySectionID && item.librarySectionID !== this.config.section) || result.has(item.ratingKey))
          throw new PlaybackError(502);
        result.set(item.ratingKey, item);
      }
      if (container.totalSize !== undefined ? offset + container.size >= container.totalSize : container.size < size) return [...result.values()];
      if (container.size !== size) throw new PlaybackError(502);
    }
    throw new PlaybackError(503);
  }

  private async findTitles(kind: "movie" | "show", expected: Record<string, string>) {
    return (await this.rows(`/library/sections/${this.config.section}/all?type=${kind === "movie" ? 1 : 2}&excludeElements=Media`, kind))
      .filter((item) => matchesPlexIdentity(item, expected));
  }

  async moviePart(expected: Record<string, string>, files: PublicationFiles) {
    const matches = await this.findTitles("movie", expected);
    if (matches.length > 12) throw new PlaybackError(503);
    const managed: PlexRow[] = [];
    for (const match of matches) {
      const items = await this.rows(`/library/metadata/${match.ratingKey}`, "movie");
      const movie = items[0];
      if (items.length !== 1 || !movie || movie.ratingKey !== match.ratingKey || !matchesPlexIdentity(movie, expected)) throw new PlaybackError(404);
      if (hasPublishedPart(movie, files)) managed.push(movie);
    }
    if (!managed.length) throw new PlaybackError(404);
    return playablePart(managed, files);
  }

  async episodeRows(expected: Record<string, string>) {
    const shows = await this.findTitles("show", expected);
    if (shows.length > 4) throw new PlaybackError(503);
    if (!shows.length) throw new PlaybackError(404);
    const episodes: PlexRow[] = [];
    for (const show of shows) episodes.push(...(await this.rows(`/library/metadata/${show.ratingKey}/allLeaves`, "episode"))
      .filter((item) => item.grandparentRatingKey === show.ratingKey));
    return episodes;
  }

  async confirmEpisodePart(part: PlayablePart, parent: string, identity: Record<string, string>, files: PublicationFiles) {
    const rows = await this.rows(`/library/metadata/${part.ratingKey}`, "episode");
    const item = rows[0];
    if (rows.length !== 1 || !item || item.ratingKey !== part.ratingKey || item.grandparentRatingKey !== parent ||
        !matchesPlexIdentity(item, identity)) throw new PlaybackError(404);
    const confirmed = playablePart(rows, files);
    if (!confirmed || confirmed.key !== part.key || confirmed.path !== part.path || confirmed.size !== part.size) throw new PlaybackError(404);
    return confirmed;
  }

  private hlsHeaders(scope: PlexHlsScope) {
    if (!sessionIdPattern.test(scope.id)) throw new PlaybackError(502);
    return { "X-Plex-Client-Identifier": scope.clientId, "X-Plex-Session-Identifier": scope.id };
  }

  startHls(part: PlayablePart, scope: PlexHlsScope) {
    if (!plexId.safeParse(part.ratingKey).success || !Number.isSafeInteger(part.mediaIndex) || part.mediaIndex < 0 ||
        !Number.isSafeInteger(part.partIndex) || part.partIndex < 0) throw new PlaybackError(502);
    const query = new URLSearchParams({ path: `/library/metadata/${part.ratingKey}`, mediaIndex: String(part.mediaIndex),
      partIndex: String(part.partIndex), protocol: "hls", directPlay: "0", directStream: "1", directStreamAudio: "1",
      transcodeSessionId: scope.id, session: scope.id, secondsPerSegment: "4", maxVideoBitrate: "12000", autoAdjustQuality: "0" });
    return this.mediaRequest(`${plexHlsRoot}/start.m3u8?${query}`, {
      ...this.hlsHeaders(scope), Accept: plexHlsPlaylistType, "X-Plex-Client-Profile-Name": "Generic", "X-Plex-Client-Profile-Extra": hlsProfile,
    });
  }

  hlsResource(scope: PlexHlsScope, name: string) {
    if (name !== "index.m3u8" && !plexHlsSegment.test(name)) throw new PlaybackError(404);
    return this.mediaRequest(`${plexHlsRoot}/session/${scope.id}/base/${name}`, this.hlsHeaders(scope), this.signal, 15_000);
  }

  async stopHls(scope: PlexHlsScope) {
    const query = new URLSearchParams({ session: scope.id, transcodeSessionId: scope.id });
    const signal = AbortSignal.timeout(2_000);
    const response = await this.mediaRequest(`${plexHlsRoot}/stop?${query}`, this.hlsHeaders(scope), signal);
    await response.body?.cancel();
  }

  file(part: PlayablePart, range: string | null) {
    return this.mediaRequest(part.key, range ? { Range: range } : {});
  }

  private async mediaRequest(path: string, headers: Record<string, string>, parentSignal = this.signal, timeoutMs = 8_000) {
    const signal = new AbortController();
    const timeout = setTimeout(() => signal.abort(), timeoutMs);
    try {
      return await this.fetcher(new URL(path, this.config.origin), {
        headers: { "X-Plex-Token": this.token, "Accept-Encoding": "identity", ...headers },
        redirect: "error", signal: AbortSignal.any([parentSignal, signal.signal]),
      });
    } finally { clearTimeout(timeout); }
  }
}
