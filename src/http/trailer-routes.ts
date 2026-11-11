import { Hono } from "hono";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import { getCurrentSession, type HttpEnv } from "./middleware";

const id = z.string().regex(/^[1-9]\d{0,15}$/).refine((value) => Number.isSafeInteger(Number(value)));
const plexId = z.union([id, z.number().int().positive().safe().transform(String)]);
const kind = z.enum(["movie", "show"]);
const entry = z.strictObject({ kind, tmdbId: id, parent: id, extra: id }).refine((value) => value.parent !== value.extra);
const machineId = z.string().regex(/^[a-f0-9]{32,64}$/);
const manifest = z.strictObject({ machineIdentifier: machineId, titles: z.array(entry).max(500) });
const maxMediaBytes = 32 * 1024 * 1024;
const maxMetadataBytes = 1024 * 1024;
const maxDuration = 5 * 60 * 1000;
const duration = z.number().int().positive().max(maxDuration);
const parentSchema = z.object({
  ratingKey: plexId, librarySectionID: plexId, type: kind, guid: z.string(),
  Guid: z.array(z.object({ id: z.string() })),
});
const extraSchema = z.object({
  ratingKey: plexId, type: z.literal("clip"), subtype: z.literal("trailer"),
  primaryGuid: z.string(), guid: z.string(), duration,
  Media: z.tuple([z.object({
    container: z.literal("mp4"), videoCodec: z.literal("h264"), audioCodec: z.literal("aac"),
    width: z.number().int().positive().max(1920), height: z.number().int().positive().max(1080),
    Part: z.tuple([z.object({
      id: plexId, key: z.string(), file: z.string().startsWith("/"), container: z.literal("mp4"),
      size: z.number().int().min(8).max(maxMediaBytes),
    })]),
  })]),
});
const listing = z.object({ MediaContainer: z.object({
  size: z.number().int().nonnegative(), Metadata: z.array(z.unknown()).max(100).default([]),
}) });
const ratingKey = z.object({ ratingKey: plexId });

function readManifest(raw: string | undefined) {
  const approved = new Map<string, z.output<typeof entry>>();
  if (!raw || raw.length > 100_000) return null;
  try {
    const { machineIdentifier, titles } = manifest.parse(JSON.parse(raw));
    for (const row of titles) {
      const key = `${row.kind}/${row.tmdbId}`;
      if (approved.has(key)) throw new Error("Duplicate title");
      approved.set(key, row);
    }
    return { machineIdentifier, approved };
  } catch { return null; }
}

async function readMetadata(response: Response): Promise<unknown> {
  if (response.status !== 200 || !response.body || Number(response.headers.get("content-length")) > maxMetadataBytes) {
    await response.body?.cancel();
    throw new Error("Plex metadata unavailable");
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maxMetadataBytes) {
        await reader.cancel();
        throw new Error("Plex metadata too large");
      }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

async function readListing(response: Response) {
  const result = listing.parse(await readMetadata(response)).MediaContainer;
  if (result.size !== result.Metadata.length) throw new Error("Incomplete Plex extras");
  return result.Metadata;
}

function localFile(guid: string, file: string) {
  const url = new URL(guid);
  return guid.startsWith("file:///") && !url.host && !url.search && !url.hash &&
    decodeURIComponent(url.pathname) === file;
}

function matchesMP4(response: Response, size: number, probe: boolean) {
  return response.status === (probe ? 206 : 200) && response.body !== null &&
    response.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase() === "video/mp4" &&
    [null, "identity"].includes(response.headers.get("content-encoding")) &&
    response.headers.get("content-length") === String(probe ? 8 : size) &&
    response.headers.get("content-range") === (probe ? `bytes 0-7/${size}` : null);
}

export function createTrailerRoutes(
  repo: Pick<BooksRepo, "getSettings">,
  rawManifest = process.env.PODIBLE_PLEX_TRAILERS,
  fetcher = fetch,
) {
  const app = new Hono<HttpEnv>();
  const manifest = readManifest(rawManifest);
  app.get("/:kind/:tmdbId/:variant?", async (c) => {
    c.header("Cache-Control", "private, no-store");
    if (!getCurrentSession(c)) return c.body(null, 401);
    const selection = manifest?.approved.get(`${c.req.param("kind")}/${c.req.param("tmdbId")}`);
    const variant = c.req.param("variant");
    if (!manifest || !selection || (variant !== undefined && variant !== "file")) return c.body(null, 404);
    // Hono sends HEAD through the GET handler. Only an actual GET may download the file.
    const download = c.req.raw.method === "GET" && variant === "file";
    try {
      const settings = repo.getSettings();
      const section = selection.kind === "movie" ? settings.movies.plexSectionId : settings.tv.plexSectionId;
      if (!id.safeParse(section).success || !settings.movies.plexToken) return c.body(null, 404);
      const base = new URL(settings.movies.plexUrl);
      if (!["http:", "https:"].includes(base.protocol) || base.username || base.password || base.search || base.hash)
        return c.body(null, 404);
      const signal = AbortSignal.any([c.req.raw.signal, AbortSignal.timeout(download ? 35_000 : 8_000)]);
      const get = (path: string, headers: Record<string, string>) => fetcher(new URL(path, base), {
        headers: { "X-Plex-Token": settings.movies.plexToken, "Accept-Encoding": "identity", ...headers },
        redirect: "error", signal,
      });
      const metadata = { Accept: "application/json" };
      const identity = z.object({ MediaContainer: z.object({ machineIdentifier: machineId }) })
        .parse(await readMetadata(await get("/identity", metadata))).MediaContainer;
      if (identity.machineIdentifier !== manifest.machineIdentifier) return c.body(null, 404);
      const parents = await readListing(await get(
        `/library/metadata/${selection.parent}?includeGuids=1&excludeElements=Media,Role,Director,Writer,Genre,Country,Image`, metadata,
      ));
      const parent = parentSchema.safeParse(parents[0]);
      if (parents.length !== 1 || !parent.success) return c.body(null, 404);
      const tmdb = parent.data.Guid.filter(({ id }) => id.startsWith("tmdb://"));
      if (parent.data.ratingKey !== selection.parent || parent.data.type !== selection.kind ||
          parent.data.librarySectionID !== section || !parent.data.guid.startsWith(`plex://${selection.kind}/`) ||
          !tmdb.length || tmdb.some(({ id }) => id !== `tmdb://${selection.tmdbId}`)) return c.body(null, 404);

      const extras = await readListing(await get(`/library/metadata/${selection.parent}/extras`, metadata));
      const matches = extras.filter((row) => ratingKey.safeParse(row).data?.ratingKey === selection.extra);
      const clip = extraSchema.safeParse(matches[0]);
      if (matches.length !== 1 || !clip.success || clip.data.primaryGuid !== parent.data.guid) return c.body(null, 404);
      const part = clip.data.Media[0].Part[0];
      if (!localFile(clip.data.guid, part.file) || !new RegExp(`^/library/parts/${part.id}/[0-9]+/file\\.mp4$`).test(part.key))
        return c.body(null, 404);

      const response = await get(`${part.key}?download=1`, download ? {} : { Range: "bytes=0-7" });
      if (!response.body || !matchesMP4(response, part.size, !download)) {
        await response.body?.cancel();
        return c.body(null, 502);
      }
      if (!download) {
        const bytes = new Uint8Array(await response.arrayBuffer());
        return bytes.length === 8 && new TextDecoder().decode(bytes.subarray(4)) === "ftyp"
          ? c.json({ available: true }) : c.body(null, 502);
      }
      c.header("Content-Type", "video/mp4");
      c.header("Content-Length", String(part.size));
      // Bun may use chunked framing for a streamed response even when the source size is known.
      c.header("X-Podible-Trailer-Bytes", String(part.size));
      c.header("X-Content-Type-Options", "nosniff");
      return c.body(response.body);
    } catch {
      // Upstream errors can contain server URLs and credentials.
      return c.body(null, 502);
    }
  });
  return app;
}
