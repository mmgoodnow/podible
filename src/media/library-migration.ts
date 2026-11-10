import { z } from "zod";
import type { AppSettings } from "../app-types";

export const migrationSourceSchema = z.enum(["radarr", "sonarr"]);
const count = z.number().int().nonnegative().safe();
const externalId = z.number().int().positive().safe();
export const migrationPreviewSchema = z.object({
  source: migrationSourceSchema,
  observedAt: z.string(),
  total: count,
  monitored: count,
  withFiles: count,
  withoutIdentity: count,
  offset: count,
  nextOffset: count.nullable(),
  items: z.array(z.object({
    sourceId: externalId,
    title: z.string(),
    year: z.number().int().nullable(),
    catalogId: externalId.nullable(),
    catalog: z.enum(["tmdb", "tvdb"]),
    monitored: z.boolean().nullable(),
    filesReported: z.boolean().nullable(),
    episodeFiles: count.nullable(),
  })),
});
export type MigrationPreview = z.infer<typeof migrationPreviewSchema>;
export const migrationPreviewParams = z.object({
  source: migrationSourceSchema,
  offset: count.default(0),
  limit: z.number().int().min(1).max(100).default(50),
}).strict();
const upstreamRow = z.object({
  id: externalId, title: z.string(), year: z.number().int().optional(),
  tmdbId: z.number().int().optional(), tvdbId: z.number().int().optional(),
  monitored: z.boolean().optional(), hasFile: z.boolean().optional(),
  statistics: z.object({ episodeFileCount: count.optional() }).optional(),
});

/** Inventory only: no filesystem, command, import, request or torrent capabilities. */
export async function previewLibraryMigration(
  settings: AppSettings,
  input: z.input<typeof migrationPreviewParams>,
  options: { fetcher?: typeof fetch; now?: () => number; label?: (value: string) => string } = {},
): Promise<MigrationPreview> {
  const { source, offset, limit } = migrationPreviewParams.parse(input);
  const url = source === "radarr" ? settings.movies.shadow.radarrUrl : settings.tv.sonarrUrl;
  const key = source === "radarr" ? settings.movies.shadow.radarrApiKey : settings.tv.sonarrApiKey;
  let base: URL;
  try {
    base = new URL(url.replace(/\/+$/, "") + "/");
    if (!key.trim() || !["https:", "http:"].includes(base.protocol) || base.username || base.password || base.hash || base.search) throw new Error();
  } catch { throw new Error(`Configure ${source === "radarr" ? "movies.shadow.radarrUrl and radarrApiKey" : "tv.sonarrUrl and sonarrApiKey"} first.`); }
  let rows: z.infer<typeof upstreamRow>[];
  try {
    const response = await (options.fetcher ?? fetch)(new URL(`api/v3/${source === "radarr" ? "movie" : "series"}`, base), {
      method: "GET", redirect: "manual", headers: { "X-Api-Key": key }, signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok || !response.body) {
      await response.body?.cancel();
      throw new Error();
    }
    const reader = response.body.getReader();
    const chunks: Uint8Array[] = [];
    let size = 0;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > 16 * 1024 * 1024) throw new Error();
        chunks.push(value);
      }
    } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
    const bytes = new Uint8Array(size);
    let position = 0;
    for (const chunk of chunks) { bytes.set(chunk, position); position += chunk.byteLength; }
    rows = z.array(upstreamRow).max(25_000).parse(JSON.parse(new TextDecoder().decode(bytes)));
  } catch {
    // Never include upstream payloads, paths, URLs or credentials in the API error.
    throw new Error(`${source === "radarr" ? "Radarr" : "Sonarr"} inventory could not be read. Check its connection and API key, then try again. Nothing was changed.`);
  }
  const label = options.label ?? ((value: string) => value.replace(/https?:\/\/\S+/gi, "[redacted URL]").slice(0, 300));
  const items = rows.map((row): MigrationPreview["items"][number] => ({
    sourceId: row.id, title: label(row.title), year: row.year && row.year > 0 ? row.year : null,
    catalogId: externalId.safeParse(source === "radarr" ? row.tmdbId : row.tvdbId).data ?? null,
    catalog: source === "radarr" ? "tmdb" : "tvdb", monitored: row.monitored ?? null,
    filesReported: source === "radarr" ? row.hasFile ?? null
      : row.statistics?.episodeFileCount === undefined ? null : row.statistics.episodeFileCount > 0,
    episodeFiles: source === "sonarr" ? row.statistics?.episodeFileCount ?? null : null,
  })).sort((a, b) => a.title.localeCompare(b.title) || a.sourceId - b.sourceId);
  return { source, observedAt: new Date((options.now ?? Date.now)()).toISOString(), total: items.length,
    monitored: items.filter(row => row.monitored === true).length,
    withFiles: items.filter(row => row.filesReported === true).length,
    withoutIdentity: items.filter(row => row.catalogId === null).length,
    offset, nextOffset: offset + limit < items.length ? offset + limit : null, items: items.slice(offset, offset + limit) };
}
