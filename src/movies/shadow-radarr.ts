import { createHash } from "node:crypto";
import { z } from "zod";
import type { AppSettings } from "../app-types";
import type { ShadowObservation } from "./shadow-types";

const movieSchema = z.object({
  id: z.number(),
  tmdbId: z.number().positive(),
  title: z.string(),
  originalTitle: z.string().optional(),
  year: z.number().nullable().optional(),
  overview: z.string().optional(),
  originalLanguage: z.object({ name: z.string() }).optional(),
  imdbId: z.string().nullable().optional(),
  runtime: z.number().optional(),
});
const recordSchema = z.object({
  id: z.number(),
  movieId: z.number().nullable().optional(),
  movie: movieSchema.nullable().optional(),
  title: z.string().optional(),
  sourceTitle: z.string().optional(),
  eventType: z.string().optional(),
  date: z.string().optional(),
  sizeleft: z.number().optional(),
  outputPath: z.string().optional(),
  trackedDownloadState: z.string().optional(),
  errorMessage: z.string().nullable().optional(),
  statusMessages: z
    .array(
      z.object({
        title: z.string().optional(),
        messages: z.array(z.string()).optional(),
      }),
    )
    .optional(),
  data: z
    .object({
      droppedPath: z.string().optional(),
      importedPath: z.string().optional(),
    })
    .optional(),
});
const pageSchema = z.object({
  records: z.array(recordSchema),
  totalRecords: z.number(),
});
const clean = (text: string) =>
  text.replace(/https?:\/\/\S+/g, "[redacted URL]").slice(0, 2000);

/** GET-only observation; no Radarr command or torrent-client capability. */
export class RadarrObserver {
  constructor(
    private settings: AppSettings["movies"]["shadow"],
    private fetcher: typeof fetch = fetch,
  ) {}
  async observe(): Promise<ShadowObservation[]> {
    const base = new URL(this.settings.radarrUrl.replace(/\/$/, "") + "/");
    if (
      !["http:", "https:"].includes(base.protocol) ||
      base.username ||
      base.password ||
      base.search ||
      base.hash
    )
      throw new Error(
        "Radarr URL must be HTTP(S), without embedded credentials or query parameters",
      );
    if (!this.settings.radarrApiKey)
      throw new Error("Configure the Radarr API key for shadow mode");
    const instance = createHash("sha256")
      .update(base.href)
      .digest("hex")
      .slice(0, 16);
    const get = async (resource: string) => {
      let response: Response;
      try {
        response = await this.fetcher(new URL(`api/v3/${resource}`, base), {
          method: "GET",
          redirect: "manual",
          headers: { "X-Api-Key": this.settings.radarrApiKey },
          signal: AbortSignal.timeout(30_000),
        });
      } catch {
        throw new Error("Radarr observation request failed");
      }
      if (response.status >= 300 && response.status < 400)
        throw new Error(
          "Radarr redirects this API URL; include its configured URL base (for example /radarr) in movies.shadow.radarrUrl",
        );
      if (!response.ok)
        throw new Error(`Radarr observation returned HTTP ${response.status}`);
      return response.json();
    };
    const observations: ShadowObservation[] = [];
    const cutoff = Date.now() - this.settings.lookbackDays * 86_400_000;
    for (const kind of ["queue", "history"] as const) {
      for (let page = 1; page <= 10; page++) {
        const result = pageSchema.parse(
          await get(
            `${kind}?page=${page}&pageSize=100&includeMovie=true&sortKey=${kind === "history" ? "date" : "id"}&sortDirection=descending`,
          ),
        );
        for (const row of result.records) {
          const imported =
            kind === "history" && row.eventType === "downloadFolderImported";
          const blocked =
            kind === "queue" &&
            (row.sizeleft === 0 ||
              row.trackedDownloadState === "importBlocked" ||
              row.trackedDownloadState === "importPending");
          if (!imported && !blocked) continue;
          if (
            kind === "history" &&
            (!row.date || Date.parse(row.date) < cutoff)
          )
            continue;
          const movie =
            row.movie ??
            (row.movieId
              ? movieSchema.parse(await get(`movie/${row.movieId}`))
              : null);
          if (!movie) continue;
          const sourcePaths = imported
            ? [row.data?.droppedPath, row.data?.importedPath]
            : [row.outputPath];
          observations.push({
            key: `${instance}:${kind}:${row.id}`,
            movie: {
              tmdbId: movie.tmdbId,
              title: movie.title,
              originalTitle: movie.originalTitle ?? movie.title,
              year: movie.year ?? null,
              overview: movie.overview ?? "",
              originalLanguage: movie.originalLanguage?.name ?? "und",
              posterUrl: null,
              imdbId: movie.imdbId ?? null,
              runtimeMinutes: movie.runtime || null,
            },
            releaseTitle: row.sourceTitle ?? row.title ?? movie.title,
            sourcePaths: sourcePaths.filter((p): p is string => Boolean(p)),
            observedAt: new Date().toISOString(),
            radarr: {
              outcome: imported ? "imported" : "blocked",
              messages: [
                row.errorMessage ?? "",
                ...(row.statusMessages ?? []).flatMap((s) => [
                  s.title ?? "",
                  ...(s.messages ?? []),
                ]),
              ]
                .filter(Boolean)
                .map(clean),
            },
          });
        }
        if (
          page * 100 >= result.totalRecords ||
          !result.records.length ||
          (kind === "history" &&
            result.records.every((r) => r.date && Date.parse(r.date) < cutoff))
        )
          break;
      }
    }
    return observations;
  }
}
