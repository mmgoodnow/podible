import { z } from "zod";
import type { TorznabSource } from "../app-types";
export const prowlarrSettingsSchema = z
  .object({ url: z.string().default(""), apiKey: z.string().default("") })
  .strict();
type Category = { id: number; subCategories?: Category[] | null };
const categorySchema: z.ZodType<Category> = z.lazy(() =>
  z.object({
    id: z.number(),
    subCategories: z.array(categorySchema).nullable().optional(),
  }),
);
const indexerSchema = z.object({
  id: z.number().int().positive(),
  name: z.string(),
  enable: z.boolean(),
  supportsSearch: z.boolean().optional(),
  supportsRss: z.boolean().optional(),
  supportsPagination: z.boolean().optional(),
  protocol: z.enum(["torrent", "usenet"]),
  capabilities: z
    .object({ categories: z.array(categorySchema).nullable().optional() })
    .nullable()
    .optional(),
  status: z
    .object({ disabledTill: z.string().nullable().optional() })
    .nullable()
    .optional(),
});
export type DiscoveredIndexer = TorznabSource & { availableAfter?: number; supportsPagination?: boolean };

/** Configured categories and temporary health are separate: outages never remove a source. */
export class ProwlarrDiscovery {
  private cached?: { until: number; indexers: z.infer<typeof indexerSchema>[] };
  private inflight?: Promise<z.infer<typeof indexerSchema>[]>;
  constructor(
    private settings: z.infer<typeof prowlarrSettingsSchema>,
    private fetcher: (
      ...args: Parameters<typeof fetch>
    ) => ReturnType<typeof fetch> = fetch,
  ) {}
  private base() {
    const base = new URL(this.settings.url.replace(/\/+$/, "") + "/");
    if (
      !["http:", "https:"].includes(base.protocol) ||
      base.username ||
      base.password ||
      base.search ||
      base.hash
    )
      throw new Error("Invalid Prowlarr URL");
    if (!this.settings.apiKey)
      throw new Error("Configure the Prowlarr API key");
    return base;
  }
  private async indexers() {
    if (this.cached && this.cached.until > Date.now())
      return this.cached.indexers;
    if (this.inflight) return this.inflight;
    this.inflight = (async () => {
      let response: Response;
      try {
        response = await this.fetcher(new URL("api/v1/indexer", this.base()), {
          headers: { "X-Api-Key": this.settings.apiKey },
          redirect: "manual",
          signal: AbortSignal.timeout(20_000),
        });
      } catch {
        throw new Error("Prowlarr discovery is unavailable");
      }
      if (!response.ok)
        throw new Error(
          `Prowlarr discovery returned HTTP ${response.status}; check its URL base and credentials`,
        );
      const indexers = z.array(indexerSchema).parse(await response.json());
      let statusResponse: Response;
      try {
        statusResponse = await this.fetcher(
          new URL("api/v1/indexerstatus", this.base()),
          {
            headers: { "X-Api-Key": this.settings.apiKey },
            redirect: "manual",
            signal: AbortSignal.timeout(20_000),
          },
        );
      } catch {
        throw new Error("Prowlarr health discovery is unavailable");
      }
      if (!statusResponse.ok)
        throw new Error(
          `Prowlarr health discovery returned HTTP ${statusResponse.status}`,
        );
      const statuses = z
        .array(
          z.object({
            indexerId: z.number(),
            disabledTill: z.string().nullable().optional(),
          }),
        )
        .parse(await statusResponse.json());
      for (const indexer of indexers)
        indexer.status =
          statuses.find((s) => s.indexerId === indexer.id) ?? indexer.status;
      this.cached = { until: Date.now() + 60_000, indexers };
      return indexers;
    })();
    try {
      return await this.inflight;
    } finally {
      this.inflight = undefined;
    }
  }
  async sources(
    kind: "movie" | "tv" | "audio" | "ebook",
    mode: "search" | "rss" = "search",
  ): Promise<DiscoveredIndexer[]> {
    const wanted =
      kind === "movie" ? [2000] : kind === "tv" ? [5000] : kind === "audio" ? [3030] : [7020];
    const supports = (categories: Category[]): boolean =>
      categories.some(
        (c) =>
          wanted.some(
            (id) => c.id === id || ([2000, 5000].includes(id) && c.id >= id && c.id < id + 1000),
          ) || supports(c.subCategories ?? []),
      );
    return (await this.indexers())
      .filter(
        (i) =>
          i.enable &&
          (mode === "rss" ? i.supportsRss !== false : i.supportsSearch !== false) &&
          i.protocol === "torrent" &&
          (!i.capabilities?.categories?.length ||
            supports(i.capabilities.categories)),
      )
      .map((i) => ({
        name: i.name,
        baseUrl: new URL(`${i.id}/api`, this.base()).href,
        apiKey: this.settings.apiKey,
        supportsPagination: i.supportsPagination,
        // Missing capabilities may be a provider outage, not proof of no category support.
        availableAfter:
          Math.max(
            i.status?.disabledTill ? Date.parse(i.status.disabledTill) || 0 : 0,
            i.capabilities?.categories?.length ? 0 : Date.now() + 60_000,
          ) || undefined,
        categories: { audio: "3030", ebook: "7020" },
      }));
  }
}
