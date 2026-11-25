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
  definitionName: z.string().nullish(),
  fields: z.array(z.object({ name: z.string(), value: z.unknown().optional() })).max(128).nullish()
    .transform(fields => (fields ?? []).filter(field => field.name === "baseUrl")),
  enable: z.boolean(),
  supportsSearch: z.boolean().optional(),
  supportsRss: z.boolean().optional(),
  supportsPagination: z.boolean().optional(),
  protocol: z.enum(["torrent", "usenet"]),
  capabilities: z
    .object({
      categories: z.array(categorySchema).nullable().optional(),
      movieSearchParams: z.array(z.string()).nullish().catch(null),
      tvSearchParams: z.array(z.string()).nullish().catch(null),
      limitsMax: z.number().int().positive().nullish().catch(null),
    })
    .nullable()
    .optional(),
  status: z
    .object({ disabledTill: z.string().nullable().optional() })
    .nullable()
    .optional(),
});
export type DiscoveredIndexer = TorznabSource & {
  availableAfter?: number; supportsPagination?: boolean;
  searchParams?: { movie: string[]; tv: string[] };
  maximumSearchResults?: number;
};
export type ProviderDownloadOrigins = ReadonlyMap<string, ReadonlySet<string>>;
export const prowlarrProviderIdentity = (name: string) => name.trim().toLowerCase();

async function boundedJson(response: Response, signal: AbortSignal): Promise<unknown> {
  if (!response.body) throw new Error("Missing Prowlarr discovery response");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  const abort = () => { void reader.cancel().catch(() => {}); };
  signal.addEventListener("abort", abort, { once: true });
  try {
    signal.throwIfAborted();
    for (;;) {
      const { done, value } = await reader.read();
      signal.throwIfAborted();
      if (done) break;
      size += value.byteLength;
      if (size > 8 * 1024 * 1024) throw new Error("Prowlarr discovery response is too large");
      chunks.push(value);
    }
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } finally {
    signal.removeEventListener("abort", abort);
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

/** Configured categories and temporary health are separate: outages never remove a source. */
export class ProwlarrDiscovery {
  private cached?: { until: number; indexers: z.infer<typeof indexerSchema>[] };
  private inflight?: Promise<z.infer<typeof indexerSchema>[]>;
  constructor(
    private settings: z.infer<typeof prowlarrSettingsSchema>,
    private fetcher: (
      ...args: Parameters<typeof fetch>
    ) => ReturnType<typeof fetch> = fetch,
    private signal?: AbortSignal,
  ) {
    this.settings = { ...settings };
  }
  private requestSignal() {
    const timeout = AbortSignal.timeout(20_000);
    return this.signal ? AbortSignal.any([this.signal, timeout]) : timeout;
  }
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
      const signal = this.requestSignal();
      let response: Response;
      try {
        signal.throwIfAborted();
        response = await this.fetcher(new URL("api/v1/indexer", this.base()), {
          headers: { "X-Api-Key": this.settings.apiKey },
          redirect: "manual",
          signal,
        });
      } catch {
        throw new Error("Prowlarr discovery is unavailable");
      }
      if (!response.ok) {
        await response.body?.cancel().catch(() => {});
        throw new Error(
          `Prowlarr discovery returned HTTP ${response.status}; check its URL base and credentials`,
        );
      }
      const indexers = z.array(indexerSchema).max(1024).parse(await boundedJson(response, signal));
      const statusSignal = this.requestSignal();
      let statusResponse: Response;
      try {
        statusSignal.throwIfAborted();
        statusResponse = await this.fetcher(
          new URL("api/v1/indexerstatus", this.base()),
          {
            headers: { "X-Api-Key": this.settings.apiKey },
            redirect: "manual",
            signal: statusSignal,
          },
        );
      } catch {
        throw new Error("Prowlarr health discovery is unavailable");
      }
      if (!statusResponse.ok) {
        await statusResponse.body?.cancel().catch(() => {});
        throw new Error(
          `Prowlarr health discovery returned HTTP ${statusResponse.status}`,
        );
      }
      const statuses = z
        .array(
          z.object({
            indexerId: z.number(),
            disabledTill: z.string().nullable().optional(),
          }),
        )
        .max(1024)
        .parse(await boundedJson(statusResponse, statusSignal));
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
  /** Only authenticated configuration grants trust, never announce URLs or suggested indexer URLs. */
  async downloadOrigins(): Promise<ProviderDownloadOrigins> {
    const providers = new Map<string, ReadonlySet<string> | null>();
    for (const indexer of await this.indexers()) {
      if (!indexer.enable || indexer.protocol !== "torrent") continue;
      const origins = new Set<string>();
      const value = indexer.fields.length === 1 ? indexer.fields[0]!.value : undefined;
      if (typeof value === "string") {
        try {
          const url = new URL(value);
          if (["https:", "http:"].includes(url.protocol) && !url.username && !url.password && !url.search && !url.hash)
            origins.add(url.origin);
        } catch { /* Invalid provider configuration grants no trust. */ }
      }
      for (const name of new Set([indexer.name, indexer.definitionName ?? ""].map(prowlarrProviderIdentity).filter(Boolean))) {
        providers.set(name, providers.has(name) ? null : origins);
      }
    }
    return new Map([...providers].flatMap(([name, provider]) =>
      provider?.size ? [[name, provider] as const] : []));
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
        searchParams: { movie: i.capabilities?.movieSearchParams ?? [], tv: i.capabilities?.tvSearchParams ?? [] },
        maximumSearchResults: i.capabilities?.limitsMax ?? undefined,
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
