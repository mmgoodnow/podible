import { z } from "zod";
import type { TvEntry } from "./types";
import type { TvLibrary, TvPublishedFile } from "./publication";
import { tvdbAiredNumber } from "./publication";

const itemSchema = z.object({ ratingKey: z.string(), parentIndex: z.number().optional(), index: z.number().optional(),
  Guid: z.array(z.object({ id: z.string() })).default([]),
  Media: z.array(z.object({ Part: z.array(z.object({ file: z.string() })).default([]) })).default([]) });
const listingSchema = z.object({ MediaContainer: z.object({ totalSize: z.number().optional(), Metadata: z.array(itemSchema).default([]) }) });
type PlexItem = z.infer<typeof itemSchema>;

export class PlexTvLibrary implements TvLibrary {
  private lastScan = 0;
  constructor(private settings: { url: string; token: string; sectionId: string }, private fetcher: typeof fetch = fetch) {
    const url = new URL(settings.url);
    if (!/^\d+$/.test(settings.sectionId) || !["http:", "https:"].includes(url.protocol) || url.username || url.password)
      throw new Error("Invalid Plex TV connection");
  }
  private async get(route: string, signal = AbortSignal.timeout(20_000)) {
    const response = await this.fetcher(new URL(route, this.settings.url), { headers: { Accept: "application/json", "X-Plex-Token": this.settings.token },
      redirect: "error", signal });
    if (!response.ok) throw new Error(`Plex TV returned HTTP ${response.status}`);
    return response;
  }
  async scan() {
    const schema = z.object({ MediaContainer: z.object({ Directory: z.array(z.object({ key: z.string(), type: z.string(), refreshing: z.boolean().optional() })).default([]) }) });
    const sections = schema.parse(await (await this.get("/library/sections")).json()).MediaContainer.Directory;
    const section = sections.find((item) => item.key === this.settings.sectionId);
    if (!section || section.type !== "show") throw new Error("Configure a dedicated Plex TV section");
    if (section.refreshing || Date.now() - this.lastScan < 10_000) return;
    await this.get(`/library/sections/${this.settings.sectionId}/refresh`); this.lastScan = Date.now();
  }
  async verify(entry: TvEntry, files: TvPublishedFile[]) {
    if (!entry.tvdbId || !files.length) return false;
    const shows = [];
    for (let start = 0; start < 100_000; start += 200) {
      const result = listingSchema.parse(await (await this.get(`/library/sections/${this.settings.sectionId}/all?type=2&includeGuids=1&X-Plex-Container-Start=${start}&X-Plex-Container-Size=200`)).json()).MediaContainer;
      shows.push(...result.Metadata.filter((show) => show.Guid.some((guid) => guid.id === `tvdb://${entry.tvdbId}`)));
      if (result.Metadata.length < 200 || (result.totalSize !== undefined && start + 200 >= result.totalSize)) break;
      if (start === 99_800) throw new Error("Plex TV listing exceeded its verification budget");
    }
    const expected = files.flatMap((file) => file.episodes.map((episode) => ({ path: file.serverPath, contentEpisode: episode, ...tvdbAiredNumber(episode) })));
    const byPath = new Map<string, { index: number; season: number; episode: number; tvdbId?: string }[]>();
    for (const [index, target] of expected.entries()) {
      const tvdbId = target.contentEpisode.refs.find(ref => ref.provider === "tvdb")?.id;
      const targets = byPath.get(target.path) ?? [];
      targets.push({ index, season: target.season, episode: target.episode, tvdbId });
      byPath.set(target.path, targets);
    }
    const found = new Set<number>();
    const verifyItems = (items: PlexItem[]) => {
      for (const item of items) for (const media of item.Media) for (const part of media.Part) {
        for (const target of byPath.get(part.file) ?? []) {
          // The season endpoint narrows candidates; it never establishes identity.
          const identity = target.tvdbId ? item.Guid.some(guid => guid.id === `tvdb://${target.tvdbId}`)
            : item.parentIndex === target.season && item.index === target.episode;
          if (identity) found.add(target.index);
        }
      }
      return found.size === expected.length;
    };
    const seasons = new Set(expected.map(target => target.season));
    for (const show of shows) {
      const root = `/library/metadata/${encodeURIComponent(show.ratingKey)}`;
      if (entry.episodes.length > 200 && seasons.size <= 3) {
        try {
          const signal = AbortSignal.timeout(2_000);
          const page = "includeGuids=1&X-Plex-Container-Start=0&X-Plex-Container-Size=200";
          const { Metadata } = listingSchema.parse(await (await this.get(`${root}/children?${page}`, signal)).json()).MediaContainer;
          const candidates = Metadata.filter(item => item.index !== undefined && seasons.has(item.index));
          if (candidates.length <= seasons.size) {
            for (const season of candidates) {
              const items = listingSchema.parse(await (await this.get(
                `/library/metadata/${encodeURIComponent(season.ratingKey)}/children?${page}`, signal)).json()).MediaContainer;
              if (verifyItems(items.Metadata)) return true;
            }
          }
        } catch { /* Fall through to the existing complete listing on unsupported endpoints. */ }
      }
      for (let start = 0; start < 100_000; start += 200) {
        const result = listingSchema.parse(await (await this.get(`${root}/allLeaves?includeGuids=1&X-Plex-Container-Start=${start}&X-Plex-Container-Size=200`)).json()).MediaContainer;
        if (verifyItems(result.Metadata)) return true;
        if (result.Metadata.length < 200 || (result.totalSize !== undefined && start + 200 >= result.totalSize)) break;
        if (start === 99_800) throw new Error("Plex episodes exceeded their verification budget");
      }
    }
    return found.size === expected.length;
  }
}
