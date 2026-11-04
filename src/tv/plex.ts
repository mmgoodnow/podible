import { z } from "zod";
import type { TvEntry } from "./types";
import type { TvLibrary, TvPublishedFile } from "./publication";
import { tvdbAiredNumber } from "./publication";

const itemSchema = z.object({ ratingKey: z.string(), parentIndex: z.number().optional(), index: z.number().optional(),
  Guid: z.array(z.object({ id: z.string() })).default([]),
  Media: z.array(z.object({ Part: z.array(z.object({ file: z.string() })).default([]) })).default([]) });
const listingSchema = z.object({ MediaContainer: z.object({ totalSize: z.number().optional(), Metadata: z.array(itemSchema).default([]) }) });
export class PlexTvLibrary implements TvLibrary {
  private lastScan = 0;
  constructor(private settings: { url: string; token: string; sectionId: string }, private fetcher: typeof fetch = fetch) {
    const url = new URL(settings.url);
    if (!/^\d+$/.test(settings.sectionId) || !["http:", "https:"].includes(url.protocol) || url.username || url.password)
      throw new Error("Invalid Plex TV connection");
  }
  private async get(route: string) {
    const response = await this.fetcher(new URL(route, this.settings.url), { headers: { Accept: "application/json", "X-Plex-Token": this.settings.token },
      redirect: "error", signal: AbortSignal.timeout(20_000) });
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
    const found = new Set<number>();
    for (const show of shows) for (let start = 0; start < 100_000; start += 200) {
      const result = listingSchema.parse(await (await this.get(`/library/metadata/${encodeURIComponent(show.ratingKey)}/allLeaves?includeGuids=1&X-Plex-Container-Start=${start}&X-Plex-Container-Size=200`)).json()).MediaContainer;
      for (const item of result.Metadata) for (const [index, target] of expected.entries()) {
        const tvdbEpisodeId = target.contentEpisode.refs.find((ref) => ref.provider === "tvdb")?.id;
        // Exact episode IDs defeat false success caused by a differently numbered provider order.
        const identity = tvdbEpisodeId ? item.Guid.some((guid) => guid.id === `tvdb://${tvdbEpisodeId}`)
          : item.parentIndex === target.season && item.index === target.episode;
        if (identity && item.Media.some((media) => media.Part.some((part) => part.file === target.path))) found.add(index);
      }
      if (result.Metadata.length < 200 || (result.totalSize !== undefined && start + 200 >= result.totalSize)) break;
      if (start === 99_800) throw new Error("Plex episodes exceeded their verification budget");
    }
    return found.size === expected.length;
  }
}
