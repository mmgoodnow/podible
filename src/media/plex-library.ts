import { z } from "zod";
import type { MovieLibrary } from "../movies/publication";
import type { Movie } from "../movies/types";

const listingSchema = z.object({
  MediaContainer: z.object({
    totalSize: z.number().optional(),
    size: z.number().optional(),
    Metadata: z
      .array(
        z.object({
          Guid: z.array(z.object({ id: z.string() })).default([]),
          Media: z
            .array(
              z.object({
                Part: z.array(z.object({ file: z.string() })).default([]),
              }),
            )
            .default([]),
        }),
      )
      .default([]),
  }),
});
export class PlexMovieLibrary implements MovieLibrary {
  private lastScan = 0;
  constructor(
    private settings: { url: string; token: string; sectionId: string },
  ) {
    const url = new URL(settings.url);
    if (
      !["http:", "https:"].includes(url.protocol) ||
      url.username ||
      url.password ||
      !/^\d+$/.test(settings.sectionId)
    )
      throw new Error("Invalid Plex library configuration");
  }
  private async request(route: string) {
    const response = await fetch(new URL(route, this.settings.url), {
      headers: {
        Accept: "application/json",
        "X-Plex-Token": this.settings.token,
      },
      signal: AbortSignal.timeout(15_000),
      redirect: "error",
    });
    if (!response.ok) throw new Error(`Plex returned ${response.status}`);
    return response;
  }
  async scan(): Promise<void> {
    const sectionSchema = z.object({
      MediaContainer: z.object({
        Directory: z
          .array(
            z.object({ key: z.string(), refreshing: z.boolean().optional() }),
          )
          .default([]),
      }),
    });
    const sections = sectionSchema.parse(
      await (await this.request("/library/sections")).json(),
    ).MediaContainer.Directory;
    const section = sections.find(
      (entry) => entry.key === this.settings.sectionId,
    );
    if (!section)
      throw new Error("Configured Plex movie library does not exist");
    // Indexing and background analysis complete independently. Never block
    // verify() on an unrelated thumbnail/codec job, or restart a live scan.
    if (section.refreshing) return;
    // Plex can return 200 for a path-scoped refresh without discovering new
    // title folders. Coalesce section scans; readiness still requires verify().
    if (Date.now() - this.lastScan < 10_000) return;
    await this.request(`/library/sections/${this.settings.sectionId}/refresh`);
    this.lastScan = Date.now();
  }
  async verify(movie: Movie, serverVisiblePaths: string[]): Promise<boolean> {
    const expected = new Set(serverVisiblePaths);
    const found = new Set<string>();
    for (let start = 0; start < 100_000; start += 200) {
      const listing = listingSchema.parse(
        await (
          await this.request(
            `/library/sections/${this.settings.sectionId}/all?includeGuids=1&X-Plex-Container-Start=${start}&X-Plex-Container-Size=200`,
          )
        ).json(),
      ).MediaContainer;
      for (const item of listing.Metadata) {
        if (
          !item.Guid.some(
            (guid) =>
              guid.id === `tmdb://${movie.tmdbId}` ||
              (movie.imdbId !== null && guid.id === `imdb://${movie.imdbId}`),
          )
        )
          continue;
        for (const media of item.Media)
          for (const part of media.Part)
            if (expected.has(part.file)) found.add(part.file);
      }
      if (found.size === expected.size && expected.size > 0) return true;
      if (
        listing.Metadata.length < 200 ||
        (listing.totalSize !== undefined && start + 200 >= listing.totalSize)
      )
        return false;
    }
    throw new Error(
      "Plex library listing exceeded bounded verification budget",
    );
  }
}
