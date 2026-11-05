import { createHash } from "node:crypto";
import { z } from "zod";
import type { JobRow } from "../app-types";
import { durableStep, type WorkerContext } from "../worker/context";
import { createMovieFeedMatcher } from "../movies/agents";
import { createTvFeedMatcher } from "../tv/feed-agent";
import { movieOfferFromResult } from "../movies/offers";
import { queueCrossSeed } from "./cross-seed";
import type { CrossSeedScope } from "./cross-seed-state";

const payload = z.object({ releaseIds: z.array(z.number().int().positive()).min(1).max(100) }).strict();
const operationId = (value: string) => {
  const hash = createHash("sha256").update(value).digest("hex");
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-4${hash.slice(13, 16)}-8${hash.slice(17, 20)}-${hash.slice(20, 32)}`;
};

/** New feed events route to retained content too, independently of missing/upgrade requests. */
export async function discoverCrossSeeds(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  const input = payload.parse(JSON.parse(job.payload_json ?? "{}"));
  const entries = ctx.repo.releaseInbox.entries(input.releaseIds);
  if (entries.length !== new Set(input.releaseIds).size) throw new Error("Reuse feed batch is missing retained entries");
  const media = entries[0]!.result.mediaType;
  if ((media !== "movie" && media !== "tv") || entries.some(entry => entry.result.mediaType !== media)) throw new Error("Unsupported reuse feed media");
  const eligible = (scope: CrossSeedScope) => ctx.repo.crossSeed.hasFiles(scope) &&
    ctx.repo.retention.get({ namespace: scope.media === "tv" ? "tv-target" : "movie-target", id: String(scope.targetId) })?.desiredPresence !== "evicted";
  const offers = entries.map(entry => ({ ...movieOfferFromResult({ ...entry.result, mediaType: "movie" }), inboxId: entry.id }));
  const scopes = media === "movie" ? ctx.repo.movies.targets().map(t => ({ media: "movie" as const, targetId: t.id })).filter(eligible)
    : ctx.repo.tv.targets().map(t => ({ media: "tv" as const, targetId: t.id })).filter(eligible);
  for (let offset = 0; offset < scopes.length; offset += 50) {
    const batch = scopes.slice(offset, offset + 50);
    const matches = await durableStep(ctx, `reuse-feed:match:${offset}`, async () => {
      if (media === "movie") {
        const matcher = ctx.movieFeedMatcher ?? createMovieFeedMatcher(ctx.getSettings(), ctx.modelProvider);
        return matcher({ targets: batch.map(scope => {
          const target = ctx.repo.movies.target(scope.targetId)!;
          return { targetId: target.id, movie: ctx.repo.movies.movie(target.movie_id)! };
        }), offers });
      }
      const matcher = ctx.tvFeedMatcher ?? createTvFeedMatcher(ctx.getSettings(), ctx.modelProvider, "reuse");
      return matcher({ targets: batch.map(scope => {
        const target = ctx.repo.tv.target(scope.targetId)!, entry = ctx.repo.tv.entry(target.entry_id)!;
        return { targetId: target.id, entry: { ...entry, episodes: [] }, episodes: [] };
      }), offers: offers.map(offer => ({ title: offer.title, provider: offer.provider, sizeBytes: offer.sizeBytes, seeders: offer.seeders, attributes: [] })) });
    });
    for (const match of matches) {
      if (!batch.some(scope => scope.targetId === match.targetId) || match.releaseIndexes.some(index => !Number.isInteger(index) || !entries[index]))
        throw new Error("Reuse matcher returned an unknown scope/release");
      for (const index of [...new Set(match.releaseIndexes)]) {
        // Queue every match durably; the shared execution lane bounds transfers.
        const scope = { media, targetId: match.targetId };
        await durableStep(ctx, `reuse-feed:queue:${scope.targetId}:${entries[index]!.id}`, () => {
          ctx.execution?.assertActive();
          if (!eligible(scope)) return;
          queueCrossSeed(ctx.repo, { operationId: operationId(`${media}:${scope.targetId}:${entries[index]!.id}:v1`),
            scope, releaseId: entries[index]!.id });
        });
      }
    }
  }
  return "done";
}
