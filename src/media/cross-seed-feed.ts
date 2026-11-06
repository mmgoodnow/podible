import { createHash } from "node:crypto";
import { z } from "zod";
import type { JobRow } from "../app-types";
import { durableStep, type WorkerContext } from "../worker/context";
import { createMovieFeedMatcher } from "../movies/agents";
import { createTvFeedMatcher } from "../tv/feed-agent";
import { releaseOfferFromResult } from "./release-offer";
import { queueCrossSeed } from "./cross-seed";
import type { CrossSeedScope } from "./cross-seed-state";
import { createBookFeedMatcher } from "../library/feed-matcher";
import type { TorznabResult } from "../library/torznab";
import type { MediaType } from "../app-types";

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
  if (media === "audio" || media === "ebook") {
    if (entries.some(entry => entry.result.mediaType !== media)) throw new Error("Mixed reuse feed media");
    return discoverBookCrossSeeds(ctx, entries as { id: number; result: TorznabResult }[], media);
  }
  if ((media !== "movie" && media !== "tv") || entries.some(entry => entry.result.mediaType !== media)) throw new Error("Unsupported reuse feed media");
  const eligible = (scope: CrossSeedScope) => ctx.repo.crossSeed.hasFiles(scope) &&
    ctx.repo.retention.get({ namespace: scope.media === "tv" ? "tv-target" : "movie-target", id: String(scope.targetId) })?.desiredPresence !== "evicted";
  const offers = entries.map(entry => ({ ...releaseOfferFromResult(entry.result), inboxId: entry.id }));
  const scopes = await durableStep(ctx, "reuse-feed:scopes", () => media === "movie"
    ? ctx.repo.movies.targets().map(t => ({ media: "movie" as const, targetId: t.id })).filter(eligible)
    : ctx.repo.tv.targets().map(t => ({ media: "tv" as const, targetId: t.id })).filter(eligible));
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

/** Already-imported books still consume new releases, independently of missing-format acquisition. */
async function discoverBookCrossSeeds(ctx: WorkerContext, entries: { id: number; result: TorznabResult }[], media: MediaType): Promise<"done"> {
  const eligible = (bookId: number) => Boolean(ctx.repo.getBook(bookId)) &&
    ctx.repo.crossSeed.hasFiles({ media, targetId: bookId });
  const targets = await durableStep(ctx, "reuse-feed:scopes", () => ctx.repo.listAllBooks()
    .filter(book => eligible(book.id))
    .map(book => ({ bookId: book.id, media, title: book.title, author: book.author, language: book.language })));
  for (let offset = 0; offset < targets.length; offset += 50) {
    const batch = targets.slice(offset, offset + 50);
    const matches = await durableStep(ctx, `reuse-feed:match:${offset}`, () =>
      (ctx.bookFeedMatcher ?? createBookFeedMatcher(ctx.getSettings(), ctx.modelProvider))({ targets: batch, releases: entries.map(entry => entry.result) }));
    for (const match of matches) {
      if (match.media !== media || !batch.some(target => target.bookId === match.bookId) || !match.releaseIndexes.length ||
        match.releaseIndexes.some(index => !Number.isInteger(index) || !entries[index]))
        throw new Error("Book reuse matcher returned an unknown scope/release");
      for (const index of new Set(match.releaseIndexes)) {
        await durableStep(ctx, `reuse-feed:queue:${match.bookId}:${entries[index]!.id}`, () => {
          ctx.execution?.assertActive();
          if (!eligible(match.bookId)) return;
          queueCrossSeed(ctx.repo, { operationId: operationId(`${media}:${match.bookId}:${entries[index]!.id}:v1`),
            scope: { media, targetId: match.bookId }, releaseId: entries[index]!.id });
        });
      }
    }
  }
  return "done";
}
