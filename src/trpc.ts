import { initTRPC, TRPCError } from "@trpc/server";
import { z } from "zod";
import { authRouter } from "./rpc/auth-router";
import { executeMethod } from "./rpc/framework";
import { libraryRouter } from "./rpc/library-router";
import { bookWebRouter } from "./rpc/book-web-router";
import { openLibraryRouter } from "./rpc/openlibrary-router";
import { importRouter } from "./rpc/import-router";
import { RpcError, type RpcContext } from "./rpc/shared";
import { moviesRouter, movieRequestsRouter } from "./rpc/movies-router";
import { movieShadowRouter } from "./rpc/movie-shadow-router";
import { movieDualWriteRouter } from "./rpc/movie-dual-write-router";
import { indexerFeedsRouter } from "./rpc/indexer-feeds-router";
import { tvShadowRouter } from "./rpc/tv-shadow-router";
import { retentionRouter } from "./rpc/retention-router";
import { tvRouter } from "./rpc/tv-router";
import { settingsRouter } from "./rpc/settings-router";
import { jobsRouter } from "./rpc/jobs-router";
import { downloadsRouter } from "./rpc/downloads-router";
import { adminWebRouter } from "./rpc/admin-web-router";
import { recoveryRouter } from "./rpc/recovery-router";
import { catalogRouter } from "./rpc/catalog-router";
import { stewardshipRouter } from "./rpc/stewardship-router";
import { tvRetentionRouter } from "./rpc/tv-retention-router";
import { torrentRetirementRouter } from "./rpc/torrent-retirement-router";
import { transferMetricsRouter } from "./rpc/transfer-metrics-router";
import { crossSeedRouter } from "./rpc/cross-seed-router";

const t = initTRPC.context<RpcContext>().create();
const id = z.number().int().positive();
const bookInput = z.object({ bookId: id });
const media = z.enum(["audio", "ebook"]);
function freshArtwork<T extends { coverUrl: string | null; updatedAt: string }>(book: T): T {
  return book.coverUrl
    ? { ...book, coverUrl: `${book.coverUrl}${book.coverUrl.includes("?") ? "&" : "?"}v=${encodeURIComponent(book.updatedAt)}` }
    : book;
}
const procedure = t.procedure.use(async ({ next }) => {
  const result = await next();
  if (!result.ok && result.error.cause instanceof RpcError) {
    const cause = result.error.cause;
    const notFound =
      cause.data !== null &&
      typeof cause.data === "object" &&
      "error" in cause.data &&
      cause.data.error === "not_found";
    throw new TRPCError({
      code:
        cause.code === -32001
          ? "UNAUTHORIZED"
          : cause.code === -32003
            ? "FORBIDDEN"
            : cause.code === -32602
              ? "BAD_REQUEST"
              : notFound
                ? "NOT_FOUND"
                : "INTERNAL_SERVER_ERROR",
      message:
        cause.code === -32000 && !notFound
          ? "The operation could not be completed"
          : cause.message,
      cause,
    });
  }
  return result;
});

export const appRouter = t.router({
  crossSeed: t.router({
    startBackfill: procedure.input(crossSeedRouter.routes.startBackfill.paramsSchema).output(crossSeedRouter.routes.startBackfill.resultSchema).mutation(({ ctx, input }) => executeMethod(crossSeedRouter.routes.startBackfill, ctx, input)),
    backfillLatest: procedure.input(crossSeedRouter.routes.backfillLatest.paramsSchema).output(crossSeedRouter.routes.backfillLatest.resultSchema).query(({ ctx, input }) => executeMethod(crossSeedRouter.routes.backfillLatest, ctx, input)),
    backfillStatus: procedure.input(crossSeedRouter.routes.backfillStatus.paramsSchema).output(crossSeedRouter.routes.backfillStatus.resultSchema).query(({ ctx, input }) => executeMethod(crossSeedRouter.routes.backfillStatus, ctx, input)),
    cancelBackfill: procedure.input(crossSeedRouter.routes.cancelBackfill.paramsSchema).output(crossSeedRouter.routes.cancelBackfill.resultSchema).mutation(({ ctx, input }) => executeMethod(crossSeedRouter.routes.cancelBackfill, ctx, input)),
  }),
  transferMetrics: t.router({
    status: procedure.input(transferMetricsRouter.routes.status.paramsSchema).output(transferMetricsRouter.routes.status.resultSchema).query(({ ctx, input }) => executeMethod(transferMetricsRouter.routes.status, ctx, input)),
    library: procedure.input(transferMetricsRouter.routes.library.paramsSchema).output(transferMetricsRouter.routes.library.resultSchema).query(({ ctx, input }) => executeMethod(transferMetricsRouter.routes.library, ctx, input)),
  }),
  tvRetention: t.router({
    audit: procedure.input(tvRetentionRouter.routes.audit.paramsSchema).output(tvRetentionRouter.routes.audit.resultSchema).query(({ ctx, input }) => executeMethod(tvRetentionRouter.routes.audit, ctx, input)),
    preview: procedure.input(tvRetentionRouter.routes.preview.paramsSchema).output(tvRetentionRouter.routes.preview.resultSchema).query(({ ctx, input }) => executeMethod(tvRetentionRouter.routes.preview, ctx, input)),
    evict: procedure.input(tvRetentionRouter.routes.evict.paramsSchema).output(tvRetentionRouter.routes.evict.resultSchema).mutation(({ ctx, input }) => executeMethod(tvRetentionRouter.routes.evict, ctx, input)),
  }),
  torrentRetirement: t.router({
    preview: procedure.input(torrentRetirementRouter.routes.preview.paramsSchema).output(torrentRetirementRouter.routes.preview.resultSchema).query(({ ctx, input }) => executeMethod(torrentRetirementRouter.routes.preview, ctx, input)),
    get: procedure.input(torrentRetirementRouter.routes.get.paramsSchema).output(torrentRetirementRouter.routes.get.resultSchema).query(({ ctx, input }) => executeMethod(torrentRetirementRouter.routes.get, ctx, input)),
  }),
  stewardship: t.router({
    status: procedure.input(stewardshipRouter.routes.status.paramsSchema).output(stewardshipRouter.routes.status.resultSchema).query(({ ctx, input }) => executeMethod(stewardshipRouter.routes.status, ctx, input)),
    collect: procedure.input(stewardshipRouter.routes.collect.paramsSchema).output(stewardshipRouter.routes.collect.resultSchema).mutation(({ ctx, input }) => executeMethod(stewardshipRouter.routes.collect, ctx, input)),
  }),
  catalog: t.router({
    recent: procedure.input(catalogRouter.routes.recent.paramsSchema).output(catalogRouter.routes.recent.resultSchema).query(({ ctx, input }) => executeMethod(catalogRouter.routes.recent, ctx, input)),
    discovery: procedure.input(catalogRouter.routes.discovery.paramsSchema).output(catalogRouter.routes.discovery.resultSchema).query(({ ctx, input }) => executeMethod(catalogRouter.routes.discovery, ctx, input)),
    available: procedure.input(catalogRouter.routes.available.paramsSchema).output(catalogRouter.routes.available.resultSchema).query(({ ctx, input }) => executeMethod(catalogRouter.routes.available, ctx, input)),
    tvEntry: procedure.input(catalogRouter.routes.tvEntry.paramsSchema).output(catalogRouter.routes.tvEntry.resultSchema).query(({ ctx, input }) => executeMethod(catalogRouter.routes.tvEntry, ctx, input)),
  }),
  recovery: t.router({
    inventory: procedure.input(recoveryRouter.routes.inventory.paramsSchema).output(recoveryRouter.routes.inventory.resultSchema).query(({ ctx, input }) => executeMethod(recoveryRouter.routes.inventory, ctx, input)),
    preview: procedure.input(recoveryRouter.routes.preview.paramsSchema).output(recoveryRouter.routes.preview.resultSchema).query(({ ctx, input }) => executeMethod(recoveryRouter.routes.preview, ctx, input)),
    recover: procedure.input(recoveryRouter.routes.recover.paramsSchema).output(recoveryRouter.routes.recover.resultSchema).mutation(({ ctx, input }) => executeMethod(recoveryRouter.routes.recover, ctx, input)),
  }),
  adminWeb: t.router({
    users: procedure.input(adminWebRouter.routes.users.paramsSchema).output(adminWebRouter.routes.users.resultSchema).query(({ ctx, input }) => executeMethod(adminWebRouter.routes.users, ctx, input)),
    plexServers: procedure.input(adminWebRouter.routes.plexServers.paramsSchema).output(adminWebRouter.routes.plexServers.resultSchema).query(({ ctx, input }) => executeMethod(adminWebRouter.routes.plexServers, ctx, input)),
    selectPlexServer: procedure.input(adminWebRouter.routes.selectPlexServer.paramsSchema).output(adminWebRouter.routes.selectPlexServer.resultSchema).mutation(({ ctx, input }) => executeMethod(adminWebRouter.routes.selectPlexServer, ctx, input)),
    ops: procedure.input(adminWebRouter.routes.ops.paramsSchema).output(adminWebRouter.routes.ops.resultSchema).query(({ ctx, input }) => executeMethod(adminWebRouter.routes.ops, ctx, input)),
    refresh: procedure.input(adminWebRouter.routes.refresh.paramsSchema).output(adminWebRouter.routes.refresh.resultSchema).mutation(({ ctx, input }) => executeMethod(adminWebRouter.routes.refresh, ctx, input)),
  }),
  settings: t.router({
    get: procedure.input(settingsRouter.routes.get.paramsSchema).output(settingsRouter.routes.get.resultSchema).query(({ ctx, input }) => executeMethod(settingsRouter.routes.get, ctx, input)),
    update: procedure.input(settingsRouter.routes.update.paramsSchema).output(settingsRouter.routes.update.resultSchema).mutation(({ ctx, input }) => executeMethod(settingsRouter.routes.update, ctx, input)),
  }),
  jobs: t.router({
    list: procedure.input(jobsRouter.routes.list.paramsSchema).output(jobsRouter.routes.list.resultSchema).query(({ ctx, input }) => executeMethod(jobsRouter.routes.list, ctx, input)),
    get: procedure.input(jobsRouter.routes.get.paramsSchema).output(jobsRouter.routes.get.resultSchema).query(({ ctx, input }) => executeMethod(jobsRouter.routes.get, ctx, input)),
    retry: procedure.input(jobsRouter.routes.retry.paramsSchema).output(jobsRouter.routes.retry.resultSchema).mutation(({ ctx, input }) => executeMethod(jobsRouter.routes.retry, ctx, input)),
  }),
  downloads: t.router({
    list: procedure.input(downloadsRouter.routes.list.paramsSchema).output(downloadsRouter.routes.list.resultSchema).query(({ ctx, input }) => executeMethod(downloadsRouter.routes.list, ctx, input)),
    get: procedure.input(downloadsRouter.routes.get.paramsSchema).output(downloadsRouter.routes.get.resultSchema).query(({ ctx, input }) => executeMethod(downloadsRouter.routes.get, ctx, input)),
    retry: procedure.input(downloadsRouter.routes.retry.paramsSchema).output(downloadsRouter.routes.retry.resultSchema).mutation(({ ctx, input }) => executeMethod(downloadsRouter.routes.retry, ctx, input)),
  }),
  tv: t.router({
    search: procedure.input(tvRouter.routes.search.paramsSchema).output(tvRouter.routes.search.resultSchema).query(({ ctx, input }) => executeMethod(tvRouter.routes.search, ctx, input)),
    details: procedure.input(tvRouter.routes.details.paramsSchema).output(tvRouter.routes.details.resultSchema).query(({ ctx, input }) => executeMethod(tvRouter.routes.details, ctx, input)),
    request: procedure.input(tvRouter.routes.request.paramsSchema).output(tvRouter.routes.request.resultSchema).mutation(({ ctx, input }) => executeMethod(tvRouter.routes.request, ctx, input)),
    myRequests: procedure.input(tvRouter.routes.myRequests.paramsSchema).output(tvRouter.routes.myRequests.resultSchema).query(({ ctx, input }) => executeMethod(tvRouter.routes.myRequests, ctx, input)),
    myRequest: procedure.input(tvRouter.routes.myRequest.paramsSchema).output(tvRouter.routes.myRequest.resultSchema).query(({ ctx, input }) => executeMethod(tvRouter.routes.myRequest, ctx, input)),
    withdraw: procedure.input(tvRouter.routes.withdraw.paramsSchema).output(tvRouter.routes.withdraw.resultSchema).mutation(({ ctx, input }) => executeMethod(tvRouter.routes.withdraw, ctx, input)),
    retryMyRequest: procedure.input(tvRouter.routes.retryMyRequest.paramsSchema).output(tvRouter.routes.retryMyRequest.resultSchema).mutation(({ ctx, input }) => executeMethod(tvRouter.routes.retryMyRequest, ctx, input)),
    status: procedure.input(tvRouter.routes.status.paramsSchema).output(tvRouter.routes.status.resultSchema).query(({ ctx, input }) => executeMethod(tvRouter.routes.status, ctx, input)),
    list: procedure.input(tvRouter.routes.list.paramsSchema).output(tvRouter.routes.list.resultSchema).query(({ ctx, input }) => executeMethod(tvRouter.routes.list, ctx, input)),
    get: procedure.input(tvRouter.routes.get.paramsSchema).output(tvRouter.routes.get.resultSchema).query(({ ctx, input }) => executeMethod(tvRouter.routes.get, ctx, input)),
    sync: procedure.input(tvRouter.routes.sync.paramsSchema).output(tvRouter.routes.sync.resultSchema).mutation(({ ctx, input }) => executeMethod(tvRouter.routes.sync, ctx, input)),
    backfill: procedure.input(tvRouter.routes.backfill.paramsSchema).output(tvRouter.routes.backfill.resultSchema).mutation(({ ctx, input }) => executeMethod(tvRouter.routes.backfill, ctx, input)),
    retry: procedure.input(tvRouter.routes.retry.paramsSchema).output(tvRouter.routes.retry.resultSchema).mutation(({ ctx, input }) => executeMethod(tvRouter.routes.retry, ctx, input)),
  }),
  retention: t.router({
    status: procedure.input(retentionRouter.routes.status.paramsSchema).output(retentionRouter.routes.status.resultSchema).query(({ ctx, input }) => executeMethod(retentionRouter.routes.status, ctx, input)),
    audit: procedure.input(retentionRouter.routes.audit.paramsSchema).output(retentionRouter.routes.audit.resultSchema).query(({ ctx, input }) => executeMethod(retentionRouter.routes.audit, ctx, input)),
    preview: procedure.input(retentionRouter.routes.preview.paramsSchema).output(retentionRouter.routes.preview.resultSchema).query(({ ctx, input }) => executeMethod(retentionRouter.routes.preview, ctx, input)),
    evict: procedure.input(retentionRouter.routes.evict.paramsSchema).output(retentionRouter.routes.evict.resultSchema).mutation(({ ctx, input }) => executeMethod(retentionRouter.routes.evict, ctx, input)),
  }),
  tvShadow: t.router({
    status: procedure.input(tvShadowRouter.routes.status.paramsSchema).output(tvShadowRouter.routes.status.resultSchema).query(({ ctx, input }) => executeMethod(tvShadowRouter.routes.status, ctx, input)),
    list: procedure.input(tvShadowRouter.routes.list.paramsSchema).output(tvShadowRouter.routes.list.resultSchema).query(({ ctx, input }) => executeMethod(tvShadowRouter.routes.list, ctx, input)),
    get: procedure.input(tvShadowRouter.routes.get.paramsSchema).output(tvShadowRouter.routes.get.resultSchema).query(({ ctx, input }) => executeMethod(tvShadowRouter.routes.get, ctx, input)),
    sync: procedure.input(tvShadowRouter.routes.sync.paramsSchema).output(tvShadowRouter.routes.sync.resultSchema).mutation(({ ctx, input }) => executeMethod(tvShadowRouter.routes.sync, ctx, input)),
    retry: procedure.input(tvShadowRouter.routes.retry.paramsSchema).output(tvShadowRouter.routes.retry.resultSchema).mutation(({ ctx, input }) => executeMethod(tvShadowRouter.routes.retry, ctx, input)),
  }),
  indexerFeeds: t.router({
    status: procedure
      .input(indexerFeedsRouter.routes.status.paramsSchema)
      .output(indexerFeedsRouter.routes.status.resultSchema)
      .query(({ ctx, input }) => executeMethod(indexerFeedsRouter.routes.status, ctx, input)),
    sync: procedure
      .input(indexerFeedsRouter.routes.sync.paramsSchema)
      .output(indexerFeedsRouter.routes.sync.resultSchema)
      .mutation(({ ctx, input }) => executeMethod(indexerFeedsRouter.routes.sync, ctx, input)),
  }),
  book: t.router({
    diagnostics: procedure
      .input(bookWebRouter.routes.diagnostics.paramsSchema)
      .output(bookWebRouter.routes.diagnostics.resultSchema)
      .query(({ ctx, input }) => executeMethod(bookWebRouter.routes.diagnostics, ctx, input)),
    activity: procedure
      .input(bookWebRouter.routes.activity.paramsSchema)
      .output(bookWebRouter.routes.activity.resultSchema)
      .query(({ ctx, input }) => executeMethod(bookWebRouter.routes.activity, ctx, input)),
    editions: procedure
      .input(bookWebRouter.routes.editions.paramsSchema)
      .output(bookWebRouter.routes.editions.resultSchema)
      .query(({ ctx, input }) => executeMethod(bookWebRouter.routes.editions, ctx, input)),
    chapters: procedure
      .input(bookWebRouter.routes.chapters.paramsSchema)
      .output(bookWebRouter.routes.chapters.resultSchema)
      .query(({ ctx, input }) => executeMethod(bookWebRouter.routes.chapters, ctx, input)),
  }),
  openlibrary: t.router({
    search: procedure
      .input(openLibraryRouter.routes.search.paramsSchema)
      .output(openLibraryRouter.routes.search.resultSchema)
      .query(({ ctx, input }) => executeMethod(openLibraryRouter.routes.search, ctx, input)),
    covers: procedure
      .input(openLibraryRouter.routes.covers.paramsSchema)
      .output(openLibraryRouter.routes.covers.resultSchema)
      .query(({ ctx, input }) => executeMethod(openLibraryRouter.routes.covers, ctx, input)),
    setCover: procedure
      .input(openLibraryRouter.routes.setCover.paramsSchema)
      .output(openLibraryRouter.routes.setCover.resultSchema)
      .mutation(async ({ ctx, input }) => {
        const result = await executeMethod(openLibraryRouter.routes.setCover, ctx, input);
        return { ...result, book: freshArtwork(result.book) };
      }),
  }),
  import: t.router({
    inspect: procedure
      .input(importRouter.routes.inspect.paramsSchema)
      .output(importRouter.routes.inspect.resultSchema)
      .query(({ ctx, input }) => executeMethod(importRouter.routes.inspect, ctx, input)),
    manual: procedure
      .input(bookInput.extend({
        mediaType: media,
        path: z.string().trim().min(1),
        selectedPaths: z.array(z.string()).optional(),
        title: z.string().optional(),
      }))
      .output(importRouter.routes.manual.resultSchema)
      .mutation(({ ctx, input }) => executeMethod(importRouter.routes.manual, ctx, input)),
  }),
  movieDualWrite: t.router({
    status: procedure.input(movieDualWriteRouter.routes.status.paramsSchema).output(movieDualWriteRouter.routes.status.resultSchema).query(({ctx,input}) => executeMethod(movieDualWriteRouter.routes.status,ctx,input)),
    list: procedure.input(movieDualWriteRouter.routes.list.paramsSchema).output(movieDualWriteRouter.routes.list.resultSchema).query(({ctx,input}) => executeMethod(movieDualWriteRouter.routes.list,ctx,input)),
    sync: procedure.input(movieDualWriteRouter.routes.sync.paramsSchema).output(movieDualWriteRouter.routes.sync.resultSchema).mutation(({ctx,input}) => executeMethod(movieDualWriteRouter.routes.sync,ctx,input)),
    backfill: procedure.input(movieDualWriteRouter.routes.backfill.paramsSchema).output(movieDualWriteRouter.routes.backfill.resultSchema).mutation(({ctx,input}) => executeMethod(movieDualWriteRouter.routes.backfill,ctx,input)),
  }),
  movieShadow: t.router({
    status: procedure
      .input(movieShadowRouter.routes.status.paramsSchema)
      .output(movieShadowRouter.routes.status.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(movieShadowRouter.routes.status, ctx, input),
      ),
    list: procedure
      .input(movieShadowRouter.routes.list.paramsSchema)
      .output(movieShadowRouter.routes.list.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(movieShadowRouter.routes.list, ctx, input),
      ),
    get: procedure
      .input(movieShadowRouter.routes.get.paramsSchema)
      .output(movieShadowRouter.routes.get.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(movieShadowRouter.routes.get, ctx, input),
      ),
    sync: procedure
      .input(movieShadowRouter.routes.sync.paramsSchema)
      .output(movieShadowRouter.routes.sync.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieShadowRouter.routes.sync, ctx, input),
      ),
    retry: procedure
      .input(movieShadowRouter.routes.retry.paramsSchema)
      .output(movieShadowRouter.routes.retry.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieShadowRouter.routes.retry, ctx, input),
      ),
    review: procedure
      .input(movieShadowRouter.routes.review.paramsSchema)
      .output(movieShadowRouter.routes.review.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieShadowRouter.routes.review, ctx, input),
      ),
  }),
  movies: t.router({
    status: procedure
      .input(moviesRouter.routes.status.paramsSchema)
      .output(moviesRouter.routes.status.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(moviesRouter.routes.status, ctx, input),
      ),
    search: procedure
      .input(moviesRouter.routes.search.paramsSchema)
      .output(moviesRouter.routes.search.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(moviesRouter.routes.search, ctx, input),
      ),
    get: procedure
      .input(moviesRouter.routes.get.paramsSchema)
      .output(moviesRouter.routes.get.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(moviesRouter.routes.get, ctx, input),
      ),
    request: procedure
      .input(moviesRouter.routes.request.paramsSchema)
      .output(moviesRouter.routes.request.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(moviesRouter.routes.request, ctx, input),
      ),
  }),
  requests: t.router({
    list: procedure
      .input(movieRequestsRouter.routes.list.paramsSchema)
      .output(movieRequestsRouter.routes.list.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.list, ctx, input),
      ),
    get: procedure
      .input(movieRequestsRouter.routes.get.paramsSchema)
      .output(movieRequestsRouter.routes.get.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.get, ctx, input),
      ),
    retry: procedure
      .input(movieRequestsRouter.routes.retry.paramsSchema)
      .output(movieRequestsRouter.routes.retry.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.retry, ctx, input),
      ),
    checkUpgrade: procedure
      .input(movieRequestsRouter.routes.checkUpgrade.paramsSchema)
      .output(movieRequestsRouter.routes.checkUpgrade.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.checkUpgrade, ctx, input),
      ),
    withdraw: procedure
      .input(movieRequestsRouter.routes.withdraw.paramsSchema)
      .output(movieRequestsRouter.routes.withdraw.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.withdraw, ctx, input),
      ),
    searchReleases: procedure
      .input(movieRequestsRouter.routes.searchReleases.paramsSchema)
      .output(movieRequestsRouter.routes.searchReleases.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.searchReleases, ctx, input),
      ),
    selectRelease: procedure
      .input(movieRequestsRouter.routes.selectRelease.paramsSchema)
      .output(movieRequestsRouter.routes.selectRelease.resultSchema)
      .mutation(({ ctx, input }) =>
        executeMethod(movieRequestsRouter.routes.selectRelease, ctx, input),
      ),
  }),
  auth: t.router({
    me: procedure
      .input(authRouter.routes.me.paramsSchema)
      .output(authRouter.routes.me.resultSchema)
      .query(({ ctx, input }) =>
        executeMethod(authRouter.routes.me, ctx, input),
      ),
  }),
  library: t.router({
    bootstrap: procedure
      .input(libraryRouter.routes.bootstrap.paramsSchema)
      .output(libraryRouter.routes.bootstrap.resultSchema)
      .query(async ({ ctx, input }) => {
        const result = await executeMethod(libraryRouter.routes.bootstrap, ctx, input);
        return { ...result, library: { ...result.library, items: result.library.items.map(freshArtwork) } };
      }),
    create: procedure
      .input(libraryRouter.routes.create.paramsSchema)
      .output(libraryRouter.routes.create.resultSchema)
      .mutation(({ ctx, input }) => executeMethod(libraryRouter.routes.create, ctx, input)),
    acquire: procedure
      .input(bookInput.extend({
        media: z.array(media).min(1).optional(),
        forceAgent: z.boolean().optional(),
        priorFailure: z.boolean().optional(),
        rejectedUrls: z.array(z.string()).optional(),
        rejectedGuids: z.array(z.string()).optional(),
        rejectedInfoHashes: z.array(z.string()).optional(),
      }))
      .output(libraryRouter.routes.acquire.resultSchema)
      .mutation(({ ctx, input }) => executeMethod(libraryRouter.routes.acquire, ctx, input)),
    series: procedure
      .input(libraryRouter.routes.series.paramsSchema)
      .output(libraryRouter.routes.series.resultSchema)
      .query(async ({ ctx, input }) => {
        const result = await executeMethod(libraryRouter.routes.series, ctx, input);
        return { ...result, libraryBooks: result.libraryBooks.map(freshArtwork) };
      }),
    author: procedure
      .input(libraryRouter.routes.author.paramsSchema)
      .output(libraryRouter.routes.author.resultSchema)
      .query(async ({ ctx, input }) => {
        const result = await executeMethod(libraryRouter.routes.author, ctx, input);
        return { ...result, libraryBooks: result.libraryBooks.map(freshArtwork) };
      }),
    reportImportIssue: procedure
      .input(bookInput.extend({ mediaType: media, releaseId: id.optional(), manifestationId: id.optional() }))
      .output(libraryRouter.routes.reportImportIssue.resultSchema)
      .mutation(({ ctx, input }) => executeMethod(libraryRouter.routes.reportImportIssue, ctx, input)),
    transcriptStatus: procedure
      .input(libraryRouter.routes.transcriptStatus.paramsSchema)
      .output(libraryRouter.routes.transcriptStatus.resultSchema)
      .query(({ ctx, input }) => executeMethod(libraryRouter.routes.transcriptStatus, ctx, input)),
    requestTranscription: procedure
      .input(libraryRouter.routes.requestTranscription.paramsSchema)
      .output(libraryRouter.routes.requestTranscription.resultSchema)
      .mutation(({ ctx, input }) => executeMethod(libraryRouter.routes.requestTranscription, ctx, input)),
    searchReleases: procedure
      .input(libraryRouter.routes.searchReleases.paramsSchema)
      .output(libraryRouter.routes.searchReleases.resultSchema)
      .query(({ ctx, input }) => executeMethod(libraryRouter.routes.searchReleases, ctx, input)),
    createManifestationFromSearch: procedure
      .input(libraryRouter.routes.createManifestationFromSearch.paramsSchema)
      .output(libraryRouter.routes.createManifestationFromSearch.resultSchema)
      .mutation(({ ctx, input }) => executeMethod(libraryRouter.routes.createManifestationFromSearch, ctx, input)),
    rehydrate: procedure
      .input(libraryRouter.routes.rehydrate.paramsSchema)
      .output(libraryRouter.routes.rehydrate.resultSchema)
      .mutation(({ ctx, input }) => executeMethod(libraryRouter.routes.rehydrate, ctx, input)),
    delete: procedure
      .input(libraryRouter.routes.delete.paramsSchema)
      .output(libraryRouter.routes.delete.resultSchema)
      .mutation(({ ctx, input }) => executeMethod(libraryRouter.routes.delete, ctx, input)),
    list: procedure
      .input(z.object({
        limit: z.number().int().positive().max(200).optional(),
        q: z.string().optional(),
        cursor: id.optional(),
      }))
      .output(libraryRouter.routes.list.resultSchema)
      .query(async ({ ctx, input }) => {
        const result = await executeMethod(libraryRouter.routes.list, ctx, input);
        return { ...result, items: result.items.map(freshArtwork) };
      }),
    get: procedure
      .input(libraryRouter.routes.get.paramsSchema)
      .output(libraryRouter.routes.get.resultSchema)
      .query(async ({ ctx, input }) => {
        const result = await executeMethod(libraryRouter.routes.get, ctx, input);
        return { ...result, book: freshArtwork(result.book) };
      }),
  }),
});

export type AppRouter = typeof appRouter;
