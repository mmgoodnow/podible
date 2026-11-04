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
  tv: t.router({
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
