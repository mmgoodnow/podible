import { z } from "zod";

import { hydrateBookFromOpenLibrary } from "../hydration";
import { resolveOpenLibraryCandidate } from "../openlibrary";
import { RtorrentClient } from "../rtorrent";
import { triggerAutoAcquire } from "../service";

import { defineMethod, defineRouter } from "./framework";
import {
  assetRowSchema,
  emptyParamsSchema,
  jobIdResultSchema,
  libraryBookSchema,
  limitSchema,
  mediaSchema,
  mediaSelectionSchema,
  optionalBooleanSchema,
  optionalPositiveIntArraySchema,
  optionalPositiveIntSchema,
  optionalStringArraySchema,
  optionalStringSchema,
  positiveIntSchema,
  nonEmptyStringSchema,
  releaseRowSchema,
} from "./schemas";
import { enrichLibraryBookProgress, removeFileIfPresent, RpcError } from "./shared";

export const libraryRouter = defineRouter({
  list: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "List books in the library.",
    paramsSchema: emptyParamsSchema.extend({
      limit: limitSchema.optional(),
      cursor: optionalPositiveIntSchema,
      q: optionalStringSchema,
    }),
    resultSchema: z.object({
      items: z.array(libraryBookSchema),
      nextCursor: positiveIntSchema.optional(),
    }),
    async handler(ctx, params) {
      return ctx.repo.listBooks(params.limit ?? 50, params.cursor, params.q);
    },
  }),

  get: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "Get one book with releases and assets.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
    }),
    resultSchema: z.object({
      book: libraryBookSchema,
      releases: z.array(releaseRowSchema),
      assets: z.array(assetRowSchema),
    }),
    async handler(ctx, params) {
      const book = ctx.repo.getBook(params.bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }
      const bookClient =
        book.audioStatus === "downloading" || book.ebookStatus === "downloading"
          ? new RtorrentClient(ctx.repo.getSettings().rtorrent)
          : null;
      const enrichedBook = await enrichLibraryBookProgress(ctx.repo, book, bookClient);
      return {
        book: enrichedBook,
        releases: ctx.repo.listReleasesByBook(params.bookId),
        assets: ctx.repo.listAssetsByBook(params.bookId),
      };
    },
  }),

  inProgress: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "List non-terminal LibraryBook rows (optionally filtered by bookIds).",
    paramsSchema: emptyParamsSchema.extend({
      bookIds: optionalPositiveIntArraySchema,
    }),
    resultSchema: z.object({
      items: z.array(libraryBookSchema),
    }),
    async handler(ctx, params) {
      const items = ctx.repo.listInProgressBooks(params.bookIds);
      const hasDownloading = items.some((book) => book.audioStatus === "downloading" || book.ebookStatus === "downloading");
      const client = hasDownloading ? new RtorrentClient(ctx.repo.getSettings().rtorrent) : null;
      return {
        items: await Promise.all(items.map((book) => enrichLibraryBookProgress(ctx.repo, book, client))),
      };
    },
  }),

  create: defineMethod({
    auth: "user",
    summary: "Create a book from an Open Library work key and queue auto-acquire.",
    paramsSchema: emptyParamsSchema.extend({
      openLibraryKey: nonEmptyStringSchema,
    }),
    resultSchema: z.object({
      book: libraryBookSchema.nullable(),
      acquisition_job_id: positiveIntSchema,
    }),
    async handler(ctx, params) {
      const resolved = await resolveOpenLibraryCandidate({ openLibraryKey: params.openLibraryKey.trim() });
      if (!resolved) {
        throw new RpcError(-32000, "Open Library match not found", { error: "not_found" });
      }

      const book = ctx.repo.createBook({
        title: resolved.title,
        author: resolved.author,
      });

      ctx.repo.updateBookMetadata(book.id, {
        publishedAt: resolved.publishedAt ?? null,
        language: resolved.language ?? null,
        identifiers: resolved.identifiers,
      });
      const hydrated = ctx.repo.getBook(book.id);
      if (hydrated) {
        await hydrateBookFromOpenLibrary(ctx.repo, hydrated);
      }

      const jobId = await triggerAutoAcquire(ctx.repo, book.id);
      return {
        book: ctx.repo.getBook(book.id),
        acquisition_job_id: jobId,
      };
    },
  }),

  acquire: defineMethod({
    auth: "user",
    summary: "Queue targeted acquire job for a book/media set.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
      media: mediaSelectionSchema.optional(),
      forceAgent: optionalBooleanSchema,
      priorFailure: optionalBooleanSchema,
      rejectedUrls: optionalStringArraySchema,
      rejectedGuids: optionalStringArraySchema,
      rejectedInfoHashes: optionalStringArraySchema,
    }),
    resultSchema: z.object({
      jobId: positiveIntSchema,
      media: z.array(mediaSchema),
      forceAgent: z.boolean(),
      priorFailure: z.boolean(),
      rejectedUrls: z.array(z.string()),
      rejectedGuids: z.array(z.string()),
      rejectedInfoHashes: z.array(z.string()),
    }),
    async handler(ctx, params) {
      const book = ctx.repo.getBookRow(params.bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }
      const media = params.media ?? ["audio", "ebook"];
      const forceAgent = params.forceAgent ?? false;
      const priorFailure = params.priorFailure ?? false;
      const rejectedUrls = params.rejectedUrls ?? [];
      const rejectedGuids = params.rejectedGuids ?? [];
      const rejectedInfoHashes = params.rejectedInfoHashes ?? [];
      const jobId = await triggerAutoAcquire(ctx.repo, params.bookId, media, {
        forceAgent,
        priorFailure,
        rejectedUrls,
        rejectedGuids,
        rejectedInfoHashes,
      });
      return { jobId, media, forceAgent, priorFailure, rejectedUrls, rejectedGuids, rejectedInfoHashes };
    },
  }),

  refresh: defineMethod({
    auth: "admin",
    summary: "Queue full library filesystem refresh scan.",
    paramsSchema: emptyParamsSchema,
    resultSchema: jobIdResultSchema,
    async handler(ctx) {
      const job = ctx.repo.createJob({
        type: "full_library_refresh",
      });
      return { jobId: job.id };
    },
  }),

  reportImportIssue: defineMethod({
    auth: "admin",
    summary: "Report wrong imported file(s), delete imported asset(s), and queue async review/reacquire.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
      mediaType: z.enum(["audio", "ebook"]),
      releaseId: optionalPositiveIntSchema,
    }),
    resultSchema: z.object({
      action: z.literal("wrong_file_review_queued"),
      jobId: positiveIntSchema,
      releaseId: positiveIntSchema,
      mediaType: mediaSchema,
      rejectedSourcePathsCount: z.number().int().nonnegative(),
      deletedAssetCount: z.number().int().nonnegative(),
      deletedAssetFileCount: z.number().int().nonnegative(),
      deletedAssetPaths: z.array(z.string()),
    }),
    async handler(ctx, params) {
      const book = ctx.repo.getBookRow(params.bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }

      const releases = ctx.repo.listReleasesByBook(params.bookId).filter((release) => release.media_type === params.mediaType);
      const assets = ctx.repo.listAssetsByBook(params.bookId);
      const release = (() => {
        if (params.releaseId !== undefined) {
          return releases.find((candidate) => candidate.id === params.releaseId) ?? null;
        }

        const mediaAsset = assets.find((asset) => {
          if (params.mediaType === "ebook") return asset.kind === "ebook";
          return asset.kind !== "ebook";
        });
        if (mediaAsset?.source_release_id) {
          const fromAsset = releases.find((candidate) => candidate.id === mediaAsset.source_release_id);
          if (fromAsset) return fromAsset;
        }

        const imported = releases.find((candidate) => candidate.status === "imported");
        if (imported) return imported;

        return releases[0] ?? null;
      })();
      if (!release) {
        throw new RpcError(-32000, "Release not found for media", {
          error: "not_found",
          bookId: params.bookId,
          mediaType: params.mediaType,
          releaseId: params.releaseId,
        });
      }
      const wrongAssets = assets.filter((asset) => asset.source_release_id === release.id).filter((asset) =>
        params.mediaType === "ebook" ? asset.kind === "ebook" : asset.kind !== "ebook"
      );
      const wrongAssetFiles = wrongAssets.flatMap((asset) => ctx.repo.getAssetFiles(asset.id));
      const rejectedSourcePaths = wrongAssetFiles.map((file) => file.source_path ?? file.path);

      const candidateDeletePaths = Array.from(new Set(wrongAssetFiles.map((file) => file.path).filter(Boolean)));
      for (const asset of wrongAssets) {
        ctx.repo.deleteAsset(asset.id);
      }
      const deletedAssetPaths: string[] = [];
      for (const filePath of candidateDeletePaths) {
        if (ctx.repo.hasAssetFilePath(filePath)) {
          continue;
        }
        if (await removeFileIfPresent(filePath)) {
          deletedAssetPaths.push(filePath);
        }
      }

      if (wrongAssets.length > 0 && release.status === "imported") {
        ctx.repo.setReleaseStatus(release.id, "downloaded", null);
      }

      const importJob = ctx.repo.createJob({
        type: "import",
        bookId: book.id,
        releaseId: release.id,
        payload: {
          reason: "user_reported_wrong_file",
          userReportedIssue: true,
          rejectedSourcePaths,
        },
      });
      return {
        action: "wrong_file_review_queued" as const,
        jobId: importJob.id,
        releaseId: release.id,
        mediaType: params.mediaType,
        rejectedSourcePathsCount: rejectedSourcePaths.length,
        deletedAssetCount: wrongAssets.length,
        deletedAssetFileCount: deletedAssetPaths.length,
        deletedAssetPaths,
      };
    },
  }),

  rehydrate: defineMethod({
    auth: "admin",
    summary: "Re-run Open Library metadata hydration for one/all books.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: optionalPositiveIntSchema,
    }),
    resultSchema: z.object({
      attempted: z.number().int().nonnegative(),
      updatedBookIds: z.array(positiveIntSchema),
    }),
    async handler(ctx, params) {
      const books = params.bookId === undefined ? ctx.repo.listAllBooks() : [ctx.repo.getBook(params.bookId)];
      const resolved = books.filter((book): book is NonNullable<typeof book> => Boolean(book));
      if (params.bookId !== undefined && resolved.length === 0) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }

      const updatedBookIds: number[] = [];
      for (const book of resolved) {
        if (await hydrateBookFromOpenLibrary(ctx.repo, book)) {
          updatedBookIds.push(book.id);
        }
      }

      return {
        attempted: resolved.length,
        updatedBookIds,
      };
    },
  }),

  delete: defineMethod({
    auth: "admin",
    summary: "Delete a book, cascading DB rows and imported files/covers.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
    }),
    resultSchema: z.object({
      deletedBookId: positiveIntSchema,
      deletedAssetFileCount: z.number().int().nonnegative(),
      deletedAssetPaths: z.array(z.string()),
      deletedCoverPath: z.string().nullable(),
    }),
    async handler(ctx, params) {
      const book = ctx.repo.getBookRow(params.bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }

      const artifacts = ctx.repo.getBookDeleteArtifacts(params.bookId);
      const deleted = ctx.repo.deleteBook(params.bookId);
      if (!deleted) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }
      const deletedAssetPaths: string[] = [];
      for (const filePath of artifacts.assetPaths) {
        if (await removeFileIfPresent(filePath)) {
          deletedAssetPaths.push(filePath);
        }
      }
      const deletedCoverPath = artifacts.coverPath ? ((await removeFileIfPresent(artifacts.coverPath)) ? artifacts.coverPath : null) : null;

      return {
        deletedBookId: params.bookId,
        deletedAssetFileCount: deletedAssetPaths.length,
        deletedAssetPaths,
        deletedCoverPath,
      };
    },
  }),
});

export const releasesRouter = defineRouter({
  list: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "List releases for a book.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
    }),
    resultSchema: z.object({
      releases: z.array(releaseRowSchema),
    }),
    async handler(ctx, params) {
      return { releases: ctx.repo.listReleasesByBook(params.bookId) };
    },
  }),
});
