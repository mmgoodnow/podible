import { randomUUID } from "node:crypto";

import { z } from "zod";

import { createOrReuseBookFromOpenLibrary } from "../library/create";
import { getBookTranscriptStatus, requestBookTranscription } from "../library/chapter-analysis";
import { hydrateBookFromOpenLibrary } from "../library/hydration";
import { RtorrentClient } from "../rtorrent";
import { runSearch, runSnatchGroup, triggerAutoAcquire } from "../library/service";

import { defineMethod, defineRouter } from "./framework";
import {
  assetRowSchema,
  emptyParamsSchema,
  jobIdResultSchema,
  libraryBookWithPlaybackSchema,
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
  torznabResultSchema,
} from "./schemas";
import { enrichLibraryBookPlayback, enrichLibraryBookProgress, removeFileIfPresent, RpcError } from "./shared";
import type { BookRow } from "../app-types";

const RELEASE_SEARCH_TTL_MS = 30 * 60 * 1000;

const releaseSearchResultSchema = z.object({
  index: z.number().int().nonnegative(),
  title: z.string(),
  provider: z.string(),
  mediaType: mediaSchema,
  sizeBytes: z.number().int().nullable(),
  guid: z.string().nullable(),
  infoHash: z.string().nullable(),
  seeders: z.number().int().nullable(),
  leechers: z.number().int().nullable(),
});
const releaseSearchIndexSchema = z.coerce.number().int().nonnegative();

function defaultSearchQuery(book: BookRow): string {
  return [book.title, book.author].map((part) => part.trim()).filter(Boolean).join(" ");
}

function publicSearchResult(result: z.infer<typeof torznabResultSchema>, index: number): z.infer<typeof releaseSearchResultSchema> {
  return {
    index,
    title: result.title,
    provider: result.provider,
    mediaType: result.mediaType,
    sizeBytes: result.sizeBytes,
    guid: result.guid,
    infoHash: result.infoHash,
    seeders: result.seeders,
    leechers: result.leechers,
  };
}

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
      items: z.array(libraryBookWithPlaybackSchema),
      nextCursor: positiveIntSchema.optional(),
    }),
    async handler(ctx, params) {
      const result = ctx.repo.listBooks(params.limit ?? 50, params.cursor, params.q);
      return {
        ...result,
        items: result.items.map((book) => enrichLibraryBookPlayback(ctx.repo, ctx.request, book)),
      };
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
      book: libraryBookWithPlaybackSchema,
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
        book: enrichLibraryBookPlayback(ctx.repo, ctx.request, enrichedBook),
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
      items: z.array(libraryBookWithPlaybackSchema),
    }),
    async handler(ctx, params) {
      const items = ctx.repo.listInProgressBooks(params.bookIds);
      const hasDownloading = items.some((book) => book.audioStatus === "downloading" || book.ebookStatus === "downloading");
      const client = hasDownloading ? new RtorrentClient(ctx.repo.getSettings().rtorrent) : null;
      return {
        items: (await Promise.all(items.map((book) => enrichLibraryBookProgress(ctx.repo, book, client)))).map((book) =>
          enrichLibraryBookPlayback(ctx.repo, ctx.request, book)
        ),
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
      book: libraryBookWithPlaybackSchema.nullable(),
      acquisition_job_id: positiveIntSchema,
    }),
    async handler(ctx, params) {
      let result: { bookId: number; acquisitionJobId: number };
      try {
        result = await createOrReuseBookFromOpenLibrary(ctx.repo, params.openLibraryKey.trim());
      } catch (error) {
        if (error instanceof Error && error.message === "Open Library match not found") {
          throw new RpcError(-32000, "Open Library match not found", { error: "not_found" });
        }
        throw error;
      }
      return {
        book: (() => {
          const book = ctx.repo.getBook(result.bookId);
          return book ? enrichLibraryBookPlayback(ctx.repo, ctx.request, book) : null;
        })(),
        acquisition_job_id: result.acquisitionJobId,
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

  searchReleases: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "Search configured indexers for releases for a library book and persist a short-lived server-side result set.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
      mediaType: mediaSchema,
      query: optionalStringSchema,
      limit: limitSchema.optional(),
    }),
    resultSchema: z.object({
      searchId: z.string(),
      query: z.string(),
      mediaType: mediaSchema,
      expiresAt: z.string(),
      results: z.array(releaseSearchResultSchema),
    }),
    async handler(ctx, params) {
      const book = ctx.repo.getBookRow(params.bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }
      const query = params.query?.trim() || defaultSearchQuery(book);
      if (!query) {
        throw new RpcError(-32602, "Search query is empty", { error: "invalid_query", bookId: params.bookId });
      }

      const limit = params.limit ?? 50;
      const results = (await runSearch(ctx.repo.getSettings(), { query, media: params.mediaType })).slice(0, limit);
      const expiresAt = new Date(Date.now() + RELEASE_SEARCH_TTL_MS).toISOString();
      ctx.repo.deleteExpiredReleaseSearches(new Date().toISOString());
      const search = ctx.repo.createReleaseSearch({
        id: randomUUID(),
        userId: ctx.session?.user_id ?? null,
        bookId: book.id,
        mediaType: params.mediaType,
        query,
        resultsJson: JSON.stringify(results),
        expiresAt,
      });

      return {
        searchId: search.id,
        query,
        mediaType: params.mediaType,
        expiresAt,
        results: results.map(publicSearchResult),
      };
    },
  }),

  createManifestationFromSearch: defineMethod({
    auth: "user",
    summary: "Create a new manifestation from ordered releases selected from a persisted library.searchReleases result.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
      mediaType: mediaSchema,
      searchId: nonEmptyStringSchema,
      indexes: z.array(releaseSearchIndexSchema).min(1),
      manifestation: z.object({
        label: optionalStringSchema.nullable(),
        editionNote: optionalStringSchema.nullable(),
      }),
    }),
    resultSchema: z.object({
      manifestationId: positiveIntSchema.nullable(),
      results: z.array(
        z.object({
          release: releaseRowSchema,
          jobId: positiveIntSchema,
          idempotent: z.boolean(),
        })
      ),
    }),
    async handler(ctx, params) {
      const book = ctx.repo.getBookRow(params.bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }
      ctx.repo.deleteExpiredReleaseSearches(new Date().toISOString());
      const search = ctx.repo.getReleaseSearch(params.searchId);
      if (!search || search.expires_at <= new Date().toISOString()) {
        throw new RpcError(-32000, "Release search expired or not found", {
          error: "not_found",
          searchId: params.searchId,
        });
      }
      if (search.book_id !== params.bookId || search.media_type !== params.mediaType) {
        throw new RpcError(-32000, "Release search does not match requested book/media", {
          error: "search_mismatch",
          searchId: params.searchId,
          bookId: params.bookId,
          mediaType: params.mediaType,
        });
      }
      if (search.user_id !== null && search.user_id !== ctx.session?.user_id) {
        throw new RpcError(-32003, "Forbidden");
      }

      const seen = new Set<number>();
      for (const index of params.indexes) {
        if (seen.has(index)) {
          throw new RpcError(-32602, "Duplicate release index", { error: "duplicate_index", index });
        }
        seen.add(index);
      }

      let storedResults: Array<z.infer<typeof torznabResultSchema>>;
      try {
        storedResults = z.array(torznabResultSchema).parse(JSON.parse(search.results_json));
      } catch {
        throw new RpcError(-32000, "Stored release search is invalid", { error: "invalid_search", searchId: params.searchId });
      }

      const selected = params.indexes.map((index) => {
        const release = storedResults[index];
        if (!release) {
          throw new RpcError(-32602, "Release index out of range", { error: "index_out_of_range", index });
        }
        return release;
      });

      return await runSnatchGroup(ctx.repo, ctx.repo.getSettings(), {
        bookId: params.bookId,
        mediaType: params.mediaType,
        forceManifestation: true,
        manifestation: {
          label: params.manifestation.label?.trim() || null,
          editionNote: params.manifestation.editionNote?.trim() || null,
        },
        parts: selected.map((release) => ({
          provider: release.provider,
          providerGuid: release.guid ?? null,
          title: release.title,
          url: release.url,
          infoHash: release.infoHash ?? null,
          sizeBytes: release.sizeBytes,
        })),
      });
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
    auth: "user",
    summary: "Report wrong imported file(s), preserve the current edition, and queue async review/reacquire.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
      mediaType: z.enum(["audio", "ebook"]),
      releaseId: optionalPositiveIntSchema,
      manifestationId: optionalPositiveIntSchema,
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
      if (params.manifestationId !== undefined) {
        const manifestation = ctx.repo.getManifestation(params.manifestationId);
        const expectedKind = params.mediaType === "ebook" ? "ebook" : "audio";
        if (!manifestation || manifestation.book_id !== params.bookId || manifestation.kind !== expectedKind) {
          throw new RpcError(-32000, "Manifestation not found for media", {
            error: "not_found",
            bookId: params.bookId,
            mediaType: params.mediaType,
            manifestationId: params.manifestationId,
          });
        }
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
      ).filter((asset) => params.manifestationId === undefined || asset.manifestation_id === params.manifestationId);
      const wrongAssetFiles = wrongAssets.flatMap((asset) => ctx.repo.getAssetFiles(asset.id));
      const rejectedSourcePaths = wrongAssetFiles.map((file) => file.source_path ?? file.path);

      const importJob = ctx.repo.createJob({
        type: "import",
        bookId: book.id,
        releaseId: release.id,
        payload: {
          reason: "user_reported_wrong_file",
          userReportedIssue: true,
          rejectedSourcePaths,
          rejectedManifestationId: params.manifestationId ?? null,
        },
      });
      return {
        action: "wrong_file_review_queued" as const,
        jobId: importJob.id,
        releaseId: release.id,
        mediaType: params.mediaType,
        rejectedSourcePathsCount: rejectedSourcePaths.length,
        deletedAssetCount: 0,
        deletedAssetFileCount: 0,
        deletedAssetPaths: [],
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

  transcriptStatus: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "Report transcript state for a book audio manifestation.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
      manifestationId: optionalPositiveIntSchema,
    }),
    resultSchema: z.object({
      status: z.enum(["current", "stale", "pending", "running", "failed", "missing_audio", "missing_config"]),
      fingerprint: z.string().nullable(),
      currentFingerprint: z.string().nullable(),
      jobId: positiveIntSchema.nullable(),
      error: z.string().nullable(),
    }),
    async handler(ctx, params) {
      const book = ctx.repo.getBookRow(params.bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }
      if (params.manifestationId !== undefined) {
        const manifestation = ctx.repo.getManifestation(params.manifestationId);
        if (!manifestation || manifestation.book_id !== params.bookId || manifestation.kind !== "audio") {
          throw new RpcError(-32000, "Audio manifestation not found", {
            error: "not_found",
            bookId: params.bookId,
            manifestationId: params.manifestationId,
          });
        }
      }
      const apiKeyConfigured = Boolean(ctx.repo.getSettings().agents.apiKey.trim());
      return await getBookTranscriptStatus(ctx.repo, params.bookId, { apiKeyConfigured, manifestationId: params.manifestationId });
    },
  }),

  requestTranscription: defineMethod({
    auth: "user",
    summary: "Pull-trigger transcription for a book audio manifestation. Idempotent; returns current status.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
      manifestationId: optionalPositiveIntSchema,
    }),
    resultSchema: z.object({
      status: z.enum(["current", "stale", "pending", "running", "failed", "missing_audio", "missing_config"]),
      fingerprint: z.string().nullable(),
      currentFingerprint: z.string().nullable(),
      jobId: positiveIntSchema.nullable(),
      error: z.string().nullable(),
    }),
    async handler(ctx, params) {
      const book = ctx.repo.getBookRow(params.bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }
      if (params.manifestationId !== undefined) {
        const manifestation = ctx.repo.getManifestation(params.manifestationId);
        if (!manifestation || manifestation.book_id !== params.bookId || manifestation.kind !== "audio") {
          throw new RpcError(-32000, "Audio manifestation not found", {
            error: "not_found",
            bookId: params.bookId,
            manifestationId: params.manifestationId,
          });
        }
      }
      const apiKeyConfigured = Boolean(ctx.repo.getSettings().agents.apiKey.trim());
      return await requestBookTranscription(ctx.repo, params.bookId, { apiKeyConfigured, manifestationId: params.manifestationId });
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
      deletedTranscriptFileCount: z.number().int().nonnegative(),
      deletedTranscriptPaths: z.array(z.string()),
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
      const deletedTranscriptPaths: string[] = [];
      for (const filePath of artifacts.transcriptPaths) {
        if (await removeFileIfPresent(filePath)) {
          deletedTranscriptPaths.push(filePath);
        }
      }
      const deletedCoverPath = artifacts.coverPath ? ((await removeFileIfPresent(artifacts.coverPath)) ? artifacts.coverPath : null) : null;

      return {
        deletedBookId: params.bookId,
        deletedAssetFileCount: deletedAssetPaths.length,
        deletedAssetPaths,
        deletedTranscriptFileCount: deletedTranscriptPaths.length,
        deletedTranscriptPaths,
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
