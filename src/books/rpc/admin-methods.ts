import path from "node:path";

import { selectManualImportPaths, selectSearchCandidate } from "../agents";
import { hydrateBookFromOpenLibrary } from "../hydration";
import { importReleaseFromPath, inspectImportPath } from "../importer";
import { RtorrentClient } from "../rtorrent";
import { runSearch, runSnatch, triggerAutoAcquire } from "../service";
import type { AppSettings } from "../types";

import {
  RpcError,
  asOptionalBoolean,
  asOptionalPositiveInt,
  asOptionalString,
  asOptionalStringArray,
  asPositiveInt,
  asString,
  enrichDownload,
  enrichJob,
  parseJobType,
  parseLimit,
  parseMedia,
  parseMediaSelection,
  removeFileIfPresent,
  uniqueManualInfoHash,
  type RpcMethodDefinition,
} from "./shared";

export const adminRpcMethods: Record<string, RpcMethodDefinition> = {
  "settings.get": {
    auth: "admin",
    readOnly: true,
    summary: "Read current application settings.",
    async handler(ctx) {
      return ctx.repo.getSettings();
    },
  },

  "settings.update": {
    auth: "admin",
    summary: "Replace application settings.",
    async handler(ctx, params) {
      return ctx.repo.updateSettings(params as unknown as AppSettings);
    },
  },

  "admin.wipeDatabase": {
    auth: "admin",
    summary: "Delete all mutable DB data and imported files/covers (local dev reset).",
    async handler(ctx) {
      const artifacts = ctx.repo.getWipeArtifacts();
      const wiped = ctx.repo.wipeDatabase();

      const deletedAssetPaths: string[] = [];
      for (const filePath of artifacts.assetPaths) {
        if (await removeFileIfPresent(filePath)) {
          deletedAssetPaths.push(filePath);
        }
      }

      const deletedCoverPaths: string[] = [];
      for (const filePath of artifacts.coverPaths) {
        if (await removeFileIfPresent(filePath)) {
          deletedCoverPaths.push(filePath);
        }
      }

      return {
        ...wiped,
        deletedAssetFileCount: deletedAssetPaths.length,
        deletedAssetPaths,
        deletedCoverFileCount: deletedCoverPaths.length,
        deletedCoverPaths,
      };
    },
  },

  "library.refresh": {
    auth: "admin",
    summary: "Queue full library filesystem refresh scan.",
    async handler(ctx) {
      const job = ctx.repo.createJob({
        type: "full_library_refresh",
      });
      return { jobId: job.id };
    },
  },

  "library.reportImportIssue": {
    auth: "admin",
    summary: "Report wrong imported file(s), delete imported asset(s), and queue async review/reacquire.",
    async handler(ctx, params) {
      const bookId = asPositiveInt(params.bookId, "bookId");
      const mediaType = parseMedia(params.mediaType);
      const releaseId = asOptionalPositiveInt(params.releaseId, "releaseId");
      const book = ctx.repo.getBookRow(bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId });
      }

      const releases = ctx.repo.listReleasesByBook(bookId).filter((release) => release.media_type === mediaType);
      const assets = ctx.repo.listAssetsByBook(bookId);
      const release = (() => {
        if (releaseId !== undefined) {
          return releases.find((candidate) => candidate.id === releaseId) ?? null;
        }

        const mediaAsset = assets.find((asset) => {
          if (mediaType === "ebook") return asset.kind === "ebook";
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
        throw new RpcError(-32000, "Release not found for media", { error: "not_found", bookId, mediaType, releaseId });
      }
      const wrongAssets = assets.filter((asset) => asset.source_release_id === release.id).filter((asset) =>
        mediaType === "ebook" ? asset.kind === "ebook" : asset.kind !== "ebook"
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
        action: "wrong_file_review_queued",
        jobId: importJob.id,
        releaseId: release.id,
        mediaType,
        rejectedSourcePathsCount: rejectedSourcePaths.length,
        deletedAssetCount: wrongAssets.length,
        deletedAssetFileCount: deletedAssetPaths.length,
        deletedAssetPaths,
      };
    },
  },

  "library.rehydrate": {
    auth: "admin",
    summary: "Re-run Open Library metadata hydration for one/all books.",
    async handler(ctx, params) {
      const targetBookId = params.bookId === undefined ? null : asPositiveInt(params.bookId, "bookId");
      const books = targetBookId === null ? ctx.repo.listAllBooks() : [ctx.repo.getBook(targetBookId)];
      const resolved = books.filter((book): book is NonNullable<typeof book> => Boolean(book));
      if (targetBookId !== null && resolved.length === 0) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: targetBookId });
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
  },

  "library.delete": {
    auth: "admin",
    summary: "Delete a book, cascading DB rows and imported files/covers.",
    async handler(ctx, params) {
      const bookId = asPositiveInt(params.bookId, "bookId");
      const book = ctx.repo.getBookRow(bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId });
      }

      const artifacts = ctx.repo.getBookDeleteArtifacts(bookId);
      const deleted = ctx.repo.deleteBook(bookId);
      if (!deleted) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId });
      }

      const deletedAssetPaths: string[] = [];
      for (const filePath of artifacts.assetPaths) {
        if (await removeFileIfPresent(filePath)) {
          deletedAssetPaths.push(filePath);
        }
      }

      const deletedCoverPath = artifacts.coverPath ? ((await removeFileIfPresent(artifacts.coverPath)) ? artifacts.coverPath : null) : null;

      return {
        deletedBookId: bookId,
        deletedAssetFileCount: deletedAssetPaths.length,
        deletedAssetPaths,
        deletedCoverPath,
      };
    },
  },

  "search.run": {
    auth: "admin",
    readOnly: true,
    summary: "Run Torznab search and return normalized results.",
    async handler(ctx, params) {
      const query = asString(params.query, "query").trim();
      const media = parseMedia(params.media);
      const results = await runSearch(ctx.repo.getSettings(), {
        query,
        media,
      });
      return { results };
    },
  },

  "agent.search.plan": {
    auth: "admin",
    readOnly: true,
    summary: "Run search selection planning (deterministic/agent) without snatching.",
    async handler(ctx, params) {
      const query = asString(params.query, "query").trim();
      const media = parseMedia(params.media);
      const bookId = asOptionalPositiveInt(params.bookId, "bookId");
      const forceAgent = asOptionalBoolean(params.forceAgent, "forceAgent") ?? false;
      const priorFailure = asOptionalBoolean(params.priorFailure, "priorFailure") ?? false;
      const rejectedUrls = asOptionalStringArray(params.rejectedUrls, "rejectedUrls") ?? [];
      const book =
        bookId === undefined
          ? null
          : (() => {
              const row = ctx.repo.getBookRow(bookId);
              if (!row) {
                throw new RpcError(-32000, "Book not found", { error: "not_found", bookId });
              }
              return { id: row.id, title: row.title, author: row.author };
            })();
      const results = await runSearch(ctx.repo.getSettings(), { query, media });
      const decision = await selectSearchCandidate(ctx.repo.getSettings(), {
        query,
        media,
        results,
        rejectedUrls,
        forceAgent,
        priorFailure,
        book,
      }, {
        repo: ctx.repo,
      });
      return {
        resultCount: results.length,
        decision,
      };
    },
  },

  "snatch.create": {
    auth: "admin",
    summary: "Create release + download job for a chosen search result.",
    async handler(ctx, params) {
      const bookId = asPositiveInt(params.bookId, "bookId");
      const provider = asString(params.provider, "provider");
      const title = asString(params.title, "title");
      const mediaType = parseMedia(params.mediaType);
      const url = asString(params.url, "url");
      const infoHash = asOptionalString(params.infoHash);
      const guid = asOptionalString(params.guid);
      const sizeBytes =
        params.sizeBytes === undefined || params.sizeBytes === null
          ? null
          : typeof params.sizeBytes === "number"
            ? Math.trunc(params.sizeBytes)
            : Number.parseInt(String(params.sizeBytes), 10);

      return runSnatch(ctx.repo, ctx.repo.getSettings(), {
        bookId,
        provider,
        providerGuid: guid,
        title,
        mediaType,
        url,
        infoHash,
        sizeBytes: Number.isFinite(sizeBytes) ? sizeBytes : null,
      });
    },
  },

  "downloads.list": {
    auth: "admin",
    readOnly: true,
    summary: "List download jobs with release status/progress.",
    async handler(ctx) {
      const downloads = ctx.repo.listDownloads();
      const hasDownloading = downloads.some((download) => download.release_status === "downloading" && download.info_hash);
      const client = hasDownloading ? new RtorrentClient(ctx.repo.getSettings().rtorrent) : null;
      const enriched = await Promise.all(downloads.map((download) => enrichDownload(download, client)));
      return { downloads: enriched };
    },
  },

  "downloads.get": {
    auth: "admin",
    readOnly: true,
    summary: "Get one download job with live progress if active.",
    async handler(ctx, params) {
      const jobId = asPositiveInt(params.jobId, "jobId");
      const download = ctx.repo.getDownload(jobId);
      if (!download) {
        throw new RpcError(-32000, "Download not found", { error: "not_found", jobId });
      }
      const client =
        download.release_status === "downloading" && download.info_hash
          ? new RtorrentClient(ctx.repo.getSettings().rtorrent)
          : null;
      return enrichDownload(download, client);
    },
  },

  "downloads.retry": {
    auth: "admin",
    summary: "Requeue a download job.",
    async handler(ctx, params) {
      const jobId = asPositiveInt(params.jobId, "jobId");
      return { job: ctx.repo.retryJob(jobId) };
    },
  },

  "jobs.list": {
    auth: "admin",
    readOnly: true,
    summary: "List recent jobs (optionally filtered by type).",
    async handler(ctx, params) {
      const limit = parseLimit(params.limit);
      const type = params.type === undefined ? undefined : parseJobType(params.type);
      const jobs = type
        ? ctx.repo.listJobsByType(type)
        : (["full_library_refresh", "acquire", "download", "import", "reconcile", "chapter_analysis"] as const).flatMap((jobType) =>
            ctx.repo.listJobsByType(jobType)
          );
      return {
        jobs: jobs
          .sort((a, b) => b.id - a.id)
          .slice(0, limit)
          .map((job) => enrichJob(ctx, job)),
      };
    },
  },

  "jobs.get": {
    auth: "admin",
    readOnly: true,
    summary: "Get one job row.",
    async handler(ctx, params) {
      const jobId = asPositiveInt(params.jobId, "jobId");
      const job = ctx.repo.getJob(jobId);
      if (!job) {
        throw new RpcError(-32000, "Job not found", { error: "not_found", jobId });
      }
      return { job: enrichJob(ctx, job) };
    },
  },

  "jobs.retry": {
    auth: "admin",
    summary: "Retry a failed/cancelled job.",
    async handler(ctx, params) {
      const jobId = asPositiveInt(params.jobId, "jobId");
      const job = ctx.repo.getJob(jobId);
      if (!job) {
        throw new RpcError(-32000, "Job not found", { error: "not_found", jobId });
      }
      if (job.status !== "failed" && job.status !== "cancelled") {
        throw new RpcError(-32000, "Job is not retryable", {
          error: "not_retryable",
          jobId,
          status: job.status,
        });
      }
      return { job: ctx.repo.retryJob(jobId) };
    },
  },

  "import.reconcile": {
    auth: "admin",
    summary: "Queue reconcile job for downloaded releases missing assets.",
    async handler(ctx) {
      const job = ctx.repo.createJob({ type: "reconcile" });
      return { jobId: job.id };
    },
  },

  "import.inspect": {
    auth: "admin",
    readOnly: true,
    summary: "Inspect a local path and list candidate import files.",
    async handler(_ctx, params) {
      const sourcePath = asString(params.path, "path").trim();
      return {
        path: sourcePath,
        files: await inspectImportPath(sourcePath),
      };
    },
  },

  "agent.import.plan": {
    auth: "admin",
    readOnly: true,
    summary: "Run import-file selection planning (deterministic/agent) without importing.",
    async handler(ctx, params) {
      const sourcePath = asString(params.path, "path").trim();
      const mediaType = parseMedia(params.mediaType);
      const bookId = asOptionalPositiveInt(params.bookId, "bookId");
      const forceAgent = asOptionalBoolean(params.forceAgent, "forceAgent") ?? false;
      const priorFailure = asOptionalBoolean(params.priorFailure, "priorFailure") ?? false;
      const book =
        bookId === undefined
          ? null
          : (() => {
              const row = ctx.repo.getBookRow(bookId);
              if (!row) {
                throw new RpcError(-32000, "Book not found", { error: "not_found", bookId });
              }
              return { id: row.id, title: row.title, author: row.author };
            })();
      const files = await inspectImportPath(sourcePath);
      const decision = await selectManualImportPaths(ctx.repo.getSettings(), {
        mediaType,
        files,
        forceAgent,
        priorFailure,
        book,
      });
      return {
        path: sourcePath,
        fileCount: files.length,
        files,
        decision,
      };
    },
  },

  "import.manual": {
    auth: "admin",
    summary: "Create a manual release and import from a local path.",
    async handler(ctx, params) {
      const bookId = asPositiveInt(params.bookId, "bookId");
      const mediaType = parseMedia(params.mediaType);
      const sourcePath = asString(params.path, "path").trim();
      const selectedPaths = asOptionalStringArray(params.selectedPaths, "selectedPaths");
      const title = asOptionalString(params.title)?.trim() || path.basename(sourcePath);

      const book = ctx.repo.getBookRow(bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId });
      }

      const release = ctx.repo.createRelease({
        bookId,
        provider: "manual",
        providerGuid: null,
        title,
        mediaType,
        infoHash: uniqueManualInfoHash(bookId, mediaType, sourcePath),
        sizeBytes: null,
        url: sourcePath,
        status: "downloaded",
      });

      try {
        const imported = await importReleaseFromPath(ctx.repo, release, sourcePath, ctx.repo.getSettings().libraryRoot, {
          selectedPaths,
        });
        const finalRelease = ctx.repo.setReleaseStatus(release.id, "imported", null);
        return {
          release: finalRelease,
          assetId: imported.assetId,
          linkedFiles: imported.linkedFiles,
        };
      } catch (error) {
        const message = (error as Error).message || "Manual import failed";
        ctx.repo.setReleaseStatus(release.id, "failed", message);
        throw new RpcError(-32000, "Manual import failed", { message, sourcePath, mediaType });
      }
    },
  },
};
