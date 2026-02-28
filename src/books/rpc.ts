import { createHash } from "node:crypto";
import { rm } from "node:fs/promises";
import path from "node:path";

import { selectManualImportPaths, selectSearchCandidate } from "./agents";
import { hydrateBookFromOpenLibrary } from "./hydration";
import { importReleaseFromPath, inspectImportPath } from "./importer";
import { resolveOpenLibraryCandidate, searchOpenLibrary } from "./openlibrary";
import { computeDownloadFraction, pseudoProgressForMediaStatus, pseudoProgressForRelease } from "./progress";
import { BooksRepo } from "./repo";
import { RtorrentClient } from "./rtorrent";
import { runSearch, runSnatch, triggerAutoAcquire } from "./service";
import type { AppSettings, JobType, LibraryBook, MediaType, ReleaseRow } from "./types";

// JSON-RPC v1 transport for Kindling control/data APIs.
// Single-call requests only; batch mode is intentionally rejected.
type RpcId = string | number | null;

type RpcRequest = {
  jsonrpc: "2.0";
  id: RpcId;
  method: string;
  params?: Record<string, unknown>;
};

type RpcErrorPayload = {
  code: number;
  message: string;
  data?: unknown;
};

type RpcSuccess = {
  jsonrpc: "2.0";
  id: RpcId;
  result: unknown;
};

type RpcFailure = {
  jsonrpc: "2.0";
  id: RpcId;
  error: RpcErrorPayload;
};

type RpcMethodHandler = (ctx: RpcContext, params: Record<string, unknown>) => Promise<unknown>;

type RpcContext = {
  repo: BooksRepo;
  startTime: number;
};

type RpcDispatchOptions = {
  id?: RpcId;
  readOnly?: boolean;
};

class RpcError extends Error {
  constructor(
    readonly code: number,
    message: string,
    readonly data?: unknown
  ) {
    super(message);
    this.name = "RpcError";
  }
}

type DownloadProgress = {
  bytesDone: number | null;
  sizeBytes: number | null;
  leftBytes: number | null;
  downRate: number | null;
  fraction: number | null;
  percent: number | null;
};

type DownloadRpcView = ReturnType<BooksRepo["listDownloads"]>[number] & {
  fullPseudoProgress: number;
  downloadProgress?: DownloadProgress;
};

async function removeFileIfPresent(filePath: string): Promise<boolean> {
  try {
    await rm(filePath, { force: true });
    return true;
  } catch (error) {
    throw new Error(`Failed to remove file ${filePath}: ${(error as Error).message}`);
  }
}

async function enrichDownload(
  download: ReturnType<BooksRepo["listDownloads"]>[number],
  client: RtorrentClient | null
): Promise<DownloadRpcView> {
  if (download.release_status !== "downloading" || !download.info_hash || !client) {
    return {
      ...download,
      fullPseudoProgress: pseudoProgressForRelease(download.release_status),
    };
  }

  try {
    const state = await client.getDownloadState(download.info_hash);
    const fraction = computeDownloadFraction({
      bytesDone: state.bytesDone,
      sizeBytes: state.sizeBytes,
      leftBytes: state.leftBytes,
    });
    return {
      ...download,
      fullPseudoProgress: pseudoProgressForRelease(download.release_status, fraction),
      downloadProgress: {
        bytesDone: state.bytesDone,
        sizeBytes: state.sizeBytes,
        leftBytes: state.leftBytes,
        downRate: state.downRate,
        fraction,
        percent: fraction === null ? null : Math.round(fraction * 100),
      },
    };
  } catch {
    // Don't fail list/get when downloader telemetry is transiently unavailable.
    return {
      ...download,
      fullPseudoProgress: pseudoProgressForRelease(download.release_status),
    };
  }
}

function mediaPseudoProgress(status: LibraryBook["audioStatus"] | LibraryBook["ebookStatus"], fraction?: number | null): number {
  if (status === "downloading") {
    return pseudoProgressForRelease("downloading", fraction);
  }
  return pseudoProgressForMediaStatus(status);
}

async function liveFractionForMedia(
  releases: ReleaseRow[],
  mediaType: MediaType,
  client: RtorrentClient | null
): Promise<number | null> {
  if (!client) return null;
  const downloading = releases.filter(
    (release) => release.media_type === mediaType && release.status === "downloading" && Boolean(release.info_hash)
  );
  if (downloading.length === 0) return null;

  let best: number | null = null;
  for (const release of downloading) {
    try {
      const state = await client.getDownloadState(release.info_hash);
      const fraction = computeDownloadFraction({
        bytesDone: state.bytesDone,
        sizeBytes: state.sizeBytes,
        leftBytes: state.leftBytes,
      });
      if (fraction !== null) {
        best = best === null ? fraction : Math.max(best, fraction);
      }
    } catch {
      // Ignore transient downloader telemetry errors and keep persisted progress.
    }
  }
  return best;
}

async function enrichLibraryBookProgress(
  repo: BooksRepo,
  book: LibraryBook,
  client: RtorrentClient | null
): Promise<LibraryBook> {
  if (book.audioStatus !== "downloading" && book.ebookStatus !== "downloading") {
    return book;
  }

  const releases = repo.listReleasesByBook(book.id);
  const [audioFraction, ebookFraction] = await Promise.all([
    liveFractionForMedia(releases, "audio", client),
    liveFractionForMedia(releases, "ebook", client),
  ]);

  const liveBookProgress =
    (mediaPseudoProgress(book.audioStatus, audioFraction) + mediaPseudoProgress(book.ebookStatus, ebookFraction)) / 2;

  if (liveBookProgress === book.fullPseudoProgress) {
    return book;
  }
  return {
    ...book,
    fullPseudoProgress: liveBookProgress,
  };
}

function response(payload: RpcSuccess | RpcFailure): Response {
  return new Response(JSON.stringify(payload, null, 2), {
    status: 200,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

function success(id: RpcId, result: unknown): Response {
  return response({
    jsonrpc: "2.0",
    id,
    result,
  });
}

function failure(id: RpcId, code: number, message: string, data?: unknown): Response {
  return response({
    jsonrpc: "2.0",
    id,
    error: { code, message, ...(data === undefined ? {} : { data }) },
  });
}

function asObject(value: unknown, errorCode: number, errorMessage: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new RpcError(errorCode, errorMessage);
  }
  return value as Record<string, unknown>;
}

function asString(value: unknown, name: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new RpcError(-32602, `${name} is required`);
  }
  return value;
}

function asOptionalString(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string") {
    throw new RpcError(-32602, "Invalid string value");
  }
  return value;
}

function asOptionalStringArray(value: unknown, name: string): string[] | undefined {
  if (value === undefined || value === null) return undefined;
  if (!Array.isArray(value)) {
    throw new RpcError(-32602, `${name} must be an array of strings`);
  }
  const out: string[] = [];
  for (const item of value) {
    if (typeof item !== "string" || !item.trim()) {
      throw new RpcError(-32602, `${name} must contain non-empty strings`);
    }
    out.push(item.trim());
  }
  return out;
}

function asOptionalPositiveIntArray(value: unknown, name: string): number[] | undefined {
  if (value === undefined || value === null) return undefined;
  if (!Array.isArray(value)) {
    throw new RpcError(-32602, `${name} must be an array of positive integers`);
  }
  const out: number[] = [];
  for (const item of value) {
    out.push(asPositiveInt(item, `${name}[]`));
  }
  return out;
}

function asOptionalBoolean(value: unknown, name: string): boolean | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    if (value === "true") return true;
    if (value === "false") return false;
  }
  throw new RpcError(-32602, `${name} must be a boolean`);
}

function asOptionalPositiveInt(value: unknown, name: string): number | undefined {
  if (value === undefined || value === null) return undefined;
  return asPositiveInt(value, name);
}

function asPositiveInt(value: unknown, name: string): number {
  const n = typeof value === "number" ? value : typeof value === "string" ? Number.parseInt(value, 10) : NaN;
  if (!Number.isInteger(n) || n <= 0) {
    throw new RpcError(-32602, `${name} must be a positive integer`);
  }
  return n;
}

function parseLimit(value: unknown): number {
  if (value === undefined || value === null) return 50;
  const parsed = typeof value === "number" ? Math.trunc(value) : Number.parseInt(String(value), 10);
  if (!Number.isInteger(parsed) || parsed <= 0) return 50;
  return Math.min(parsed, 200);
}

function parseMedia(value: unknown): MediaType {
  if (value === "audio" || value === "ebook") {
    return value;
  }
  throw new RpcError(-32602, "media must be audio or ebook");
}

function parseMediaSelection(value: unknown): MediaType[] {
  if (value === undefined || value === null) {
    return ["audio", "ebook"];
  }
  if (Array.isArray(value)) {
    if (value.length === 0) {
      throw new RpcError(-32602, "media must include at least one value");
    }
    return Array.from(new Set(value.map((item) => parseMedia(item))));
  }
  return [parseMedia(value)];
}

function parseJobType(value: unknown): JobType {
  if (
    value === "full_library_refresh" ||
    value === "acquire" ||
    value === "download" ||
    value === "import" ||
    value === "reconcile"
  ) {
    return value;
  }
  throw new RpcError(-32602, "type must be one of full_library_refresh|acquire|download|import|reconcile");
}

function uniqueManualInfoHash(bookId: number, mediaType: MediaType, sourcePath: string): string {
  return createHash("sha1")
    .update(`manual:${bookId}:${mediaType}:${sourcePath}:${Date.now()}:${Math.random()}`)
    .digest("hex");
}

function parseRequest(raw: unknown): RpcRequest {
  if (Array.isArray(raw)) {
    throw new RpcError(-32600, "Batch requests are not supported");
  }
  const object = asObject(raw, -32600, "Invalid Request");

  if (object.jsonrpc !== "2.0") {
    throw new RpcError(-32600, "jsonrpc must be \"2.0\"");
  }
  if (typeof object.method !== "string" || !object.method.trim()) {
    throw new RpcError(-32600, "method must be a non-empty string");
  }
  if (!("id" in object)) {
    throw new RpcError(-32600, "id is required");
  }
  const id = object.id;
  if (!(id === null || typeof id === "string" || typeof id === "number")) {
    throw new RpcError(-32600, "id must be string, number, or null");
  }
  if ("params" in object && object.params !== undefined) {
    asObject(object.params, -32600, "params must be an object");
  }

  return {
    jsonrpc: "2.0",
    id,
    method: object.method,
    params: (object.params as Record<string, unknown> | undefined) ?? {},
  };
}

const handlers: Record<string, RpcMethodHandler> = {
  async help() {
    const methods = Object.keys(handlers)
      .sort()
      .map((name) => ({
        name,
        readOnly: readOnlyMethods.has(name),
        description: methodSummaries[name] ?? null,
      }));
    return {
      name: "podible-rpc",
      version: "v1",
      methodCount: methods.length,
      methods,
    };
  },

  async "system.health"(ctx) {
    return {
      ok: true,
      ...ctx.repo.getHealthSummary(),
    };
  },

  async "system.server"(ctx) {
    return {
      name: "podible-backend",
      runtime: "bun",
      uptimeMs: Date.now() - ctx.startTime,
      now: new Date().toISOString(),
    };
  },

  async "settings.get"(ctx) {
    return ctx.repo.getSettings();
  },

  async "settings.update"(ctx, params) {
    return ctx.repo.updateSettings(params as unknown as AppSettings);
  },

  async "admin.wipeDatabase"(ctx) {
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

  async "openlibrary.search"(_ctx, params) {
    const query = asString(params.q, "q").trim();
    const limit = Math.min(parseLimit(params.limit), 50);
    const results = await searchOpenLibrary(query, limit);
    return { results };
  },

  async "library.list"(ctx, params) {
    const limit = parseLimit(params.limit);
    const cursor = params.cursor === undefined ? undefined : asPositiveInt(params.cursor, "cursor");
    const q = asOptionalString(params.q);
    return ctx.repo.listBooks(limit, cursor, q);
  },

  async "library.get"(ctx, params) {
    const bookId = asPositiveInt(params.bookId, "bookId");
    const book = ctx.repo.getBook(bookId);
    if (!book) {
      throw new RpcError(-32000, "Book not found", { error: "not_found", bookId });
    }
    const bookClient =
      book.audioStatus === "downloading" || book.ebookStatus === "downloading"
        ? new RtorrentClient(ctx.repo.getSettings().rtorrent)
        : null;
    const enrichedBook = await enrichLibraryBookProgress(ctx.repo, book, bookClient);
    return {
      book: enrichedBook,
      releases: ctx.repo.listReleasesByBook(bookId),
      assets: ctx.repo.listAssetsByBook(bookId),
    };
  },

  async "library.inProgress"(ctx, params) {
    const bookIds = asOptionalPositiveIntArray(params.bookIds, "bookIds");
    const items = ctx.repo.listInProgressBooks(bookIds);
    const hasDownloading = items.some((book) => book.audioStatus === "downloading" || book.ebookStatus === "downloading");
    const client = hasDownloading ? new RtorrentClient(ctx.repo.getSettings().rtorrent) : null;
    return {
      items: await Promise.all(items.map((book) => enrichLibraryBookProgress(ctx.repo, book, client))),
    };
  },

  async "library.create"(ctx, params) {
    const openLibraryKey = asString(params.openLibraryKey, "openLibraryKey").trim();
    const resolved = await resolveOpenLibraryCandidate({ openLibraryKey });
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

  async "library.refresh"(ctx) {
    const job = ctx.repo.createJob({
      type: "full_library_refresh",
    });
    return { jobId: job.id };
  },

  async "library.acquire"(ctx, params) {
    const bookId = asPositiveInt(params.bookId, "bookId");
    const book = ctx.repo.getBookRow(bookId);
    if (!book) {
      throw new RpcError(-32000, "Book not found", { error: "not_found", bookId });
    }
    const media = parseMediaSelection(params.media);
    const forceAgent = asOptionalBoolean(params.forceAgent, "forceAgent") ?? false;
    const priorFailure = asOptionalBoolean(params.priorFailure, "priorFailure") ?? false;
    const rejectedUrls = asOptionalStringArray(params.rejectedUrls, "rejectedUrls") ?? [];
    const rejectedGuids = asOptionalStringArray(params.rejectedGuids, "rejectedGuids") ?? [];
    const rejectedInfoHashes = asOptionalStringArray(params.rejectedInfoHashes, "rejectedInfoHashes") ?? [];
    const jobId = await triggerAutoAcquire(ctx.repo, bookId, media, {
      forceAgent,
      priorFailure,
      rejectedUrls,
      rejectedGuids,
      rejectedInfoHashes,
    });
    return { jobId, media, forceAgent, priorFailure, rejectedUrls, rejectedGuids, rejectedInfoHashes };
  },

  async "library.reportImportIssue"(ctx, params) {
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

  async "library.rehydrate"(ctx, params) {
    const targetBookId = params.bookId === undefined ? null : asPositiveInt(params.bookId, "bookId");
    const books = targetBookId === null ? ctx.repo.listAllBooks() : [ctx.repo.getBook(targetBookId)];
    const resolved = books.filter((book): book is LibraryBook => Boolean(book));
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

  async "library.delete"(ctx, params) {
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

  async "search.run"(ctx, params) {
    const query = asString(params.query, "query").trim();
    const media = parseMedia(params.media);
    const results = await runSearch(ctx.repo.getSettings(), {
      query,
      media,
    });
    return { results };
  },

  async "agent.search.plan"(ctx, params) {
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

  async "snatch.create"(ctx, params) {
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

  async "releases.list"(ctx, params) {
    const bookId = asPositiveInt(params.bookId, "bookId");
    return { releases: ctx.repo.listReleasesByBook(bookId) };
  },

  async "downloads.list"(ctx) {
    const downloads = ctx.repo.listDownloads();
    const hasDownloading = downloads.some((download) => download.release_status === "downloading" && download.info_hash);
    const client = hasDownloading ? new RtorrentClient(ctx.repo.getSettings().rtorrent) : null;
    const enriched = await Promise.all(downloads.map((download) => enrichDownload(download, client)));
    return { downloads: enriched };
  },

  async "downloads.get"(ctx, params) {
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

  async "downloads.retry"(ctx, params) {
    const jobId = asPositiveInt(params.jobId, "jobId");
    return { job: ctx.repo.retryJob(jobId) };
  },

  async "jobs.list"(ctx, params) {
    const limit = parseLimit(params.limit);
    const type = params.type === undefined ? undefined : parseJobType(params.type);
    const jobs = type
      ? ctx.repo.listJobsByType(type)
      : (["full_library_refresh", "acquire", "download", "import", "reconcile"] as JobType[]).flatMap((jobType) =>
          ctx.repo.listJobsByType(jobType)
        );
    return {
      jobs: jobs
        .sort((a, b) => b.id - a.id)
        .slice(0, limit),
    };
  },

  async "jobs.get"(ctx, params) {
    const jobId = asPositiveInt(params.jobId, "jobId");
    const job = ctx.repo.getJob(jobId);
    if (!job) {
      throw new RpcError(-32000, "Job not found", { error: "not_found", jobId });
    }
    return { job };
  },

  async "jobs.retry"(ctx, params) {
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

  async "import.reconcile"(ctx) {
    const job = ctx.repo.createJob({ type: "reconcile" });
    return { jobId: job.id };
  },

  async "import.inspect"(_ctx, params) {
    const sourcePath = asString(params.path, "path").trim();
    return {
      path: sourcePath,
      files: await inspectImportPath(sourcePath),
    };
  },

  async "agent.import.plan"(ctx, params) {
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

  async "import.manual"(ctx, params) {
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
};

const methodSummaries: Record<string, string> = {
  help: "List available RPC methods with read-only flags and short descriptions.",
  "system.health": "Service health summary (job/release counts and queue size).",
  "system.server": "Server runtime metadata (name, runtime, uptime, time).",
  "settings.get": "Read current application settings.",
  "settings.update": "Replace application settings.",
  "admin.wipeDatabase": "Delete all mutable DB data and imported files/covers (local dev reset).",
  "openlibrary.search": "Search Open Library for works to add.",
  "library.list": "List books in the library.",
  "library.get": "Get one book with releases and assets.",
  "library.inProgress": "List non-terminal LibraryBook rows (optionally filtered by bookIds).",
  "library.create": "Create a book from an Open Library work key and queue auto-acquire.",
  "library.refresh": "Queue full library filesystem refresh scan.",
  "library.acquire": "Queue targeted acquire job for a book/media set.",
  "library.reportImportIssue": "Report wrong imported file(s), delete imported asset(s), and queue async review/reacquire.",
  "library.rehydrate": "Re-run Open Library metadata hydration for one/all books.",
  "library.delete": "Delete a book, cascading DB rows and imported files/covers.",
  "search.run": "Run Torznab search and return normalized results.",
  "agent.search.plan": "Run search selection planning (deterministic/agent) without snatching.",
  "snatch.create": "Create release + download job for a chosen search result.",
  "releases.list": "List releases for a book.",
  "downloads.list": "List download jobs with release status/progress.",
  "downloads.get": "Get one download job with live progress if active.",
  "downloads.retry": "Requeue a download job.",
  "jobs.list": "List recent jobs (optionally filtered by type).",
  "jobs.get": "Get one job row.",
  "jobs.retry": "Retry a failed/cancelled job.",
  "import.reconcile": "Queue reconcile job for downloaded releases missing assets.",
  "import.inspect": "Inspect a local path and list candidate import files.",
  "agent.import.plan": "Run import-file selection planning (deterministic/agent) without importing.",
  "import.manual": "Create a manual release and import from a local path.",
};

const readOnlyMethods = new Set<string>([
  "help",
  "system.health",
  "system.server",
  "settings.get",
  "openlibrary.search",
  "library.list",
  "library.get",
  "library.inProgress",
  "search.run",
  "agent.search.plan",
  "releases.list",
  "downloads.list",
  "downloads.get",
  "jobs.list",
  "jobs.get",
  "agent.import.plan",
  "import.inspect",
]);

async function dispatchRpcMethod(
  methodName: string,
  params: Record<string, unknown>,
  ctx: RpcContext,
  options: RpcDispatchOptions = {}
): Promise<Response> {
  const id = options.id ?? null;
  try {
    const method = handlers[methodName];
    if (!method || (options.readOnly && !readOnlyMethods.has(methodName))) {
      throw new RpcError(-32601, "Method not found");
    }
    const result = await method(ctx, params);
    return success(id, result);
  } catch (error) {
    if (error instanceof RpcError) {
      return failure(id, error.code, error.message, error.data);
    }
    const message = (error as Error).message;
    return failure(id, -32000, message || "Application error", { message });
  }
}

export async function handleRpcRequest(request: Request, ctx: RpcContext): Promise<Response> {
  let parsed: RpcRequest | null = null;
  try {
    const body = await request.text();
    const payload = JSON.parse(body);
    parsed = parseRequest(payload);
  } catch (error) {
    if (error instanceof SyntaxError) {
      return failure(null, -32700, "Parse error");
    }
    if (error instanceof RpcError) {
      return failure(parsed?.id ?? null, error.code, error.message, error.data);
    }
    return failure(null, -32603, "Internal error");
  }

  return dispatchRpcMethod(parsed.method, parsed.params ?? {}, ctx, { id: parsed.id });
}

export async function handleRpcMethod(
  methodName: string,
  params: Record<string, unknown>,
  ctx: RpcContext,
  options: RpcDispatchOptions = {}
): Promise<Response> {
  if (typeof methodName !== "string" || !methodName.trim()) {
    return failure(options.id ?? null, -32600, "Invalid Request");
  }
  if (!params || typeof params !== "object" || Array.isArray(params)) {
    return failure(options.id ?? null, -32600, "params must be an object");
  }
  return dispatchRpcMethod(methodName, params, ctx, options);
}

export { RpcError };
