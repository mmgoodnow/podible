import { createHash } from "node:crypto";
import { rm } from "node:fs/promises";

import { z } from "zod";

import { computeDownloadFraction, pseudoProgressForMediaStatus, pseudoProgressForRelease } from "../library/progress";
import { BooksRepo } from "../repo";
import { RtorrentClient } from "../rtorrent";
import type { JobRow, LibraryBook, ReleaseRow, SessionWithUserRow } from "../app-types";

export type RpcId = string | number | null;

export type RpcRequest = {
  jsonrpc: "2.0";
  id: RpcId;
  method: string;
  params?: Record<string, unknown>;
};

export type RpcErrorPayload = {
  code: number;
  message: string;
  data?: unknown;
};

export type RpcSuccess = {
  jsonrpc: "2.0";
  id: RpcId;
  result: unknown;
};

export type RpcFailure = {
  jsonrpc: "2.0";
  id: RpcId;
  error: RpcErrorPayload;
};

export type RpcAuthLevel = "public" | "user" | "admin";

export type RpcContext = {
  repo: BooksRepo;
  startTime: number;
  request: Request;
  session: SessionWithUserRow | null;
};

export type RpcDispatchOptions = {
  id?: RpcId;
  readOnly?: boolean;
};

export class RpcError extends Error {
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

export type DownloadRpcView = ReturnType<BooksRepo["listDownloads"]>[number] & {
  fullPseudoProgress: number;
  downloadProgress?: DownloadProgress;
};

export async function removeFileIfPresent(filePath: string): Promise<boolean> {
  try {
    await rm(filePath, { force: true });
    return true;
  } catch (error) {
    throw new Error(`Failed to remove file ${filePath}: ${(error as Error).message}`);
  }
}

export async function enrichDownload(
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
    return {
      ...download,
      fullPseudoProgress: pseudoProgressForRelease(download.release_status),
    };
  }
}

export function enrichJob(ctx: RpcContext, job: JobRow): JobRow {
  if (job.book_id == null) return job;
  const book = ctx.repo.getBookRow(job.book_id);
  return {
    ...job,
    book_title: book?.title ?? null,
  };
}

function mediaPseudoProgress(status: LibraryBook["audioStatus"] | LibraryBook["ebookStatus"], fraction?: number | null): number {
  if (status === "downloading") {
    return pseudoProgressForRelease("downloading", fraction);
  }
  return pseudoProgressForMediaStatus(status);
}

async function liveFractionForMedia(
  releases: ReleaseRow[],
  mediaType: "audio" | "ebook",
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

export async function enrichLibraryBookProgress(
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

export function uniqueManualInfoHash(bookId: number, mediaType: "audio" | "ebook", sourcePath: string): string {
  return createHash("sha1")
    .update(`manual:${bookId}:${mediaType}:${sourcePath}:${Date.now()}:${Math.random()}`)
    .digest("hex");
}

const rpcRequestSchema = z
  .object({
    jsonrpc: z.literal("2.0"),
    id: z.union([z.string(), z.number(), z.null()]),
    method: z.string().trim().min(1, "method must be a non-empty string"),
    params: z.record(z.string(), z.unknown()).optional(),
  })
  .strict();

export function parseRequest(raw: unknown): RpcRequest {
  if (Array.isArray(raw)) {
    throw new RpcError(-32600, "Batch requests are not supported");
  }
  const parsed = rpcRequestSchema.safeParse(raw);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    throw new RpcError(-32600, issue?.message || "Invalid Request");
  }
  return {
    jsonrpc: "2.0",
    id: parsed.data.id,
    method: parsed.data.method,
    params: parsed.data.params ?? {},
  };
}
