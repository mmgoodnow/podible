import { createHash } from "node:crypto";
import { rm } from "node:fs/promises";

import { computeDownloadFraction, pseudoProgressForMediaStatus, pseudoProgressForRelease } from "../progress";
import { BooksRepo } from "../repo";
import { RtorrentClient } from "../rtorrent";
import type { JobRow, JobType, LibraryBook, MediaType, ReleaseRow, SessionWithUserRow } from "../types";

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

export type RpcMethodHandler = (ctx: RpcContext, params: Record<string, unknown>) => Promise<unknown>;
export type RpcAuthLevel = "public" | "user" | "admin";
export type RpcMethodDefinition = {
  auth: RpcAuthLevel;
  readOnly?: boolean;
  summary: string;
  handler: RpcMethodHandler;
};

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

export function asObject(value: unknown, errorCode: number, errorMessage: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new RpcError(errorCode, errorMessage);
  }
  return value as Record<string, unknown>;
}

export function asString(value: unknown, name: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new RpcError(-32602, `${name} is required`);
  }
  return value;
}

export function asOptionalString(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string") {
    throw new RpcError(-32602, "Invalid string value");
  }
  return value;
}

export function asOptionalStringArray(value: unknown, name: string): string[] | undefined {
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

export function asOptionalPositiveIntArray(value: unknown, name: string): number[] | undefined {
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

export function asOptionalBoolean(value: unknown, name: string): boolean | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    if (value === "true") return true;
    if (value === "false") return false;
  }
  throw new RpcError(-32602, `${name} must be a boolean`);
}

export function asOptionalPositiveInt(value: unknown, name: string): number | undefined {
  if (value === undefined || value === null) return undefined;
  return asPositiveInt(value, name);
}

export function asPositiveInt(value: unknown, name: string): number {
  const n = typeof value === "number" ? value : typeof value === "string" ? Number.parseInt(value, 10) : NaN;
  if (!Number.isInteger(n) || n <= 0) {
    throw new RpcError(-32602, `${name} must be a positive integer`);
  }
  return n;
}

export function parseLimit(value: unknown): number {
  if (value === undefined || value === null) return 50;
  const parsed = typeof value === "number" ? Math.trunc(value) : Number.parseInt(String(value), 10);
  if (!Number.isInteger(parsed) || parsed <= 0) return 50;
  return Math.min(parsed, 200);
}

export function parseMedia(value: unknown): MediaType {
  if (value === "audio" || value === "ebook") {
    return value;
  }
  throw new RpcError(-32602, "media must be audio or ebook");
}

export function parseMediaSelection(value: unknown): MediaType[] {
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

export function parseJobType(value: unknown): JobType {
  if (
    value === "full_library_refresh" ||
    value === "acquire" ||
    value === "download" ||
    value === "import" ||
    value === "reconcile" ||
    value === "chapter_analysis"
  ) {
    return value;
  }
  throw new RpcError(-32602, "type must be one of full_library_refresh|acquire|download|import|reconcile|chapter_analysis");
}

export function uniqueManualInfoHash(bookId: number, mediaType: MediaType, sourcePath: string): string {
  return createHash("sha1")
    .update(`manual:${bookId}:${mediaType}:${sourcePath}:${Date.now()}:${Math.random()}`)
    .digest("hex");
}

export function parseRequest(raw: unknown): RpcRequest {
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
