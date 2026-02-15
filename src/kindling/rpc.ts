import { hydrateBookFromOpenLibrary } from "./hydration";
import { resolveOpenLibraryCandidate, searchOpenLibrary } from "./openlibrary";
import { KindlingRepo } from "./repo";
import { runSearch, runSnatch, triggerAutoAcquire } from "./service";
import type { AppSettings, JobType, LibraryBook, MediaType } from "./types";

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
  repo: KindlingRepo;
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
  if (value === "scan" || value === "download" || value === "import" || value === "transcode" || value === "reconcile") {
    return value;
  }
  throw new RpcError(-32602, "type must be one of scan|download|import|transcode|reconcile");
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
    return {
      book,
      releases: ctx.repo.listReleasesByBook(bookId),
      assets: ctx.repo.listAssetsByBook(bookId),
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
      type: "scan",
      payload: { fullRefresh: true },
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
    const jobId = await triggerAutoAcquire(ctx.repo, bookId, media);
    return { jobId, media };
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

  async "search.run"(ctx, params) {
    const query = asString(params.query, "query").trim();
    const media = parseMedia(params.media);
    const results = await runSearch(ctx.repo.getSettings(), {
      query,
      media,
    });
    return { results };
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
    return { downloads: ctx.repo.listDownloads() };
  },

  async "downloads.get"(ctx, params) {
    const jobId = asPositiveInt(params.jobId, "jobId");
    const download = ctx.repo.getDownload(jobId);
    if (!download) {
      throw new RpcError(-32000, "Download not found", { error: "not_found", jobId });
    }
    return download;
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
      : (["scan", "download", "import", "transcode", "reconcile"] as JobType[]).flatMap((jobType) =>
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

  async "import.reconcile"(ctx) {
    const job = ctx.repo.createJob({ type: "reconcile" });
    return { jobId: job.id };
  },
};

const readOnlyMethods = new Set<string>([
  "system.health",
  "system.server",
  "settings.get",
  "openlibrary.search",
  "library.list",
  "library.get",
  "search.run",
  "releases.list",
  "downloads.list",
  "downloads.get",
  "jobs.list",
  "jobs.get",
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
