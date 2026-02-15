import { fetchOpenLibraryMetadata, resolveOpenLibraryCandidate, searchOpenLibrary } from "./openlibrary";
import { KindlingRepo } from "./repo";
import { runSearch, runSnatch, triggerAutoAcquire } from "./service";
import type { AppSettings, MediaType } from "./types";

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
    const title = asOptionalString(params.title);
    const author = asOptionalString(params.author);
    const openLibraryKey = asOptionalString(params.openLibraryKey);
    const isbn = asOptionalString(params.isbn);

    const hasIdentifier = Boolean(openLibraryKey?.trim() || isbn?.trim());
    const resolved = hasIdentifier
      ? await resolveOpenLibraryCandidate({
          openLibraryKey,
          isbn,
          title,
          author,
        })
      : null;

    let finalTitle = title?.trim() ?? "";
    let finalAuthor = author?.trim() ?? "";
    if (resolved) {
      finalTitle = resolved.title;
      finalAuthor = resolved.author;
    }

    if (!finalTitle || !finalAuthor) {
      throw new RpcError(-32602, "title and author are required (or provide openLibraryKey/isbn)");
    }
    if (hasIdentifier && !resolved) {
      throw new RpcError(-32000, "Open Library match not found", { error: "not_found" });
    }

    const book = ctx.repo.createBook({
      title: finalTitle,
      author: finalAuthor,
    });

    const metadata = resolved
      ? {
          publishedAt: resolved.publishedAt ?? null,
          language: resolved.language ?? null,
          isbn: (isbn?.trim() || resolved.isbn) ?? null,
          identifiers: {
            ...resolved.identifiers,
            ...(isbn?.trim() ? { isbn: isbn.trim() } : {}),
          },
        }
      : await fetchOpenLibraryMetadata({
          title: book.title,
          author: book.author,
          isbn: isbn ?? null,
          openLibraryKey: openLibraryKey ?? null,
        }).catch(() => null);

    if (metadata) {
      ctx.repo.updateBookMetadata(book.id, {
        publishedAt: metadata.publishedAt ?? null,
        language: metadata.language ?? null,
        isbn: metadata.isbn ?? null,
        identifiers: metadata.identifiers,
      });
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
    const infoHash = asString(params.infoHash, "infoHash");
    const sizeBytes =
      params.sizeBytes === undefined || params.sizeBytes === null
        ? null
        : typeof params.sizeBytes === "number"
          ? Math.trunc(params.sizeBytes)
          : Number.parseInt(String(params.sizeBytes), 10);

    return runSnatch(ctx.repo, ctx.repo.getSettings(), {
      bookId,
      provider,
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

  async "import.reconcile"(ctx) {
    const job = ctx.repo.createJob({ type: "reconcile" });
    return { jobId: job.id };
  },
};

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

  try {
    const method = handlers[parsed.method];
    if (!method) {
      throw new RpcError(-32601, "Method not found");
    }
    const result = await method(ctx, parsed.params ?? {});
    return success(parsed.id, result);
  } catch (error) {
    if (error instanceof RpcError) {
      return failure(parsed.id, error.code, error.message, error.data);
    }
    const message = (error as Error).message;
    return failure(parsed.id, -32000, message || "Application error", { message });
  }
}

export { RpcError };
