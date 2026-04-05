import { hydrateBookFromOpenLibrary } from "../hydration";
import { resolveOpenLibraryCandidate, searchOpenLibrary } from "../openlibrary";
import { RtorrentClient } from "../rtorrent";
import { triggerAutoAcquire } from "../service";

import {
  RpcError,
  asOptionalBoolean,
  asOptionalPositiveInt,
  asOptionalPositiveIntArray,
  asOptionalString,
  asOptionalStringArray,
  asPositiveInt,
  asString,
  enrichLibraryBookProgress,
  parseLimit,
  parseMediaSelection,
  type RpcMethodDefinition,
} from "./shared";

export const userRpcMethods: Record<string, RpcMethodDefinition> = {
  "auth.me": {
    auth: "user",
    readOnly: true,
    summary: "Return the current authenticated user and session metadata.",
    async handler(ctx) {
      const session = ctx.session;
      if (!session) {
        throw new RpcError(-32001, "Unauthorized");
      }
      return {
        user: {
          id: session.user_id,
          provider: session.provider,
          username: session.username,
          displayName: session.display_name,
          thumbUrl: session.thumb_url,
          isAdmin: session.is_admin === 1,
        },
        session: {
          kind: session.kind,
          expiresAt: session.expires_at,
        },
      };
    },
  },

  "auth.logout": {
    auth: "user",
    summary: "Invalidate the current authenticated session.",
    async handler(ctx) {
      const session = ctx.session;
      if (!session) {
        throw new RpcError(-32001, "Unauthorized");
      }
      ctx.repo.deleteSession(session.id);
      return { ok: true };
    },
  },

  "openlibrary.search": {
    auth: "user",
    readOnly: true,
    summary: "Search Open Library for works to add.",
    async handler(_ctx, params) {
      const query = asString(params.q, "q").trim();
      const limit = Math.min(parseLimit(params.limit), 50);
      const results = await searchOpenLibrary(query, limit);
      return { results };
    },
  },

  "library.list": {
    auth: "user",
    readOnly: true,
    summary: "List books in the library.",
    async handler(ctx, params) {
      const limit = parseLimit(params.limit);
      const cursor = params.cursor === undefined ? undefined : asPositiveInt(params.cursor, "cursor");
      const q = asOptionalString(params.q);
      return ctx.repo.listBooks(limit, cursor, q);
    },
  },

  "library.get": {
    auth: "user",
    readOnly: true,
    summary: "Get one book with releases and assets.",
    async handler(ctx, params) {
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
  },

  "library.inProgress": {
    auth: "user",
    readOnly: true,
    summary: "List non-terminal LibraryBook rows (optionally filtered by bookIds).",
    async handler(ctx, params) {
      const bookIds = asOptionalPositiveIntArray(params.bookIds, "bookIds");
      const items = ctx.repo.listInProgressBooks(bookIds);
      const hasDownloading = items.some((book) => book.audioStatus === "downloading" || book.ebookStatus === "downloading");
      const client = hasDownloading ? new RtorrentClient(ctx.repo.getSettings().rtorrent) : null;
      return {
        items: await Promise.all(items.map((book) => enrichLibraryBookProgress(ctx.repo, book, client))),
      };
    },
  },

  "library.create": {
    auth: "user",
    summary: "Create a book from an Open Library work key and queue auto-acquire.",
    async handler(ctx, params) {
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
  },

  "library.acquire": {
    auth: "user",
    summary: "Queue targeted acquire job for a book/media set.",
    async handler(ctx, params) {
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
  },

  "releases.list": {
    auth: "user",
    readOnly: true,
    summary: "List releases for a book.",
    async handler(ctx, params) {
      const bookId = asPositiveInt(params.bookId, "bookId");
      return { releases: ctx.repo.listReleasesByBook(bookId) };
    },
  },
};
