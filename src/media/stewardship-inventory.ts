import type { Database } from "bun:sqlite";
import { z } from "zod";

const schema = z.object({ source: z.enum(["all", "books", "screen"]), offset: z.number().int().nonnegative().safe(), limit: z.number().int().min(1).max(50) }).strict();
export type StewardshipInventoryInput = z.input<typeof schema>;
export type StewardshipInventoryPage = { rows: { source: "books" | "screen"; row: Record<string, unknown> }[]; total: number };
const screenDomain = `(CASE WHEN json_valid(t.location_json) THEN json_extract(t.location_json,'$.rootId') END IN ('movies','tv')
    OR t.id IN (SELECT torrent_id FROM movie_acquisition_downloads UNION SELECT torrent_id FROM tv_acquisition_downloads))`;

/** A single bounded raw page across domain adapters, not a second torrent ownership store. */
export function readStewardshipInventory(db: Database, input: StewardshipInventoryInput): StewardshipInventoryPage {
  const { source, offset, limit } = schema.parse(input);
  const common = !!db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='book_torrent_bindings'").get();
  const screenWhere = common ? `NOT EXISTS(SELECT 1 FROM book_torrent_bindings book WHERE book.torrent_id=t.id) AND ${screenDomain}` : screenDomain;
  const counts = db.query(`SELECT (SELECT COUNT(*) FROM ${common ? "book_torrent_bindings" : "book_torrents"}) AS books,
    (SELECT COUNT(*) FROM media_torrents t WHERE ${screenWhere}) AS screen`).get() as { books: number; screen: number };
  const rows: StewardshipInventoryPage["rows"] = [];
  const bookLimit = source !== "screen" && offset < counts.books ? Math.min(limit, counts.books - offset) : 0;
  if (bookLimit) {
    const bookSelect = common ? `SELECT t.native_id AS info_hash,b.book_id,b.release_id,t.backend,t.client_id,t.root_path,t.root_token,
      CASE WHEN json_valid(t.location_json) THEN json_extract(t.location_json,'$.rootId') END AS root_id,
      CASE WHEN json_valid(t.location_json) THEN json_extract(t.location_json,'$.relativePath') END AS relative_path,b.created_at
      FROM book_torrent_bindings b JOIN media_torrents t ON t.id=b.torrent_id ORDER BY t.native_id,t.client_id`
      : "SELECT * FROM book_torrents ORDER BY info_hash,client_id";
    const books = db.query(`${bookSelect} LIMIT ? OFFSET ?`).all(bookLimit, offset) as Record<string, unknown>[];
    rows.push(...books.map(row => ({ source: "books" as const, row })));
  }
  const screenLimit = source === "books" ? 0 : limit - rows.length;
  if (screenLimit) {
    const screenOffset = source === "screen" ? offset : Math.max(0, offset - counts.books);
    const screen = db.query(`SELECT t.* FROM media_torrents t WHERE ${screenWhere} ORDER BY t.id LIMIT ? OFFSET ?`).all(screenLimit, screenOffset) as Record<string, unknown>[];
    rows.push(...screen.map(row => ({ source: "screen" as const, row })));
  }
  return { rows, total: source === "books" ? counts.books : source === "screen" ? counts.screen : counts.books + counts.screen };
}
