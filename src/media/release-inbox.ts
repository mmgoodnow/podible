import { createHash } from "node:crypto";
import type { Database } from "bun:sqlite";
import { z } from "zod";
import { dbSql, typedAll, typedGet, typedRun } from "../db/typed";
import type {
  IndexerMedia,
  SearchResult,
  SearchSource,
} from "./indexer-gateway";
import type { IndexerFailure } from "./indexer-errors";
import { feedGapDiagnosticsSchema, type FeedGapDiagnostics } from "./feed-gap";

export const inboxResultSchema = z.object({
  title: z.string().min(1),
  provider: z.string(),
  mediaType: z.enum(["movie", "audio", "ebook", "tv"]),
  sizeBytes: z.number().nonnegative().nullable(),
  url: z
    .url()
    .refine((value) => ["http:", "https:"].includes(new URL(value).protocol)),
  guid: z.string().nullable(),
  infoHash: z.string().nullable(),
  seeders: z.number().nullable(),
  leechers: z.number().nullable(),
  raw: z.record(z.string(), z.unknown()),
});
const digest = (value: string) =>
  createHash("sha256").update(value).digest("hex");
export function feedIdentity(source: SearchSource, media: IndexerMedia) {
  const url = new URL(source.baseUrl);
  url.searchParams.delete("apikey");
  return digest(url.href + ":" + media);
}
export function feedReleaseKey(result: SearchResult<string>) {
  return digest(result.guid ?? result.infoHash ?? result.url);
}

export const MAX_FEED_BOUNDARY_KEYS = 5;
export function boundedFeedBoundary(keys: Iterable<string | null>): string[] {
  const distinct = new Set<string>();
  for (const key of keys) {
    if (key === null) continue;
    if (typeof key !== "string" || !key.length || key.length > 256)
      throw new Error("Invalid feed boundary key");
    distinct.add(key);
    if (distinct.size === MAX_FEED_BOUNDARY_KEYS) break;
  }
  return [...distinct];
}
type FeedBoundarySnapshot = {
  id: string;
  head_key: string | null;
  last_success_at: string | null;
  boundary_json: string | null;
};

/** Feed inventory and domain associations; execution/retry state stays in Reflow. */
export class ReleaseInboxRepo {
  constructor(
    private db: Database,
    private beforeWrite: () => void = () => {},
  ) {}
  ensureFeed(id: string, provider: string, media: IndexerMedia, now: string) {
    this.beforeWrite();
    typedRun(
      this.db,
      dbSql
        .insertInto("indexer_feeds")
        .values({
          id,
          provider,
          media_type: media,
          updated_at: now,
        })
        .onConflict((oc) => oc.column("id").doUpdateSet({ provider })),
    );
    return this.feed(id)!;
  }
  feed(id: string) {
    return typedGet(
      this.db,
      dbSql.selectFrom("indexer_feeds").selectAll().where("id", "=", id),
    );
  }
  feeds() {
    return typedAll(
      this.db,
      dbSql
        .selectFrom("indexer_feeds")
        .selectAll()
        .orderBy("provider")
        .orderBy("media_type"),
    );
  }
  /** Only retained release IDs establish overlap; provider publication dates never do. */
  boundaryKeys(feed: FeedBoundarySnapshot): string[] {
    if (feed.boundary_json && feed.boundary_json.length <= 2048) {
      try {
        const keys: unknown = JSON.parse(feed.boundary_json);
        if (Array.isArray(keys) && keys.length > 0 && keys.length <= MAX_FEED_BOUNDARY_KEYS &&
            keys.every(key => typeof key === "string" && key.length > 0 && key.length <= 256))
          return boundedFeedBoundary([feed.head_key, ...keys]);
      } catch { /* Legacy/corrupt snapshots retain the known head, never an invented boundary. */ }
    }
    const head = feed.head_key;
    if (!head || !feed.last_success_at) return boundedFeedBoundary([head]);
    const captured = typedGet(this.db, dbSql.selectFrom("release_inbox")
      .select(["id", "first_seen_at"]).where("feed_id", "=", feed.id)
      .where("release_key", "=", head).where("first_seen_at", "<=", feed.last_success_at)
      .orderBy("id", "desc").limit(1));
    if (!captured) return [head];
    // Legacy ingest wrote newest-first rows and the cursor in the same commit/capture time.
    // Same-feed IDs following that captured head prove prior capture, unlike pubDate alone.
    const older = typedAll(this.db, dbSql.selectFrom("release_inbox")
      .select("release_key").select(eb => eb.fn.min<number>("id").as("capture_order"))
      .where("feed_id", "=", feed.id).where("first_seen_at", "=", captured.first_seen_at)
      .where("id", ">=", captured.id).groupBy("release_key").orderBy("capture_order")
      .limit(MAX_FEED_BOUNDARY_KEYS));
    return boundedFeedBoundary([head, ...older.map(row => row.release_key)]);
  }
  entries(ids: number[]) {
    if (!ids.length) return [];
    if (
      ids.length > 1000 ||
      ids.some((id) => !Number.isSafeInteger(id) || id <= 0)
    )
      throw new Error("Invalid inbox entry IDs");
    return typedAll(
      this.db,
      dbSql
        .selectFrom("release_inbox")
        .selectAll()
        .where("id", "in", ids)
        .orderBy("id"),
    ).map((row) => ({
      ...row,
      result: inboxResultSchema.parse(JSON.parse(row.payload_json)),
    }));
  }
  /** Explicit backfills pin an inbox high-water mark without moving RSS boundaries. */
  retainedHighWater(): number {
    return (this.db.query("SELECT COALESCE(MAX(id),0) AS id FROM release_inbox").get() as { id: number }).id;
  }
  retainedPage(media: IndexerMedia, afterId: number, throughId: number, limit = 100) {
    z.enum(["audio", "ebook", "movie", "tv"]).parse(media);
    for (const value of [afterId, throughId]) z.number().int().nonnegative().safe().parse(value);
    z.number().int().min(1).max(1000).parse(limit);
    const rows = typedAll(this.db, dbSql.selectFrom("release_inbox")
      .innerJoin("indexer_feeds", "indexer_feeds.id", "release_inbox.feed_id")
      .selectAll("release_inbox").where("indexer_feeds.media_type", "=", media)
      .where("release_inbox.id", ">", afterId).where("release_inbox.id", "<=", throughId)
      .orderBy("release_inbox.id").limit(limit));
    return rows.map(row => ({ ...row, result: inboxResultSchema.parse(JSON.parse(row.payload_json)) }));
  }
  /** Retain server search results, returning existing IDs too; never updates a feed cursor. */
  retainSearchResults(feedId: string, results: SearchResult<IndexerMedia>[], now: string) {
    if (results.length > 150) throw new Error("Search retention batch exceeds 150 results");
    const feed = this.feed(feedId);
    if (!feed || results.some(result => result.mediaType !== feed.media_type)) throw new Error("Search retention media does not match its feed");
    this.ingest(feedId, results, now);
    return results.map(result => {
      const row = typedGet(this.db, dbSql.selectFrom("release_inbox").selectAll()
        .where("feed_id", "=", feedId).where("release_key", "=", feedReleaseKey(result))
        .where("content_key", "=", digest(JSON.stringify([result.title, result.sizeBytes, result.infoHash]))));
      if (!row) throw new Error("Search result was not retained");
      return { ...row, result: inboxResultSchema.parse(JSON.parse(row.payload_json)) };
    });
  }
  ingest(feedId: string, results: SearchResult<IndexerMedia>[], now: string) {
    this.beforeWrite();
    const inserted: number[] = [];
    for (const raw of results) {
      // Keep useful provider identity attributes, not arbitrary HTML descriptions.
      const result = inboxResultSchema.parse({
        ...raw,
        raw: {
          attr: raw.raw.attr,
          category: raw.raw.category,
          pubDate: raw.raw.pubDate,
        },
      });
      const row = typedGet(
        this.db,
        dbSql
          .insertInto("release_inbox")
          .values({
            feed_id: feedId,
            release_key: feedReleaseKey(result),
            content_key: digest(
              JSON.stringify([result.title, result.sizeBytes, result.infoHash]),
            ),
            payload_json: JSON.stringify(result),
            first_seen_at: now,
          })
          .onConflict((oc) =>
            oc.columns(["feed_id", "release_key", "content_key"]).doNothing(),
          )
          .returning("id"),
      );
      if (row) inserted.push(row.id);
      else
        typedRun(
          this.db,
          dbSql
            .updateTable("release_inbox")
            .set({ payload_json: JSON.stringify(result) })
            .where("feed_id", "=", feedId)
            .where("release_key", "=", feedReleaseKey(result))
            .where(
              "content_key",
              "=",
              digest(
                JSON.stringify([
                  result.title,
                  result.sizeBytes,
                  result.infoHash,
                ]),
              ),
            ),
        );
    }
    return inserted;
  }
  succeed(
    id: string,
    head: string | null,
    gap: string | null,
    now: string,
    expectedHead?: string | null,
    boundaryKeys?: readonly string[],
    expectedBoundaryJson?: string | null,
    gapDiagnostics?: FeedGapDiagnostics | null,
  ) {
    this.beforeWrite();
    const previous = this.feed(id)!;
    if (
      expectedHead !== undefined &&
      previous.head_key !== expectedHead &&
      previous.head_key !== head
    )
      return;
    if (expectedBoundaryJson !== undefined && previous.boundary_json !== expectedBoundaryJson)
      return;
    const boundary = boundedFeedBoundary([head, ...(boundaryKeys ?? []), ...this.boundaryKeys(previous)]);
    typedRun(
      this.db,
      dbSql
        .updateTable("indexer_feeds")
        .set({
          head_key: head ?? previous.head_key,
          boundary_json: boundary.length ? JSON.stringify(boundary) : null,
          last_polled_at: now,
          last_success_at: now,
          last_error: null,
          retry_at: null,
          gap_since: previous.gap_since ?? (gap ? now : null),
          gap_note: previous.gap_note ?? gap,
          gap_diagnostics_json: gap && gapDiagnostics
            ? JSON.stringify(feedGapDiagnosticsSchema.parse(gapDiagnostics))
            : previous.gap_diagnostics_json,
          updated_at: now,
        })
        .where("id", "=", id)
        .where("head_key", previous.head_key === null ? "is" : "=", previous.head_key)
        .where("boundary_json", previous.boundary_json === null ? "is" : "=", previous.boundary_json),
    );
  }
  fail(
    id: string,
    failure: IndexerFailure,
    now: string,
    expectedSuccessAt?: string | null,
  ) {
    this.beforeWrite();
    if (
      expectedSuccessAt !== undefined &&
      this.feed(id)?.last_success_at !== expectedSuccessAt
    )
      return;
    typedRun(
      this.db,
      dbSql
        .updateTable("indexer_feeds")
        .set({
          last_polled_at: now,
          last_error: failure.message,
          retry_at:
            failure.retryAt === null
              ? null
              : new Date(failure.retryAt).toISOString(),
          updated_at: now,
        })
        .where("id", "=", id),
    );
  }
  considered(
    releaseId: number,
    kind: string,
    targetId: number,
    revision: string,
  ) {
    return typedGet(
      this.db,
      dbSql
        .selectFrom("release_considerations")
        .select("job_id")
        .where("release_id", "=", releaseId)
        .where("target_kind", "=", kind)
        .where("target_id", "=", targetId)
        .where("target_revision", "=", revision),
    );
  }
  /** Read retained matching evidence, independent of the acquisition's later outcome. */
  consideredEntries(kind: string, targetId: number, limit = 1000) {
    if (!Number.isSafeInteger(targetId) || targetId <= 0 ||
        !Number.isSafeInteger(limit) || limit < 1 || limit > 1000)
      throw new Error("Invalid considered inbox lookup");
    return typedAll(
      this.db,
      dbSql.selectFrom("release_inbox")
        .innerJoin("release_considerations", "release_considerations.release_id", "release_inbox.id")
        .selectAll("release_inbox")
        .distinct()
        .where("release_considerations.target_kind", "=", kind)
        .where("release_considerations.target_id", "=", targetId)
        .orderBy("release_inbox.id", "desc")
        .limit(limit),
    ).map((row) => ({ ...row, result: inboxResultSchema.parse(JSON.parse(row.payload_json)) }));
  }
  consideredForDispatch(
    releaseId: number,
    kind: string,
    targetId: number,
    sourceJobId: number,
  ) {
    return typedGet(
      this.db,
      dbSql
        .selectFrom("release_considerations")
        .select("job_id")
        .where("release_id", "=", releaseId)
        .where("target_kind", "=", kind)
        .where("target_id", "=", targetId)
        .where("source_job_id", "=", sourceJobId),
    );
  }
  consider(
    releaseId: number,
    kind: string,
    targetId: number,
    revision: string,
    jobId: number,
    note: string,
    sourceJobId = jobId,
  ) {
    this.beforeWrite();
    typedRun(
      this.db,
      dbSql.insertInto("release_considerations").values({
        release_id: releaseId,
        target_kind: kind,
        target_id: targetId,
        target_revision: revision,
        job_id: jobId,
        source_job_id: sourceJobId,
        decision_note: note,
        created_at: new Date().toISOString(),
      }),
    );
  }
}
