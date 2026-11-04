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

export const inboxResultSchema = z.object({
  title: z.string().min(1),
  provider: z.string(),
  mediaType: z.enum(["movie", "audio", "ebook"]),
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
  ) {
    this.beforeWrite();
    const previous = this.feed(id)!;
    if (
      expectedHead !== undefined &&
      previous.head_key !== expectedHead &&
      previous.head_key !== head
    )
      return;
    typedRun(
      this.db,
      dbSql
        .updateTable("indexer_feeds")
        .set({
          head_key: head ?? previous.head_key,
          last_polled_at: now,
          last_success_at: now,
          last_error: null,
          retry_at: null,
          gap_since: previous.gap_since ?? (gap ? now : null),
          gap_note: previous.gap_note ?? gap,
          updated_at: now,
        })
        .where("id", "=", id),
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
