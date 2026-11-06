import { createHash } from "node:crypto";
import { z } from "zod";
import type { JobRow, MediaType } from "../app-types";
import type { BooksRepo } from "../repo";
import {
  createBookFeedMatcher,
  type BookFeedMatcher,
  type BookFeedTarget,
} from "../library/feed-matcher";
import type { TorznabResult } from "../library/torznab";
import { durableStep, type WorkerContext } from "./context";
import { bookAutomaticUpgradeAllowed } from "../library/quality-state";

const payloadSchema = z
  .object({
    releaseIds: z.array(z.number().int().positive()).min(1).max(100),
  })
  .strict();

export type BookFeedContext = WorkerContext & {
  bookFeedMatcher?: BookFeedMatcher;
};

/** Missing slots plus compatible RSS upgrades; no recurring per-title searches. */
export function hasWantedBookMedia(repo: BooksRepo): boolean {
  const canReview = !!repo.getSettings().agents.apiKey.trim();
  return repo
    .listAllBooks()
    .some(
      (book) =>
        book.audioStatus !== "imported" || book.ebookStatus !== "imported" ||
        (canReview && ["audio", "ebook"].some(media =>
          bookAutomaticUpgradeAllowed(repo, book.id, media as MediaType))),
    );
}

export function bookFeedRevision(
  ctx: WorkerContext,
  bookId: number,
  media: MediaType,
): string | null {
  const book = ctx.repo.getBook(bookId);
  if (
    !book ||
    ((media === "audio" ? book.audioStatus : book.ebookStatus) === "imported" &&
      (!bookAutomaticUpgradeAllowed(ctx.repo, bookId, media) ||
        (!ctx.modelProvider && !ctx.getSettings().agents.apiKey.trim())))
  )
    return null;
  return createHash("sha256")
    .update(
      JSON.stringify({
        title: book.title,
        author: book.author,
        language: book.language,
        identifiers: book.identifiers,
        media,
        editionPreference: ctx.getSettings().agents.editionPreference,
        releases: ctx.repo
          .listReleasesByBook(bookId)
          .filter((release) => release.media_type === media)
          .map((release) => [release.id, release.status, release.info_hash]),
        manifestations: ctx.repo
          .listManifestationsByBook(bookId)
          .filter((manifestation) => manifestation.kind === media)
          .map((manifestation) => [
            manifestation.id,
            manifestation.preferred_score,
            manifestation.language,
          ]),
      }),
    )
    .digest("hex");
}

function activeAcquisition(
  ctx: WorkerContext,
  bookId: number,
  media: MediaType,
): boolean {
  const active = (job: JobRow) =>
    job.status === "queued" || job.status === "running";
  if (
    ctx.repo.listJobsByType("acquire").some((job) => {
      if (!active(job)) return false;
      const payload = JSON.parse(job.payload_json ?? "{}");
      return (
        (job.book_id ?? payload.bookId) === bookId &&
        (!Array.isArray(payload.media) ||
          payload.media.length === 0 ||
          payload.media.includes(media))
      );
    })
  )
    return true;
  const releases = new Set(
    ctx.repo
      .listReleasesByBook(bookId)
      .filter((release) => release.media_type === media)
      .map((release) => release.id),
  );
  return ["download", "import"].some((type) =>
    ctx.repo
      .listJobsByType(type as "download" | "import")
      .some(
        (job) =>
          active(job) &&
          job.release_id !== null &&
          releases.has(job.release_id),
      ),
  );
}

/** Match shared inbox evidence, then atomically dispatch missing-media acquisition and its receipts. */
export async function matchBookFeed(
  ctx: BookFeedContext,
  job: JobRow,
): Promise<"done"> {
  const { releaseIds } = payloadSchema.parse(
    JSON.parse(job.payload_json ?? "{}"),
  );
  const entries = ctx.repo.releaseInbox.entries(releaseIds);
  if (entries.length !== new Set(releaseIds).size)
    throw new Error("Book RSS inbox entries are missing");
  if (entries.some((entry) => entry.result.mediaType === "movie"))
    throw new Error("Not a book feed batch");
  const releases = entries.map((entry) => entry.result as TorznabResult);
  const media = new Set(releases.map((release) => release.mediaType));
  const targets = await durableStep(ctx, "book-rss:targets", () =>
    ctx.repo.listAllBooks().flatMap((book) =>
      [...media].flatMap((kind): BookFeedTarget[] =>
        (kind === "audio" ? book.audioStatus : book.ebookStatus) === "imported" &&
          (!bookAutomaticUpgradeAllowed(ctx.repo, book.id, kind) ||
            (!ctx.modelProvider && !ctx.getSettings().agents.apiKey.trim()))
          ? []
          : [
              {
                bookId: book.id,
                media: kind,
                title: book.title,
                author: book.author,
                language: book.language,
              },
            ],
      ),
    ),
  );
  if (!targets.length) return "done";
  const matcher =
    ctx.bookFeedMatcher ??
    createBookFeedMatcher(ctx.getSettings(), ctx.modelProvider);
  const matches = [] as Awaited<ReturnType<BookFeedMatcher>>;
  for (let start = 0; start < targets.length; start += 100) {
    matches.push(
      ...(await durableStep(ctx, "book-rss:match:" + start, () =>
        matcher({ targets: targets.slice(start, start + 100), releases }),
      )),
    );
  }
  const key = (bookId: number, kind: MediaType) => bookId + ":" + kind;
  const allowed = new Set(
    targets.map((target) => key(target.bookId, target.media)),
  );
  for (const match of matches) {
    if (
      !allowed.has(key(match.bookId, match.media)) ||
      !match.releaseIndexes.length ||
      match.releaseIndexes.some(
        (index) => !Number.isInteger(index) || !entries[index],
      ) ||
      typeof match.reason !== "string" ||
      !match.reason.trim()
    )
      throw new Error(
        "Book RSS matcher returned an unknown target/release or missing evidence",
      );
    if (
      match.releaseIndexes.some(
        (index) => releases[index].mediaType !== match.media,
      )
    )
      throw new Error("Book RSS matcher crossed media types");
  }
  let pending = [
    ...new Map(
      matches.map((match) => [key(match.bookId, match.media), match]),
    ).values(),
  ];
  for (let wave = 0; pending.length; wave++) {
    const busy: typeof pending = [];
    for (const target of pending) {
      ctx.execution?.assertActive();
      const revision = bookFeedRevision(ctx, target.bookId, target.media);
      if (revision === null) continue;
      const matching = matches.filter(
        (match) =>
          match.bookId === target.bookId && match.media === target.media,
      );
      const kind = "book:" + target.media;
      const considered = (index: number) =>
        ctx.repo.releaseInbox.considered(
          entries[index].id,
          kind,
          target.bookId,
          revision,
        ) ||
        ctx.repo.releaseInbox.consideredForDispatch(
          entries[index].id,
          kind,
          target.bookId,
          job.id,
        );
      const unseen = [
        ...new Set(matching.flatMap((match) => match.releaseIndexes)),
      ].filter((index) => !considered(index));
      if (!unseen.length) continue;
      if (activeAcquisition(ctx, target.bookId, target.media)) {
        busy.push(target);
        continue;
      }
      await durableStep(
        ctx,
        "book-rss:dispatch:" + key(target.bookId, target.media),
        () =>
          ctx.repo.transaction(() => {
            ctx.execution?.assertActive();
            if (bookFeedRevision(ctx, target.bookId, target.media) !== revision)
              throw new Error(
                "Book state or edition preferences changed while dispatching RSS candidates",
              );
            const current = unseen.filter((index) => !considered(index));
            if (!current.length) return;
            if (activeAcquisition(ctx, target.bookId, target.media))
              throw new Error(
                "Book acquisition became active before RSS dispatch",
              );
            const note = matching
              .map((match) => match.reason)
              .join("\n")
              .slice(0, 4000);
            // A prior no-selection may only mean the other parts were not in that
            // poll yet. Keep its matched evidence available when a new part arrives.
            const poolIds = [
              ...new Set([
                ...current.map((index) => entries[index].id),
                ...ctx.repo.releaseInbox
                  .consideredEntries(kind, target.bookId)
                  .map((entry) => entry.id),
              ]),
            ].slice(0, 1000);
            const rejected = ctx.repo
              .listReleasesByBook(target.bookId)
              .filter(
                (release) =>
                  release.media_type === target.media &&
                  release.status === "failed",
              );
            const acquire = ctx.repo.createJob({
              type: "acquire",
              bookId: target.bookId,
              payload: {
                bookId: target.bookId,
                media: [target.media],
                feedCandidateIds: poolIds,
                feedRoutingNote: note,
                considerUpgrade: (target.media === "audio"
                  ? ctx.repo.getBook(target.bookId)?.audioStatus : ctx.repo.getBook(target.bookId)?.ebookStatus) === "imported",
                forceAgent: true,
                rejectedUrls: rejected.map((release) => release.url),
                rejectedGuids: rejected.flatMap((release) =>
                  release.provider_guid ? [release.provider_guid] : [],
                ),
                rejectedInfoHashes: rejected.map(
                  (release) => release.info_hash,
                ),
              },
              idempotencyKey:
                "rss-book-acquire:" +
                job.id +
                ":" +
                key(target.bookId, target.media),
            });
            for (const index of current)
              ctx.repo.releaseInbox.consider(
                entries[index].id,
                kind,
                target.bookId,
                revision,
                acquire.id,
                note,
                job.id,
              );
          }),
      );
    }
    if (!busy.length) break;
    if (!ctx.execution)
      throw new Error(
        "Waiting for an active book acquisition requires durable execution",
      );
    await ctx.execution.sleep("book-rss:wait-for-targets:" + wave, 60_000);
    pending = busy;
  }
  return "done";
}
