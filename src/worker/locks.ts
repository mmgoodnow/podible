import type { JobRow, MediaType } from "../app-types";
import type { BooksRepo } from "../repo";

import type { AcquirePayload } from "./acquire";

const GLOBAL_LOCK_KEY = "*";

function parseJson<T>(value: string | null): T | null {
  if (!value) return null;
  try {
    return JSON.parse(value) as T;
  } catch {
    return null;
  }
}

function unique(keys: Array<string | null | undefined>): string[] {
  return Array.from(new Set(keys.filter((value): value is string => Boolean(value))));
}

function mediaLaneKey(bookId: number | null | undefined, mediaType: MediaType | null | undefined): string | null {
  if (!bookId || !mediaType) return null;
  return `book:${bookId}:media:${mediaType}`;
}

function releaseLockKeys(repo: BooksRepo, releaseId: number | null | undefined): string[] {
  if (!releaseId) return [];
  const release = repo.getRelease(releaseId);
  if (!release) return [`release:${releaseId}`];
  return unique([`release:${release.id}`, mediaLaneKey(release.book_id, release.media_type)]);
}

export function jobLockKeys(repo: BooksRepo, job: JobRow): string[] {
  if (job.type === "full_library_refresh") {
    return [GLOBAL_LOCK_KEY];
  }

  if (job.type === "acquire") {
    const payload = parseJson<AcquirePayload>(job.payload_json) ?? {};
    const bookId = payload.bookId ?? job.book_id ?? null;
    const mediaList: MediaType[] = payload.media && payload.media.length > 0 ? payload.media : ["audio", "ebook"];
    return unique(mediaList.map((media) => mediaLaneKey(bookId, media)));
  }

  if (job.type === "download" || job.type === "import" || job.type === "reconcile") {
    return releaseLockKeys(repo, job.release_id);
  }

  if (job.type === "chapter_analysis") {
    const payload = parseJson<{ assetId?: number }>(job.payload_json) ?? {};
    const assetId = payload.assetId ?? null;
    if (!assetId) return [];
    const asset = repo.getAsset(assetId);
    return unique([`asset:${assetId}`, mediaLaneKey(asset?.book_id ?? job.book_id ?? null, "audio")]);
  }

  return [];
}

export function lockSetsConflict(activeLocks: Set<string>, candidateKeys: string[]): boolean {
  if (candidateKeys.length === 0) return activeLocks.has(GLOBAL_LOCK_KEY);
  if (activeLocks.has(GLOBAL_LOCK_KEY) || candidateKeys.includes(GLOBAL_LOCK_KEY)) return activeLocks.size > 0;
  return candidateKeys.some((key) => activeLocks.has(key));
}
