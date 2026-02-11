import { KindlingRepo } from "./repo";
import { RtorrentClient } from "./rtorrent";
import { normalizeInfoHash } from "./torrent";
import { searchTorznab } from "./torznab";
import type { AppSettings, MediaType, ReleaseRow } from "./types";

type SearchRequest = {
  query: string;
  media: MediaType;
};

type SnatchRequest = {
  bookId: number;
  provider: string;
  title: string;
  mediaType: MediaType;
  url: string;
  infoHash: string;
  sizeBytes?: number | null;
};

function isMagnet(url: string): boolean {
  return url.trim().toLowerCase().startsWith("magnet:?");
}

async function fetchTorrentBytes(url: string): Promise<Uint8Array> {
  const response = await fetch(url, { method: "GET" });
  if (!response.ok) {
    throw new Error(`Failed to download torrent: ${response.status}`);
  }
  return new Uint8Array(await response.arrayBuffer());
}

function resolveInfoHash(explicitHash: string): string {
  return normalizeInfoHash(explicitHash);
}

export async function runSearch(settings: AppSettings, request: SearchRequest) {
  const results = await searchTorznab(settings.torznab, request.query, request.media);

  const needle = request.query.trim().toLowerCase();
  const words = needle.split(/\s+/).filter(Boolean);
  const setMarkers = ["box set", "collection", "complete", "omnibus", "books 1-7", "1-3", "series"];

  function score(title: string, seeders: number | null, sizeBytes: number | null): number {
    const lower = title.toLowerCase();
    let value = 0;
    if (needle && lower.includes(needle)) value += 80;
    if (words.length > 0 && words.every((word) => lower.includes(word))) value += 35;
    if (setMarkers.some((marker) => lower.includes(marker))) value -= 120;
    value += Math.min(60, seeders ?? 0);
    if (typeof sizeBytes === "number" && Number.isFinite(sizeBytes)) {
      value -= Math.min(50, Math.round(sizeBytes / (1024 * 1024 * 200)));
    }
    return value;
  }

  return results.sort((a, b) => {
    const scoreDiff = score(b.title, b.seeders, b.sizeBytes) - score(a.title, a.seeders, a.sizeBytes);
    if (scoreDiff !== 0) return scoreDiff;
    const aSize = a.sizeBytes ?? Number.MAX_SAFE_INTEGER;
    const bSize = b.sizeBytes ?? Number.MAX_SAFE_INTEGER;
    return aSize - bSize;
  });
}

export async function runSnatch(repo: KindlingRepo, settings: AppSettings, request: SnatchRequest): Promise<{ release: ReleaseRow; jobId: number; idempotent: boolean }> {
  const book = repo.getBookRow(request.bookId);
  if (!book) {
    throw new Error(`Book ${request.bookId} not found`);
  }
  if (isMagnet(request.url)) {
    throw new Error("Magnet URLs are not supported for snatch; provide a .torrent URL");
  }

  const resolvedHash = resolveInfoHash(request.infoHash);
  const existing = repo.findReleaseByInfoHash(resolvedHash);
  if (existing) {
    if (existing.book_id !== request.bookId) {
      throw new Error(`Infohash already linked to book ${existing.book_id}`);
    }
    const existingJob = repo
      .listJobsByType("download")
      .find((job) => job.release_id === existing.id && (job.status === "queued" || job.status === "running"));
    return {
      release: existing,
      jobId: existingJob?.id ?? -1,
      idempotent: true,
    };
  }

  const client = new RtorrentClient(settings.rtorrent);
  const torrentBytes = await fetchTorrentBytes(request.url);
  await client.loadRawStart(torrentBytes);

  const release = repo.createRelease({
    bookId: request.bookId,
    provider: request.provider,
    title: request.title,
    mediaType: request.mediaType,
    infoHash: resolvedHash,
    sizeBytes: request.sizeBytes ?? null,
    url: request.url,
    status: "snatched",
  });

  const job = repo.createJob({
    type: "download",
    releaseId: release.id,
    bookId: release.book_id,
    payload: {
      infoHash: release.info_hash,
    },
  });

  return {
    release,
    jobId: job.id,
    idempotent: false,
  };
}

export async function triggerAutoAcquire(repo: KindlingRepo, bookId: number): Promise<number> {
  const scanJob = repo.createJob({
    type: "scan",
    bookId,
    payload: {
      bookId,
      media: ["audio", "ebook"],
    },
  });
  return scanJob.id;
}
