import { KindlingRepo } from "./repo";
import { RtorrentClient } from "./rtorrent";
import { infoHashFromTorrentBytes, normalizeInfoHash } from "./torrent";
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
  sizeBytes?: number | null;
  infoHash?: string | null;
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

async function resolveInfoHash(url: string, explicitHash?: string | null): Promise<{ hash: string; torrentBytes?: Uint8Array }> {
  if (explicitHash) {
    return { hash: normalizeInfoHash(explicitHash) };
  }

  if (isMagnet(url)) {
    const match = /(?:\?|&)xt=urn:btih:([^&]+)/i.exec(url);
    if (!match) {
      throw new Error("Magnet URL missing btih infohash");
    }
    return { hash: normalizeInfoHash(decodeURIComponent(match[1] ?? "")) };
  }

  const torrentBytes = await fetchTorrentBytes(url);
  return { hash: infoHashFromTorrentBytes(torrentBytes), torrentBytes };
}

export async function runSearch(settings: AppSettings, request: SearchRequest) {
  const results = await searchTorznab(settings.torznab, request.query, request.media);
  return results.sort((a, b) => {
    const aSeed = a.seeders ?? 0;
    const bSeed = b.seeders ?? 0;
    if (bSeed !== aSeed) return bSeed - aSeed;
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

  const resolved = await resolveInfoHash(request.url, request.infoHash);
  const existing = repo.findReleaseByInfoHash(resolved.hash);
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
  if (resolved.torrentBytes) {
    await client.loadRawStart(resolved.torrentBytes);
  } else if (isMagnet(request.url)) {
    await client.loadStart(request.url);
  } else {
    const bytes = await fetchTorrentBytes(request.url);
    await client.loadRawStart(bytes);
  }

  const release = repo.createRelease({
    bookId: request.bookId,
    provider: request.provider,
    title: request.title,
    mediaType: request.mediaType,
    infoHash: resolved.hash,
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
