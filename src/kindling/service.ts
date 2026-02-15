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
  providerGuid?: string | null;
  title: string;
  mediaType: MediaType;
  url: string;
  infoHash?: string | null;
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

function resolveInfoHash(explicitHash?: string | null): string | null {
  if (!explicitHash) return null;
  return normalizeInfoHash(explicitHash);
}

function idempotentResult(repo: KindlingRepo, existing: ReleaseRow): { release: ReleaseRow; jobId: number; idempotent: boolean } {
  const existingJob = repo
    .listJobsByType("download")
    .find((job) => job.release_id === existing.id && (job.status === "queued" || job.status === "running"));
  return {
    release: existing,
    jobId: existingJob?.id ?? -1,
    idempotent: true,
  };
}

export async function runSearch(settings: AppSettings, request: SearchRequest) {
  const results = await searchTorznab(settings.torznab, request.query, request.media);

  const needle = request.query.trim().toLowerCase();
  const words = needle.split(/\s+/).filter(Boolean);
  const setMarkers = ["box set", "collection", "complete", "omnibus", "books 1-7", "1-3", "series"];
  const audioMarkers = ["m4b", "m4a", "mp3", "aac", "flac", "opus", "ogg", "wav", "audiobook", "audio book", "audio"];
  const ebookMarkers = ["epub", "pdf", "mobi", "azw", "ebook", "e-book", "djvu", "cbz", "cbr"];
  const hasAudioMarker = (title: string): boolean => audioMarkers.some((marker) => title.includes(marker));
  const hasEbookMarker = (title: string): boolean => ebookMarkers.some((marker) => title.includes(marker));

  const filtered = results.filter((row) => {
    const lower = row.title.toLowerCase();
    const audio = hasAudioMarker(lower);
    const ebook = hasEbookMarker(lower);
    if (request.media === "audio") {
      return audio && !ebook;
    }
    return ebook && !audio;
  });

  function score(media: MediaType, title: string, seeders: number | null, sizeBytes: number | null): number {
    const lower = title.toLowerCase();
    let value = 0;
    if (needle && lower.includes(needle)) value += 80;
    if (words.length > 0 && words.every((word) => lower.includes(word))) value += 35;
    if (setMarkers.some((marker) => lower.includes(marker))) value -= 120;
    const hasAudio = hasAudioMarker(lower);
    const hasEbook = hasEbookMarker(lower);
    if (media === "audio") {
      if (hasAudio) value += 160;
      if (hasEbook) value -= 220;
    } else {
      if (hasEbook) value += 160;
      if (hasAudio) value -= 220;
    }
    value += Math.min(60, seeders ?? 0);
    if (typeof sizeBytes === "number" && Number.isFinite(sizeBytes)) {
      value -= Math.min(50, Math.round(sizeBytes / (1024 * 1024 * 200)));
    }
    return value;
  }

  return filtered.sort((a, b) => {
    const scoreDiff =
      score(request.media, b.title, b.seeders, b.sizeBytes) - score(request.media, a.title, a.seeders, a.sizeBytes);
    if (scoreDiff !== 0) return scoreDiff;
    const aSize = a.sizeBytes ?? Number.MAX_SAFE_INTEGER;
    const bSize = b.sizeBytes ?? Number.MAX_SAFE_INTEGER;
    return aSize - bSize;
  });
}

export async function runSnatch(
  repo: KindlingRepo,
  settings: AppSettings,
  request: SnatchRequest
): Promise<{ release: ReleaseRow; jobId: number; idempotent: boolean }> {
  const book = repo.getBookRow(request.bookId);
  if (!book) {
    throw new Error(`Book ${request.bookId} not found`);
  }
  if (isMagnet(request.url)) {
    throw new Error("Magnet URLs are not supported for snatch; provide a .torrent URL");
  }

  const providerGuid = request.providerGuid?.trim() || null;
  const explicitHash = resolveInfoHash(request.infoHash);

  if (explicitHash) {
    const existingByHash = repo.findReleaseByInfoHash(explicitHash);
    if (existingByHash) {
      if (existingByHash.book_id !== request.bookId) {
        throw new Error(`Infohash already linked to book ${existingByHash.book_id}`);
      }
      return idempotentResult(repo, existingByHash);
    }
  }

  if (providerGuid) {
    const existingByGuid = repo.findReleaseByProviderGuid(request.provider, providerGuid);
    if (existingByGuid) {
      if (existingByGuid.book_id !== request.bookId) {
        throw new Error(`Provider guid already linked to book ${existingByGuid.book_id}`);
      }
      return idempotentResult(repo, existingByGuid);
    }
  }

  const torrentBytes = await fetchTorrentBytes(request.url);
  const derivedHash = infoHashFromTorrentBytes(torrentBytes);

  const existingByDerived = repo.findReleaseByInfoHash(derivedHash);
  if (existingByDerived) {
    if (existingByDerived.book_id !== request.bookId) {
      throw new Error(`Infohash already linked to book ${existingByDerived.book_id}`);
    }
    return idempotentResult(repo, existingByDerived);
  }

  const client = new RtorrentClient(settings.rtorrent);
  await client.loadRawStart(torrentBytes);

  const release = repo.createRelease({
    bookId: request.bookId,
    provider: request.provider,
    providerGuid,
    title: request.title,
    mediaType: request.mediaType,
    infoHash: derivedHash,
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

export async function triggerAutoAcquire(repo: KindlingRepo, bookId: number, media: MediaType[] = ["audio", "ebook"]): Promise<number> {
  const mediaList = media.length > 0 ? Array.from(new Set(media)) : ["audio", "ebook"];
  const scanJob = repo.createJob({
    type: "scan",
    bookId,
    payload: {
      bookId,
      media: mediaList,
    },
  });
  return scanJob.id;
}
