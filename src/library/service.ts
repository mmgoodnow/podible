import { BooksRepo } from "../repo";
import { ensureBookTorrentAdded } from "../media/book-torrents";
import type { TorrentClient } from "../media/torrent-client";
import { getOrFetchCachedTorrentBytes } from "./torrent-cache";
import { infoHashFromTorrentBytes, normalizeInfoHash } from "./torrent";
import { searchTorznabBatch } from "./torznab";
import { bookIndexerSources } from "./indexer-discovery";
import { IndexerError, IndexerSearchError } from "./indexer-errors";
import { inferLanguageFromReleaseTitles, normalizeManifestationLanguageCode } from "./language";
import type { IndexerSearchBatch, TorznabResult } from "./torznab";
import type { AppSettings, MediaType, ReleaseRow } from "../app-types";
import type { WorkerContext } from "../worker/context";
import { releaseOfferFromResult } from "../media/release-offer";
import { rememberSelectedCopies } from "../media/selected-copies";
import { queueSelectedCrossSeeds } from "../media/cross-seed";
import { z } from "zod";
import { bookManualSelectionKey, stageBookUpgrade, stageBookRepair, type BookUpgradeBaseline } from "./quality-state";
import type { BookQualityAssessment } from "./quality";
import { audioEditionPart, PENDING_AUDIO_EDITION_SCORE, recordAudioEditionPart, stageAudioEdition } from "./audio-editions";

type SearchRequest = {
  query: string;
  media: MediaType;
};

export type SnatchRequest = {
  bookId: number;
  provider: string;
  providerGuid?: string | null;
  title: string;
  mediaType: MediaType;
  url: string;
  infoHash?: string | null;
  sizeBytes?: number | null;
  manifestationId?: number | null;
  sequenceInManifestation?: number | null;
  selectionNote?: string | null;
  manifestationLanguage?: string | null;
};

export type SnatchGroupRequest = {
  bookId: number;
  mediaType: MediaType;
  forceManifestation?: boolean;
  manifestation: {
    label: string | null;
    editionNote: string | null;
    selectionNote?: string | null;
    language?: string | null;
  };
  parts: Array<{
    provider: string;
    providerGuid?: string | null;
    title: string;
    url: string;
    infoHash?: string | null;
    sizeBytes?: number | null;
  }>;
};

export type SnatchResult = { release: ReleaseRow; jobId: number | null; idempotent: boolean };

export type SnatchGroupResult = {
  manifestationId: number | null;
  results: SnatchResult[];
};

type AutoAcquireOptions = {
  forceAgent?: boolean;
  priorFailure?: boolean;
  requireResult?: boolean;
  notifyOnFailure?: boolean;
  failureContext?: string;
  rejectedUrls?: string[];
  rejectedGuids?: string[];
  rejectedInfoHashes?: string[];
};

type SnatchRuntimeOptions = {
  /** Worker-owned acquisition only; public/manual snatches establish a replacement hold. */
  automatic?: boolean;
  bookUpgrade?: { baseline: BookUpgradeBaseline; preflight: BookQualityAssessment };
  bookRepair?: { reviewJobId: number };
  audioEditionRequestId?: number;
  onLog?: (message: string) => void;
  preferAgentImport?: boolean;
  execution?: WorkerContext["execution"];
  operationKey?: string;
  stepPrefix?: string;
  bookTorrentClient?: TorrentClient;
  /** Internal server search evidence only; never exposed on snatch RPC inputs. */
  selectedCopies?: { selected: TorznabResult; alternatives: TorznabResult[] };
  selectedCopiesByPart?: Array<{ selected: TorznabResult; alternatives: TorznabResult[] }>;
};

const bookSelectionKey = (releaseId: number) => `book-release-selection:${releaseId}`;
const selectionResultSchema = z.object({
  title: z.string(), provider: z.string(), mediaType: z.enum(["audio", "ebook"]),
  sizeBytes: z.number().int().nullable(), url: z.url(), guid: z.string().nullable(),
  infoHash: z.string().nullable(), seeders: z.number().int().nullable(), leechers: z.number().int().nullable(),
  raw: z.record(z.string(), z.unknown()),
});
const selectionIntentSchema = z.object({
  version: z.literal(1), bookId: z.number().int().positive(), media: z.enum(["audio", "ebook"]),
  infoHash: z.string().regex(/^[a-f0-9]{40}$/), selectedOfferKey: z.string().regex(/^[a-f0-9]{64}$/),
  selected: selectionResultSchema, alternatives: z.array(selectionResultSchema).min(1).max(2000),
}).strict();

/** Independent of Reflow: internal/manual callers can crash after resume but before release publication. */
function retainBookSelectionIntent(repo: BooksRepo, request: SnatchRequest, infoHash: string,
  evidence: SnatchRuntimeOptions["selectedCopies"]) {
  const prefix = `book-selected-intent:${request.bookId}:${request.mediaType}:${infoHash}:`;
  const index = `book-selected-intents:${request.bookId}:${request.mediaType}:${infoHash}`;
  return repo.transaction(() => {
    const keys = z.array(z.string().regex(/^[a-f0-9]{64}$/)).parse(repo.getJsonState(index) ?? []);
    if (evidence?.alternatives.length) {
      const key = releaseOfferFromResult(evidence.selected).key;
      // First-write-wins: later searches cannot mutate already dispatched evidence.
      if (repo.getJsonState(prefix + key) === null) repo.setJsonState(prefix + key, selectionIntentSchema.parse({
        version: 1, bookId: request.bookId, media: request.mediaType, infoHash, selectedOfferKey: key,
        selected: evidence.selected, alternatives: evidence.alternatives,
      }));
      if (!keys.includes(key)) keys.push(key);
      repo.setJsonState(index, keys);
    }
    return keys.map(key => {
      const intent = selectionIntentSchema.parse(repo.getJsonState(prefix + key));
      if (intent.bookId !== request.bookId || intent.media !== request.mediaType || intent.infoHash !== infoHash ||
        intent.selectedOfferKey !== key || releaseOfferFromResult(intent.selected).key !== key ||
        intent.selected.mediaType !== request.mediaType || intent.alternatives.some(copy => copy.mediaType !== request.mediaType))
        throw new Error("Retained book selection intent changed identity");
      return { selected: intent.selected, alternatives: intent.alternatives };
    });
  });
}

function retainBookSelectedCopies(repo: BooksRepo, release: ReleaseRow, evidence: SnatchRuntimeOptions["selectedCopies"]) {
  if (!evidence || !evidence.alternatives.length) return;
  if (evidence.selected.mediaType !== release.media_type)
    throw new Error("Book copy evidence has another release media type");
  const selected = evidence.selected;
  const offer = releaseOfferFromResult(selected);
  rememberSelectedCopies(repo, { media: release.media_type, targetId: release.book_id }, release.id,
    offer, evidence.alternatives.map(copy => releaseOfferFromResult(copy)));
  const keys = z.array(z.string().min(1)).parse(repo.getJsonState(bookSelectionKey(release.id)) ?? []);
  repo.setJsonState(bookSelectionKey(release.id), [...new Set([...keys, offer.key])]);
}

/** Imported data may predate newly retained search evidence; import replay must reconsider it too. */
export function queueBookSelectedCopies(repo: BooksRepo, release: ReleaseRow) {
  return repo.transaction(() => {
    const keys = z.array(z.string().min(1)).parse(repo.getJsonState(bookSelectionKey(release.id)) ?? []);
    return keys.flatMap(key => queueSelectedCrossSeeds(repo,
      { media: release.media_type, targetId: release.book_id }, release.id, key));
  });
}

type RankedSearchResult = {
  result: TorznabResult;
  score: number;
};

function isMagnet(url: string): boolean {
  return url.trim().toLowerCase().startsWith("magnet:?");
}

function resolveInfoHash(explicitHash?: string | null): string | null {
  if (!explicitHash) return null;
  return normalizeInfoHash(explicitHash);
}

function idempotentResult(repo: BooksRepo, existing: ReleaseRow): SnatchResult {
  const existingJob = repo
    .listReleaseDownloadJobs(existing.id)
    .find((job) => job.status === "queued" || job.status === "running");
  return {
    release: existing,
    jobId: existingJob?.id ?? null,
    idempotent: true
  };
}

const setMarkers = ["box set", "collection", "complete", "omnibus", "books 1-7", "1-3", "series"];
const audioMarkers = ["m4b", "m4a", "mp3", "aac", "flac", "opus", "ogg", "wav", "audiobook", "audio book", "audio"];
const ebookMarkers = ["epub", "pdf", "mobi", "azw", "ebook", "e-book", "djvu", "cbz", "cbr"];

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function hasMarker(title: string, marker: string): boolean {
  const pattern = escapeRegExp(marker).replace(/\\ /g, "\\s+");
  return new RegExp(`(?:^|[^a-z0-9])${pattern}(?:$|[^a-z0-9])`, "i").test(title);
}

function hasAudioMarker(title: string): boolean {
  return audioMarkers.some((marker) => hasMarker(title, marker));
}

function hasEbookMarker(title: string): boolean {
  return ebookMarkers.some((marker) => hasMarker(title, marker));
}

function mediaMatchTier(media: MediaType, title: string): 0 | 1 | 2 {
  const audio = hasAudioMarker(title);
  const ebook = hasEbookMarker(title);
  if (media === "audio") {
    if (ebook && !audio) return 0;
    return audio ? 2 : 1;
  }
  if (audio && !ebook) return 0;
  return ebook ? 2 : 1;
}

function scoreSearchResult(media: MediaType, query: string, row: TorznabResult): number {
  const needle = query.trim().toLowerCase();
  const words = needle.split(/\s+/).filter(Boolean);
  const lower = row.title.toLowerCase();
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
  value += Math.min(60, row.seeders ?? 0);
  if (typeof row.sizeBytes === "number" && Number.isFinite(row.sizeBytes)) {
    value -= Math.min(50, Math.round(row.sizeBytes / (1024 * 1024 * 200)));
  }
  return value;
}

export function rankSearchResults(query: string, media: MediaType, results: TorznabResult[]): RankedSearchResult[] {
  return results
    .map((result) => ({
      result,
      score:
        scoreSearchResult(media, query, result) + (mediaMatchTier(media, result.title.toLowerCase()) === 2 ? 80 : 0)
    }))
    .sort((a, b) => {
      const scoreDiff = b.score - a.score;
      if (scoreDiff !== 0) return scoreDiff;
      const aSize = a.result.sizeBytes ?? Number.MAX_SAFE_INTEGER;
      const bSize = b.result.sizeBytes ?? Number.MAX_SAFE_INTEGER;
      return aSize - bSize;
    });
}

export async function runSearch(settings: AppSettings, request: SearchRequest) {
  const batch = await runSearchBatch(settings, request);
  if (!batch.results.length && batch.failures.length) throw new IndexerSearchError(batch.failures);
  return batch.results;
}

export async function runSearchBatch(settings: AppSettings, request: SearchRequest, options?: import("../media/indexer-gateway").SearchOptions): Promise<IndexerSearchBatch> {
  let sources;
  try {
    sources = await bookIndexerSources(settings, request.media);
  } catch (error) {
    if (!(error instanceof IndexerError)) throw error;
    return { results: [], failures: [error.failure] };
  }
  const batch = await searchTorznabBatch(sources, request.query, request.media, options);
  return { ...batch, results: rankSearchResults(request.query, request.media, batch.results).map((entry) => entry.result) };
}

export async function runSnatch(
  repo: BooksRepo,
  settings: AppSettings,
  request: SnatchRequest,
  runtime: SnatchRuntimeOptions = {}
): Promise<SnatchResult> {
  const snatchLog = (message: string): void => {
    if (runtime.onLog) {
      runtime.onLog(message);
    }
  };
  const book = repo.getBookRow(request.bookId);
  if (!book) {
    throw new Error(`Book ${request.bookId} not found`);
  }
  const retainManualChoice = () => {
    if (!runtime.automatic && runtime.audioEditionRequestId === undefined) repo.setJsonState(bookManualSelectionKey(book.id, request.mediaType), {
      releaseTitle: request.title, selectedAt: new Date().toISOString(),
    });
  };
  const evidence = runtime.selectedCopies;
  if (evidence && (evidence.selected.mediaType !== request.mediaType || evidence.selected.provider !== request.provider ||
    evidence.selected.title !== request.title || (evidence.selected.guid?.trim() || null) !== (request.providerGuid?.trim() || null) ||
    evidence.selected.url !== request.url || evidence.alternatives.some(copy => copy.mediaType !== request.mediaType)))
    throw new Error("Book copy evidence does not belong to the selected release");
  if (isMagnet(request.url)) {
    throw new Error("Magnet URLs are not supported for snatch; provide a .torrent URL");
  }

  const providerGuid = request.providerGuid?.trim() || null;
  const explicitHash = resolveInfoHash(request.infoHash);
  let manifestationId = request.manifestationId ?? null;
  let sequenceInManifestation = request.sequenceInManifestation ?? null;
  if (sequenceInManifestation !== null && (!Number.isInteger(sequenceInManifestation) || sequenceInManifestation < 0)) {
    throw new Error("sequenceInManifestation must be a nonnegative integer");
  }
  if (manifestationId !== null) {
    const manifestation = repo.getManifestation(manifestationId);
    if (!manifestation || manifestation.book_id !== request.bookId) {
      throw new Error(`Manifestation ${manifestationId} not found for book ${request.bookId}`);
    }
    const expectedKind = request.mediaType === "ebook" ? "ebook" : "audio";
    if (manifestation.kind !== expectedKind) {
      throw new Error(`Manifestation ${manifestationId} is ${manifestation.kind}, not ${expectedKind}`);
    }
  }
  snatchLog(
    `[snatch] start book=${request.bookId} media=${request.mediaType} provider=${JSON.stringify(request.provider)} title=${JSON.stringify(request.title)} guid=${JSON.stringify(providerGuid)} explicit_hash=${JSON.stringify(explicitHash)} manifestation=${manifestationId ?? "auto"} sequence=${sequenceInManifestation ?? "auto"}`
  );

  const existingByGuid = providerGuid ? repo.findReleaseByProviderGuid(request.provider, providerGuid) : null;
  const existingByClaimedHash = explicitHash ? repo.findReleaseByInfoHash(explicitHash) : null;

  snatchLog(`[snatch] fetch_torrent start`);
  const { bytes: torrentBytes, cacheHit } = await getOrFetchCachedTorrentBytes(
    repo,
    {
      provider: request.provider,
      providerGuid,
      url: request.url,
      infoHash: explicitHash
    },
    {
      onLog: (line) => snatchLog(line),
      // Reacquisition must still check mutable provider listings for changed content.
      revalidate: existingByGuid !== null || existingByClaimedHash !== null,
    }
  );
  snatchLog(`[snatch] fetch_torrent ok bytes=${torrentBytes.byteLength} cache_hit=${cacheHit}`);
  const derivedHash = infoHashFromTorrentBytes(torrentBytes);
  snatchLog(`[snatch] derived_hash=${derivedHash}`);

  if (runtime.audioEditionRequestId !== undefined) {
    const { part } = audioEditionPart(repo, runtime.audioEditionRequestId, manifestationId, sequenceInManifestation);
    if (part && repo.getRelease(part.releaseId)?.info_hash !== derivedHash)
      throw new Error("Audiobook edition part changed torrent identity");
  }

  const checkEditionReplay = (existing: ReleaseRow) => {
    if (runtime.audioEditionRequestId === undefined) return;
    const { part } = audioEditionPart(repo, runtime.audioEditionRequestId, manifestationId, sequenceInManifestation);
    if (part?.releaseId !== existing.id) throw new Error("Audiobook edition part is already linked to another acquisition");
  };

  const retainedEvidence = retainBookSelectionIntent(repo, request, derivedHash, runtime.selectedCopies);
  const existingByDerived = repo.findReleaseByInfoHash(derivedHash);
  if (existingByDerived) {
    if (existingByDerived.book_id !== request.bookId) {
      throw new Error(`Infohash already linked to book ${existingByDerived.book_id}`);
    }
    snatchLog(
      `[snatch] dedupe reason=derived_hash release=${existingByDerived.id} info_hash=${existingByDerived.info_hash} provider_guid=${JSON.stringify(existingByDerived.provider_guid)}`
    );
    return repo.transaction(() => {
      checkEditionReplay(existingByDerived);
      retainManualChoice();
      for (const evidence of retainedEvidence) retainBookSelectedCopies(repo, existingByDerived, evidence);
      if (repo.listAssetsByBook(existingByDerived.book_id).some(asset => asset.source_release_id === existingByDerived.id))
        queueBookSelectedCopies(repo, existingByDerived);
      return idempotentResult(repo, existingByDerived);
    });
  }

  snatchLog(`[snatch] private Transmission add begin`);
  try {
    await ensureBookTorrentAdded(repo, settings, request.bookId, torrentBytes, {
      currentMetainfo: () => repo.trackerAnnounces.currentMetainfo(request.provider, torrentBytes),
      bookTorrentClient: runtime.bookTorrentClient,
      assertActive: () => runtime.execution?.assertActive(),
    });
    snatchLog(`[snatch] private Transmission add ok`);
  } catch (error) {
    snatchLog(`[snatch] private Transmission add error=${JSON.stringify((error as Error).message)}`);
    throw error;
  }

  return repo.transaction(() => {
    retainManualChoice();
    const existing = repo.findReleaseByInfoHash(derivedHash);
    if (existing) {
      if (existing.book_id !== request.bookId) throw new Error(`Infohash already linked to book ${existing.book_id}`);
      checkEditionReplay(existing);
      for (const evidence of retainedEvidence) retainBookSelectedCopies(repo, existing, evidence);
      if (repo.listAssetsByBook(existing.book_id).some(asset => asset.source_release_id === existing.id))
        queueBookSelectedCopies(repo, existing);
      return idempotentResult(repo, existing);
    }
    if (manifestationId === null) {
      manifestationId = repo.addManifestation({
        bookId: request.bookId,
        kind: request.mediaType === "ebook" ? "ebook" : "audio",
        selectionNote: request.selectionNote ?? null,
        language:
          normalizeManifestationLanguageCode(request.manifestationLanguage) ??
          inferLanguageFromReleaseTitles([request.title])
      }).id;
      sequenceInManifestation ??= 0;
    }

    const release = repo.createRelease({
      bookId: request.bookId,
      provider: request.provider,
      providerGuid,
      title: request.title,
      mediaType: request.mediaType,
      infoHash: derivedHash,
      sizeBytes: request.sizeBytes ?? null,
      url: request.url,
      status: "snatched"
    });
    repo.bookTorrents.attach(derivedHash, release.id);
    for (const evidence of retainedEvidence) retainBookSelectedCopies(repo, release, evidence);

    const job = repo.createJob({
      type: "download",
      releaseId: release.id,
      bookId: release.book_id,
      payload: {
        infoHash: release.info_hash,
        preferAgentImport: runtime.preferAgentImport === true,
        manifestationId,
        sequenceInManifestation,
        repairReviewJobId: runtime.bookRepair?.reviewJobId,
        audioEditionRequestId: runtime.audioEditionRequestId,
      }
    });
    if (runtime.audioEditionRequestId !== undefined)
      recordAudioEditionPart(repo, runtime.audioEditionRequestId, manifestationId, sequenceInManifestation,
        { releaseId: release.id, downloadJobId: job.id });
    snatchLog(`[snatch] created release=${release.id} download_job=${job.id} info_hash=${release.info_hash}`);

    return {
      release,
      jobId: job.id,
      idempotent: false
    };
  });
}

export async function runSnatchGroup(
  repo: BooksRepo,
  settings: AppSettings,
  request: SnatchGroupRequest,
  runtime: SnatchRuntimeOptions = {}
): Promise<SnatchGroupResult> {
  if (request.parts.length === 0) {
    throw new Error("At least one release is required");
  }
  const needsExplicitManifestation =
    runtime.audioEditionRequestId !== undefined ||
    runtime.bookUpgrade !== undefined ||
    runtime.bookRepair !== undefined ||
    request.forceManifestation === true ||
    request.parts.length > 1 ||
    request.manifestation.label !== null ||
    request.manifestation.editionNote !== null ||
    request.manifestation.selectionNote != null;
  const language =
    normalizeManifestationLanguageCode(request.manifestation.language) ??
    inferLanguageFromReleaseTitles(request.parts.map((part) => part.title));
  const createManifestation = () => repo.transaction(() => {
    const input = {
      bookId: request.bookId,
      kind: request.mediaType === "ebook" ? ("ebook" as const) : ("audio" as const),
      label: request.manifestation.label,
      editionNote: request.manifestation.editionNote,
      selectionNote: request.manifestation.selectionNote ?? null,
      language,
      preferredScore: runtime.audioEditionRequestId !== undefined ? PENDING_AUDIO_EDITION_SCORE :
        runtime.bookUpgrade || runtime.bookRepair ? -1 : 0
    };
    const id = runtime.operationKey
      ? repo.ensureManifestation(runtime.operationKey, input).id
      : repo.addManifestation(input).id;
    if (runtime.audioEditionRequestId !== undefined) stageAudioEdition(repo, runtime.audioEditionRequestId, id, request.parts.length);
    return id;
  });
  const prefix = runtime.stepPrefix ?? "snatch";
  let manifestationId = needsExplicitManifestation
    ? runtime.execution
      ? await runtime.execution.run(`${prefix}:manifestation`, createManifestation)
      : createManifestation()
    : null;
  const results: SnatchResult[] = [];
  if (runtime.bookRepair && manifestationId !== null) {
    const repair = runtime.bookRepair, candidateId = manifestationId;
    const stage = () => stageBookRepair(repo, { bookId: request.bookId, media: request.mediaType,
      candidateId, parts: request.parts.length, ...repair });
    if (runtime.execution) await runtime.execution.run(`${prefix}:stage-repair`, stage);
    else stage();
  }
  if (runtime.bookUpgrade && manifestationId !== null) {
    const upgrade = runtime.bookUpgrade, candidateId = manifestationId;
    const stage = () => stageBookUpgrade(repo, { bookId: request.bookId, media: request.mediaType,
      candidateId, parts: request.parts.length, ...upgrade });
    if (runtime.execution) await runtime.execution.run(`${prefix}:stage-upgrade`, stage);
    else stage();
  }
  try {
    for (const [index, part] of request.parts.entries()) {
      const snatch = () =>
        runSnatch(
          repo,
          settings,
          {
            ...part,
            bookId: request.bookId,
            mediaType: request.mediaType,
            manifestationId,
            sequenceInManifestation: needsExplicitManifestation ? index : null,
            manifestationLanguage: language
          },
          { ...runtime, selectedCopies: runtime.selectedCopiesByPart?.[index] ?? runtime.selectedCopies }
        );
      results.push(await (runtime.execution ? runtime.execution.run(`${prefix}:part:${index}`, snatch) : snatch()));
    }
  } finally {
    if (runtime.audioEditionRequestId === undefined && !runtime.execution && manifestationId !== null && repo.pruneEmptyManifestationIfUnreferenced(manifestationId)) {
      manifestationId = null;
    }
  }
  if (runtime.audioEditionRequestId === undefined && runtime.execution && manifestationId !== null) {
    const id = manifestationId;
    manifestationId = await runtime.execution.run(`${prefix}:cleanup`, () =>
      repo.pruneEmptyManifestationIfUnreferenced(id) ? null : id
    );
  }
  return { manifestationId, results };
}

export async function triggerAutoAcquire(
  repo: BooksRepo,
  bookId: number,
  media: MediaType[] = ["audio", "ebook"],
  options: AutoAcquireOptions = {}
): Promise<number> {
  const mediaList = media.length > 0 ? Array.from(new Set(media)) : ["audio", "ebook"];
  const acquireJob = repo.createJob({
    type: "acquire",
    bookId,
    payload: {
      bookId,
      media: mediaList,
      forceAgent: options.forceAgent === true,
      priorFailure: options.priorFailure === true,
      requireResult: options.requireResult === true,
      notifyOnFailure: options.notifyOnFailure === true,
      failureContext: options.failureContext ?? null,
      rejectedUrls: options.rejectedUrls ?? [],
      rejectedGuids: options.rejectedGuids ?? [],
      rejectedInfoHashes: options.rejectedInfoHashes ?? []
    }
  });
  return acquireJob.id;
}
