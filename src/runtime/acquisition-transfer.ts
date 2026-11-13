import { setTimeout as sleep } from "node:timers/promises";
import type { BooksRepo } from "../repo";
import { adminTorrentKey, readAdminTorrents, type AdminTorrentSource, type AdminTransmissionReader } from "../rpc/admin-torrents";
import { transferEvent, transferIntervalMs, type AcquisitionTransferScope as Scope, type AcquisitionTransferSample as Sample } from "./acquisition-transfer-contracts";

export function canWatchTransfer(repo: BooksRepo, userId: number, scope: Scope) {
  if (scope.media === "movie") {
    const request = repo.movies.requestById(scope.id);
    return request?.user_id === userId && request.withdrawn_at === null;
  }
  return Boolean(repo.tv.publishedEpisodeIds(scope.id).length || repo.tv.showRequests(scope.id, userId).some(r => r.withdrawn_at === null));
}

export async function readTransfer(repo: BooksRepo, scope: Scope, transmission?: AdminTransmissionReader | null): Promise<Sample | null> {
  let sources: AdminTorrentSource[];
  if (scope.media === "movie") {
    const request = repo.movies.requestById(scope.id);
    if (!request || request.withdrawn_at !== null) return null;
    const acquisition = repo.movies.latestAcquisition(request.target_id);
    if (!acquisition || acquisition.finished_at || acquisition.job_id === null || repo.isJobCancellationRequested(acquisition.job_id) ||
        !["queued", "running"].includes(repo.getJobStatus(acquisition.job_id) ?? "")) return null;
    if (repo.movies.latestTransferStage(request.target_id, acquisition.id) !== "downloading") return null;
    const bindings = repo.movies.downloads(request.target_id, acquisition.id).filter(row => row.ownership_tag === "Podible Movies");
    const selectedKey = acquisition.decision_json ? JSON.parse(acquisition.decision_json)?.selection?.offerKey : null;
    const matching = selectedKey ? bindings.filter(row => row.offer_key === selectedKey) : bindings;
    const binding = matching.length === 1 ? matching[0] : undefined;
    sources = binding ? [{ media: "movie", binding }] : [];
  } else {
    const active = repo.tv.showDownloadingAcquisitions(scope.id);
    const statuses = repo.getJobStatuses(active.map(a => a.job_id));
    const ids = active.filter(a => ["queued", "running"].includes(statuses.get(a.job_id) ?? "") &&
      !repo.isJobCancellationRequested(a.job_id)).map(a => a.id);
    sources = ids.length ? repo.tv.downloadBindings(ids)
      .map(row => ({ media: "tv", binding: { ...row, id: row.torrentId } })) : [];
  }
  const settings = sources.length ? repo.readSettings() : null;
  if (!settings) return null;
  const { observations } = await readAdminTorrents(sources, settings, Date.now(), transmission);
  if (new Set(sources.map(adminTorrentKey)).size !== observations.size) return null;
  const active = [...observations.values()].filter(row => row.active && !row.seedOnly &&
    ["downloading", "download-queued"].includes(row.state) && row.bytesDone < row.sizeBytes);
  if (!active.length) return null;
  const result = transferEvent.safeParse({ sample: {
    bytesDone: active.reduce((sum, row) => sum + row.bytesDone, 0),
    sizeBytes: active.reduce((sum, row) => sum + row.sizeBytes, 0),
    bytesPerSecond: active.reduce((sum, row) => sum + row.bytesPerSecond, 0),
    etaSeconds: active.some(row => row.bytesPerSecond === 0) ? null
      : Math.max(...active.map(row => Math.ceil((row.sizeBytes - row.bytesDone) / row.bytesPerSecond))),
  } });
  return result.success ? result.data.sample : null;
}

const caches = new WeakMap<BooksRepo, Map<string, { at: number; result: Promise<Sample | null> }>>();
export async function* transferSamples(repo: BooksRepo, scope: Scope, authorized: () => boolean, signal: AbortSignal,
  transmission?: AdminTransmissionReader | null) {
  let cache = caches.get(repo);
  if (!cache) caches.set(repo, cache = new Map());
  const key = `${scope.media}:${scope.id}`;
  while (!signal.aborted && authorized()) {
    const now = Date.now();
    let cached = cache.get(key);
    if (!cached || now - cached.at >= transferIntervalMs) {
      if (cache.size >= 256) cache.delete(cache.keys().next().value!);
      cache.set(key, cached = { at: now, result: readTransfer(repo, scope, transmission).catch(() => null) });
    }
    const sample = await cached.result;
    if (signal.aborted || !authorized()) break;
    yield { sample };
    try { await sleep(transferIntervalMs, undefined, { signal, ref: false }); }
    catch (error) { if (!signal.aborted) throw error; }
  }
}
