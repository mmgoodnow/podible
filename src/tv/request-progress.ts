import { z } from "zod";
import type { JobStatus } from "../app-types";
import type { TvRepo } from "./repo";
import { tvAcquisitionBatch } from "./acquisition";
import type { TvAcquisition, TvEpisode, TvEvent } from "./types";
import type { AdminTorrentObservation } from "../rpc/admin-torrents";

const stage = z.enum(["ready", "queued", "searching", "downloading", "verifying", "publishing", "pack-pending", "waiting", "failed", "upcoming"]);
const count = z.number().int().nonnegative().safe();
const download = z.object({ bytesDone: count, sizeBytes: count.positive(), bytesPerSecond: count.nullable(),
  lastReportedAt: z.iso.datetime().optional() });
const observation = download.omit({ bytesPerSecond: true, lastReportedAt: true }).extend({
  state: z.literal("downloading"), complete: z.literal(false), seedOnly: z.boolean().optional(),
  downloadBytesPerSecond: count.nullable(), observedAt: z.string(),
});
const episodeProgress = z.object({ id: count, number: count, title: z.string(), airDate: z.string().nullable(),
  status: z.enum(["ready", "upcoming", "missing"]) });
type Transfer = z.infer<typeof download>;
const TRANSFER_MAX_AGE_MS = 60_000;
export const tvRequestProgressSchema = z.object({
  activeBatches: z.number().int().nonnegative(),
  searchableAiredEpisodes: z.number().int().nonnegative(),
  lastAttemptAt: z.string().nullable(), lastPublishedAt: z.string().nullable(),
  download: download.nullable(),
  seasons: z.array(z.object({ seasonNumber: z.number().int().nonnegative(), total: z.number().int().nonnegative(),
    available: z.number().int().nonnegative(), future: z.number().int().nonnegative(), stage,
    download: download.nullable(), episodes: z.array(episodeProgress) })),
});

function summarize(transfers: { torrentId: number; sample: Transfer }[]): Transfer | null {
  const unique = [...new Map(transfers.map(t => [t.torrentId, t.sample])).values()];
  if (!unique.length) return null;
  const bytesDone = unique.reduce((sum, t) => sum + t.bytesDone, 0);
  const sizeBytes = unique.reduce((sum, t) => sum + t.sizeBytes, 0);
  const bytesPerSecond = unique.some(t => t.bytesPerSecond === null) ? null
    : unique.reduce((sum, t) => sum + t.bytesPerSecond!, 0);
  const lastReportedAt = unique.flatMap(t => t.lastReportedAt ? [t.lastReportedAt] : []).sort()[0];
  const parsed = download.safeParse({ bytesDone, sizeBytes, bytesPerSecond, ...(lastReportedAt ? { lastReportedAt } : {}) });
  return parsed.success ? parsed.data : null;
}

type LiveTransfer = Pick<AdminTorrentObservation, "state" | "active" | "seedOnly" | "bytesDone" | "sizeBytes" | "bytesPerSecond">;
type ProgressInput = {
  episodes: TvEpisode[]; available: ReadonlySet<number>; events: TvEvent[]; acquisitions: TvAcquisition[];
  jobStatuses: ReadonlyMap<number, JobStatus>;
  readTransfers?: TvRepo["requestDownloadObservations"];
};

function prepareWork({ episodes, available, events, acquisitions, jobStatuses }: ProgressInput) {
  const latestEvents = new Map(events.flatMap(e => e.acquisitionId === null ? [] : [[e.acquisitionId, e] as const]));
  const missing = episodes.filter(ep => !available.has(ep.id));
  const legacyScope = tvAcquisitionBatch(missing).map(ep => ep.id);
  const work = acquisitions.map(a => {
    const status = a.job_id ? jobStatuses.get(a.job_id) : undefined;
    const scoped = z.object({ batchEpisodeIds: z.array(z.number().int().positive()).optional(),waitingForSources:z.boolean().optional() })
      .safeParse(a.decision_json ? JSON.parse(a.decision_json) : {});
    const ids = scoped.success ? scoped.data.batchEpisodeIds ?? legacyScope : legacyScope;
    const current = !a.finished_at && (status === "queued" || status === "running");
    const last = latestEvents.get(a.id);
    const runningStage = scoped.success && scoped.data.waitingForSources ? "pack-pending" : last?.stage === "downloading" || last?.stage === "verifying" || last?.stage === "publishing"
      ? last.stage : last?.stage === "searching" || last?.stage === "selecting" || last?.stage === "replacing" ? "searching" : "queued";
    return { id: a.id, ids: new Set(ids), current, last, unscoped: !scoped.success || !scoped.data.batchEpisodeIds,
      stage: current ? runningStage : status === "failed" ? "failed" : "waiting" } as const;
  });
  const activeWork = work.filter(w => w.current);
  const downloading = new Map(activeWork.filter(w => w.stage === "downloading").map(w => [w.id, w]));
  return { missing, work, activeWork, downloading };
}

function currentTransferRows(downloading: ReturnType<typeof prepareWork>["downloading"], readTransfers: ProgressInput["readTransfers"], now: number) {
  return (downloading.size ? readTransfers?.([...downloading.keys()]) ?? [] : []).flatMap(row => {
    const batch = downloading.get(row.acquisitionId);
    if (!batch?.last) return [];
    try {
      const result = observation.safeParse(JSON.parse(row.observation));
      if (!result.success) return [];
      const value = result.data, observed = Date.parse(value.observedAt), age = now - observed;
      if (value.seedOnly || value.bytesDone >= value.sizeBytes || !Number.isFinite(age) || age < 0
        || observed < Date.parse(batch.last.createdAt)) return [];
      return [{ row, ids: batch.ids, value, observed }];
    } catch { return []; }
  });
}

/** Scope the daemon read to still-current download attempts, then project public progress once. */
export function prepareTvRequestProgress(input: ProgressInput) {
  const now = Date.now();
  const { missing, work, activeWork, downloading } = prepareWork(input);
  const rows = currentTransferRows(downloading, input.readTransfers, now);
  return {
    bindings: rows.map(({ row }) => row),
    project(liveTransfers?: ReadonlyMap<number, LiveTransfer>): z.infer<typeof tvRequestProgressSchema> {
      const transfers = rows.flatMap<{ torrentId: number; ids: Set<number>; sample: Transfer }>(({ row, ids, value, observed }) => {
        const live = liveTransfers?.get(row.torrentId);
        if (live) {
          if (live.seedOnly || !["downloading", "download-queued"].includes(live.state)
            || !live.active || live.sizeBytes <= 0 || live.bytesDone >= live.sizeBytes) return [];
          return [{ torrentId: row.torrentId, ids, sample: {
            bytesDone: live.bytesDone, sizeBytes: live.sizeBytes, bytesPerSecond: live.bytesPerSecond,
          } }];
        }
        const stale = now - observed > TRANSFER_MAX_AGE_MS;
        return [{ torrentId: row.torrentId, ids, sample: {
          bytesDone: value.bytesDone, sizeBytes: value.sizeBytes,
          bytesPerSecond: stale ? null : value.downloadBytesPerSecond,
          ...(stale ? { lastReportedAt: new Date(observed).toISOString() } : {}),
        } }];
      });
      return projectProgress(input, { missing, work, activeWork }, transfers, now);
    },
  };
}

export function tvRequestProgress(input: ProgressInput) {
  return prepareTvRequestProgress(input).project();
}

function projectProgress({ episodes, available, events, acquisitions, readTransfers }: ProgressInput,
  { missing, work, activeWork }: Pick<ReturnType<typeof prepareWork>, "missing" | "work" | "activeWork">,
  transfers: { torrentId: number; ids: Set<number>; sample: Transfer }[], now: number): z.infer<typeof tvRequestProgressSchema> {
  const queuedIds = new Set(activeWork.flatMap(w => [...w.ids]));
  const searchableAiredEpisodes = activeWork.some(w => w.unscoped) ? 0 : missing.filter(ep =>
    ep.airDate && Date.parse(ep.airDate) <= now && !queuedIds.has(ep.id)).length;
  const seasons = [...new Set(episodes.flatMap(ep => ep.numbering.filter(n => n.scheme === "tvdb:aired" && n.season !== null).map(n => n.season!)))].sort((a,b) => a-b);
  return {
    activeBatches: activeWork.length,
    searchableAiredEpisodes,
    download: summarize(transfers),
    lastAttemptAt: events.filter(e => e.stage === "searching" && e.acquisitionId !== null && acquisitions.some(a => a.id === e.acquisitionId)).at(-1)?.createdAt ?? null,
    lastPublishedAt: events.filter(e => e.stage === "available").at(-1)?.createdAt ?? null,
    seasons: seasons.map(seasonNumber => {
      const members = episodes.filter(ep => ep.numbering.some(n => n.scheme === "tvdb:aired" && n.season === seasonNumber));
      const ready = members.filter(ep => available.has(ep.id)).length;
      const pending = members.filter(ep => !available.has(ep.id));
      const upcoming = (ep: TvEpisode) => !ep.airDate || Date.parse(ep.airDate) > now;
      const future = pending.filter(upcoming).length;
      const matching = work.filter(w => pending.some(ep => w.ids.has(ep.id)));
      const active = matching.find(w => w.current);
      const seasonDownload = summarize(transfers.filter(t => pending.some(ep => t.ids.has(ep.id))));
      return { seasonNumber, total: members.length, available: ready, future,
        stage: ready === members.length ? "ready" : seasonDownload ? "downloading"
          : active?.stage ?? (future === pending.length ? "upcoming" : matching[0]?.stage ?? "waiting"),
        download: seasonDownload,
        episodes: readTransfers ? members.flatMap(ep => {
          const number = ep.numbering.find(n => n.scheme === "tvdb:aired" && n.season === seasonNumber)?.episode;
          if (number == null) return [];
          return [{ id: ep.id, number, title: ep.title, airDate: ep.airDate,
            status: available.has(ep.id) ? "ready" as const : upcoming(ep) ? "upcoming" as const : "missing" as const }];
        }).sort((a, b) => a.number - b.number || a.id - b.id) : [] };
    }),
  };
}
