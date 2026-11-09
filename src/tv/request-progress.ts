import { z } from "zod";
import type { BooksRepo } from "../repo";
import { tvAcquisitionBatch } from "./acquisition";
import type { TvAcquisition, TvEpisode, TvEvent } from "./types";

const stage = z.enum(["ready", "queued", "searching", "downloading", "verifying", "publishing", "pack-pending", "waiting", "failed", "upcoming"]);
export const tvRequestProgressSchema = z.object({
  activeBatches: z.number().int().nonnegative(),
  searchableAiredEpisodes: z.number().int().nonnegative(),
  lastAttemptAt: z.string().nullable(), lastPublishedAt: z.string().nullable(),
  seasons: z.array(z.object({ seasonNumber: z.number().int().nonnegative(), total: z.number().int().nonnegative(),
    available: z.number().int().nonnegative(), future: z.number().int().nonnegative(), stage })),
});

/** User-facing facts from persisted work, never a live client poll or an inference that a release does not exist. */
export function tvRequestProgress(repo: Pick<BooksRepo, "getJob">, input: {
  episodes: TvEpisode[]; available: ReadonlySet<number>; events: TvEvent[]; acquisitions: TvAcquisition[];
}): z.infer<typeof tvRequestProgressSchema> {
  const {episodes,available,events,acquisitions}=input;
  const missing = episodes.filter(ep => !available.has(ep.id));
  const legacyScope = tvAcquisitionBatch(missing).map(ep => ep.id);
  const work = acquisitions.map(a => {
    const job = a.job_id ? repo.getJob(a.job_id) : null;
    const scoped = z.object({ batchEpisodeIds: z.array(z.number().int().positive()).optional(),waitingForSources:z.boolean().optional() })
      .safeParse(a.decision_json ? JSON.parse(a.decision_json) : {});
    const ids = scoped.success ? scoped.data.batchEpisodeIds ?? legacyScope : legacyScope;
    const current = !a.finished_at && !!job && ["queued", "running"].includes(job.status);
    const last = events.filter(e => e.acquisitionId === a.id).at(-1);
    const runningStage = scoped.success && scoped.data.waitingForSources ? "pack-pending" : last?.stage === "downloading" || last?.stage === "verifying" || last?.stage === "publishing"
      ? last.stage : last?.stage === "searching" || last?.stage === "selecting" || last?.stage === "replacing" ? "searching" : "queued";
    return { ids, current, unscoped: !scoped.success || !scoped.data.batchEpisodeIds,
      stage: current ? runningStage : job?.status === "failed" ? "failed" : "waiting" } as const;
  });
  const activeWork = work.filter(w => w.current);
  const queuedIds = new Set(activeWork.flatMap(w => w.ids));
  const now = Date.now();
  const searchableAiredEpisodes = activeWork.some(w => w.unscoped) ? 0 : missing.filter(ep =>
    ep.airDate && Date.parse(ep.airDate) <= now && !queuedIds.has(ep.id)).length;
  const seasons = [...new Set(episodes.flatMap(ep => ep.numbering.filter(n => n.scheme === "tvdb:aired" && n.season !== null).map(n => n.season!)))].sort((a,b) => a-b);
  return {
    activeBatches: activeWork.length,
    searchableAiredEpisodes,
    lastAttemptAt: events.filter(e => e.stage === "searching" && e.acquisitionId !== null && acquisitions.some(a => a.id === e.acquisitionId)).at(-1)?.createdAt ?? null,
    lastPublishedAt: events.filter(e => e.stage === "available").at(-1)?.createdAt ?? null,
    seasons: seasons.map(seasonNumber => {
      const members = episodes.filter(ep => ep.numbering.some(n => n.scheme === "tvdb:aired" && n.season === seasonNumber));
      const ready = members.filter(ep => available.has(ep.id)).length;
      const pending = members.filter(ep => !available.has(ep.id));
      const future = pending.filter(ep => !ep.airDate || Date.parse(ep.airDate) > Date.now()).length;
      const matching = work.filter(w => pending.some(ep => w.ids.includes(ep.id)));
      const active = matching.find(w => w.current);
      return { seasonNumber, total: members.length, available: ready, future,
        stage: ready === members.length ? "ready" : active?.stage ?? (future === pending.length ? "upcoming" : matching[0]?.stage ?? "waiting") };
    }),
  };
}
