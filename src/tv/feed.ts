import { createHash } from "node:crypto";
import { z } from "zod";
import type { JobRow } from "../app-types";
import type { BooksRepo } from "../repo";
import { movieOfferFromResult, type MovieOffer } from "../movies/offers";
import { durableStep, type WorkerContext } from "../worker/context";
import { queueTvAcquisition, tvAcquisitionBatch } from "./acquisition";
import type { TvEntry, TvEpisode } from "./types";
import { createTvFeedMatcher } from "./feed-agent";

export type TvFeedOffer = Pick<MovieOffer, "title" | "provider" | "sizeBytes" | "seeders"> & {
  attributes: { name: string; value: string }[];
};
export interface TvFeedTarget {
  targetId: number;
  entry: TvEntry;
  episodes: TvEpisode[];
}
export interface TvFeedMatch {
  targetId: number;
  releaseIndexes: number[];
  reason: string;
}
/** Broad semantic routing, not quality selection or authority to assert episode coverage.
 * Implementations must keep plausible packs, aliases, and native-anime/provider-numbering matches.
 * All text is untrusted evidence. No external search or human confirmation belongs in this step.
 */
export type TvFeedMatcher = (input: {
  targets: TvFeedTarget[];
  offers: TvFeedOffer[];
}) => Promise<TvFeedMatch[]>;
export type TvFeedContext = WorkerContext & { tvFeedMatcher?: TvFeedMatcher };

const payloadSchema = z.object({
  releaseIds: z.array(z.number().int().positive()).min(1).max(100),
}).strict();
const matchesSchema = z.array(z.object({
  targetId: z.number().int().positive(),
  releaseIndexes: z.array(z.number().int().nonnegative()).min(1).max(100),
  reason: z.string().trim().min(1).max(2000),
}).strict()).max(1000);
const hash = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
const isAired = (episode: TvEpisode) => Boolean(episode.airDate &&
  Number.isFinite(Date.parse(episode.airDate)) && Date.parse(episode.airDate) <= Date.now());

function publicOffer(offer: MovieOffer): TvFeedOffer {
  const attributes = Array.isArray(offer.attributes.attr) ? offer.attributes.attr
    : offer.attributes.attr ? [offer.attributes.attr] : [];
  return { title: offer.title.slice(0, 512), provider: offer.provider.slice(0, 120),
    sizeBytes: offer.sizeBytes, seeders: offer.seeders,
    attributes: attributes.filter((a): a is Record<string, unknown> => Boolean(a && typeof a === "object"))
      .filter(a => ["tvdb", "tvdbid", "tmdb", "tmdbid", "anidb", "anidbid", "anilist", "anilistid", "year", "category"]
        .includes(String(a.name).toLowerCase()))
      .slice(0, 20).map(a => ({ name: String(a.name).slice(0, 80), value: String(a.value).slice(0, 120) })) };
}

/** Refresh expiring URLs, never the immutable inbox/content identity or the server's offer key. */
export function refreshTvFeedOffer(repo: BooksRepo, offer: MovieOffer): MovieOffer {
  if (offer.inboxId === undefined) return structuredClone(offer);
  const entry = repo.releaseInbox.entries([offer.inboxId])[0];
  if (!entry || entry.result.mediaType !== "tv") throw new Error("TV RSS candidate is missing from the inbox");
  const fresh = movieOfferFromResult({ ...entry.result, mediaType: "movie" });
  if (fresh.key !== offer.key) throw new Error("TV RSS candidate changed its immutable identity");
  return { ...fresh, inboxId: entry.id };
}

type Scope = TvFeedTarget & {
  requests: { requestId: number; episodeIds: number[] }[];
  revision: string;
};
function acquisitionBatches(target: Scope) {
  return target.requests.flatMap(request => {
    const requested = new Set(request.episodeIds);
    const episodes = tvAcquisitionBatch(target.episodes.filter(ep => requested.has(ep.id)));
    return episodes.length ? [{ requestId: request.requestId, episodes, ids: episodes.map(ep => ep.id) }] : [];
  });
}

function scope(ctx: TvFeedContext, targetId: number): Scope | null {
  const target = ctx.repo.tv.target(targetId);
  const retention = ctx.repo.retention.get({ namespace: "tv-target", id: String(targetId) });
  if (!target || retention?.desiredPresence === "evicted") return null;
  const requests = ctx.repo.tv.requests().filter(r => r.target_id === targetId && r.withdrawn_at === null)
    .sort((a, b) => a.id - b.id).map(r => ({ requestId: r.id,
      episodes: ctx.repo.tv.missingEpisodes(r.id).filter(isAired) })).filter(r => r.episodes.length);
  if (!requests.length) return null;
  const catalog = ctx.repo.tv.entry(target.entry_id);
  if (!catalog) throw new Error("TV target catalog entry is missing");
  const episodes = [...new Map(requests.flatMap(r => r.episodes).map(ep => [ep.id, ep])).values()]
    .sort((a, b) => a.id - b.id);
  const requestIds = requests.map(r => ({ requestId: r.requestId, episodeIds: r.episodes.map(ep => ep.id).sort((a, b) => a - b) }));
  // Do not expose unrequested episode metadata or let aggregate numbering define work identity.
  const entry = { ...catalog, episodes };
  return { targetId, entry, episodes, requests: requestIds,
    revision: hash([target.policy_json, target.revision, retention?.revision ?? 0, requestIds, entry]) };
}
function validateMatches(raw: unknown, targets: TvFeedTarget[], releaseCount: number): TvFeedMatch[] {
  const allowed = new Set(targets.map(t => t.targetId));
  const matches = matchesSchema.parse(raw);
  for (const match of matches) if (!allowed.has(match.targetId) || match.releaseIndexes.some(i => i >= releaseCount))
    throw new Error("TV RSS matcher returned an unknown target or release");
  return matches.map(m => ({ ...m, releaseIndexes: [...new Set(m.releaseIndexes)] }));
}

/** Shared feed intake only. No per-title search, quality decision, or publication occurs here. */
export async function matchTvFeed(ctx: TvFeedContext, job: JobRow, injectedMatcher?: TvFeedMatcher): Promise<"done"> {
  const { releaseIds } = payloadSchema.parse(JSON.parse(job.payload_json ?? "{}"));
  const entries = ctx.repo.releaseInbox.entries(releaseIds);
  const wanted = new Set(releaseIds);
  if (entries.length !== wanted.size || entries.some(e => !wanted.has(e.id))) throw new Error("TV RSS inbox entries are missing");
  if (entries.some(e => e.result.mediaType !== "tv")) throw new Error("Not a TV feed batch");
  const offers = entries.map(e => ({ ...movieOfferFromResult({ ...e.result, mediaType: "movie" }), inboxId: e.id }));
  const projectedOffers = offers.map(publicOffer);
  const releaseRevision = hash([offers.map(o => [o.inboxId, o.key]), projectedOffers]);
  const seen = (index: number, target: Scope) => Boolean(
    ctx.repo.releaseInbox.considered(entries[index]!.id, "tv", target.targetId, target.revision) ||
    ctx.repo.releaseInbox.consideredForDispatch(entries[index]!.id, "tv", target.targetId, job.id));
  const targets = await durableStep(ctx, `tv-rss:targets:${releaseRevision}`, () =>
    ctx.repo.tv.targets().flatMap(t => {
      const current = scope(ctx, t.id);
      return current && entries.some((_, i) => !seen(i, current)) ? [current] : [];
    }));
  if (!targets.length) return "done";
  const matcher = injectedMatcher ?? ctx.tvFeedMatcher ?? createTvFeedMatcher(ctx.getSettings(), ctx.modelProvider);
  const runMatcher = async (batch: Scope[], name: string) => {
    const matches = await durableStep(ctx, name, () => matcher({
      targets: structuredClone(batch.map(({ targetId, entry, episodes }) => ({ targetId, entry, episodes }))),
      offers: structuredClone(projectedOffers),
    }));
    ctx.execution?.assertActive();
    return validateMatches(matches, batch, entries.length);
  };
  // Complete and validate all semantic calls before dispatching anything; provider failure is not no-match.
  const matches: TvFeedMatch[] = [];
  for (let start = 0; start < targets.length; start += 100)
    matches.push(...await runMatcher(targets.slice(start, start + 100), `tv-rss:match:${releaseRevision}:${start}`));
  let pending = targets.map(t => t.targetId);
  for (let wave = 0; pending.length; wave++) {
    const busy: number[] = [];
    for (const targetId of pending) {
      ctx.execution?.assertActive();
      const current = scope(ctx, targetId);
      if (!current) continue;
      const original = targets.find(t => t.targetId === targetId)!;
      const matching = current.revision === original.revision
        ? matches.filter(m => m.targetId === targetId)
        : await runMatcher([current], `tv-rss:rematch:${releaseRevision}:${targetId}:${current.revision}`);
      const indexes = [...new Set(matching.flatMap(m => m.releaseIndexes))];
      const unseen = indexes.filter(i => !seen(i, current));
      if (!unseen.length) continue;
      const batches = acquisitionBatches(current);
      const active = ctx.repo.tv.acquisitions(targetId).filter(a => !a.finished_at);
      const statuses = ctx.repo.getJobStatuses(active.flatMap(a => a.job_id === null ? [] : [a.job_id]));
      const unfinished = active.some(a => {
        const batch = batches.find(batch => batch.requestId === a.request_id);
        if (!batch || (a.job_id !== null && !["queued", "running"].includes(statuses.get(a.job_id) ?? ""))) return false;
        const reserved = a.decision_json ? JSON.parse(a.decision_json).batchEpisodeIds as number[] | undefined : undefined;
        return !reserved || batch.ids.some(id => reserved.includes(id));
      });
      if (unfinished) { busy.push(targetId); continue; }
      await durableStep(ctx, `tv-rss:dispatch:${releaseRevision}:${targetId}:${current.revision}`, () => ctx.repo.transaction(() => {
        ctx.execution?.assertActive();
        const latest = scope(ctx, targetId);
        if (!latest) return;
        if (latest.revision !== current.revision) throw new Error("TV target changed while dispatching RSS candidates");
        const stillUnseen = unseen.filter(i => !seen(i, latest));
        if (!stillUnseen.length) return;
        const feedOffers = stillUnseen.map(i => refreshTvFeedOffer(ctx.repo, offers[i]!));
        const reasons = matching.map(m => m.reason);
        const sources = stillUnseen.map(i => ({ inboxId: entries[i]!.id, feedId: entries[i]!.feed_id,
          releaseKey: entries[i]!.release_key, contentKey: entries[i]!.content_key, firstSeenAt: entries[i]!.first_seen_at }));
        const queuedJobs: number[] = [];
        for (const batch of batches) {
          const queued = queueTvAcquisition(ctx.repo, batch.requestId, "recovery", batch.ids);
          if (queued.shared || queued.acquisition.job_id === null) throw new Error("TV acquisition became active before RSS dispatch");
          const savedDecision = ctx.repo.tv.acquisition(queued.acquisition.id)!.decision_json;
          ctx.repo.tv.decision(queued.acquisition.id, { ...(savedDecision ? JSON.parse(savedDecision) : {}), feedOffers, feedEvidence: {
            source: "release-inbox", sourceJobId: job.id, targetId, targetRevision: latest.revision,
            entryKey: latest.entry.key, requestId: batch.requestId, requestedEpisodeIds: batch.ids,
            episodes: batch.episodes.map(ep =>
              ({ id: ep.id, key: ep.key, numbering: ep.numbering })), releases: sources, reasons,
          } });
          queuedJobs.push(queued.acquisition.job_id);
        }
        const note = JSON.stringify({ entryKey: latest.entry.key, requestIds: latest.requests.map(r => r.requestId),
          acquisitionJobIds: queuedJobs, reasons }).slice(0, 4000);
        for (const index of stillUnseen)
          ctx.repo.releaseInbox.consider(entries[index]!.id, "tv", targetId, latest.revision, queuedJobs[0]!, note, job.id);
      }));
    }
    if (!busy.length) break;
    if (!ctx.execution) throw new Error("Waiting for an active TV acquisition requires durable execution");
    await ctx.execution.sleep(`tv-rss:wait-for-targets:${releaseRevision}:${wave}`, 60_000);
    pending = busy;
  }
  return "done";
}
