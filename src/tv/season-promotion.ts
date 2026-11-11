import { createHash } from "node:crypto";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import { factsFromMovieProbe, movieProbeSchema } from "../movies/probe";
import { evaluateMovieQuality, movieQualityPolicySchema, type MovieQualityPolicy } from "../movies/quality";
import { tvImportPlanSchema, type TvFileCandidate, type TvImportPlan } from "./agents";
import { canonicalTvSeasons, type TvCatalog } from "./catalog";
import type { TvActiveEpisode, TvEntry, TvEpisode, TvTarget, TvVersion } from "./types";

const maxEpisodes = 64;
const id = z.number().int().positive().safe();
export const tvSeasonPromotionSchema = z.object({
  season: id, revision: z.number().int().nonnegative(), fingerprint: z.string().regex(/^[0-9a-f]{64}$/),
  episodeIds: z.array(id).min(2).max(maxEpisodes),
}).strict();
export type TvSeasonPromotion = z.infer<typeof tvSeasonPromotionSchema>;
type Quality = { tier: number; preference: number };
export type TvFeedDemand = { requestId: number; episodeIds: number[] }[];
type TvSeasonScope = {
  entry: TvEntry; episodes: TvEpisode[]; intent: TvSeasonPromotion; requestId: number;
  policyJson: string; selections: TvActiveEpisode[];
};
export type TvSeasonSelection = TvSeasonScope & { policy: MovieQualityPolicy; qualities: Map<number, Quality> };

const sorted = (ids: readonly number[]) => [...ids].sort((a, b) => a - b);
const exact = (a: readonly number[], b: readonly number[]) => JSON.stringify(sorted(a)) === JSON.stringify(sorted(b));
const aired = (episode: { airDate: string | null }) => Boolean(episode.airDate && Date.parse(episode.airDate) <= Date.now());
const airedNumber = (episode: TvEpisode) => episode.numbering.find(number => number.scheme === "tvdb:aired");
export const playableTvMember = (name: string) => /\.(mkv|mp4|m4v|avi|mov|ts)$/i.test(name);

/** A feed-title hint limits speculative intake; it never proves pack coverage. */
export function tvSeasonPackHint(title: string): number | null {
  const seasons = [...title.matchAll(/(?:^|[^\p{L}\p{N}])(?:s|season[ ._-]+)(\d{1,3})(?![\p{L}\p{N}])/giu)];
  if (seasons.length !== 1 || /(?:^|[^\p{L}\p{N}])(?:e|ep|episode)[ ._-]*\d+/iu.test(title)) return null;
  return Number(seasons[0]![1]) || null;
}

function quality(policy: MovieQualityPolicy, rawProbe: unknown, claims: TvImportPlan["files"][number]["facts"]): Quality | null {
  const probe = movieProbeSchema.safeParse(rawProbe);
  if (!probe.success || !probe.data.streams.some(stream => stream.codec_type === "audio")) return null;
  const result = evaluateMovieQuality(policy, factsFromMovieProbe(probe.data, claims));
  return result.eligibility === "eligible" && result.tier !== null ? { tier: result.tier, preference: result.preferenceScore } : null;
}

function versionQuality(version: TvVersion, episodeId: number, policy: MovieQualityPolicy): Quality | null {
  const files = version.files.filter(file => file.episodeIds.includes(episodeId));
  const facts = version.facts as { plan?: unknown; identityVerified?: unknown } | null;
  const plan = tvImportPlanSchema.safeParse(facts?.plan);
  if (files.length !== 1 || facts?.identityVerified !== true || !plan.success) return null;
  const mappings = plan.data.files.filter(file => file.episodeIds.includes(episodeId));
  return mappings.length === 1 && exact(mappings[0]!.episodeIds, files[0]!.episodeIds)
    ? quality(policy, files[0]!.probe, mappings[0]!.facts) : null;
}

export function captureTvSeasonScope(repo: BooksRepo, target: TvTarget, entry: TvEntry, demand: TvFeedDemand,
  activeByEpisode: ReadonlyMap<number, TvActiveEpisode>, season: number, targetIntent: ReturnType<BooksRepo["retention"]["get"]>): TvSeasonScope | null {
  if (season <= 0 || !entry.tmdbId || !entry.tvdbId || entry.episodes.length > 5000 || targetIntent?.desiredPresence === "evicted") return null;
  const episodes = entry.episodes.filter(episode => airedNumber(episode)?.season === season);
  if (episodes.length < 2 || episodes.length > maxEpisodes || episodes.some(episode =>
    !aired(episode) || episode.identity.stability !== "provider" || !/^tvdb:[1-9]\d*$/.test(episode.key) ||
    !airedNumber(episode)?.episode) || new Set(episodes.map(episode => airedNumber(episode)!.episode)).size !== episodes.length) return null;
  const episodeIds = sorted(episodes.map(episode => episode.id));
  const request = demand.find(request => episodeIds.every(id => request.episodeIds.includes(id)));
  const selections = episodeIds.flatMap(id => activeByEpisode.get(id) ?? []);
  if (!request || selections.length !== episodes.length || new Set(selections.map(row => row.version_id)).size < 2) return null;
  const seasonIntent = repo.retention.get({ namespace: "tv-season", id: `${target.id}:tvdb:aired:${season}` });
  if (seasonIntent?.desiredPresence === "evicted") return null;
  const fingerprint = createHash("sha256").update(JSON.stringify([
    entry.id, entry.key, entry.tvdbId, entry.tmdbId, entry.title, entry.year, target.policy_json, target.revision,
    request.requestId, request.episodeIds, targetIntent?.revision, seasonIntent?.revision, selections, episodes,
  ])).digest("hex");
  return { entry, episodes, selections, policyJson: target.policy_json, requestId: request.requestId,
    intent: { season, revision: target.revision, fingerprint, episodeIds } };
}

export function readTvSeasonPromotion(repo: BooksRepo, targetId: number, requestId: number, season: number) {
  const target = repo.tv.target(targetId), request = repo.tv.requestById(requestId);
  if (!target || !request || request.target_id !== targetId || request.withdrawn_at) return null;
  const entry = repo.tv.entry(target.entry_id);
  if (!entry) return null;
  return captureTvSeasonScope(repo, target, entry, [{ requestId, episodeIds: repo.tv.requestedEpisodeIds(requestId) }],
    new Map(repo.tv.activeEpisodes(targetId).map(row => [row.episode_id, row])), season,
    repo.retention.get({ namespace: "tv-target", id: String(targetId) }));
}

/** Probe/plan reads belong to the acquisition, not to scanning every target in an RSS batch. */
export function readTvSeasonSelection(repo: BooksRepo, targetId: number, scope: TvSeasonScope): TvSeasonSelection | null {
  const parsedPolicy = movieQualityPolicySchema.safeParse(JSON.parse(scope.policyJson));
  if (!parsedPolicy.success) return null;
  const policy = parsedPolicy.data;
  const seasonIds = new Set(scope.intent.episodeIds);
  const versions = new Map([...new Set(scope.selections.map(row => row.version_id))].map(id => [id, repo.tv.version(id)]));
  const qualities = new Map<number, Quality>();
  for (const row of scope.selections) {
    const version = versions.get(row.version_id);
    if (!version || version.targetId !== targetId || version.entryId !== scope.entry.id ||
        version.files.some(file => file.episodeIds.includes(row.episode_id) && file.episodeIds.some(id => !seasonIds.has(id)))) return null;
    const selected = versionQuality(version, row.episode_id, policy);
    if (!selected) return null;
    qualities.set(row.episode_id, selected);
  }
  return { ...scope, policy, qualities };
}

/** Compare the entire live canonical roster to the captured, already selected season. */
export async function matchesTvSeasonRoster(catalog: Pick<TvCatalog, "resolve">, entry: TvEntry, episodes: TvEpisode[], season: number) {
  if (!entry.tmdbId) return false;
  const canonical = canonicalTvSeasons(await catalog.resolve(entry.tmdbId), entry.tmdbId);
  const roster = canonical.seasons.get(season);
  if (canonical.entry.key !== entry.key || canonical.entry.tvdbId !== entry.tvdbId ||
      !roster || roster.length !== episodes.length || !roster.every(aired)) return false;
  const coordinates = new Map(episodes.map(episode => [episode.key, airedNumber(episode)!.episode]));
  return roster.every(episode => coordinates.get(episode.key) === episode.numbering.find(number => number.scheme === "tvdb:aired")?.episode);
}

function nonRegressing(from: Quality | undefined, to: Quality | null) {
  return !!from && !!to && (to.tier > from.tier || (to.tier === from.tier && to.preference >= from.preference));
}

export function eligibleTvSeasonPlan(selection: TvSeasonSelection, plan: TvImportPlan, files: TvFileCandidate[], memberCount: number) {
  if (files.length !== memberCount || plan.files.length !== memberCount ||
      !exact(plan.files.flatMap(file => file.episodeIds), selection.intent.episodeIds)) return false;
  return plan.files.every(mapping => {
    const file = files.find(file => file.index === mapping.fileIndex);
    const candidate = file ? quality(selection.policy, file.probe, mapping.facts) : null;
    return mapping.episodeIds.every(id => nonRegressing(selection.qualities.get(id), candidate));
  });
}

export function eligibleTvSeasonVersion(selection: TvSeasonSelection, version: TvVersion) {
  return exact(version.files.flatMap(file => file.episodeIds), selection.intent.episodeIds) &&
    selection.intent.episodeIds.every(id => nonRegressing(selection.qualities.get(id), versionQuality(version, id, selection.policy)));
}
