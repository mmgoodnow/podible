import { z } from "zod";
import type { BooksRepo } from "../repo";
import { ensurePresentIntent } from "../media/retention-service";
import { queueTvSeasons } from "./acquisition";
import { canonicalTvSeasons, configuredTvCatalog, tvSpecialEpisodeKeySchema, verifiedTvSpecials, TvCatalogError, type TvCatalog } from "./catalog";
import { nativeTvRequestKey } from "./repo";
import type { TvRequest } from "./types";

const unique = <T>(values: T[]) => new Set(values).size === values.length;
const tmdbIdSchema = z.number().int().positive().safe();
const seasonNumber = z.number().int().nonnegative().safe();
const legacyScopeSchema = z.object({ tmdbId: tmdbIdSchema,
  seasonNumbers: z.array(seasonNumber).min(1).max(500).refine(unique) });
export const tvRequestSelectionSchema = z.object({
  tmdbId: tmdbIdSchema,
  seasonNumbers: z.array(seasonNumber.positive()).max(500).refine(unique, "Duplicate season selection").optional(),
  specialEpisodeKeys: z.array(tvSpecialEpisodeKeySchema).max(500).refine(unique, "Duplicate special selection").optional(),
}).strict().refine(scope => !!(scope.seasonNumbers?.length || scope.specialEpisodeKeys?.length), "Select at least one season or special");
type NativeTvScope = { tmdbId: number; seasonNumbers: number[]; specialEpisodeKeys?: string[] };
export function nativeTvRequestScope(request: TvRequest): NativeTvScope | null {
  const match = /^native-seasons:([1-9]\d*):(\[[\d,]*\])(?::(\[.*\]))?$/.exec(request.request_key);
  if (!match) return null;
  const tmdbId = Number(match[1]), seasonNumbers = JSON.parse(match[2]!);
  if (!match[3]) return legacyScopeSchema.parse({ tmdbId, seasonNumbers });
  const scope = tvRequestSelectionSchema.parse({ tmdbId, seasonNumbers, specialEpisodeKeys: JSON.parse(match[3]) });
  return { ...scope, seasonNumbers: scope.seasonNumbers! };
}
export function nativeTvRequestSeasonNumbers(scope: NativeTvScope | null): number[] | undefined {
  return scope?.specialEpisodeKeys?.length ? [0, ...scope.seasonNumbers] : scope?.seasonNumbers;
}

const aired = (episode: { airDate: string | null }) => !!episode.airDate && Date.parse(episode.airDate) <= Date.now();

function selectedEpisodeKeys(scope: NativeTvScope, seasons: ReturnType<typeof canonicalTvSeasons>["seasons"]) {
  for (const season of scope.seasonNumbers)
    if (!seasons.has(season)) throw new TvCatalogError(`Season ${season} has no canonical TVDB episode metadata; refresh the catalog or select a supported season.`, "unsupported");
  const specialKeys = scope.specialEpisodeKeys ?? [];
  if (specialKeys.length) {
    const verified = new Set(verifiedTvSpecials(seasons).map(special => special.episodeKey));
    if (specialKeys.some(key => !verified.has(key)))
      throw new TvCatalogError("A selected episode is not a verified TVDB special for this show. Refresh the catalog and select again.", "unsupported");
  }
  return new Set([...specialKeys, ...scope.seasonNumbers.flatMap(season => seasons.get(season)!.map(ep => ep.key))]);
}

export async function requestTvSelection(repo: BooksRepo, catalog: TvCatalog, userId: number,
  input: z.input<typeof tvRequestSelectionSchema>) {
  const selection = tvRequestSelectionSchema.parse(input);
  const scope = { ...selection, seasonNumbers: selection.seasonNumbers ?? [], specialEpisodeKeys: selection.specialEpisodeKeys ?? [] };
  const { entry, seasons } = canonicalTvSeasons(await catalog.resolve(scope.tmdbId), scope.tmdbId);
  const wanted = selectedEpisodeKeys(scope, seasons);
  return repo.transaction(() => {
    const published = repo.tv.publishedEpisodeKeys(wanted);
    if (scope.seasonNumbers.some(season => seasons.get(season)!.every(ep => published.has(ep.key))))
      throw new TvCatalogError("A selected season is already in Plex. Refresh the show and select only seasons with missing episodes.", "unsupported");
    if (scope.specialEpisodeKeys.some(key => published.has(key)))
      throw new TvCatalogError("A selected special is already in Plex. Refresh the show and select only missing specials.", "unsupported");
    const requestKey = nativeTvRequestKey(scope.tmdbId, scope.seasonNumbers, scope.specialEpisodeKeys);
    const previousRequests = repo.tv.requests(userId).filter(request => request.request_key === requestKey);
    for (const previous of previousRequests) {
      const target = repo.tv.target(previous.target_id)!;
      const priorEntry = repo.tv.entry(target.entry_id)!;
      if (priorEntry.key !== entry.key || priorEntry.tvdbId !== entry.tvdbId ||
          repo.tv.requestedEpisodes(previous.id).some((ep) => !wanted.has(ep.key)))
        throw new TvCatalogError("Canonical mapping changed for this TV request; correct the mapping before re-requesting. Existing identities were retained.", "unsupported");
    }
    const prior = scope.specialEpisodeKeys.length ? previousRequests.find(request => !request.withdrawn_at) ?? previousRequests[0] : undefined;
    if (scope.specialEpisodeKeys.length && repo.tv.requestedEpisodeKeys(scope.specialEpisodeKeys, prior?.id).size)
      throw new TvCatalogError("A selected special is already requested. Refresh the show and select only unrequested specials.", "unsupported");
    const policy = prior ? JSON.parse(repo.tv.target(prior.target_id)!.policy_json) : repo.getSettings().movies.qualityPolicy;
    const request = repo.tv.requestNativeSelection(userId, entry, policy, scope.seasonNumbers, scope.specialEpisodeKeys, [...wanted]);
    const key = { namespace: "tv-target", id: String(request.targetId) };
    if (repo.retention.get(key)?.desiredPresence !== "present")
      ensurePresentIntent(repo, key, "User explicitly requested TV episodes");
    if (repo.tv.missingEpisodes(request.requestId).some(aired)) queueTvSeasons(repo, request.requestId);
    return request;
  });
}

/** Metadata-only intake hook. No acquisition, external mutation, or implicit resurrection. */
export async function refreshNativeTvRequests(repo: BooksRepo, catalog: TvCatalog = configuredTvCatalog(repo.getSettings()),
  options: { limit?: number; assertActive?: () => void } = {}) {
  const limit = z.number().int().min(1).max(100).parse(options.limit ?? 25);
  const requests = repo.tv.requests().filter((request) => !request.withdrawn_at && nativeTvRequestScope(request));
  // Rotate bounded batches; one unavailable series must not starve the rest.
  const cursorKey = "tv-native-catalog-refresh-cursor";
  const cursor = repo.getJsonState<number>(cursorKey) ?? 0;
  const sorted = [...requests].sort((a, b) => a.id - b.id);
  const batch = [...sorted.filter((r) => r.id > cursor), ...sorted.filter((r) => r.id <= cursor)].slice(0, limit);
  const errors: { requestId: number; message: string }[] = [];
  let refreshed = 0;
  const resolved = new Map<number, ReturnType<TvCatalog["resolve"]>>();
  for (const request of batch) {
    options.assertActive?.();
    const scope = nativeTvRequestScope(request)!;
    try {
      let pending = resolved.get(scope.tmdbId);
      if (!pending) { pending = catalog.resolve(scope.tmdbId); resolved.set(scope.tmdbId, pending); }
      const { entry, seasons } = canonicalTvSeasons(await pending, scope.tmdbId);
      options.assertActive?.();
      const selected = selectedEpisodeKeys(scope, seasons);
      repo.transaction(() => {
        options.assertActive?.();
        const current = repo.tv.requestById(request.id);
        if (!current || current.withdrawn_at) return;
        const target = repo.tv.target(current.target_id)!;
        const previous = repo.tv.entry(target.entry_id)!;
        if (previous.key !== entry.key || previous.tvdbId !== entry.tvdbId ||
            repo.tv.requestedEpisodes(request.id).some((ep) => !selected.has(ep.key)))
          throw new TvCatalogError("Canonical work or episode membership changed; retaining existing request identities.", "unsupported");
        // The existing request/policy identity must survive administrator policy changes.
        repo.tv.requestNativeSelection(current.user_id, entry, JSON.parse(target.policy_json), scope.seasonNumbers,
          scope.specialEpisodeKeys ?? [], [...selected]);
        refreshed++;
      });
    } catch (error) {
      options.assertActive?.();
      errors.push({ requestId: request.id, message: error instanceof TvCatalogError ? error.message : "TV metadata refresh failed; existing request scope was retained." });
    }
  }
  options.assertActive?.();
  if (batch.length) repo.setJsonState(cursorKey, batch.at(-1)!.id);
  return { refreshed, errors };
}

export function withdrawTvRequest(repo: BooksRepo, requestId: number) {
  return repo.transaction(() => {
    const request = repo.tv.requestById(requestId);
    if (!request) throw new Error("Unknown TV request");
    if (request.withdrawn_at) return;
    repo.tv.withdrawRequest(requestId);
    for (const acquisition of repo.tv.acquisitions(request.target_id)) {
      if (acquisition.request_id !== requestId || !acquisition.job_id || acquisition.finished_at) continue;
      const job = repo.getJob(acquisition.job_id);
      if (job && ["queued", "running"].includes(job.status))
        repo.markJobCancelled(acquisition.job_id, "TV request withdrawn");
      repo.tv.finishAcquisition(acquisition.id);
    }
    const key = { namespace: "tv-target", id: String(request.target_id) };
    const retention = repo.retention.get(key);
    if (retention) repo.retention.withdraw(key, { expectedRevision: retention.revision, reason: "User withdrew a TV request; other demand and media are unchanged" });
  });
}
