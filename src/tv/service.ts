import { z } from "zod";
import type { BooksRepo } from "../repo";
import { ensurePresentIntent } from "../media/retention-service";
import { queueTvAcquisition } from "./acquisition";
import { canonicalTvSeasons, configuredTvCatalog, TvCatalogError, type TvCatalog } from "./catalog";
import type { TvRequest } from "./types";

export const tvSeasonSelectionSchema = z.array(z.number().int().nonnegative().safe()).min(1).max(500)
  .refine((seasons) => new Set(seasons).size === seasons.length, "Duplicate season selection");
const scopeSchema = z.object({ tmdbId: z.number().int().positive().safe(), seasonNumbers: tvSeasonSelectionSchema });
export function nativeTvRequestScope(request: TvRequest) {
  const match = /^native-seasons:([1-9]\d*):(\[[\d,]+\])$/.exec(request.request_key);
  if (!match) return null;
  return scopeSchema.parse({ tmdbId: Number(match[1]), seasonNumbers: JSON.parse(match[2]!) });
}
const aired = (episode: { airDate: string | null }) => !!episode.airDate && Date.parse(episode.airDate) <= Date.now();

export async function requestTvSeasons(repo: BooksRepo, catalog: TvCatalog, userId: number,
  input: { tmdbId: number; seasonNumbers: number[] }) {
  const scope = scopeSchema.parse(input);
  const { entry, seasons } = canonicalTvSeasons(await catalog.resolve(scope.tmdbId), scope.tmdbId);
  for (const season of scope.seasonNumbers)
    if (!seasons.has(season)) throw new TvCatalogError(`Season ${season} has no canonical TVDB episode metadata; refresh the catalog or select a supported season.`, "unsupported");
  return repo.transaction(() => {
    const wanted = new Set(scope.seasonNumbers.flatMap((season) => seasons.get(season)!.map((ep) => ep.key)));
    const normalized = JSON.stringify([...scope.seasonNumbers].sort((a, b) => a - b));
    for (const previous of repo.tv.requests(userId)) {
      const priorScope = nativeTvRequestScope(previous);
      if (!priorScope || priorScope.tmdbId !== scope.tmdbId || JSON.stringify(priorScope.seasonNumbers) !== normalized) continue;
      const target = repo.tv.target(previous.target_id)!;
      const priorEntry = repo.tv.entry(target.entry_id)!;
      if (priorEntry.key !== entry.key || priorEntry.tvdbId !== entry.tvdbId ||
          repo.tv.requestedEpisodes(previous.id).some((ep) => !wanted.has(ep.key)))
        throw new TvCatalogError("Canonical mapping changed for this season request; correct the mapping before re-requesting. Existing identities were retained.", "unsupported");
    }
    const request = repo.tv.requestSeasons(userId, entry, repo.getSettings().movies.qualityPolicy, scope.seasonNumbers);
    const key = { namespace: "tv-target", id: String(request.targetId) };
    if (repo.retention.get(key)?.desiredPresence !== "present")
      ensurePresentIntent(repo, key, "User explicitly requested TV seasons");
    if (repo.tv.missingEpisodes(request.requestId).some(aired)) queueTvAcquisition(repo, request.requestId);
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
      if (scope.seasonNumbers.some((season) => !seasons.has(season)))
        throw new TvCatalogError("Previously requested season no longer has canonical episode metadata; retaining existing scope.", "unsupported");
      const selected = new Set(scope.seasonNumbers.flatMap((season) => seasons.get(season)!.map((ep) => ep.key)));
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
        repo.tv.requestSeasons(current.user_id, entry, JSON.parse(target.policy_json), scope.seasonNumbers);
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
    if (retention) repo.retention.withdraw(key, { expectedRevision: retention.revision, reason: "User withdrew a TV season request; other demand and media are unchanged" });
  });
}
