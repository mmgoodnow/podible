import { createHash, randomUUID } from "node:crypto";
import { downloadCapacityProblem } from "../media/download-capacity";
import { advanceDownloadProgress, downloadComplete } from "../media/download-lifecycle";
import { waitForPlexPublication, waitForTorrentCompletion } from "../worker/media-completion";
import { recordPublicationLatency, recordViewLatency, savedCompletionTime } from "../media/acquisition-latency";
import { promises as fs } from "node:fs";
import path from "node:path";
import type { JobRow } from "../app-types";
import type { BooksRepo } from "../repo";
import { durableStep, type WorkerContext } from "../worker/context";
import { inspectMetainfo } from "../media/metainfo";
import { adoptContentFile } from "../media/content-store";
import { assertMovieStorage, prepareMovieStorage } from "../movies/storage";
import { factsFromMovieProbe } from "../movies/probe";
import { evaluateMovieQuality, movieQualityPolicySchema } from "../movies/quality";
import { MovieIndexerError } from "../movies/offers";
import { publishTvVersion, tvRoots } from "./publication";
import { rememberSelectedCopies } from "../media/selected-copies";
import { queueSelectedCrossSeeds } from "../media/cross-seed";
import { TvUpstreamError } from "./upstream";
import { configuredTvServices, type TvServices } from "./runtime";
import { tvImportPlanSchema, validateTvImportPlan, type TvFileCandidate } from "./agents";
import type { TvContentFile, TvEpisode } from "./types";
import type { MovieOffer } from "../movies/offers";
import { refreshTvFeedOffer } from "./feed";
import { completeTvSource, recordedLegacyTvSource, reusableTvSources, tvSourceFiles, tvSourceSchema, type TvSource } from "./source-reuse";
import { recoverMissingTorrentSource } from "../media/recovery-source";
import { withTorrentRegistration } from "../media/torrent-use";
import type { DownloadLocation, TorrentRef, TorrentSnapshot } from "../media/torrent-client";
import { prepareCanonicalDownload, readCanonicalDownload, resolveDownloadFile, sealCanonicalDownload } from "../media/canonical-download";

export async function tvUpstreamStep<T>(ctx: WorkerContext, name: string, action: () => Promise<T>): Promise<T> {
  for (let attempt = 0; attempt < (ctx.execution ? Infinity : 3); attempt++) {
    const result = await durableStep(ctx, `${name}:request:${attempt}`, async () => {
      try { return { ok: true as const, value: await action() }; }
      catch (error) {
        if (!(error instanceof MovieIndexerError || error instanceof TvUpstreamError) || !error.retryable || (!ctx.execution && attempt === 2)) throw error;
        return { ok: false as const, delay: Math.max(1000, Math.min(3_600_000, error.retryAfterMs)) };
      }
    });
    ctx.execution?.assertActive();
    if (result.ok) return result.value;
    if (ctx.execution) await ctx.execution.sleep(`${name}:backoff:${attempt}`, result.delay);
    else await Bun.sleep(result.delay);
  }
  throw new Error("TV upstream retry budget exhausted");
}
export function queueTvAcquisition(repo: BooksRepo, requestId: number, reason: "initial" | "recovery" = "initial", batchEpisodeIds?: number[]) {
  const request = repo.tv.requestById(requestId);
  if (!request || request.withdrawn_at) throw new Error("Unknown active TV request");
  return repo.tv.startAcquisition(request.target_id, reason, (acquisitionId) => repo.createJob({
    type: "tv_acquire", payload: { acquisitionId }, idempotencyKey: `tv-acquire:${acquisitionId}`, maxAttempts: 5,
  }).id, requestId, batchEpisodeIds);
}
type StalledPack = { key: string; hash: string };
function queueTvPackFallback(repo: BooksRepo, requestId: number, ids: number[], stalledPacks: StalledPack[]) {
  const work = queueTvAcquisition(repo, requestId, "recovery", ids);
  if (!work.shared) repo.tv.decision(work.acquisition.id, { batchEpisodeIds: ids, stalledPacks });
}
/** Explicit requests/retries fan out once. A failed season waits for RSS, not a title-search timer. */
export function queueTvSeasons(repo: BooksRepo, requestId: number, reason: "initial" | "recovery" = "initial", episodeIds?: number[]) {
  return repo.transaction(() => {
    const request = repo.tv.requestById(requestId);
    if (!request || request.withdrawn_at) throw new Error("Unknown active TV request");
    let remaining = repo.tv.missingEpisodes(requestId).filter(ep => (!episodeIds || episodeIds.includes(ep.id)) && ep.airDate && Date.parse(ep.airDate) <= Date.now());
    const queued: ReturnType<typeof queueTvAcquisition>[] = [];
    const searchGroup = randomUUID();
    for (const acquisition of repo.tv.acquisitions(request.target_id)) {
      if (acquisition.request_id !== requestId || acquisition.finished_at || !acquisition.job_id ||
        !["queued", "running"].includes(repo.getJob(acquisition.job_id)?.status ?? "")) continue;
      const scope = acquisition.decision_json ? JSON.parse(acquisition.decision_json).batchEpisodeIds as number[] | undefined : undefined;
      if (!scope || remaining.some(ep => scope.includes(ep.id))) queued.push({ acquisition, shared: true });
      remaining = scope ? remaining.filter(ep => !scope.includes(ep.id)) : [];
    }
    while (remaining.length) {
      const ids = tvAcquisitionBatch(remaining).map(ep => ep.id);
      const work=queueTvAcquisition(repo, requestId, reason, ids);
      if(!work.shared) repo.tv.decision(work.acquisition.id,{...JSON.parse(work.acquisition.decision_json!),searchGroup});
      queued.push(work);
      const batch = new Set(ids);
      remaining = remaining.filter(ep => !batch.has(ep.id));
    }
    return queued;
  });
}
/** Bound execution, not request intent. Remaining episodes continue after publication. */
export function tvAcquisitionBatch(episodes: TvEpisode[]): TvEpisode[] {
  if (!episodes.length) return [];
  const coordinate = (episode: TvEpisode) => episode.numbering.find(n => n.scheme === "tvdb:aired");
  const ordered = [...episodes].sort((a, b) => {
    const left = coordinate(a), right = coordinate(b);
    return (left?.season ?? Infinity) - (right?.season ?? Infinity) ||
      (left?.episode ?? Infinity) - (right?.episode ?? Infinity) || a.id - b.id;
  });
  const season = coordinate(ordered[0]!)?.season;
  return ordered.filter(episode => coordinate(episode)?.season === season).slice(0, 64);
}
export async function acquireTv(ctx: WorkerContext, job: JobRow, services: TvServices = ctx.tvServices ?? configuredTvServices(ctx.getSettings(), ctx.modelProvider, ctx.repo)): Promise<"done"> {
  const acquisition = ctx.repo.tv.acquisition(JSON.parse(job.payload_json ?? "{}").acquisitionId);
  if (!acquisition?.request_id || acquisition.job_id !== job.id) throw new Error("Unknown TV acquisition execution");
  const repo = ctx.repo.tv;
  const target = repo.target(acquisition.target_id)!;
  const entry = repo.entry(target.entry_id)!;
  const requestId = acquisition.request_id;
  const intake = acquisition.decision_json ? JSON.parse(acquisition.decision_json) as { batchEpisodeIds?: number[]; searchGroup?: string; feedOffers?: MovieOffer[]; feedEvidence?: unknown; stalledPacks?: StalledPack[]; splitStalledPack?: boolean } : {};
  if (acquisition.finished_at && intake.splitStalledPack) return "done";
  const settings = ctx.getSettings();
  const roots = tvRoots(settings.tv.managedRoot, settings.tv.plexViewPath);
  const configuration = () => createHash("sha256").update(JSON.stringify([
    ctx.getSettings().tv, ctx.getSettings().movies.plexUrl, services.client.id,
  ])).digest("hex");
  const expected = await durableStep(ctx, "tv:configuration", configuration);
  const active = () => {
    ctx.execution?.assertActive();
    if (configuration() !== expected) throw new Error("TV configuration changed during acquisition");
    if (repo.requestById(requestId)?.withdrawn_at !== null) throw new Error("TV request was withdrawn");
    const retention = ctx.repo.retention.get({ namespace: "tv-target", id: String(target.id) });
    if (retention?.desiredPresence === "evicted") throw new Error("TV target is intentionally evicted");
  };
  const emit = (key: string, stage: Parameters<typeof repo.event>[3], message: string) => {
    active(); repo.event(target.id, acquisition.id, `tv-acquisition:${acquisition.id}:${key}`, stage, message);
    ctx.onAcquisitionChange?.();
  };
  const sharedSearch = async (wave: string, scoped = false, queryEpisodes = episodes) => {
    const key=intake.searchGroup && !scoped ? `tv-search:${intake.searchGroup}:${wave}` : null;
    const fingerprint=createHash("sha256").update(JSON.stringify([ctx.getSettings(),entry.key,entry.title])).digest("hex");
    const saved=key ? ctx.repo.getJsonState<{fingerprint:string;capturedAt:number;value:Awaited<ReturnType<TvServices["search"]>>}>(key) : null;
    if(saved?.fingerprint===fingerprint && Date.now()-saved.capturedAt>=0 && Date.now()-saved.capturedAt<30*60_000) return saved.value;
    const value=await services.search(entry, undefined, key ? undefined : queryEpisodes);
    active();
    if(key) ctx.repo.setJsonState(key,{fingerprint,capturedAt:Date.now(),value});
    return value;
  };
  active();
  const snapshot = await durableStep(ctx, "tv:target-snapshot", async () => {
    const episodeIds = tvAcquisitionBatch(repo.missingEpisodes(requestId)
      .filter((episode) => (!intake.batchEpisodeIds || intake.batchEpisodeIds.includes(episode.id)) && episode.airDate && Date.parse(episode.airDate) <= Date.now())).map((episode) => episode.id);
    const reuse = await reusableTvSources(ctx.repo, target.id, acquisition.id, episodeIds);
    const retry = ctx.repo.contentLoss.retryDownloads({ namespace: "tv-target", id: String(target.id) });
    const rejected = repo.episodeRejections(target.id, episodeIds).filter(row => !retry.some(d => d.native_id === row.native_id));
    return { episodeIds, priorKeys: rejected.map(row => row.offer_key), policy: JSON.parse(target.policy_json),
      priorHashes: rejected.map(row => row.native_id), sourceReuse: true, scopedRecovery: true,
      lossRecoveryHashes: retry.map(d => d.native_id),
      reuseSources: reuse.sources.filter(item => !rejected.some(row => row.native_id === item.source.ref.nativeId)), consumedSources: reuse.consumed };
  });
  const episodes = snapshot.episodeIds.map((id) => repo.episode(id, entry.id)!);
  // Multiple episodes from one season would still produce a season-wide ID
  // query. Probe distinct episode coordinates, while selecting for the group.
  const packProbes = intake.stalledPacks
    ? [...new Set([episodes[0], episodes.at(-1), episodes[Math.floor(episodes.length / 2)]].filter(ep => ep !== undefined))]
    : [];
  const policy = movieQualityPolicySchema.parse(snapshot.policy);
  if (!episodes.length) {
    emit("waiting", "retained", repo.missingEpisodes(requestId).length ? "Waiting for requested episodes to air and arrive in indexer feeds" : "All requested episodes are already available");
    repo.finishAcquisition(acquisition.id); return "done";
  }
  await durableStep(ctx, "tv:storage", () => { active(); return prepareMovieStorage(ctx.repo, roots.contentRoot); });
  await assertMovieStorage(ctx.repo, roots.contentRoot);
  const excluded = new Set(snapshot.priorKeys);
  let capacityProblem: string | undefined;
  const excludedHashes = new Set(snapshot.priorHashes ?? []);
  let scopedRecovery = snapshot.scopedRecovery === true;
  const currentFailures: { key: string; hash?: string }[] = [];
  const stalledPacks: StalledPack[] = [];
  for (const stalled of intake.stalledPacks ?? []) {
    excluded.add(stalled.key);
    excludedHashes.add(stalled.hash);
  }
  const reject = (key: string, hash?: string) => {
    currentFailures.push({ key, hash });
    excluded.add(key);
    if (hash) excludedHashes.add(hash);
  };
  const splitStalledBatch = async () => {
    if (!stalledPacks.length || intake.stalledPacks || capacityProblem || episodes.length < 2) return false;
    const handedOff = await durableStep(ctx, "tv:split-stalled-pack", () => ctx.repo.transaction(() => {
      active();
      const prior = repo.acquisition(acquisition.id);
      // The app transaction may have committed before the workflow saved its step.
      if (prior?.finished_at) return Boolean(JSON.parse(prior.decision_json ?? "{}").splitStalledPack);
      const missing = new Set(repo.missingEpisodes(requestId).map(ep => ep.id));
      const ids = episodes.filter(ep => missing.has(ep.id)).map(ep => ep.id);
      if (ids.length < 2) return false;
      const midpoint = Math.ceil(ids.length / 2);
      repo.decision(acquisition.id, { ...intake, splitStalledPack: true });
      repo.finishAcquisition(acquisition.id);
      queueTvPackFallback(ctx.repo, requestId, ids.slice(0, midpoint), stalledPacks);
      queueTvPackFallback(ctx.repo, requestId, ids.slice(midpoint), stalledPacks);
      repo.event(target.id, acquisition.id, `tv-acquisition:${acquisition.id}:pack-fallback`, "replacing",
        "Pack downloads stalled; searching for the remaining episodes in smaller groups");
      return true;
    }));
    if (handedOff) ctx.onAcquisitionChange?.();
    return handedOff;
  };
  const coverPack = async (name: string, pack: TvSource) => durableStep(ctx, name, async () => {
    active();
    try {
      const match = services.decider(entry).matchPack;
      if (!match) return [];
      const ids = await match({ entry, episodes, title: pack.title, files: pack.manifest.files });
      return [...new Set(ids)].filter(id => snapshot.episodeIds.includes(id));
    } catch {
      return [];
    }
  });
  const observePack = async (pack: TvSource) => {
    active();
    try {
      const seen = await services.client.inspect(pack.ref);
      return seen.ref.clientId === pack.ref.clientId && seen.ref.nativeId === pack.ref.nativeId &&
        seen.location.rootId === pack.location.rootId && seen.location.relativePath === pack.location.relativePath &&
        seen.sizeBytes === pack.manifest.size && !seen.integrityError ? seen : null;
    } catch { return null; }
  };
  const progressing = (seen: TorrentSnapshot | null, priorBytes?: number | null) =>
    seen?.state === "downloading" && !seen.seedOnly && !seen.message && !seen.errorKind && seen.bytesDone !== null &&
    ((seen.downloadBytesPerSecond ?? 0) > 0 || (priorBytes != null && seen.bytesDone > priorBytes));
  const stallDeadlineMs = services.timing?.stallMs ?? settings.recovery.stalledTorrentMinutes * 60_000;
  const waitForPack = async (name: string, source: TvSource, observation: TorrentSnapshot, coverage: number[], reason: string) => {
    if (ctx.execution?.armCompletion && !completeTvSource(source, observation)) {
      await durableStep(ctx, `${name}:wait-state`, () => {
        active();
        repo.decision(acquisition.id, { ...intake, selection: { title: source.title, reason },
          waitingForSources: true, waitingPack: { title: source.title, episodeCount: coverage.length } });
      });
      return waitForTorrentCompletion(ctx, name, source.ref, stallDeadlineMs, () => observePack(source),
        seen => completeTvSource(source, seen));
    }
    let seen: TorrentSnapshot | null = observation;
    for (let poll = 0; poll < 20_000; poll++) {
      if (seen && completeTvSource(source, seen)) return true;
      if (!seen || !ctx.execution) break;
      active();
      if (poll === 0) await durableStep(ctx, `${name}:wait-state`, () => {
        active();
        repo.decision(acquisition.id, { ...intake, selection: { title: source.title, reason },
          waitingForSources: true, waitingPack: { title: source.title, episodeCount: coverage.length } });
      });
      await ctx.execution.sleep(`${name}:wait:${poll}`, services.timing?.pollMs ?? 5000);
      const previousBytes: number | null = seen.bytesDone;
      seen = await durableStep(ctx, `${name}:observe:${poll}`, async () => {
        const observed = await observePack(source);
        return observed && (completeTvSource(source, observed) || progressing(observed, previousBytes)) ? observed : null;
      });
    }
    return false;
  };
  if (snapshot.consumedSources) emit("sources-consumed", "retained", `Skipped ${snapshot.consumedSources} verified source(s): no unused playable members for missing requested episodes`);
  const publish = async (prefix: string, versionId: number, ref: TorrentRef) => {
    const completeAt = savedCompletionTime(repo.torrent(ref.clientId, ref.nativeId)?.observation_json);
    const facts = repo.version(versionId)?.facts as { selectedOfferKey?: string; source?: { offerKey?: string } } | undefined;
    const selectedKey = facts?.selectedOfferKey ?? facts?.source?.offerKey;
    if (selectedKey) await durableStep(ctx, `${prefix}:cross-seed-copies`, () => {
      active();
      return queueSelectedCrossSeeds(ctx.repo, { media: "tv", targetId: target.id }, acquisition.id, selectedKey);
    });
    const eventKey = prefix.startsWith("tv:attempt:") ? prefix.slice("tv:attempt:".length) : prefix;
    emit(`${eventKey}:publishing`, "publishing", "Publishing an isolated TV view and verifying each episode in Plex");
    const onReconcile = (ms: number) => recordViewLatency(ctx.repo, "tv", ms);
    if (ctx.execution?.armCompletion) {
      await waitForPlexPublication(ctx, prefix, settings.tv.plexSectionId, async scan => {
        if (repo.publications().some(item => item.target_id === target.id && item.version_id !== versionId)) return false;
        active();
        return publishTvVersion({ repo, roots, library: services.library, targetId: target.id, versionId,
          assertActive: active, scan, pendingIsExpected: true, onReconcile });
      });
    } else for (let scan = 0; ; scan++) {
      const published = await durableStep(ctx, `${prefix}:publish:${scan}`, async () => {
        if (repo.publications().some(item => item.target_id === target.id && item.version_id !== versionId)) return false;
        try { active(); await publishTvVersion({ repo, roots, library: services.library, targetId: target.id, versionId, assertActive: active, onReconcile }); return true; }
        catch (error) {
          active();
          if (repo.publications().some((item) => item.target_id === target.id && item.version_id === versionId)) {
            if (scan + 1 >= (services.timing?.plexAttempts ?? 120))
              throw new Error("Plex did not verify the TV episodes; existing episodes remain selected");
            return false;
          }
          throw error;
        }
      });
      if (published) break;
      if (scan + 1 >= (services.timing?.plexAttempts ?? 120)) throw new Error("Plex did not verify the TV episodes; existing episodes remain selected");
      if (ctx.execution) await ctx.execution.sleep(`${prefix}:plex-wait:${scan}`, services.timing?.plexRetryMs ?? 5000);
      else await Bun.sleep(services.timing?.plexRetryMs ?? 5000);
    }
    recordPublicationLatency(ctx.repo, { media: "tv", acquisitionId: acquisition.id, queuedAt: acquisition.created_at,
      completeAt, publishingEvent: `tv-acquisition:${acquisition.id}:${eventKey}:publishing`,
      publicationEvent: `tv-published:${target.id}:${versionId}` });
    await durableStep(ctx, "tv:finish", () => ctx.repo.transaction(() => {
      active(); repo.finishAcquisition(acquisition.id);
      const remaining = repo.missingEpisodes(requestId).filter(episode =>
        (!intake.batchEpisodeIds || intake.batchEpisodeIds.includes(episode.id)) && episode.airDate && Date.parse(episode.airDate) <= Date.now());
      if (remaining.length) {
        const ids = remaining.map(ep => ep.id);
        if (intake.stalledPacks) queueTvPackFallback(ctx.repo, requestId, ids, intake.stalledPacks);
        else queueTvSeasons(ctx.repo, requestId, "recovery", ids);
      }
    }));
    ctx.onAcquisitionChange?.();
    return "done" as const;
  };
  // Old running jobs have neither this flag nor new source facts: retain their exact receipt sequence.
  if (snapshot.sourceReuse) {
    const recorded = repo.versionSources(target.id).find((row) => row.version.acquisitionId === acquisition.id)?.version;
    const facts = recorded?.facts as { source?: unknown; attemptKey?: string } | undefined;
    if (recorded && facts?.attemptKey && /^tv:(?:attempt|reuse):\d+$/.test(facts.attemptKey) && tvSourceSchema.safeParse(facts.source).success) {
      const source = tvSourceSchema.parse(facts.source);
      active();
      if (!completeTvSource(source, await services.client.inspect(source.ref))) throw new Error("Recorded TV source is no longer complete and owned");
      await tvSourceFiles(source, services.client, settings.tv.managedRoot);
      await durableStep(ctx, `${facts.attemptKey}:version`, () => recorded.id);
      return publish(facts.attemptKey, recorded.id, source.ref);
    }
  } else if (repo.versionSources(target.id).some((row) => row.version.acquisitionId === acquisition.id)) {
    const recorded = await recordedLegacyTvSource(ctx.repo, target.id, acquisition.id);
    if (!recorded) throw new Error("Cannot prove the recorded legacy TV version's source attempt");
    active();
    if (!completeTvSource(recorded.source, await services.client.inspect(recorded.source.ref))) throw new Error("Legacy TV source is no longer complete and owned");
    const prepared = await tvSourceFiles(recorded.source, services.client, settings.tv.managedRoot);
    const plan = tvImportPlanSchema.parse((recorded.version.facts as { plan: unknown }).plan);
    for (const file of recorded.version.files) {
      const mappings = plan.files.filter((mapping) => mapping.episodeIds.length === file.episodeIds.length && mapping.episodeIds.every((id) => file.episodeIds.includes(id)));
      const selected = mappings.length === 1 ? prepared.files.find((item) => item.index === mappings[0]!.fileIndex) : null;
      if (!selected || file.relativePath !== `files/${file.id.slice(0, 2)}/${file.id}${path.extname(selected.path).toLowerCase()}`)
        throw new Error("Recorded legacy TV file does not match its verified source mapping");
      const canonical = path.resolve(roots.contentRoot, file.relativePath), stat = await fs.lstat(canonical);
      if (!stat.isFile() || stat.isSymbolicLink() || stat.size !== file.sizeBytes || await fs.realpath(canonical) !== canonical)
        throw new Error("Recorded legacy TV canonical file is unavailable");
      const adopted = await adoptContentFile({ contentRoot: roots.contentRoot, sourceRoot: prepared.sourceRoot, sourcePath: selected.path,
        fileId: file.id, expectedSize: file.sizeBytes, assertActive: active });
      const receipt = recorded.adoptions.find((item) => item.fileId === file.id && item.fileIndex === mappings[0]!.fileIndex);
      if (!receipt || adopted.sha256 !== receipt.sha256)
        throw new Error("Recorded legacy TV content differs from its original verified digest");
    }
    await tvSourceFiles(prepared.source, services.client, settings.tv.managedRoot);
    return publish(recorded.prefix, recorded.version.id, recorded.source.ref);
  }
  const attempts = [
    ...(snapshot.sourceReuse ? snapshot.reuseSources ?? [] : []).map(({ source, skipFileIndexes }, attempt) => ({ source: source as TvSource | null, skipFileIndexes, attempt, prefix: `tv:reuse:${attempt}` })),
    ...Array.from({ length: 3 }, (_, attempt) => ({ source: null as TvSource | null, skipFileIndexes: [] as number[], attempt, prefix: `tv:attempt:${attempt}` })),
  ];
  const triedSources=new Set((snapshot.reuseSources??[]).map(item=>`${item.source.ref.clientId}:${item.source.ref.nativeId}`));
  let reuseSequence=(snapshot.reuseSources??[]).length;
  for (const [position,{ source: reusable, skipFileIndexes, attempt, prefix }] of attempts.entries()) {
    let releaseEvidence: Pick<MovieOffer, "key" | "title" | "provider">;
    let prepared: Awaited<ReturnType<typeof tvSourceFiles>>;
    if (reusable) {
      let observation;
      try {
        active();
        observation = await services.client.inspect(reusable.ref);
        if (!completeTvSource(reusable, observation)) throw new Error("Retained TV source is not complete and owned");
        prepared = await tvSourceFiles(reusable, services.client, settings.tv.managedRoot);
      } catch {
        active(); emit(`${prefix}:unavailable`, "replacing", "Previously verified pack is no longer safely reusable; searching another release"); continue;
      }
      active();
      const id = repo.bindDownload(acquisition.id, reusable.ref, reusable.location, reusable.offerKey);
      repo.observeDownload(id, observation);
      releaseEvidence = { key: reusable.offerKey, title: reusable.title, provider: reusable.provider };
    } else {
      const probe = packProbes[attempt];
      if (packProbes.length && !probe) break;
      const searchEpisodes = probe ? [probe] : episodes;
      emit(`${attempt}:searching`, "searching", `Searching releases for ${entry.title}`);
      let search = await tvUpstreamStep(ctx, `${prefix}:search`, () => attempt === 0 && intake.feedOffers?.length
        ? Promise.resolve({ offers: intake.feedOffers.map((offer) => refreshTvFeedOffer(ctx.repo, offer)), warnings: [] })
        : sharedSearch(`${attempt}:initial`, attempt > 0, searchEpisodes));
      let selection;
      for (let wave = 0; ; wave++) {
        selection = await durableStep(ctx, `${prefix}:select:${wave}`, async () => {
          const result = await services.decider(entry).select({ entry, episodes, policy, excludedOfferKeys: [...excluded], maximumReleaseBytes: settings.tv.maximumReleaseBytes ?? undefined,
            offers: search.offers });
          if (result.offer && excluded.has(result.offer.key)) throw new Error("TV selector repeated a retained failed release");
          if (result.offer) {
            active();
            rememberSelectedCopies(ctx.repo, { media: "tv", targetId: target.id }, acquisition.id, result.offer,
              (result.matchingOffers ?? []).filter(copy => !excluded.has(copy.key) &&
                (copy.sizeBytes === null || copy.sizeBytes <= (settings.tv.maximumReleaseBytes ?? Infinity))));
          }
          active(); repo.decision(acquisition.id, { ...intake, selection: { title: result.offer?.title ?? null, provider: result.offer?.provider ?? null, reason: result.reason }, attempt });
          return result;
        });
        const deferred = [...search.deferred ?? [], ...selection.deferred ?? []];
        if (selection.offer || !deferred.length) break;
        const retryAt = Math.min(...deferred.map((provider) => Date.parse(provider.retryAt)).filter(Number.isFinite));
        if (!ctx.execution) throw new Error("Deferred TV indexers require durable execution");
        await ctx.execution.sleep(`${prefix}:indexer-wait:${wave}`, Math.max(1000, Math.min(3_600_000, retryAt - Date.now() || 60_000)));
        search = await tvUpstreamStep(ctx, `${prefix}:search-wave:${wave}`, () => sharedSearch(`${attempt}:retry:${wave}`, attempt > 0, searchEpisodes));
      }
      if (!selection.offer) {
        const followSource = (source: TvSource, skipFileIndexes: number[] = []) => {
          triedSources.add(`${source.ref.clientId}:${source.ref.nativeId}`);
          const next = reuseSequence++;
          return { source, skipFileIndexes, attempt: next, prefix: `tv:reuse:${next}` };
        };
        // Preserve the handoff sequence for runs already checkpointed on the old loop.
        let handoff = false;
        for (let wave = 0; wave < 20_000; wave++) {
          const retained = await durableStep(ctx, `${prefix}:sibling-sources:${wave}`, async () => {
            active();
            const found = await reusableTvSources(ctx.repo, target.id, acquisition.id, snapshot.episodeIds);
            return { sources: found.sources.filter(item => !triedSources.has(`${item.source.ref.clientId}:${item.source.ref.nativeId}`)
              && (!scopedRecovery || !excludedHashes.has(item.source.ref.nativeId))), pending: false };
          });
          if (retained.sources.length) {
            attempts.splice(position + 1, 0, ...retained.sources.map(item => followSource(item.source, item.skipFileIndexes)));
            handoff = true;
            break;
          }
          if (!retained.pending || !ctx.execution) break;
          await ctx.execution.sleep(`${prefix}:sibling-wait:${wave}`, services.timing?.pollMs ?? 5000);
        }
        if (handoff) continue;
        const pack = await durableStep(ctx, `${prefix}:pack-candidate-v2`, async () => {
          active();
          const candidates = repo.siblingPackEvidence(target.id, acquisition.id);
          const statuses = ctx.repo.getJobStatuses(candidates.flatMap(row => JSON.parse(row.job_ids_json) as number[]));
          const viable: { id: number; source: TvSource; observation: TorrentSnapshot }[] = [];
          for (const row of candidates) {
            if (!(JSON.parse(row.job_ids_json) as number[]).some(id => ["queued", "running"].includes(statuses.get(id) ?? ""))) continue;
            const parsed = tvSourceSchema.safeParse(JSON.parse(row.source_json));
            if (!parsed.success) continue;
            const source = parsed.data;
            if (source.ref.clientId !== services.client.id || triedSources.has(`${source.ref.clientId}:${source.ref.nativeId}`) ||
                (scopedRecovery && excludedHashes.has(source.ref.nativeId))) continue;
            const seen = await observePack(source);
            if (seen && (completeTvSource(source, seen) || progressing(seen))) viable.push({ id: row.torrent_id, source, observation: seen });
          }
          return viable;
        });
        for (const { id, source, observation } of pack) {
          const coverage = await coverPack(`${prefix}:pack-coverage:${id}`, source);
          if (coverage.length !== episodes.length) continue;
          if (!await waitForPack(`${prefix}:sibling-pack:${id}`, source, observation, coverage, selection.reason)) continue;
          await durableStep(ctx, `${prefix}:pack-seal:${id}`, async () => {
            const directory = path.join(settings.tv.managedRoot, "downloads", source.location.relativePath);
            const reservation = await readCanonicalDownload(roots.contentRoot, directory);
            if (reservation && !reservation.sealed)
              await sealCanonicalDownload(roots.contentRoot, directory, services.client, source.ref, active);
          });
          attempts.splice(position + 1, 0, followSource(source));
          handoff = true;
          break;
        }
        await durableStep(ctx, `${prefix}:pack-wait-clear`, () => {
          active(); repo.decision(acquisition.id, { ...intake, selection: { title: null, reason: selection.reason } });
        });
        if (handoff) continue;
        // Give a parked pre-change run a fresh attempt without rewriting its cached selections.
        if (!scopedRecovery) {
          const rejected = await durableStep(ctx, `${prefix}:scoped-recovery-fence`, () => {
            active(); return repo.episodeRejections(target.id, snapshot.episodeIds)
              .filter(row => !snapshot.lossRecoveryHashes?.includes(row.native_id));
          });
          excluded.clear();
          excludedHashes.clear();
          for (const row of rejected) { excluded.add(row.offer_key); excludedHashes.add(row.native_id); }
          for (const stalled of intake.stalledPacks ?? []) { excluded.add(stalled.key); excludedHashes.add(stalled.hash); }
          for (const failure of currentFailures) { excluded.add(failure.key); if (failure.hash) excludedHashes.add(failure.hash); }
          scopedRecovery = true;
          attempts.splice(position + 1, attempts.length, { source: null, skipFileIndexes: [], attempt: 3, prefix: "tv:attempt:3" });
          continue;
        }
        // A scoped search can find results buried under a shared show-wide lookup.
        if (attempt === 0 && (intake.searchGroup || intake.feedOffers?.length)) continue;
        if (attempt + 1 < packProbes.length) continue;
        if (await splitStalledBatch()) return "done";
        repo.decision(acquisition.id, { ...intake, selection: { title: null, reason: selection.reason },
          blockedCode: capacityProblem ? "stopped" : "no-match" });
        emit("blocked", "blocked", capacityProblem ?? selection.reason); repo.finishAcquisition(acquisition.id); return "done";
      }
      const offer = selection.offer;
      releaseEvidence = offer;
      const encoded = await tvUpstreamStep(ctx, `${prefix}:metainfo`, async () => Buffer.from(await services.fetchMetainfo(offer)).toString("base64"));
      const metainfo = Buffer.from(encoded, "base64"), metadata = inspectMetainfo(metainfo);
      active();
      if (metadata.size > (settings.tv.maximumReleaseBytes ?? Infinity)) {
        reject(offer.key); emit(`${attempt}:oversize`, "replacing", "Release exceeds the configured size limit; selecting another release"); continue;
      }
      if (excludedHashes.has(metadata.infoHash)) {
        reject(offer.key); emit(`${attempt}:retained-source`, "replacing", "Torrent was rejected for these episodes; trying another release"); continue;
      }
      const binding = await durableStep(ctx, `${prefix}:download-intent`, () => {
        const previous = repo.torrent(services.client.id, metadata.infoHash);
        return { location: previous ? JSON.parse(previous.location_json) as { rootId: string; relativePath: string }
          : { rootId: "tv", relativePath: `acquisition-${acquisition.id}-${attempt}` },
          canonicalDownload: !previous && Boolean(services.client.capabilities.canonicalDownloads) };
      });
      // In-flight pre-canonical jobs retain their original intent and regular-file layout.
      const intendedLocation = "location" in binding ? binding.location : binding as DownloadLocation;
      const registered = await durableStep<TorrentRef | { ref: TorrentRef; location: DownloadLocation; shared?: boolean } | { noSpace: string }>(ctx, `${prefix}:snatch`, () =>
        withTorrentRegistration(ctx.repo, { clientId: services.client.id, infoHash: metadata.infoHash }, async () => {
          active(); await assertMovieStorage(ctx.repo, roots.contentRoot);
          // A sibling may have registered this identity after our earlier intent was checkpointed.
          const existing = repo.torrent(services.client.id, metadata.infoHash);
          if (existing && existing.ownership_tag !== "Podible TV") throw new Error("TV torrent ownership conflicts with another domain");
          const location: DownloadLocation = existing ? JSON.parse(existing.location_json) : intendedLocation;
          const sourceRoot = path.resolve(settings.tv.managedRoot, "downloads", location.relativePath);
          if (location.rootId !== "tv" || !sourceRoot.startsWith(path.resolve(settings.tv.managedRoot, "downloads") + path.sep)) throw new Error("TV download escaped its owned root");
          await fs.mkdir(sourceRoot, { recursive: true });
          if (await fs.realpath(sourceRoot) !== sourceRoot) throw new Error("TV download layout traverses a symlink");
          const canonical = !existing && "canonicalDownload" in binding && binding.canonicalDownload;
          if (!existing) {
            const noSpace = await downloadCapacityProblem({ contentRoot: roots.contentRoot, sourceRoot, canonical, files: metadata.files });
            if (noSpace) return { noSpace };
          }
          await prepareCanonicalDownload({ contentRoot: roots.contentRoot, sourceRoot, clientId: services.client.id, metainfo,
            allowCreate: canonical, assertActive: active });
          const added = await services.client.ensureAdded({
            metainfo: ctx.repo.trackerAnnounces.currentMetainfo(offer.provider, metainfo), location,
          });
          if (added.clientId !== services.client.id || added.nativeId.toLowerCase() !== metadata.infoHash)
            throw new Error("TV client returned another torrent identity");
          active(); repo.transaction(() => {
            const id = repo.bindDownload(acquisition.id, added, location, offer.key);
            const proof = tvSourceSchema.safeParse({ ref: added, location, offerKey: offer.key, title: offer.title, provider: offer.provider, manifest: metadata });
            if (proof.success) repo.recordPackEvidence(id, proof.data);
          });
          return { ref: added, location, shared: Boolean(existing && repo.hasOtherUnfinishedDownload(existing.id, acquisition.id)) };
        }));
      // Completed pre-change checkpoints contain only the ref and retain their original location.
      if ("noSpace" in registered) {
        capacityProblem = registered.noSpace;
        reject(offer.key); emit(`${attempt}:disk-space`, "replacing", registered.noSpace); continue;
      }
      const ref = "ref" in registered ? registered.ref : registered;
      const location = "ref" in registered ? registered.location : intendedLocation;
      const sourceRoot = path.resolve(settings.tv.managedRoot, "downloads", location.relativePath);
      if (location.rootId !== "tv" || !sourceRoot.startsWith(path.resolve(settings.tv.managedRoot, "downloads") + path.sep)) throw new Error("TV download escaped its owned root");
      const downloadId = repo.torrent(ref.clientId, ref.nativeId)!.id;
      const shared = "ref" in registered && registered.shared;
      if (shared && scopedRecovery) {
        const pack = tvSourceSchema.parse({ ref, location, offerKey: offer.key, title: offer.title, provider: offer.provider, manifest: metadata });
        const observed = await durableStep(ctx, `${prefix}:shared-observation`, () => observePack(pack));
        let ready = observed !== null && completeTvSource(pack, observed);
        if (!ready && observed && progressing(observed)) {
          const coverage = await coverPack(`${prefix}:shared-coverage`, pack);
          if (coverage.length === episodes.length)
            ready = await waitForPack(`${prefix}:shared-pack`, pack, observed, coverage, selection.reason);
        }
        await durableStep(ctx, `${prefix}:shared-wait-clear`, () => {
          active(); repo.decision(acquisition.id, { ...intake, selection: { title: offer.title, provider: offer.provider, reason: selection.reason }, attempt });
        });
        if (!ready) {
          reject(offer.key, ref.nativeId);
          emit(`${attempt}:shared-unproven`, "replacing", "An existing torrent is not progressing or its files do not match this batch; trying another release");
          continue;
        }
      }
      if (snapshot.lossRecoveryHashes?.includes(metadata.infoHash))
        await recoverMissingTorrentSource({ ctx, client: services.client, ref, sourceRoot, metadata, prefix, active });
      emit(`${attempt}:downloading`, "downloading", `Downloading ${offer.title}`);
      let complete = false, lastBytes = -1, progressedAt = 0, startedAt = 0;
      if (ctx.execution?.armCompletion) {
        complete = await waitForTorrentCompletion(ctx, prefix, ref, stallDeadlineMs, async () => {
          const seen = await services.client.inspect(ref);
          active();
          if (seen.ref.clientId !== ref.clientId || seen.ref.nativeId !== ref.nativeId ||
              seen.location.rootId !== location.rootId || seen.location.relativePath !== location.relativePath)
            throw new Error("TV torrent observation changed identity or layout");
          repo.observeDownload(downloadId, seen);
          ctx.onAcquisitionChange?.();
          return seen;
        }, downloadComplete, shared ? undefined : () => { active(); return services.client.resume(ref); });
      } else for (let poll = 0; poll < 20_000; poll++) {
        active();
        const observation = await durableStep(ctx, `${prefix}:poll:${poll}`, async () => {
          const observed = await services.client.inspect(ref); active(); repo.observeDownload(downloadId, observed); return observed;
        });
        const progress = advanceDownloadProgress(observation, { lastBytes, progressedAt, startedAt });
        const now = progress.now;
        if (observation.ref.clientId !== ref.clientId || observation.ref.nativeId !== ref.nativeId ||
            observation.location.rootId !== location.rootId || observation.location.relativePath !== location.relativePath)
          throw new Error("TV torrent observation changed identity or layout");
        ({ lastBytes, progressedAt, startedAt } = progress);
        if (downloadComplete(observation)) { complete = true; break; }
        if (observation.seedOnly) break;
        if (!shared && observation.state === "inactive") await durableStep(ctx, `${prefix}:resume:${poll}`, () => { active(); return services.client.resume(ref); });
        const stallMs = services.timing?.stallMs ?? settings.recovery.stalledTorrentMinutes * 60_000;
        if ((stallMs > 0 && now - progressedAt >= stallMs) || now - startedAt > 86_400_000) break;
        if (ctx.execution) await ctx.execution.sleep(`${prefix}:wait:${poll}`, services.timing?.pollMs ?? 5000);
        else await Bun.sleep(services.timing?.pollMs ?? 5000);
      }
      if (!complete) {
        if (!shared) await durableStep(ctx, `${prefix}:retain-stalled`, () => { active(); return services.client.quiesce(ref); });
        stalledPacks.push({ key: offer.key, hash: ref.nativeId });
        reject(offer.key, ref.nativeId); emit(`${attempt}:stalled`, "replacing", "Retained a stalled TV download and searching another release"); continue;
      }
      await durableStep(ctx, `${prefix}:seal-download`, () => sealCanonicalDownload(roots.contentRoot, sourceRoot, services.client, ref, active));
      prepared = await tvSourceFiles(tvSourceSchema.parse({ ref, location, offerKey: offer.key, title: offer.title, provider: offer.provider, manifest: metadata }), services.client, settings.tv.managedRoot);
    }
    const source = snapshot.sourceReuse
      ? await durableStep(ctx, `${prefix}:source-evidence`, () => prepared.source)
      : prepared.source;
    const { sourceRoot } = await tvSourceFiles(source, services.client, settings.tv.managedRoot);
    const { ref } = source;
    emit(`${attempt}:verifying`, "verifying", "Checking playable streams and exact episode coverage");
    const files: TvFileCandidate[] = [];
    let incompleteProbe = false;
    for (const file of prepared.files) {
      if (skipFileIndexes.includes(file.index)) continue;
      const expectedFile = source.manifest.files[file.index]!;
      if (![".mkv", ".mp4", ".m4v", ".avi", ".mov", ".ts"].includes(path.extname(file.path).toLowerCase())) continue;
      const probeFile = async () => {
        try { return await services.probe(file.path); } catch { return null; }
      };
      // Legacy receipts have no captured file identities. Preserve their names, not their authority over current bytes.
      const probe = snapshot.sourceReuse ? await durableStep(ctx, `${prefix}:probe:${file.index}`, probeFile) : await probeFile();
      if (!snapshot.sourceReuse) await durableStep(ctx, `${prefix}:probe:${file.index}`, () => probe);
      if (probe) files.push({ index: file.index, name: expectedFile.path, sizeBytes: expectedFile.size, probe });
      else incompleteProbe = true;
    }
    const inspectFiles = async () => {
      const result = validateTvImportPlan(await services.decider(entry).inspect({ entry, episodes, policy, offer: releaseEvidence, files }), files, episodes);
      active(); repo.decision(acquisition.id, { ...intake, releaseTitle: releaseEvidence.title, provider: releaseEvidence.provider, plan: result, attempt }); return result;
    };
    const plan = snapshot.sourceReuse ? await durableStep(ctx, `${prefix}:inspect`, inspectFiles) : await inspectFiles();
    if (!snapshot.sourceReuse) await durableStep(ctx, `${prefix}:inspect`, () => plan);
    const eligible = plan.files.filter((mapping) => {
      const file = files.find((candidate) => candidate.index === mapping.fileIndex);
      return file && evaluateMovieQuality(policy, factsFromMovieProbe(file.probe, mapping.facts)).eligibility === "eligible";
    });
    if (!eligible.length) {
      active();
      const torrent = repo.torrent(ref.clientId, ref.nativeId);
      if (torrent && files.length && !incompleteProbe && !skipFileIndexes.length)
        repo.recordEpisodeRejection(acquisition.id, prefix, torrent.id, releaseEvidence.key, snapshot.episodeIds);
      reject(releaseEvidence.key, ref.nativeId); emit(`${prefix}:invalid`, "replacing", plan.reason); continue;
    }
    const adoptionSource = await tvSourceFiles(source, services.client, settings.tv.managedRoot);
    const sourceByIndex = new Map(adoptionSource.files.map(file => [file.index, file]));
    const content: TvContentFile[] = [];
    for (const mapping of eligible) {
      const candidate = files.find((file) => file.index === mapping.fileIndex)!;
      const id = await durableStep(ctx, `${prefix}:file-id:${mapping.fileIndex}`, randomUUID);
      const stored = await durableStep(ctx, `${prefix}:adopt:${mapping.fileIndex}`, async () => {
        active(); await assertMovieStorage(ctx.repo, roots.contentRoot);
        const selected = sourceByIndex.get(mapping.fileIndex);
        const expectedFile = source.manifest.files[mapping.fileIndex]!;
        if (!selected || path.resolve(selected.path) !== path.resolve(sourceRoot, expectedFile.path)) throw new Error("Selected TV file changed layout");
        const resolved = await resolveDownloadFile(roots.contentRoot, sourceRoot, selected.path, expectedFile.size);
        const stat = await fs.lstat(resolved.path), identity = expectedFile.identity!;
        if (stat.size !== expectedFile.size || stat.mtimeMs !== identity.mtimeMs || stat.ctimeMs !== identity.ctimeMs || stat.dev !== identity.device || stat.ino !== identity.inode)
          throw new Error("Selected TV file changed after verification");
        return adoptContentFile({ contentRoot: roots.contentRoot, sourceRoot, sourcePath: selected.path, fileId: id, expectedSize: candidate.sizeBytes, assertActive: active });
      });
      if (!snapshot.sourceReuse) {
        // A cached adoption may point at an older copy even when the new source still looks playable.
        const selected = prepared.files.find((file) => file.index === mapping.fileIndex)!;
        await adoptContentFile({ contentRoot: roots.contentRoot, sourceRoot, sourcePath: selected.path,
          fileId: stored.id, expectedSize: candidate.sizeBytes, assertActive: active });
      }
      content.push({ ...stored, probe: candidate.probe, episodeIds: mapping.episodeIds });
    }
    await tvSourceFiles(source, services.client, settings.tv.managedRoot);
    const facts = {
      identityVerified: true, plan, provider: releaseEvidence.provider, releaseTitle: releaseEvidence.title, selectedOfferKey: releaseEvidence.key,
      ...(snapshot.sourceReuse ? { source, attemptKey: prefix } : {}),
    };
    const versionId = await durableStep(ctx, `${prefix}:version`, () => { active(); return repo.recordVersion(acquisition.id, facts, content); });
    if (!snapshot.sourceReuse) { active(); if (repo.recordVersion(acquisition.id, facts, content) !== versionId) throw new Error("Legacy TV version differs from fresh verification"); }
    return publish(prefix, versionId, ref);
  }
  if (await splitStalledBatch()) return "done";
  emit("blocked", "blocked", capacityProblem ?? "TV acquisition alternatives exhausted; current episodes and completed torrent data retained");
  repo.finishAcquisition(acquisition.id); return "done";
}
