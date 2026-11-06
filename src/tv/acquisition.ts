import { createHash, randomUUID } from "node:crypto";
import { advanceDownloadProgress, downloadComplete } from "../media/download-lifecycle";
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
import { withTorrentUse } from "../media/torrent-use";
import { prepareCanonicalDownload, resolveDownloadFile, sealCanonicalDownload } from "../media/canonical-download";

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
export function queueTvAcquisition(repo: BooksRepo, requestId: number, reason: "initial" | "recovery" = "initial") {
  const request = repo.tv.requestById(requestId);
  if (!request || request.withdrawn_at) throw new Error("Unknown active TV request");
  return repo.tv.startAcquisition(request.target_id, reason, (acquisitionId) => repo.createJob({
    type: "tv_acquire", payload: { acquisitionId }, idempotencyKey: `tv-acquire:${acquisitionId}`, maxAttempts: 5,
  }).id, requestId);
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
export async function acquireTv(ctx: WorkerContext, job: JobRow, services: TvServices = ctx.tvServices ?? configuredTvServices(ctx.getSettings(), ctx.modelProvider)): Promise<"done"> {
  const acquisition = ctx.repo.tv.acquisition(JSON.parse(job.payload_json ?? "{}").acquisitionId);
  if (!acquisition?.request_id || acquisition.job_id !== job.id) throw new Error("Unknown TV acquisition execution");
  const repo = ctx.repo.tv;
  const target = repo.target(acquisition.target_id)!;
  const entry = repo.entry(target.entry_id)!;
  const requestId = acquisition.request_id;
  const intake = acquisition.decision_json ? JSON.parse(acquisition.decision_json) as { feedOffers?: MovieOffer[]; feedEvidence?: unknown } : {};
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
  };
  active();
  const snapshot = await durableStep(ctx, "tv:target-snapshot", async () => {
    const episodeIds = tvAcquisitionBatch(repo.missingEpisodes(requestId)
      .filter((episode) => episode.airDate && Date.parse(episode.airDate) <= Date.now())).map((episode) => episode.id);
    const reuse = await reusableTvSources(ctx.repo, target.id, acquisition.id, episodeIds);
    const retry = ctx.repo.contentLoss.retryDownloads({ namespace: "tv-target", id: String(target.id) });
    return { episodeIds, priorKeys: repo.downloads(target.id).map((download) => download.offer_key).filter(key => !retry.some(d => d.offer_key === key)), policy: JSON.parse(target.policy_json),
      priorHashes: repo.downloads(target.id).map((download) => download.native_id).filter(hash => !retry.some(d => d.native_id === hash)), sourceReuse: true,
      lossRecoveryHashes: retry.map(d => d.native_id),
      reuseSources: reuse.sources, consumedSources: reuse.consumed };
  });
  const episodes = snapshot.episodeIds.map((id) => repo.episode(id, entry.id)!);
  const policy = movieQualityPolicySchema.parse(snapshot.policy);
  if (!episodes.length) {
    emit("waiting", "retained", repo.missingEpisodes(requestId).length ? "Waiting for requested episodes to air and arrive in indexer feeds" : "All requested episodes are already available");
    repo.finishAcquisition(acquisition.id); return "done";
  }
  await durableStep(ctx, "tv:storage", () => { active(); return prepareMovieStorage(ctx.repo, roots.contentRoot); });
  await assertMovieStorage(ctx.repo, roots.contentRoot);
  const excluded = new Set(snapshot.priorKeys);
  const excludedHashes = new Set(snapshot.priorHashes ?? []);
  if (snapshot.consumedSources) emit("sources-consumed", "retained", `Skipped ${snapshot.consumedSources} verified source(s): no unused playable members for missing requested episodes`);
  const publish = async (prefix: string, versionId: number) => {
    const facts = repo.version(versionId)?.facts as { selectedOfferKey?: string; source?: { offerKey?: string } } | undefined;
    const selectedKey = facts?.selectedOfferKey ?? facts?.source?.offerKey;
    if (selectedKey) await durableStep(ctx, `${prefix}:cross-seed-copies`, () => {
      active();
      return queueSelectedCrossSeeds(ctx.repo, { media: "tv", targetId: target.id }, acquisition.id, selectedKey);
    });
    const eventKey = prefix.startsWith("tv:attempt:") ? prefix.slice("tv:attempt:".length) : prefix;
    emit(`${eventKey}:publishing`, "publishing", "Publishing an isolated TV view and verifying each episode in Plex");
    for (let scan = 0; ; scan++) {
      const published = await durableStep(ctx, `${prefix}:publish:${scan}`, async () => {
        try { active(); await publishTvVersion({ repo, roots, library: services.library, targetId: target.id, versionId, assertActive: active }); return true; }
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
    await durableStep(ctx, "tv:finish", () => ctx.repo.transaction(() => {
      active(); repo.finishAcquisition(acquisition.id);
      if (repo.missingEpisodes(requestId).some((episode) => episode.airDate && Date.parse(episode.airDate) <= Date.now())) queueTvAcquisition(ctx.repo, requestId, "recovery");
    }));
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
      return publish(facts.attemptKey, recorded.id);
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
    return publish(recorded.prefix, recorded.version.id);
  }
  const attempts = [
    ...(snapshot.sourceReuse ? snapshot.reuseSources ?? [] : []).map(({ source, skipFileIndexes }, attempt) => ({ source: source as TvSource | null, skipFileIndexes, attempt, prefix: `tv:reuse:${attempt}` })),
    ...Array.from({ length: 3 }, (_, attempt) => ({ source: null as TvSource | null, skipFileIndexes: [] as number[], attempt, prefix: `tv:attempt:${attempt}` })),
  ];
  for (const { source: reusable, skipFileIndexes, attempt, prefix } of attempts) {
    let releaseEvidence: Pick<MovieOffer, "key" | "title" | "provider">;
    let prepared: Awaited<ReturnType<typeof tvSourceFiles>>;
    if (reusable) {
      let observation;
      try {
        active();
        if (reusable.manifest.size > settings.tv.maximumReleaseBytes) throw new Error("Retained TV source exceeds acquisition limits");
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
      emit(`${attempt}:searching`, "searching", `Searching releases for ${entry.title}`);
      let search = await tvUpstreamStep(ctx, `${prefix}:search`, () => attempt === 0 && intake.feedOffers?.length
        ? Promise.resolve({ offers: intake.feedOffers.map((offer) => refreshTvFeedOffer(ctx.repo, offer)), warnings: [] })
        : services.search(entry));
      let selection;
      for (let wave = 0; ; wave++) {
        selection = await durableStep(ctx, `${prefix}:select:${wave}`, async () => {
          const result = await services.decider(entry).select({ entry, episodes, policy, excludedOfferKeys: [...excluded], maximumReleaseBytes: settings.tv.maximumReleaseBytes,
            offers: search.offers.filter((offer) => !excluded.has(offer.key) && (offer.sizeBytes === null || offer.sizeBytes <= settings.tv.maximumReleaseBytes)) });
          if (result.offer && excluded.has(result.offer.key)) throw new Error("TV selector repeated a retained failed release");
          if (result.offer) {
            active();
            rememberSelectedCopies(ctx.repo, { media: "tv", targetId: target.id }, acquisition.id, result.offer,
              (result.matchingOffers ?? []).filter(copy => !excluded.has(copy.key) &&
                (copy.sizeBytes === null || copy.sizeBytes <= settings.tv.maximumReleaseBytes)));
          }
          active(); repo.decision(acquisition.id, { ...intake, selection: { title: result.offer?.title ?? null, provider: result.offer?.provider ?? null, reason: result.reason }, attempt });
          return result;
        });
        const deferred = [...search.deferred ?? [], ...selection.deferred ?? []];
        if (selection.offer || !deferred.length) break;
        const retryAt = Math.min(...deferred.map((provider) => Date.parse(provider.retryAt)).filter(Number.isFinite));
        if (!ctx.execution) throw new Error("Deferred TV indexers require durable execution");
        await ctx.execution.sleep(`${prefix}:indexer-wait:${wave}`, Math.max(1000, Math.min(3_600_000, retryAt - Date.now() || 60_000)));
        search = await tvUpstreamStep(ctx, `${prefix}:search-wave:${wave}`, () => services.search(entry));
      }
      if (!selection.offer) { emit("blocked", "blocked", selection.reason); repo.finishAcquisition(acquisition.id); return "done"; }
      const offer = selection.offer;
      releaseEvidence = offer;
      const encoded = await tvUpstreamStep(ctx, `${prefix}:metainfo`, async () => Buffer.from(await services.fetchMetainfo(offer)).toString("base64"));
      const metainfo = Buffer.from(encoded, "base64"), metadata = inspectMetainfo(metainfo);
      if (metadata.size > settings.tv.maximumReleaseBytes || metadata.files.length > 300) {
        excluded.add(offer.key); emit(`${attempt}:oversize`, "replacing", "Release exceeds bounded TV acquisition limits; selecting another release"); continue;
      }
      if (excludedHashes.has(metadata.infoHash)) {
        excluded.add(offer.key); emit(`${attempt}:retained-source`, "replacing", "Previously attempted torrent identity remains excluded from new downloads"); continue;
      }
      const binding = await durableStep(ctx, `${prefix}:download-intent`, () => {
        const previous = repo.torrent(services.client.id, metadata.infoHash);
        return { location: previous ? JSON.parse(previous.location_json) as { rootId: string; relativePath: string }
          : { rootId: "tv", relativePath: `acquisition-${acquisition.id}-${attempt}` },
          canonicalDownload: !previous && Boolean(services.client.capabilities.canonicalDownloads) };
      });
      // In-flight pre-canonical jobs retain their original intent and regular-file layout.
      const location = "location" in binding ? binding.location : binding as { rootId: string; relativePath: string };
      const sourceRoot = path.resolve(settings.tv.managedRoot, "downloads", location.relativePath);
      if (location.rootId !== "tv" || !sourceRoot.startsWith(path.resolve(settings.tv.managedRoot, "downloads") + path.sep)) throw new Error("TV download escaped its owned root");
      const ref = await durableStep(ctx, `${prefix}:snatch`, async () => {
        active(); await assertMovieStorage(ctx.repo, roots.contentRoot);
        await fs.mkdir(sourceRoot, { recursive: true });
        if (await fs.realpath(sourceRoot) !== sourceRoot) throw new Error("TV download layout traverses a symlink");
        return withTorrentUse(ctx.repo, { clientId: services.client.id, infoHash: metadata.infoHash }, async () => {
          await prepareCanonicalDownload({ contentRoot: roots.contentRoot, sourceRoot, clientId: services.client.id, metainfo,
            allowCreate: "canonicalDownload" in binding && binding.canonicalDownload, assertActive: active });
          const added = await services.client.ensureAdded({ metainfo, location });
          if (added.clientId !== services.client.id || added.nativeId.toLowerCase() !== metadata.infoHash)
            throw new Error("TV client returned another torrent identity");
          active(); repo.bindDownload(acquisition.id, added, location, offer.key); return added;
        });
      });
      const downloadId = repo.torrent(ref.clientId, ref.nativeId)!.id;
      if (snapshot.lossRecoveryHashes?.includes(metadata.infoHash))
        await recoverMissingTorrentSource({ ctx, client: services.client, ref, sourceRoot, metadata, prefix, active });
      emit(`${attempt}:downloading`, "downloading", `Downloading ${offer.title}`);
      let complete = false, lastBytes = -1, progressedAt = 0, startedAt = 0;
      for (let poll = 0; poll < 20_000; poll++) {
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
        if (observation.state === "inactive") await durableStep(ctx, `${prefix}:resume:${poll}`, () => { active(); return services.client.resume(ref); });
        const stallMs = services.timing?.stallMs ?? settings.recovery.stalledTorrentMinutes * 60_000;
        if ((stallMs > 0 && now - progressedAt >= stallMs) || now - startedAt > 86_400_000) break;
        if (ctx.execution) await ctx.execution.sleep(`${prefix}:wait:${poll}`, services.timing?.pollMs ?? 5000);
        else await Bun.sleep(services.timing?.pollMs ?? 5000);
      }
      if (!complete) {
        await durableStep(ctx, `${prefix}:retain-stalled`, () => { active(); return services.client.quiesce(ref); });
        excluded.add(offer.key); excludedHashes.add(ref.nativeId); emit(`${attempt}:stalled`, "replacing", "Retained a stalled TV download and searching another release"); continue;
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
    if (!eligible.length) { excluded.add(releaseEvidence.key); excludedHashes.add(ref.nativeId); emit(`${prefix}:invalid`, "replacing", plan.reason); continue; }
    await tvSourceFiles(source, services.client, settings.tv.managedRoot);
    const content: TvContentFile[] = [];
    for (const mapping of eligible) {
      const candidate = files.find((file) => file.index === mapping.fileIndex)!;
      const id = await durableStep(ctx, `${prefix}:file-id:${mapping.fileIndex}`, randomUUID);
      const stored = await durableStep(ctx, `${prefix}:adopt:${mapping.fileIndex}`, async () => {
        active(); await assertMovieStorage(ctx.repo, roots.contentRoot);
        const selected = (await services.client.files(ref)).find((file) => file.index === mapping.fileIndex);
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
    return publish(prefix, versionId);
  }
  emit("blocked", "blocked", "TV acquisition alternatives exhausted; current episodes and completed torrent data retained");
  repo.finishAcquisition(acquisition.id); return "done";
}
