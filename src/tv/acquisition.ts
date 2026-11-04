import { createHash, randomUUID } from "node:crypto";
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
import { TvUpstreamError } from "./upstream";
import { configuredTvServices, type TvServices } from "./runtime";
import { validateTvImportPlan, type TvFileCandidate } from "./agents";
import type { TvContentFile } from "./types";
import type { MovieOffer } from "../movies/offers";
import { refreshTvFeedOffer } from "./feed";

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
  const snapshot = await durableStep(ctx, "tv:target-snapshot", () => ({
    episodeIds: repo.missingEpisodes(requestId).filter((episode) => episode.airDate && Date.parse(episode.airDate) <= Date.now()).slice(0, 50).map((episode) => episode.id),
    priorKeys: repo.downloads(target.id).map((download) => download.offer_key), policy: JSON.parse(target.policy_json),
  }));
  const episodes = snapshot.episodeIds.map((id) => repo.episode(id, entry.id)!);
  const policy = movieQualityPolicySchema.parse(snapshot.policy);
  if (!episodes.length) {
    emit("waiting", "retained", repo.missingEpisodes(requestId).length ? "Waiting for requested episodes to air and arrive in indexer feeds" : "All requested episodes are already available");
    repo.finishAcquisition(acquisition.id); return "done";
  }
  await durableStep(ctx, "tv:storage", () => { active(); return prepareMovieStorage(ctx.repo, roots.contentRoot); });
  await assertMovieStorage(ctx.repo, roots.contentRoot);
  const excluded = new Set(snapshot.priorKeys);
  for (let attempt = 0; attempt < 3; attempt++) {
    const prefix = `tv:attempt:${attempt}`;
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
    const offer = selection.offer;
    if (!offer) { emit("blocked", "blocked", selection.reason); repo.finishAcquisition(acquisition.id); return "done"; }
    const encoded = await tvUpstreamStep(ctx, `${prefix}:metainfo`, async () => Buffer.from(await services.fetchMetainfo(offer)).toString("base64"));
    const metainfo = Buffer.from(encoded, "base64"), metadata = inspectMetainfo(metainfo);
    if (metadata.size > settings.tv.maximumReleaseBytes || metadata.files.length > 300) {
      excluded.add(offer.key); emit(`${attempt}:oversize`, "replacing", "Release exceeds bounded TV acquisition limits; selecting another release"); continue;
    }
    const location = await durableStep(ctx, `${prefix}:download-intent`, () => {
      const previous = repo.torrent(services.client.id, metadata.infoHash);
      return previous ? JSON.parse(previous.location_json) as { rootId: string; relativePath: string }
        : { rootId: "tv", relativePath: `acquisition-${acquisition.id}-${attempt}` };
    });
    const sourceRoot = path.resolve(settings.tv.managedRoot, "downloads", location.relativePath);
    if (location.rootId !== "tv" || !sourceRoot.startsWith(path.resolve(settings.tv.managedRoot, "downloads") + path.sep)) throw new Error("TV download escaped its owned root");
    const ref = await durableStep(ctx, `${prefix}:snatch`, async () => {
      active(); await assertMovieStorage(ctx.repo, roots.contentRoot);
      await fs.mkdir(sourceRoot, { recursive: true });
      if (await fs.realpath(sourceRoot) !== sourceRoot) throw new Error("TV download layout traverses a symlink");
      const added = await services.client.ensureAdded({ metainfo, location });
      if (added.clientId !== services.client.id || added.nativeId.toLowerCase() !== metadata.infoHash)
        throw new Error("TV client returned another torrent identity");
      active(); repo.bindDownload(acquisition.id, added, location, offer.key); return added;
    });
    const downloadId = repo.torrent(ref.clientId, ref.nativeId)!.id;
    emit(`${attempt}:downloading`, "downloading", `Downloading ${offer.title}`);
    let complete = false, lastBytes = -1, progressedAt = 0, startedAt = 0;
    for (let poll = 0; poll < 20_000; poll++) {
      active();
      const observation = await durableStep(ctx, `${prefix}:poll:${poll}`, async () => {
        const observed = await services.client.inspect(ref); active(); repo.observeDownload(downloadId, observed); return observed;
      });
      const now = Date.parse(observation.observedAt);
      if (observation.ref.clientId !== ref.clientId || observation.ref.nativeId !== ref.nativeId ||
          observation.location.rootId !== location.rootId || observation.location.relativePath !== location.relativePath)
        throw new Error("TV torrent observation changed identity or layout");
      if (!Number.isFinite(now)) throw new Error("TV engine returned an invalid observation time");
      startedAt ||= now;
      if (observation.complete && observation.state !== "checking") { complete = true; break; }
      if (observation.bytesDone === null || observation.bytesDone !== lastBytes || observation.state === "checking") {
        lastBytes = observation.bytesDone ?? -1; progressedAt = now;
      }
      if (observation.state === "inactive") await durableStep(ctx, `${prefix}:resume:${poll}`, () => { active(); return services.client.resume(ref); });
      const stallMs = services.timing?.stallMs ?? settings.recovery.stalledTorrentMinutes * 60_000;
      if ((stallMs > 0 && now - progressedAt >= stallMs) || now - startedAt > 86_400_000) break;
      if (ctx.execution) await ctx.execution.sleep(`${prefix}:wait:${poll}`, services.timing?.pollMs ?? 5000);
      else await Bun.sleep(services.timing?.pollMs ?? 5000);
    }
    if (!complete) {
      await durableStep(ctx, `${prefix}:retain-stalled`, () => { active(); return services.client.quiesce(ref); });
      excluded.add(offer.key); emit(`${attempt}:stalled`, "replacing", "Retained a stalled TV download and searching another release"); continue;
    }
    emit(`${attempt}:verifying`, "verifying", "Checking playable streams and exact episode coverage");
    const clientFiles = await services.client.files(ref);
    const files: TvFileCandidate[] = [];
    for (const file of clientFiles) {
      const expectedFile = metadata.files[file.index];
      if (!expectedFile || path.resolve(file.path) !== path.resolve(sourceRoot, expectedFile.path) ||
          !path.resolve(file.path).startsWith(sourceRoot + path.sep) || await fs.realpath(file.path) !== path.resolve(file.path))
        throw new Error("TV client file escaped metainfo layout");
      if (![".mkv", ".mp4", ".m4v", ".avi", ".mov", ".ts"].includes(path.extname(file.path).toLowerCase())) continue;
      if ((await fs.lstat(file.path)).size !== expectedFile.size) throw new Error("Completed TV file size differs from metainfo");
      const probe = await durableStep(ctx, `${prefix}:probe:${file.index}`, async () => {
        try { return await services.probe(file.path); } catch { return null; }
      });
      if (probe) files.push({ index: file.index, name: expectedFile.path, sizeBytes: expectedFile.size, probe });
    }
    const plan = await durableStep(ctx, `${prefix}:inspect`, async () => {
      const result = validateTvImportPlan(await services.decider(entry).inspect({ entry, episodes, policy, offer, files }), files, episodes);
      active(); repo.decision(acquisition.id, { ...intake, releaseTitle: offer.title, provider: offer.provider, plan: result, attempt }); return result;
    });
    const eligible = plan.files.filter((mapping) => {
      const file = files.find((candidate) => candidate.index === mapping.fileIndex);
      return file && evaluateMovieQuality(policy, factsFromMovieProbe(file.probe, mapping.facts)).eligibility === "eligible";
    });
    if (!eligible.length) { excluded.add(offer.key); emit(`${attempt}:invalid`, "replacing", plan.reason); continue; }
    const content: TvContentFile[] = [];
    for (const mapping of eligible) {
      const candidate = files.find((file) => file.index === mapping.fileIndex)!;
      const id = await durableStep(ctx, `${prefix}:file-id:${mapping.fileIndex}`, randomUUID);
      const stored = await durableStep(ctx, `${prefix}:adopt:${mapping.fileIndex}`, async () => {
        active(); await assertMovieStorage(ctx.repo, roots.contentRoot);
        const source = (await services.client.files(ref)).find((file) => file.index === mapping.fileIndex);
        if (!source) throw new Error("Selected TV file disappeared");
        return adoptContentFile({ contentRoot: roots.contentRoot, sourceRoot, sourcePath: source.path, fileId: id, expectedSize: candidate.sizeBytes, assertActive: active });
      });
      content.push({ ...stored, probe: candidate.probe, episodeIds: mapping.episodeIds });
    }
    const versionId = await durableStep(ctx, `${prefix}:version`, () => { active(); return repo.recordVersion(acquisition.id, { identityVerified: true, plan, provider: offer.provider, releaseTitle: offer.title }, content); });
    emit(`${attempt}:publishing`, "publishing", "Publishing an isolated TV view and verifying each episode in Plex");
    for (let scan = 0; ; scan++) {
      const published = await durableStep(ctx, `${prefix}:publish:${scan}`, async () => {
        try { active(); await publishTvVersion({ repo, roots, library: services.library, targetId: target.id, versionId, assertActive: active }); return true; }
        catch (error) {
          active();
          if (repo.publications().some((item) => item.target_id === target.id && item.version_id === versionId)) {
            // Leave the final observation as a failed step, not a cached negative result.
            // Reflow retry then rechecks Plex while retaining the snatch/adoption receipts.
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
    return "done";
  }
  emit("blocked", "blocked", "TV acquisition alternatives exhausted; current episodes and completed torrent data retained");
  repo.finishAcquisition(acquisition.id); return "done";
}
