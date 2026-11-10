import path from "node:path";
import { downloadCapacityProblem } from "../media/download-capacity";
import { advanceDownloadProgress, downloadComplete } from "../media/download-lifecycle";
import { promises as fs } from "node:fs";
import { randomUUID, createHash } from "node:crypto";
import { z } from "zod";
import type { JobRow } from "../app-types";
import { durableStep, type WorkerContext } from "../worker/context";
import { waitForPlexPublication, waitForTorrentCompletion } from "../worker/media-completion";
import { assertMovieStorage, prepareMovieStorage } from "./storage";
import { adoptContentFile } from "../media/content-store";
import { inspectMetainfo } from "../media/metainfo";
import type { DownloadLocation, TorrentRef } from "../media/torrent-client";
import {
  assessMovieUpgrade,
  evaluateMovieQuality,
  movieQualityPolicySchema,
} from "./quality";
import { factsFromMovieProbe } from "./probe";
import { publishMovieVersion } from "./publication";
import type { MovieOffer, MovieSearch } from "./offers";
import type { MovieServices, MovieFileCandidate } from "./operations";
import { readTransmissionIdentity } from "../media/managed-transmission";
import { movieUpstreamStep } from "./upstream";
import { refreshMovieFeedOffer } from "./feed-offers";
import { recoverMissingTorrentSource } from "../media/recovery-source";
import { withTorrentUse } from "../media/torrent-use";
import { assessConfirmedNukeReplacement } from "../media/release-health";
import { rememberSelectedCopies } from "../media/selected-copies";
import { queueSelectedCrossSeeds } from "../media/cross-seed";
import { prepareCanonicalDownload, resolveDownloadFile, sealCanonicalDownload } from "../media/canonical-download";

export const movieAcquisitionPayloadSchema = z.object({
  acquisitionId: z.number().int().positive(),
});
export function movieRoots(root: string, serverViewRoot: string) {
  if (!path.isAbsolute(root) || !path.isAbsolute(serverViewRoot))
    throw new Error("Movie roots must be absolute paths");
  return {
    contentRoot: path.join(root, "objects"),
    viewRoot: path.join(root, "views", "plex", "movies"),
    serverViewRoot,
  };
}
export function movieConfigurationIdentity(ctx: WorkerContext): string {
  const settings = ctx.getSettings();
  return createHash("sha256")
    .update(
      JSON.stringify([
        settings.movies.managedRoot,
        ctx.movieServices?.client.id ??
          `transmission:${readTransmissionIdentity().id}`,
        settings.movies.plexUrl,
        settings.movies.plexSectionId,
        settings.movies.plexViewPath,
      ]),
    )
    .digest("hex");
}

export async function acquireMovie(
  ctx: WorkerContext,
  job: JobRow,
  services: MovieServices,
): Promise<"done"> {
  const payload = movieAcquisitionPayloadSchema.parse(
    JSON.parse(job.payload_json ?? "{}"),
  );
  const repo = ctx.repo.movies;
  const acquisition = repo.acquisition(payload.acquisitionId);
  const target = acquisition && repo.target(acquisition.target_id);
  const movie = target && repo.movie(target.movie_id);
  if (!acquisition || !target || !movie || acquisition.job_id !== job.id)
    throw new Error("Invalid movie acquisition identity");
  const settings = ctx.getSettings();
  const roots = movieRoots(
    settings.movies.managedRoot,
    settings.movies.plexViewPath,
  );
  const configuration = await durableStep(ctx, "movie:configuration", () =>
    movieConfigurationIdentity(ctx),
  );
  const active = () => {
    ctx.execution?.assertActive();
    if (movieConfigurationIdentity(ctx) !== configuration)
      throw new Error(
        "Movie storage/client configuration changed; start a new acquisition",
      );
    if (!repo.activeRequestCount(target.id))
      throw new Error("All requests for this target were withdrawn");
    if (ctx.repo.retention.get({ namespace: "movie-target", id: String(target.id) })?.desiredPresence === "evicted")
      throw new Error("Movie target is intentionally evicted; only an explicit re-request restores demand");
  };
  active();
  const initial = await durableStep(ctx, "movie:target-snapshot", () => {
    const retry = new Set(ctx.repo.contentLoss.retryDownloads({ namespace: "movie-target", id: String(target.id) }).map(d => d.offer_key));
    return {
    policy: JSON.parse(target.policy_json),
    current: target.active_version_id
      ? repo.version(target.active_version_id)!.facts
      : null,
    currentVersionId: target.active_version_id,
    priorKeys: repo.downloads(target.id).map((download) => download.offer_key).filter(key => !retry.has(key)),
    lossRecoveryKeys: [...retry],
  }; });
  const policy = movieQualityPolicySchema.parse(initial.policy);
  const current = initial.current;
  const replacingNukedCurrent = () => Boolean(current &&
    ctx.repo.releaseHealth.currentMovieNuke(target.id)?.versionId ===
      (initial.currentVersionId ?? target.active_version_id));
  const improvesOrRepairs = (facts: Parameters<typeof evaluateMovieQuality>[1]) => !current ||
    (replacingNukedCurrent()
      ? assessConfirmedNukeReplacement(policy, current, facts).allowed
      : assessMovieUpgrade(policy, current, facts).upgrade);
  const manual = acquisition.decision_json
    ? (JSON.parse(acquisition.decision_json) as {
        manualOffer?: MovieOffer;
        feedOffers?: MovieOffer[];
      })
    : null;
  await durableStep(ctx, "movie:storage", async () => {
    active();
    await prepareMovieStorage(ctx.repo, roots.contentRoot);
  });
  await assertMovieStorage(ctx.repo, roots.contentRoot);
  const emit = (
    key: string,
    stage: Parameters<typeof repo.event>[3],
    message: string,
  ) => {
    active();
    repo.event(
      target.id,
      acquisition.id,
      `acquisition:${acquisition.id}:${key}`,
      stage,
      message,
    );
    ctx.onAcquisitionChange?.();
  };
  const priorKeys = new Set(initial.priorKeys);
  const excluded = new Set<string>();
  let capacityProblem: string | undefined;
  let feedDownloadFailed = false;
  for (let attempt = 0; attempt < 3; attempt++) {
    active();
    const prefix = `movie:attempt:${attempt}`;
    const useFeed = Boolean(
      manual?.feedOffers &&
        (!feedDownloadFailed ||
          manual.feedOffers.some((offer) => !excluded.has(offer.key))),
    );
    emit(
      `${attempt}:search`,
      useFeed ? "selecting" : "searching",
      useFeed
        ? "Considering releases received from indexer feeds"
        : feedDownloadFailed
          ? "Searching for a replacement after a failed feed download"
          : attempt
            ? "Searching for another release; previous downloads are retained"
            : "Searching movie releases",
    );
    const { offer, selection, selectedInfoHash } = await (async () => {
      for (let wave = 0; ; wave++) {
        const phase = wave ? `${prefix}:indexer-wave:${wave}` : prefix;
        const search = await movieUpstreamStep<MovieSearch>(
          ctx,
          `${phase}:search`,
          async () =>
            useFeed && manual?.feedOffers
              ? { offers: manual.feedOffers, warnings: [] }
              : attempt === 0 && manual?.manualOffer
                ? { offers: [manual.manualOffer], warnings: [] }
                : services.search(movie),
        );
        const candidates = search.offers
          .map((offer) => refreshMovieFeedOffer(ctx.repo, offer))
          .filter(
            (offer) =>
              !excluded.has(offer.key) &&
              !ctx.repo.releaseHealth.isOfferBlocked(offer) &&
              (!priorKeys.has(offer.key) ||
                manual?.manualOffer?.key === offer.key),
          )
          .filter(
            (offer) =>
              offer.sizeBytes === null ||
              offer.sizeBytes <= (settings.movies.maximumReleaseBytes ?? Infinity),
          );
        if (
          attempt === 0 &&
          manual?.manualOffer &&
          !ctx.repo.releaseHealth.isOfferBlocked(manual.manualOffer) &&
          !candidates.some((offer) => offer.key === manual.manualOffer!.key)
        )
          candidates.push(manual.manualOffer);
        emit(
          `${attempt}:select`,
          "selecting",
          "Checking movie identity, cut and quality",
        );
        const repairVersion = replacingNukedCurrent()
          ? (initial.currentVersionId ?? target.active_version_id) : null;
        const selection = await durableStep(
          ctx,
          `${phase}:select${repairVersion === null ? "" : `:nuke:${repairVersion}`}`,
          async () => {
            const selected = await services.decider.select({
              movie,
              policy,
              offers: candidates.map((offer) =>
                refreshMovieFeedOffer(ctx.repo, offer),
              ),
              // Equal quality may repair a trusted nuke; local gates still prohibit downgrades.
              current: repairVersion === null ? current : null,
            });
            if (
              manual?.manualOffer &&
              attempt === 0 &&
              selected.offerKey !== manual.manualOffer.key
            )
              throw new Error(
                "The selected manual release did not pass semantic review",
              );
            if (
              selected.offerKey &&
              !candidates.some((offer) => offer.key === selected.offerKey)
            )
              throw new Error("Selection references an unknown release");
            if (selected.offerKey) {
              active();
              const selectedOffer = candidates.find(offer => offer.key === selected.offerKey)!;
              const copies = (selected.matchingOffers ?? []).map(copy => {
                const known = candidates.find(offer => offer.key === copy.key);
                if (!known) throw new Error("Selection copy references an unknown release");
                return known;
              });
              rememberSelectedCopies(ctx.repo, { media: "movie", targetId: target.id }, acquisition.id, selectedOffer, copies);
            }
            repo.decision(acquisition.id, {
              ...manual,
              selection: selected,
              attempt,
            });
            return selected;
          },
        );
        // A report arriving during selection invalidates the old "no upgrade" decision.
        if (!selection.offerKey && repairVersion === null && replacingNukedCurrent())
          continue;
        const offer = candidates.find(
          (entry) => entry.key === selection.offerKey,
        );
        if (!offer && search.deferred?.length) {
          active();
          const retryAt = Math.min(
            ...search.deferred
              .map((source) => Date.parse(source.retryAt))
              .filter(Number.isFinite),
          );
          const delay = Math.max(
            1000,
            Math.min(3_600_000, retryAt - Date.now() || 60_000),
          );
          emit(
            `${attempt}:waiting-indexers:${wave}`,
            "searching",
            `Waiting for deferred indexers: ${search.deferred.map((source) => source.provider).join(", ")}`,
          );
          if (ctx.execution)
            await ctx.execution.sleep(`${phase}:wait-indexers`, delay);
          else {
            if (wave >= 2)
              throw new Error("Indexer retry requires durable execution");
            await Bun.sleep(delay);
          }
          continue;
        }
        return { offer, selection, selectedInfoHash: search.offers.find((entry) => entry.key === selection.offerKey)?.infoHash ?? offer?.infoHash ?? null };
      }
    })();
    if (selection.offerKey && ctx.repo.releaseHealth.isOfferBlocked({ key: selection.offerKey, infoHash: selectedInfoHash })) {
      excluded.add(selection.offerKey);
      emit(`${attempt}:nuked-selection`, "replacing", "Confirmed defective release excluded; looking for another source");
      continue;
    }
    if (!offer) {
      emit(
        "blocked",
        current ? "retained" : capacityProblem ? "blocked" : "waiting",
        capacityProblem ?? (current ? selection.reason : "No matching release has been found yet. Watching incoming indexer releases automatically."),
      );
      await durableStep(ctx, "movie:finish-no-candidate", () =>
        repo.finishAcquisition(acquisition.id),
      );
      return "done";
    }
    const provisional = evaluateMovieQuality(policy, selection.facts);
    if (
      provisional.eligibility === "ineligible" ||
      (current &&
        provisional.eligibility === "eligible" &&
        !improvesOrRepairs(selection.facts))
    ) {
      excluded.add(offer.key);
      emit(
        `${attempt}:reject`,
        "retained",
        provisional.reasons.join("; ") ||
          "Release does not improve the current version",
      );
      continue;
    }
    const encoded = await movieUpstreamStep(
      ctx,
      `${prefix}:metainfo`,
      async () =>
        Buffer.from(
          await services.fetchMetainfo(refreshMovieFeedOffer(ctx.repo, offer)),
        ).toString("base64"),
    );
    const metainfo = Buffer.from(encoded, "base64");
    const metadata = inspectMetainfo(metainfo);
    active();
    if (ctx.repo.releaseHealth.isOfferBlocked({ key: offer.key, infoHash: metadata.infoHash })) {
      excluded.add(offer.key);
      emit(`${attempt}:nuked-metainfo`, "replacing", "Torrent identity belongs to a confirmed defective release; trying another source");
      continue;
    }
    if (metadata.size > (settings.movies.maximumReleaseBytes ?? Infinity)) {
      excluded.add(offer.key);
      emit(`${attempt}:oversize`, "replacing", "Release exceeds the configured size limit; trying another release");
      continue;
    }
    const binding = await durableStep(ctx, `${prefix}:download-intent`, () => {
      const prior = repo.torrent(services.client.id, metadata.infoHash);
      const location: DownloadLocation = prior
        ? JSON.parse(prior.location_json)
        : {
            rootId: "movies",
            relativePath: `acquisition-${acquisition.id}-${attempt}`,
          };
      return { location, canonicalDownload: !prior && Boolean(services.client.capabilities.canonicalDownloads) };
    });
    const downloadRoot = path.join(settings.movies.managedRoot, "downloads");
    const sourceRoot = path.resolve(
      downloadRoot,
      binding.location.relativePath,
    );
    if (!sourceRoot.startsWith(`${path.resolve(downloadRoot)}${path.sep}`))
      throw new Error("Download escaped its managed root");
    const ref = await durableStep(ctx, `${prefix}:snatch`, async () => {
      active();
      await assertMovieStorage(ctx.repo, roots.contentRoot);
      await fs.mkdir(sourceRoot, { recursive: true });
      if ((await fs.realpath(sourceRoot)) !== sourceRoot)
        throw new Error("Download layout traverses a symlink");
      if (settings.movies.downloadGroupId !== null) {
        await fs.chown(sourceRoot, -1, settings.movies.downloadGroupId);
        await fs.chmod(sourceRoot, 0o2770);
      }
      return withTorrentUse(ctx.repo, { clientId: services.client.id, infoHash: metadata.infoHash }, async () => {
        if (!repo.torrent(services.client.id, metadata.infoHash)) {
          const noSpace = await downloadCapacityProblem({ contentRoot: roots.contentRoot, sourceRoot,
            canonical: binding.canonicalDownload ?? false, files: metadata.files });
          if (noSpace) return { noSpace };
        }
        await prepareCanonicalDownload({ contentRoot: roots.contentRoot, sourceRoot, clientId: services.client.id,
          metainfo, allowCreate: binding.canonicalDownload ?? false, assertActive: active });
        const added = await services.client.ensureAdded({
          metainfo: ctx.repo.trackerAnnounces.currentMetainfo(offer.provider, metainfo),
          location: binding.location,
        });
        active();
        repo.bindDownload(acquisition.id, added, binding.location, offer.key);
        return added;
      });
    });
    if ("noSpace" in ref) {
      capacityProblem = ref.noSpace;
      excluded.add(offer.key);
      emit(`${attempt}:disk-space`, "replacing", ref.noSpace);
      continue;
    }
    const downloadId = repo.torrent(ref.clientId, ref.nativeId)!.id;
    if (initial.lossRecoveryKeys?.includes(offer.key))
      await recoverMissingTorrentSource({ ctx, client: services.client, ref, sourceRoot, metadata, prefix, active });
    emit(`${attempt}:download`, "downloading", `Downloading ${offer.title}`);
    let lastBytes = -1;
    let progressedAt = 0;
    let startedAt = 0;
    let completed = false;
    if (ctx.execution?.armCompletion) {
      completed = await waitForTorrentCompletion(ctx, prefix, ref,
        services.timing?.stallMs ?? settings.recovery.stalledTorrentMinutes * 60_000,
        async () => {
          const observed = await services.client.inspect(ref);
          active();
          if (observed.ref.clientId !== ref.clientId || observed.ref.nativeId !== ref.nativeId ||
              observed.location.rootId !== binding.location.rootId || observed.location.relativePath !== binding.location.relativePath)
            throw new Error("Movie torrent observation changed identity or layout");
          repo.observeDownload(downloadId, observed);
          ctx.onAcquisitionChange?.();
          return observed;
        }, downloadComplete, () => { active(); return services.client.resume(ref); });
    } else for (let poll = 0; poll < 20_000; poll++) {
      active();
      const snapshot = await durableStep(
        ctx,
        `${prefix}:poll:${poll}`,
        async () => {
          const observation = await services.client.inspect(ref);
          active();
          repo.observeDownload(downloadId, observation);
          return observation;
        },
      );
      const progress = advanceDownloadProgress(snapshot, { lastBytes, progressedAt, startedAt });
      const observedAt = progress.now;
      ({ lastBytes, progressedAt, startedAt } = progress);
      if (downloadComplete(snapshot)) {
        completed = true;
        break;
      }
      if (snapshot.state === "inactive")
        await durableStep(ctx, `${prefix}:resume:${poll}`, () => {
          active();
          return services.client.resume(ref);
        });
      const stallMs =
        services.timing?.stallMs ??
        settings.recovery.stalledTorrentMinutes * 60_000;
      if (
        (stallMs > 0 && observedAt - progressedAt >= stallMs) ||
        observedAt - startedAt > 24 * 60 * 60_000
      )
        break;
      const interval =
        services.timing?.pollMs ?? Math.max(5000, settings.polling.rtorrentMs);
      if (ctx.execution)
        await ctx.execution.sleep(`${prefix}:wait:${poll}`, interval);
      else await Bun.sleep(interval);
    }
    if (!completed) {
      feedDownloadFailed = Boolean(manual?.feedOffers);
      await durableStep(ctx, `${prefix}:stall`, async () => {
        active();
        await services.client.quiesce(ref);
      });
      excluded.add(offer.key);
      emit(
        `${attempt}:replacing`,
        "replacing",
        "Download stopped making progress; retained its data and looking for a different release",
      );
      continue;
    }
    await durableStep(ctx, `${prefix}:seal-download`, () => sealCanonicalDownload(roots.contentRoot, sourceRoot, services.client, ref, active));
    emit(
      `${attempt}:verify`,
      "verifying",
      "Probing completed media and checking the selected movie",
    );
    const files = await durableStep(ctx, `${prefix}:probe`, async () => {
      const clientFiles = await services.client.files(ref);
      const result: MovieFileCandidate[] = [];
      for (const file of clientFiles) {
        const expected = metadata.files[file.index];
        if (
          !expected ||
          path.resolve(file.path) !== path.resolve(sourceRoot, expected.path)
        )
          throw new Error("Client file mapping escaped its layout");
        await resolveDownloadFile(roots.contentRoot, sourceRoot, file.path, expected.size);
        if (
          ![".mkv", ".mp4", ".m4v", ".avi", ".mov", ".ts"].includes(
            path.extname(file.path).toLowerCase(),
          )
        )
          continue;
        const stat = await fs.stat(file.path);
        if (stat.size !== expected.size)
          throw new Error("Completed file size differs from metainfo");
        try {
          result.push({
            index: file.index,
            name: expected.path,
            sizeBytes: expected.size,
            probe: await services.probe(file.path),
          });
        } catch {
          /* A bad sample must not prevent inspecting the main movie. */
        }
      }
      return result;
    });
    const inspected = await durableStep(ctx, `${prefix}:inspect`, async () => {
      const result = await services.decider.inspect({
        movie,
        policy,
        offer,
        files,
        selection,
      });
      active();
      repo.decision(acquisition.id, {
        ...manual,
        selection,
        importDecision: result,
        attempt,
      });
      return result;
    });
    const chosen = files.find((file) => file.index === inspected.fileIndex);
    if (!chosen) {
      feedDownloadFailed = Boolean(manual?.feedOffers);
      excluded.add(offer.key);
      emit(
        `${attempt}:invalid-content`,
        "replacing",
        inspected.reason || "No verified main movie in this release",
      );
      continue;
    }
    const facts = factsFromMovieProbe(chosen.probe, inspected.facts);
    const assessed = evaluateMovieQuality(policy, facts);
    if (
      assessed.eligibility !== "eligible" ||
      !improvesOrRepairs(facts) ||
      ctx.repo.releaseHealth.isOfferBlocked({ key: offer.key, infoHash: metadata.infoHash })
    ) {
      if (assessed.eligibility !== "eligible")
        feedDownloadFailed = Boolean(manual?.feedOffers);
      excluded.add(offer.key);
      emit(
        `${attempt}:invalid-quality`,
        "retained",
        assessed.reasons.join("; ") ||
          "Probed release is not an improvement; kept the current movie",
      );
      continue;
    }
    const fileIdentity = await durableStep(ctx, `${prefix}:file-identity`, () =>
      randomUUID(),
    );
    const content = await durableStep(ctx, `${prefix}:adopt`, async () => {
      active();
      await assertMovieStorage(ctx.repo, roots.contentRoot);
      const file = (await services.client.files(ref)).find(
        (entry) => entry.index === chosen.index,
      );
      if (!file) throw new Error("Selected client file disappeared");
      return adoptContentFile({
        contentRoot: roots.contentRoot,
        sourceRoot,
        sourcePath: file.path,
        fileId: fileIdentity,
        expectedSize: chosen.sizeBytes,
        assertActive: active,
      });
    });
    const versionId = await durableStep(ctx, `${prefix}:version`, () => {
      active();
      return ctx.repo.transaction(() => {
        const versionId = repo.recordVersion(acquisition.id, facts, [
          { ...content, probe: chosen.probe },
        ]);
        if (!repo.version(versionId)?.files.some((file) => file.id === content.id))
          throw new Error("Movie version belongs to another import attempt; source identity was not changed");
        ctx.repo.releaseHealth.bindMovieVersionSource(versionId, downloadId);
        return versionId;
      });
    });
    await durableStep(ctx, `${prefix}:cross-seed-copies`, () => {
      active();
      return queueSelectedCrossSeeds(ctx.repo, { media: "movie", targetId: target.id }, acquisition.id, offer.key);
    });
    emit(
      `${attempt}:publish`,
      "publishing",
      "Publishing to Plex; the previous version stays available until verification",
    );
    if (ctx.execution?.armCompletion) {
      await waitForPlexPublication(ctx, prefix, settings.movies.plexSectionId, async scan => {
        await assertMovieStorage(ctx.repo, roots.contentRoot);
        return publishMovieVersion({ repo, roots, library: services.library, targetId: target.id, versionId,
          assertActive: active, scan, pendingIsExpected: true });
      });
    } else for (let scan = 0; ; scan++) {
      const published = await durableStep(
        ctx,
        `${prefix}:publish:${scan}`,
        async () => {
          try {
            await assertMovieStorage(ctx.repo, roots.contentRoot);
            await publishMovieVersion({
              repo,
              roots,
              library: services.library,
              targetId: target.id,
              versionId,
              assertActive: active,
            });
            return true;
          } catch (error) {
            active();
            if (
              repo
                .publications()
                .some(
                  (entry) =>
                    entry.target_id === target.id &&
                    entry.version_id === versionId,
                )
            )
              return false;
            throw error;
          }
        },
      );
      if (published) break;
      // A busy library can take minutes to discover a new part. These are
      // durable sleeps, not a reason to download a different release.
      if (scan + 1 >= (services.timing?.plexAttempts ?? 120))
        throw new Error(
          "Plex did not verify the published movie; previous version retained",
        );
      const delay = services.timing?.plexRetryMs ?? 5000;
      if (ctx.execution)
        await ctx.execution.sleep(`${prefix}:plex-wait:${scan}`, delay);
      else await Bun.sleep(delay);
    }
    await durableStep(ctx, "movie:finish", () => {
      active();
      repo.finishAcquisition(acquisition.id);
    });
    ctx.onAcquisitionChange?.();
    return "done";
  }
  emit(
    "blocked",
    current ? "retained" : "blocked",
    capacityProblem ?? "Acquisition attempts exhausted; inspect releases or retry to search again",
  );
  await durableStep(ctx, "movie:finish-exhausted", () =>
    repo.finishAcquisition(acquisition.id),
  );
  return "done";
}
