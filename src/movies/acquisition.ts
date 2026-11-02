import path from "node:path";
import { promises as fs } from "node:fs";
import { randomUUID, createHash } from "node:crypto";
import { z } from "zod";
import type { JobRow } from "../app-types";
import { durableStep, type WorkerContext } from "../worker/context";
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
import type { MovieOffer } from "./offers";
import type { MovieServices, MovieFileCandidate } from "./operations";
import { movieUpstreamStep } from "./upstream";

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
        settings.rtorrent.url,
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
  if (!settings.movies.enabled) throw new Error("Movies are disabled");
  const roots = movieRoots(
    settings.movies.managedRoot,
    settings.movies.plexViewPath,
  );
  const configuration = await durableStep(ctx, "movie:configuration", () =>
    movieConfigurationIdentity(ctx),
  );
  const active = () => {
    ctx.execution?.assertActive();
    if (
      !ctx.getSettings().movies.enabled ||
      movieConfigurationIdentity(ctx) !== configuration
    )
      throw new Error(
        "Movie storage/client configuration changed; start a new acquisition",
      );
    if (!repo.activeRequestCount(target.id))
      throw new Error("All requests for this target were withdrawn");
  };
  active();
  const initial = await durableStep(ctx, "movie:target-snapshot", () => ({
    policy: JSON.parse(target.policy_json),
    current: target.active_version_id
      ? repo.version(target.active_version_id)!.facts
      : null,
    priorKeys: repo.downloads(target.id).map((download) => download.offer_key),
  }));
  const policy = movieQualityPolicySchema.parse(initial.policy);
  const current = initial.current;
  const manual = acquisition.decision_json
    ? (JSON.parse(acquisition.decision_json) as { manualOffer?: MovieOffer })
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
  };
  const priorKeys = new Set(initial.priorKeys);
  const excluded = new Set<string>();
  for (let attempt = 0; attempt < 3; attempt++) {
    active();
    const prefix = `movie:attempt:${attempt}`;
    emit(
      `${attempt}:search`,
      "searching",
      attempt
        ? "Searching for another release; previous downloads are retained"
        : "Searching movie releases",
    );
    const search = await movieUpstreamStep(ctx, `${prefix}:search`, async () =>
      attempt === 0 && manual?.manualOffer
        ? { offers: [manual.manualOffer], warnings: [] }
        : services.search(movie),
    );
    const candidates = search.offers
      .filter(
        (offer) =>
          !excluded.has(offer.key) &&
          (!priorKeys.has(offer.key) || manual?.manualOffer?.key === offer.key),
      )
      .filter(
        (offer) =>
          offer.sizeBytes === null ||
          offer.sizeBytes <= settings.movies.maximumReleaseBytes,
      );
    if (
      attempt === 0 &&
      manual?.manualOffer &&
      !candidates.some((offer) => offer.key === manual.manualOffer!.key)
    )
      candidates.push(manual.manualOffer);
    emit(
      `${attempt}:select`,
      "selecting",
      "Checking movie identity, cut and quality",
    );
    const selection = await durableStep(ctx, `${prefix}:select`, async () => {
      const selected = await services.decider.select({
        movie,
        policy,
        offers: candidates,
        current,
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
      repo.decision(acquisition.id, {
        ...manual,
        selection: selected,
        attempt,
      });
      return selected;
    });
    const offer = candidates.find((entry) => entry.key === selection.offerKey);
    if (!offer) {
      emit(
        "blocked",
        current ? "retained" : "blocked",
        selection.reason ||
          "No suitable release found; try manual release search",
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
        !assessMovieUpgrade(policy, current, selection.facts).upgrade)
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
    const encoded = await movieUpstreamStep(ctx, `${prefix}:metainfo`, async () =>
      Buffer.from(await services.fetchMetainfo(offer)).toString("base64"),
    );
    const metainfo = Buffer.from(encoded, "base64");
    const metadata = inspectMetainfo(metainfo);
    if (metadata.size > settings.movies.maximumReleaseBytes)
      throw new Error("Torrent exceeds the configured acquisition byte limit");
    const binding = await durableStep(ctx, `${prefix}:download-intent`, () => {
      const prior = repo.torrent(services.client.id, metadata.infoHash);
      const location: DownloadLocation = prior
        ? JSON.parse(prior.location_json)
        : {
            rootId: "movies",
            relativePath: `acquisition-${acquisition.id}-${attempt}`,
          };
      return { location };
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
      const added = await services.client.ensureAdded({
        metainfo,
        location: binding.location,
      });
      active();
      repo.bindDownload(acquisition.id, added, binding.location, offer.key);
      return added;
    });
    const downloadId = repo.torrent(ref.clientId, ref.nativeId)!.id;
    emit(`${attempt}:download`, "downloading", `Downloading ${offer.title}`);
    let lastBytes = -1;
    let progressedAt = 0;
    let startedAt = 0;
    let completed = false;
    for (let poll = 0; poll < 20_000; poll++) {
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
      const observedAt = Date.parse(snapshot.observedAt);
      if (!Number.isFinite(observedAt))
        throw new Error("Invalid client observation time");
      if (!startedAt) startedAt = observedAt;
      if (snapshot.complete && snapshot.state !== "checking") {
        completed = true;
        break;
      }
      if (
        snapshot.bytesDone === null ||
        snapshot.bytesDone !== lastBytes ||
        snapshot.state === "checking"
      ) {
        lastBytes = snapshot.bytesDone ?? -1;
        progressedAt = observedAt;
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
          !path.resolve(file.path).startsWith(`${sourceRoot}${path.sep}`) ||
          (await fs.realpath(file.path)) !== path.resolve(file.path)
        )
          throw new Error("Client file mapping escaped its layout");
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
      (current && !assessMovieUpgrade(policy, current, facts).upgrade)
    ) {
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
      return repo.recordVersion(acquisition.id, facts, [
        { ...content, probe: chosen.probe },
      ]);
    });
    emit(
      `${attempt}:publish`,
      "publishing",
      "Publishing to Plex; the previous version stays available until verification",
    );
    for (let scan = 0; ; scan++) {
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
    return "done";
  }
  emit(
    "blocked",
    current ? "retained" : "blocked",
    "Acquisition attempts exhausted; inspect releases or retry to search again",
  );
  await durableStep(ctx, "movie:finish-exhausted", () =>
    repo.finishAcquisition(acquisition.id),
  );
  return "done";
}
