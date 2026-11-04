import { durableStep, type WorkerContext } from "../worker/context";
import { movieRoots } from "./acquisition";
import { configuredMovieServices } from "./runtime";
import { prepareMovieStorage, assertMovieStorage } from "./storage";
import { publishMovieVersion, reconcileMovieView } from "./publication";

export async function reconcileMovies(ctx: WorkerContext): Promise<"done"> {
  const settings = ctx.getSettings();
  if (!ctx.repo.movies.hasTargets()) return "done";
  const roots = movieRoots(
    settings.movies.managedRoot,
    settings.movies.plexViewPath,
  );
  const active = () => ctx.execution?.assertActive();
  await durableStep(ctx, "movies:storage", () =>
    prepareMovieStorage(ctx.repo, roots.contentRoot),
  );
  await assertMovieStorage(ctx.repo, roots.contentRoot);
  const services =
    ctx.movieServices ?? configuredMovieServices(settings, ctx.modelProvider);
  for (const target of ctx.repo.movies.targets()) {
    if (ctx.repo.movies.activeRequestCount(target.id)) continue;
    for (const torrent of ctx.repo.movies.downloads(target.id)) {
      if (
        torrent.client_id !== services.client.id ||
        ctx.repo.movies.activeTorrentDemand(torrent.id)
      )
        continue;
      await durableStep(
        ctx,
        `movies:pause-unrequested:${torrent.id}`,
        async () => {
          active();
          if (!ctx.repo.movies.activeTorrentDemand(torrent.id)) {
            const ref = {
              clientId: torrent.client_id,
              nativeId: torrent.native_id,
            };
            // Withdrawal is not permission to stop seeding completed data.
            if (!(await services.client.inspect(ref)).complete)
              await services.client.quiesce(ref);
          }
        },
      );
    }
  }
  const pending = await durableStep(ctx, "movies:pending", () =>
    ctx.repo.movies.publications(),
  );
  for (const publication of pending) {
    const target = ctx.repo.movies.target(publication.target_id)!;
    const owners = ctx.repo.movies.acquisitions(target.id);
    const activeOwner = owners.some(
      (entry) =>
        !entry.finished_at &&
        entry.job_id &&
        ["queued", "running"].includes(
          ctx.repo.getJob(entry.job_id)?.status ?? "",
        ),
    );
    if (activeOwner && target.active_version_id !== publication.version_id)
      continue;
    await durableStep(
      ctx,
      `movies:recover-publication:${target.id}`,
      async () => {
        active();
        if (
          ctx.repo.movies.target(target.id)?.active_version_id ===
          publication.version_id
        ) {
          await publishMovieVersion({
            repo: ctx.repo.movies,
            roots,
            library: services.library,
            targetId: target.id,
            versionId: publication.version_id,
            assertActive: active,
          });
        } else
          ctx.repo.movies.abortPublication(target.id, publication.version_id);
      },
    );
  }
  await durableStep(ctx, "movies:views", async () => {
    active();
    await assertMovieStorage(ctx.repo, roots.contentRoot);
    await reconcileMovieView(ctx.repo.movies, roots, active);
    await services.library.scan();
  });
  // Source discovery is event-driven via RSS; reconciliation only repairs existing views.
  return "done";
}
