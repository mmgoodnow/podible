import { durableStep, type WorkerContext } from "../worker/context";
import { prepareMovieStorage, assertMovieStorage } from "../movies/storage";
import { tvRoots, reconcileTvView, publishTvVersion } from "./publication";
import { configuredTvServices } from "./runtime";

/** Repair declarative views without searching titles or evicting seed data. */
export async function reconcileTv(ctx: WorkerContext): Promise<"done"> {
  if (!ctx.repo.tv.hasTargets()) return "done";
  const settings = ctx.getSettings(), repo = ctx.repo.tv;
  const roots = tvRoots(settings.tv.managedRoot, settings.tv.plexViewPath);
  const services = ctx.tvServices ?? configuredTvServices(settings, ctx.modelProvider);
  const active = () => ctx.execution?.assertActive();
  await durableStep(ctx, "tv-maintenance:storage", () => prepareMovieStorage(ctx.repo, roots.contentRoot));
  await assertMovieStorage(ctx.repo, roots.contentRoot);
  for (const publication of repo.publications()) {
    const owned = repo.acquisitions(publication.target_id).some((acquisition) => !acquisition.finished_at && acquisition.job_id &&
      ["queued", "running"].includes(ctx.repo.getJob(acquisition.job_id)?.status ?? ""));
    if (owned) continue;
    await durableStep(ctx, `tv-maintenance:publication:${publication.target_id}`, async () => {
      active();
      const selected = repo.activeEpisodes(publication.target_id).some((episode) => episode.version_id === publication.version_id);
      if (selected) await publishTvVersion({ repo, roots, library: services.library, targetId: publication.target_id, versionId: publication.version_id, assertActive: active });
      else repo.abortPublication(publication.target_id, publication.version_id);
    });
  }
  await durableStep(ctx, "tv-maintenance:views", async () => {
    active(); await assertMovieStorage(ctx.repo, roots.contentRoot);
    await reconcileTvView(repo, roots, active); await services.library.scan();
  });
  return "done";
}
