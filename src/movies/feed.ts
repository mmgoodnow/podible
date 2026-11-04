import { createHash } from "node:crypto";
import { z } from "zod";
import type { JobRow } from "../app-types";
import { durableStep, type WorkerContext } from "../worker/context";
import { createMovieFeedMatcher } from "./agents";
import { movieOfferFromResult } from "./offers";
import { queueMovieAcquisition } from "./service";
import type { SearchResult } from "../media/indexer-gateway";

const payloadSchema = z
  .object({ releaseIds: z.array(z.number().int().positive()).min(1).max(100) })
  .strict();
function revision(policy: string, version: number) {
  return createHash("sha256").update(policy).digest("hex") + ":" + version;
}

/** RSS is candidate intake, not authority to replace a movie or bypass the existing agent/policy. */
export async function matchMovieFeed(
  ctx: WorkerContext,
  job: JobRow,
): Promise<"done"> {
  const { releaseIds } = payloadSchema.parse(
    JSON.parse(job.payload_json ?? "{}"),
  );
  const entries = ctx.repo.releaseInbox.entries(releaseIds);
  if (entries.length !== new Set(releaseIds).size)
    throw new Error("RSS inbox entries are missing");
  if (entries.some((entry) => entry.result.mediaType !== "movie"))
    throw new Error("Not a movie feed batch");
  const offers = entries.map((entry) => ({
    ...movieOfferFromResult(entry.result as SearchResult<"movie">),
    inboxId: entry.id,
  }));
  const targets = await durableStep(ctx, "rss:targets", () =>
    ctx.repo.movies
      .targets()
      .filter(
        (target) =>
          ctx.repo.movies.activeRequestCount(target.id) > 0 &&
          (!target.active_version_id ||
            ctx.getSettings().movies.automaticUpgrades),
      )
      .map((target) => ({
        targetId: target.id,
        movie: ctx.repo.movies.movie(target.movie_id)!,
      })),
  );
  const matcher =
    ctx.movieFeedMatcher ??
    createMovieFeedMatcher(ctx.getSettings(), ctx.modelProvider);
  const matches: {
    targetId: number;
    releaseIndexes: number[];
    reason: string;
  }[] = [];
  for (let start = 0; start < targets.length; start += 100) {
    const batch = targets.slice(start, start + 100);
    matches.push(
      ...(await durableStep(ctx, "rss:match:" + start, () =>
        matcher({ targets: batch, offers }),
      )),
    );
  }
  const allowed = new Set(targets.map((target) => target.targetId));
  for (const match of matches)
    if (
      !allowed.has(match.targetId) ||
      !match.releaseIndexes.length ||
      match.releaseIndexes.some(
        (index) => !Number.isInteger(index) || !entries[index],
      )
    )
      throw new Error("RSS matcher returned an unknown target or release");
  let pending = [...new Set(matches.map((match) => match.targetId))];
  for (let wave = 0; pending.length; wave++) {
    const busy: number[] = [];
    for (const targetId of pending) {
      ctx.execution?.assertActive();
      const target = ctx.repo.movies.target(targetId);
      if (
        !target ||
        !ctx.repo.movies.activeRequestCount(targetId) ||
        (target.active_version_id &&
          !ctx.getSettings().movies.automaticUpgrades)
      )
        continue;
      const unfinished = ctx.repo.movies
        .acquisitions(targetId)
        .some(
          (a) =>
            !a.finished_at &&
            a.job_id &&
            ["queued", "running"].includes(
              ctx.repo.getJob(a.job_id)?.status ?? "",
            ),
        );
      if (unfinished) {
        busy.push(targetId);
        continue;
      }
      const targetRevision = revision(target.policy_json, target.revision);
      const matching = matches.filter((match) => match.targetId === targetId);
      const indexes = [
        ...new Set(matching.flatMap((match) => match.releaseIndexes)),
      ];
      const considered = (index: number) =>
        ctx.repo.releaseInbox.considered(
          entries[index].id,
          "movie",
          targetId,
          targetRevision,
        ) ||
        ctx.repo.releaseInbox.consideredForDispatch(
          entries[index].id,
          "movie",
          targetId,
          job.id,
        );
      const unseen = indexes.filter((index) => !considered(index));
      if (!unseen.length) continue;
      await durableStep(ctx, "rss:dispatch:" + targetId, () =>
        ctx.repo.transaction(() => {
          ctx.execution?.assertActive();
          const current = ctx.repo.movies.target(targetId);
          if (
            !current ||
            revision(current.policy_json, current.revision) !== targetRevision
          )
            throw new Error(
              "Movie target changed while dispatching RSS candidates",
            );
          const stillUnseen = unseen.filter((index) => !considered(index));
          if (!stillUnseen.length) return;
          const queued = queueMovieAcquisition(
            ctx.repo,
            targetId,
            current.active_version_id ? "upgrade" : "recovery",
            undefined,
            stillUnseen.map((index) => offers[index]),
          );
          if (queued.shared)
            throw new Error(
              "Movie acquisition became active before RSS dispatch",
            );
          const note = matching
            .map((match) => match.reason)
            .join("\n")
            .slice(0, 4000);
          for (const index of stillUnseen)
            ctx.repo.releaseInbox.consider(
              entries[index].id,
              "movie",
              targetId,
              targetRevision,
              queued.acquisition.job_id!,
              note,
              job.id,
            );
        }),
      );
    }
    if (!busy.length) break;
    if (!ctx.execution)
      throw new Error(
        "Waiting for an active movie acquisition requires durable execution",
      );
    await ctx.execution.sleep("rss:wait-for-targets:" + wave, 60_000);
    pending = busy;
  }
  return "done";
}
