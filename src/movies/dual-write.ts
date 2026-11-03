import { z } from "zod";
import type { JobRow } from "../app-types";
import type { WorkerContext } from "../worker/context";
import { durableStep } from "../worker/context";
import {
  SeerrClient,
  seerrInstance,
  approvedSeerrMovie,
  type SeerrSource,
  type SeerrRequest,
} from "./seerr";
import { queueMovieAcquisition } from "./service";

export const dualWritePayloadSchema = z
  .object({
    backfillRequestIds: z
      .array(z.number().int().positive())
      .min(1)
      .max(5)
      .optional(),
  })
  .strict();
export async function syncSeerrRequests(
  ctx: WorkerContext,
  job: JobRow,
  source: SeerrSource = ctx.seerrSource ??
    new SeerrClient(ctx.getSettings().movies.seerr),
) {
  const config = ctx.getSettings().movies.seerr;
  if (!config.url) throw new Error("Configure the Seerr request feed URL");
  const instance = seerrInstance(config.url);
  const input = dualWritePayloadSchema.parse(
    JSON.parse(job.payload_json ?? "{}"),
  );
  const snapshot = await durableStep(ctx, "seerr:configuration", () => ({
    instance,
    policy: ctx.getSettings().movies.qualityPolicy,
  }));
  const active = () => {
    ctx.execution?.assertActive();
    if (snapshot.instance !== seerrInstance(ctx.getSettings().movies.seerr.url))
      throw new Error("Seerr connection changed; start a new sync");
  };
  active();
  const feed = await durableStep(ctx, "seerr:feed", async () => {
    const existing = ctx.repo.movieSeerr.feed(instance);
    if (existing) return existing;
    const latest = await source.latest();
    active();
    // Persist before the step receipt: a crash cannot move the activation boundary.
    return ctx.repo.movieSeerr.start(
      instance,
      latest?.id ?? 0,
      latest
        ? new Date(latest.createdAt).toISOString()
        : new Date().toISOString(),
    );
  });
  const importRequest = async (request: SeerrRequest) => {
    active();
    if (!approvedSeerrMovie(request)) return;
    if (ctx.repo.movieSeerr.origin(instance, request.id)) return;
    const movie = await durableStep(ctx, `seerr:metadata:${request.id}`, () =>
      source.movie(request.media.tmdbId),
    );
    if (movie.tmdbId !== request.media.tmdbId)
      throw new Error("Seerr returned a different movie identity");
    await durableStep(ctx, `seerr:import:${request.id}`, () =>
      ctx.repo.transaction(() => {
        active();
        if (ctx.repo.movieSeerr.origin(instance, request.id)) return;
        const actor = request.requestedBy;
        const plex = actor.plexId
          ? ctx.repo
              .listUsers("plex")
              .find((u) => u.provider_user_id === String(actor.plexId))
          : undefined;
        // External requesters are not provisioned as Plex logins or given sessions.
        const principalId = `seerr:${instance}:${actor.id}`;
        const user =
          plex ??
          ctx.repo
            .listUsers("local")
            .find((u) => u.provider_user_id === principalId) ??
          ctx.repo.upsertUser({
            provider: "local",
            providerUserId: principalId,
            username: `Seerr: ${actor.displayName ?? actor.username ?? actor.id}`,
            isAdmin: false,
          });
        const basePolicy = snapshot.policy;
        const policy = request.is4k
          ? {
              ...basePolicy,
              tiers: basePolicy.tiers.some((t) => t.resolution === 2160)
                ? basePolicy.tiers.filter((t) => t.resolution === 2160)
                : basePolicy.tiers
                    .map((t) => ({ ...t, resolution: 2160 as const }))
                    .filter(
                      (t, i, a) =>
                        a.findIndex((x) => x.source === t.source) === i,
                    ),
            }
          : basePolicy;
        const native = ctx.repo.movies.request(user.id, movie, policy);
        ctx.repo.movieSeerr.attach(instance, request, native.requestId);
        queueMovieAcquisition(ctx.repo, native.targetId, "initial");
      }),
    );
  };
  if (input.backfillRequestIds) {
    const requests: SeerrRequest[] = [];
    // Validate the complete explicit batch before starting any acquisition.
    for (const id of [...new Set(input.backfillRequestIds)]) {
      const request = await source.request(id);
      if (request.id !== id || !approvedSeerrMovie(request))
        throw new Error(`Seerr request ${id} is not an approved movie`);
      requests.push(request);
    }
    for (const request of requests) await importRequest(request);
  } else {
    const changes = await source.changes(feed.cursor);
    for (const request of changes.requests)
      if (request.id > feed.baselineId) await importRequest(request);
    active();
    ctx.repo.movieSeerr.advance(instance, changes.through);
  }
  return "done" as const;
}
