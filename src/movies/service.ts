import type { BooksRepo } from "../repo";
import type { MovieAcquisition } from "./repo";
import type { MovieOffer } from "./offers";
import { evaluateMovieQuality, movieQualityPolicySchema } from "./quality";
import type { TorrentSnapshot } from "../media/torrent-client";

export function queueMovieAcquisition(
  repo: BooksRepo,
  targetId: number,
  reason: MovieAcquisition["reason"],
  manualOffer?: MovieOffer,
) {
  return repo.transaction(() => {
    const queued = repo.movies.startAcquisition(
      targetId,
      reason,
      (id) =>
        repo.createJob({
          type: "movie_acquire",
          payload: { acquisitionId: id },
          maxAttempts: 3,
          idempotencyKey: `movie-acquisition:${id}`,
        }).id,
    );
    if (!queued.shared) {
      if (manualOffer)
        repo.movies.decision(queued.acquisition.id, { manualOffer });
      for (const publication of repo.movies
        .publications()
        .filter((entry) => entry.target_id === targetId)) {
        if (
          repo.movies.target(targetId)?.active_version_id !==
          publication.version_id
        )
          repo.movies.abortPublication(targetId, publication.version_id);
      }
    }
    return queued;
  });
}
export function movieRequestView(repo: BooksRepo, requestId: number) {
  const request = repo.movies.requestById(requestId);
  if (!request) throw new Error("Unknown movie request");
  const target = repo.movies.target(request.target_id)!;
  const movie = repo.movies.movie(target.movie_id)!;
  const version = target.active_version_id
    ? repo.movies.version(target.active_version_id)
    : null;
  const quality = version
    ? evaluateMovieQuality(
        movieQualityPolicySchema.parse(JSON.parse(target.policy_json)),
        version.facts,
      )
    : null;
  const available = quality?.eligibility === "eligible";
  const acquisition = repo.movies.acquisitions(target.id)[0];
  const job = acquisition?.job_id ? repo.getJob(acquisition.job_id) : null;
  const events = repo.movies.events(target.id);
  const latest = events.at(-1);
  const state = request.withdrawn_at
    ? ("withdrawn" as const)
    : available
      ? ("available" as const)
      : job?.status === "failed" ||
          job?.status === "cancelled" ||
          acquisition?.finished_at ||
          latest?.stage === "blocked"
        ? ("blocked" as const)
        : job?.status === "running"
          ? ("running" as const)
          : ("queued" as const);
  const observation = repo.movies.downloads(target.id)[0]?.observation_json;
  const snapshot = observation
    ? (JSON.parse(observation) as TorrentSnapshot)
    : null;
  const decision = acquisition?.decision_json
    ? (JSON.parse(acquisition.decision_json) as {
        selection?: { reason?: string };
        importDecision?: { reason?: string };
      })
    : null;
  return {
    id: request.id,
    targetId: target.id,
    movie,
    createdAt: request.created_at,
    state,
    available,
    upgrading:
      available &&
      Boolean(
        job &&
        ["running", "queued"].includes(job.status) &&
        !acquisition?.finished_at,
      ),
    stage:
      state === "withdrawn"
        ? null
        : job?.status === "failed" || job?.status === "cancelled"
          ? ("blocked" as const)
          : (latest?.stage ?? ("requested" as const)),
    job: job
      ? {
          id: job.id,
          status: job.status,
          error:
            job.status === "failed"
              ? "Acquisition failed. Retry or select a release manually; administrators can inspect the operation log."
              : null,
        }
      : null,
    quality,
    events,
    decisionNote:
      decision?.importDecision?.reason ?? decision?.selection?.reason ?? null,
    download: snapshot
      ? {
          bytesDone: snapshot.bytesDone,
          sizeBytes: snapshot.sizeBytes,
          bytesPerSecond: snapshot.downloadBytesPerSecond,
        }
      : null,
  };
}
