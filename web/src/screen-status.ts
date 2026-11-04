import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import type { MovieRequestView } from "../../src/movies/contracts";

type RequestBadge = { label: string; tone: "available" | "active" | "waiting" | "paused" };

export type MovieStatusInput = Pick<MovieRequestView, "available" | "upgrading" | "state" | "stage">;
type TvRequest = inferRouterOutputs<AppRouter>["tv"]["myRequests"]["requests"][number];
export type TvStatusInput = Pick<TvRequest, "intentionallyAbsent" | "requestedEpisodes" | "availableEpisodes" | "futureEpisodes"> & {
  acquisitions: ReadonlyArray<Pick<TvRequest["acquisitions"][number], "status">>;
};

export function movieBadge(request: MovieStatusInput): RequestBadge {
  if (request.available) return { label: request.upgrading ? "Available · Upgrading" : "Available", tone: "available" };
  if (request.state === "blocked" || request.state === "withdrawn") return { label: "Request stopped", tone: "paused" };
  if (request.state === "waiting-for-release") return { label: "Waiting for release", tone: "waiting" };
  if (request.state === "running") return { label: request.stage === "downloading" ? "Downloading" : "In progress", tone: "active" };
  if (request.state === "queued") return { label: "Requested", tone: "active" };
  return { label: "Request status unknown", tone: "paused" };
}

export function movieDetail(request: MovieStatusInput): string {
  if (request.available) return "Ready in Plex";
  if (request.state === "blocked") return "Open request to resolve";
  if (request.state === "running" && request.stage === "downloading") return "Acquiring your movie";
  return "Track your request";
}

function tvStopped(request: TvStatusInput): boolean {
  // The request API orders acquisitions newest-first; older failures must not mask a retry.
  const latest = request.acquisitions[0]?.status;
  return request.availableEpisodes === 0 && !request.intentionallyAbsent && (latest === "failed" || latest === "cancelled");
}

export function tvBadge(request: TvStatusInput): RequestBadge {
  if (request.intentionallyAbsent) return { label: "Intentionally unavailable", tone: "paused" };
  if (request.requestedEpisodes > 0 && request.availableEpisodes >= request.requestedEpisodes) return { label: "Available", tone: "available" };
  if (request.availableEpisodes > 0) return { label: "Partly available", tone: "available" };
  if (tvStopped(request)) return { label: "Request stopped", tone: "paused" };
  const latest = request.acquisitions[0]?.status;
  if (latest === "queued" || latest === "running") return { label: "In progress", tone: "active" };
  return { label: request.futureEpisodes ? "Upcoming episodes" : "Waiting for release", tone: "waiting" };
}

export function tvDetail(request: TvStatusInput): string {
  if (tvStopped(request)) return `${request.availableEpisodes}/${request.requestedEpisodes} episodes ready · Open request to resolve`;
  return `${request.availableEpisodes}/${request.requestedEpisodes} episodes ready${request.futureEpisodes ? ` · ${request.futureEpisodes} upcoming` : ""}`;
}
