import type { SeerrRequest, SeerrSource } from "../../src/movies/seerr";
import { fixtureMovie } from "./movie-services";

export function seerrRequest(
  id: number,
  status = 2,
  overrides: Partial<SeerrRequest> = {},
): SeerrRequest {
  return {
    id,
    status,
    type: "movie",
    is4k: false,
    createdAt: new Date(Date.UTC(2026, 10, 2, 12) + id * 1000).toISOString(),
    updatedAt: new Date(Date.UTC(2026, 10, 2, 12) + id * 1000).toISOString(),
    media: { tmdbId: fixtureMovie.tmdbId },
    requestedBy: { id: 42, plexId: 120148426, displayName: "Katya" },
    ...overrides,
  };
}

export function createFixtureSeerrSource(initial: SeerrRequest[] = []) {
  const records = new Map(initial.map((request) => [request.id, request]));
  const calls = {
    latest: 0,
    changes: [] as string[],
    requests: [] as number[],
    movies: [] as number[],
  };
  const source: SeerrSource = {
    latest: async () => {
      calls.latest++;
      return [...records.values()].sort((a, b) => b.id - a.id)[0] ?? null;
    },
    changes: async (since) => {
      calls.changes.push(since);
      const requests = [...records.values()].filter(
        (r) => Date.parse(r.updatedAt) >= Date.parse(since) - 120_000,
      );
      const through = new Date(
        Math.max(
          Date.parse(since),
          ...requests.map((r) => Date.parse(r.updatedAt)),
        ),
      ).toISOString();
      return { requests, through };
    },
    request: async (id) => {
      calls.requests.push(id);
      const request = records.get(id);
      if (!request) throw new Error("Unknown fixture Seerr request");
      return request;
    },
    movie: async (id) => {
      calls.movies.push(id);
      return { ...fixtureMovie, tmdbId: id };
    },
  };
  return { source, records, calls };
}
