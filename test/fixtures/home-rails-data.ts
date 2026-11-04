import { randomUUID } from "node:crypto";
import type { Database } from "bun:sqlite";
import type { BooksRepo } from "../../src/repo";
import { fixtureMovie, fixtureFacts } from "./movie-services";
import { fixtureTvEntry } from "./tv-catalog";

export function publishRailMovie(repo: BooksRepo, db: Database, userId: number, tmdbId: number, title: string, date: string) {
  const request = repo.movies.request(userId, { ...fixtureMovie, tmdbId, title, posterUrl: `https://image.tmdb.org/t/p/w342/${tmdbId}.jpg` }, repo.getSettings().movies.qualityPolicy);
  const acquisition = repo.movies.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "metadata_hydration" }).id).acquisition;
  const fileId = randomUUID();
  const version = repo.movies.recordVersion(acquisition.id, fixtureFacts(), [{ id: fileId, relativePath: `files/${fileId.slice(0, 2)}/${fileId}.mkv`, sizeBytes: 10, probe: {} }]);
  repo.movies.selectVersion(request.targetId, version, 0); repo.movies.finishAcquisition(acquisition.id);
  const key = `published:${request.targetId}:${version}`;
  repo.movies.event(request.targetId, null, key, "available", "Verified in Plex and ready to watch");
  db.query("UPDATE movie_events SET created_at=? WHERE event_key=?").run(date, key);
  return { request, version, fileId };
}

export function publishRailTv(repo: BooksRepo, db: Database, userId: number, externalId: number, title: string, date: string, airDate: string | null) {
  const entry = fixtureTvEntry();
  entry.episodes = entry.episodes.map(ep => ({ ...ep, key: ep.key.replace("700", String(externalId)), airDate }));
  const request = repo.tv.requestSeasons(userId, { ...entry, key: `tvdb:${externalId}`, tmdbId: externalId, tvdbId: externalId, title,
    posterUrl: `https://image.tmdb.org/t/p/w342/${externalId}.jpg` }, repo.getSettings().movies.qualityPolicy, [1]);
  const acquisition = repo.tv.startAcquisition(request.targetId, "initial", () => repo.createJob({ type: "metadata_hydration" }).id, request.requestId).acquisition;
  const fileId = randomUUID();
  const version = repo.tv.recordVersion(acquisition.id, { identityVerified: true }, [{ id: fileId,
    relativePath: `files/${fileId.slice(0, 2)}/${fileId}.mkv`, sizeBytes: 10, probe: {}, episodeIds: [request.episodeIds[0]!] }]);
  repo.tv.stagePublication(request.targetId, version, 0); repo.tv.promotePublication(request.targetId, version, 0);
  repo.tv.clearPublication(request.targetId, version); repo.tv.finishAcquisition(acquisition.id);
  const key = `tv-published:${request.targetId}:${version}`;
  repo.tv.event(request.targetId, null, key, "available", "Episode identity and file paths verified in Plex");
  db.query("UPDATE tv_events SET created_at=? WHERE event_key=?").run(date, key);
  return { request, version, fileId };
}
