import type { Database } from "bun:sqlite";
import { createHash, randomUUID } from "node:crypto";
import type { MovieOffer } from "./offers";
import type {
  DownloadLocation,
  TorrentRef,
  TorrentSnapshot,
} from "../media/torrent-client";
import type { JobRow } from "../app-types";
import {
  catalogMovieSchema,
  type CatalogMovie,
  type Movie,
  type MovieStage,
} from "./types";
import {
  movieFactsSchema,
  movieQualityPolicySchema,
  type MovieFacts,
  type MovieQualityPolicy,
} from "./quality";
import { assessMovieUpgrade, evaluateMovieQuality } from "./quality";
import { RetentionRepo } from "../media/retention-repo";
import { assertVersionContentAvailable } from "../media/content-loss";

export type MovieTarget = {
  id: number;
  movie_id: number;
  policy_json: string;
  active_version_id: number | null;
  revision: number;
};
export type MovieAcquisition = {
  id: number;
  target_id: number;
  reason: "initial" | "upgrade" | "recovery";
  job_id: number | null;
  decision_json: string | null;
  finished_at: string | null;
  created_at: string;
};
export class MovieRepo {
  constructor(
    private db: Database,
    private beforeWrite: () => void,
    private getJob: (id: number) => JobRow | null,
  ) {}
  transaction<T>(fn: () => T): T {
    return this.db.transaction(() => {
      this.beforeWrite();
      return fn();
    })();
  }
  upsertMovie(input: CatalogMovie): Movie {
    const metadata = catalogMovieSchema.parse(input);
    return this.transaction(() => {
      const row = this.db
        .query(
          `INSERT INTO movies(tmdb_id,metadata_json,updated_at) VALUES(?,?,?)
        ON CONFLICT(tmdb_id) DO UPDATE SET metadata_json=excluded.metadata_json,updated_at=excluded.updated_at RETURNING id`,
        )
        .get(
          metadata.tmdbId,
          JSON.stringify(metadata),
          new Date().toISOString(),
        ) as { id: number };
      return { id: row.id, ...metadata };
    });
  }
  movie(id: number): Movie | null {
    const row = this.db
      .query("SELECT id,metadata_json FROM movies WHERE id=?")
      .get(id) as { id: number; metadata_json: string } | null;
    return row
      ? {
          id: row.id,
          ...catalogMovieSchema.parse(JSON.parse(row.metadata_json)),
        }
      : null;
  }
  availableByTmdb(tmdbId: number): boolean {
    return !!this.db.query(`SELECT 1 FROM movies m JOIN movie_targets t ON t.movie_id=m.id
      WHERE m.tmdb_id=? AND t.active_version_id IS NOT NULL LIMIT 1`).get(tmdbId);
  }
  availableCatalog(query: string, limit: number): CatalogMovie[] {
    return (this.db.query(`SELECT m.metadata_json FROM movies m
      WHERE EXISTS(SELECT 1 FROM movie_targets t WHERE t.movie_id=m.id AND t.active_version_id IS NOT NULL)
      AND (instr(lower(json_extract(m.metadata_json,'$.title')),lower(?)) > 0
        OR instr(lower(json_extract(m.metadata_json,'$.originalTitle')),lower(?)) > 0)
      ORDER BY m.id DESC LIMIT ?`).all(query, query, limit) as { metadata_json: string }[])
      .map(row => catalogMovieSchema.parse(JSON.parse(row.metadata_json)));
  }
  movieByTmdb(tmdbId: number): Movie | null {
    const row = this.db
      .query("SELECT id FROM movies WHERE tmdb_id=?")
      .get(tmdbId) as { id: number } | null;
    return row ? this.movie(row.id) : null;
  }
  target(id: number): MovieTarget | null {
    return this.db
      .query("SELECT * FROM movie_targets WHERE id=?")
      .get(id) as MovieTarget | null;
  }
  targets(): MovieTarget[] {
    return this.db
      .query("SELECT * FROM movie_targets ORDER BY id")
      .all() as MovieTarget[];
  }
  hasTargets(): boolean {
    return Boolean(this.db.query("SELECT 1 FROM movie_targets LIMIT 1").get());
  }
  request(userId: number, movie: CatalogMovie, rawPolicy: MovieQualityPolicy) {
    const policy = movieQualityPolicySchema.parse(rawPolicy);
    const policyJson = JSON.stringify(policy);
    const key = createHash("sha256").update(policyJson).digest("hex");
    return this.transaction(() => {
      const stored = this.upsertMovie(movie);
      const target = this.db
        .query(
          `INSERT INTO movie_targets(movie_id,policy_key,policy_json,created_at) VALUES(?,?,?,?)
        ON CONFLICT(movie_id,policy_key) DO UPDATE SET policy_key=excluded.policy_key RETURNING *`,
        )
        .get(
          stored.id,
          key,
          policyJson,
          new Date().toISOString(),
        ) as MovieTarget;
      const request = this.db
        .query(
          `INSERT INTO movie_requests(user_id,target_id,created_at) VALUES(?,?,?)
        ON CONFLICT(user_id,target_id) DO UPDATE SET withdrawn_at=NULL RETURNING id`,
        )
        .get(userId, target.id, new Date().toISOString()) as { id: number };
      this.event(
        target.id,
        null,
        `request:${request.id}`,
        "requested",
        "Movie requested",
      );
      const retention = new RetentionRepo(this.db, this.beforeWrite);
      const resource = { namespace: "movie-target", id: String(target.id) };
      const intent = retention.get(resource);
      if (!intent || intent.desiredPresence !== "present")
        retention.request(resource, { expectedRevision: intent?.revision ?? 0, reason: "Explicit movie request" });
      return { requestId: request.id, targetId: target.id, movieId: stored.id };
    });
  }
  requests(userId: number) {
    return this.db
      .query(
        "SELECT id,target_id AS targetId,created_at AS createdAt FROM movie_requests WHERE user_id=? ORDER BY id DESC",
      )
      .all(userId) as { id: number; targetId: number; createdAt: string }[];
  }
  canAccessTarget(userId: number, targetId: number): boolean {
    return Boolean(
      this.db
        .query("SELECT 1 FROM movie_requests WHERE user_id=? AND target_id=?")
        .get(userId, targetId),
    );
  }
  requestById(id: number) {
    return this.db
      .query(
        "SELECT id,user_id,target_id,withdrawn_at,created_at FROM movie_requests WHERE id=?",
      )
      .get(id) as {
      id: number;
      user_id: number;
      target_id: number;
      withdrawn_at: string | null;
      created_at: string;
    } | null;
  }
  activeRequestCount(targetId: number): number {
    return (
      this.db
        .query(
          "SELECT COUNT(*) AS n FROM movie_requests WHERE target_id=? AND withdrawn_at IS NULL",
        )
        .get(targetId) as { n: number }
    ).n;
  }
  withdrawRequest(id: number): void {
    this.transaction(() =>
      this.db
        .query("UPDATE movie_requests SET withdrawn_at=? WHERE id=?")
        .run(new Date().toISOString(), id),
    );
  }
  saveReleaseSearch(
    userId: number,
    targetId: number,
    offers: MovieOffer[],
  ): string {
    return this.transaction(() => {
      const now = new Date();
      this.db
        .query("DELETE FROM movie_release_searches WHERE expires_at<?")
        .run(now.toISOString());
      const id = randomUUID();
      this.db
        .query("INSERT INTO movie_release_searches VALUES(?,?,?,?,?)")
        .run(
          id,
          userId,
          targetId,
          JSON.stringify(offers),
          new Date(now.getTime() + 30 * 60_000).toISOString(),
        );
      return id;
    });
  }
  searchedOffer(
    userId: number,
    targetId: number,
    searchId: string,
    index: number,
  ): MovieOffer | null {
    const search = this.db
      .query(
        "SELECT offers_json FROM movie_release_searches WHERE id=? AND user_id=? AND target_id=? AND expires_at>?",
      )
      .get(searchId, userId, targetId, new Date().toISOString()) as {
      offers_json: string;
    } | null;
    return search
      ? ((JSON.parse(search.offers_json) as MovieOffer[])[index] ?? null)
      : null;
  }
  bindDownload(
    acquisitionId: number,
    ref: TorrentRef,
    location: DownloadLocation,
    offerKey: string,
  ): number {
    return this.transaction(() => {
      const existing = this.db
        .query("SELECT * FROM media_torrents WHERE client_id=? AND native_id=?")
        .get(ref.clientId, ref.nativeId) as {
        id: number;
        location_json: string;
      } | null;
      if (existing && existing.location_json !== JSON.stringify(location))
        throw new Error("Torrent binding uses another layout");
      const id =
        existing?.id ??
        (
          this.db
            .query(
              "INSERT INTO media_torrents(client_id,native_id,location_json,offer_key) VALUES(?,?,?,?) RETURNING id",
            )
            .get(
              ref.clientId,
              ref.nativeId,
              JSON.stringify(location),
              offerKey,
            ) as { id: number }
        ).id;
      this.db
        .query("INSERT OR IGNORE INTO movie_acquisition_downloads VALUES(?,?)")
        .run(acquisitionId, id);
      return id;
    });
  }
  observeDownload(id: number, snapshot: TorrentSnapshot): void {
    this.transaction(() =>
      this.db
        .query("UPDATE media_torrents SET observation_json=? WHERE id=?")
        .run(JSON.stringify(snapshot), id),
    );
  }
  activeTorrentDemand(id: number): number {
    return (
      this.db
        .query(
          `SELECT COUNT(DISTINCT r.id) AS n FROM movie_requests r
      JOIN movie_acquisitions a ON a.target_id=r.target_id
      JOIN movie_acquisition_downloads d ON d.acquisition_id=a.id
      WHERE d.torrent_id=? AND r.withdrawn_at IS NULL`,
        )
        .get(id) as { n: number }
    ).n;
  }
  downloads(targetId: number) {
    return this.db
      .query(
        `SELECT t.* FROM media_torrents t JOIN movie_acquisition_downloads d ON d.torrent_id=t.id
      JOIN movie_acquisitions a ON a.id=d.acquisition_id WHERE a.target_id=? ORDER BY a.id DESC,t.id DESC`,
      )
      .all(targetId) as {
      id: number;
      client_id: string;
      native_id: string;
      location_json: string;
      offer_key: string;
      observation_json: string | null;
    }[];
  }
  torrent(clientId: string, nativeId: string) {
    return this.db
      .query("SELECT * FROM media_torrents WHERE client_id=? AND native_id=?")
      .get(clientId, nativeId) as {
      id: number;
      client_id: string;
      native_id: string;
      location_json: string;
      offer_key: string;
    } | null;
  }
  abortPublication(targetId: number, versionId: number): void {
    this.transaction(() => {
      if (this.target(targetId)?.active_version_id === versionId)
        throw new Error("Cannot abort an active version");
      this.db
        .query(
          "DELETE FROM movie_publications WHERE target_id=? AND version_id=?",
        )
        .run(targetId, versionId);
    });
  }
  acquisition(id: number): MovieAcquisition | null {
    return this.db
      .query("SELECT * FROM movie_acquisitions WHERE id=?")
      .get(id) as MovieAcquisition | null;
  }
  acquisitions(targetId: number): MovieAcquisition[] {
    return this.db
      .query(
        "SELECT * FROM movie_acquisitions WHERE target_id=? ORDER BY id DESC",
      )
      .all(targetId) as MovieAcquisition[];
  }
  startAcquisition(
    targetId: number,
    reason: MovieAcquisition["reason"],
    enqueue: (acquisitionId: number) => number,
  ) {
    return this.transaction(() => {
      if (!this.target(targetId)) throw new Error("Unknown movie target");
      const existing = this.acquisitions(targetId).find((entry) => {
        if (entry.finished_at) return false;
        const job = entry.job_id === null ? null : this.getJob(entry.job_id);
        return !job || job.status === "queued" || job.status === "running";
      });
      if (existing) return { acquisition: existing, shared: true };
      const row = this.db
        .query(
          "INSERT INTO movie_acquisitions(target_id,reason,created_at) VALUES(?,?,?) RETURNING id",
        )
        .get(targetId, reason, new Date().toISOString()) as { id: number };
      const jobId = enqueue(row.id);
      this.db
        .query("UPDATE movie_acquisitions SET job_id=? WHERE id=?")
        .run(jobId, row.id);
      return { acquisition: this.acquisition(row.id)!, shared: false };
    });
  }
  decision(id: number, value: unknown): void {
    this.transaction(() =>
      this.db
        .query("UPDATE movie_acquisitions SET decision_json=? WHERE id=?")
        .run(JSON.stringify(value), id),
    );
  }
  event(
    targetId: number,
    acquisitionId: number | null,
    key: string,
    stage: MovieStage,
    message: string,
  ): void {
    this.transaction(() =>
      this.db
        .query(
          `INSERT INTO movie_events(target_id,acquisition_id,event_key,stage,message,created_at)
      VALUES(?,?,?,?,?,?) ON CONFLICT(event_key) DO NOTHING`,
        )
        .run(
          targetId,
          acquisitionId,
          key,
          stage,
          message,
          new Date().toISOString(),
        ),
    );
  }
  events(targetId: number) {
    return this.db
      .query(
        "SELECT id,stage,message,created_at AS createdAt FROM movie_events WHERE target_id=? ORDER BY id",
      )
      .all(targetId) as {
      id: number;
      stage: MovieStage;
      message: string;
      createdAt: string;
    }[];
  }
  version(id: number): {
    id: number;
    facts: MovieFacts;
    files: {
      id: string;
      relativePath: string;
      sizeBytes: number;
      probe: unknown;
    }[];
  } | null {
    const row = this.db
      .query("SELECT id,facts_json FROM movie_versions WHERE id=?")
      .get(id) as { id: number; facts_json: string } | null;
    if (!row) return null;
    const files = this.db
      .query(
        `SELECT f.* FROM managed_content_files f JOIN movie_version_files v ON v.file_id=f.id
      WHERE v.version_id=? ORDER BY v.sequence`,
      )
      .all(id) as {
      id: string;
      relative_path: string;
      size_bytes: number;
      probe_json: string;
    }[];
    return {
      id: row.id,
      facts: movieFactsSchema.parse(JSON.parse(row.facts_json)),
      files: files.map((f) => ({
        id: f.id,
        relativePath: f.relative_path,
        sizeBytes: f.size_bytes,
        probe: JSON.parse(f.probe_json),
      })),
    };
  }
  recordVersion(
    acquisitionId: number,
    rawFacts: MovieFacts,
    files: {
      id: string;
      relativePath: string;
      sizeBytes: number;
      probe: unknown;
    }[],
  ): number {
    const facts = movieFactsSchema.parse(rawFacts);
    if (!files.length) throw new Error("A movie version needs content files");
    return this.transaction(() => {
      const acquisition = this.acquisition(acquisitionId);
      const target = acquisition && this.target(acquisition.target_id);
      if (!target) throw new Error("Unknown acquisition");
      const prior = this.db
        .query("SELECT id FROM movie_versions WHERE acquisition_id=?")
        .get(acquisitionId) as { id: number } | null;
      if (prior) return prior.id;
      const now = new Date().toISOString();
      const version = this.db
        .query(
          "INSERT INTO movie_versions(movie_id,acquisition_id,facts_json,created_at) VALUES(?,?,?,?) RETURNING id",
        )
        .get(target.movie_id, acquisitionId, JSON.stringify(facts), now) as {
        id: number;
      };
      for (const [index, file] of files.entries()) {
        this.db
          .query(
            "INSERT INTO managed_content_files(id,relative_path,size_bytes,probe_json,created_at) VALUES(?,?,?,?,?)",
          )
          .run(
            file.id,
            file.relativePath,
            file.sizeBytes,
            JSON.stringify(file.probe),
            now,
          );
        this.db
          .query("INSERT INTO movie_version_files VALUES(?,?,?)")
          .run(version.id, file.id, index);
      }
      return version.id;
    });
  }
  selectVersion(
    targetId: number,
    versionId: number,
    expectedRevision: number,
  ): void {
    this.transaction(() => {
      assertVersionContentAvailable(this.db, "movie", versionId);
      const target = this.target(targetId);
      const version = this.db
        .query("SELECT movie_id FROM movie_versions WHERE id=?")
        .get(versionId) as { movie_id: number } | null;
      if (!target || version?.movie_id !== target.movie_id)
        throw new Error("Version belongs to another movie");
      if (target.active_version_id === versionId) return;
      const changed = this.db
        .query(
          "UPDATE movie_targets SET active_version_id=?,revision=revision+1 WHERE id=? AND revision=?",
        )
        .run(versionId, targetId, expectedRevision);
      if (changed.changes !== 1)
        throw new Error("Movie target changed during publication");
    });
  }
  finishAcquisition(id: number): void {
    this.transaction(() =>
      this.db
        .query("UPDATE movie_acquisitions SET finished_at=? WHERE id=?")
        .run(new Date().toISOString(), id),
    );
  }
  publications() {
    return this.db
      .query(
        "SELECT target_id,version_id,expected_revision FROM movie_publications ORDER BY target_id",
      )
      .all() as {
      target_id: number;
      version_id: number;
      expected_revision: number;
    }[];
  }
  stagePublication(targetId: number, versionId: number): void {
    this.transaction(() => {
      assertVersionContentAvailable(this.db, "movie", versionId);
      const target = this.target(targetId);
      const version = this.version(versionId);
      const owner = this.db
        .query("SELECT movie_id FROM movie_versions WHERE id=?")
        .get(versionId) as { movie_id: number } | null;
      if (!target || !version || owner?.movie_id !== target.movie_id)
        throw new Error("Invalid movie publication identity");
      const policy = movieQualityPolicySchema.parse(
        JSON.parse(target.policy_json),
      );
      if (
        evaluateMovieQuality(policy, version.facts).eligibility !== "eligible"
      )
        throw new Error("Version does not satisfy quality policy");
      if (target.active_version_id && target.active_version_id !== versionId) {
        const current = this.version(target.active_version_id)!;
        if (!assessMovieUpgrade(policy, current.facts, version.facts).upgrade)
          throw new Error("Replacement is not an upgrade");
      }
      const prior = this.publications().find(
        (entry) => entry.target_id === targetId,
      );
      if (prior && prior.version_id !== versionId)
        throw new Error("Another publication is pending");
      if (!prior)
        this.db
          .query("INSERT INTO movie_publications VALUES(?,?,?,?)")
          .run(targetId, versionId, target.revision, new Date().toISOString());
    });
  }
  clearPublication(targetId: number, versionId: number): void {
    this.transaction(() => {
      if (this.target(targetId)?.active_version_id !== versionId)
        throw new Error("Cannot retire an unpromoted publication");
      this.db
        .query(
          "DELETE FROM movie_publications WHERE target_id=? AND version_id=?",
        )
        .run(targetId, versionId);
    });
  }
}
