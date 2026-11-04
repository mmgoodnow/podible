import type { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { z } from "zod";
import type { JobRow } from "../app-types";
import type { DownloadLocation, TorrentRef, TorrentSnapshot } from "../media/torrent-client";
import {
  tvCatalogEntrySchema, tvContentFileSchema, tvStages,
  type TvAcquisition, type TvActiveEpisode, type TvCatalogEntry,
  type TvContentFile, type TvEntry, type TvEpisode, type TvEvent,
  type TvExternalRef, type TvPolicy, type TvPublication, type TvRequest,
  type TvRequestOrigin, type TvRequestResult, type TvStage, type TvTarget,
  type TvVersion,
} from "./types";

function json(value: unknown): string {
  const result = JSON.stringify(value, (_key, item) =>
    item && typeof item === "object" && !Array.isArray(item)
      ? Object.fromEntries(Object.keys(item).sort().map((key) => [key, item[key]]))
      : item,
  );
  if (result === undefined) throw new Error("Expected JSON data");
  return result;
}
function ids(values: number[]): number[] {
  return [...new Set(z.array(z.number().int().positive()).parse(values))].sort((a, b) => a - b);
}
function keyRef(key: string, source: string): TvExternalRef | null {
  if (key.startsWith("ephemeral:")) return null;
  const split = key.indexOf(":");
  const provider = key.slice(0, split) as TvExternalRef["provider"];
  const rest = key.slice(split + 1);
  if (provider === "sonarr") {
    const end = rest.lastIndexOf(":");
    if (end < 1) throw new Error("Sonarr episode keys require an instance and ID");
    return { provider, instance: rest.slice(0, end), id: rest.slice(end + 1), source };
  }
  return { provider, instance: null, id: rest, source };
}

export class TvRepo {
  constructor(
    private db: Database,
    private beforeWrite: () => void,
    private getJob: (id: number) => Pick<JobRow, "status"> | null,
  ) {}

  transaction<T>(fn: () => T): T {
    return this.db.transaction(() => {
      this.beforeWrite();
      return fn();
    })();
  }

  upsertEntry(input: TvCatalogEntry): TvEntry {
    const catalog = tvCatalogEntrySchema.parse(input);
    if (new Set(catalog.episodes.map((ep) => ep.key)).size !== catalog.episodes.length)
      throw new Error("Duplicate catalog episode key");
    const source = catalog.source ?? catalog.key;
    const primary = keyRef(catalog.key, source)!;
    for (const [provider, id] of [["tvdb", catalog.tvdbId], ["tmdb", catalog.tmdbId]] as const) {
      if (primary.provider === provider && id !== null && primary.id !== String(id))
        throw new Error("Catalog key conflicts with its provider ID");
    }
    return this.transaction(() => {
      const now = new Date().toISOString();
      const { episodes, relations, refs, ...metadata } = catalog;
      const row = this.db.query(`INSERT INTO tv_entries(catalog_key,metadata_json,updated_at) VALUES(?,?,?)
        ON CONFLICT(catalog_key) DO UPDATE SET metadata_json=excluded.metadata_json,updated_at=excluded.updated_at RETURNING id`)
        .get(catalog.key, json(metadata), now) as { id: number };
      const entryRefs = [primary, ...refs];
      if (catalog.tvdbId !== null) entryRefs.push({ provider: "tvdb", id: String(catalog.tvdbId), instance: null, source });
      if (catalog.tmdbId !== null) entryRefs.push({ provider: "tmdb", id: String(catalog.tmdbId), instance: null, source });
      this.storeRefs("entry", row.id, entryRefs);
      for (const ep of episodes) {
        if (new Set(ep.numbering.map((n) => n.scheme)).size !== ep.numbering.length)
          throw new Error("Duplicate episode numbering scheme");
        const identity = ep.identity ?? {
          stability: ep.key.startsWith("ephemeral:") ? "ephemeral" as const : "provider" as const,
          source,
        };
        const episode = this.db.query(`INSERT INTO tv_episodes(episode_key,metadata_json,updated_at) VALUES(?,?,?)
          ON CONFLICT(episode_key) DO UPDATE SET metadata_json=excluded.metadata_json,updated_at=excluded.updated_at RETURNING id`)
          .get(ep.key, json({ key: ep.key, title: ep.title, airDate: ep.airDate, runtimeMinutes: ep.runtimeMinutes, identity }), now) as { id: number };
        this.db.query("INSERT OR IGNORE INTO tv_entry_episodes(entry_id,episode_id) VALUES(?,?)").run(row.id, episode.id);
        const ref = keyRef(ep.key, identity.source);
        this.storeRefs("episode", episode.id, [...(ref ? [ref] : []), ...ep.refs]);
        // Replace this entry's provider maps, never its episode identity or other memberships.
        this.db.query("DELETE FROM tv_episode_numbering WHERE entry_id=? AND episode_id=?").run(row.id, episode.id);
        for (const n of ep.numbering)
          this.db.query(`INSERT INTO tv_episode_numbering(entry_id,episode_id,scheme,season,episode,absolute,source)
            VALUES(?,?,?,?,?,?,?)`).run(row.id, episode.id, n.scheme, n.season, n.episode, n.absolute, n.source ?? source);
      }
      this.db.query("DELETE FROM tv_entry_relations WHERE entry_id=?").run(row.id);
      for (const rel of relations)
        this.db.query(`INSERT OR IGNORE INTO tv_entry_relations(entry_id,related_key,title,relation,source)
          VALUES(?,?,?,?,?)`).run(row.id, rel.key, rel.title, rel.relation, rel.source);
      return this.entry(row.id)!;
    });
  }

  private storeRefs(kind: "entry" | "episode", id: number, refs: TvExternalRef[]): void {
    for (const ref of refs)
      this.db.query(`INSERT INTO tv_${kind}_refs(${kind}_id,provider,instance,external_id,source) VALUES(?,?,?,?,?)
        ON CONFLICT(${kind}_id,provider,instance,external_id) DO UPDATE SET source=excluded.source`)
        .run(id, ref.provider, ref.instance ?? "", ref.id, ref.source);
  }

  private refs(kind: "entry" | "episode", id: number): TvExternalRef[] {
    return this.db.query(`SELECT provider,external_id AS id,NULLIF(instance,'') AS instance,source
      FROM tv_${kind}_refs WHERE ${kind}_id=? ORDER BY provider,instance,external_id`).all(id) as TvExternalRef[];
  }

  entry(id: number): TvEntry | null {
    const row = this.db.query("SELECT metadata_json FROM tv_entries WHERE id=?").get(id) as { metadata_json: string } | null;
    if (!row) return null;
    return {
      ...JSON.parse(row.metadata_json), id, refs: this.refs("entry", id), episodes: this.episodes(id),
      relations: this.db.query(`SELECT related_key AS key,title,relation,source FROM tv_entry_relations
        WHERE entry_id=? ORDER BY related_key,relation,source`).all(id),
    } as TvEntry;
  }

  entryByKey(key: string): TvEntry | null {
    const row = this.db.query("SELECT id FROM tv_entries WHERE catalog_key=?").get(key) as { id: number } | null;
    return row ? this.entry(row.id) : null;
  }

  entries(): TvEntry[] {
    return (this.db.query("SELECT id FROM tv_entries ORDER BY id").all() as { id: number }[]).map((r) => this.entry(r.id)!);
  }

  episode(id: number, entryId?: number): TvEpisode | null {
    const row = this.db.query("SELECT metadata_json FROM tv_episodes WHERE id=?").get(id) as { metadata_json: string } | null;
    if (!row) return null;
    if (entryId !== undefined && !this.db.query("SELECT 1 FROM tv_entry_episodes WHERE entry_id=? AND episode_id=?").get(entryId, id)) return null;
    return {
      ...JSON.parse(row.metadata_json), id, refs: this.refs("episode", id),
      numbering: this.db.query(`SELECT entry_id AS entryId,scheme,season,episode,absolute,source FROM tv_episode_numbering
        WHERE episode_id=? AND (? IS NULL OR entry_id=?) ORDER BY entry_id,scheme`).all(id, entryId ?? null, entryId ?? null),
    } as TvEpisode;
  }

  episodes(entryId: number): TvEpisode[] {
    return (this.db.query("SELECT episode_id FROM tv_entry_episodes WHERE entry_id=? ORDER BY episode_id").all(entryId) as { episode_id: number }[])
      .map((r) => this.episode(r.episode_id, entryId)!);
  }

  target(id: number): TvTarget | null {
    return this.db.query("SELECT * FROM tv_targets WHERE id=?").get(id) as TvTarget | null;
  }

  targets(): TvTarget[] {
    return this.db.query("SELECT * FROM tv_targets ORDER BY id").all() as TvTarget[];
  }

  hasTargets(): boolean {
    return Boolean(this.db.query("SELECT 1 FROM tv_targets LIMIT 1").get());
  }

  activeRequestCount(targetId: number): number {
    return (this.db.query("SELECT COUNT(*) AS n FROM tv_requests WHERE target_id=? AND withdrawn_at IS NULL").get(targetId) as { n: number }).n;
  }

  torrent(clientId: string, nativeId: string) {
    return this.db.query("SELECT * FROM media_torrents WHERE client_id=? AND native_id=?").get(clientId, nativeId) as {
      id: number; client_id: string; native_id: string; location_json: string; offer_key: string; observation_json: string | null;
    } | null;
  }

  bindDownload(acquisitionId: number, ref: TorrentRef, location: DownloadLocation, offerKey: string): number {
    return this.transaction(() => {
      if (!this.acquisition(acquisitionId)) throw new Error("Unknown TV acquisition");
      const existing = this.torrent(ref.clientId, ref.nativeId);
      if (existing && existing.location_json !== JSON.stringify(location))
        throw new Error("Torrent binding uses another layout");
      const id = existing?.id ?? (this.db.query(`INSERT INTO media_torrents(client_id,native_id,location_json,offer_key)
        VALUES(?,?,?,?) RETURNING id`).get(ref.clientId, ref.nativeId, JSON.stringify(location), offerKey) as { id: number }).id;
      this.db.query("INSERT OR IGNORE INTO tv_acquisition_downloads(acquisition_id,torrent_id) VALUES(?,?)").run(acquisitionId, id);
      return id;
    });
  }

  observeDownload(id: number, snapshot: TorrentSnapshot): void {
    this.transaction(() => this.db.query("UPDATE media_torrents SET observation_json=? WHERE id=?").run(JSON.stringify(snapshot), id));
  }

  downloads(targetId: number) {
    return this.db.query(`SELECT t.* FROM media_torrents t JOIN tv_acquisition_downloads d ON d.torrent_id=t.id
      JOIN tv_acquisitions a ON a.id=d.acquisition_id WHERE a.target_id=? ORDER BY a.id DESC,t.id DESC`).all(targetId) as {
      id: number; client_id: string; native_id: string; location_json: string; offer_key: string; observation_json: string | null;
    }[];
  }

  request(userId: number, catalog: TvCatalogEntry, policy: TvPolicy, episodeKeys: string[]): TvRequestResult {
    return this.transaction(() => {
      const entry = this.upsertEntry(catalog);
      return this.requestEpisodes(userId, entry.id, policy, this.resolveKeys(entry, episodeKeys));
    });
  }

  requestApproved(input: {
    userId: number; entry: TvCatalogEntry; policy: TvPolicy; episodeKeys: string[]; origin: TvRequestOrigin;
  }): TvRequestResult {
    return this.transaction(() => {
      const entry = this.upsertEntry(input.entry);
      return this.requestEpisodes(input.userId, entry.id, input.policy, this.resolveKeys(entry, input.episodeKeys), input.origin);
    });
  }

  private resolveKeys(entry: TvEntry, keys: string[]): number[] {
    return ids(keys.map((key) => {
      const ep = entry.episodes.find((e) => e.key === key);
      if (!ep) throw new Error("Selected episode key is not a member of this entry");
      return ep.id;
    }));
  }

  requestEpisodes(userId: number, entryId: number, policy: TvPolicy, episodeIds: number[], origin?: TvRequestOrigin): TvRequestResult {
    const selected = ids(episodeIds);
    if (!selected.length) throw new Error("A TV request needs selected episodes resolved upstream");
    const policyJson = json(z.record(z.string(), z.unknown()).parse(policy));
    if (origin) {
      z.string().min(1).parse(origin.instance);
      z.number().int().positive().parse(origin.externalRequestId);
    }
    return this.transaction(() => {
      if (!this.entry(entryId)) throw new Error("Unknown TV entry");
      for (const id of selected)
        if (!this.episode(id, entryId)) throw new Error("Selected episode belongs to another entry");
      const prior = origin && this.origin(origin.instance, origin.externalRequestId, entryId);
      if (prior) {
        const request = this.requestById(prior.requestId)!;
        const target = this.target(request.target_id)!;
        if (request.user_id !== userId || target.entry_id !== entryId || target.policy_json !== policyJson)
          throw new Error("Seerr origin conflicts with its original principal, entry or policy");
        for (const episodeId of selected)
          this.db.query("INSERT OR IGNORE INTO tv_request_episodes(request_id,entry_id,episode_id) VALUES(?,?,?)").run(request.id, entryId, episodeId);
        this.db.query("UPDATE tv_request_origins SET source_json=? WHERE instance=? AND external_request_id=? AND entry_id=?")
          .run(json(origin.source), origin.instance, origin.externalRequestId, entryId);
        return { requestId: request.id, targetId: target.id, entryId, episodeIds: this.requestedEpisodeIds(request.id) };
      }
      const now = new Date().toISOString();
      const target = this.db.query(`INSERT INTO tv_targets(entry_id,policy_key,policy_json,created_at) VALUES(?,?,?,?)
        ON CONFLICT(entry_id,policy_key) DO UPDATE SET policy_key=excluded.policy_key RETURNING *`)
        .get(entryId, createHash("sha256").update(policyJson).digest("hex"), policyJson, now) as TvTarget;
      const requestKey = origin ? json(["seerr", origin.instance, origin.externalRequestId]) : "direct";
      const request = this.db.query(`INSERT INTO tv_requests(user_id,target_id,request_key,created_at) VALUES(?,?,?,?)
        ON CONFLICT(user_id,target_id,request_key) DO UPDATE SET withdrawn_at=NULL RETURNING *`)
        .get(userId, target.id, requestKey, now) as TvRequest;
      for (const episodeId of selected)
        this.db.query("INSERT OR IGNORE INTO tv_request_episodes(request_id,entry_id,episode_id) VALUES(?,?,?)").run(request.id, entryId, episodeId);
      if (origin)
        this.db.query(`INSERT INTO tv_request_origins(instance,external_request_id,entry_id,request_id,source_json,created_at)
          VALUES(?,?,?,?,?,?)`).run(origin.instance, origin.externalRequestId, entryId, request.id, json(origin.source), now);
      this.event(target.id, null, `tv-request:${request.id}`, "requested", "TV episodes requested");
      return { requestId: request.id, targetId: target.id, entryId, episodeIds: this.requestedEpisodeIds(request.id) };
    });
  }

  origins(instance?: string, externalRequestId?: number) {
    return (this.db.query(`SELECT instance,external_request_id AS externalRequestId,entry_id AS entryId,
      request_id AS requestId,source_json,created_at AS createdAt FROM tv_request_origins
      WHERE (? IS NULL OR instance=?) AND (? IS NULL OR external_request_id=?) ORDER BY instance,external_request_id,entry_id`)
      .all(instance ?? null, instance ?? null, externalRequestId ?? null, externalRequestId ?? null) as {
        instance: string; externalRequestId: number; entryId: number; requestId: number; source_json: string; createdAt: string;
      }[]).map(({ source_json, ...row }) => ({ ...row, source: JSON.parse(source_json) as unknown }));
  }

  origin(instance: string, externalRequestId: number, entryId?: number) {
    const origins = this.origins(instance, externalRequestId).filter((o) => entryId === undefined || o.entryId === entryId);
    if (origins.length > 1) throw new Error("Seerr origin spans multiple entries; specify entryId or use origins()");
    return origins[0] ?? null;
  }

  requestById(id: number): TvRequest | null {
    return this.db.query("SELECT * FROM tv_requests WHERE id=?").get(id) as TvRequest | null;
  }

  requests(userId?: number): TvRequest[] {
    return this.db.query("SELECT * FROM tv_requests WHERE (? IS NULL OR user_id=?) ORDER BY id DESC").all(userId ?? null, userId ?? null) as TvRequest[];
  }

  canAccessTarget(userId: number, targetId: number): boolean {
    return Boolean(this.db.query("SELECT 1 FROM tv_requests WHERE user_id=? AND target_id=? AND withdrawn_at IS NULL").get(userId, targetId));
  }

  withdrawRequest(id: number): void {
    this.transaction(() => this.db.query("UPDATE tv_requests SET withdrawn_at=COALESCE(withdrawn_at,?) WHERE id=?").run(new Date().toISOString(), id));
  }

  requestedEpisodeIds(requestId: number): number[] {
    return (this.db.query("SELECT episode_id FROM tv_request_episodes WHERE request_id=? ORDER BY episode_id").all(requestId) as { episode_id: number }[]).map((r) => r.episode_id);
  }

  requestedEpisodes(requestId: number): TvEpisode[] {
    const request = this.requestById(requestId);
    if (!request) return [];
    const entryId = this.target(request.target_id)!.entry_id;
    return this.requestedEpisodeIds(requestId).map((id) => this.episode(id, entryId)!);
  }

  missingEpisodes(requestId: number): TvEpisode[] {
    const request = this.requestById(requestId);
    if (!request) throw new Error("Unknown TV request");
    const available = new Set(this.activeEpisodes(request.target_id).map((ep) => ep.episode_id));
    return this.requestedEpisodes(requestId).filter((ep) => !available.has(ep.id));
  }

  acquisition(id: number): TvAcquisition | null {
    return this.db.query("SELECT * FROM tv_acquisitions WHERE id=?").get(id) as TvAcquisition | null;
  }

  acquisitions(targetId: number): TvAcquisition[] {
    return this.db.query("SELECT * FROM tv_acquisitions WHERE target_id=? ORDER BY id DESC").all(targetId) as TvAcquisition[];
  }

  startAcquisition(targetId: number, reason: TvAcquisition["reason"], enqueue: (id: number) => number, requestId?: number) {
    return this.transaction(() => {
      if (!this.target(targetId)) throw new Error("Unknown TV target");
      if (requestId !== undefined) {
        const request = this.requestById(requestId);
        if (!request || request.target_id !== targetId || request.withdrawn_at !== null)
          throw new Error("Acquisition request does not belong to the active target");
      }
      const existing = this.acquisitions(targetId).find((a) => {
        if (a.request_id !== (requestId ?? null) || a.finished_at) return false;
        const job = a.job_id === null ? null : this.getJob(a.job_id);
        return !job || job.status === "queued" || job.status === "running";
      });
      if (existing) return { acquisition: existing, shared: true };
      const row = this.db.query("INSERT INTO tv_acquisitions(target_id,request_id,reason,created_at) VALUES(?,?,?,?) RETURNING id")
        .get(targetId, requestId ?? null, reason, new Date().toISOString()) as { id: number };
      const jobId = z.number().int().positive().parse(enqueue(row.id));
      this.db.query("UPDATE tv_acquisitions SET job_id=? WHERE id=?").run(jobId, row.id);
      return { acquisition: this.acquisition(row.id)!, shared: false };
    });
  }

  decision(id: number, value: unknown): void {
    this.transaction(() => {
      if (!this.acquisition(id)) throw new Error("Unknown TV acquisition");
      this.db.query("UPDATE tv_acquisitions SET decision_json=? WHERE id=?").run(json(value), id);
    });
  }

  finishAcquisition(id: number): void {
    this.transaction(() => {
      if (!this.acquisition(id)) throw new Error("Unknown TV acquisition");
      this.db.query("UPDATE tv_acquisitions SET finished_at=COALESCE(finished_at,?) WHERE id=?").run(new Date().toISOString(), id);
    });
  }

  version(id: number): TvVersion | null {
    const row = this.db.query("SELECT * FROM tv_versions WHERE id=?").get(id) as {
      id: number; entry_id: number; target_id: number; acquisition_id: number; facts_json: string;
    } | null;
    if (!row) return null;
    const files = (this.db.query(`SELECT f.* FROM managed_content_files f JOIN tv_version_files v ON v.file_id=f.id
      WHERE v.version_id=? ORDER BY v.sequence`).all(id) as {
        id: string; relative_path: string; size_bytes: number; probe_json: string;
      }[]).map((f) => ({
        id: f.id, relativePath: f.relative_path, sizeBytes: f.size_bytes, probe: JSON.parse(f.probe_json) as unknown,
        episodeIds: (this.db.query("SELECT episode_id FROM tv_version_file_episodes WHERE version_id=? AND file_id=? ORDER BY episode_id")
          .all(id, f.id) as { episode_id: number }[]).map((r) => r.episode_id),
      }));
    return { id, entryId: row.entry_id, targetId: row.target_id, acquisitionId: row.acquisition_id,
      facts: JSON.parse(row.facts_json) as unknown, files, episodeIds: ids(files.flatMap((f) => f.episodeIds)) };
  }

  recordVersion(acquisitionId: number, facts: unknown, rawFiles: TvContentFile[]): number {
    z.object({ identityVerified: z.literal(true) }).parse(facts);
    const files = z.array(tvContentFileSchema).min(1, "A TV version needs content files").parse(rawFiles)
      .map((f) => ({ ...f, episodeIds: ids(f.episodeIds) }));
    if (new Set(files.map((f) => f.id)).size !== files.length) throw new Error("Duplicate content file identity");
    return this.transaction(() => {
      const acquisition = this.acquisition(acquisitionId);
      const target = acquisition && this.target(acquisition.target_id);
      if (!target) throw new Error("Unknown TV acquisition");
      const prior = this.db.query("SELECT id FROM tv_versions WHERE acquisition_id=?").get(acquisitionId) as { id: number } | null;
      if (prior) {
        const version = this.version(prior.id)!;
        if (json(version.facts) !== json(facts) || json(version.files) !== json(files))
          throw new Error("Acquisition already has a different verified version");
        return prior.id;
      }
      for (const file of files) {
        for (const ep of file.episodeIds)
          if (!this.episode(ep, target.entry_id)) throw new Error("Content episode belongs to another entry");
        if (this.db.query("SELECT 1 FROM movie_version_files WHERE file_id=?").get(file.id) ||
          this.db.query("SELECT 1 FROM tv_version_files WHERE file_id=? AND entry_id<>?").get(file.id, target.entry_id))
          throw new Error("Content file is owned by another media entry");
        const previousCoverage = ids((this.db.query("SELECT DISTINCT episode_id FROM tv_version_file_episodes WHERE file_id=?")
          .all(file.id) as { episode_id: number }[]).map((r) => r.episode_id));
        if (previousCoverage.length && json(previousCoverage) !== json(file.episodeIds))
          throw new Error("Content file already has different episode coverage");
        const stored = this.db.query("SELECT relative_path,size_bytes,probe_json FROM managed_content_files WHERE id=?").get(file.id) as {
          relative_path: string; size_bytes: number; probe_json: string;
        } | null;
        if (stored && (stored.relative_path !== file.relativePath || stored.size_bytes !== file.sizeBytes || json(JSON.parse(stored.probe_json)) !== json(file.probe)))
          throw new Error("Managed content identity is already occupied by different content");
      }
      const now = new Date().toISOString();
      const row = this.db.query("INSERT INTO tv_versions(entry_id,target_id,acquisition_id,facts_json,created_at) VALUES(?,?,?,?,?) RETURNING id")
        .get(target.entry_id, target.id, acquisitionId, json(facts), now) as { id: number };
      for (const [sequence, file] of files.entries()) {
        this.db.query(`INSERT INTO managed_content_files(id,relative_path,size_bytes,probe_json,created_at) VALUES(?,?,?,?,?)
          ON CONFLICT(id) DO NOTHING`).run(file.id, file.relativePath, file.sizeBytes, json(file.probe), now);
        this.db.query("INSERT INTO tv_version_files(version_id,entry_id,file_id,sequence) VALUES(?,?,?,?)").run(row.id, target.entry_id, file.id, sequence);
        for (const ep of file.episodeIds)
          this.db.query("INSERT INTO tv_version_file_episodes(version_id,file_id,entry_id,episode_id) VALUES(?,?,?,?)")
            .run(row.id, file.id, target.entry_id, ep);
      }
      return row.id;
    });
  }

  activeEpisodes(targetId: number): TvActiveEpisode[] {
    return this.db.query("SELECT episode_id,version_id FROM tv_active_episodes WHERE target_id=? ORDER BY episode_id").all(targetId) as TvActiveEpisode[];
  }

  publications(): TvPublication[] {
    return this.db.query("SELECT * FROM tv_publications ORDER BY target_id").all() as TvPublication[];
  }

  private ownedVersion(targetId: number, versionId: number): { target: TvTarget; version: TvVersion } {
    const target = this.target(targetId);
    const version = this.version(versionId);
    if (!target || !version || version.targetId !== targetId || version.entryId !== target.entry_id)
      throw new Error("TV version belongs to another target or entry");
    if (!version.files.length || version.files.some((f) => !f.episodeIds.length))
      throw new Error("TV version has unverified episode coverage");
    return { target, version };
  }

  stagePublication(targetId: number, versionId: number, expectedRevision?: number): void {
    this.transaction(() => {
      const { target } = this.ownedVersion(targetId, versionId);
      const prior = this.publications().find((p) => p.target_id === targetId);
      if (prior) {
        if (prior.version_id !== versionId) throw new Error("Another TV publication is pending");
        if (expectedRevision !== undefined && prior.expected_revision !== expectedRevision)
          throw new Error("TV publication revision conflicts with staged intent");
        return;
      }
      const revision = expectedRevision ?? target.revision;
      if (target.revision !== revision) throw new Error("TV target changed during publication");
      this.db.query("INSERT INTO tv_publications(target_id,version_id,expected_revision,created_at) VALUES(?,?,?,?)")
        .run(targetId, versionId, revision, new Date().toISOString());
    });
  }

  promotePublication(targetId: number, versionId: number, expectedRevision: number): void {
    this.transaction(() => {
      const { target, version } = this.ownedVersion(targetId, versionId);
      const publication = this.publications().find((p) => p.target_id === targetId);
      if (!publication || publication.version_id !== versionId || publication.expected_revision !== expectedRevision)
        throw new Error("TV publication is not staged at the expected revision");
      const active = this.activeEpisodes(targetId);
      if (target.revision === expectedRevision + 1 && version.episodeIds.every((id) => active.some((a) => a.episode_id === id && a.version_id === versionId))) return;
      const next = new Map(active.map((ep) => [ep.episode_id, ep.version_id]));
      for (const ep of version.episodeIds) next.set(ep, versionId);
      for (const oldId of new Set(active.map((ep) => ep.version_id))) {
        if (oldId === versionId) continue;
        for (const file of this.version(oldId)!.files) {
          const remaining = file.episodeIds.filter((ep) => next.get(ep) === oldId);
          // A playable multi-episode file cannot truthfully represent only a subset.
          if (remaining.length > 0 && remaining.length !== file.episodeIds.length)
            throw new Error("TV promotion would partially invalidate an active multi-episode file");
        }
      }
      const changed = this.db.query("UPDATE tv_targets SET revision=revision+1 WHERE id=? AND revision=?").run(targetId, expectedRevision);
      if (changed.changes !== 1) throw new Error("TV target changed during publication");
      for (const ep of version.episodeIds)
        this.db.query(`INSERT INTO tv_active_episodes(target_id,entry_id,episode_id,version_id) VALUES(?,?,?,?)
          ON CONFLICT(target_id,episode_id) DO UPDATE SET version_id=excluded.version_id`).run(targetId, target.entry_id, ep, versionId);
    });
  }

  clearPublication(targetId: number, versionId: number): void {
    this.transaction(() => {
      const { target, version } = this.ownedVersion(targetId, versionId);
      const publication = this.publications().find((p) => p.target_id === targetId);
      if (!publication) return;
      const active = this.activeEpisodes(targetId);
      if (publication.version_id !== versionId || target.revision !== publication.expected_revision + 1 ||
        !version.episodeIds.every((id) => active.some((a) => a.episode_id === id && a.version_id === versionId)))
        throw new Error("Cannot retire an unpromoted TV publication");
      this.db.query("DELETE FROM tv_publications WHERE target_id=? AND version_id=?").run(targetId, versionId);
    });
  }

  abortPublication(targetId: number, versionId: number): void {
    this.transaction(() => {
      this.ownedVersion(targetId, versionId);
      if (this.activeEpisodes(targetId).some((a) => a.version_id === versionId))
        throw new Error("Cannot abort an active TV version");
      this.db.query("DELETE FROM tv_publications WHERE target_id=? AND version_id=?").run(targetId, versionId);
    });
  }

  event(targetId: number, acquisitionId: number | null, key: string, stage: TvStage, message: string): void {
    z.enum(tvStages).parse(stage);
    this.transaction(() => {
      const prior = this.db.query("SELECT target_id,acquisition_id FROM tv_events WHERE event_key=?").get(key) as {
        target_id: number; acquisition_id: number | null;
      } | null;
      if (prior && (prior.target_id !== targetId || prior.acquisition_id !== acquisitionId))
        throw new Error("TV event key is owned by another target or acquisition");
      this.db.query(`INSERT INTO tv_events(target_id,acquisition_id,event_key,stage,message,created_at) VALUES(?,?,?,?,?,?)
        ON CONFLICT(event_key) DO NOTHING`).run(targetId, acquisitionId, key, stage, message, new Date().toISOString());
    });
  }

  events(targetId: number): TvEvent[] {
    return this.db.query("SELECT id,acquisition_id AS acquisitionId,stage,message,created_at AS createdAt FROM tv_events WHERE target_id=? ORDER BY id").all(targetId) as TvEvent[];
  }
}
