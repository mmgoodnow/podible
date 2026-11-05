import type { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import type { MovieOffer } from "../movies/offers";
import { queueMovieAcquisition } from "../movies/service";
import { evaluateMovieQuality, movieFactsSchema, movieQualityPolicySchema,
  type MovieFacts, type MovieQualityPolicy } from "../movies/quality";

const id = z.number().int().positive().safe();
const sourceSchema = z.object({ id, clientId: z.string().min(1), nativeId: z.string().min(1), offerKey: z.string() }).strict();
const reportSchema = z.object({
  reportId: z.string().uuid(), reportedByUserId: id, reason: z.string().trim().min(1).max(2000),
  reportedAt: z.string().datetime(),
}).strict();
const evidenceSchema = z.object({
  version: z.literal(1), status: z.literal("confirmed-nuked"), source: sourceSchema,
  reports: z.array(reportSchema).min(1),
}).strict();
export const confirmedReleaseNukeSchema = z.object({
  reportId: z.string().uuid(), sourceTorrentId: id, reason: z.string().trim().min(1).max(2000),
}).strict();
export type ConfirmedReleaseNuke = z.infer<typeof confirmedReleaseNukeSchema>;
export type ReleaseHealthEvidence = z.infer<typeof evidenceSchema>;
export type ReleaseHealthSource = z.infer<typeof sourceSchema>;
export type ReleaseNukeReceipt = {
  commandHash: string; evidence: ReleaseHealthEvidence;
  replacements: Array<{ targetId: number; acquisitionId: number; jobId: number; shared: boolean }>;
};
const PREFIX = "release-health:torrent:";
const stateKey = (torrentId: number) => PREFIX + id.parse(torrentId);
const reportKey = (reportId: string) => `release-health:report:${z.string().uuid().parse(reportId)}`;
const versionSourceKey = (versionId: number) => `release-health:movie-version-source:${id.parse(versionId)}`;

/** Explicit trusted release defects, not inferred tracker registration/availability. */
export class ReleaseHealthRepo {
  constructor(private db: Database, private beforeWrite: () => void = () => {}) {}

  source(torrentId: number): ReleaseHealthSource | null {
    return this.db.query(`SELECT id,client_id AS clientId,native_id AS nativeId,offer_key AS offerKey
      FROM media_torrents WHERE id=?`).get(id.parse(torrentId)) as ReleaseHealthSource | null;
  }

  evidence(torrentId: number): ReleaseHealthEvidence | null {
    const row = this.db.query("SELECT value_json FROM app_state WHERE key=?").get(stateKey(torrentId)) as { value_json: string } | null;
    if (!row) return null;
    const evidence = evidenceSchema.parse(JSON.parse(row.value_json));
    if (evidence.source.id !== torrentId) throw new Error("Release health evidence references another torrent");
    const source = this.source(torrentId);
    if (source && JSON.stringify(source) !== JSON.stringify(evidence.source))
      throw new Error("Release health source identity changed");
    return evidence;
  }

  isOfferBlocked(offer: Pick<MovieOffer, "key" | "infoHash">): boolean {
    const hash = offer.infoHash?.toLowerCase() ?? "";
    const rows = this.db.query(`SELECT value_json FROM app_state WHERE key>=? AND key<? AND
      (json_extract(value_json,'$.source.offerKey')=? OR (?<>'' AND lower(json_extract(value_json,'$.source.nativeId'))=?))`)
      .all(PREFIX, PREFIX + "~", offer.key, hash, hash) as { value_json: string }[];
    return rows.some((row) => evidenceSchema.parse(JSON.parse(row.value_json)).status === "confirmed-nuked");
  }

  /** Selection evidence narrows a multi-attempt acquisition to its actual imported release. */
  versionSources(versionId: number): ReleaseHealthSource[] {
    const pin = this.db.query("SELECT value_json FROM app_state WHERE key=?").get(versionSourceKey(versionId)) as { value_json: string } | null;
    if (pin) {
      const source = sourceSchema.parse(JSON.parse(pin.value_json));
      const current = this.source(source.id);
      if (current && JSON.stringify(current) !== JSON.stringify(source)) throw new Error("Pinned movie source identity changed");
      return [source];
    }
    const rows = this.db.query(`SELECT DISTINCT t.id,t.client_id AS clientId,t.native_id AS nativeId,t.offer_key AS offerKey,
      a.decision_json AS decisionJson FROM movie_versions v JOIN movie_acquisitions a ON a.id=v.acquisition_id
      JOIN movie_acquisition_downloads d ON d.acquisition_id=a.id JOIN media_torrents t ON t.id=d.torrent_id
      WHERE v.id=? ORDER BY t.id`).all(id.parse(versionId)) as (ReleaseHealthSource & { decisionJson: string | null })[];
    const key = rows[0]?.decisionJson ? (JSON.parse(rows[0].decisionJson) as { selection?: { offerKey?: unknown } }).selection?.offerKey : null;
    const selected = typeof key === "string" ? rows.filter((row) => row.offerKey === key) : rows.length === 1 ? rows : [];
    return selected.map(({ decisionJson: _, ...source }) => source);
  }

  bindMovieVersionSource(versionId: number, sourceTorrentId: number): void {
    const related = this.db.query(`SELECT 1 FROM movie_versions v
      JOIN movie_acquisition_downloads d ON d.acquisition_id=v.acquisition_id
      WHERE v.id=? AND d.torrent_id=?`).get(id.parse(versionId), id.parse(sourceTorrentId));
    const source = this.source(sourceTorrentId);
    if (!related || !source) throw new Error("Movie source does not belong to the version acquisition");
    const key = versionSourceKey(versionId);
    const previous = this.db.query("SELECT value_json FROM app_state WHERE key=?").get(key) as { value_json: string } | null;
    if (previous) {
      if (JSON.stringify(sourceSchema.parse(JSON.parse(previous.value_json))) !== JSON.stringify(source))
        throw new Error("Movie version already has another pinned source");
      return;
    }
    this.beforeWrite();
    this.db.query("INSERT INTO app_state(key,value_json,updated_at) VALUES(?,?,?)")
      .run(key, JSON.stringify(source), new Date().toISOString());
  }

  currentMovieNuke(targetId: number): { versionId: number; acquisitionId: number; evidence: ReleaseHealthEvidence } | null {
    const target = this.db.query(`SELECT t.active_version_id,v.acquisition_id FROM movie_targets t
      LEFT JOIN movie_versions v ON v.id=t.active_version_id WHERE t.id=?`).get(id.parse(targetId)) as {
      active_version_id: number | null; acquisition_id: number | null;
    } | null;
    if (!target?.active_version_id) return null;
    const sources = this.versionSources(target.active_version_id);
    if (sources.length !== 1) return null;
    const evidence = this.evidence(sources[0].id);
    return evidence && target.acquisition_id ? { versionId: target.active_version_id, acquisitionId: target.acquisition_id, evidence } : null;
  }

  isContentFileBlocked(fileId: string): boolean {
    const versions = this.db.query("SELECT version_id FROM movie_version_files WHERE file_id=?").all(fileId) as { version_id: number }[];
    return versions.some((version) => this.isMovieVersionBlocked(version.version_id));
  }

  isMovieVersionBlocked(versionId: number): boolean {
    const sources = this.versionSources(versionId);
    if (sources.length) return sources.some((source) => this.evidence(source.id));
    // Ambiguous legacy lineage must not launder bytes from a known nuked attempt.
    return (this.db.query(`SELECT d.torrent_id AS id FROM movie_versions v
      JOIN movie_acquisition_downloads d ON d.acquisition_id=v.acquisition_id WHERE v.id=?`)
      .all(id.parse(versionId)) as { id: number }[]).some((source) => this.evidence(source.id));
  }

  activeMovieTargets(torrentId: number): number[] {
    return (this.db.query(`SELECT DISTINCT target.id FROM movie_targets target
      JOIN movie_versions v ON v.id=target.active_version_id JOIN movie_acquisitions a ON a.id=v.acquisition_id
      JOIN movie_acquisition_downloads d ON d.acquisition_id=a.id WHERE d.torrent_id=? ORDER BY target.id`)
      .all(id.parse(torrentId)) as { id: number }[]).filter((target) =>
      this.currentMovieNuke(target.id)?.evidence.source.id === torrentId).map((target) => target.id);
  }

  recordConfirmedNuke(input: ConfirmedReleaseNuke, actorUserId: number): ReleaseHealthEvidence {
    const command = confirmedReleaseNukeSchema.parse(input);
    const actor = this.db.query("SELECT is_admin FROM users WHERE id=?").get(id.parse(actorUserId)) as { is_admin: number } | null;
    if (!actor?.is_admin) throw new Error("Only administrators may confirm a release nuke");
    const source = this.source(command.sourceTorrentId);
    if (!source) throw new Error("Unknown source torrent");
    const previous = this.evidence(source.id);
    const report = previous?.reports.find((entry) => entry.reportId === command.reportId);
    if (report && (report.reason !== command.reason || report.reportedByUserId !== actorUserId))
      throw new Error("Nuke report UUID belongs to another command");
    if (report) return previous!;
    const evidence: ReleaseHealthEvidence = { version: 1, status: "confirmed-nuked", source,
      reports: [...(previous?.reports ?? []), { reportId: command.reportId, reportedByUserId: actorUserId,
        reason: command.reason, reportedAt: new Date().toISOString() }] };
    this.beforeWrite();
    this.db.query(`INSERT INTO app_state(key,value_json,updated_at) VALUES(?,?,?)
      ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at`)
      .run(stateKey(source.id), JSON.stringify(evidence), new Date().toISOString());
    return evidence;
  }
}

/** The caller supplies the authenticated actor, never an actor ID from RPC parameters. */
export function reportConfirmedReleaseNuke(repo: BooksRepo, raw: ConfirmedReleaseNuke, actorUserId: number) {
  const command = confirmedReleaseNukeSchema.parse(raw);
  if (!repo.getUserById(actorUserId)?.is_admin) throw new Error("Only administrators may confirm a release nuke");
  const commandHash = createHash("sha256").update(JSON.stringify([command, actorUserId])).digest("hex");
  return repo.transaction(() => {
    const previous = repo.getJsonState<ReleaseNukeReceipt>(reportKey(command.reportId));
    if (previous) {
      if (previous.commandHash !== commandHash) throw new Error("Nuke report UUID belongs to another command");
      return { ...previous, idempotent: true };
    }
    const evidence = repo.releaseHealth.recordConfirmedNuke(command, actorUserId);
    const replacements: ReleaseNukeReceipt["replacements"] = [];
    for (const targetId of repo.releaseHealth.activeMovieTargets(command.sourceTorrentId)) {
      if (!repo.movies.activeRequestCount(targetId) || repo.retention.get({ namespace: "movie-target", id: String(targetId) })?.desiredPresence === "evicted") continue;
      // A report after promotion must not be consumed by the producing job's final checkpoint.
      const producingAcquisitionId = repo.releaseHealth.currentMovieNuke(targetId)!.acquisitionId;
      const queued = queueMovieAcquisition(repo, targetId, "recovery", undefined, undefined, producingAcquisitionId);
      replacements.push({ targetId, acquisitionId: queued.acquisition.id, jobId: queued.acquisition.job_id!, shared: queued.shared });
      repo.movies.event(targetId, queued.acquisition.id, `release-nuke:${command.reportId}:${targetId}`, "replacing",
        "A release defect was confirmed; finding a replacement while keeping the current movie available");
    }
    const receipt: ReleaseNukeReceipt = { commandHash, evidence, replacements };
    repo.setJsonState(reportKey(command.reportId), receipt);
    return { ...receipt, idempotent: false };
  });
}

export function assessConfirmedNukeReplacement(policy: MovieQualityPolicy, current: MovieFacts, candidate: MovieFacts) {
  const from = evaluateMovieQuality(policy, current), to = evaluateMovieQuality(policy, candidate);
  if (to.eligibility !== "eligible") return { allowed: false, reason: "Replacement does not satisfy identity/cut/language/quality policy" };
  if (from.tier === null || to.tier === null || to.tier < from.tier)
    return { allowed: false, reason: "Replacement would downgrade the current quality tier or its tier is unknown" };
  if (to.tier === from.tier && to.preferenceScore < from.preferenceScore)
    return { allowed: false, reason: "Replacement would downgrade same-tier quality preferences" };
  return { allowed: true, reason: "Eligible non-downgrading replacement for a confirmed defective release" };
}

/** Publication gate: normal upgrades remain normal; equal-tier replacement needs current nuke evidence. */
export function canReplaceConfirmedNuke(db: Database, targetId: number, versionId: number): boolean {
  const health = new ReleaseHealthRepo(db);
  if (health.isMovieVersionBlocked(versionId))
    throw new Error("A confirmed nuked release cannot be published");
  const current = health.currentMovieNuke(targetId);
  if (!current || current.versionId === versionId) return false;
  const target = db.query("SELECT policy_json FROM movie_targets WHERE id=?").get(targetId) as { policy_json: string };
  const facts = (id: number) => movieFactsSchema.parse(JSON.parse((db.query("SELECT facts_json FROM movie_versions WHERE id=?").get(id) as { facts_json: string }).facts_json));
  const assessment = assessConfirmedNukeReplacement(movieQualityPolicySchema.parse(JSON.parse(target.policy_json)), facts(current.versionId), facts(versionId));
  if (!assessment.allowed) throw new Error(assessment.reason);
  return true;
}
