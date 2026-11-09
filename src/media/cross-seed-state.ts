import { createHash } from "node:crypto";
import type { Database } from "bun:sqlite";
import { z } from "zod";
import type { TorrentRef } from "./torrent-client";
import type { TorrentRecord } from "./torrent-records-schema";
import type { DownloadLocation } from "./torrent-client";
import { immediateTransaction } from "../db-transaction";
import { reuseDigest, type ReusePlan } from "./reuse-plan";

const id = z.number().int().positive().safe();
export const crossSeedScopeSchema = z.object({ media: z.enum(["movie", "tv", "audio", "ebook"]), targetId: id }).strict();
export type CrossSeedScope = z.infer<typeof crossSeedScopeSchema>;
export const isBookCrossSeedScope = (scope: CrossSeedScope): scope is CrossSeedScope & { media: "audio" | "ebook" } =>
  scope.media === "audio" || scope.media === "ebook";
const reuseBudgets = {
  minimumReuseRatio: z.number().min(0.01).max(1).default(0.8),
  maximumExtraBytes: z.number().int().nonnegative().safe().max(10 * 1024 ** 3).default(512 * 1024 ** 2),
};
export const crossSeedPolicySchema = z.union([
  z.object({ version: z.literal(2).default(2), ...reuseBudgets }).strict(),
  // Preserve immutable historical command hashes; execution no longer applies
  // this retired total-size field. New commands default to the uncapped policy.
  z.object({ version: z.literal(1), ...reuseBudgets,
    maximumCandidateBytes: id.max(1024 ** 4).default(100 * 1024 ** 3),
  }).strict(),
]);
export type CrossSeedPolicy = z.infer<typeof crossSeedPolicySchema>;
export const crossSeedCommandSchema = z.object({
  operationId: z.string().uuid(), scope: crossSeedScopeSchema, releaseId: id.optional(),
  selectionCopyId: z.string().uuid().optional(),
  policy: crossSeedPolicySchema.default(() => crossSeedPolicySchema.parse({})),
}).strict().refine(command => Number(command.releaseId !== undefined) + Number(command.selectionCopyId !== undefined) === 1,
  "Provide exactly one retained release or server-owned selected copy");
export type CrossSeedCommand = z.infer<typeof crossSeedCommandSchema>;
export type CrossSeedOutcome = {
  kind: "seeding" | "skipped" | "unsupported" | "stalled" | "incompatible";
  reason: string; at: string; torrentId?: number; infoHash?: string;
  reusedBytes?: number; downloadedBytes?: number; totalBytes?: number;
  /** Verified payload accounting, not network counters (which include retries/overhead). */
  accountingVersion?: 1; sidecarDownloadBytes?: number; contentRepairBytes?: number;
  contentFileCount?: number;
  planId?: string; verification?: "assumed" | "verified";
};
export type CrossSeedReceipt = { command: CrossSeedCommand; commandHash: string; jobId: number; createdAt: string; outcome?: CrossSeedOutcome };
const prefix = "cross-seed:operation:";
const optimisticPrefix = "cross-seed:optimistic:";
const hashSchema = z.string().regex(/^[a-f0-9]{40}$/), planIdSchema = z.string().regex(/^[a-f0-9]{64}$/);
export type OptimisticRegistration = {
  operationId: string; planId: string; ref: TorrentRef; location: DownloadLocation; plan: ReusePlan;
  torrentId: number; status: "monitoring" | "removing" | "removed"; recordedAt: string;
};
export const optimisticRegistrationFits = (input: { planId: string; ref: TorrentRef; location: DownloadLocation; plan: ReusePlan }) =>
  Buffer.byteLength(JSON.stringify(input)) <= 32 * 1024 * 1024;
export const crossSeedStateKey = (id: string) => prefix + z.string().uuid().parse(id);
export const crossSeedRegistrationKey = (ref: TorrentRef) => "cross-seed:registration:" + createHash("sha256").update(JSON.stringify([ref.clientId, ref.nativeId])).digest("hex");
export const crossSeedCommandHash = (command: CrossSeedCommand) => createHash("sha256").update(JSON.stringify(crossSeedCommandSchema.parse(command))).digest("hex");

/** Domain intent/results only. Attempts, sleeps and leases remain in Reflow. */
export class CrossSeedRepo {
  constructor(private db: Database, private beforeWrite: () => void = () => {}) {}
  private save(key: string, value: unknown) {
    this.db.query(`INSERT INTO app_state(key,value_json,updated_at) VALUES(?,?,?)
      ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at`)
      .run(key, JSON.stringify(value), new Date().toISOString());
  }
  isIncompatible(candidateHash: string, planId: string): boolean {
    hashSchema.parse(candidateHash); planIdSchema.parse(planId);
    return !!this.db.query("SELECT 1 FROM app_state WHERE key=?")
      .get("cross-seed:incompatible:" + reuseDigest([candidateHash, planId]));
  }
  markIncompatible(candidateHash: string, planId: string, reason: string): void {
    hashSchema.parse(candidateHash); planIdSchema.parse(planId); z.string().trim().min(1).max(500).parse(reason);
    immediateTransaction(this.db, this.beforeWrite, () => {
      const at = new Date().toISOString();
      this.save("cross-seed:incompatible:" + reuseDigest([candidateHash, planId]), { candidateHash, planId, reason, at });
      const rows = this.db.query(`SELECT value_json FROM app_state WHERE key>=? AND key<?
        AND json_extract(value_json,'$.ref.nativeId')=? AND json_extract(value_json,'$.planId')=?
        AND json_extract(value_json,'$.status')!='removed'`).all(optimisticPrefix, optimisticPrefix + "~", candidateHash, planId) as { value_json: string }[];
      for (const row of rows) {
        const registration = JSON.parse(row.value_json) as OptimisticRegistration;
        this.save(optimisticPrefix + registration.operationId, { ...registration, status: "removing" });
        const receipt = this.receipt(registration.operationId);
        if (receipt) this.save(crossSeedStateKey(registration.operationId), { ...receipt, outcome: {
          ...receipt.outcome, kind: "incompatible", reason, at, planId, verification: "assumed",
          torrentId: registration.torrentId, infoHash: candidateHash
        } });
      }
    });
  }
  recordOptimistic(operationId: string, input: { planId: string; ref: TorrentRef; location: DownloadLocation; plan: ReusePlan }): void {
    z.string().uuid().parse(operationId); planIdSchema.parse(input.planId); hashSchema.parse(input.ref.nativeId);
    if (input.planId !== input.plan.id || input.plan.candidate.identity.v1 !== input.ref.nativeId ||
      input.plan.files.length !== input.plan.candidate.files.length || !input.plan.files.length ||
      input.plan.files.some((file, index) => !file.source || file.source.size !== file.size ||
        file.candidatePath !== input.plan.candidate.files[index]!.path || file.size !== input.plan.candidate.files[index]!.size) ||
      !optimisticRegistrationFits(input)) throw new Error("Optimistic registration needs a bounded fully mapped plan");
    immediateTransaction(this.db, this.beforeWrite, () => {
      const receipt = this.receipt(operationId);
      const record = this.db.query("SELECT * FROM media_torrents WHERE client_id=? AND native_id=?")
        .get(input.ref.clientId, input.ref.nativeId) as TorrentRecord | null;
      if (!receipt || !record || this.registrationOwner(input.ref) !== operationId || this.owns(receipt.command.scope, input.ref.clientId, input.ref.nativeId) ||
        reuseDigest(JSON.parse(record.location_json)) !== reuseDigest(input.location)) throw new Error("Optimistic registration is not exclusively owned");
      const previous = this.optimisticRegistration(operationId);
      if (previous) {
        if (reuseDigest({ ...previous, status: "monitoring", recordedAt: "" }) !== reuseDigest({ ...input,
          operationId, torrentId: record.id, status: "monitoring", recordedAt: "" })) throw new Error("Optimistic registration intent changed");
        return;
      }
      this.save(optimisticPrefix + operationId, { ...input, operationId, torrentId: record.id,
        status: this.isIncompatible(input.ref.nativeId, input.planId) ? "removing" : "monitoring", recordedAt: new Date().toISOString() });
    });
  }
  optimisticRegistration(operationId: string): OptimisticRegistration | null {
    z.string().uuid().parse(operationId);
    const row = this.db.query("SELECT value_json FROM app_state WHERE key=?").get(optimisticPrefix + operationId) as { value_json: string } | null;
    return row ? JSON.parse(row.value_json) : null;
  }
  optimisticRegistrations(limit = 25, afterOperationId = ""): OptimisticRegistration[] {
    z.number().int().min(1).max(25).parse(limit);
    if (afterOperationId) z.string().uuid().parse(afterOperationId);
    return (this.db.query(`SELECT value_json FROM app_state WHERE key>? AND key<?
      AND json_extract(value_json,'$.status')!='removed' ORDER BY key LIMIT ?`)
      .all(optimisticPrefix + afterOperationId, optimisticPrefix + "~", limit) as { value_json: string }[]).map(row => JSON.parse(row.value_json));
  }
  ownsOptimisticRegistration(operationId: string): boolean {
    const registration = this.optimisticRegistration(operationId);
    if (!registration || registration.status === "removed" || this.registrationOwner(registration.ref) !== operationId) return false;
    const row = this.db.query("SELECT * FROM media_torrents WHERE id=?").get(registration.torrentId) as TorrentRecord | null;
    const receipt = this.receipt(operationId);
    const tag = receipt?.command.scope.media === "tv" ? "Podible TV" : receipt?.command.scope.media === "movie" ? "Podible Movies" : "Podible Books";
    if (!row || !receipt || row.backend !== "transmission" || row.ownership_tag !== tag ||
      row.client_id !== registration.ref.clientId || row.native_id !== registration.ref.nativeId ||
      reuseDigest(JSON.parse(row.location_json)) !== reuseDigest(registration.location)) return false;
    return !this.db.query(`SELECT 1 FROM book_torrent_bindings WHERE torrent_id=?
      UNION ALL SELECT 1 FROM movie_acquisition_downloads WHERE torrent_id=?
      UNION ALL SELECT 1 FROM tv_acquisition_downloads WHERE torrent_id=? LIMIT 1`)
      .get(registration.torrentId, registration.torrentId, registration.torrentId);
  }
  completeOptimisticRemoval(operationId: string): void {
    immediateTransaction(this.db, this.beforeWrite, () => {
      const registration = this.optimisticRegistration(operationId);
      if (!registration || registration.status === "removed") return;
      if (registration.status !== "removing" || !this.isIncompatible(registration.ref.nativeId, registration.planId) ||
        this.registrationOwner(registration.ref) !== operationId) throw new Error("Optimistic removal ownership changed");
      const row = this.db.query("SELECT * FROM media_torrents WHERE id=?").get(registration.torrentId) as TorrentRecord | null;
      if (row && (row.client_id !== registration.ref.clientId || row.native_id !== registration.ref.nativeId ||
        reuseDigest(JSON.parse(row.location_json)) !== reuseDigest(registration.location))) throw new Error("Optimistic removal registry changed");
      const bound = this.db.query(`SELECT 1 FROM book_torrent_bindings WHERE torrent_id=?
        UNION ALL SELECT 1 FROM movie_acquisition_downloads WHERE torrent_id=?
        UNION ALL SELECT 1 FROM tv_acquisition_downloads WHERE torrent_id=? LIMIT 1`)
        .get(registration.torrentId, registration.torrentId, registration.torrentId);
      if (bound) throw new Error("Primary torrent cannot be removed by optimistic monitoring");
      if (row) this.db.query("DELETE FROM media_torrents WHERE id=?").run(registration.torrentId);
      this.db.query("DELETE FROM app_state WHERE key=?").run(crossSeedRegistrationKey(registration.ref));
      this.save(optimisticPrefix + operationId, { ...registration, status: "removed" });
    });
  }
  registrationOwner(ref: TorrentRef): string | null {
    const row = this.db.query("SELECT value_json FROM app_state WHERE key=?").get(crossSeedRegistrationKey(ref)) as { value_json: string } | null;
    return row ? z.object({ operationId: z.string().uuid() }).parse(JSON.parse(row.value_json)).operationId : null;
  }
  receipt(operationId: string): CrossSeedReceipt | null {
    const row = this.db.query("SELECT value_json FROM app_state WHERE key=?").get(crossSeedStateKey(operationId)) as { value_json: string } | null;
    return row ? JSON.parse(row.value_json) : null;
  }
  attemptsForRelease(scope: CrossSeedScope, releaseId: number): CrossSeedReceipt[] {
    crossSeedScopeSchema.parse(scope); id.parse(releaseId);
    return (this.db.query(`SELECT value_json FROM app_state WHERE key>=? AND key<?
      AND json_extract(value_json,'$.command.scope.media')=?
      AND json_extract(value_json,'$.command.scope.targetId')=?
      AND json_extract(value_json,'$.command.releaseId')=?`)
      .all(prefix, prefix + "~", scope.media, scope.targetId, releaseId) as { value_json: string }[])
      .map(row => JSON.parse(row.value_json));
  }
  hasRegistration(receipt: CrossSeedReceipt): boolean {
    if (receipt.outcome?.torrentId === undefined) return false;
    const ref = this.db.query("SELECT client_id,native_id FROM media_torrents WHERE id=?")
      .get(receipt.outcome.torrentId) as { client_id: string; native_id: string } | null;
    return !!ref && this.registrationOwner({ clientId: ref.client_id, nativeId: ref.native_id }) === receipt.command.operationId;
  }
  receipts(limit = 25): CrossSeedReceipt[] {
    z.number().int().min(1).max(100).parse(limit);
    return (this.db.query("SELECT value_json FROM app_state WHERE key>=? AND key<? ORDER BY updated_at DESC,key LIMIT ?")
      .all(prefix, prefix + "~", limit) as { value_json: string }[]).map(row => JSON.parse(row.value_json));
  }
  owns(scope: CrossSeedScope, clientId: string, nativeId: string): boolean {
    crossSeedScopeSchema.parse(scope);
    if (isBookCrossSeedScope(scope)) return !!this.db.query(`SELECT 1 FROM media_torrents t
      JOIN book_torrent_bindings b ON b.torrent_id=t.id JOIN releases r ON r.id=b.release_id
      WHERE b.book_id=? AND r.book_id=b.book_id AND r.media_type=? AND t.client_id=? AND t.native_id=? LIMIT 1`)
      .get(scope.targetId, scope.media, clientId, nativeId);
    return !!this.db.query(`SELECT 1 FROM media_torrents t
      JOIN ${scope.media}_acquisition_downloads d ON d.torrent_id=t.id
      JOIN ${scope.media}_acquisitions a ON a.id=d.acquisition_id
      WHERE a.target_id=? AND t.client_id=? AND t.native_id=? LIMIT 1`)
      .get(scope.targetId, clientId, nativeId);
  }
  hasFiles(scope: CrossSeedScope): boolean {
    crossSeedScopeSchema.parse(scope);
    if (isBookCrossSeedScope(scope)) return !!this.db.query(`SELECT 1 FROM asset_files f
      JOIN assets a ON a.id=f.asset_id JOIN manifestations m ON m.id=a.manifestation_id AND m.book_id=a.book_id
      WHERE a.book_id=? AND m.kind=? LIMIT 1`).get(scope.targetId, scope.media);
    return !!this.db.query(`SELECT 1 FROM ${scope.media}_versions v JOIN ${scope.media}_version_files f ON f.version_id=v.id
      JOIN ${scope.media}_acquisitions a ON a.id=v.acquisition_id
      WHERE a.target_id=? AND NOT EXISTS(SELECT 1 FROM media_content_losses l WHERE l.file_id=f.file_id) LIMIT 1`).get(scope.targetId);
  }
  contentRevision(scope: CrossSeedScope): string {
    crossSeedScopeSchema.parse(scope);
    if (isBookCrossSeedScope(scope)) {
      const rows = this.db.query(`SELECT f.id,f.path,f.size,f.updated_at FROM asset_files f
        JOIN assets a ON a.id=f.asset_id JOIN manifestations m ON m.id=a.manifestation_id AND m.book_id=a.book_id
        WHERE a.book_id=? AND m.kind=? ORDER BY f.id`).all(scope.targetId, scope.media);
      return createHash("sha256").update(JSON.stringify(rows)).digest("hex");
    }
    const row = this.db.query(`SELECT COALESCE(MAX(v.id),0) AS newest, COUNT(DISTINCT f.file_id) AS files
      FROM ${scope.media}_versions v JOIN ${scope.media}_version_files f ON f.version_id=v.id
      JOIN ${scope.media}_acquisitions a ON a.id=v.acquisition_id
      WHERE a.target_id=? AND NOT EXISTS(SELECT 1 FROM media_content_losses l WHERE l.file_id=f.file_id)`)
      .get(scope.targetId) as { newest: number; files: number };
    return `${row.newest}:${row.files}`;
  }
  files(scope: CrossSeedScope, sizes: number[]) {
    return [...this.filePages(scope, sizes)].flat();
  }
  *filePages(scope: CrossSeedScope, sizes: number[]) {
    crossSeedScopeSchema.parse(scope);
    const unique = [...new Set(z.array(z.number().int().nonnegative().safe()).parse(sizes))];
    for (let start = 0; start < unique.length; start += 250) {
      const batch = unique.slice(start, start + 250), params = batch.map(() => "?").join(",");
      type File = { id: string; relative_path: string; size_bytes: number };
      const book = isBookCrossSeedScope(scope);
      const query = this.db.query<File, (number | string)[]>(book ? `SELECT 'book-file:' || f.id AS id,
      f.path AS relative_path,f.size AS size_bytes FROM asset_files f
      JOIN assets a ON a.id=f.asset_id JOIN manifestations m ON m.id=a.manifestation_id AND m.book_id=a.book_id
      WHERE a.book_id=? AND m.kind=? AND f.size IN (${params}) AND f.id>?
      ORDER BY f.id LIMIT 250` : `SELECT DISTINCT f.id,f.relative_path,f.size_bytes FROM managed_content_files f
      JOIN ${scope.media}_version_files vf ON vf.file_id=f.id
      JOIN ${scope.media}_versions v ON v.id=vf.version_id
      JOIN ${scope.media}_acquisitions a ON a.id=v.acquisition_id
      WHERE a.target_id=? AND f.size_bytes IN (${params}) AND f.id>?
      AND NOT EXISTS(SELECT 1 FROM media_content_losses l WHERE l.file_id=f.id)
      ORDER BY f.id LIMIT 250`);
      let cursor: string | number = book ? 0 : "";
      for (;;) {
        const rows = book ? query.all(scope.targetId, scope.media, ...batch, cursor) : query.all(scope.targetId, ...batch, cursor);
        if (!rows.length) break;
        yield rows;
        if (rows.length < 250) break;
        cursor = book ? Number(rows.at(-1)!.id.slice("book-file:".length)) : rows.at(-1)!.id;
      }
    }
  }
  sourceDownloads(scope: CrossSeedScope): TorrentRecord[] {
    return [...this.sourceDownloadPages(scope)].flat();
  }
  *sourceDownloadPages(scope: CrossSeedScope) {
    crossSeedScopeSchema.parse(scope);
    const book = isBookCrossSeedScope(scope);
    const query = this.db.query<TorrentRecord, (number | string)[]>(book ? `SELECT DISTINCT t.* FROM media_torrents t
      JOIN book_torrent_bindings b ON b.torrent_id=t.id JOIN releases r ON r.id=b.release_id
      WHERE b.book_id=? AND r.book_id=b.book_id AND r.media_type=? AND t.id>? ORDER BY t.id LIMIT 128`
      : `SELECT DISTINCT t.* FROM media_torrents t
      JOIN ${scope.media}_acquisition_downloads d ON d.torrent_id=t.id
      JOIN ${scope.media}_acquisitions a ON a.id=d.acquisition_id
      WHERE a.target_id=? AND t.id>? ORDER BY t.id LIMIT 128`);
    let cursor = 0;
    for (;;) {
      const rows = book ? query.all(scope.targetId, scope.media, cursor) : query.all(scope.targetId, cursor);
      if (!rows.length) return;
      yield rows;
      if (rows.length < 128) return;
      cursor = rows.at(-1)!.id;
    }
  }
  *sourceProofs(scope: CrossSeedScope, torrentId: number) {
    crossSeedScopeSchema.parse(scope);
    if (isBookCrossSeedScope(scope)) return;
    const query = this.db.query<{ job_id: number | null; source_json: string | null }, [number, number, number]>(`SELECT DISTINCT a.job_id,json_extract(v.facts_json,'$.source') AS source_json
      FROM ${scope.media}_acquisition_downloads d JOIN ${scope.media}_acquisitions a ON a.id=d.acquisition_id
      LEFT JOIN ${scope.media}_versions v ON v.acquisition_id=a.id
      WHERE a.target_id=? AND d.torrent_id=? ORDER BY a.job_id,source_json LIMIT 32 OFFSET ?`);
    for (let offset = 0; ; offset += 32) {
      let count = 0;
      for (const row of query.iterate(scope.targetId, torrentId, offset)) { count++; yield row; }
      if (count < 32) return;
    }
  }
}
