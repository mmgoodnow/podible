import { createHash } from "node:crypto";
import type { Database } from "bun:sqlite";
import { z } from "zod";
import type { TorrentRef } from "./torrent-client";

const id = z.number().int().positive().safe();
export const crossSeedScopeSchema = z.object({ media: z.enum(["movie", "tv"]), targetId: id }).strict();
export type CrossSeedScope = z.infer<typeof crossSeedScopeSchema>;
export const crossSeedPolicySchema = z.object({
  version: z.literal(1).default(1),
  minimumReuseRatio: z.number().min(0.01).max(1).default(0.8),
  maximumExtraBytes: z.number().int().nonnegative().safe().max(10 * 1024 ** 3).default(512 * 1024 ** 2),
  maximumCandidateBytes: id.max(1024 ** 4).default(100 * 1024 ** 3),
}).strict();
export type CrossSeedPolicy = z.infer<typeof crossSeedPolicySchema>;
export const crossSeedCommandSchema = z.object({
  operationId: z.string().uuid(), scope: crossSeedScopeSchema, releaseId: id,
  policy: crossSeedPolicySchema.default(() => crossSeedPolicySchema.parse({})),
}).strict();
export type CrossSeedCommand = z.infer<typeof crossSeedCommandSchema>;
export type CrossSeedOutcome = {
  kind: "seeding" | "skipped" | "unsupported" | "stalled";
  reason: string; at: string; torrentId?: number; infoHash?: string;
  reusedBytes?: number; downloadedBytes?: number; totalBytes?: number;
};
export type CrossSeedReceipt = { command: CrossSeedCommand; commandHash: string; jobId: number; createdAt: string; outcome?: CrossSeedOutcome };
const prefix = "cross-seed:operation:";
export const crossSeedStateKey = (id: string) => prefix + z.string().uuid().parse(id);
export const crossSeedRegistrationKey = (ref: TorrentRef) => "cross-seed:registration:" + createHash("sha256").update(JSON.stringify([ref.clientId, ref.nativeId])).digest("hex");
export const crossSeedCommandHash = (command: CrossSeedCommand) => createHash("sha256").update(JSON.stringify(crossSeedCommandSchema.parse(command))).digest("hex");

/** Domain intent/results only. Attempts, sleeps and leases remain in Reflow. */
export class CrossSeedRepo {
  constructor(private db: Database) {}
  registrationOwner(ref: TorrentRef): string | null {
    const row = this.db.query("SELECT value_json FROM app_state WHERE key=?").get(crossSeedRegistrationKey(ref)) as { value_json: string } | null;
    return row ? z.object({ operationId: z.string().uuid() }).parse(JSON.parse(row.value_json)).operationId : null;
  }
  receipt(operationId: string): CrossSeedReceipt | null {
    const row = this.db.query("SELECT value_json FROM app_state WHERE key=?").get(crossSeedStateKey(operationId)) as { value_json: string } | null;
    return row ? JSON.parse(row.value_json) : null;
  }
  receipts(limit = 25): CrossSeedReceipt[] {
    z.number().int().min(1).max(100).parse(limit);
    return (this.db.query("SELECT value_json FROM app_state WHERE key>=? AND key<? ORDER BY updated_at DESC,key LIMIT ?")
      .all(prefix, prefix + "~", limit) as { value_json: string }[]).map(row => JSON.parse(row.value_json));
  }
  owns(scope: CrossSeedScope, clientId: string, nativeId: string): boolean {
    crossSeedScopeSchema.parse(scope);
    return !!this.db.query(`SELECT 1 FROM media_torrents t
      JOIN ${scope.media}_acquisition_downloads d ON d.torrent_id=t.id
      JOIN ${scope.media}_acquisitions a ON a.id=d.acquisition_id
      WHERE a.target_id=? AND t.client_id=? AND t.native_id=? LIMIT 1`)
      .get(scope.targetId, clientId, nativeId);
  }
  hasFiles(scope: CrossSeedScope): boolean {
    crossSeedScopeSchema.parse(scope);
    return !!this.db.query(`SELECT 1 FROM ${scope.media}_versions v JOIN ${scope.media}_version_files f ON f.version_id=v.id
      JOIN ${scope.media}_acquisitions a ON a.id=v.acquisition_id
      WHERE a.target_id=? AND NOT EXISTS(SELECT 1 FROM media_content_losses l WHERE l.file_id=f.file_id) LIMIT 1`).get(scope.targetId);
  }
  files(scope: CrossSeedScope, sizes: number[]) {
    crossSeedScopeSchema.parse(scope);
    z.array(z.number().int().nonnegative().safe()).max(2000).parse(sizes);
    if (!sizes.length) return [];
    return this.db.query(`SELECT DISTINCT f.id,f.relative_path,f.size_bytes FROM managed_content_files f
      JOIN ${scope.media}_version_files vf ON vf.file_id=f.id
      JOIN ${scope.media}_versions v ON v.id=vf.version_id
      JOIN ${scope.media}_acquisitions a ON a.id=v.acquisition_id
      WHERE a.target_id=? AND f.size_bytes IN (${sizes.map(() => "?").join(",")})
      AND NOT EXISTS(SELECT 1 FROM media_content_losses l WHERE l.file_id=f.id)
      ORDER BY f.created_at,f.id LIMIT 2001`).all(scope.targetId, ...sizes) as { id: string; relative_path: string; size_bytes: number }[];
  }
}
