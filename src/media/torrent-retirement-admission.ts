import { randomUUID } from "node:crypto";
import { z } from "zod";
import type { Database } from "bun:sqlite";
import type { BooksRepo } from "../repo";
import { readTransmissionIdentity } from "./managed-transmission";
import { stewardshipBinding } from "./torrent-stewardship";
import { retirementDigest } from "./torrent-retirement-evidence";
import type { RetirementBinding, RetirementConsumerSnapshot, TorrentRetirementFence } from "./torrent-retirement-types";
import type { TorrentRetirementConsumers } from "./torrent-retirement-query";

export type TorrentRecordRef = { clientId: string; infoHash: string };
type AdmissionRepo = Pick<BooksRepo, "transaction" | "getJsonState" | "setJsonState">;
const refSchema = z.object({ clientId: z.string().regex(/^transmission:[a-f0-9-]{36}$/), infoHash: z.string().regex(/^[a-f0-9]{40}$/) }).strict();
const refsSchema = z.array(refSchema).max(5);
const claimSchema = z.object({ operationId: z.string().uuid(), id: z.string().uuid(), generation: z.number().int().positive(),
  bindingDigest: z.string(), configurationDigest: z.string(), consumerRevision: z.string() });
const stateSchema = z.object({
  useRevision: z.number().int().nonnegative(), generation: z.number().int().nonnegative(), retired: z.boolean(),
  users: z.array(z.object({ token: z.string().uuid(), operationId: z.string().uuid() })).max(128), claim: claimSchema.nullable(),
});
const key = (ref: TorrentRecordRef) => "torrent-retirement:admission:v1:" + retirementDigest([ref.clientId, ref.infoHash]);
const indexKey = "torrent-retirement:claims:v1";
function state(repo: AdmissionRepo, ref: TorrentRecordRef) {
  refSchema.parse(ref);
  const raw = repo.getJsonState(key(ref));
  return raw === null ? { useRevision: 0, generation: 0, retired: false, users: [] as { token: string; operationId: string }[], claim: null as z.infer<typeof claimSchema> | null }
    : stateSchema.parse(raw);
}
function claims(repo: AdmissionRepo) { const raw = repo.getJsonState(indexKey); return raw === null ? [] : refsSchema.parse(raw); }

/** Acquire before producer I/O; keep this token until the daemon operation AND binding commit are reconciled. */
export function admitTorrentUse(repo: AdmissionRepo, ref: TorrentRecordRef, operationId: string): string {
  z.string().uuid().parse(operationId);
  return repo.transaction(() => {
    const current = state(repo, ref);
    if (current.claim || current.retired) throw new Error("Torrent record is retiring or retired; implicit reuse is blocked");
    const prior = current.users.find((user) => user.operationId === operationId);
    if (prior) return prior.token;
    if (current.users.length >= 128) throw new Error("Torrent record admission budget exhausted");
    const token = randomUUID(); current.users.push({ token, operationId }); current.useRevision++;
    repo.setJsonState(key(ref), current); return token;
  });
}
/** Call in the same transaction as reserve/reclaim/attach/bindDownload; the async producer also holds its use token. */
export function assertTorrentBindingWritable(repo: AdmissionRepo, ref: TorrentRecordRef, token?: string): void {
  const current = state(repo, ref);
  if (current.claim || current.retired || (token !== undefined && !current.users.some((user) => user.token === token)))
    throw new Error("Torrent binding is fenced against retirement or has no valid producer admission");
}
/** Raw synchronous guard for repositories that only hold Bun:sqlite. No adapter, transaction or network is created. */
export function assertTorrentDatabaseWritable(db: Database, ref: TorrentRecordRef): void {
  refSchema.parse(ref);
  const row = db.query("SELECT value_json FROM app_state WHERE key=?").get(key(ref)) as { value_json: string } | null;
  if (!row) return;
  const current = stateSchema.parse(JSON.parse(row.value_json));
  if (current.claim || current.retired) throw new Error("Torrent record is retiring or retired; admission is blocked");
}
export function assertTorrentAdmitted(db: Database, clientId: string, infoHash: string): void {
  assertTorrentDatabaseWritable(db, { clientId, infoHash });
}
/** Do not call in an unconditional finally after ambiguous I/O. Failed producers retain their token for reconciliation/retry. */
export function finishTorrentUse(repo: AdmissionRepo, ref: TorrentRecordRef, token: string): void {
  repo.transaction(() => {
    assertTorrentBindingWritable(repo, ref, token);
    const current = state(repo, ref);
    current.users = current.users.filter((user) => user.token !== token); current.useRevision++;
    repo.setJsonState(key(ref), current);
  });
}
/** Place inside the settings/identity mutation transaction, before changing relevant roots or private-engine identity. */
export function assertNoPendingTorrentRetirements(repo: AdmissionRepo): void {
  if (claims(repo).some((ref) => state(repo, ref).claim !== null)) throw new Error("Torrent record retirement is unresolved; configuration changes are fenced");
}

/** Concrete bounded orphan predicate, independent of active library/request existence as a scheduling shortcut. */
function domainBindings(repo: BooksRepo, binding: RetirementBinding) {
  const bridge = repo as BooksRepo & { readTorrentRetirementConsumers?: (binding: RetirementBinding) => TorrentRetirementConsumers };
  return bridge.readTorrentRetirementConsumers?.(binding) ?? { complete: false, count: 0, jobIds: [], activePublications: 0 };
}

/** Default durable claim implementation; global '*' excludes worker callbacks, admission tokens exclude async HTTP producers. */
export function createOrphanRetirementFence(repo: BooksRepo, options: {
  globalMaintenanceHeld: () => boolean;
  readIdentityDigest?: () => string;
}): TorrentRetirementFence {
  const refFor = (binding: RetirementBinding) => ({ clientId: binding.clientId, infoHash: binding.hash });
  const inspect = (binding: RetirementBinding): RetirementConsumerSnapshot => {
    const ref = refFor(binding), current = state(repo, ref), settings = repo.readSettings();
    const raw = binding.media === "books" ? repo.bookTorrents.get(binding.hash) : repo.movies.torrent(binding.clientId, binding.hash);
    const actual = raw ? stewardshipBinding(repo, binding.media === "books" ? "books" : "screen", { ...raw }, binding.clientId) : null;
    const domains = domainBindings(repo, binding);
    let complete = domains.complete, activeAcquisitions = 0;
    for (const id of domains.jobIds) {
      try {
        const job = repo.getJob(id);
        if (!job) complete = false;
        else if (["queued", "running"].includes(job.status)) activeAcquisitions++;
      } catch { complete = false; }
    }
    let identityDigest: string | null = null;
    try { identityDigest = options.readIdentityDigest?.() ?? retirementDigest(readTransmissionIdentity()); } catch { /* Unknown identity never authorizes a claim. */ }
    const configurationDigest = retirementDigest([settings?.libraryRoot, settings?.movies.managedRoot, settings?.tv.managedRoot, identityDigest]);
    return { completeInventory: !!identityDigest && !!actual && complete,
      domainBindings: domains.count, activeConsumers: current.users.length, activeAcquisitions, activePublications: domains.activePublications,
      bindingDigest: actual?.bindingDigest ?? "", configurationDigest,
      revision: retirementDigest([actual?.bindingDigest, domains, activeAcquisitions, current.useRevision, configurationDigest]) };
  };
  return {
    inspect,
    claim(binding, operationId, expected) {
      return repo.transaction(() => {
        const ref = refFor(binding), current = state(repo, ref), snapshot = inspect(binding);
        if (!options.globalMaintenanceHeld() || !snapshot.completeInventory || snapshot.domainBindings || snapshot.activeConsumers || snapshot.activeAcquisitions || snapshot.activePublications ||
            snapshot.bindingDigest !== binding.bindingDigest || retirementDigest(snapshot) !== retirementDigest(expected) || current.retired)
          throw new Error("Global orphan retirement claim no longer has complete zero-use proof");
        if (current.claim) {
          if (current.claim.operationId !== operationId) throw new Error("Another retirement owns this torrent record");
          return { id: current.claim.id, generation: current.claim.generation };
        }
        const index = claims(repo);
        if (index.length >= 5) throw new Error("Retirement claim budget exhausted");
        const value = { operationId, id: randomUUID(), generation: current.generation + 1, bindingDigest: binding.bindingDigest,
          configurationDigest: snapshot.configurationDigest, consumerRevision: retirementDigest(snapshot.revision) };
        current.claim = claimSchema.parse(value); current.generation = value.generation;
        repo.setJsonState(key(ref), current); repo.setJsonState(indexKey, [...index, ref]);
        return { id: value.id, generation: value.generation };
      });
    },
    assert(binding, operationId, claim, expected) {
      const current = state(repo, refFor(binding)), actual = inspect(binding);
      if (!options.globalMaintenanceHeld() || !actual.completeInventory || actual.domainBindings || actual.activeConsumers || actual.activeAcquisitions || actual.activePublications ||
          current.retired || current.claim?.id !== claim.id || current.claim.generation !== claim.generation || current.claim.operationId !== operationId ||
          current.claim.bindingDigest !== binding.bindingDigest || current.claim.configurationDigest !== actual.configurationDigest ||
          current.claim.consumerRevision !== retirementDigest(actual.revision) || retirementDigest(actual) !== retirementDigest(expected))
        throw new Error("Torrent retirement admission/consumer/configuration fence changed");
    },
    finish(binding, operationId, claim, outcome) {
      repo.transaction(() => {
        const ref = refFor(binding), current = state(repo, ref);
        if (current.claim?.id !== claim.id || current.claim.generation !== claim.generation || current.claim.operationId !== operationId || current.users.length)
          throw new Error("Torrent retirement claim changed before completion");
        current.claim = null; current.retired = outcome === "retired";
        repo.setJsonState(key(ref), current);
        repo.setJsonState(indexKey, claims(repo).filter((item) => item.clientId !== ref.clientId || item.infoHash !== ref.infoHash));
      });
    },
  };
}
