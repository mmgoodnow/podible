import { z } from "zod";
import type { BooksRepo } from "../repo";
import type { JobRow } from "../app-types";
import type { WorkerContext } from "../worker/context";
import { stewardshipBinding, stewardshipStatusPage, stewardshipPageTitles, stewardshipConnection, verifyStewardshipDirectory } from "./torrent-stewardship";
import { qualifiedRetirementEvidence, readRetirementEvidence, retirementDigest } from "./torrent-retirement-evidence";
import { eligibleDaemonRecord, privateRetirementDaemon, type RetirementDaemon, type RetirementDaemonSnapshot } from "./torrent-retirement-transmission";
import { createOrphanRetirementFence } from "./torrent-retirement-admission";
import {
  retirementIntentSchema, retirementIntentViewSchema, retirementPreviewParamsSchema, retirementPreviewSchema,
  type RetirementBinding, type RetirementConsumerSnapshot, type RetirementIntent, type TorrentRetirementFence,
} from "./torrent-retirement-types";

const prefix = "torrent-retirement:intent:v1:";
const activeKey = (key: string) => "torrent-retirement:binding:v1:" + key;
const pendingKey = "torrent-retirement:pending:v1";
export type TorrentRetirementAdapters = { fence?: TorrentRetirementFence; daemon?: RetirementDaemon | null;
  now?: () => number; readIdentityDigest?: () => string;
  afterCheckpoint?: (stage: "intent-committed" | "dispatch-committed" | "remove-returned") => Promise<void> };
export type RetirementLocator = { source: "books" | "screen"; clientId: string; infoHash: string };
const locatorSchema = z.object({ source: z.enum(["books", "screen"]), clientId: z.string().regex(/^transmission:[a-f0-9-]{36}$/),
  infoHash: z.string().regex(/^[a-f0-9]{40}$/), operationId: z.string().uuid() }).strict();
const consumerSchema = z.object({ completeInventory: z.boolean(), domainBindings: z.number().int().nonnegative(),
  activeConsumers: z.number().int().nonnegative(), activeAcquisitions: z.number().int().nonnegative(), activePublications: z.number().int().nonnegative(),
  bindingDigest: z.string(), configurationDigest: z.string().regex(/^[a-f0-9]{64}$/), revision: z.string().min(1) });
const view = (intent: RetirementIntent) => retirementIntentViewSchema.parse({ ...intent, recordOnly: true, filesDeleted: false });
function findBinding(repo: BooksRepo, locator: RetirementLocator): RetirementBinding | null {
  const row = locator.source === "books" ? repo.bookTorrents.get(locator.infoHash) : repo.movies.torrent(locator.clientId, locator.infoHash);
  return row ? stewardshipBinding(repo, locator.source, { ...row }, locator.clientId) : null;
}
function consumersClear(snapshot: RetirementConsumerSnapshot, binding: RetirementBinding) {
  if (!snapshot.completeInventory || snapshot.domainBindings !== 0 || snapshot.activeConsumers !== 0 ||
      snapshot.activeAcquisitions !== 0 || snapshot.activePublications !== 0 || snapshot.bindingDigest !== binding.bindingDigest)
    throw new Error("Orphan-only consumer inventory is incomplete or not empty");
}
function assertSameDaemon(intent: RetirementIntent, snapshot: RetirementDaemonSnapshot) {
  if (intent.clientId !== snapshot.clientId || intent.daemonIdentityDigest !== snapshot.identityDigest ||
      intent.daemonSessionDigest !== snapshot.sessionDigest || (snapshot.record && snapshot.record.id !== intent.daemonRecordId))
    throw new Error("Private daemon identity/session/record changed; removal is not replayed");
}
export class TorrentRetirement {
  constructor(private repo: BooksRepo, private adapters: TorrentRetirementAdapters = {}) {}
  intent(operationId: string) {
    const parsed = retirementIntentSchema.safeParse(this.repo.getJsonState(prefix + operationId));
    return parsed.success ? parsed.data : null;
  }
  get(operationId: string) { const intent = this.intent(operationId); return intent ? view(intent) : null; }
  pending() {
    const parsed = z.array(z.string().uuid()).max(5).safeParse(this.repo.getJsonState(pendingKey));
    return parsed.success ? parsed.data.flatMap((id) => { const intent = this.intent(id); return intent && !["retired", "aborted"].includes(intent.phase) ? [intent] : []; }) : [];
  }
  preview(input: z.input<typeof retirementPreviewParamsSchema> = {}) {
    const params = retirementPreviewParamsSchema.parse(input), now = (this.adapters.now ?? Date.now)();
    const fence = this.adapters.fence ?? createOrphanRetirementFence(this.repo, { globalMaintenanceHeld: () => false, readIdentityDigest: this.adapters.readIdentityDigest });
    const clientId = this.adapters.daemon === null ? null : this.adapters.daemon?.clientId ?? stewardshipConnection({})?.clientId ?? null;
    const current = stewardshipStatusPage(this.repo, params.source, params.offset, params.limit, clientId);
    const titles = stewardshipPageTitles(this.repo, current);
    return retirementPreviewSchema.parse({ ...params, nextOffset: current.nextOffset, automaticConfigured: !!clientId,
      sourceRows: current.sourceRows, scannedCount: current.scannedCount, excludedCount: current.excludedCount,
      recordOnly: true, filesDeleted: false, items: current.bindings.map((binding) => {
        const qualified = qualifiedRetirementEvidence(readRetirementEvidence(this.repo, binding.key), binding.bindingDigest, now);
        const blockers: string[] = [];
        let consumer: RetirementConsumerSnapshot | null = null;
        if (!qualified) blockers.push("Three complete, all-tracker unregistered announcements spaced at least 15 minutes apart with known, nonregressing seed counters are not proven.");
        {
          try { consumer = consumerSchema.parse(fence.inspect(binding)); consumersClear(consumer, binding); }
          catch { blockers.push("Complete zero-domain-binding/consumer/acquisition/publication proof is unavailable."); }
        }
        const active = this.repo.getJsonState<string>(activeKey(binding.key));
        if (active) blockers.push("A retirement intent or retired binding tombstone already exists.");
        const intent = active ? this.intent(active) : null;
        const eligible = blockers.length === 0;
        const disposition = eligible ? "candidate" : intent?.phase === "retired" ? "complete" : intent && intent.phase !== "aborted" ? "pending"
          : consumer?.completeInventory && (consumer.domainBindings > 0 || consumer.activeConsumers > 0 || consumer.activeAcquisitions > 0 || consumer.activePublications > 0) ? "protected" : "unknown";
        const explanation = disposition === "candidate" ? "Saved evidence qualifies this unused record for fresh background verification; removal has not been confirmed."
          : disposition === "protected" ? "Existing library references or active work protect this record."
            : disposition === "pending" ? "A retirement operation already exists; do not start a second operation."
              : disposition === "complete" ? "Record retirement was already confirmed; its retained binding history is not a new candidate."
                : "Required tracker or complete ownership/use evidence could not be verified. No retirement is authorized by this preview.";
        const ref = current.titleIds.get(binding.key), metadata = ref ? titles.get(`${ref.source}:${ref.id}`) : null;
        return { bindingKey: binding.key, media: binding.media, label: metadata?.titles[0]?.title ?? "Unavailable title",
          technicalLabel: binding.label, titles: metadata?.titles ?? [], disposition, explanation, evidenceQualified: qualified,
          eligible, blockers };
      }) });
  }
  /** The concrete default may remove only under owned durable execution and the participating global writer fence. */
  async execute(ctx: WorkerContext, job: JobRow, raw: RetirementLocator & { operationId: string }) {
    const input = locatorSchema.parse(raw);
    if (!ctx.execution || ctx.repo !== this.repo || !ctx.activeLocks?.has("*")) throw new Error("Record retirement requires owned durable execution in the global maintenance lane");
    const active = () => ctx.execution!.assertActive(); active();
    let intent = this.intent(input.operationId);
    if (intent && (intent.clientId !== input.clientId || intent.infoHash !== input.infoHash || intent.source !== input.source))
      throw new Error("Retirement operation belongs to another binding");
    if (intent && ["retired", "aborted"].includes(intent.phase)) return { status: intent.phase, intent: view(intent), blockers: [] };
    const fence = this.adapters.fence ?? createOrphanRetirementFence(this.repo, { globalMaintenanceHeld: () => ctx.activeLocks?.has("*") === true,
      readIdentityDigest: this.adapters.readIdentityDigest });
    const binding = findBinding(this.repo, input);
    if (!binding || (intent && (intent.bindingKey !== binding.key || intent.bindingDigest !== binding.bindingDigest)))
      return { status: "blocked", intent: intent ? view(intent) : null, blockers: ["binding-or-configuration-changed"] };
    let daemon: RetirementDaemon;
    if (this.adapters.daemon === null) return { status: "blocked", intent: intent ? view(intent) : null, blockers: ["private-daemon-unavailable"] };
    try { daemon = this.adapters.daemon ?? privateRetirementDaemon(); }
    catch { return { status: "blocked", intent: intent ? view(intent) : null, blockers: ["private-daemon-unavailable"] }; }
    if (daemon.clientId !== binding.clientId) return { status: "blocked", intent: intent ? view(intent) : null, blockers: ["foreign-daemon"] };
    const now = () => (this.adapters.now ?? Date.now)();
    const inspectConsumers = () => { const snapshot = consumerSchema.parse(fence.inspect(binding)); consumersClear(snapshot, binding); return snapshot; };
    const originCancelled = (value: RetirementIntent) => this.repo.isJobCancellationRequested(value.jobId) || this.repo.getJob(value.jobId)?.status === "cancelled";
    const assertIntent = (value: RetirementIntent) => {
      active(); const current = findBinding(this.repo, input);
      if (!current || current.bindingDigest !== value.bindingDigest) throw new Error("Retirement binding changed");
      const consumer = inspectConsumers();
      if (retirementDigest(consumer.revision) !== value.consumerRevision || consumer.configurationDigest !== value.configurationDigest) throw new Error("Retirement consumer/configuration revision changed");
      fence.assert(binding, value.operationId, { id: value.fenceId, generation: value.fenceGeneration }, consumer);
    };
    const transition = (value: RetirementIntent, phase: RetirementIntent["phase"], reason: string) => this.repo.transaction(() => {
      assertIntent(value);
      const stored = this.intent(value.operationId);
      if (!stored || stored.phase !== value.phase) throw new Error("Retirement phase changed");
      const next = retirementIntentSchema.parse({ ...stored, phase, updatedAt: new Date(now()).toISOString(), reason });
      this.repo.setJsonState(prefix + next.operationId, next); return next;
    });
    const finish = (value: RetirementIntent, phase: "retired" | "aborted", reason: string) => this.repo.transaction(() => {
      const next = transition(value, phase, reason);
      fence.finish(binding, next.operationId, { id: next.fenceId, generation: next.fenceGeneration }, phase === "retired" ? "retired" : "aborted-before-dispatch");
      if (phase === "aborted") this.repo.setJsonState(activeKey(binding.key), null);
      this.repo.setJsonState(pendingKey, this.pending().map((value) => value.operationId));
      return next;
    });
    if (intent?.phase === "planned" && originCancelled(intent)) {
      intent = finish(intent, "aborted", "Origin execution cancelled before any remove dispatch.");
      return { status: "aborted", intent: view(intent), blockers: [] };
    }
    active(); await verifyStewardshipDirectory(this.repo, binding); active();
    const snapshot = await daemon.read(binding); active();
    if (intent) {
      assertIntent(intent);
      if (intent.phase === "planned" && intent.clientId === snapshot.clientId && intent.daemonIdentityDigest === snapshot.identityDigest &&
          intent.daemonSessionDigest !== snapshot.sessionDigest) {
        intent = finish(intent, "aborted", "Private daemon session changed before any remove dispatch; original record and files were not changed.");
        return { status: "aborted", intent: view(intent), blockers: [] };
      }
      assertSameDaemon(intent, snapshot);
      if (!snapshot.record && intent.phase !== "planned") {
        intent = finish(intent, "retired", "Exact record absence confirmed after record-only dispatch; files and binding history were retained.");
        return { status: "retired", intent: view(intent), blockers: [] };
      }
      if (originCancelled(intent)) return { status: "pending", intent: view(intent), blockers: ["cancelled-dispatch-reconciliation-only"] };
    }
    const evidence = readRetirementEvidence(this.repo, binding.key);
    if (!qualifiedRetirementEvidence(evidence, binding.bindingDigest, now()))
      return { status: "blocked", intent: intent ? view(intent) : null, blockers: ["all-tracker-evidence-not-qualified"] };
    const proof = eligibleDaemonRecord(binding, snapshot, now());
    if (proof.trackers.trackerSetDigest !== evidence!.trackerSetDigest || proof.row.secondsSeeding < evidence!.actualSeedSeconds! ||
        proof.trackers.trackers.some((tracker, index) => tracker.announcedAt < evidence!.samples.at(-1)!.trackers[index]!.announcedAt))
      return { status: "blocked", intent: intent ? view(intent) : null, blockers: ["current-daemon-evidence-changed"] };
    if (!intent) {
      const consumer = inspectConsumers();
      intent = this.repo.transaction(() => {
        active(); const current = findBinding(this.repo, input);
        if (!current || current.bindingDigest !== binding.bindingDigest || this.repo.getJsonState(activeKey(binding.key)))
          throw new Error("Binding changed or retirement already claimed");
        const fresh = inspectConsumers();
        if (retirementDigest(fresh) !== retirementDigest(consumer)) throw new Error("Consumer proof changed before retirement claim");
        const pending = this.pending();
        if (pending.length >= 5) throw new Error("Outstanding retirement reconciliation budget exhausted");
        const claim = fence.claim(binding, input.operationId, fresh);
        const value = retirementIntentSchema.parse({ ...input, bindingKey: binding.key, bindingDigest: binding.bindingDigest,
          phase: "planned", createdAt: new Date(now()).toISOString(), updatedAt: new Date(now()).toISOString(),
          daemonSessionDigest: snapshot.sessionDigest, daemonIdentityDigest: snapshot.identityDigest, daemonRecordId: proof.row.id,
          proofDigest: retirementDigest([evidence, snapshot, fresh]), consumerRevision: retirementDigest(fresh.revision), configurationDigest: fresh.configurationDigest,
          fenceId: claim.id, fenceGeneration: claim.generation, jobId: job.id, executionKey: ctx.execution!.key,
          reason: "Durable orphan-only record retirement intent; files must not be deleted." });
        this.repo.setJsonState(prefix + value.operationId, value); this.repo.setJsonState(activeKey(binding.key), value.operationId);
        this.repo.setJsonState(pendingKey, [...pending.map((item) => item.operationId), value.operationId]);
        return value;
      });
      await this.adapters.afterCheckpoint?.("intent-committed");
    }
    assertIntent(intent);
    intent = transition(intent, "dispatching", "Record-only remove may be in flight; writer fence must remain closed until absence is confirmed.");
    await this.adapters.afterCheckpoint?.("dispatch-committed");
    assertIntent(intent);
    try { await daemon.removeRecord(binding, snapshot, () => assertIntent(intent!)); }
    catch {
      active(); intent = transition(intent, "uncertain", "Remove acknowledgement unavailable; retain the durable fence and reconcile the exact record before replay.");
      return { status: "pending", intent: view(intent), blockers: [] };
    }
    await this.adapters.afterCheckpoint?.("remove-returned"); active();
    const after = await daemon.read(binding); active(); assertSameDaemon(intent, after);
    if (after.record) {
      intent = transition(intent, "uncertain", "Record remains present after remove acknowledgement; reconcile before retrying.");
      return { status: "pending", intent: view(intent), blockers: [] };
    }
    intent = finish(intent, "retired", "Exact record absence confirmed; files, original bindings and observation history were retained.");
    return { status: "retired", intent: view(intent), blockers: [] };
  }
}
