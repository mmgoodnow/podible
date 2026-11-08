import path from "node:path";
import { promises as fs } from "node:fs";
import bencode from "bencode";
import { createHash } from "node:crypto";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import type { AppSettings, JobRow } from "../app-types";
import { durableStep, type WorkerContext } from "../worker/context";
import { effectPhase } from "../runtime/execution-effects";
import { inspectMetainfo } from "./metainfo";
import { managedTransmission, managedTvTransmission, managedBookTransmission } from "./managed-transmission";
import { adoptBookContent } from "../library/book-storage";
import { finalizedCanonicalObject } from "./canonical-download";
import { fetchMovieMetainfo } from "../movies/offers";
import { releaseOfferFromResult, type ReleaseOffer } from "./release-offer";
import { assertMovieStorage } from "../movies/storage";
import type { TorrentClient } from "./torrent-client";
import { withTorrentRegistration } from "./torrent-use";
import { crossSeedCommandSchema, crossSeedCommandHash, crossSeedStateKey, isBookCrossSeedScope,
  type CrossSeedCommand, type CrossSeedReceipt, type CrossSeedScope, type CrossSeedOutcome } from "./cross-seed-state";
import { planContentReuse, type ReuseCandidateManifest, type ReusePlan, type ReuseSourceFile } from "./reuse-plan";
import { captureReuseFile, captureReuseRoot, materializeReuseLayout, prepareReuseLayoutRoot, verifyContentReuse, publishedReuseLayoutCapabilities } from "./reuse-layout";
import { selectedCopies, selectedCopyOffer, selectedCopyId } from "./selected-copies";
import { completedCanonicalReuseSources } from "./reuse-source-inventory";

export type CrossSeedAdapters = {
  client?: (scope: CrossSeedScope) => TorrentClient;
  fetchMetainfo?: (offer: ReleaseOffer) => Promise<Uint8Array>;
  materialize?: typeof materializeReuseLayout;
  pollMs?: number;
  maxPolls?: number;
};
const root = (settings: AppSettings, scope: CrossSeedScope) => path.resolve(isBookCrossSeedScope(scope)
  ? settings.libraryRoot : scope.media === "tv" ? settings.tv.managedRoot : settings.movies.managedRoot);
const storageRoots = async (settings: AppSettings, scope: CrossSeedScope) => {
  if (isBookCrossSeedScope(scope)) {
    const libraryRoot = await fs.realpath(root(settings, scope));
    return { contentRoot: path.join(libraryRoot, ".podible", "content"), downloadRoot: path.join(libraryRoot, ".podible", "downloads") };
  }
  return { contentRoot: path.join(root(settings, scope), "objects"), downloadRoot: path.join(root(settings, scope), "downloads") };
};
const configuredClient = async (settings: AppSettings, scope: CrossSeedScope) => isBookCrossSeedScope(scope)
  ? managedBookTransmission((await storageRoots(settings, scope)).downloadRoot) : scope.media === "tv"
    ? managedTvTransmission(settings.tv.managedRoot) : managedTransmission(settings.movies.managedRoot);
const configuration = (settings: AppSettings, scope: CrossSeedScope) => createHash("sha256")
  .update(JSON.stringify([root(settings, scope), settings.prowlarr, settings.torznab])).digest("hex");

export function assertCrossSeedDemand(repo: BooksRepo, scope: CrossSeedScope) {
  if (isBookCrossSeedScope(scope)) {
    if (!repo.getBookRow(scope.targetId)) throw new Error("Reuse book is missing");
    return;
  }
  const intent = repo.retention.get({ namespace: scope.media === "tv" ? "tv-target" : "movie-target", id: String(scope.targetId) });
  const target = scope.media === "tv" ? repo.tv.target(scope.targetId) : repo.movies.target(scope.targetId);
  if (!target || intent?.desiredPresence === "evicted") throw new Error("Reuse target is missing or intentionally evicted");
}

export function crossSeedOffer(repo: BooksRepo, command: Pick<CrossSeedCommand, "scope" | "releaseId" | "selectionCopyId">) {
  if (command.selectionCopyId) return selectedCopyOffer(repo, command.scope, command.selectionCopyId);
  if (!command.releaseId) throw new Error("Missing reuse candidate");
  const entry = repo.releaseInbox.entries([command.releaseId])[0];
  if (!entry || entry.result.mediaType !== command.scope.media) throw new Error("Reuse release is not a retained result for this media type");
  return releaseOfferFromResult(entry.result);
}

export function queueCrossSeed(repo: BooksRepo, raw: z.input<typeof crossSeedCommandSchema>) {
  const command = crossSeedCommandSchema.parse(raw), commandHash = crossSeedCommandHash(command);
  return repo.transaction(() => {
    const previous = repo.crossSeed.receipt(command.operationId);
    if (previous) {
      if (previous.commandHash !== commandHash) throw new Error("Reuse operation UUID belongs to another command");
      return previous;
    }
    assertCrossSeedDemand(repo, command.scope); crossSeedOffer(repo, command);
    const job = repo.createJob({ type: "torrent_reuse", payload: { operationId: command.operationId },
      idempotencyKey: `torrent-reuse:${command.operationId}`, maxAttempts: 3 });
    const receipt: CrossSeedReceipt = { command, commandHash, jobId: job.id, createdAt: new Date().toISOString() };
    repo.setJsonState(crossSeedStateKey(command.operationId), receipt);
    return receipt;
  });
}

/** Called after canonical import, before Plex verification; each copy is independently recoverable. */
export function queueSelectedCrossSeeds(repo: BooksRepo, scope: CrossSeedScope, acquisitionId: number, selectedKey: string) {
  return repo.transaction(() => {
    const revision = repo.crossSeed.contentRevision(scope);
    return selectedCopies(repo, scope, acquisitionId, selectedKey).map(copy => {
      const pointer = `cross-seed:selected-latest:${copy.id}`;
      const previousId = repo.getJsonState<string>(pointer);
      const previous = previousId ? repo.crossSeed.receipt(previousId) : null;
      // Never transfer an existing daemon registration/layout to a new operation UUID.
      if (previous && (!previous.outcome || previous.outcome.torrentId !== undefined || previous.outcome.infoHash !== undefined ||
        previous.outcome.kind === "seeding" || previous.outcome.kind === "unsupported")) return previous;
      const receipt = queueCrossSeed(repo, { operationId: selectedCopyId(`${copy.id}:${revision}`), scope, selectionCopyId: copy.id });
      repo.setJsonState(pointer, receipt.command.operationId);
      return receipt;
    });
  });
}

export function reuseCandidate(bytes: Uint8Array): ReuseCandidateManifest {
  const meta = inspectMetainfo(bytes);
  const decoded = bencode.decode(Buffer.from(bytes)) as { info: { "piece length": number; pieces: Uint8Array } };
  const pieces = Buffer.from(decoded.info.pieces), pieceHashes: string[] = [];
  for (let i = 0; i < pieces.length; i += 20) pieceHashes.push(pieces.subarray(i, i + 20).toString("hex"));
  return { identity: { v1: meta.infoHash }, files: meta.files.map(file => ({ ...file,
    role: /\.(mkv|mp4|m4v|avi|mov|ts|mp3|m4b|m4a|epub|pdf)$/i.test(file.path) ? "content" : "sidecar" })),
    v1: { pieceLength: decoded.info["piece length"], pieceHashes } };
}

/** Read a bounded pool of exact registered objects, not a synthetic season/torrent. */
export async function crossSeedPlan(repo: BooksRepo, settings: AppSettings, command: CrossSeedCommand,
  bytes: Uint8Array): Promise<ReusePlan> {
  assertCrossSeedDemand(repo, command.scope);
  const candidate = reuseCandidate(bytes), total = candidate.files.reduce((sum, file) => sum + file.size, 0);
  if (total > command.policy.maximumCandidateBytes || candidate.files.length > 2000) throw new Error("Reuse candidate exceeds policy bounds");
  const { contentRoot } = await storageRoots(settings, command.scope);
  if (!isBookCrossSeedScope(command.scope)) await assertMovieStorage(repo, contentRoot);
  const ownedRoot = await captureReuseRoot({ rootPath: contentRoot, markerName: ".podible-content.json" });
  const files = repo.crossSeed.files(command.scope, [...new Set(candidate.files.map(file => file.size))]);
  if (files.length > 2000) throw new Error("Reuse source inventory exceeds the bounded plan");
  const sources: ReuseSourceFile[] = [];
  for (const file of files) {
    try {
      let sourcePath: string;
      if (isBookCrossSeedScope(command.scope)) {
        sourcePath = file.relative_path;
        const relative = path.relative(contentRoot, sourcePath);
        if (!path.isAbsolute(sourcePath) || path.resolve(sourcePath) !== sourcePath ||
          (!/^files\/[a-f0-9]{2}\/[a-f0-9]{64}\/content(?:\.[a-z0-9]{1,16})?$/.test(relative) &&
            !await finalizedCanonicalObject(contentRoot, sourcePath)))
          throw new Error("Reuse book source is not registered canonical content");
        // The existing helper validates its per-file ownership record. Only an
        // already-canonical path is allowed here, never its adoption branch.
        if (await adoptBookContent(settings.libraryRoot, sourcePath) !== sourcePath)
          throw new Error("Reuse book source ownership changed");
      } else {
        if (repo.releaseHealth.isContentFileBlocked(file.id)) continue;
        z.string().uuid().parse(file.id);
        if (!file.relative_path.startsWith(`files/${file.id.slice(0, 2)}/${file.id}.`)) throw new Error("Reuse source is not in its registered UUID layout");
        sourcePath = path.resolve(contentRoot, file.relative_path);
      }
      const source = await captureReuseFile({ root: ownedRoot, sourceId: file.id,
        sourcePath });
      if (source.size === file.size_bytes) sources.push(source);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
  const completed = await completedCanonicalReuseSources({ repo, scope: command.scope, root: ownedRoot,
    downloadRoot: (await storageRoots(settings, command.scope)).downloadRoot,
    sizes: [...new Set(candidate.files.map(file => file.size))] });
  const byPath = new Map(sources.map(source => [source.path, source]));
  for (const source of completed) if (!byPath.has(source.path)) byPath.set(source.path, source);
  if (byPath.size > 2000) throw new Error("Reuse source inventory exceeds the bounded plan");
  return planContentReuse({ candidate, sources: [...byPath.values()], policy: { version: 1,
    minReuseRatio: command.policy.minimumReuseRatio, maxExtraDownloadBytes: command.policy.maximumExtraBytes } });
}

export async function processCrossSeed(ctx: WorkerContext, job: JobRow,
  adapters: CrossSeedAdapters = ctx.crossSeedAdapters ?? {}): Promise<"done"> {
  if (!ctx.execution) throw new Error("Torrent reuse requires durable execution");
  const { operationId } = z.object({ operationId: z.string().uuid() }).strict().parse(JSON.parse(job.payload_json ?? "{}"));
  const receipt = ctx.repo.crossSeed.receipt(operationId);
  if (!receipt || receipt.jobId !== job.id) throw new Error("Unknown reuse execution");
  if (receipt.outcome) return "done";
  const { command } = receipt, settings = ctx.getSettings();
  const expected = await durableStep(ctx, "reuse:configuration", () => configuration(settings, command.scope));
  const active = () => { ctx.execution!.assertActive(); assertCrossSeedDemand(ctx.repo, command.scope);
    if (configuration(ctx.getSettings(), command.scope) !== expected) throw new Error("Reuse configuration changed"); };
  const finish = async (outcome: Omit<CrossSeedOutcome, "at">) => {
    await durableStep(ctx, "reuse:outcome", () => { active(); ctx.repo.setJsonState(crossSeedStateKey(operationId),
      { ...receipt, outcome: { ...outcome, at: new Date().toISOString() } }); });
    return "done" as const;
  };
  active();
  const services = command.scope.media === "movie" ? ctx.movieServices : command.scope.media === "tv" ? ctx.tvServices : undefined;
  const encoded = await durableStep(ctx, "reuse:metainfo", async () => Buffer.from(await (adapters.fetchMetainfo ?? services?.fetchMetainfo ?? fetchMovieMetainfo)(crossSeedOffer(ctx.repo, command))).toString("base64"));
  const bytes = Buffer.from(encoded, "base64"), metadata = inspectMetainfo(bytes);
  active();
  ctx.repo.transferMetrics.learnMetainfo(bytes, crossSeedOffer(ctx.repo, command).provider);
  const client = adapters.client?.(command.scope) ?? services?.client ?? await configuredClient(settings, command.scope);
  if (ctx.repo.releaseHealth.isOfferBlocked({ ...crossSeedOffer(ctx.repo, command), infoHash: metadata.infoHash }))
    return finish({ kind: "skipped", reason: "Candidate is a confirmed nuked release" });
  if (!client.capabilities.pausedAdd || !client.capabilities.recheck || !client.capabilities.derivedLayouts || !client.capabilities.metainfoVersions.includes(1))
    return finish({ kind: "unsupported", reason: "Client cannot safely add paused and verify this derived v1 layout" });
  if (ctx.repo.crossSeed.owns(command.scope, client.id, metadata.infoHash))
    return finish({ kind: "skipped", reason: "Target already owns this torrent identity", infoHash: metadata.infoHash });
  const identity = { clientId: client.id, nativeId: metadata.infoHash };
  if (ctx.repo.reuseTorrent(identity) && ctx.repo.crossSeed.registrationOwner(identity) !== operationId)
    return finish({ kind: "skipped", reason: "Torrent identity already belongs to another registered operation", infoHash: metadata.infoHash });
  const plan = await durableStep(ctx, "reuse:plan", () => crossSeedPlan(ctx.repo, settings, command, bytes));
  const sourceBlocked = () => plan.files.some(file => file.source && ctx.repo.releaseHealth.isContentFileBlocked(file.source.id));
  if (sourceBlocked()) return finish({ kind: "skipped", reason: "Reuse source was confirmed nuked after planning" });
  if (plan.status === "rejected" || !plan.summary.likelyReuseBytes) return finish({ kind: "skipped", reason: plan.reasons.join("; ") || "No reusable registered files" });
  const { downloadRoot, contentRoot } = await storageRoots(settings, command.scope);
  const destination = path.join(downloadRoot, "reuse");
  await durableStep(ctx, "reuse:prepare", async () => {
    active();
    if (isBookCrossSeedScope(command.scope)) {
      await captureReuseRoot({ rootPath: contentRoot, markerName: ".podible-content.json" });
    } else await assertMovieStorage(ctx.repo, contentRoot);
    await fs.mkdir(downloadRoot, { recursive: true });
    if (await fs.realpath(downloadRoot) !== downloadRoot) throw new Error("Reuse download root traverses a symlink");
    return prepareReuseLayoutRoot({ rootPath: destination, ownerId: client.id });
  });
  // Revalidate layout receipts on every retry, including a lost stage checkpoint.
  active();
  const destinationRoot = await captureReuseRoot({ rootPath: destination, markerName: ".podible-reuse-root.json", ownerId: client.id });
  const materialize = (readOnly: boolean, savedProof?: ReusePlan) => (adapters.materialize ?? materializeReuseLayout)({
    plan: savedProof ?? plan, root: destinationRoot, capabilities: readOnly
      ? { cow: "unavailable", symlinkMode: "verified-read-only" }
      : { cow: "clone-force", symlinkMode: "never" }, verification: savedProof ? "journal-v1-pieces" : "client" });
  const staged = await durableStep(ctx, "reuse:layout", async () => {
    effectPhase("reuse:layout-receipt");
    const previous = await publishedReuseLayoutCapabilities(destinationRoot, plan);
    if (previous?.symlinkMode === "verified-read-only") {
      if (!client.capabilities.readOnlySeeding || !client.resumeSeedOnly)
        return { status: "unsupported" as const, readOnly: false, reason: "Client cannot safely resume the existing seed-only symlink layout" };
      effectPhase("reuse:symlink-layout");
      return { ...await materialize(true), readOnly: true };
    }
    effectPhase("reuse:clone-layout");
    const cow = await materialize(false);
    if (cow.status !== "unsupported" || !client.capabilities.readOnlySeeding || !client.resumeSeedOnly)
      return { ...cow, readOnly: false };
    effectPhase("reuse:piece-proof");
    const verified = await verifyContentReuse(plan);
    if (verified.status !== "ready" || verified.summary.verifiedReuseBytes !== metadata.size)
      return { status: "unsupported" as const, readOnly: false,
        reason: "CoW is unavailable and candidate bytes are not a complete exact match; shared files were not linked" };
    effectPhase("reuse:symlink-layout");
    return { ...await materialize(true), readOnly: true };
  });
  if (staged.status !== "ready") return finish({ kind: staged.status === "unsupported" ? "unsupported" : "skipped", reason: staged.reason });
  const readOnly = staged.readOnly ?? false;
  const stage = () => materialize(readOnly, readOnly ? staged.plan : undefined);
  const replay = await stage();
  if (replay.status !== "ready" || replay.dataPath !== staged.dataPath) throw new Error("Reuse layout changed after staging");
  const dataPath = await fs.realpath(staged.dataPath);
  if (!dataPath.startsWith(destination + path.sep)) throw new Error("Reuse layout escaped its pinned root");
  const proveLayout = async () => {
    if (readOnly) {
      const current = await stage();
      if (current.status !== "ready" || current.dataPath !== staged.dataPath)
        throw new Error("Verified symlink layout changed");
      return current.plan;
    }
    const files: ReuseSourceFile[] = [], bindings: { candidatePath: string; sourceId: string }[] = [];
    for (let index = 0; index < plan.candidate.files.length; index++) {
      const file = plan.candidate.files[index]!;
      try {
        const captured = await captureReuseFile({ root: destinationRoot, sourceId: String(index), sourcePath: path.join(dataPath, file.path) });
        if (captured.size === file.size) { files.push(captured); bindings.push({ candidatePath: file.path, sourceId: captured.id }); }
      } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    }
    return verifyContentReuse(planContentReuse({ candidate: plan.candidate, sources: files, bindings, policy: plan.policy }));
  };
  // POC proves only this isolated candidate's pieces, never rechecks the library.
  // This is also the fallback when a daemon check completes between observations.
  const initialProof = await durableStep(ctx, "reuse:piece-proof", proveLayout);
  if (initialProof.status === "rejected") return finish({ kind: "skipped", reason: initialProof.reasons.join("; ") });
  const location = { rootId: isBookCrossSeedScope(command.scope) ? "books" : command.scope.media === "tv" ? "tv" : "movies", relativePath: path.relative(downloadRoot, dataPath) };
  const ref = await durableStep(ctx, "reuse:add", () => withTorrentRegistration(ctx.repo, { clientId: client.id, infoHash: metadata.infoHash }, async () => {
    active();
    if (ctx.repo.reuseTorrent(identity) && ctx.repo.crossSeed.registrationOwner(identity) !== operationId)
      return { skipped: true as const };
    if (sourceBlocked()) throw new Error("Reuse source was confirmed nuked before torrent addition");
    const result = await client.ensureAdded({ metainfo: bytes, location, ...(readOnly ? { seedOnly: true } : {}) });
    if (result.clientId !== client.id || result.nativeId !== metadata.infoHash) throw new Error("Reuse client returned another identity");
    ctx.repo.reserveReuseTorrent(result, location, crossSeedOffer(ctx.repo, command).key, command.scope, operationId);
    return result;
  }));
  if ("skipped" in ref) return finish({ kind: "skipped", reason: "Torrent identity already belongs to another registered operation", infoHash: metadata.infoHash });
  const torrent = ctx.repo.reuseTorrent(ref)!;
  const check = async (requireSeedOnly = true) => {
    active();
    const snapshot = await client.inspect(ref);
    if (snapshot.ref.clientId !== ref.clientId || snapshot.ref.nativeId !== ref.nativeId ||
      snapshot.location.rootId !== location.rootId || snapshot.location.relativePath !== location.relativePath ||
      snapshot.sizeBytes !== metadata.size) throw new Error("Reuse client observation changed identity or layout");
    if (requireSeedOnly && readOnly && snapshot.seedOnly !== true) throw new Error("Shared torrent lost its no-download file selection");
    return snapshot;
  };
  try {
  await durableStep(ctx, "reuse:verify", () => { active(); return client.recheck(ref); });
  // Wait for actual verification to run, not just acknowledgement of torrent-verify.
  let checking = false, verified = false;
  for (let i = 0; i < (adapters.maxPolls ?? 1200); i++) {
    const snapshot = await check();
    checking ||= snapshot.state === "checking";
    if (checking && snapshot.state !== "checking") { verified = true; break; }
    // Tiny checks can finish between polls; our independent piece proof bounds
    // repair and final proof below is required even if the daemon says complete.
    if (snapshot.state !== "checking" && i >= 10) { verified = true; break; }
    await ctx.execution.sleep(`reuse:verify-wait:${i}`, adapters.pollMs ?? 500);
  }
  if (!verified) return finish({ kind: "stalled", reason: "Client did not finish the bounded candidate check", torrentId: torrent.id });
  const initial = await check();
  if (readOnly && !initial.complete)
    return finish({ kind: "skipped", reason: "Symlink source is no longer fully verified; seed-only record remains paused", torrentId: torrent.id });
  const remaining = metadata.size - (initial.bytesDone ?? 0);
  if (remaining > command.policy.maximumExtraBytes || (metadata.size && (initial.bytesDone ?? 0) / metadata.size < command.policy.minimumReuseRatio))
    return finish({ kind: "skipped", reason: "Verified missing pieces exceed the reuse policy; record remains paused", torrentId: torrent.id });
  // Running is a reconciled intent, not a once-only effect: error cleanup or an
  // external stop can undo a previously checkpointed start acknowledgement.
  const resume = () => readOnly ? client.resumeSeedOnly!(ref) : client.resume(ref);
  await check(); await resume();
  for (let poll = 0; poll < (adapters.maxPolls ?? 1200); poll++) {
    if (sourceBlocked() || ctx.repo.releaseHealth.isOfferBlocked({ ...crossSeedOffer(ctx.repo, command), infoHash: metadata.infoHash })) {
      await check(); await client.quiesce(ref);
      return finish({ kind: "stalled", reason: "Candidate was confirmed nuked during reuse", torrentId: torrent.id });
    }
    const snapshot = await check();
    if (snapshot.complete && snapshot.state !== "checking") {
      const proof = await proveLayout();
      if (proof.summary.verifiedReuseBytes !== metadata.size) {
        await durableStep(ctx, "reuse:stop-unverified", () => client.quiesce(ref));
        return finish({ kind: "stalled", reason: "Engine completion did not match candidate piece hashes", torrentId: torrent.id });
      }
      const live = await check();
      if (live.state === "inactive" && !live.message) await resume();
      if (!live.complete || live.state !== "seeding") {
        await ctx.execution.sleep(`reuse:confirm-running-wait:${poll}`, adapters.pollMs ?? 500);
        continue;
      }
      return finish({ kind: "seeding", reason: "Candidate independently verified complete; existing library selection is unchanged",
        torrentId: torrent.id, infoHash: metadata.infoHash, totalBytes: metadata.size,
        accountingVersion: 1,
        sidecarDownloadBytes: initialProof.files.filter(file => file.role === "sidecar").reduce((sum, file) => sum + file.size - file.verifiedBytes, 0),
        contentRepairBytes: initialProof.files.filter(file => file.role === "content").reduce((sum, file) => sum + file.size - file.verifiedBytes, 0),
        contentFileCount: initialProof.files.filter(file => file.role === "content").length,
        reusedBytes: initialProof.summary.verifiedReuseBytes, downloadedBytes: metadata.size - initialProof.summary.verifiedReuseBytes });
    }
    // One tracker warning must not stop a transfer that can still make progress.
    if (snapshot.message && snapshot.state === "inactive") {
      await durableStep(ctx, "reuse:stop-error", () => client.quiesce(ref));
      return finish({ kind: "stalled", reason: "Candidate engine reports a tracker/storage error; retained for later recovery", torrentId: torrent.id });
    }
    await ctx.execution.sleep(`reuse:download-wait:${poll}`, adapters.pollMs ?? 5000);
  }
  await durableStep(ctx, "reuse:stop-stalled", () => client.quiesce(ref));
  return finish({ kind: "stalled", reason: "Candidate exceeded the bounded transfer window; old torrents and library remain intact", torrentId: torrent.id });
  } catch (error) {
    // Retry must not leave an unmonitored writable transfer running. Never stop
    // a foreign layout or act after this execution has lost its lease.
    try { await check(false); await client.quiesce(ref); } catch { /* Next attempt reconciles ambiguous ownership. */ }
    throw error;
  }
}

