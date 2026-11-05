import { promises as fs } from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import bencode from "bencode";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import type { SQLiteStorage } from "reflow-ts/sqlite-bun";
import { inspectMetainfo } from "./metainfo";
import type { TorrentClient } from "./torrent-client";
import type { TransmissionRpc } from "./transmission-client";
import { admitTorrentUse, finishTorrentUse } from "./torrent-retirement-admission";

export const onePieceResumeScope = {
  hash: "50f425010d3a3c72a66721e3fca5b66ce47abdfe", requestId: 5,
  targetId: 5, acquisitionId: 17, jobId: 8924, torrentId: 167,
  entryKey: "tvdb:81797", relativePath: "acquisition-17-0",
} as const;
type Scope = { [K in keyof typeof onePieceResumeScope]: typeof onePieceResumeScope[K] extends number ? number : string };
type Input = {
  scope: Scope; repo: BooksRepo; journal: Pick<SQLiteStorage, "getRun" | "getStepResults">;
  client: TorrentClient; rpc: Pick<TransmissionRpc, "call">;
  engineDirectory: string; defaultDirectory: string; reportPath: string;
  build: string; requiredBuild?: string; retryOriginalJob: () => Promise<void>;
  timeoutMs?: number; pollMs?: number;
};
const receiptSchema = z.object({ proof: z.string(), stage: z.enum(["intent", "registered", "rechecked", "retry-queued"]),
  originalBytes: z.number().nonnegative(), build: z.string(), operationId: z.string().uuid() }).strict();
const rowSchema = z.object({ hashString: z.string(), downloadDir: z.string(), labels: z.array(z.string()), status: z.number(),
  totalSize: z.number(), haveValid: z.number(), haveUnchecked: z.number(),
  files: z.array(z.object({ name: z.string(), length: z.number() })) });
const fail = (message: string): never => { throw new Error(`Resume repair refused: ${message}`); };
const hash = (input: unknown) => createHash("sha256").update(JSON.stringify(input)).digest("hex");
async function regular(file: string) {
  const stat = await fs.lstat(file).catch((error: NodeJS.ErrnoException) => { if (error.code === "ENOENT") return null; throw error; });
  if (stat && (!stat.isFile() || stat.isSymbolicLink() || await fs.realpath(file) !== file)) fail("unsafe file path");
  return stat;
}
async function readReceipt(file: string) {
  return await regular(file) ? receiptSchema.parse(JSON.parse(await fs.readFile(file, "utf8"))) : null;
}
async function saveReceipt(file: string, value: z.infer<typeof receiptSchema>) {
  const temporary = `${file}.${randomUUID()}.tmp`;
  const handle = await fs.open(temporary, "wx", 0o600);
  try { await handle.writeFile(JSON.stringify(value)); await handle.sync(); } finally { await handle.close(); }
  await fs.rename(temporary, file);
  const directory = await fs.open(path.dirname(file), "r");
  try { await directory.sync(); } finally { await directory.close(); }
}

/** Privileged one-off; callers must pin an operator-approved scope, never client-authored identities. */
export async function repairTorrentResume(input: Input, apply = false) {
  const { scope, repo, client, rpc } = input;
  const request = repo.tv.requestById(scope.requestId), target = repo.tv.target(scope.targetId);
  const acquisition = repo.tv.acquisition(scope.acquisitionId), job = repo.getJob(scope.jobId);
  if (!request || request.target_id !== scope.targetId || request.withdrawn_at || !target ||
      repo.tv.entry(target.entry_id)?.key !== scope.entryKey || !acquisition ||
      acquisition.target_id !== scope.targetId || acquisition.request_id !== scope.requestId || acquisition.job_id !== scope.jobId || acquisition.finished_at ||
      job?.type !== "tv_acquire" || JSON.parse(job.payload_json ?? "{}").acquisitionId !== scope.acquisitionId)
    fail("request/acquisition/job identity changed");
  if (repo.retention.get({ namespace: "tv-target", id: String(scope.targetId) })?.desiredPresence !== "present") fail("request is not retained");
  const record = repo.tv.downloads(scope.targetId).find(row => row.id === scope.torrentId);
  const location = { rootId: "tv", relativePath: scope.relativePath };
  if (!record || record.native_id !== scope.hash || record.client_id !== client.id || record.ownership_tag !== "Podible TV" ||
      !isDeepStrictEqual(JSON.parse(record.location_json), location)) fail("registry ownership changed");
  const ref = { clientId: client.id, nativeId: scope.hash }, link = repo.getJobExecution(scope.jobId);
  const run = link && await input.journal.getRun(link.run_id);
  if (!run || run.workflow !== "media-tv_acquire-v1" || (run.input as { jobId?: number }).jobId !== scope.jobId) fail("execution identity changed");
  const steps = (await input.journal.getStepResults(link!.run_id)).filter(s => s.status === "completed");
  const output = (name: string) => { const rows = steps.filter(s => s.name === name); if (rows.length !== 1) fail("missing exact execution receipt"); return rows[0]!.output; };
  if (!isDeepStrictEqual(output("tv:attempt:0:snatch"), ref) ||
      !isDeepStrictEqual(output("tv:attempt:0:download-intent"), location)) fail("snatch/location receipt mismatch");
  const encoded = z.object({ ok: z.literal(true), value: z.string().max(12 * 1024 ** 2) }).parse(output("tv:attempt:0:metainfo:request:0"));
  const metadata = inspectMetainfo(Buffer.from(encoded.value, "base64"));
  if (metadata.infoHash !== scope.hash) fail("metainfo receipt hash mismatch");
  const engineMetainfo = path.join(input.engineDirectory, "torrents", `${scope.hash}.torrent`);
  if (!await regular(engineMetainfo) || inspectMetainfo(await fs.readFile(engineMetainfo)).infoHash !== scope.hash) fail("engine metainfo mismatch");
  const root = path.resolve(repo.getSettings().tv.managedRoot, "downloads");
  const owned = path.resolve(root, scope.relativePath);
  if (!owned.startsWith(root + path.sep) || await fs.realpath(root) !== root || await fs.realpath(owned) !== owned) fail("unsafe owned directory");
  let presentFiles = 0;
  for (const file of metadata.files) {
    const present = await regular(path.join(owned, file.path));
    if (present) { if (present.size > file.size) fail("retained file exceeds metainfo size"); presentFiles++; }
    if (await regular(path.join(input.defaultDirectory, file.path))) fail("data exists in the default directory");
  }
  if (!presentFiles) fail("no retained source files");
  const observation = JSON.parse(record!.observation_json ?? "null");
  if (observation?.ref?.nativeId !== scope.hash || observation.ref.clientId !== client.id ||
      !isDeepStrictEqual(observation.location, location) || observation.sizeBytes !== metadata.size ||
      !(observation.bytesDone > 0 && observation.bytesDone <= metadata.size)) fail("missing historical owned download evidence");
  const proof = hash([scope, client.id, root, owned, metadata, link!.run_id]);
  let receipt = await readReceipt(input.reportPath);
  if (receipt && receipt.proof !== proof) fail("repair report belongs to different provenance");
  if (!receipt && (job!.status !== "failed" || !job!.error?.includes("Refusing to control an unmanaged torrent"))) fail("original job is not the known failed execution");
  const raw = async () => {
    const response = await rpc.call("torrent-get", { ids: [scope.hash], fields: ["hashString", "downloadDir", "labels", "status", "totalSize", "haveValid", "haveUnchecked", "files"] });
    const rows = z.array(rowSchema).parse(response.torrents);
    if (rows.length !== 1 || rows[0]!.hashString.toLowerCase() !== scope.hash) fail("daemon identity changed");
    const row = rows[0]!;
    if (row.totalSize !== metadata.size || row.files.length !== metadata.files.length || row.files.some((f, i) => f.name !== metadata.files[i]!.path || f.length !== metadata.files[i]!.size)) fail("daemon file manifest mismatch");
    return row;
  };
  const state = await raw();
  const damaged = state.status === 0 && state.labels.length === 0 && state.downloadDir === input.defaultDirectory && state.haveValid === 0 && state.haveUnchecked === 0;
  const intermediate = receipt && state.status === 0 && state.labels.length === 0 && state.downloadDir === owned;
  const restored = receipt && state.labels.length === 1 && state.labels[0] === "Podible TV" && state.downloadDir === owned;
  if (!damaged && !intermediate && !restored) fail("record is not the proven stopped/default-directory failure");
  const resumeFile = path.join(input.engineDirectory, "resume", `${scope.hash}.resume`);
  let resumeState = "missing";
  if (await regular(resumeFile)) {
    const resume = bencode.decode(await fs.readFile(resumeFile)) as Record<string, unknown>;
    if (!(resume.name instanceof Uint8Array) || new TextDecoder().decode(resume.name) !== metadata.name) fail("resume metadata has another torrent name");
    const destination = Buffer.isBuffer(resume.destination) || resume.destination instanceof Uint8Array ? new TextDecoder().decode(resume.destination) : null;
    const labels = Array.isArray(resume.labels) ? resume.labels.map(v => new TextDecoder().decode(v)) : null;
    if (destination === input.defaultDirectory && labels?.length === 0 && resume.paused === 1) resumeState = "damaged-default";
    else if (receipt && destination === owned && (labels?.length === 0 || labels?.length === 1 && labels[0] === "Podible TV")) resumeState = "repair-in-progress";
    else fail("resume metadata does not confirm the known failure");
  }
  const report = { mode: apply ? "apply" : "dry-run", hash: scope.hash, requestId: scope.requestId, jobId: scope.jobId,
    presentFiles, totalFiles: metadata.files.length, totalBytes: metadata.size, originalBytes: observation.bytesDone,
    resumeState, phase: receipt?.stage ?? "not-started" };
  if (!apply) return report;
  if (!input.requiredBuild || !/^[a-f0-9]{40}$/.test(input.requiredBuild) || input.build !== input.requiredBuild) fail("running build is not the explicitly approved shutdown-fixed build");
  if (receipt?.stage === "retry-queued") return { ...report, phase: "retry-queued" };
  const assertIdle = () => {
    if (repo.tv.requestById(scope.requestId)?.withdrawn_at !== null || repo.retention.get({ namespace: "tv-target", id: String(scope.targetId) })?.desiredPresence !== "present") fail("request changed during repair");
    if (repo.tv.acquisitions(scope.targetId).some(a => a.id !== scope.acquisitionId && !a.finished_at &&
      a.job_id && ["queued", "running"].includes(repo.getJob(a.job_id)?.status ?? ""))) fail("another acquisition is active");
    if (repo.getJob(scope.jobId)?.status !== "failed") fail("original job is no longer quiescent");
    if (repo.getJobExecution(scope.jobId)?.run_id !== link!.run_id ||
        path.resolve(repo.getSettings().tv.managedRoot, "downloads") !== root ||
        repo.tv.downloads(scope.targetId).find(row => row.id === scope.torrentId)?.location_json !== record!.location_json)
      fail("configuration or binding changed during repair");
    if (repo.listJobCommands().some(command => command.job_id === scope.jobId)) fail("original job has a pending command");
  };
  assertIdle();
  await fs.mkdir(path.dirname(input.reportPath), { recursive: true, mode: 0o700 });
  if (await fs.realpath(path.dirname(input.reportPath)) !== path.dirname(input.reportPath)) fail("unsafe report directory");
  const lock = await fs.open(`${input.reportPath}.lock`, "wx", 0o600);
  try {
    const fresh = await readReceipt(input.reportPath);
    if (fresh?.proof !== receipt?.proof || fresh?.stage !== receipt?.stage) fail("repair report changed");
    receipt ??= { proof, stage: "intent", originalBytes: observation.bytesDone, build: input.build, operationId: randomUUID() };
    await saveReceipt(input.reportPath, receipt);
    const admissionRef = { clientId: client.id, infoHash: scope.hash };
    const token = admitTorrentUse(repo, admissionRef, receipt.operationId);
    const result = await (async () => {
      assertIdle();
      let row = await raw();
      if (row.downloadDir === owned && row.labels.length === 1 && row.labels[0] === "Podible TV" && row.status !== 0) {
        await client.quiesce(ref); row = await raw();
      }
      if (row.status !== 0 || row.labels.some(label => label !== "Podible TV") || ![input.defaultDirectory, owned].includes(row.downloadDir)) fail("record changed before control");
      if (row.downloadDir !== owned) await rpc.call("torrent-set-location", { ids: [scope.hash], location: owned, move: false });
      row = await raw(); assertIdle();
      if (row.downloadDir !== owned || row.status !== 0 || row.labels.some(label => label !== "Podible TV")) fail("relocation was not confirmed");
      if (!row.labels.includes("Podible TV")) await rpc.call("torrent-set", { ids: [scope.hash], labels: ["Podible TV"] });
      await client.inspect(ref);
      receipt!.stage = "registered"; await saveReceipt(input.reportPath, receipt!);
      await client.recheck(ref);
      const deadline = Date.now() + (input.timeoutMs ?? 30 * 60_000);
      let sawChecking = false, settledSince = 0;
      for (;;) {
        assertIdle(); row = await raw();
        if ([1, 2].includes(row.status)) sawChecking = true;
        if (row.status === 0 && row.haveUnchecked === 0) {
          settledSince ||= Date.now();
          if (sawChecking || row.haveValid > 0 && Date.now() - settledSince >= 1000) break;
        } else settledSince = 0;
        if (Date.now() >= deadline && row.status === 0 && row.haveValid === 0) fail("hash verification recovered no valid pieces");
        if (![0, 1, 2].includes(row.status) || Date.now() >= deadline) fail("recheck did not settle safely");
        await Bun.sleep(input.pollMs ?? 1000);
      }
      if (!(row.haveValid > 0)) fail("hash verification recovered no valid pieces");
      receipt!.stage = "rechecked"; await saveReceipt(input.reportPath, receipt!);
      await client.resume(ref);
      for (;;) {
        assertIdle(); const snapshot = await client.inspect(ref);
        if (snapshot.complete || snapshot.bytesDone !== null && snapshot.bytesDone > receipt!.originalBytes) break;
        if (Date.now() >= deadline || snapshot.message) fail("verified source did not make new progress; original job remains failed");
        await Bun.sleep(input.pollMs ?? 1000);
      }
      assertIdle(); await input.retryOriginalJob();
      receipt!.stage = "retry-queued"; await saveReceipt(input.reportPath, receipt!);
      return { ...report, phase: "retry-queued", verifiedBytes: row.haveValid };
    })();
    finishTorrentUse(repo, admissionRef, token);
    return result;
  } finally { await lock.close(); await fs.unlink(`${input.reportPath}.lock`); }
}
