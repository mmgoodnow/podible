import { Database } from "bun:sqlite";
import { constants, existsSync, promises as fs } from "node:fs";
import path from "node:path";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import { finalizedCanonicalObject, readCanonicalDownload, resolveDownloadMembers } from "./canonical-download";
import { isBookCrossSeedScope, type CrossSeedScope } from "./cross-seed-state";
import { captureReuseFile, captureReuseRoot } from "./reuse-layout";
import type { ReuseRootSnapshot, ReuseSourceFile } from "./reuse-plan";
import { reuseDigest } from "./reuse-plan";
import { effectCheckpoint } from "../runtime/execution-effects";

const count = z.number().nonnegative().safe();
const sourceProofSchema = z.object({
  ref: z.object({ clientId: z.string(), nativeId: z.string() }),
  location: z.object({ rootId: z.string(), relativePath: z.string() }),
  manifest: z.object({ infoHash: z.string(), size: count, files: z.array(z.object({
    path: z.string(), size: count,
    identity: z.object({ device: count, inode: count, mtimeMs: z.number(), ctimeMs: z.number() }).optional(),
  })).min(1).max(100_000) }),
});
type SourceProof = z.infer<typeof sourceProofSchema>;
const safeRelative = (value: string) => !path.isAbsolute(value) && !/[\\\x00-\x1f\x7f]/.test(value) &&
  value.split("/").every(part => part && part !== "." && part !== "..");

/** Only exact registered layouts are opened. No directory enumeration or legacy adoption. */
export async function completedCanonicalReuseSources(input: {
  repo: BooksRepo; scope: CrossSeedScope; root: ReuseRootSnapshot; downloadRoot: string; sizes: number[];
  run?: <T>(name: string, work: () => Promise<T>) => Promise<T>;
}): Promise<ReuseSourceFile[]> {
  const { repo, scope, root, downloadRoot } = input;
  const records = repo.crossSeed.sourceDownloadPages(scope);
  let page = records.next();
  if (page.done) return [];
  if (path.resolve(downloadRoot) !== downloadRoot || await fs.realpath(downloadRoot) !== downloadRoot)
    throw new Error("Reuse download root is not canonical");
  const wanted = new Set(input.sizes), sources = new Map<string, ReuseSourceFile>();
  let journal: Database | undefined;
  function* journalProofs(jobId: number): Generator<unknown> {
    if (repo.executionDatabasePath() === ":memory:" || !existsSync(repo.executionDatabasePath())) return;
    const link = repo.getJobExecution(jobId);
    if (!link) return;
    journal ??= new Database(repo.executionDatabasePath(), { readonly: true });
    journal.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=2000");
    const run = journal.query("SELECT workflow,input FROM workflow_runs WHERE id=?").get(link.run_id) as { workflow: string; input: string } | null;
    if (!run || run.workflow !== `media-${scope.media}_acquire-v1` || JSON.parse(run.input).jobId !== jobId) return;
    const query = journal.query<{ output: string }, [string, number]>(`SELECT output FROM workflow_steps WHERE run_id=? AND status='completed'
      AND name LIKE 'tv:%:source-evidence' ORDER BY name LIMIT 32 OFFSET ?`);
    for (let offset = 0; ; offset += 32) {
      let count = 0;
      for (const row of query.iterate(link.run_id, offset)) { count++; yield JSON.parse(row.output); }
      if (count < 32) return;
    }
  }
  try {
    for (; !page.done; page = records.next()) for (const record of page.value) {
      const book = isBookCrossSeedScope(scope), rootId = book ? "books" : scope.media === "tv" ? "tv" : "movies";
      const tag = book ? "Podible Books" : scope.media === "tv" ? "Podible TV" : "Podible Movies";
      if (record.backend !== "transmission" || !/^transmission:[a-f0-9-]{36}$/.test(record.client_id) ||
        record.ownership_tag !== tag || !/^[a-f0-9]{40}$/.test(record.native_id)) continue;
      const location = z.object({ rootId: z.string(), relativePath: z.string() }).parse(JSON.parse(record.location_json));
      if (location.rootId !== rootId || !safeRelative(location.relativePath)) throw new Error("Reuse source registration escaped its media root");
      const sourceRoot = path.join(downloadRoot, location.relativePath);
      const snapshot = JSON.parse(record.observation_json ?? "null");
      if (!snapshot || snapshot.ref?.clientId !== record.client_id || snapshot.ref?.nativeId !== record.native_id ||
        snapshot.location?.rootId !== rootId || snapshot.location?.relativePath !== location.relativePath ||
        snapshot.complete !== true || !["seeding", "inactive"].includes(snapshot.state) || snapshot.message !== null ||
        snapshot.bytesDone !== snapshot.sizeBytes) continue;
      if (repo.releaseHealth.isOfferBlocked({ key: record.offer_key, infoHash: record.native_id })) continue;
      const collect = async (): Promise<ReuseSourceFile[]> => { try {
        const plan = await readCanonicalDownload(root.path, sourceRoot);
        if (!plan?.ready || !plan.sealed) return []; // Do not promote legacy regular layouts.
        if (plan.clientId !== record.client_id || plan.infoHash !== record.native_id ||
          plan.files.reduce((sum, file) => sum + file.size, 0) !== snapshot.sizeBytes)
          throw new Error("Reuse canonical source differs from its completed registration");
        if (book && (record.root_path !== downloadRoot || location.relativePath !== record.native_id || !record.root_token))
          throw new Error("Reuse book source ownership differs from its registered root");
        if (book) {
          await captureReuseRoot({ rootPath: downloadRoot, markerName: ".podible-content.json", ownerId: record.root_token! });
          const handle = await fs.open(path.join(sourceRoot, ".podible-book-torrent.json"), constants.O_RDONLY | constants.O_NOFOLLOW);
          try {
            const stat = await handle.stat();
            if (!stat.isFile() || stat.size > 4096) throw new Error("Invalid reuse book ownership record");
            const owner = JSON.parse(await handle.readFile("utf8"));
            if (owner.rootToken !== record.root_token || owner.clientId !== record.client_id || owner.infoHash !== record.native_id)
              throw new Error("Reuse book source ownership changed");
          } finally { await handle.close(); }
        }
        const proofs = new Map<number, Set<string>>();
        const identityKey = (value: NonNullable<SourceProof["manifest"]["files"][number]["identity"]>) =>
          JSON.stringify([value.device, value.inode, value.mtimeMs, value.ctimeMs]);
        for (const row of repo.crossSeed.sourceProofs(scope, record.id)) {
          const values = row.source_json ? [JSON.parse(row.source_json)] : row.job_id ? journalProofs(row.job_id) : [];
          for (const value of values) {
            effectCheckpoint();
            const parsed = sourceProofSchema.safeParse(value);
            if (!parsed.success) continue;
            const proof = parsed.data;
            if (proof.ref.clientId === record.client_id && proof.ref.nativeId === record.native_id &&
              proof.location.rootId === rootId && proof.location.relativePath === location.relativePath &&
              proof.manifest.infoHash === record.native_id && proof.manifest.size === snapshot.sizeBytes &&
              proof.manifest.files.length === plan.files.length && proof.manifest.files.every((file, index) =>
                file.path === plan.files[index]!.path && file.size === plan.files[index]!.size)) {
              for (const [index, file] of proof.manifest.files.entries()) {
                if (!file.identity || !plan.files[index]!.relativePath || !wanted.has(file.size)) continue;
                const identities = proofs.get(index) ?? new Set<string>();
                identities.add(identityKey(file.identity));
                proofs.set(index, identities);
              }
            }
          }
        }
        const members: ReuseSourceFile[] = [];
        const resolve = resolveDownloadMembers(root.path, sourceRoot, plan);
        for (const [index, file] of plan.files.entries()) {
          effectCheckpoint();
          if (!file.relativePath || !wanted.has(file.size) || repo.releaseHealth.isContentFileBlocked(file.id)) continue;
          const canonical = path.join(root.path, file.relativePath);
          const resolved = await resolve(path.join(sourceRoot, file.path), file.size);
          if (resolved.path !== canonical) throw new Error("Reuse source member left its canonical reservation");
          const finalized = await finalizedCanonicalObject(root.path, canonical);
          const matchesProof = async () => {
            const current = await fs.lstat(canonical);
            return current.size === file.size && proofs.get(index)?.has(identityKey({
              device: current.dev, inode: current.ino, mtimeMs: current.mtimeMs, ctimeMs: current.ctimeMs,
            })) === true;
          };
          if (!finalized && !await matchesProof()) continue;
          const source = await captureReuseFile({ root, sourceId: file.id, sourcePath: canonical, sourceTorrentId: String(record.id) });
          if (source.identity.device !== file.device || source.identity.inode !== file.inode || source.size !== file.size ||
            (finalized ? !await finalizedCanonicalObject(root.path, canonical) : !await matchesProof()))
            throw new Error("Reuse canonical source changed while capturing identity");
          members.push(source);
        }
        if (members.length && JSON.stringify(await readCanonicalDownload(root.path, sourceRoot)) !== JSON.stringify(plan))
          throw new Error("Reuse canonical source reservation changed during inspection");
        return members;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        return [];
      } };
      const key = reuseDigest([root, record.id, record.client_id, record.native_id, location, record.root_token, snapshot.sizeBytes]);
      const members = await (input.run ? input.run(`reuse:inventory:torrent:${key}`, collect) : collect());
      for (const member of members) sources.set(member.id, member);
    }
    return [...sources.values()];
  } finally { journal?.close(); }
}
