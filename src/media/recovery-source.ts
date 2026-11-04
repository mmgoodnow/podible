import { promises as fs } from "node:fs";
import path from "node:path";
import type { WorkerContext } from "../worker/context";
import { durableStep } from "../worker/context";
import type { TorrentClient, TorrentRef } from "./torrent-client";
import type { inspectMetainfo } from "./metainfo";

/** Recheck only an explicitly retried successful source with genuinely absent bytes, never every existing torrent. */
export async function recoverMissingTorrentSource(input: {
  ctx: WorkerContext; client: TorrentClient; ref: TorrentRef; sourceRoot: string;
  metadata: ReturnType<typeof inspectMetainfo>; prefix: string; active: () => void;
}) {
  const { ctx, client, ref, metadata, sourceRoot, prefix, active } = input;
  const missing = await durableStep(ctx, `${prefix}:recovery-source-presence`, async () => {
    active();
    if (await fs.realpath(sourceRoot) !== sourceRoot) throw new Error("Recovery source root is not canonical");
    let absent = false;
    for (const file of metadata.files) {
      const full = path.resolve(sourceRoot, file.path);
      if (!full.startsWith(sourceRoot + path.sep)) throw new Error("Recovery source escapes its metainfo root");
      let parent = sourceRoot, missingParent = false;
      for (const part of path.relative(sourceRoot, path.dirname(full)).split(path.sep).filter(Boolean)) {
        parent = path.join(parent, part);
        try {
          const stat = await fs.lstat(parent);
          if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("Recovery source has a noncanonical ancestor");
        } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; missingParent = true; break; }
      }
      if (missingParent) { absent = true; continue; }
      try {
        const stat = await fs.lstat(full);
        if (!stat.isFile() || stat.isSymbolicLink() || stat.size !== file.size || await fs.realpath(full) !== full)
          throw new Error("Existing recovery source changed; absence is not established");
      } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; absent = true; }
    }
    return absent;
  });
  if (!missing) return;
  await durableStep(ctx, `${prefix}:recovery-source-recheck`, () => { active(); return client.recheck(ref); });
  // A verify RPC acknowledgement is not evidence that the daemon has stopped
  // serving its old completion state. Require a check/incomplete observation.
  for (let poll = 0; poll < 100; poll++) {
    const started = await durableStep(ctx, `${prefix}:recovery-source-check:${poll}`, async () => {
      active(); const observed = await client.inspect(ref); active();
      if (observed.ref.clientId !== ref.clientId || observed.ref.nativeId !== ref.nativeId)
        throw new Error("Recovery recheck changed torrent identity");
      const started = observed.state === "checking" || !observed.complete;
      if (!started && poll === 99) throw new Error("Recovery recheck did not establish a fresh incomplete/checking state");
      return started;
    });
    if (started) return;
    if (ctx.execution) await ctx.execution.sleep(`${prefix}:recovery-source-wait:${poll}`, 100);
    else await Bun.sleep(100);
  }
  throw new Error("Recovery recheck did not establish a fresh incomplete/checking state");
}
