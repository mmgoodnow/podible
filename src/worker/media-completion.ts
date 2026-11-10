import type { TorrentRef, TorrentSnapshot } from "../media/torrent-client";
import { durableStep, type WorkerContext } from "./context";

export async function waitForTorrentCompletion(ctx: WorkerContext, name: string, ref: TorrentRef,
  stallMs: number, observe: () => Promise<TorrentSnapshot | null>, isComplete: (seen: TorrentSnapshot) => boolean,
  resume?: () => Promise<unknown>) {
  const execution = ctx.execution!;
  let started = 0, initialBytes: number | null = null, checkedStall = false, deadline = 0;
  for (let wake = 0; ; wake++) {
    const slot = `${name}:completion:${wake}`;
    const seen = await durableStep(ctx, `${slot}:inspect`, async () => {
      execution.armCompletion!(slot, "torrent:" + ref.nativeId.toLowerCase());
      return observe();
    });
    if (!seen || isComplete(seen) || seen.seedOnly || seen.integrityError) {
      execution.clearCompletion!(slot);
      return seen !== null && isComplete(seen);
    }
    const now = Date.parse(seen.observedAt);
    if (!Number.isFinite(now)) throw new Error("Invalid client observation time");
    if (wake === 0) {
      started = now;
      initialBytes = seen.bytesDone;
      deadline = started + Math.min(stallMs > 0 ? stallMs : 86_400_000, 86_400_000);
    }
    if (now >= deadline) {
      const advanced = seen.bytesDone !== null && initialBytes !== null && seen.bytesDone > initialBytes;
      if (checkedStall || now >= started + 86_400_000 || (!advanced && !(seen.downloadBytesPerSecond && seen.downloadBytesPerSecond > 0))) {
        execution.clearCompletion!(slot);
        return false;
      }
      // One early stall check, then a hard deadline. Neither check rearms a polling loop.
      checkedStall = true;
      deadline = started + 86_400_000;
    }
    if (resume && seen.state === "inactive") await durableStep(ctx, `${slot}:resume`, resume);
    await execution.waitForCompletion!(slot, Math.max(1, deadline - now));
  }
}

export async function waitForPlexPublication(ctx: WorkerContext, prefix: string, section: string,
  publish: (scan: boolean) => Promise<boolean>) {
  const execution = ctx.execution!;
  const deadline = await durableStep(ctx, `${prefix}:plex-deadline`, () => Date.now() + 10 * 60_000);
  let timedOut = false;
  for (let wake = 0; ; wake++) {
    const slot = `${prefix}:plex-publication:${wake}`;
    const result = await durableStep(ctx, slot, async () => {
      execution.armCompletion!(slot, "plex:" + section);
      const published = await publish(!timedOut && wake <= 1);
      return { published, remaining: Math.max(0, deadline - Date.now()) };
    });
    if (result.published) { execution.clearCompletion!(slot); return; }
    if (timedOut || !result.remaining)
      throw new Error("Plex did not verify the exact media before the scan deadline; existing media remains selected");
    timedOut = !await execution.waitForCompletion!(slot, result.remaining);
  }
}
