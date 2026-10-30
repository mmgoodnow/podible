import { createHash, randomUUID } from "node:crypto";
import { mkdir, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { isRunControlError } from "reflow-ts";

import { configDir } from "../config";
import type { WorkerContext } from "../worker/context";

export type ChapterExecutionContext = Pick<WorkerContext, "execution">;

export function rethrowChapterExecutionError(ctx: ChapterExecutionContext, error: unknown): void {
  ctx.execution?.assertActive();
  if (isRunControlError(error)) throw error;
}

/** Keep agent traces/transcript evidence in artifacts, not the workflow journal. */
export async function chapterArtifactStep<T>(ctx: ChapterExecutionContext, name: string, handler: () => Promise<T>): Promise<T> {
  if (!ctx.execution) return handler();
  const execution = ctx.execution;
  execution.assertActive();
  const artifact = await execution.run(name, async () => {
    execution.assertActive();
    const value = await handler();
    execution.assertActive();
    const directory = path.join(configDir, "chapter-execution-artifacts", createHash("sha256").update(execution.key).digest("hex"));
    await mkdir(directory, { recursive: true });
    const artifactPath = path.join(directory, `${createHash("sha256").update(name).digest("hex")}-${randomUUID()}.json`);
    const temporaryPath = `${artifactPath}.tmp`;
    try {
      await writeFile(temporaryPath, JSON.stringify(value));
      execution.assertActive();
      await rename(temporaryPath, artifactPath);
      execution.assertActive();
      return { path: artifactPath };
    } finally {
      await rm(temporaryPath, { force: true });
    }
  });
  execution.assertActive();
  return JSON.parse(await Bun.file(artifact.path).text()) as T;
}
