import { durableStep, type WorkerContext } from "../worker/context";
import { MovieIndexerError } from "./offers";

/** Rate-limit waits survive restart rather than burning immediate step retries. */
export async function movieUpstreamStep<T>(
  ctx: WorkerContext,
  name: string,
  action: () => Promise<T>,
): Promise<T> {
  for (let attempt = 0; attempt < (ctx.execution ? Infinity : 3); attempt++) {
    ctx.execution?.assertActive();
    const result = await durableStep(
      ctx,
      `${name}:request:${attempt}`,
      async () => {
        try {
          return { ok: true as const, value: await action() };
        } catch (error) {
          if (
            !(error instanceof MovieIndexerError) ||
            !error.retryable ||
            (!ctx.execution && attempt === 2)
          )
            throw error;
          return {
            ok: false as const,
            delay: Math.max(1000, Math.min(3_600_000, error.retryAfterMs)),
          };
        }
      },
    );
    if (result.ok) return result.value;
    if (ctx.execution)
      await ctx.execution.sleep(`${name}:backoff:${attempt}`, result.delay);
    else await Bun.sleep(result.delay);
  }
  throw new Error("Upstream retry budget exhausted");
}
