import type { MediaType } from "../app-types";
import { IndexerSearchError, type IndexerFailure } from "../library/indexer-errors";
import { runSearchBatch } from "../library/service";
import type { TorznabResult } from "../library/torznab";
import { durableStep, workerLog, type WorkerContext } from "./context";
import { planIndexerQuery } from "../media/indexer-query-plan";
import { recordIndexerTraffic } from "../media/indexer-traffic";

type SearchCheckpoint = TorznabResult[] | {
  kind: "indexer-search-deferred";
  results: TorznabResult[];
  failures: IndexerFailure[];
};

/** Completed legacy searches remain arrays at the original step name. */
export async function selectWithIndexerRecovery<T extends { selections: unknown[] }>(
  ctx: WorkerContext,
  request: { query: string; media: MediaType; jobId: number; bookId: number },
  select: (results: TorznabResult[], stepName: string) => Promise<T>,
): Promise<T> {
  for (let phase = 0; ; phase++) {
    const suffix = phase === 0 ? "" : `:retry:${phase}`;
    let searched = false;
    const checkpoint = await durableStep<SearchCheckpoint>(ctx, `${request.media}:search${suffix}`, async () => {
      searched = true;
      const settings = ctx.getSettings();
      const book = ctx.repo.getBookRow(request.bookId);
      const plan = await planIndexerQuery(settings, { media: request.media, query: request.query, title: book?.title, author: book?.author }, ctx.modelProvider);
      const batch = await runSearchBatch(settings, { ...request, query: plan.primaryQuery }, { alternateQuery: plan.alternateQuery,
        onRequest: () => recordIndexerTraffic(ctx.repo, "acquisition", "search_text") });
      return batch.failures.length ? { kind: "indexer-search-deferred", ...batch } : batch.results;
    });
    const { results, failures } = Array.isArray(checkpoint) ? { results: checkpoint, failures: [] } : checkpoint;
    workerLog(ctx, `[acquire] job=${request.jobId} book=${request.bookId} media=${request.media} query=${JSON.stringify(request.query)} results=${results.length}`);
    // An empty incomplete batch is not a terminal "no results" decision (or a paid agent call).
    const decision = results.length || !failures.length
      ? await select(results, `${request.media}:selection${suffix}`)
      : null;
    if (decision?.selections.length || !failures.length) return decision!;
    const retryAt = failures.flatMap((failure) => failure.retryAt === null ? [] : [failure.retryAt]);
    if (!retryAt.length) {
      // Explicit retry preserves completed checkpoints. Skip an old configuration
      // failure, but fail a fresh probe, so fixing credentials can recover the run.
      if (!searched && ctx.execution) continue;
      throw new IndexerSearchError(failures);
    }
    if (!ctx.execution) throw new IndexerSearchError(failures);
    for (const failure of failures) workerLog(ctx, `[acquire] job=${request.jobId} media=${request.media} indexer_wait=${JSON.stringify(failure.message)}`);
    ctx.execution.assertActive();
    // Sleep is outside run(): it releases ownership/lanes without spending step retry attempts.
    await ctx.execution.sleep(`${request.media}:search:wait:${phase}`, Math.max(0, Math.min(...retryAt) - Date.now()));
  }
}
