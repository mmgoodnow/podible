import { createHash } from "node:crypto";
import { isRunControlError } from "reflow-ts";
import { z } from "zod";
import type { JobRow } from "../app-types";
import type { BooksRepo } from "../repo";
import { durableStep, type WorkerContext } from "../worker/context";
import { createBookFeedMatcher } from "../library/feed-matcher";
import { createMovieFeedMatcher } from "../movies/agents";
import { createTvFeedMatcher } from "../tv/feed-agent";
import { indexerSources } from "./indexer-discovery";
import { indexerGateway, type IndexerGateway, type IndexerMedia, type SearchResult } from "./indexer-gateway";
import { IndexerError, indexerFailure } from "./indexer-errors";
import type { DiscoveredIndexer } from "./prowlarr";
import { feedIdentity } from "./release-inbox";
import { releaseOfferFromResult } from "./release-offer";
import { crossSeedScopeSchema, type CrossSeedScope } from "./cross-seed-state";
import { queueCrossSeed } from "./cross-seed";
import { selectedCopyId } from "./selected-copies";
import { movieQueryEvidence, movieSearchIds, planIndexerQuery, tvSearchEvidence } from "./indexer-query-plan";
import { recordIndexerTraffic } from "./indexer-traffic";

const prefix = "cross-seed:backfill:";
const latestKey = prefix + "latest";
const key = (id: string) => prefix + z.string().uuid().parse(id);
const targetKey = (id: string, index: number) => `${key(id)}:target:${index}`;
const safeName = (name: string) => indexerFailure(name, null, null).provider;
const count = z.number().int().nonnegative();
const providerSchema = z.object({ key: z.string(), provider: z.string(),
  status: z.enum(["pending", "waiting", "completed", "error"]), queued: count, skipped: count,
  error: z.string().nullable(), retryAt: z.number().nullable() });
const targetSchema = z.object({ scope: crossSeedScopeSchema, title: z.string(), retainedDone: z.boolean(),
  retainedQueued: count, retainedSkipped: count, providers: z.array(providerSchema) });
export const backfillSummarySchema = z.object({ requestId: z.string().uuid(), jobId: z.number().int().positive(),
  status: z.enum(["queued", "running", "waiting", "completed", "cancelled", "failed"]),
  scanned: count, total: count, queued: count, skipped: count, errors: count,
  limitPerProvider: z.number().int().min(1).max(150), createdAt: z.string(), updatedAt: z.string() });
export const backfillStartSchema = z.object({ requestId: z.string().uuid(), limitPerProvider: z.number().int().min(1).max(150).default(50) }).strict();
export const backfillStatusParamsSchema = z.object({ requestId: z.string().uuid(), offset: count.default(0), limit: z.number().int().min(1).max(100).default(25) }).strict();
export const backfillStatusSchema = backfillSummarySchema.extend({ targets: z.array(targetSchema), nextOffset: count.nullable() });
type Summary = z.infer<typeof backfillSummarySchema>;
type TargetProgress = z.infer<typeof targetSchema>;
type Target = { scope: CrossSeedScope; title: string; query: string };
type Snapshot = { targets: Target[]; throughId: number };
type Provider = { key: string; provider: string };
export type CrossSeedBackfillAdapters = {
  sources?: (media: IndexerMedia) => Promise<DiscoveredIndexer[]>;
  gateway?: Pick<IndexerGateway, "search">;
};

function eligible(repo: BooksRepo, scope: CrossSeedScope) {
  if (!repo.crossSeed.hasFiles(scope)) return false;
  if (scope.media === "audio" || scope.media === "ebook") return !!repo.getBookRow(scope.targetId);
  return repo.retention.get({ namespace: `${scope.media}-target`, id: String(scope.targetId) })?.desiredPresence !== "evicted";
}

function snapshot(repo: BooksRepo): Snapshot {
  const targets: Target[] = [];
  for (const book of repo.listAllBooks()) for (const media of ["audio", "ebook"] as const) {
    const scope = { media, targetId: book.id };
    if (eligible(repo, scope)) targets.push({ scope, title: book.title, query: `${book.title} ${book.author}`.trim() });
  }
  for (const target of repo.movies.targets()) {
    const scope = { media: "movie" as const, targetId: target.id }, movie = repo.movies.movie(target.movie_id);
    if (movie && target.active_version_id !== null && eligible(repo, scope))
      targets.push({ scope, title: movie.title, query: `${movie.title}${movie.year ? ` ${movie.year}` : ""}` });
  }
  for (const target of repo.tv.targets()) {
    const scope = { media: "tv" as const, targetId: target.id }, entry = repo.tv.entry(target.entry_id);
    if (entry && repo.tv.activeEpisodes(target.id).length && eligible(repo, scope))
      targets.push({ scope, title: entry.title, query: entry.title });
  }
  return { targets, throughId: repo.releaseInbox.retainedHighWater() };
}

export function startCrossSeedBackfill(repo: BooksRepo, raw: z.input<typeof backfillStartSchema>): Summary {
  const input = backfillStartSchema.parse(raw);
  return repo.transaction(() => {
    const prior = repo.getJsonState<Summary>(key(input.requestId));
    if (prior) {
      if (prior.limitPerProvider !== input.limitPerProvider) throw new Error("Backfill request ID already has different parameters");
      return backfillSummarySchema.parse(prior);
    }
    const latest = latestCrossSeedBackfill(repo).backfill;
    if (latest && !repo.isJobCancellationRequested(latest.jobId) && ["queued", "running"].includes(repo.getJobStatus(latest.jobId) ?? "")) return latest;
    const job = repo.createJob({ type: "torrent_reuse_backfill", payload: { requestId: input.requestId }, idempotencyKey: key(input.requestId) });
    const now = new Date().toISOString();
    const state: Summary = { ...input, jobId: job.id, status: "queued", scanned: 0, total: 0, queued: 0, skipped: 0, errors: 0, createdAt: now, updatedAt: now };
    repo.setJsonState(key(input.requestId), state);
    repo.setJsonState(latestKey, input.requestId);
    return state;
  });
}

export function latestCrossSeedBackfill(repo: BooksRepo): { backfill: Summary | null } {
  const id = repo.getJsonState<string>(latestKey);
  if (!id) return { backfill: null };
  return { backfill: backfillSummarySchema.parse(crossSeedBackfillStatus(repo, { requestId: id, limit: 1 })) };
}

export function cancelCrossSeedBackfill(repo: BooksRepo, id: string): Summary {
  return repo.transaction(() => {
    const state = backfillSummarySchema.parse(repo.getJsonState(key(id)));
    repo.markJobCancelled(state.jobId, "Administrator cancelled cross-seed backfill");
    const status = repo.getJobStatus(state.jobId);
    if (status !== "succeeded" && status !== "failed") state.status = "cancelled";
    state.updatedAt = new Date().toISOString(); repo.setJsonState(key(id), state); return state;
  });
}

export function crossSeedBackfillStatus(repo: BooksRepo, raw: z.input<typeof backfillStatusParamsSchema>) {
  const input = backfillStatusParamsSchema.parse(raw), stored = repo.getJsonState<Summary>(key(input.requestId));
  if (!stored) throw new Error("Unknown cross-seed backfill");
  const state = backfillSummarySchema.parse(stored), jobStatus = repo.getJobStatus(state.jobId);
  if (repo.isJobCancellationRequested(state.jobId)) state.status = "cancelled";
  else if ((jobStatus === "cancelled" || jobStatus === "failed") && repo.listJobCommands().some(command => command.job_id === state.jobId &&
    command.generation === repo.jobExecutionGeneration(state.jobId) && ["retry", "enqueue"].includes(command.kind))) state.status = "queued";
  else if (jobStatus === "cancelled" || jobStatus === "failed") state.status = jobStatus;
  else if ((jobStatus === "queued" || jobStatus === "running") && ["failed", "cancelled", "queued"].includes(state.status)) state.status = jobStatus;
  const targets: TargetProgress[] = [];
  for (let index = input.offset; index < Math.min(state.total, input.offset + input.limit); index++) {
    const progress = repo.getJsonState<TargetProgress>(targetKey(input.requestId, index));
    if (progress) targets.push(targetSchema.parse(progress));
  }
  return { ...state, targets, nextOffset: input.offset + input.limit < state.total ? input.offset + input.limit : null };
}

function totals(target: TargetProgress) {
  return { queued: target.retainedQueued + target.providers.reduce((n, p) => n + p.queued, 0),
    skipped: target.retainedSkipped + target.providers.reduce((n, p) => n + p.skipped, 0),
    errors: target.providers.filter(p => p.status === "error").length,
    scanned: Number(target.retainedDone && target.providers.every(p => p.status === "completed" || p.status === "error")) };
}
function updateTarget(repo: BooksRepo, id: string, index: number, change: (target: TargetProgress) => void, marker?: string) {
  return repo.transaction(() => {
    const markerKey = marker ? `${key(id)}:record:${createHash("sha256").update(`${index}:${marker}`).digest("hex")}` : null;
    if (markerKey && repo.getJsonState(markerKey)) return;
    const target = targetSchema.parse(repo.getJsonState(targetKey(id, index))), previous = totals(target);
    change(target);
    const next = totals(target), state = backfillSummarySchema.parse(repo.getJsonState(key(id)));
    for (const field of ["queued", "skipped", "errors", "scanned"] as const) state[field] += next[field] - previous[field];
    state.updatedAt = new Date().toISOString();
    repo.setJsonState(targetKey(id, index), target); repo.setJsonState(key(id), state);
    if (markerKey) repo.setJsonState(markerKey, true);
  });
}

async function matchingIndexes(ctx: WorkerContext, target: Target, results: SearchResult<IndexerMedia>[]): Promise<number[]> {
  if (!results.length) return [];
  const { scope } = target;
  let indexes: number[];
  if (scope.media === "audio" || scope.media === "ebook") {
    const book = ctx.repo.getBookRow(scope.targetId)!;
    const matches = await (ctx.bookFeedMatcher ?? createBookFeedMatcher(ctx.getSettings(), ctx.modelProvider))({
      targets: [{ bookId: book.id, media: scope.media, title: book.title, author: book.author, language: book.language }],
      releases: results as SearchResult<"audio" | "ebook">[] });
    if (matches.some(match => match.bookId !== book.id || match.media !== scope.media)) throw new Error("Backfill matcher returned an unknown target");
    indexes = matches.flatMap(match => match.releaseIndexes);
  } else if (scope.media === "movie") {
    const row = ctx.repo.movies.target(scope.targetId)!;
    const matches = await (ctx.movieFeedMatcher ?? createMovieFeedMatcher(ctx.getSettings(), ctx.modelProvider))({
      targets: [{ targetId: row.id, movie: ctx.repo.movies.movie(row.movie_id)! }], offers: results.map(releaseOfferFromResult) });
    if (matches.some(match => match.targetId !== scope.targetId)) throw new Error("Backfill matcher returned an unknown target");
    indexes = matches.flatMap(match => match.releaseIndexes);
  } else {
    const row = ctx.repo.tv.target(scope.targetId)!, entry = ctx.repo.tv.entry(row.entry_id)!;
    const matches = await (ctx.tvFeedMatcher ?? createTvFeedMatcher(ctx.getSettings(), ctx.modelProvider, "reuse"))({
      targets: [{ targetId: row.id, entry: { ...entry, episodes: [] }, episodes: [] }],
      offers: results.map(result => ({ ...releaseOfferFromResult(result), attributes: [] })) });
    if (matches.some(match => match.targetId !== scope.targetId)) throw new Error("Backfill matcher returned an unknown target");
    indexes = matches.flatMap(match => match.releaseIndexes);
  }
  if (indexes.some(index => !Number.isSafeInteger(index) || index < 0 || !results[index])) throw new Error("Backfill matcher returned an unknown release");
  return [...new Set(indexes)];
}

async function route(ctx: WorkerContext, id: string, target: Target, entries: { id: number; result: SearchResult<IndexerMedia> }[], step: string) {
  const indexes = await durableStep(ctx, step + ":match", () => eligible(ctx.repo, target.scope) ? matchingIndexes(ctx, target, entries.map(entry => entry.result)) : []);
  return durableStep(ctx, step + ":queue", () => ctx.repo.transaction(() => {
    ctx.execution!.assertActive();
    const receiptKey = `${key(id)}:batch:${createHash("sha256").update(step).digest("hex")}`;
    const previous = ctx.repo.getJsonState<{ queued: number; skipped: number }>(receiptKey);
    if (previous) return previous;
    let queued = 0, skipped = entries.length - indexes.length;
    for (const index of indexes) {
      if (!eligible(ctx.repo, target.scope)) { skipped++; continue; }
      const releaseId = entries[index]!.id;
      const attempts = ctx.repo.crossSeed.attemptsForRelease(target.scope, releaseId);
      if (attempts.some(attempt => attempt.outcome?.kind === "seeding" && ctx.repo.crossSeed.hasRegistration(attempt) ||
        ["queued", "running"].includes(ctx.repo.getJobStatus(attempt.jobId) ?? ""))) { skipped++; continue; }
      const operationId = selectedCopyId(`backfill:${id}:${target.scope.media}:${target.scope.targetId}:${releaseId}:${ctx.repo.crossSeed.contentRevision(target.scope)}`);
      if (ctx.repo.crossSeed.receipt(operationId)) { skipped++; continue; }
      queueCrossSeed(ctx.repo, { operationId, scope: target.scope, releaseId: entries[index]!.id }); queued++;
    }
    const result = { queued, skipped }; ctx.repo.setJsonState(receiptKey, result); return result;
  }));
}

/** Explicit one-off library search. Provider cooldown sleeps are outside durable handlers. */
export async function processCrossSeedBackfill(ctx: WorkerContext, job: JobRow, adapters: CrossSeedBackfillAdapters = {}): Promise<"done"> {
  if (!ctx.execution) throw new Error("Cross-seed backfill requires durable execution");
  const { requestId: id } = z.object({ requestId: z.string().uuid() }).strict().parse(JSON.parse(job.payload_json ?? "{}"));
  const state = backfillSummarySchema.parse(ctx.repo.getJsonState(key(id)));
  if (state.jobId !== job.id) throw new Error("Backfill execution does not own its intent");
  const generation = ctx.repo.jobExecutionGeneration(job.id);
  const active = () => ctx.execution!.assertActive();
  const sourceList = adapters.sources ?? ((media: IndexerMedia) => indexerSources(ctx.getSettings(), media, "search"));
  const gateway = adapters.gateway ?? indexerGateway;
  const inventory = await durableStep(ctx, "backfill:snapshot", () => ctx.repo.transaction(() => {
    active();
    const prior = ctx.repo.getJsonState<Snapshot>(`${key(id)}:snapshot`);
    if (prior) return prior;
    const captured = snapshot(ctx.repo); ctx.repo.setJsonState(`${key(id)}:snapshot`, captured); return captured;
  }));
  await durableStep(ctx, "backfill:initialize", () => ctx.repo.transaction(() => {
    active(); const current = backfillSummarySchema.parse(ctx.repo.getJsonState(key(id)));
    current.status = "running"; current.total = inventory.targets.length;
    ctx.repo.setJsonState(`${key(id)}:snapshot`, inventory);
    for (const [index, target] of inventory.targets.entries()) if (!ctx.repo.getJsonState(targetKey(id, index)))
      ctx.repo.setJsonState(targetKey(id, index), { scope: target.scope, title: target.title, retainedDone: false, retainedQueued: 0, retainedSkipped: 0,
        providers: [{ key: `discovery:${target.scope.media}`, provider: "Indexer discovery", status: "pending", queued: 0, skipped: 0, error: null, retryAt: null }] });
    ctx.repo.setJsonState(key(id), current);
  }));
  // Retained routing never waits on discovery or a provider outage in another format.
  for (const [index, target] of inventory.targets.entries()) {
      active();
      const retainedDone = await durableStep(ctx, `backfill:target:${index}:retained-progress`, () =>
        targetSchema.parse(ctx.repo.getJsonState(targetKey(id, index))).retainedDone);
      if (!retainedDone) {
        let queued = 0, skipped = 0, afterId = 0;
        for (;;) {
          const entries = ctx.repo.releaseInbox.retainedPage(target.scope.media, afterId, inventory.throughId, 25);
          if (!entries.length) break;
          afterId = entries[entries.length - 1]!.id;
          const result = await route(ctx, id, target, entries, `backfill:retained:${index}:${afterId}`);
          queued += result.queued; skipped += result.skipped;
        }
        await durableStep(ctx, `backfill:target:${index}:retained-complete`, () => {
          active(); updateTarget(ctx.repo, id, index, progress => { progress.retainedDone = true; progress.retainedQueued = queued; progress.retainedSkipped = skipped; });
        });
      }
  }
  for (let round = 0; ; round++) {
    const waits: number[] = [];
    const providers: Partial<Record<IndexerMedia, Provider[]>> = {};
    for (const media of [...new Set(inventory.targets.map(target => target.scope.media))]) {
      const step = `backfill:sources:${media}:g${generation}:${round}`, cacheKey = `${key(id)}:sources:${media}`;
      type Discovery = { generation: number | null; round: number; sources: Provider[]; failure: import("./indexer-errors").IndexerFailure | null };
      const discovered = await durableStep(ctx, step, async (): Promise<Discovery> => {
        active(); const cached = ctx.repo.getJsonState<Discovery>(cacheKey);
        if (cached && (!cached.failure && cached.sources.length || cached.failure?.retryAt !== null && cached.failure?.retryAt !== undefined && cached.failure.retryAt > Date.now() ||
          cached.failure?.retryAt === null && cached.generation === generation && cached.round >= round)) return cached;
        let result: Discovery;
        try {
          const sources = [...new Map((await sourceList(media)).map(source => [feedIdentity(source, media), { key: feedIdentity(source, media), provider: safeName(source.name) }])).values()];
          result = { generation, round, sources, failure: sources.length ? null : indexerFailure(`No configured ${media} indexers`, 400, null) };
        } catch (error) {
          if (isRunControlError(error)) throw error;
          result = { generation, round, sources: [], failure: error instanceof IndexerError ? error.failure : indexerFailure("Indexer discovery", null, Date.now() + 60_000) };
        }
        active(); ctx.repo.setJsonState(cacheKey, result); return result;
      });
      if (!discovered.failure) providers[media] = discovered.sources;
      else if (discovered.failure.retryAt !== null) waits.push(discovered.failure.retryAt);
      await durableStep(ctx, step + ":record", () => {
        active();
        for (const [index, target] of inventory.targets.entries()) if (target.scope.media === media) updateTarget(ctx.repo, id, index, progress => {
          if (discovered.failure) progress.providers = [{ key: `discovery:${media}`, provider: discovered.failure.provider,
            status: discovered.failure.retryAt === null ? "error" : "waiting", queued: 0, skipped: 0,
            error: discovered.failure.message, retryAt: discovered.failure.retryAt }];
          else if (progress.providers.every(provider => provider.key.startsWith("discovery:")))
            progress.providers = discovered.sources.map(provider => ({ ...provider, status: "pending", queued: 0, skipped: 0, error: null, retryAt: null }));
        }, step + ":record");
      });
    }
    for (const [index, target] of inventory.targets.entries()) for (const provider of providers[target.scope.media] ?? []) {
        const step = `backfill:search:${index}:${provider.key}:g${generation}:${round}`;
        const progress = await durableStep(ctx, step + ":progress", () => targetSchema.parse(ctx.repo.getJsonState(targetKey(id, index))).providers.find(p => p.key === provider.key)!);
        if (progress.status === "completed") continue;
        const attempt = await durableStep(ctx, step, async () => {
          active();
          const { media, targetId } = target.scope;
          const movieTarget = media === "movie" ? ctx.repo.movies.target(targetId) : null;
          const movie = movieTarget ? ctx.repo.movies.movie(movieTarget.movie_id) : null;
          const tvTarget = media === "tv" ? ctx.repo.tv.target(targetId) : null;
          const entry = tvTarget ? ctx.repo.tv.entry(tvTarget.entry_id) : null;
          const book = media === "audio" || media === "ebook" ? ctx.repo.getBookRow(targetId) : null;
          const owned = new Set(entry ? ctx.repo.tv.activeEpisodes(targetId).map(ep => ep.episode_id) : []);
          const tv = entry ? tvSearchEvidence(entry, entry.episodes.filter(ep => owned.has(ep.id))) : null;
          const evidence = movie ? movieQueryEvidence(movie) : tv?.evidence ??
            { media, query: target.query, title: target.title, author: book?.author };
          const options = movie ? { ids: movieSearchIds(movie) } : tv?.options;
          const cacheKey = `${key(id)}:search:${createHash("sha256").update(JSON.stringify([evidence, options, provider.key])).digest("hex")}`;
          type Cached = { generation: number | null; round: number; entries: ReturnType<BooksRepo["releaseInbox"]["retainSearchResults"]>; failure: import("./indexer-errors").IndexerFailure | null };
          const cached = ctx.repo.getJsonState<Cached>(cacheKey);
          if (cached && (!cached.failure || cached.failure.retryAt !== null && cached.failure.retryAt > Date.now() || cached.failure.retryAt === null && cached.generation === generation && cached.round >= round))
            return cached;
          const remember = (entries: Cached["entries"], failure: Cached["failure"]) => {
            active(); const result = { generation, round, entries, failure }; ctx.repo.setJsonState(cacheKey, result); return result;
          };
          let batch: Awaited<ReturnType<IndexerGateway["search"]>>;
          try {
            const source = (await sourceList(target.scope.media)).find(source => feedIdentity(source, target.scope.media) === provider.key);
            if (!source) return remember([], indexerFailure(provider.provider, 400, null));
            const plan = await planIndexerQuery(ctx.getSettings(), evidence, ctx.modelProvider);
            batch = await gateway.search([source], plan.primaryQuery, media, { limit: state.limitPerProvider,
              ...options, alternateQuery: plan.alternateQuery,
              onRequest: mode => recordIndexerTraffic(ctx.repo, "reuse", mode === "id" ? "search_id" : "search_text") });
          } catch (error) { if (isRunControlError(error)) throw error; return remember([], error instanceof IndexerError ? error.failure : indexerFailure(provider.provider, null, Date.now() + 60_000)); }
          active();
          return ctx.repo.transaction(() => {
            ctx.repo.releaseInbox.ensureFeed(provider.key, provider.provider, target.scope.media, new Date().toISOString());
            return remember(ctx.repo.releaseInbox.retainSearchResults(provider.key, batch.results.slice(0, state.limitPerProvider), new Date().toISOString()), batch.failures[0] ?? null);
          });
        });
        let queued = 0, skipped = 0;
        for (let offset = 0; offset < attempt.entries.length; offset += 25) {
          const result = await route(ctx, id, target, attempt.entries.slice(offset, offset + 25), step + ":batch:" + offset);
          queued += result.queued; skipped += result.skipped;
        }
        await durableStep(ctx, step + ":record", () => {
          active(); updateTarget(ctx.repo, id, index, target => {
            const current = target.providers.find(p => p.key === provider.key)!;
            current.queued += queued; current.skipped += skipped;
            current.status = attempt.failure ? attempt.failure.retryAt === null ? "error" : "waiting" : "completed";
            current.error = attempt.failure?.message ?? null; current.retryAt = attempt.failure?.retryAt ?? null;
          }, step + ":record");
        });
        if (attempt.failure?.retryAt !== null && attempt.failure?.retryAt !== undefined) waits.push(attempt.failure.retryAt);
      }
    let fresh = false;
    const summary = await durableStep(ctx, `backfill:round:${round}:finish`, () => {
      fresh = true; active(); const current = backfillSummarySchema.parse(ctx.repo.getJsonState(key(id)));
      current.status = waits.length ? "waiting" : current.errors ? "failed" : "completed";
      current.updatedAt = new Date().toISOString(); ctx.repo.setJsonState(key(id), current); return current;
    });
    if (summary.status === "completed") return "done";
    if (!waits.length && fresh) throw new Error(`Cross-seed backfill has ${summary.errors} provider errors; retry after fixing indexer configuration`);
    if (waits.length) await ctx.execution.sleep(`backfill:wait:${round}`, Math.max(1, Math.min(...waits) - Date.now()));
  }
}
