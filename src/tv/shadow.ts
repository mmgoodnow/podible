import type { AppSettings, JobRow } from "../app-types";
import type { BooksRepo } from "../repo";
import { TEXT_AGENT_MODEL } from "../ai-models";
import { observedFiles } from "../media/observed-files";
import { durableStep, type WorkerContext } from "../worker/context";
import { factsFromMovieProbe, movieProbeSchema, probeMovie } from "../movies/probe";
import { evaluateMovieQuality } from "../movies/quality";
import { createTvDecider, validateTvImportPlan, type TvDecider } from "./agents";
import { SonarrObserver, type SonarrObservation } from "./sonarr";
import { ANILIST_URL, ANIME_LINKS_URL, ANIME_MAPPING_URL, enrichAnimeEntry, type AnimeCatalogProjection } from "./anime";
import type { TvCatalogEntry } from "./types";
import {
  tvShadowInputSchema, tvShadowObservationSchema, tvShadowResultSchema,
  type TvShadowInput, type TvShadowResult,
} from "./shadow-types";

export type TvShadowObservation = SonarrObservation & { sourceKey?: string; warnings?: string[] };
export type TVShadowServices = {
  observe(): Promise<TvShadowObservation[]>;
  inspect: TvDecider["inspect"];
  probe: typeof probeMovie;
};
export type TvShadowServices = TVShadowServices;
type Context = WorkerContext & { tvShadowServices?: TVShadowServices };

export function configuredTvShadowServices(ctx: WorkerContext): TVShadowServices {
  const settings = ctx.getSettings();
  const observer = new SonarrObserver({ url: settings.tv.sonarrUrl, apiKey: settings.tv.sonarrApiKey, lookbackDays: settings.tv.lookbackDays });
  return { observe: async () => {
    const observations: TvShadowObservation[] = [];
    for (const observation of await observer.observe()) observations.push(...await projectTvShadowObservation(observation));
    return observations;
  }, inspect: createTvDecider(settings, ctx.modelProvider).inspect, probe: probeMovie };
}

/** A projected work may narrow an observation's membership, never invent coverage. */
export async function projectTvShadowObservation(
  observation: SonarrObservation,
  enrich: (entry: TvCatalogEntry) => Promise<AnimeCatalogProjection> = enrichAnimeEntry,
): Promise<TvShadowObservation[]> {
  if (!observation.entry.anime) return [observation];
  const fallback = (): TvShadowObservation[] => [{ ...observation, sourceKey: observation.key,
    warnings: ["Anime enrichment pending or ambiguous; original provider entry and exact episode membership retained"] }];
  try {
    const projection = await enrich(observation.entry);
    const wanted = new Set(observation.episodeKeys), assigned = new Set<string>();
    const sourceKeys = new Set(observation.entry.episodes.map((episode) => episode.key));
    const entries = new Set<string>(), observations: TvShadowObservation[] = [];
    for (const entry of projection.entries) {
      const episodeKeys = entry.episodes.filter((episode) => wanted.has(episode.key)).map((episode) => episode.key);
      if (!episodeKeys.length) continue;
      if (entries.has(entry.key) || episodeKeys.some((key) => assigned.has(key) || !sourceKeys.has(key))) return fallback();
      entries.add(entry.key);
      for (const key of episodeKeys) assigned.add(key);
      const projected = { ...observation, key: `${observation.key}:entry:${entry.key}`, sourceKey: observation.key,
        entry, episodeKeys, warnings: projection.warnings.map((warning) => `${warning.code}: ${warning.message}`) };
      if (!tvShadowObservationSchema.safeParse(projected).success) return fallback();
      observations.push(projected);
    }
    return assigned.size === wanted.size && observations.length ? observations : fallback();
  } catch { return fallback(); }
}

/** Persist/return neither connection secrets nor raw URL-bearing model/provider errors. */
export function redactTvShadow<T>(value: T, settings: AppSettings): T {
  const secrets = [settings.agents.apiKey, settings.tv.sonarrApiKey, settings.movies.seerr.apiKey,
    settings.movies.plexToken, settings.movies.tmdbAccessToken, settings.prowlarr.apiKey].filter(Boolean);
  const visit = (item: unknown): unknown => {
    if (typeof item === "string") {
      for (const secret of secrets) item = (item as string).replaceAll(secret, "[redacted]");
      return (item as string).replace(/https?:\/\/[^\s<>"']+/gi, (url) =>
        [ANIME_MAPPING_URL, ANIME_LINKS_URL, ANILIST_URL].includes(url.split("#")[0]!) ? url : "[redacted URL]")
        .replace(/\b(api[-_]?key|passkey|token|authorization)\s*[:=]\s*[^\s,;]+/gi, "$1=[redacted]");
    }
    if (Array.isArray(item)) return item.map(visit);
    if (item && typeof item === "object") return Object.fromEntries(Object.entries(item).map(([key, entry]) => [key, visit(entry)]));
    return item;
  };
  return visit(value) as T;
}

export function queueTvShadowCase(repo: BooksRepo, caseId: number) {
  return repo.tvShadow.transaction(() => {
    const job = repo.createJob({ type: "tv_shadow_evaluate", payload: { caseId }, idempotencyKey: `tv-shadow:${caseId}`, maxAttempts: 3 });
    repo.tvShadow.attachJob(caseId, job.id);
    return job;
  });
}

export async function syncTvShadow(ctx: Context, services = ctx.tvShadowServices ?? configuredTvShadowServices(ctx)) {
  const settings = ctx.getSettings();
  if (!settings.tv.sonarrUrl.trim() || !settings.tv.sonarrApiKey.trim()) throw new Error("Configure the server Sonarr connection for TV observation");
  // Never checkpoint the whole lookback: every pass must see current filesystem evidence.
  const observations = await services.observe();
  let created = 0;
  for (const raw of observations) {
    ctx.execution?.assertActive();
    if (created >= settings.tv.maxCasesPerSync) break;
    const parsed = tvShadowObservationSchema.safeParse(raw);
    if (!parsed.success) throw new Error("TV observation has malformed episode metadata");
    const observation = redactTvShadow(parsed.data, settings);
    const inventory = await observedFiles(observation, settings.tv.pathMappings);
    ctx.repo.tvShadow.transaction(() => {
      const entry = ctx.repo.tv.upsertEntry(observation.entry);
      const wanted = new Set(observation.episodeKeys);
      const episodeIds = entry.episodes.filter((episode) => wanted.has(episode.key)).map((episode) => episode.id).sort((a, b) => a - b);
      const input = tvShadowInputSchema.parse({ observation, entry, episodeIds, policy: settings.movies.qualityPolicy, ...inventory,
        warnings: [...inventory.warnings, ...(observation.warnings ?? [])] });
      const captured = ctx.repo.tvShadow.capture(input);
      if (captured.created) { queueTvShadowCase(ctx.repo, captured.caseId); created++; }
    });
  }
  return "done" as const;
}

const unchanged = async (input: TvShadowInput, settings: AppSettings) => {
  const current = await observedFiles(input.observation, settings.tv.pathMappings);
  return current.source === input.source && JSON.stringify(current.files) === JSON.stringify(input.files);
};
const changedReason = "Source files changed since capture; this snapshot is unobservable. A later observation must capture fresh evidence.";

export async function evaluateTvShadow(ctx: Context, job: JobRow, services = ctx.tvShadowServices ?? configuredTvShadowServices(ctx)) {
  let caseId: number;
  try { caseId = JSON.parse(job.payload_json ?? "{}").caseId; } catch { throw new Error("Invalid TV audit execution payload"); }
  if (!Number.isSafeInteger(caseId!) || caseId! < 1) throw new Error("Invalid TV audit case identity");
  const row = ctx.repo.tvShadow.get(caseId!);
  if (!row || row.jobId !== job.id || job.type !== "tv_shadow_evaluate") throw new Error("Unknown TV audit case execution");
  if (row.result) return "done" as const;
  const input = tvShadowInputSchema.parse(row.input), settings = ctx.getSettings();
  const result: TvShadowResult = {
    decision: "unobservable", reason: "Source files are unavailable; this is not evidence of a wrong release",
    files: [], unmappedEpisodeIds: [...input.episodeIds], probes: [], probeErrors: [],
    model: TEXT_AGENT_MODEL, evaluatedAt: new Date().toISOString(),
  };
  if (!await unchanged(input, settings)) result.reason = changedReason;
  else if (input.files.length) {
    for (const [index, file] of input.files.entries()) {
      ctx.execution?.assertActive();
      if (!await unchanged(input, settings)) { result.reason = changedReason; break; }
      try {
        const raw = await durableStep(ctx, `tv-shadow:probe:${index}`, () => services.probe(file.path));
        const probe = movieProbeSchema.parse(raw);
        const duration = Number(probe.format?.duration);
        if (!probe.streams.some((s) => s.codec_type === "video" && !s.disposition?.attached_pic) ||
            !probe.streams.some((s) => s.codec_type === "audio") || !Number.isFinite(duration) || duration <= 0)
          throw new Error("Missing playable stream or duration evidence");
        result.probes.push({ index, name: file.name, probe });
      } catch {
        ctx.execution?.assertActive();
        result.probeErrors.push({ name: file.name, message: "Candidate probe is unavailable, incomplete or timed out" });
      }
    }
    if (!await unchanged(input, settings)) result.reason = changedReason;
    else if (result.probes.length) {
      const episodes = input.entry.episodes.filter((episode) => input.episodeIds.includes(episode.id));
      const candidates = result.probes.map((probe) => ({ ...probe, sizeBytes: input.files[probe.index]!.sizeBytes }));
      let plan;
      try {
        plan = validateTvImportPlan(await durableStep(ctx, "tv-shadow:inspect", () => services.inspect({
          entry: input.entry, episodes, policy: input.policy,
          offer: { key: "tv-shadow-candidate", title: input.observation.releaseTitle, provider: "Read-only TV evidence",
            url: "", infoHash: null, sizeBytes: null, seeders: null, attributes: {} }, files: candidates,
        })), candidates, episodes);
      } catch {
        ctx.execution?.assertActive();
        throw new Error("TV audit inspection failed or returned an invalid episode mapping; no decision was recorded");
      }
      result.reason = plan.reason;
      for (const selected of plan.files) {
        const probe = result.probes.find((candidate) => candidate.index === selected.fileIndex)!;
        const facts = factsFromMovieProbe(probe.probe, selected.facts);
        result.files.push({ index: probe.index, name: probe.name, episodeIds: selected.episodeIds, reason: selected.reason,
          facts, quality: evaluateMovieQuality(input.policy, facts) });
      }
      const mapped = new Set(result.files.flatMap((file) => file.episodeIds));
      result.unmappedEpisodeIds = input.episodeIds.filter((id) => !mapped.has(id));
      if (!await unchanged(input, settings)) result.reason = changedReason;
      else if (!result.files.length || result.unmappedEpisodeIds.length || result.files.some((file) => !file.facts.identityVerified)) {
        result.reason = `Requested episode identity or coverage is incomplete; no rejection is established. ${plan.reason}`.slice(0, 4000);
      } else result.decision = result.files.some((file) => file.quality.eligibility === "needs-verification")
        ? "needs-verification" : result.files.some((file) => file.quality.eligibility === "ineligible") ? "reject" : "import";
    } else if (result.reason !== changedReason) result.reason = "No candidate has complete playable audio/video and duration evidence";
  }
  // A resumed checkpoint never authorizes a decision about changed source bytes.
  if (!await unchanged(input, settings)) { result.decision = "unobservable"; result.reason = changedReason; }
  ctx.execution?.assertActive();
  ctx.repo.tvShadow.finish(caseId!, job.id, tvShadowResultSchema.parse(redactTvShadow(result, settings)));
  return "done" as const;
}
