import { observedFiles } from "../media/observed-files";
import type { JobRow, AppSettings } from "../app-types";
import type { BooksRepo } from "../repo";
import { TEXT_AGENT_MODEL } from "../ai-models";
import { durableStep, type WorkerContext } from "../worker/context";
import { createMovieDecider } from "./agents";
import type { MovieDecider } from "./operations";
import { factsFromMovieProbe, probeMovie } from "./probe";
import { evaluateMovieQuality, movieFactsSchema } from "./quality";
import { RadarrObserver } from "./shadow-radarr";
import {
  shadowInputSchema,
  type ShadowInput,
  type ShadowObservation,
  type ShadowResult,
} from "./shadow-types";

export type MovieShadowServices = {
  observe(): Promise<ShadowObservation[]>;
  inspect: MovieDecider["inspect"];
  probe: typeof probeMovie;
};
export function configuredShadowServices(
  ctx: WorkerContext,
): MovieShadowServices {
  const settings = ctx.getSettings();
  const observer = new RadarrObserver(settings.movies.shadow);
  return {
    observe: () => observer.observe(),
    inspect: createMovieDecider(settings, ctx.modelProvider).inspect,
    probe: probeMovie,
  };
}
export async function shadowInventory(
  observation: ShadowObservation,
  settings: AppSettings["movies"],
): Promise<ShadowInput> {
  const inventory = await observedFiles(observation, settings.shadow.pathMappings);
  return {
    observation,
    policy: settings.qualityPolicy,
    ...inventory,
    // Preserve the existing movie fingerprint contract while TV uses richer stats.
    files: inventory.files.map(({ path, name, sizeBytes, mtimeMs }) => ({ path, name, sizeBytes, mtimeMs })),
  };
}
export function queueShadowCase(repo: BooksRepo, caseId: number) {
  return repo.movieShadow.transaction(() => {
    const job = repo.createJob({
      type: "movie_shadow_evaluate",
      payload: { caseId },
      idempotencyKey: `movie-shadow:${caseId}`,
      maxAttempts: 3,
    });
    repo.movieShadow.attachJob(caseId, job.id);
    return job;
  });
}
export async function syncMovieShadow(
  ctx: WorkerContext,
  services = ctx.movieShadowServices ?? configuredShadowServices(ctx),
) {
  const settings = ctx.getSettings();
  if (!settings.movies.shadow.enabled)
    throw new Error("Movie shadow mode is disabled");
  // GETs are safe to repeat. Do not journal the whole lookback on every poll;
  // immutable case fingerprints, not duplicate history snapshots, dedupe work.
  const observations = await services.observe();
  let created = 0;
  for (const observation of observations) {
    if (created >= settings.movies.shadow.maxCasesPerSync) break;
    const input = await shadowInventory(observation, settings.movies);
    ctx.repo.movieShadow.transaction(() => {
      const captured = ctx.repo.movieShadow.capture(input);
      if (captured.created) {
        queueShadowCase(ctx.repo, captured.caseId);
        created++;
      }
    });
  }
  return "done" as const;
}
export async function evaluateMovieShadow(
  ctx: WorkerContext,
  job: JobRow,
  services = ctx.movieShadowServices ?? configuredShadowServices(ctx),
) {
  if (!ctx.getSettings().movies.shadow.enabled)
    throw new Error("Movie shadow mode is disabled");
  const caseId = JSON.parse(job.payload_json ?? "{}").caseId;
  const row = ctx.repo.movieShadow.get(caseId);
  if (!row || row.jobId !== job.id)
    throw new Error("Unknown shadow case execution");
  if (row.result) return "done" as const;
  const input = shadowInputSchema.parse(row.input);
  const result: ShadowResult = {
    decision: "unobservable",
    selectedFile: null,
    reason:
      "Source files are missing or unavailable; this is not evidence of a wrong release",
    facts: null,
    quality: null,
    probes: [],
    probeErrors: [],
    model: TEXT_AGENT_MODEL,
    evaluatedAt: new Date().toISOString(),
  };
  const current = await shadowInventory(
    input.observation,
    ctx.getSettings().movies,
  );
  if (JSON.stringify(current.files) !== JSON.stringify(input.files)) {
    result.reason =
      "Source files changed since capture; a later poll will create a new evidence snapshot";
  } else if (input.files.length) {
    for (const [index, file] of input.files.entries()) {
      try {
        const probe = await durableStep(ctx, `shadow:probe:${index}`, () =>
          services.probe(file.path),
        );
        result.probes.push({ index, name: file.name, probe });
      } catch {
        result.probeErrors.push({
          name: file.name,
          message: "File probe failed or timed out",
        });
      }
    }
    if (result.probes.length) {
      const offer = {
        key: "shadow-candidate",
        title: input.observation.releaseTitle,
        provider: "Radarr shadow",
        url: "",
        infoHash: null,
        sizeBytes: null,
        seeders: null,
        attributes: {},
      };
      const claims = movieFactsSchema.parse({
        identityVerified: false,
        cut: null,
        source: null,
        width: null,
        height: null,
        videoCodec: null,
        dynamicRange: null,
        audioLanguages: [],
        proper: null,
      });
      const decision = await durableStep(ctx, "shadow:inspect", () =>
        services.inspect({
          movie: { id: caseId, ...input.observation.movie },
          policy: input.policy,
          offer,
          files: result.probes.map((p) => ({
            ...p,
            sizeBytes: input.files[p.index]!.sizeBytes,
          })),
          selection: {
            offerKey: offer.key,
            facts: claims,
            reason:
              "Independent read-only import review; no prior selection decision",
          },
        }),
      );
      result.reason = decision.reason;
      if (decision.fileIndex === null) result.decision = "reject";
      else {
        const file = result.probes.find((f) => f.index === decision.fileIndex);
        if (!file)
          throw new Error("Shadow agent selected an unknown file index");
        result.selectedFile = file.name;
        result.facts = factsFromMovieProbe(file.probe, decision.facts);
        result.quality = evaluateMovieQuality(input.policy, result.facts);
        result.decision =
          result.quality.eligibility === "eligible"
            ? "import"
            : result.quality.eligibility === "ineligible"
              ? "reject"
              : "needs-verification";
      }
    } else
      result.reason = "No candidate has readable audio/video probe evidence";
  }
  ctx.repo.movieShadow.finish(caseId, result);
  return "done" as const;
}
