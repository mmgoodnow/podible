import path from "node:path";
import { promises as fs } from "node:fs";
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
const within = (root: string, file: string) =>
  file === root || file.startsWith(root + path.sep);
export async function shadowInventory(
  observation: ShadowObservation,
  settings: AppSettings["movies"],
): Promise<ShadowInput> {
  const result: ShadowInput = {
    observation,
    policy: settings.qualityPolicy,
    files: [],
    source: "unavailable",
    warnings: [],
  };
  for (const [index, remote] of observation.sourcePaths.entries()) {
    const mapping = [...settings.shadow.pathMappings]
      .sort((a, b) => b.remoteRoot.length - a.remoteRoot.length)
      .find((m) => within(path.resolve(m.remoteRoot), path.resolve(remote)));
    if (
      !mapping ||
      !path.isAbsolute(remote) ||
      !path.isAbsolute(mapping.remoteRoot) ||
      !path.isAbsolute(mapping.localRoot)
    ) {
      result.warnings.push(
        "Source path has no absolute, explicit shadow mapping",
      );
      continue;
    }
    const root = await fs.realpath(mapping.localRoot).catch(() => null);
    if (!root) {
      result.warnings.push("Mapped source root is unavailable");
      continue;
    }
    const source = path.resolve(
      mapping.localRoot,
      path.relative(mapping.remoteRoot, remote),
    );
    let visited = 0;
    let truncated = false;
    const walk = async (file: string, depth: number): Promise<void> => {
      if (++visited > 5000 || depth > 6 || result.files.length >= 50) {
        truncated = true;
        return;
      }
      const stat = await fs.lstat(file);
      const real = await fs.realpath(file);
      if (stat.isSymbolicLink() || !within(root, real))
        throw new Error("Unsafe shadow source");
      if (stat.isDirectory()) {
        for (const name of (await fs.readdir(file)).sort()) {
          if (truncated) break;
          await walk(path.join(file, name), depth + 1);
        }
      } else if (
        stat.isFile() &&
        /\.(mkv|mp4|m4v|avi|mov|ts|m2ts|mpg|mpeg)$/i.test(file)
      ) {
        result.files.push({
          path: real,
          name: path.relative(source, file) || path.basename(file),
          sizeBytes: stat.size,
          mtimeMs: stat.mtimeMs,
        });
      }
    };
    try {
      await walk(source, 0);
      if (truncated) {
        result.files = [];
        result.warnings.push(
          "Shadow inventory exceeded safety bounds; manual inspection required",
        );
        return result;
      }
      if (result.files.length) {
        result.source = index === 0 ? "download" : "library-fallback";
        return result;
      }
    } catch (error) {
      result.files = [];
      result.warnings.push(
        (error as NodeJS.ErrnoException).code === "ENOENT"
          ? "Source is missing or has moved"
          : "Source cannot be inspected safely",
      );
    }
  }
  return result;
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
