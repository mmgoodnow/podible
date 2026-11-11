import { z } from "zod";
import type { ChapterAnalysisRow } from "../app-types";
import { buildManifestationChapters } from "../library/media";
import { audioEditionSchema, audioEditionStatusSchema, PENDING_AUDIO_EDITION_SCORE, queueAudioEditionRequest, readAudioEditionStatus } from "../library/audio-editions";
import { defineMethod, defineRouter } from "./framework";
import { positiveIntSchema } from "./schemas";
import { RpcError } from "./shared";

const bookParams = z.object({ bookId: z.number().int().positive() });
const analysisSchema = z.object({
  status: z.string(),
  source: z.string(),
  algorithmVersion: z.string(),
  resolvedBoundaries: z.number(),
  totalBoundaries: z.number(),
  hasChapters: z.boolean(),
  error: z.string().nullable(),
  curationStatus: z.string().nullable(),
  budget: z.string().nullable(),
  unresolvedCount: z.number(),
  unresolvedNodes: z.array(z.object({
    title: z.string(),
    failureKind: z.string(),
    expectedStartTime: z.number().nullable(),
    errors: z.array(z.string()),
  })),
  eventLogPath: z.string().nullable(),
  traceDir: z.string().nullable(),
});

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : {};
}
function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}
function number(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
function analysisSummary(analysis: ChapterAnalysisRow | null) {
  if (!analysis) return null;
  let debug: Record<string, unknown> = {};
  try { debug = record(JSON.parse(analysis.debug_json ?? "{}")); } catch { /* Old debug reports may be incomplete. */ }
  const curation = record(debug.curation);
  const summary = record(curation.summary);
  const budget = record(summary.budget);
  const unresolved = Array.isArray(summary.unresolvedNodes) ? summary.unresolvedNodes : [];
  return {
    status: analysis.status,
    source: analysis.source,
    algorithmVersion: analysis.algorithm_version,
    resolvedBoundaries: analysis.resolved_boundary_count,
    totalBoundaries: analysis.total_boundary_count,
    hasChapters: Boolean(analysis.chapters_json),
    error: analysis.error,
    curationStatus: text(curation.status) ?? text(summary.status),
    budget: Object.keys(budget).length
      ? `${number(budget.agentCallsStarted) ?? "?"}/${number(budget.maxAgentCalls) ?? "?"} agent calls, ${number(budget.agentNodeTasksStarted) ?? "?"}/${number(budget.maxAgentNodeTasks) ?? "?"} hard nodes${budget.exhausted ? `; exhausted: ${text(budget.exhaustedReason) ?? "yes"}` : ""}`
      : null,
    unresolvedCount: unresolved.length,
    unresolvedNodes: unresolved.slice(0, 12).map((value) => {
      const node = record(value);
      return {
        title: text(node.title) ?? "Untitled",
        failureKind: text(node.failureKind) ?? "unknown",
        expectedStartTime: number(node.expectedStartTime),
        errors: Array.isArray(node.errors)
          ? node.errors.filter((error): error is string => typeof error === "string").slice(0, 3)
          : [],
      };
    }),
    eventLogPath: text(curation.eventLogPath),
    traceDir: text(curation.traceDir),
  };
}

export const bookWebRouter = defineRouter({
  audioEditionRequests: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "Read safe acquisition status for the supported audiobook performances.",
    paramsSchema: bookParams.strict(),
    resultSchema: z.object({ enabled: z.boolean(), requests: z.array(audioEditionStatusSchema) }),
    handler(ctx, { bookId }) {
      if (!ctx.repo.getBookRow(bookId)) throw new RpcError(-32000, "Book not found", { error: "not_found" });
      return {
        enabled: Boolean(ctx.repo.readSettings()?.agents.apiKey.trim()),
        requests: audioEditionSchema.options.map((edition) => readAudioEditionStatus(ctx.repo, bookId, edition)),
      };
    },
  }),
  requestAudioEdition: defineMethod({
    auth: "user",
    summary: "Request a specific audiobook performance without replacing current playback.",
    paramsSchema: bookParams.extend({ edition: audioEditionSchema }).strict(),
    resultSchema: audioEditionStatusSchema,
    handler(ctx, { bookId, edition }) {
      if (!ctx.repo.getBookRow(bookId)) throw new RpcError(-32000, "Book not found", { error: "not_found" });
      if (!ctx.repo.readSettings()?.agents.apiKey.trim())
        throw new RpcError(-32602, "Targeted audiobook search is not configured");
      return queueAudioEditionRequest(ctx.repo, bookId, edition);
    },
  }),
  diagnostics: defineMethod({
    auth: "admin",
    readOnly: true,
    summary: "Inspect a book's manifestation tree and bounded curation diagnostics.",
    paramsSchema: bookParams,
    resultSchema: z.object({
      manifestations: z.array(z.object({
        id: positiveIntSchema,
        kind: z.enum(["audio", "ebook"]),
        label: z.string().nullable(),
        editionNote: z.string().nullable(),
        selectionNote: z.string().nullable(),
        language: z.string().nullable(),
        durationMs: z.number().nullable(),
        sizeBytes: z.number(),
        preferredScore: z.number(),
        analysis: analysisSchema.nullable(),
        containers: z.array(z.object({
          id: positiveIntSchema,
          kind: z.string(),
          mimeType: z.string(),
          sequence: z.number(),
          importNote: z.string().nullable(),
          release: z.object({
            id: positiveIntSchema,
            title: z.string(),
            provider: z.string(),
            status: z.string(),
          }).nullable(),
          files: z.array(z.object({
            id: positiveIntSchema,
            path: z.string(),
            sourcePath: z.string().nullable(),
            sizeBytes: z.number(),
            durationMs: z.number(),
          })),
        })),
      })),
    }),
    handler(ctx, { bookId }) {
      if (!ctx.repo.getBookRow(bookId))
        throw new RpcError(-32000, "Book not found", { error: "not_found" });
      return {
        manifestations: ctx.repo.listManifestationsByBook(bookId).map((edition) => ({
          id: edition.id,
          kind: edition.kind,
          label: edition.label,
          editionNote: edition.edition_note,
          selectionNote: edition.selection_note,
          language: edition.language,
          durationMs: edition.duration_ms,
          sizeBytes: edition.total_size,
          preferredScore: edition.preferred_score,
          analysis: analysisSummary(ctx.repo.getChapterAnalysis(edition.id)),
          containers: ctx.repo.listAssetsByManifestation(edition.id).map((asset) => {
            const release = asset.source_release_id ? ctx.repo.getRelease(asset.source_release_id) : null;
            return {
              id: asset.id,
              kind: asset.kind,
              mimeType: asset.mime,
              sequence: asset.sequence_in_manifestation,
              importNote: asset.import_note,
              release: release ? { id: release.id, title: release.title, provider: release.provider, status: release.status } : null,
              files: ctx.repo.getAssetFiles(asset.id).map((file) => ({
                id: file.id,
                path: file.path,
                sourcePath: file.source_path,
                sizeBytes: file.size,
                durationMs: file.duration_ms,
              })),
            };
          }),
        })),
      };
    },
  }),
  activity: defineMethod({
    auth: "user",
    readOnly: true,
    summary:
      "Show book acquisition progress without exposing provider credentials or logs.",
    paramsSchema: bookParams,
    resultSchema: z.object({
      activeAcquisition: z.boolean(),
      jobs: z.array(
        z.object({
          id: positiveIntSchema,
          type: z.string(),
          status: z.enum([
            "queued",
            "running",
            "succeeded",
            "failed",
            "cancelled",
          ]),
          state: z.enum([
            "waiting_provider",
            "scheduled",
            "needs_attention",
            "failed",
            "queued",
            "running",
            "succeeded",
            "cancelled",
          ]),
          message: z.string(),
          nextRunAt: z.string().nullable(),
          updatedAt: z.string(),
        }),
      ),
    }),
    handler(ctx, { bookId }) {
      if (!ctx.repo.getBookRow(bookId))
        throw new RpcError(-32000, "Book not found", { error: "not_found" });
      const { jobs, activeAcquisition } = ctx.repo.readBookActivity(bookId);
      return {
        activeAcquisition,
        jobs: jobs.map((job) => {
          const unavailable =
            /429|rate.?limit|too many requests|temporarily unavailable/i.test(
              job.error ?? "",
            );
          const providerAuth =
            /401|403|credential|unauthorized|forbidden/i.test(job.error ?? "");
          const blocked = job.status === "failed" || job.status === "queued";
          const scheduled =
            job.status === "queued" &&
            job.next_run_at !== null &&
            Date.parse(job.next_run_at) > Date.now();
          const state =
            blocked && unavailable
              ? ("waiting_provider" as const)
              : blocked && providerAuth
                ? ("needs_attention" as const)
                : scheduled
                  ? ("scheduled" as const)
                  : job.status;
          return {
            id: job.id,
            type: job.type,
            status: job.status,
            state,
            message:
              state === "waiting_provider"
                ? "The search provider is unavailable or rejecting requests. An administrator may need to check its health and credentials before retrying."
                : state === "scheduled"
                  ? "Waiting before the next scheduled attempt. No need to start another acquisition."
                  : state === "needs_attention"
                    ? "The provider could not authorize this request. An administrator may need to refresh its credentials."
                    : job.status === "failed"
                      ? "This step could not finish. You can retry acquisition or ask an administrator to inspect the job."
                      : job.status === "queued"
                        ? "Queued for processing."
                        : job.status === "running"
                          ? "Working on your editions."
                          : job.status === "succeeded"
                            ? "This step completed."
                            : "This step was cancelled.",
            nextRunAt: job.next_run_at,
            updatedAt: job.updated_at,
          };
        }),
      };
    },
  }),
  editions: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "List downloadable ebook editions with server-owned media URLs.",
    paramsSchema: bookParams,
    resultSchema: z.object({
      audio: z.array(
        z.object({
          manifestationId: positiveIntSchema,
          releaseIds: z.array(positiveIntSchema),
        }),
      ),
      ebooks: z.array(
        z.object({
          assetId: positiveIntSchema,
          manifestationId: positiveIntSchema.nullable(),
          releaseId: positiveIntSchema.nullable(),
          label: z.string(),
          editionNote: z.string().nullable(),
          mimeType: z.string(),
          sizeBytes: z.number(),
          downloadUrl: z.string(),
        }),
      ),
    }),
    handler(ctx, { bookId }) {
      if (!ctx.repo.getBookRow(bookId))
        throw new RpcError(-32000, "Book not found", { error: "not_found" });
      return {
        audio: ctx.repo
          .listManifestationsByBook(bookId)
          .filter((edition) => edition.kind === "audio" && edition.preferred_score !== PENDING_AUDIO_EDITION_SCORE)
          .map((edition) => ({
            manifestationId: edition.id,
            releaseIds: Array.from(
              new Set(
                ctx.repo
                  .listAssetsByManifestation(edition.id)
                  .flatMap((asset) =>
                    asset.source_release_id ? [asset.source_release_id] : [],
                  ),
              ),
            ),
          })),
        ebooks: ctx.repo
          .listAssetsByBook(bookId)
          .filter(
            (asset) =>
              asset.mime === "application/epub+zip" ||
              asset.mime === "application/pdf",
          )
          .map((asset) => {
            const manifestation = asset.manifestation_id
              ? ctx.repo.getManifestation(asset.manifestation_id)
              : null;
            return {
              assetId: asset.id,
              manifestationId: asset.manifestation_id,
              releaseId: asset.source_release_id,
              label:
                manifestation?.label ||
                (asset.mime === "application/pdf"
                  ? "PDF edition"
                  : "EPUB edition"),
              editionNote: manifestation?.edition_note ?? null,
              mimeType: asset.mime,
              sizeBytes: asset.total_size,
              downloadUrl: new URL(`/ebook/${asset.id}`, ctx.request.url).href,
            };
          }),
      };
    },
  }),
  chapters: defineMethod({
    auth: "user",
    readOnly: true,
    summary: "Preview chapters for an audio edition belonging to this book.",
    paramsSchema: bookParams.extend({
      manifestationId: z.number().int().positive(),
    }),
    resultSchema: z.object({
      chapters: z.array(z.object({ startTime: z.number(), title: z.string() })),
    }),
    async handler(ctx, { bookId, manifestationId }) {
      const target = ctx.repo.getManifestationWithContainers(manifestationId);
      if (
        !target ||
        target.manifestation.book_id !== bookId ||
        target.manifestation.kind !== "audio" ||
        target.manifestation.preferred_score === PENDING_AUDIO_EDITION_SCORE
      ) {
        throw new RpcError(-32000, "Audio edition not found", {
          error: "not_found",
        });
      }
      const result = await buildManifestationChapters(
        ctx.repo,
        target.manifestation,
        target.containers,
      );
      return { chapters: result?.chapters ?? [] };
    },
  }),
});
