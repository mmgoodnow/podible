import { z } from "zod";
import { buildManifestationChapters } from "../library/media";
import { defineMethod, defineRouter } from "./framework";
import { positiveIntSchema } from "./schemas";
import { RpcError } from "./shared";

const bookParams = z.object({ bookId: z.number().int().positive() });

// Browser-only read models. Mutations continue to use the public domain RPCs.
export const bookWebRouter = defineRouter({
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
      const jobs = (["acquire", "download", "import"] as const)
        .flatMap((type) => ctx.repo.listJobsByType(type))
        .filter((job) => job.book_id === bookId)
        .sort((a, b) => b.id - a.id);
      return {
        activeAcquisition: jobs.some(
          (job) =>
            job.type === "acquire" &&
            (job.status === "queued" || job.status === "running"),
        ),
        jobs: jobs.slice(0, 12).map((job) => {
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
          .filter((edition) => edition.kind === "audio")
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
        target.manifestation.kind !== "audio"
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
