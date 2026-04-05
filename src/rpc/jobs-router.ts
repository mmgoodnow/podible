import { z } from "zod";

import { defineMethod, defineRouter } from "./framework";
import { emptyParamsSchema, jobRowSchema, jobTypeSchema, limitSchema, positiveIntSchema } from "./schemas";
import { enrichJob, RpcError } from "./shared";

const allJobTypes = ["full_library_refresh", "acquire", "download", "import", "reconcile", "chapter_analysis"] as const;

export const jobsRouter = defineRouter({
  list: defineMethod({
    auth: "admin",
    readOnly: true,
    summary: "List recent jobs (optionally filtered by type).",
    paramsSchema: emptyParamsSchema.extend({
      limit: limitSchema.optional(),
      type: z.preprocess((value) => {
        if (value === undefined || value === null || value === "") return undefined;
        return value;
      }, jobTypeSchema.optional()),
    }),
    resultSchema: z.object({
      jobs: z.array(jobRowSchema),
    }),
    async handler(ctx, params) {
      const jobs = params.type
        ? ctx.repo.listJobsByType(params.type)
        : allJobTypes.flatMap((jobType) => ctx.repo.listJobsByType(jobType));
      return {
        jobs: jobs
          .sort((a, b) => b.id - a.id)
          .slice(0, params.limit ?? 50)
          .map((job) => enrichJob(ctx, job)),
      };
    },
  }),

  get: defineMethod({
    auth: "admin",
    readOnly: true,
    summary: "Get one job row.",
    paramsSchema: emptyParamsSchema.extend({
      jobId: positiveIntSchema,
    }),
    resultSchema: z.object({
      job: jobRowSchema,
    }),
    async handler(ctx, params) {
      const job = ctx.repo.getJob(params.jobId);
      if (!job) {
        throw new RpcError(-32000, "Job not found", { error: "not_found", jobId: params.jobId });
      }
      return { job: enrichJob(ctx, job) };
    },
  }),

  retry: defineMethod({
    auth: "admin",
    summary: "Retry a failed/cancelled job.",
    paramsSchema: emptyParamsSchema.extend({
      jobId: positiveIntSchema,
    }),
    resultSchema: z.object({
      job: jobRowSchema,
    }),
    async handler(ctx, params) {
      const job = ctx.repo.getJob(params.jobId);
      if (!job) {
        throw new RpcError(-32000, "Job not found", { error: "not_found", jobId: params.jobId });
      }
      if (job.status !== "failed" && job.status !== "cancelled") {
        throw new RpcError(-32000, "Job is not retryable", {
          error: "not_retryable",
          jobId: params.jobId,
          status: job.status,
        });
      }
      return { job: ctx.repo.retryJob(params.jobId) };
    },
  }),
});
