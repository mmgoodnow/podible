import { RtorrentClient } from "../rtorrent";

import { defineMethod, defineRouter } from "./framework";
import { emptyParamsSchema, positiveIntSchema } from "./schemas";
import { enrichDownload, RpcError } from "./shared";

export const downloadsRouter = defineRouter({
  downloads: defineRouter({
    list: defineMethod({
      auth: "admin",
      readOnly: true,
      summary: "List download jobs with release status/progress.",
      paramsSchema: emptyParamsSchema,
      async handler(ctx) {
        const downloads = ctx.repo.listDownloads();
        const hasDownloading = downloads.some((download) => download.release_status === "downloading" && download.info_hash);
        const client = hasDownloading ? new RtorrentClient(ctx.repo.getSettings().rtorrent) : null;
        const enriched = await Promise.all(downloads.map((download) => enrichDownload(download, client)));
        return { downloads: enriched };
      },
    }),

    get: defineMethod({
      auth: "admin",
      readOnly: true,
      summary: "Get one download job with live progress if active.",
      paramsSchema: emptyParamsSchema.extend({
        jobId: positiveIntSchema,
      }),
      async handler(ctx, params) {
        const download = ctx.repo.getDownload(params.jobId);
        if (!download) {
          throw new RpcError(-32000, "Download not found", { error: "not_found", jobId: params.jobId });
        }
        const client =
          download.release_status === "downloading" && download.info_hash
            ? new RtorrentClient(ctx.repo.getSettings().rtorrent)
            : null;
        return enrichDownload(download, client);
      },
    }),

    retry: defineMethod({
      auth: "admin",
      summary: "Requeue a download job.",
      paramsSchema: emptyParamsSchema.extend({
        jobId: positiveIntSchema,
      }),
      async handler(ctx, params) {
        return { job: ctx.repo.retryJob(params.jobId) };
      },
    }),
  }),
});
