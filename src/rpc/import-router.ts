import path from "node:path";

import { importReleaseFromPath, inspectImportPath } from "../importer";

import { defineMethod, defineRouter } from "./framework";
import {
  emptyParamsSchema,
  mediaSchema,
  nonEmptyStringSchema,
  optionalStringArraySchema,
  optionalStringSchema,
  positiveIntSchema,
} from "./schemas";
import { RpcError, uniqueManualInfoHash } from "./shared";

export const importRouter = defineRouter({
  reconcile: defineMethod({
    auth: "admin",
    summary: "Queue reconcile job for downloaded releases missing assets.",
    paramsSchema: emptyParamsSchema,
    async handler(ctx) {
      const job = ctx.repo.createJob({ type: "reconcile" });
      return { jobId: job.id };
    },
  }),

  inspect: defineMethod({
    auth: "admin",
    readOnly: true,
    summary: "Inspect a local path and list candidate import files.",
    paramsSchema: emptyParamsSchema.extend({
      path: nonEmptyStringSchema,
    }),
    async handler(_ctx, params) {
      return {
        path: params.path.trim(),
        files: await inspectImportPath(params.path.trim()),
      };
    },
  }),

  manual: defineMethod({
    auth: "admin",
    summary: "Create a manual release and import from a local path.",
    paramsSchema: emptyParamsSchema.extend({
      bookId: positiveIntSchema,
      mediaType: mediaSchema,
      path: nonEmptyStringSchema,
      selectedPaths: optionalStringArraySchema,
      title: optionalStringSchema,
    }),
    async handler(ctx, params) {
      const sourcePath = params.path.trim();
      const title = params.title?.trim() || path.basename(sourcePath);
      const book = ctx.repo.getBookRow(params.bookId);
      if (!book) {
        throw new RpcError(-32000, "Book not found", { error: "not_found", bookId: params.bookId });
      }

      const release = ctx.repo.createRelease({
        bookId: params.bookId,
        provider: "manual",
        providerGuid: null,
        title,
        mediaType: params.mediaType,
        infoHash: uniqueManualInfoHash(params.bookId, params.mediaType, sourcePath),
        sizeBytes: null,
        url: sourcePath,
        status: "downloaded",
      });

      try {
        const imported = await importReleaseFromPath(ctx.repo, release, sourcePath, ctx.repo.getSettings().libraryRoot, {
          selectedPaths: params.selectedPaths,
        });
        const finalRelease = ctx.repo.setReleaseStatus(release.id, "imported", null);
        return {
          release: finalRelease,
          assetId: imported.assetId,
          linkedFiles: imported.linkedFiles,
        };
      } catch (error) {
        const message = (error as Error).message || "Manual import failed";
        ctx.repo.setReleaseStatus(release.id, "failed", message);
        throw new RpcError(-32000, "Manual import failed", { message, sourcePath, mediaType: params.mediaType });
      }
    },
  }),
});
