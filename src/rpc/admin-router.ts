import { defineMethod, defineRouter } from "./framework";
import { emptyParamsSchema } from "./schemas";
import { z } from "zod";
import { removeFileIfPresent } from "./shared";

export const adminRouter = defineRouter({
  wipeDatabase: defineMethod({
    auth: "admin",
    summary: "Delete all mutable DB data and imported files/covers (local dev reset).",
    paramsSchema: emptyParamsSchema,
    resultSchema: z.object({
      deleted: z.object({
        books: z.number().int().nonnegative(),
        releases: z.number().int().nonnegative(),
        assets: z.number().int().nonnegative(),
        assetFiles: z.number().int().nonnegative(),
        jobs: z.number().int().nonnegative(),
        torrentCache: z.number().int().nonnegative(),
        chapterAnalysis: z.number().int().nonnegative(),
        assetTranscripts: z.number().int().nonnegative(),
        users: z.number().int().nonnegative(),
        sessions: z.number().int().nonnegative(),
        plexLoginAttempts: z.number().int().nonnegative(),
        appLoginAttempts: z.number().int().nonnegative(),
        authCodes: z.number().int().nonnegative(),
        appState: z.number().int().nonnegative(),
      }),
      settingsPreserved: z.boolean(),
      deletedAssetFileCount: z.number().int().nonnegative(),
      deletedAssetPaths: z.array(z.string()),
      deletedTranscriptFileCount: z.number().int().nonnegative(),
      deletedTranscriptPaths: z.array(z.string()),
      deletedCoverFileCount: z.number().int().nonnegative(),
      deletedCoverPaths: z.array(z.string()),
    }),
    async handler(ctx) {
      const artifacts = ctx.repo.getWipeArtifacts();
      const wiped = ctx.repo.wipeDatabase();

      const deletedAssetPaths: string[] = [];
      for (const filePath of artifacts.assetPaths) {
        if (await removeFileIfPresent(filePath)) {
          deletedAssetPaths.push(filePath);
        }
      }

      const deletedTranscriptPaths: string[] = [];
      for (const filePath of artifacts.transcriptPaths) {
        if (await removeFileIfPresent(filePath)) {
          deletedTranscriptPaths.push(filePath);
        }
      }

      const deletedCoverPaths: string[] = [];
      for (const filePath of artifacts.coverPaths) {
        if (await removeFileIfPresent(filePath)) {
          deletedCoverPaths.push(filePath);
        }
      }

      return {
        ...wiped,
        deletedAssetFileCount: deletedAssetPaths.length,
        deletedAssetPaths,
        deletedTranscriptFileCount: deletedTranscriptPaths.length,
        deletedTranscriptPaths,
        deletedCoverFileCount: deletedCoverPaths.length,
        deletedCoverPaths,
      };
    },
  }),
});
