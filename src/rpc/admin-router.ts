import { defineMethod, defineRouter } from "./framework";
import { emptyParamsSchema } from "./schemas";
import { removeFileIfPresent } from "./shared";

export const adminRouter = defineRouter({
  wipeDatabase: defineMethod({
    auth: "admin",
    summary: "Delete all mutable DB data and imported files/covers (local dev reset).",
    paramsSchema: emptyParamsSchema,
    async handler(ctx) {
      const artifacts = ctx.repo.getWipeArtifacts();
      const wiped = ctx.repo.wipeDatabase();

      const deletedAssetPaths: string[] = [];
      for (const filePath of artifacts.assetPaths) {
        if (await removeFileIfPresent(filePath)) {
          deletedAssetPaths.push(filePath);
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
        deletedCoverFileCount: deletedCoverPaths.length,
        deletedCoverPaths,
      };
    },
  }),
});
