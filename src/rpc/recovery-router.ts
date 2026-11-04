import { RecoveryService } from "../media/recovery";
import {
  recoveryInventoryParamsSchema, recoveryInventorySchema, recoveryPreviewParamsSchema, recoveryPreviewSchema,
  recoveryRecoverParamsSchema, recoveryRecoverSchema,
} from "../media/recovery-types";
import { defineMethod, defineRouter } from "./framework";

/** Standalone: parent composition wires this router; no scheduler or registry side effects. */
export const recoveryRouter = defineRouter({
  inventory: defineMethod({
    auth: "admin", readOnly: true,
    summary: "Inspect a bounded page of owned movie/TV recovery evidence without changing intent or files.",
    paramsSchema: recoveryInventoryParamsSchema, resultSchema: recoveryInventorySchema,
    handler: (ctx, params) => new RecoveryService(ctx.repo).inventory(params),
  }),
  preview: defineMethod({
    auth: "admin", readOnly: true,
    summary: "Preview at most 20 namespaced recovery targets; missing mounts block recovery.",
    paramsSchema: recoveryPreviewParamsSchema, resultSchema: recoveryPreviewSchema,
    handler: (ctx, params) => new RecoveryService(ctx.repo).preview(params),
  }),
  recover: defineMethod({
    auth: "admin",
    summary: "Explicitly revalidate recovery: add missing view links or enqueue supported acquisition through the outbox.",
    paramsSchema: recoveryRecoverParamsSchema, resultSchema: recoveryRecoverSchema,
    handler: (ctx, params) => new RecoveryService(ctx.repo).recover(params),
  }),
});
