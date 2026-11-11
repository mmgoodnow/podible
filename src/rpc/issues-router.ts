import { createScreenIssueSchema, listScreenIssuesSchema, screenIssueInboxSchema, screenIssuePageSchema,
  screenIssueSchema } from "../issues/contracts";
import { defineMethod, defineRouter } from "./framework";
import { RpcError } from "./shared";

function assertReportOrigin(request: Request) {
  const origin = request.headers.get("origin"), proto = request.headers.get("x-forwarded-proto");
  const external = new URL(request.url);
  if (proto === "https" || proto === "http") external.protocol = `${proto}:`;
  if ((proto !== null && proto !== "https" && proto !== "http") || (origin !== null && origin !== external.origin) ||
    request.headers.get("sec-fetch-site") === "cross-site") throw new RpcError(-32003, "Cross-origin mutation rejected");
}

export const issuesRouter = defineRouter({
  create: defineMethod({
    auth: "user", summary: "File a movie or TV report and record a local, non-destructive first check.",
    paramsSchema: createScreenIssueSchema, resultSchema: screenIssueSchema,
    handler(ctx, input) {
      // tRPC already checks POST origins; the legacy JSON-RPC transport does not.
      assertReportOrigin(ctx.request);
      const report = ctx.repo.screenIssues.create(ctx.session!.user_id, input);
      if (!report) throw new RpcError(-32000, "A report cannot be attached to that title or episode.", { error: "not_found" });
      return report;
    },
  }),
  listMine: defineMethod({
    auth: "user", readOnly: true, summary: "Read the signed-in user's own movie and TV reports.",
    paramsSchema: listScreenIssuesSchema, resultSchema: screenIssuePageSchema,
    handler(ctx, input) { return ctx.repo.screenIssues.listMine(ctx.session!.user_id, input); },
  }),
  inbox: defineMethod({
    auth: "admin", readOnly: true, summary: "Read filed movie and TV reports for manual review. Does not apply fixes.",
    paramsSchema: screenIssueInboxSchema, resultSchema: screenIssuePageSchema,
    handler(ctx, input) { return ctx.repo.screenIssues.inbox(input); },
  }),
});
