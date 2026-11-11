import { createScreenIssueSchema, diagnoseScreenIssueSchema, listScreenIssuesSchema, screenIssueDiagnosisRequestResultSchema,
  screenIssueInboxPageSchema, screenIssueInboxSchema, screenIssuePageSchema, screenIssueSchema } from "../issues/contracts";
import { queueScreenIssueDiagnosis } from "../issues/diagnosis-job";
import { defineMethod, defineRouter } from "./framework";
import { RpcError } from "./shared";

function assertIssueMutationOrigin(request: Request) {
  const origin = request.headers.get("origin"), proto = request.headers.get("x-forwarded-proto");
  const external = new URL(request.url);
  if (proto === "https" || proto === "http") external.protocol = `${proto}:`;
  if ((proto !== null && proto !== "https" && proto !== "http") || (origin !== null && origin !== external.origin) ||
    request.headers.get("sec-fetch-site") === "cross-site") throw new RpcError(-32003, "Cross-origin mutation rejected");
}

export const issuesRouter = defineRouter({
  create: defineMethod({
    auth: "user", summary: "File a movie or TV report, record a local check, and queue non-destructive diagnosis.",
    paramsSchema: createScreenIssueSchema, resultSchema: screenIssueSchema,
    handler(ctx, input) {
      // tRPC already checks POST origins; the legacy JSON-RPC transport does not.
      assertIssueMutationOrigin(ctx.request);
      const report = ctx.repo.screenIssues.create(ctx.session!.user_id, input, reportId => queueScreenIssueDiagnosis(ctx.repo, reportId));
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
    paramsSchema: screenIssueInboxSchema, resultSchema: screenIssueInboxPageSchema,
    handler(ctx, input) { return ctx.repo.screenIssues.inbox(input); },
  }),
  diagnose: defineMethod({
    auth: "admin", summary: "Read a saved assessment or get or enqueue its diagnostic job. No recovery is run.",
    paramsSchema: diagnoseScreenIssueSchema, resultSchema: screenIssueDiagnosisRequestResultSchema,
    handler(ctx, { reportId }) {
      assertIssueMutationOrigin(ctx.request);
      return ctx.repo.transaction(() => {
        const input = ctx.repo.screenIssues.diagnosisInput(reportId);
        if (!input) throw new RpcError(-32000, "Issue report not found.", { error: "not_found" });
        if (input.saved) return { status: "assessed", assessment: input.saved } as const;
        return { status: "job", jobId: queueScreenIssueDiagnosis(ctx.repo, reportId).id } as const;
      });
    },
  }),
});
