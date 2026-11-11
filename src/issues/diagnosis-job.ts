import type { JobRow } from "../app-types";
import type { BooksRepo } from "../repo";
import { durableStep, type WorkerContext } from "../worker/context";
import { diagnoseScreenIssueSchema } from "./contracts";
import { diagnoseScreenIssue } from "./diagnosis";

export function queueScreenIssueDiagnosis(repo: BooksRepo, reportId: number): JobRow {
  return repo.createJob({ type: "screen_issue_diagnosis", payload: { reportId },
    idempotencyKey: `screen-issue-diagnosis:${reportId}`, maxAttempts: 1 });
}

export async function processScreenIssueDiagnosisJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  await durableStep(ctx, "screen-issue-assessment", async () => {
    const { reportId } = readPayload(job.payload_json);
    const result = await diagnoseScreenIssue(ctx.repo, reportId);
    ctx.execution?.assertActive();
    if (result?.status === "unavailable") throw new Error("Screen issue diagnosis is not configured");
    if (result?.status === "failed") throw new Error("Screen issue diagnosis failed");
    // Missing reports are harmless, and completed assessments are reused on replay.
    return true;
  });
  return "done";
}

function readPayload(payload: string | null) {
  try { return diagnoseScreenIssueSchema.parse(JSON.parse(payload ?? "null")); }
  catch { throw new Error("Invalid screen issue diagnosis job payload"); }
}
