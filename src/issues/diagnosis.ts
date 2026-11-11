import OpenAI, { type ClientOptions } from "openai";
import { zodTextFormat } from "openai/helpers/zod";
import { TEXT_AGENT_MODEL } from "../ai-models";
import type { AppSettings } from "../app-types";
import type { BooksRepo } from "../repo";
import { currentEffects, effectCheckpoint } from "../runtime/execution-effects";
import { screenIssueAssessmentSchema, screenIssueCheckForFocus, screenIssueDecisionSchema, screenIssueEvidenceSchema,
  type ScreenIssueAssessment, type ScreenIssueEvidence } from "./contracts";

type Assess = (evidence: ScreenIssueEvidence) => Promise<unknown>;
type DiagnosisResult = { status: "assessed"; assessment: ScreenIssueAssessment } | { status: "unavailable" | "failed" };

const instructions = `Classify the focus for a reported movie or TV issue using only the supplied evidence.
The filed and current findings are snapshots of local publication records, not tests of Plex or file presence.
For tv-show, a publication record means at least one episode, not that the entire show is present.
A historical loss does not establish current file loss. Do not claim to have verified the cause or run a repair.
Base the focus on currentFinding; use filedFinding for context if the record changed.
- recorded-content-loss: currentFinding is content-loss-recorded; the loss record is historical, not a current verification.
- publication-gap: currentFinding is no-publication-recorded; the absence of a record is not proof of a missing file.
- library-availability-unverified: publication is recorded but a missing/playback report warrants checking library links.
- playback-unverified: publication is recorded but a missing/playback symptom should be reproduced in Plex.
- tracks-unverified: publication is recorded and the report is about audio-subtitles.
- identity-unverified: publication is recorded and the report is about wrong-content.
- inconclusive: the evidence is insufficient for a more specific focus.
Return only the focus code. Do not suggest commands or repairs.`;

export function createScreenIssueAssessor(
  settings: Pick<AppSettings["agents"], "apiKey" | "timeoutMs">, fetcher?: ClientOptions["fetch"],
): Assess {
  const timeout = Math.min(15_000, Math.max(1000, settings.timeoutMs));
  return async evidence => {
    const input = JSON.stringify(screenIssueEvidenceSchema.parse(evidence));
    const client = new OpenAI({ apiKey: settings.apiKey, timeout, maxRetries: 0, logLevel: "off", fetch: fetcher });
    const result = await client.responses.parse({
      model: TEXT_AGENT_MODEL, instructions, input, tools: [], tool_choice: "none", store: false, max_output_tokens: 512,
      text: { format: zodTextFormat(screenIssueDecisionSchema, "screen_issue_focus") },
    }, { signal: currentEffects()?.signal });
    return result.status === "completed" ? result.output_parsed : null;
  };
}

export async function diagnoseScreenIssue(repo: BooksRepo, reportId: number, assess?: Assess): Promise<DiagnosisResult | null> {
  const issues = repo.screenIssues;
  const input = issues.diagnosisInput(reportId);
  if (!input) return null;
  if (input.saved) return { status: "assessed", assessment: input.saved };
  if (!assess) {
    const settings = repo.readSettings()?.agents;
    if (!settings?.apiKey.trim()) return { status: "unavailable" };
    assess = createScreenIssueAssessor(settings);
  }

  let assessment;
  try {
    const decision = screenIssueDecisionSchema.parse(await assess(input.evidence));
    assessment = screenIssueAssessmentSchema.parse({ ...decision, suggestedCheck: screenIssueCheckForFocus[decision.focus],
      evidence: input.evidence, checkedAt: input.checkedAt });
  } catch {
    effectCheckpoint();
    // Provider errors and rejected output can contain private request details.
    return { status: "failed" };
  }
  const saved = issues.saveAssessment(reportId, assessment);
  return saved ? { status: "assessed", assessment: saved } : null;
}
