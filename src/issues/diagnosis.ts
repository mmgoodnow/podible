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

const instructions = `Choose a focus from currentFinding; use filedFinding for context if it changed. These are publication records, not live Plex/file checks. A show record covers at least one episode, not necessarily the whole show. Historical loss or absent records don't prove current loss. Don't claim a verified cause or repair.
content-loss-recorded -> recorded-content-loss; no-publication-recorded -> publication-gap.
If publication is recorded: missing/playback -> library-availability-unverified if links need checking, playback-unverified if reproduction in Plex is needed; audio-subtitles -> tracks-unverified; wrong-content -> identity-unverified.
Use inconclusive if evidence is insufficient. Return focus only, no commands or repairs.`;

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
