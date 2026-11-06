import { z } from "zod";
import { assessCompatibleUpgrade } from "../media/quality-decision";

const compatibility = z.enum(["compatible", "incompatible", "unknown", "not-applicable"]);
export const bookQualityAssessmentSchema = z.object({
  requirements: z.object({
    work: compatibility, language: compatibility, translation: compatibility,
    narration: compatibility, abridgement: compatibility, edition: compatibility,
  }).strict(),
  improvement: z.enum(["better", "equal", "worse", "unknown"]),
  reason: z.string().trim().min(1).max(3000),
}).strict();
export type BookQualityAssessment = z.infer<typeof bookQualityAssessmentSchema>;

export function assessBookQuality(raw: BookQualityAssessment, verified = true) {
  const assessment = bookQualityAssessmentSchema.parse(raw);
  const requirements = assessment.requirements;
  const compatible = requirements.work === "compatible" && requirements.language === "compatible" &&
    Object.values(requirements).every(value => value === "compatible" || value === "not-applicable");
  return { ...assessCompatibleUpgrade({ compatible, verified, improvement: assessment.improvement }), assessment };
}

export type BookQualityEvidence = {
  title: string; author: string; description: string | null; media: "audio" | "ebook";
  editionPreference: string;
  current: unknown;
  candidate: unknown;
  phase: "release" | "imported";
};
