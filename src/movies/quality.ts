import { z } from "zod";
import { assessCompatibleUpgrade } from "../media/quality-decision";
import { normalizeManifestationLanguageCode } from "../library/language";

const sourceSchema = z.enum(["web", "bluray", "hdtv", "dvd", "cam"]);
const languageNames = new Intl.DisplayNames(["en"], {
  type: "language",
  fallback: "none",
});
function languageCode(value: string): string | null {
  try {
    const code = new Intl.Locale(
      normalizeManifestationLanguageCode(value) ?? value,
    ).language;
    return ["und", "mul", "zxx"].includes(code) || !languageNames.of(code)
      ? null
      : code;
  } catch {
    return null;
  }
}
export const movieQualityPolicySchema = z
  .object({
    tiers: z
      .array(
        z.object({
          source: sourceSchema,
          resolution: z.union([
            z.literal(480),
            z.literal(720),
            z.literal(1080),
            z.literal(2160),
          ]),
        }),
      )
      .min(1),
    cut: z.string().trim().min(1).nullable(),
    audioLanguages: z.array(z.string().trim().min(2)),
    allowedCodecs: z.array(z.string().trim().min(1)).min(1),
    preferredCodecs: z.array(z.string().trim().min(1)).default([]),
    allowHdr: z.boolean(),
    minimumPreferenceGain: z.number().int().positive(),
    // Preserve serialized policy identity for existing movie and TV targets.
    preferProper: z.boolean().default(true),
  })
  .strict();
export type MovieQualityPolicy = z.infer<typeof movieQualityPolicySchema>;
export const defaultMovieQualityPolicy: MovieQualityPolicy = {
  tiers: [
    { source: "hdtv", resolution: 720 },
    { source: "web", resolution: 720 },
    { source: "bluray", resolution: 720 },
    { source: "web", resolution: 1080 },
    { source: "bluray", resolution: 1080 },
  ],
  cut: null,
  audioLanguages: [],
  allowedCodecs: ["h264", "hevc", "av1"],
  preferredCodecs: [],
  allowHdr: true,
  minimumPreferenceGain: 10,
  preferProper: true,
};
export const movieFactsSchema = z
  .object({
    identityVerified: z.boolean(),
    cut: z.string().nullable(),
    source: sourceSchema.nullable(),
    width: z.number().int().positive().nullable(),
    height: z.number().int().positive().nullable(),
    videoCodec: z.string().nullable(),
    dynamicRange: z.enum(["sdr", "hdr"]).nullable(),
    audioLanguages: z.array(z.string()),
    proper: z.boolean().nullable(),
  })
  .strict();
export type MovieFacts = z.infer<typeof movieFactsSchema>;
export type QualityEvaluation = {
  eligibility: "eligible" | "ineligible" | "needs-verification";
  tier: number | null;
  preferenceScore: number;
  reasons: string[];
  breakdown: { rule: string; contribution: number }[];
};

/** Ordered tiers are policy, not a weighted sum that could override a hard constraint. */
export function evaluateMovieQuality(
  rawPolicy: MovieQualityPolicy,
  rawFacts: MovieFacts,
): QualityEvaluation {
  const policy = movieQualityPolicySchema.parse(rawPolicy);
  const facts = movieFactsSchema.parse(rawFacts);
  const rejected: string[] = [];
  const missing: string[] = [];
  if (!facts.identityVerified) missing.push("movie identity is not verified");
  if (policy.cut !== null) {
    if (facts.cut === null) missing.push("required cut is unknown");
    else if (facts.cut !== policy.cut) rejected.push("wrong cut");
  }
  if (!facts.videoCodec) missing.push("video codec is unknown");
  else if (!policy.allowedCodecs.includes(facts.videoCodec))
    rejected.push("video codec is not allowed");
  if (!facts.dynamicRange && !policy.allowHdr)
    missing.push("dynamic range is unknown");
  else if (facts.dynamicRange === "hdr" && !policy.allowHdr)
    rejected.push("HDR is not allowed");
  const languages = facts.audioLanguages.map(languageCode);
  if (
    !policy.audioLanguages.every((language) => languageCode(language) !== null)
  )
    throw new Error("Policy requires recognized language codes");
  if (policy.audioLanguages.length && !languages.length)
    missing.push("audio language is unknown");
  else if (
    policy.audioLanguages.length &&
    !policy.audioLanguages.some((language) =>
      languages.includes(languageCode(language)),
    )
  ) {
    if (languages.includes(null))
      missing.push(
        "unidentified audio track may satisfy the language requirement",
      );
    else rejected.push("required audio language is missing");
  }
  let tier: number | null = null;
  if (!facts.width || !facts.height || !facts.source)
    missing.push("source or resolution is unknown");
  else {
    const resolution =
      facts.width >= 3840 || facts.height >= 2160
        ? 2160
        : facts.width >= 1920 || facts.height >= 1080
          ? 1080
          : facts.width >= 1280 || facts.height >= 720
            ? 720
            : 480;
    const index = policy.tiers.findIndex(
      (entry) =>
        entry.source === facts.source && entry.resolution === resolution,
    );
    if (index < 0) rejected.push("source/resolution tier is not allowed");
    else tier = index;
  }
  const codecIndex =
    facts.videoCodec === null
      ? -1
      : policy.preferredCodecs.indexOf(facts.videoCodec);
  const breakdown = [
    {
      rule: "preferred codec",
      contribution:
        codecIndex < 0 ? 0 : policy.preferredCodecs.length - codecIndex,
    },
    {
      rule: "proper/repack",
      contribution: facts.proper === true ? 10 : 0,
    },
  ];
  return {
    eligibility: rejected.length
      ? "ineligible"
      : missing.length
        ? "needs-verification"
        : "eligible",
    tier,
    preferenceScore: breakdown.reduce(
      (sum, entry) => sum + entry.contribution,
      0,
    ),
    reasons: [...rejected, ...missing],
    breakdown,
  };
}

export function assessMovieUpgrade(
  policy: MovieQualityPolicy,
  current: MovieFacts,
  candidate: MovieFacts,
) {
  const from = evaluateMovieQuality(policy, current);
  const to = evaluateMovieQuality(policy, candidate);
  const tierGain = to.tier !== null && from.tier !== null ? to.tier - from.tier : 0;
  const preferenceGain = to.preferenceScore - from.preferenceScore;
  const { upgrade } = assessCompatibleUpgrade({
    compatible: to.eligibility === "eligible", verified: to.eligibility === "eligible",
    improvement: from.eligibility !== "eligible" || tierGain > 0 ||
      (tierGain === 0 && preferenceGain >= policy.minimumPreferenceGain) ? "better" : tierGain < 0 ? "worse" : "equal",
  });
  if (to.eligibility !== "eligible")
    return { upgrade: false, reason: to.eligibility, from, to };
  if (from.eligibility !== "eligible")
    return { upgrade, reason: "replace noncompliant version", from, to };
  if (tierGain > 0)
    return { upgrade, reason: "higher quality tier", from, to };
  if (tierGain < 0)
    return { upgrade: false, reason: "lower quality tier", from, to };
  return {
    upgrade,
    reason: upgrade
      ? "same-tier preference improvement"
      : "keep current version",
    from,
    to,
  };
}
