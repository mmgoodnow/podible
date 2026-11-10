import { z } from "zod";
import { movieQualityPolicySchema, type MovieQualityPolicy } from "./quality";

const languageNames = new Intl.DisplayNames(["en"], { type: "language", fallback: "none" });
const language = z.string().trim().min(2).max(35).transform((value, ctx) => {
  try {
    const code = new Intl.Locale(value).language;
    if (["und", "mul", "zxx"].includes(code) || !languageNames.of(code)) throw new Error();
    return code;
  } catch { ctx.addIssue({ code: "custom", message: "Use a recognized audio language code, such as en or ja." }); return z.NEVER; }
});
export const movieVersionSpecSchema = z.object({
  resolution: z.union([z.literal(480), z.literal(720), z.literal(1080), z.literal(2160)]).optional(),
  cut: z.string().trim().min(1).max(100).transform(value => value.toLowerCase()).optional(),
  audioLanguage: language.optional(),
  sdrOnly: z.literal(true).optional(),
}).strict().refine(value => Object.values(value).some(value => value !== undefined), "Choose at least one version requirement.");

/** A separate desired target, not a request to replace another target's active version. */
export function policyForMovieVersion(base: MovieQualityPolicy, spec: z.infer<typeof movieVersionSpecSchema>): MovieQualityPolicy {
  const policy = movieQualityPolicySchema.parse(base);
  return movieQualityPolicySchema.parse({ ...policy,
    tiers: spec.resolution === undefined ? policy.tiers : [...new Set(policy.tiers.map(tier => tier.source))]
      .map(source => ({ source, resolution: spec.resolution! })),
    cut: spec.cut ?? policy.cut,
    audioLanguages: spec.audioLanguage ? [spec.audioLanguage] : policy.audioLanguages,
    allowHdr: spec.sdrOnly ? false : policy.allowHdr,
  });
}

export function movieVersionLabel(policy: MovieQualityPolicy): string {
  const resolutions = [...new Set(policy.tiers.map(tier => tier.resolution))].sort((a, b) => a - b);
  return [resolutions.map(value => value === 2160 ? "4K" : `${value}p`).join(" / "),
    policy.cut, policy.audioLanguages.map(code => languageNames.of(code) ?? code).join(" or "),
    policy.allowHdr ? null : "SDR"].filter(Boolean).join(" · ");
}
