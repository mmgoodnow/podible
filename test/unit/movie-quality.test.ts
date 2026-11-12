import { expect, test } from "bun:test";
import {
  assessMovieUpgrade,
  defaultMovieQualityPolicy,
  evaluateMovieQuality,
  type MovieFacts,
  type MovieQualityPolicy,
} from "../../src/movies/quality";

const policy: MovieQualityPolicy = {
  tiers: [
    { source: "web", resolution: 720 },
    { source: "web", resolution: 1080 },
    { source: "bluray", resolution: 1080 },
  ],
  cut: "theatrical",
  audioLanguages: ["en"],
  allowedCodecs: ["h264", "hevc"],
  preferredCodecs: ["hevc", "h264"],
  allowHdr: false,
  minimumPreferenceGain: 5,
  preferProper: true,
};
const facts: MovieFacts = {
  identityVerified: true,
  cut: "theatrical",
  source: "web",
  width: 1280,
  height: 534,
  videoCodec: "h264",
  dynamicRange: "sdr",
  audioLanguages: ["eng"],
  proper: false,
};

test("quality tiers handle cropped movie dimensions and language aliases", () => {
  expect(evaluateMovieQuality(policy, facts)).toMatchObject({
    eligibility: "eligible",
    tier: 0,
  });
  expect(
    evaluateMovieQuality(policy, { ...facts, width: 1920, height: 800 }),
  ).toMatchObject({ eligibility: "eligible", tier: 1 });
  expect(
    evaluateMovieQuality(
      { ...policy, audioLanguages: ["ru"] },
      { ...facts, audioLanguages: ["rus"] },
    ).eligibility,
  ).toBe("eligible");
  expect(
    assessMovieUpgrade(policy, facts, { ...facts, width: 1920, height: 800 })
      .upgrade,
  ).toBe(true);
});
test("default policy automates valid films without inventing cut, language or HDR restrictions", () => {
  expect(evaluateMovieQuality(defaultMovieQualityPolicy, { ...facts, cut: null, dynamicRange: null, audioLanguages: [] }).eligibility).toBe("eligible");
  expect(evaluateMovieQuality(defaultMovieQualityPolicy, { ...facts, cut: "uncut", audioLanguages: ["hin"], dynamicRange: "hdr" }).eligibility).toBe("eligible");
  expect(evaluateMovieQuality(defaultMovieQualityPolicy, { ...facts, identityVerified: false }).eligibility).toBe("needs-verification");
  expect(evaluateMovieQuality(policy, { ...facts, cut: null }).eligibility).toBe("needs-verification");
});
test("wrong identity/cut/language, unknown facts and device incompatibility cannot be scored into an upgrade", () => {
  for (const change of [
    { cut: "director" },
    { audioLanguages: ["spa"] },
    { videoCodec: "unsupported" },
    { dynamicRange: "hdr" as const },
    { width: 3840 },
  ]) {
    expect(
      assessMovieUpgrade(policy, facts, { ...facts, ...change, proper: true })
        .upgrade,
    ).toBe(false);
  }
  expect(
    evaluateMovieQuality(policy, { ...facts, identityVerified: false })
      .eligibility,
  ).toBe("needs-verification");
  expect(
    evaluateMovieQuality(policy, { ...facts, source: null }).eligibility,
  ).toBe("needs-verification");
  expect(
    evaluateMovieQuality(policy, { ...facts, audioLanguages: [] }).eligibility,
  ).toBe("needs-verification");
  expect(
    evaluateMovieQuality(policy, { ...facts, audioLanguages: ["spa", "und"] })
      .eligibility,
  ).toBe("needs-verification");
});
test("preference scores cannot beat a tier, ties do not churn, and policy changes reevaluate facts", () => {
  const better = { ...facts, width: 1920, height: 800 };
  expect(
    assessMovieUpgrade(policy, better, { ...facts, proper: true }).upgrade,
  ).toBe(false);
  expect(assessMovieUpgrade(policy, facts, facts).upgrade).toBe(false);
  expect(
    assessMovieUpgrade(policy, facts, { ...facts, videoCodec: "hevc" }).upgrade,
  ).toBe(false);
  expect(
    assessMovieUpgrade(policy, facts, { ...facts, proper: true }).upgrade,
  ).toBe(true);
  expect(
    assessMovieUpgrade({ ...policy, audioLanguages: ["es"] }, facts, {
      ...facts,
      audioLanguages: ["spa"],
    }).upgrade,
  ).toBe(true);
});
test("older policies cannot turn off preference for an otherwise eligible proper", () => {
  const legacyPolicy = { ...policy, preferProper: false };
  expect(assessMovieUpgrade(legacyPolicy, facts, { ...facts, proper: true }).upgrade).toBe(true);
  expect(assessMovieUpgrade(legacyPolicy, facts, { ...facts, proper: true, cut: "director" }).upgrade).toBe(false);
});
