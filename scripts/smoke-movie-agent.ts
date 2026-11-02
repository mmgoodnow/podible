import { createMovieDecider } from "../src/movies/agents";
import { defaultSettings } from "../src/settings";
import { TEXT_AGENT_MODEL } from "../src/ai-models";
import { promises as fs } from "node:fs";
if (!process.argv.includes("--live"))
  throw new Error("Pass --live to authorize a real model request");
const key = process.env.OPENAI_API_KEY?.trim();
if (!key) throw new Error("No local OpenAI key configured");
const settings = defaultSettings({
  agents: { apiKey: key, timeoutMs: 60_000 },
});
const decider = createMovieDecider(settings);
const start = Date.now();
const result = await decider.select({
  movie: {
    id: 1,
    tmdbId: 286217,
    title: "The Martian",
    originalTitle: "The Martian",
    year: 2015,
    overview: "An astronaut stranded on Mars tries to survive.",
    originalLanguage: "en",
    imdbId: "tt3659388",
    posterUrl: null,
  },
  policy: settings.movies.qualityPolicy,
  current: null,
  offers: [
    {
      key: "wrong-cut",
      title: "The Martian 2015 EXTENDED CUT 1080p BluRay English H264 SDR",
      provider: "Synthetic fixture",
      sizeBytes: 8 * 1024 ** 3,
      seeders: 10,
      infoHash: null,
      url: "https://example.invalid/not-a-download",
      attributes: {},
    },
    {
      key: "correct",
      title: "The Martian 2015 THEATRICAL 1080p WEB-DL English H264 SDR",
      provider: "Synthetic fixture",
      sizeBytes: 6 * 1024 ** 3,
      seeders: 5,
      infoHash: null,
      url: "https://example.invalid/not-a-download",
      attributes: {},
    },
  ],
});
if (result.offerKey !== "correct")
  throw new Error(
    "Live movie agent did not select the eligible theatrical release",
  );
const report = {
  checkedAt: new Date().toISOString(),
  model: TEXT_AGENT_MODEL,
  elapsedMs: Date.now() - start,
  result,
};
await fs.mkdir("tmp/movies-lab", { recursive: true });
await fs.writeFile(
  "tmp/movies-lab/live-agent-smoke.json",
  JSON.stringify(report, null, 2),
);
console.log(
  JSON.stringify({
    model: TEXT_AGENT_MODEL,
    elapsedMs: report.elapsedMs,
    selectedEligibleRelease: true,
  }),
);
