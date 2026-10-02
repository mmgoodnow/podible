import path from "node:path";
import { mkdir, writeFile } from "node:fs/promises";

const base = await Bun.file("tmp/transcription-eval/excerpts/manifest.json").json();
const glossary = await Bun.file("tmp/transcription-eval/glossary/manifest.json").json();
const audioDir = path.resolve("tmp/transcription-eval/speed2x/audio");
await mkdir(audioDir, { recursive: true });
for (const sample of base.samples) {
  const audioPath = path.join(audioDir, `${sample.id}.m4a`);
  // Read the same source interval rather than re-encoding the 1x AAC excerpt.
  const proc = Bun.spawn(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", String(sample.startSeconds),
    "-t", "1200", "-i", path.resolve("tmp/chapter-analysis-corpus/library", sample.sourcePath),
    "-vn", "-af", "atempo=2", "-ac", "1", "-ar", "24000", "-c:a", "aac", "-b:a", "64k", audioPath],
    { stdout: "inherit", stderr: "inherit" });
  if (await proc.exited !== 0) throw new Error(`Failed to prepare ${sample.id}`);
  const probe = Bun.spawn(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", audioPath], { stdout: "pipe" });
  const duration = Number((await new Response(probe.stdout).text()).trim());
  if (await probe.exited !== 0 || Math.abs(duration - 600) > 1) throw new Error(`Expected ten-minute sped-up clip, got ${duration}`);
  sample.audioPath = audioPath;
  sample.submittedDurationSeconds = duration;
  sample.speedMultiplier = 2;
  const prompted = glossary.samples.find((row: { id: string }) => row.id === sample.id);
  if (!prompted) throw new Error(`Missing glossary sample ${sample.id}`);
  prompted.audioPath = audioPath;
  prompted.submittedDurationSeconds = duration;
  prompted.speedMultiplier = 2;
  console.log(JSON.stringify({ event: "sped-up-excerpt-prepared", sample: sample.id, duration }));
}
for (const [condition, manifest] of [["no-glossary", base], ["glossary", glossary]] as const) {
  const output = path.resolve("tmp/transcription-eval/speed2x", condition);
  await mkdir(output, { recursive: true });
  await writeFile(path.join(output, "manifest.json"), JSON.stringify(manifest, null, 2));
  console.log(path.join(output, "manifest.json"));
}
