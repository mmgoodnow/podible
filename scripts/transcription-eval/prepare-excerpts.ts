import path from "node:path";
import { mkdir, writeFile } from "node:fs/promises";

const output = path.resolve("tmp/transcription-eval/excerpts");
await mkdir(output, { recursive: true });
const sources = [
  { id: "dungeon-crawler-carl", title: "Dungeon Crawler Carl", source: "Matt Dinniman/Dungeon Crawler Carl/Dungeon Crawler Carl.m4b", start: 600 },
  { id: "project-hail-mary", title: "Project Hail Mary", source: "Andy Weir/Project Hail Mary/Project Hail Mary/01 - Chapter 1.mp3", start: 0 },
  { id: "twilight", title: "Twilight", source: "Stephenie Meyer/Twilight/Twilight.m4b", start: 600 },
];
const samples = [];
for (const source of sources) {
  const audioPath = path.join(output, `${source.id}.m4a`);
  const proc = Bun.spawn(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", String(source.start),
    "-i", path.resolve("tmp/chapter-analysis-corpus/library", source.source), "-t", "1200", "-vn", "-ac", "1", "-ar", "24000",
    "-c:a", "aac", "-b:a", "64k", audioPath], { stdout: "inherit", stderr: "inherit" });
  if (await proc.exited !== 0) throw new Error(`Failed to extract ${source.id}`);
  const probe = Bun.spawn(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", audioPath], { stdout: "pipe" });
  const duration = Number((await new Response(probe.stdout).text()).trim());
  if (await probe.exited !== 0 || Math.abs(duration - 1200) > 1) throw new Error(`Expected twenty minutes: ${source.id}, got ${duration}`);
  samples.push({ id: source.id, title: source.title, audioPath, language: "en", sourcePath: source.source,
    startSeconds: source.start, durationSeconds: duration });
  console.log(JSON.stringify({ event: "excerpt-prepared", sample: source.id, duration }));
}
await writeFile(path.join(output, "manifest.json"), JSON.stringify({ samples }, null, 2));
console.log(path.join(output, "manifest.json"));
