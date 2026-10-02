import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { wordErrorRate } from "./wer";

type Sample = { id: string; title: string; audioPath: string; prompt?: string; language?: string;
  referencePath?: string; referenceStatus?: "audio-verified" | "epub-provisional" };
const manifestPath = process.argv[2];
if (!manifestPath) throw new Error("Usage: bun scripts/transcription-eval/run.ts <manifest.json>");
const manifest = await Bun.file(manifestPath).json() as { samples: Sample[] };
const outputDir = path.resolve(path.dirname(manifestPath), "results");
await mkdir(outputDir, { recursive: true });
const key = process.env.OPENAI_API_KEY;
if (!key) throw new Error("OPENAI_API_KEY is required");
const results: Array<{ sample: string; title: string; model: string; elapsedMs: number; outputPath: string;
  referenceStatus: string; score: ReturnType<typeof wordErrorRate> | null }> = [];
let persistence = Promise.resolve();
async function runSample(sample: Sample) {
  if (!/^[a-z0-9-]+$/.test(sample.id)) throw new Error("Sample id must be kebab case");
  const bytes = await readFile(sample.audioPath);
  const reference = sample.referencePath ? await readFile(sample.referencePath, "utf8") : null;
  for (const model of ["whisper-1", "gpt-transcribe"]) {
    const requestConfig = { model, prompt: sample.prompt ?? "", language: sample.language ?? "en", protocol: 1 };
    const fingerprint = createHash("sha256").update(bytes).update(JSON.stringify(requestConfig)).digest("hex");
    const outputPath = path.join(outputDir, `${sample.id}-${model}-${fingerprint.slice(0, 12)}.json`);
    let response: { text: string; elapsedMs: number; payload: unknown };
    if (await Bun.file(outputPath).exists()) response = await Bun.file(outputPath).json();
    else {
      const form = new FormData();
      form.set("model", model);
      form.set("file", new Blob([bytes]), path.basename(sample.audioPath));
      form.set("response_format", model === "whisper-1" ? "verbose_json" : "json");
      if (model === "whisper-1") {
        form.append("timestamp_granularities[]", "word");
        form.append("timestamp_granularities[]", "segment");
        form.set("language", requestConfig.language);
      } else form.append("languages[]", requestConfig.language);
      if (requestConfig.prompt) form.set("prompt", requestConfig.prompt);
      console.log(JSON.stringify({ event: "request-start", sample: sample.id, model }));
      const start = performance.now();
      const res = await fetch("https://api.openai.com/v1/audio/transcriptions", {
        method: "POST", headers: { Authorization: `Bearer ${key}` }, body: form,
        signal: AbortSignal.timeout(300_000),
      });
      if (!res.ok) throw new Error(`${model}: HTTP ${res.status}: ${(await res.text()).slice(0, 500)}`);
      const payload = await res.json() as { text?: string };
      if (typeof payload.text !== "string") throw new Error(`${model} returned no transcript text`);
      response = { text: payload.text, elapsedMs: Math.round(performance.now() - start), payload };
      await writeFile(outputPath, JSON.stringify({ ...response, requestConfig, fingerprint }, null, 2));
    }
    const result = { sample: sample.id, title: sample.title, model, elapsedMs: response.elapsedMs, outputPath,
      referenceStatus: sample.referenceStatus ?? "unreviewed", score: reference ? wordErrorRate(reference, response.text) : null };
    results.push(result);
    await writeFile(path.join(outputDir, `${sample.id}-${model}.txt`), response.text);
    // Serialize report writes so concurrent completions cannot overwrite newer progress.
    persistence = persistence.then(async () => {
    await writeFile(path.join(outputDir, "summary.json"), JSON.stringify(results, null, 2));
    const report = ["# Transcription comparison", "", "References marked epub-provisional are not verified spoken ground truth. Unreviewed samples have no WER score.", "",
      "| Sample | Model | Request time (s) | Reference status | WER |", "| --- | --- | ---: | --- | ---: |",
      ...results.map(row => `| ${row.title} | ${row.model} | ${(row.elapsedMs / 1000).toFixed(1)} | ${row.referenceStatus} | ${row.score ? (row.score.wer * 100).toFixed(2) + "%" : "not scored"} |`)];
    await writeFile(path.join(outputDir, "report.md"), report.join("\n") + "\n");
    });
    await persistence;
    console.log(JSON.stringify({ event: "result", sample: sample.id, model, elapsedMs: response.elapsedMs,
      referenceStatus: result.referenceStatus, wer: result.score?.wer ?? null }));
  }
}
let nextSample = 0;
await Promise.all(Array.from({ length: Math.min(3, manifest.samples.length) }, async () => {
  while (nextSample < manifest.samples.length) await runSample(manifest.samples[nextSample++]!);
}));
