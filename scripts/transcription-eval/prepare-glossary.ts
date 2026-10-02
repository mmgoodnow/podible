import path from "node:path";
import { mkdir, writeFile } from "node:fs/promises";
import { extractGlossaryTerms, loadEpubEntries, promptForChunk } from "../../src/library/chapter-analysis";

const manifest = await Bun.file("tmp/transcription-eval/excerpts/manifest.json").json();
const books: Record<string, { author: string; epub: string }> = {
  "dungeon-crawler-carl": { author: "Matt Dinniman", epub: "Matt Dinniman/Dungeon Crawler Carl/Dungeon Crawler Carl.epub" },
  "project-hail-mary": { author: "Andy Weir", epub: "Andy Weir/Project Hail Mary/Project Hail Mary.epub" },
  twilight: { author: "Stephenie Meyer", epub: "Stephenie Meyer/Twilight/Twilight.epub" },
};
for (const sample of manifest.samples) {
  const book = books[sample.id];
  if (!book) throw new Error(`Unknown book: ${sample.id}`);
  const glossary = extractGlossaryTerms(await loadEpubEntries(path.resolve("tmp/chapter-analysis-corpus/library", book.epub)));
  sample.prompt = promptForChunk({ title: sample.title, author: book.author, language: "eng" }, glossary, { language: "en" });
  console.log(JSON.stringify({ sample: sample.id, glossary, prompt: sample.prompt }));
}
const output = path.resolve("tmp/transcription-eval/glossary");
await mkdir(output, { recursive: true });
await writeFile(path.join(output, "manifest.json"), JSON.stringify(manifest, null, 2));
console.log(path.join(output, "manifest.json"));
