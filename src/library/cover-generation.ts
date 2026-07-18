import { mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import OpenAI from "openai";

import type { BookRow, JobRow } from "../app-types";
import type { BooksRepo } from "../repo";
import type { WorkerContext } from "../worker/context";
import { htmlToPlainText } from "../utils/strings";

import { coverDirectoryForBook } from "./covers";

const COVER_IMAGE_MODEL = "gpt-image-1-mini";
const GENERATED_COVER_FILENAME = "cover.generated.ai.jpg";
const LEGACY_GENERATED_COVER_FILENAME = "cover.generated.jpg";
const AI_BADGE_WIDTH = 128;
const AI_BADGE_HEIGHT = 64;
const AI_BADGE_MARGIN = 24;

const AI_BADGE_GLYPHS = [
  ["01110", "10001", "10001", "11111", "10001", "10001", "10001"],
  ["11111", "00100", "00100", "00100", "00100", "00100", "11111"],
];

export function buildGeneratedCoverPrompt(book: BookRow, settingsTitle: string): string {
  const description = (book.description_html ? htmlToPlainText(book.description_html) : book.description)?.trim() ?? "";
  const identifiers = book.identifiers_json ? safeJsonRecord(book.identifiers_json) : {};
  const metadata = [
    `Title: ${book.title}`,
    `Author: ${book.author}`,
    book.published_at ? `Published: ${book.published_at}` : null,
    book.language ? `Language: ${book.language}` : null,
    settingsTitle ? `Library/feed context: ${settingsTitle}` : null,
    Object.keys(identifiers).length > 0 ? `Identifiers: ${JSON.stringify(identifiers)}` : null,
    description ? `Description: ${description.slice(0, 2_400)}` : null,
  ].filter(Boolean);

  return [
    "Create original square cover art for a digital audiobook/ebook library item.",
    "Use the supplied book metadata for mood, genre, setting, symbols, and visual direction.",
    "Do not include readable text, title lettering, author names, logos, watermarks, badges, or UI chrome.",
    "Do not imitate a known published cover. Make it distinct, polished, and atmospheric.",
    "Prefer a clean composition that still reads well as a small thumbnail.",
    "",
    metadata.join("\n"),
  ].join("\n");
}

export async function generateCoverForBook(repo: BooksRepo, bookId: number, apiKey: string): Promise<string> {
  if (!apiKey.trim()) throw new Error("OpenAI API key not configured");
  const book = repo.getBookRow(bookId);
  if (!book) throw new Error(`Book ${bookId} not found`);
  if (book.cover_path) return book.cover_path;

  const settings = repo.getSettings();
  const client = new OpenAI({ apiKey, timeout: settings.agents.timeoutMs });
  const response = await client.images.generate({
    model: COVER_IMAGE_MODEL,
    prompt: buildGeneratedCoverPrompt(book, settings.feed.title),
    size: "1024x1024",
    quality: "low",
    output_format: "jpeg",
    n: 1,
  });
  const b64 = response.data?.[0]?.b64_json;
  if (!b64) throw new Error("OpenAI image generation returned no image data");

  const generatedBytes = Buffer.from(b64, "base64");
  if (generatedBytes.length === 0) throw new Error("OpenAI image generation returned empty image data");
  const bytes = await watermarkGeneratedCover(generatedBytes);

  const coverDir = coverDirectoryForBook(repo, book);
  await mkdir(coverDir, { recursive: true });
  const coverPath = path.join(coverDir, GENERATED_COVER_FILENAME);
  await writeFile(coverPath, bytes);
  repo.updateBookMetadata(book.id, { coverPath });
  return coverPath;
}

export async function watermarkGeneratedCover(bytes: Uint8Array): Promise<Buffer> {
  const workingDirectory = await mkdtemp(path.join(tmpdir(), "podible-cover-watermark-"));
  const inputPath = path.join(workingDirectory, "input.jpg");
  const badgePath = path.join(workingDirectory, "ai-badge.pam");
  const outputPath = path.join(workingDirectory, "output.jpg");

  try {
    await Promise.all([
      writeFile(inputPath, bytes),
      writeFile(badgePath, createAiBadgePam()),
    ]);
    await runFfmpeg([
      "-hide_banner",
      "-loglevel",
      "error",
      "-y",
      "-i",
      inputPath,
      "-i",
      badgePath,
      "-filter_complex",
      `overlay=main_w-overlay_w-${AI_BADGE_MARGIN}:main_h-overlay_h-${AI_BADGE_MARGIN}:format=auto`,
      "-frames:v",
      "1",
      "-q:v",
      "2",
      outputPath,
    ]);
    return await readFile(outputPath);
  } finally {
    await rm(workingDirectory, { recursive: true, force: true });
  }
}

export async function watermarkLegacyGeneratedCovers(
  repo: BooksRepo,
  onLog: (message: string) => void = console.log
): Promise<{ updated: number; failed: number }> {
  let updated = 0;
  let failed = 0;

  for (const book of repo.listAllBooks()) {
    const row = repo.getBookRow(book.id);
    if (!row?.cover_path || path.basename(row.cover_path) !== LEGACY_GENERATED_COVER_FILENAME) continue;

    try {
      const targetPath = path.join(path.dirname(row.cover_path), GENERATED_COVER_FILENAME);
      const temporaryTargetPath = path.join(path.dirname(row.cover_path), ".cover.generated.ai.tmp.jpg");
      const watermarked = await watermarkGeneratedCover(await readFile(row.cover_path));
      await writeFile(temporaryTargetPath, watermarked);
      await rename(temporaryTargetPath, targetPath);
      repo.updateBookMetadata(row.id, { coverPath: targetPath });
      await rm(row.cover_path, { force: true });
      updated += 1;
    } catch (error) {
      failed += 1;
      onLog(`[cover-generation] legacy watermark failed book=${row.id} error=${errorMessage(error)}`);
    }
  }

  return { updated, failed };
}

export function createAiBadgePam(): Buffer {
  const pixels = Buffer.alloc(AI_BADGE_WIDTH * AI_BADGE_HEIGHT * 4);
  const radius = 14;

  for (let y = 0; y < AI_BADGE_HEIGHT; y += 1) {
    for (let x = 0; x < AI_BADGE_WIDTH; x += 1) {
      if (insideRoundedRectangle(x, y, AI_BADGE_WIDTH, AI_BADGE_HEIGHT, radius)) {
        setRgba(pixels, x, y, 12, 18, 16, 180);
      }
    }
  }

  const scale = 7;
  const glyphGap = 2 * scale;
  const glyphWidth = AI_BADGE_GLYPHS[0]![0]!.length * scale;
  const totalWidth = glyphWidth * AI_BADGE_GLYPHS.length + glyphGap;
  const originX = Math.floor((AI_BADGE_WIDTH - totalWidth) / 2);
  const originY = Math.floor((AI_BADGE_HEIGHT - AI_BADGE_GLYPHS[0]!.length * scale) / 2);

  AI_BADGE_GLYPHS.forEach((glyph, glyphIndex) => {
    const glyphX = originX + glyphIndex * (glyphWidth + glyphGap);
    glyph.forEach((row, rowIndex) => {
      for (let column = 0; column < row.length; column += 1) {
        if (row[column] !== "1") continue;
        for (let dy = 0; dy < scale; dy += 1) {
          for (let dx = 0; dx < scale; dx += 1) {
            setRgba(pixels, glyphX + column * scale + dx, originY + rowIndex * scale + dy, 255, 255, 255, 245);
          }
        }
      }
    });
  });

  const header = Buffer.from(
    `P7\nWIDTH ${AI_BADGE_WIDTH}\nHEIGHT ${AI_BADGE_HEIGHT}\nDEPTH 4\nMAXVAL 255\nTUPLTYPE RGB_ALPHA\nENDHDR\n`,
    "ascii"
  );
  return Buffer.concat([header, pixels]);
}

function insideRoundedRectangle(x: number, y: number, width: number, height: number, radius: number): boolean {
  if (x >= radius && x < width - radius) return true;
  if (y >= radius && y < height - radius) return true;
  const centerX = x < radius ? radius - 0.5 : width - radius - 0.5;
  const centerY = y < radius ? radius - 0.5 : height - radius - 0.5;
  return (x - centerX) ** 2 + (y - centerY) ** 2 <= radius ** 2;
}

function setRgba(buffer: Buffer, x: number, y: number, red: number, green: number, blue: number, alpha: number): void {
  const offset = (y * AI_BADGE_WIDTH + x) * 4;
  buffer[offset] = red;
  buffer[offset + 1] = green;
  buffer[offset + 2] = blue;
  buffer[offset + 3] = alpha;
}

async function runFfmpeg(args: string[]): Promise<void> {
  const child = Bun.spawn(["ffmpeg", ...args], {
    stdout: "ignore",
    stderr: "pipe",
  });
  const stderr = await new Response(child.stderr).text();
  const exitCode = await child.exited;
  if (exitCode !== 0) {
    throw new Error(`ffmpeg cover watermark failed (${exitCode}): ${stderr.trim()}`);
  }
}

export function queueMissingCoverGeneration(repo: BooksRepo, bookId: number): JobRow | null {
  const book = repo.getBookRow(bookId);
  if (!book || book.cover_path) return null;
  if (!repo.getSettings().agents.apiKey.trim()) return null;
  const existing = repo.listJobsByType("cover_generation").find((job) => {
    if (job.book_id !== bookId || (job.status !== "queued" && job.status !== "running")) return false;
    return true;
  });
  if (existing) return existing;
  return repo.createJob({
    type: "cover_generation",
    bookId,
    maxAttempts: 2,
    payload: { reason: "missing_cover" },
  });
}

export async function processCoverGenerationJob(ctx: WorkerContext, job: JobRow): Promise<"done"> {
  if (!job.book_id) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }
  const settings = ctx.getSettings();
  const book = ctx.repo.getBookRow(job.book_id);
  if (!book || book.cover_path) {
    ctx.repo.markJobSucceeded(job.id);
    return "done";
  }
  const coverPath = await generateCoverForBook(ctx.repo, book.id, settings.agents.apiKey);
  ctx.repo.markJobSucceeded(job.id);
  log(ctx, `[cover-generation] job=${job.id} book=${book.id} cover=${coverPath}`);
  return "done";
}

function safeJsonRecord(value: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(value) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function log(ctx: WorkerContext, message: string): void {
  if (ctx.onLog) {
    ctx.onLog(message);
    return;
  }
  console.log(message);
}
