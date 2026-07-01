import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

import OpenAI from "openai";

import type { BookRow, JobRow } from "../app-types";
import type { BooksRepo } from "../repo";
import type { WorkerContext } from "../worker/context";
import { htmlToPlainText } from "../utils/strings";

import { coverDirectoryForBook } from "./covers";

const COVER_IMAGE_MODEL = "gpt-image-1-mini";

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

  const bytes = Buffer.from(b64, "base64");
  if (bytes.length === 0) throw new Error("OpenAI image generation returned empty image data");

  const coverDir = coverDirectoryForBook(repo, book);
  await mkdir(coverDir, { recursive: true });
  const coverPath = path.join(coverDir, "cover.generated.jpg");
  await writeFile(coverPath, bytes);
  repo.updateBookMetadata(book.id, { coverPath });
  return coverPath;
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

function log(ctx: WorkerContext, message: string): void {
  if (ctx.onLog) {
    ctx.onLog(message);
    return;
  }
  console.log(message);
}
