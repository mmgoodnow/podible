import path from "node:path";

import { Book, ChapterTiming } from "../types";

async function buildChapterTimings(book: Book): Promise<ChapterTiming[] | null> {
  if (book.kind === "single") {
    if (book.chapters && book.chapters.length > 0) return book.chapters;
    return null;
  }
  if (!book.files) return null;
  const timings: ChapterTiming[] = [];
  let cursorMs = 0;
  book.files.forEach((segment, index) => {
    const durationMs = segment.durationMs;
    const startMs = cursorMs;
    const endMs = startMs + durationMs;
    const chapterTitle =
      segment.title ||
      path.basename(segment.name, path.extname(segment.name)) ||
      `Part ${index + 1}`;
    timings.push({
      id: `ch${index}`,
      title: chapterTitle,
      startMs,
      endMs,
    });
    cursorMs = endMs;
  });
  return timings;
}

async function buildChapters(book: Book) {
  const timings = await buildChapterTimings(book);
  if (!timings) return null;
  return {
    version: "1.2.0",
    chapters: timings.map((chap) => ({
      startTime: chap.startMs / 1000,
      title: chap.title,
    })),
  };
}

export { buildChapters, buildChapterTimings };
