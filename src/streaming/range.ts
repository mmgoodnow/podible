import { createReadStream } from "node:fs";
import { Readable } from "node:stream";

import { AudioSegment } from "../types";

function parseRange(rangeHeader: string | null, size: number): { start: number; end: number } | null {
  if (!rangeHeader) return null;
  const match = /^bytes=(\d*)-(\d*)$/.exec(rangeHeader);
  if (!match) return null;
  let start = match[1] ? Number(match[1]) : 0;
  let end = match[2] ? Number(match[2]) : size - 1;
  if (Number.isNaN(start) || Number.isNaN(end)) return null;
  if (match[1] === "" && match[2] !== "") {
    // suffix range
    const length = end;
    start = size - length;
    end = size - 1;
  }
  if (start < 0 || end < start || start >= size) return null;
  end = Math.min(end, size - 1);
  return { start, end };
}

function segmentsForRange(files: AudioSegment[], start: number, end: number): AudioSegment[] {
  return files
    .map((file) => {
      if (file.end < start || file.start > end) return null;
      const relativeStart = Math.max(start, file.start) - file.start;
      const relativeEnd = Math.min(end, file.end) - file.start;
      return {
        ...file,
        start: relativeStart,
        end: relativeEnd,
      };
    })
    .filter((f): f is AudioSegment => Boolean(f));
}

function streamSegments(segments: AudioSegment[]): ReadableStream<Uint8Array> {
  let index = 0;
  return new ReadableStream({
    async pull(controller) {
      if (index >= segments.length) {
        controller.close();
        return;
      }
      const segment = segments[index];
      const reader = Readable.toWeb(
        createReadStream(segment.path, { start: segment.start, end: segment.end })
      ).getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        controller.enqueue(value);
      }
      index += 1;
    },
  });
}

export { parseRange, segmentsForRange, streamSegments };
