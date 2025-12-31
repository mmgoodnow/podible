import { statSync } from "node:fs";
import path from "node:path";

import { AudioSegment, Book, ChapterTiming } from "../types";

type CoverArt = {
  mime: string;
  data: Uint8Array;
};

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

function synchsafeSize(size: number): Uint8Array {
  const out = new Uint8Array(4);
  out[0] = (size >> 21) & 0x7f;
  out[1] = (size >> 14) & 0x7f;
  out[2] = (size >> 7) & 0x7f;
  out[3] = size & 0x7f;
  return out;
}

function writeUint32BE(value: number): Uint8Array {
  const out = new Uint8Array(4);
  out[0] = (value >>> 24) & 0xff;
  out[1] = (value >>> 16) & 0xff;
  out[2] = (value >>> 8) & 0xff;
  out[3] = value & 0xff;
  return out;
}

const encoder = new TextEncoder();

function id3Frame(id: string, payload: Uint8Array): Uint8Array {
  const header = new Uint8Array(10);
  header.set(encoder.encode(id).slice(0, 4));
  header.set(synchsafeSize(payload.byteLength), 4);
  // flags remain zeroed
  return concatBytes([header, payload]);
}

function textFrame(id: string, text: string): Uint8Array {
  const textBytes = encoder.encode(text);
  const payload = new Uint8Array(1 + textBytes.length);
  payload[0] = 0x03; // UTF-8
  payload.set(textBytes, 1);
  return id3Frame(id, payload);
}

function apicFrame(cover: CoverArt): Uint8Array {
  const mimeBytes = encoder.encode(cover.mime);
  const payload = concatBytes([
    new Uint8Array([0x03]), // UTF-8
    mimeBytes,
    new Uint8Array([0x00]), // MIME terminator
    new Uint8Array([0x03]), // picture type: front cover
    new Uint8Array([0x00]), // empty description terminator
    cover.data,
  ]);
  return id3Frame("APIC", payload);
}

function chapFrame(chapterId: string, title: string, startMs: number, endMs: number): Uint8Array {
  const idBytes = encoder.encode(chapterId);
  const titleFrame = textFrame("TIT2", title);
  const payload = concatBytes([
    idBytes,
    new Uint8Array([0x00]), // terminator
    writeUint32BE(startMs),
    writeUint32BE(endMs),
    writeUint32BE(0xffffffff), // start offset: unknown
    writeUint32BE(0xffffffff), // end offset: unknown
    titleFrame,
  ]);
  return id3Frame("CHAP", payload);
}

function ctocFrame(childIds: string[]): Uint8Array {
  const elementId = encoder.encode("toc");
  const childrenBytes = concatBytes(
    childIds.map((id) => concatBytes([encoder.encode(id), new Uint8Array([0x00])]))
  );
  const titleFrame = textFrame("TIT2", "Chapters");
  const payload = concatBytes([
    elementId,
    new Uint8Array([0x00]), // terminator
    new Uint8Array([0x03]), // flags: top-level + ordered
    new Uint8Array([childIds.length]),
    childrenBytes,
    titleFrame,
  ]);
  return id3Frame("CTOC", payload);
}

function buildId3ChaptersTag(timings: ChapterTiming[], cover?: CoverArt): Uint8Array {
  if (timings.length === 0 && !cover) return new Uint8Array();
  const frames: Uint8Array[] = [];
  if (cover) frames.push(apicFrame(cover));
  if (timings.length > 0) {
    const childIds = timings.map((c) => c.id);
    frames.push(ctocFrame(childIds));
    frames.push(...timings.map((chap) => chapFrame(chap.id, chap.title, chap.startMs, chap.endMs)));
  }
  const framesBytes = concatBytes(frames);
  const header = new Uint8Array(10);
  header.set(encoder.encode("ID3"));
  header[3] = 0x04; // version 2.4.0
  header[4] = 0x00;
  header[5] = 0x00; // flags
  header.set(synchsafeSize(framesBytes.byteLength), 6);
  return concatBytes([header, framesBytes]);
}

function coverMimeFromPath(coverPath: string): string {
  const ext = path.extname(coverPath).toLowerCase();
  if (ext === ".png") return "image/png";
  return "image/jpeg";
}

function estimateCoverFrameLength(coverPath: string | undefined): number {
  if (!coverPath) return 0;
  try {
    const stat = statSync(coverPath);
    if (!stat.isFile() || stat.size <= 0) return 0;
    const mimeLen = encoder.encode(coverMimeFromPath(coverPath)).byteLength;
    const payloadLen = 1 + mimeLen + 1 + 1 + 1 + stat.size;
    return 10 + payloadLen;
  } catch {
    return 0;
  }
}

function estimateId3TagLength(book: Book): number {
  if (book.kind !== "multi" || !book.files) return 0;
  const dummyTimings: ChapterTiming[] = book.files.map((segment: AudioSegment, index) => ({
    id: `ch${index}`,
    title: path.basename(segment.name, path.extname(segment.name)),
    startMs: 0,
    endMs: 0,
  }));
  const base = buildId3ChaptersTag(dummyTimings).byteLength;
  return base + estimateCoverFrameLength(book.coverPath);
}

export { buildId3ChaptersTag, estimateId3TagLength };
