import { createReadStream, promises as fs } from "node:fs";
import { Readable } from "node:stream";

import { AudioSegment } from "../types";

type XingPatchOptions = {
  durationSeconds?: number;
  audioSize?: number;
};

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

function bitrateKbps(version: number, bitrateIndex: number): number | null {
  if (bitrateIndex <= 0 || bitrateIndex >= 15) return null;
  if (version === 1) {
    const table = [0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0];
    return table[bitrateIndex] ?? null;
  }
  const table = [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, 0];
  return table[bitrateIndex] ?? null;
}

function sampleRateHz(version: number, sampleIndex: number): number | null {
  if (sampleIndex < 0 || sampleIndex > 2) return null;
  if (version === 1) {
    const table = [44100, 48000, 32000];
    return table[sampleIndex] ?? null;
  }
  if (version === 2) {
    const table = [22050, 24000, 16000];
    return table[sampleIndex] ?? null;
  }
  if (version === 2.5) {
    const table = [11025, 12000, 8000];
    return table[sampleIndex] ?? null;
  }
  return null;
}

function parseFrameHeader(header: number) {
  if ((header & 0xffe00000) !== 0xffe00000) return null;
  const versionBits = (header >> 19) & 0x3;
  const layerBits = (header >> 17) & 0x3;
  const bitrateIndex = (header >> 12) & 0xf;
  const sampleIndex = (header >> 10) & 0x3;
  const padding = (header >> 9) & 0x1;
  const channelMode = (header >> 6) & 0x3;
  const version = versionBits === 0x3 ? 1 : versionBits === 0x2 ? 2 : versionBits === 0x0 ? 2.5 : 0;
  if (version === 0) return null;
  if (layerBits !== 0x1) return null; // Layer III only
  const bitrate = bitrateKbps(version === 2.5 ? 2 : version, bitrateIndex);
  const sampleRate = sampleRateHz(version, sampleIndex);
  if (!bitrate || !sampleRate) return null;
  const samplesPerFrame = version === 1 ? 1152 : 576;
  const frameLength =
    Math.floor(((version === 1 ? 144000 : 72000) * bitrate) / sampleRate) + padding;
  const sideInfoSize =
    version === 1 ? (channelMode === 0x3 ? 17 : 32) : channelMode === 0x3 ? 9 : 17;
  return { version, bitrate, sampleRate, samplesPerFrame, frameLength, sideInfoSize };
}

async function patchXingHeader(
  segment: AudioSegment,
  options: XingPatchOptions
): Promise<{ frame: Uint8Array; frameLength: number } | null> {
  if (!options.durationSeconds || !options.audioSize) return null;
  if (segment.start !== 0 || segment.end <= 0) return null;
  const handle = await fs.open(segment.path, "r").catch(() => null);
  if (!handle) return null;
  try {
    const probe = Buffer.allocUnsafe(4096);
    const { bytesRead } = await handle.read(probe, 0, probe.length, 0);
    if (bytesRead < 4) return null;
    const header = probe.readUInt32BE(0);
    const parsed = parseFrameHeader(header);
    if (!parsed) return null;
    const frameLength = parsed.frameLength;
    if (frameLength <= 0 || frameLength > segment.size) return null;
    const frameBuf = Buffer.allocUnsafe(frameLength);
    const readFrame = await handle.read(frameBuf, 0, frameLength, 0);
    if (readFrame.bytesRead < frameLength) return null;
    const xingOffset = 4 + parsed.sideInfoSize;
    if (xingOffset + 8 > frameLength) return null;
    const xingId = frameBuf.slice(xingOffset, xingOffset + 4).toString("ascii");
    if (xingId !== "Xing" && xingId !== "Info") return null;
    const flags = frameBuf.readUInt32BE(xingOffset + 4);
    let cursor = xingOffset + 8;
    if (flags & 0x1) {
      const frames = Math.max(
        1,
        Math.round((options.durationSeconds * parsed.sampleRate) / parsed.samplesPerFrame)
      );
      frameBuf.writeUInt32BE(frames >>> 0, cursor);
      cursor += 4;
    }
    if (flags & 0x2) {
      frameBuf.writeUInt32BE(options.audioSize >>> 0, cursor);
    }
    return { frame: new Uint8Array(frameBuf), frameLength };
  } finally {
    await handle.close().catch(() => {});
  }
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

async function streamSegmentsWithXingPatch(
  segments: AudioSegment[],
  options?: XingPatchOptions
): Promise<ReadableStream<Uint8Array>> {
  if (!options || segments.length === 0) return streamSegments(segments);
  const patched = await patchXingHeader(segments[0], options);
  if (!patched) return streamSegments(segments);
  let index = 0;
  let patchedFirst = false;
  return new ReadableStream({
    async pull(controller) {
      if (index >= segments.length) {
        controller.close();
        return;
      }
      if (!patchedFirst && index === 0) {
        const segment = segments[0];
        controller.enqueue(patched.frame);
        const restStart = segment.start + patched.frameLength;
        if (restStart <= segment.end) {
          const reader = Readable.toWeb(
            createReadStream(segment.path, { start: restStart, end: segment.end })
          ).getReader();
          for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            controller.enqueue(value);
          }
        }
        patchedFirst = true;
        index = 1;
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

export { parseRange, segmentsForRange, streamSegments, streamSegmentsWithXingPatch };
