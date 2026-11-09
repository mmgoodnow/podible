import { createHash } from "node:crypto";
import bencode from "bencode";

export function normalizeInfoHash(value: string): string {
  const trimmed = value.trim();
  if (!/^[0-9a-fA-F]{40}$/.test(trimmed)) {
    throw new Error("Unsupported info hash format");
  }
  return trimmed.toLowerCase();
}

function ensureAsciiDigit(byte: number): boolean {
  return byte >= 0x30 && byte <= 0x39;
}

function readLength(bytes: Uint8Array, start: number, delimiter: number): { value: number; next: number } {
  let i = start;
  if (i >= bytes.length || !ensureAsciiDigit(bytes[i] ?? 0)) {
    throw new Error("Invalid bencode length");
  }
  while (i < bytes.length && bytes[i] !== delimiter) {
    if (!ensureAsciiDigit(bytes[i] ?? 0)) {
      throw new Error("Invalid bencode length");
    }
    i += 1;
  }
  if (i >= bytes.length) {
    throw new Error("Invalid bencode length");
  }
  const value = Number.parseInt(Buffer.from(bytes.slice(start, i)).toString("ascii"), 10);
  if (!Number.isInteger(value) || value < 0) {
    throw new Error("Invalid bencode length");
  }
  return { value, next: i + 1 };
}

function stringBounds(bytes: Uint8Array, start: number) {
  const { value: length, next } = readLength(bytes, start, 0x3a);
  const end = next + length;
  if (end > bytes.length) {
    throw new Error("Invalid bencode string");
  }
  return { next, end };
}

function readString(bytes: Uint8Array, start: number): { value: string; end: number } {
  const { next, end } = stringBounds(bytes, start);
  return {
    value: Buffer.from(bytes.subarray(next, end)).toString("utf8"),
    end,
  };
}

function skipElement(bytes: Uint8Array, start: number): number {
  const token = bytes[start];
  if (token === undefined) {
    throw new Error("Unexpected end of torrent data");
  }

  if (token === 0x69) {
    let i = start + 1;
    if (i >= bytes.length) throw new Error("Invalid bencode integer");
    while (i < bytes.length && bytes[i] !== 0x65) {
      const ch = bytes[i] ?? 0;
      if (!(ensureAsciiDigit(ch) || ch === 0x2d)) {
        throw new Error("Invalid bencode integer");
      }
      i += 1;
    }
    if (i >= bytes.length) throw new Error("Invalid bencode integer");
    return i + 1;
  }

  if (token === 0x6c) {
    let i = start + 1;
    while (i < bytes.length && bytes[i] !== 0x65) {
      i = skipElement(bytes, i);
    }
    if (i >= bytes.length) throw new Error("Invalid bencode list");
    return i + 1;
  }

  if (token === 0x64) {
    let i = start + 1;
    while (i < bytes.length && bytes[i] !== 0x65) {
      const key = stringBounds(bytes, i);
      i = skipElement(bytes, key.end);
    }
    if (i >= bytes.length) throw new Error("Invalid bencode dictionary");
    return i + 1;
  }

  if (ensureAsciiDigit(token)) {
    return stringBounds(bytes, start).end;
  }

  throw new Error("Invalid bencode token");
}

function extractInfoDictionary(bytes: Uint8Array): Uint8Array {
  if (bytes[0] !== 0x64) {
    throw new Error("Torrent payload must be a bencoded dictionary");
  }

  let i = 1;
  while (i < bytes.length && bytes[i] !== 0x65) {
    const key = readString(bytes, i);
    const valueStart = key.end;
    const valueEnd = skipElement(bytes, valueStart);

    if (key.value === "info") {
      return bytes.slice(valueStart, valueEnd);
    }
    i = valueEnd;
  }

  throw new Error("Torrent payload missing info dictionary");
}

export function infoHashFromTorrentBytes(bytes: Uint8Array): string {
  const infoDict = extractInfoDictionary(bytes);
  return createHash("sha1").update(infoDict).digest("hex");
}

type AnnounceFields = { primary: string | null; tiers: string[][] };

function announceFields(bytes: Uint8Array) {
  if (bytes[0] !== 0x64) throw new Error("Invalid torrent dictionary");
  const fields = new Map<string, { start: number; value: number; end: number }>();
  let i = 1;
  while (i < bytes.length && bytes[i] !== 0x65) {
    const key = readString(bytes, i);
    const end = skipElement(bytes, key.end);
    if (fields.has(key.value)) throw new Error("Duplicate torrent dictionary key");
    fields.set(key.value, { start: i, value: key.end, end });
    i = end;
  }
  if (i !== bytes.length - 1 || bytes[i] !== 0x65 || bytes[fields.get("info")?.value ?? -1] !== 0x64)
    throw new Error("Invalid torrent dictionary");
  return fields;
}

export function readTorrentAnnounces(bytes: Uint8Array): AnnounceFields {
  const fields = announceFields(bytes);
  const string = (value: unknown) => {
    if (!(value instanceof Uint8Array)) throw new Error("Invalid tracker URL");
    return new TextDecoder("utf-8", { fatal: true }).decode(value);
  };
  const decode = (key: string): unknown => {
    const field = fields.get(key);
    return field ? bencode.decode(bytes.subarray(field.value, field.end)) : null;
  };
  const primary = decode("announce"), list = decode("announce-list");
  if (list !== null && (!Array.isArray(list) || list.some(tier => !Array.isArray(tier) || !tier.length)))
    throw new Error("Invalid announce tiers");
  return { primary: primary === null ? null : string(primary),
    tiers: list === null ? [] : (list as unknown[][]).map(tier => tier.map(string)) };
}

export function rewriteTorrentAnnounces(bytes: Uint8Array, { primary, tiers }: AnnounceFields): Uint8Array {
  const fields = announceFields(bytes);
  const parts: Uint8Array[] = [Buffer.from("d")];
  const append = (key: string, value: string | string[][] | null) => {
    if (value !== null) parts.push(bencode.encode(key), bencode.encode(value));
  };
  append("announce", primary);
  append("announce-list", tiers.length ? tiers : null);
  // Preserve every other bencoded value byte-for-byte, especially the hashed info dictionary.
  for (const [key, field] of fields)
    if (key !== "announce" && key !== "announce-list") parts.push(bytes.subarray(field.start, field.end));
  return Buffer.concat([...parts, Buffer.from("e")]);
}
