import { createHash } from "node:crypto";

function hexToBytes(hex: string): Uint8Array {
  const clean = hex.trim().toLowerCase();
  if (!/^[0-9a-f]+$/.test(clean) || clean.length % 2 !== 0) {
    throw new Error("Invalid hex string");
  }
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i += 1) {
    out[i] = Number.parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

function base32ToBytes(base32: string): Uint8Array {
  const alphabet = "abcdefghijklmnopqrstuvwxyz234567";
  const clean = base32.trim().toLowerCase().replace(/=+$/g, "");
  if (!/^[a-z2-7]+$/.test(clean)) {
    throw new Error("Invalid base32 string");
  }
  let bits = 0;
  let value = 0;
  const bytes: number[] = [];
  for (const char of clean) {
    const idx = alphabet.indexOf(char);
    if (idx < 0) throw new Error("Invalid base32 character");
    value = (value << 5) | idx;
    bits += 5;
    if (bits >= 8) {
      bytes.push((value >>> (bits - 8)) & 0xff);
      bits -= 8;
    }
  }
  return new Uint8Array(bytes);
}

export function normalizeInfoHash(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 40 && /^[0-9a-fA-F]+$/.test(trimmed)) {
    return trimmed.toLowerCase();
  }
  if (trimmed.length === 32 && /^[a-zA-Z2-7]+$/.test(trimmed)) {
    return Buffer.from(base32ToBytes(trimmed)).toString("hex");
  }
  throw new Error("Unsupported info hash format");
}

type ParseCursor = { index: number };

function parseLength(source: Uint8Array, cursor: ParseCursor, delimiter: number): number {
  const start = cursor.index;
  let end = source.indexOf(delimiter, start);
  if (end < 0) {
    throw new Error("Invalid bencode length");
  }
  const raw = Buffer.from(source.slice(start, end)).toString("ascii");
  if (!/^\d+$/.test(raw)) {
    throw new Error("Invalid bencode number");
  }
  cursor.index = end + 1;
  return Number.parseInt(raw, 10);
}

function skipValue(source: Uint8Array, cursor: ParseCursor): void {
  if (cursor.index >= source.length) {
    throw new Error("Unexpected end of bencode");
  }
  const token = source[cursor.index];

  if (token >= 0x30 && token <= 0x39) {
    const length = parseLength(source, cursor, 0x3a);
    cursor.index += length;
    if (cursor.index > source.length) throw new Error("String exceeds payload");
    return;
  }

  if (token === 0x69) {
    cursor.index += 1;
    const end = source.indexOf(0x65, cursor.index);
    if (end < 0) throw new Error("Unterminated integer");
    cursor.index = end + 1;
    return;
  }

  if (token === 0x6c) {
    cursor.index += 1;
    while (source[cursor.index] !== 0x65) {
      skipValue(source, cursor);
      if (cursor.index >= source.length) throw new Error("Unterminated list");
    }
    cursor.index += 1;
    return;
  }

  if (token === 0x64) {
    cursor.index += 1;
    while (source[cursor.index] !== 0x65) {
      skipValue(source, cursor);
      skipValue(source, cursor);
      if (cursor.index >= source.length) throw new Error("Unterminated dict");
    }
    cursor.index += 1;
    return;
  }

  throw new Error("Invalid bencode token");
}

export function infoHashFromTorrentBytes(bytes: Uint8Array): string {
  const key = Buffer.from("4:info", "ascii");
  const pos = Buffer.from(bytes).indexOf(key);
  if (pos < 0) {
    throw new Error("Missing info dictionary");
  }

  const cursor: ParseCursor = { index: pos + key.length };
  const start = cursor.index;
  skipValue(bytes, cursor);
  const infoSlice = bytes.slice(start, cursor.index);
  return createHash("sha1").update(infoSlice).digest("hex");
}

export function torrentBytesFromHex(hex: string): Uint8Array {
  return hexToBytes(hex);
}
