import { promises as fs } from "node:fs";
import path from "node:path";

import { XMLParser } from "fast-xml-parser";

import { cleanMetaValue, htmlToPlainText, nodeText, normalizeDescriptionHtml, slugify, toArray } from "../utils/strings";
import { AudioTagMetadata, Book, OpfMetadata } from "../types";
import { probeData } from "./probe-cache";

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
  removeNSPrefix: true,
  trimValues: false,
  textNodeName: "#text",
});

function parseOpfContent(content: string): OpfMetadata | null {
  let parsed: any;
  try {
    parsed = xmlParser.parse(content);
  } catch (err) {
    console.warn(`Failed to parse OPF XML: ${(err as Error).message}`);
    return null;
  }
  const metadata = parsed?.package?.metadata;
  if (!metadata) return null;

  const title = cleanMetaValue(nodeText(metadata.title ?? metadata["dc:title"]));
  const author = cleanMetaValue(nodeText(metadata.creator ?? metadata["dc:creator"]));
  const rawDescription = normalizeDescriptionHtml(nodeText(metadata.description ?? metadata["dc:description"]));
  const description = htmlToPlainText(rawDescription);
  const language = cleanMetaValue(nodeText(metadata.language ?? metadata["dc:language"]));
  const rawDate = nodeText(metadata.date ?? metadata["dc:date"]);

  const identifiers: Record<string, string> = {};
  const identifierNodes = toArray(metadata.identifier ?? metadata["dc:identifier"]);
  identifierNodes.forEach((idNode: any) => {
    const value = cleanMetaValue(nodeText(idNode));
    if (!value) return;
    const scheme = typeof idNode === "object" ? idNode.scheme || idNode["opf:scheme"] : undefined;
    if (scheme) identifiers[String(scheme).toLowerCase()] = value;
  });
  const isbn = identifiers["isbn"];

  let publishedAt: Date | undefined;
  if (rawDate) {
    const parsedDate = new Date(rawDate);
    if (!Number.isNaN(parsedDate.getTime())) {
      publishedAt = parsedDate;
    }
  }

  return {
    title,
    author,
    description,
    descriptionHtml: rawDescription,
    language,
    publishedAt,
    isbn,
    identifiers,
  };
}

function parseAudioTagDate(raw: string | undefined): Date | undefined {
  if (!raw) return undefined;
  const cleaned = raw.trim();
  if (!cleaned) return undefined;
  const parsed = new Date(cleaned);
  if (!Number.isNaN(parsed.getTime())) return parsed;
  return undefined;
}

function preferLonger(first?: string, second?: string): string | undefined {
  const a = first?.trim() ?? "";
  const b = second?.trim() ?? "";
  const aLen = a.length;
  const bLen = b.length;
  if (bLen > aLen) return b;
  if (aLen > bLen) return a;
  return bLen > 0 ? b : aLen > 0 ? a : undefined;
}

function readAudioMetadata(filePath: string, mtimeMs: number): AudioTagMetadata | null {
  const probed = probeData(filePath, mtimeMs);
  if (!probed || !probed.tags) return null;
  const tags = probed.tags;
  const descriptionRaw = normalizeDescriptionHtml(
    tags.description || tags.DESCRIPTION || tags.comment || tags.COMMENT
  );
  const description = htmlToPlainText(descriptionRaw);
  return {
    title: cleanMetaValue(tags.title || tags.TITLE),
    artist: cleanMetaValue(tags.artist || tags.ARTIST),
    albumArtist: cleanMetaValue(tags.album_artist || tags.ALBUM_ARTIST),
    description,
    descriptionHtml: descriptionRaw,
    language: cleanMetaValue(tags.language || tags.LANGUAGE),
    date: parseAudioTagDate(tags.date || tags.DATE),
  };
}

async function readOpfMetadata(bookDir: string, files: string[]): Promise<OpfMetadata | null> {
  const opfFile = files.find((f) => f.toLowerCase().endsWith(".opf"));
  if (!opfFile) return null;
  const opfPath = path.join(bookDir, opfFile);
  try {
    const content = await fs.readFile(opfPath, "utf8");
    return parseOpfContent(content);
  } catch (err) {
    console.warn(`Failed to read OPF for ${bookDir}: ${(err as Error).message}`);
    return null;
  }
}

function normalizeAudioExt(ext: string): "mp3" | "m4a" {
  const lower = ext.toLowerCase();
  if (lower === ".mp3") return "mp3";
  if (lower === ".m4a" || lower === ".m4b" || lower === ".mp4") return "m4a";
  return "mp3";
}

function mimeFromExt(ext: string): string {
  const normalized = normalizeAudioExt(ext);
  return normalized === "m4a" ? "audio/mp4" : "audio/mpeg";
}

function bookExtension(book: Book): string {
  const sourcePath =
    book.kind === "single"
      ? book.primaryFile
      : book.files && book.files.length > 0
        ? book.files[0].path || book.files[0].name
        : undefined;
  if (sourcePath) {
    return normalizeAudioExt(path.extname(sourcePath));
  }
  return normalizeAudioExt(book.mime);
}

function bookMime(book: Book): string {
  const ext = bookExtension(book);
  return mimeFromExt(ext);
}

function bookId(author: string, title: string): string {
  return slugify(`${author}-${title}`);
}

function formatDateIso(date: Date | undefined): string | undefined {
  if (!date) return undefined;
  const iso = date.toISOString();
  return iso.slice(0, 10);
}

function bookIsbn(book: Book): string | undefined {
  if (book.isbn) return book.isbn;
  const identifiers = book.identifiers ?? {};
  return identifiers["isbn"] ?? identifiers["ISBN"];
}

function cleanLanguage(language: string | undefined): string | undefined {
  if (!language) return undefined;
  const trimmed = language.trim();
  if (!trimmed) return undefined;
  if (trimmed.toLowerCase() === "unknown") return undefined;
  return trimmed;
}

export {
  bookExtension,
  bookId,
  bookIsbn,
  bookMime,
  cleanLanguage,
  formatDateIso,
  mimeFromExt,
  normalizeAudioExt,
  parseAudioTagDate,
  parseOpfContent,
  preferLonger,
  readAudioMetadata,
  readOpfMetadata,
};
