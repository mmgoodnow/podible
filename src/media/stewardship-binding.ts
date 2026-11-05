import path from "node:path";
import { createHash } from "node:crypto";
import { z } from "zod";
import type { AppSettings } from "../app-types";
import type { StewardshipSource } from "./torrent-stewardship-types";

export type Binding = { key: string; media: "books" | "movies" | "tv"; label: string; hash: string;
  clientId: string; directory: string; root: string; rootToken: string | null; bindingDigest: string };
export const stewardshipPrivateIdSchema = z.string().regex(/^transmission:[a-f0-9-]{36}$/);
const hashSchema = z.string().regex(/^[a-f0-9]{40}$/);
const digest = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
function safeRelative(value: unknown): value is string {
  return typeof value === "string" && !path.isAbsolute(value) && !/[\\\x00-\x1f]/.test(value) &&
    value.split("/").every(part => !!part && part !== "." && part !== "..");
}

/** Preserve the original collector/evidence identities; the status projection is not ownership authority. */
export function stewardshipBindingFromSettings(settings: AppSettings | null, source: StewardshipSource, row: Record<string, unknown>, privateId: string): Binding | null {
  if (!settings || !stewardshipPrivateIdSchema.safeParse(privateId).success || row.client_id !== privateId) return null;
  const hash = source === "books" ? row.info_hash : row.native_id;
  if (!hashSchema.safeParse(hash).success) return null;
  let media: Binding["media"], root: string, directory: string, rootToken: string | null = null, label: string;
  if (source === "books") {
    if (row.backend !== "transmission" || row.root_id !== "books" || row.relative_path !== hash ||
        typeof row.root_path !== "string" || !path.isAbsolute(row.root_path) || !z.string().uuid().safeParse(row.root_token).success) return null;
    media = "books"; root = row.root_path; directory = path.join(root, String(hash)); rootToken = String(row.root_token);
    label = `Book ${Number.isSafeInteger(row.book_id) ? row.book_id : "binding"}`;
  } else {
    let location: unknown;
    try { location = JSON.parse(String(row.location_json)); } catch { return null; }
    const parsed = z.object({ rootId: z.enum(["movies", "tv"]), relativePath: z.string().refine(safeRelative) }).strict().safeParse(location);
    if (!parsed.success) return null;
    media = parsed.data.rootId;
    const managedRoot = media === "movies" ? settings.movies.managedRoot : settings.tv.managedRoot;
    if (!managedRoot || !path.isAbsolute(managedRoot)) return null;
    root = path.join(managedRoot, "downloads"); directory = path.join(root, parsed.data.relativePath);
    label = `${media === "movies" ? "Movie" : "TV"} torrent ${row.id}`;
  }
  return { key: digest([source, privateId, hash]), media, label, hash: String(hash), clientId: privateId, root, directory, rootToken,
    bindingDigest: digest([source, privateId, hash, root, directory, rootToken,
      source === "books" ? [row.book_id, row.release_id, settings.libraryRoot] : row.id]) };
}

export const stewardshipIndexPrefix = "torrent-stewardship:v1:binding-index:";
export function stewardshipIndexKey(source: StewardshipSource, clientId: string, hash: string) {
  return stewardshipIndexPrefix + source + ":" + clientId + ":" + hash;
}
export function stewardshipIndexContext(settings: AppSettings, source: StewardshipSource, row: Record<string, unknown>) {
  return JSON.stringify(source === "books"
    ? [source, row.client_id, row.info_hash, row.book_id, row.release_id, row.backend, row.root_path, row.root_token, row.root_id, row.relative_path, settings.libraryRoot]
    : [source, row.client_id, row.native_id, row.id, row.location_json,
      JSON.parse(String(row.location_json)).rootId === "movies" ? settings.movies.managedRoot : settings.tv.managedRoot]);
}
