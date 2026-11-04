import { createHash, randomUUID } from "node:crypto";
import {
  constants, closeSync, fstatSync, fsyncSync, lstatSync, mkdirSync,
  openSync, readFileSync, readlinkSync, readdirSync, realpathSync, renameSync, symlinkSync, writeFileSync,
  type BigIntStats,
} from "node:fs";
import path from "node:path";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import type { LibraryViewEntry } from "./library-view";
import { RECOVERY_ROW_LIMIT } from "./recovery-types";

export function recoveryDigest(value: unknown): string {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}
export function recoveryContained(root: string, relative: string): string {
  if (!relative || path.isAbsolute(relative) || /[\\\x00-\x1f\x7f]/.test(relative) ||
      relative.split("/").some((part) => !part || part === "." || part === ".." || part.startsWith(".podible")))
    throw new Error("Unsafe recovery path");
  const result = path.resolve(root, relative);
  if (!result.startsWith(root + path.sep)) throw new Error("Recovery path escaped root");
  return result;
}
function stat(file: string): BigIntStats | null {
  try { return lstatSync(file, { bigint: true }); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; throw error; }
}
export function recoveryIdentity(file: BigIntStats | null) {
  return file ? [file.dev, file.ino, file.size, file.mtimeNs, file.ctimeNs, file.mode].map(String) : null;
}
/** Walk from /, not just the last parent: existing ancestors must never be symlinks. */
export function recoveryDirectory(root: string, create = false): boolean {
  if (!path.isAbsolute(root) || path.resolve(root) !== root) throw new Error("Noncanonical recovery root");
  let current = path.parse(root).root;
  for (const part of root.split(path.sep).filter(Boolean)) {
    current = path.join(current, part);
    const found = stat(current);
    if (found && (!found.isDirectory() || found.isSymbolicLink())) throw new Error("Noncanonical recovery directory");
    if (!found) { if (!create) return false; mkdirSync(current); }
  }
  return true;
}
function json(file: string): unknown | null {
  const found = stat(file);
  if (!found) return null;
  if (!found.isFile() || found.isSymbolicLink() || found.size > 1024n * 1024n) throw new Error("Unsafe ownership marker");
  const fd = openSync(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const opened = fstatSync(fd, { bigint: true });
    if (JSON.stringify(recoveryIdentity(opened)) !== JSON.stringify(recoveryIdentity(found))) throw new Error("Ownership marker changed");
    return JSON.parse(readFileSync(fd, "utf8"));
  } finally { closeSync(fd); }
}

export type RecoveryMount = {
  state: "verified" | "unavailable"; contentId: string | null; reason: string;
  root: string; contentRoot: string; viewRoot: string; evidence: unknown;
};
/** The exact existing movie-content-mount pin is also used by owned TV storage. Never initializes it. */
export function observeRecoveryMount(repo: BooksRepo, root: string, kind: "movies" | "tv"): RecoveryMount {
  const contentRoot = path.join(root, "objects"), viewRoot = path.join(root, "views", "plex", kind);
  try {
    if (!recoveryDirectory(root) || !recoveryDirectory(contentRoot) || realpathSync(root) !== root)
      throw new Error("Missing root");
    const pin = repo.getJsonState<string>(`movie-content-mount:${createHash("sha256").update(path.resolve(contentRoot)).digest("hex")}`);
    const marker = z.object({ id: z.string().uuid() }).parse(json(path.join(contentRoot, ".podible-content.json")));
    if (!pin || marker.id !== pin) throw new Error("Unpinned or replaced mount");
    return { state: "verified", contentId: marker.id, root, contentRoot, viewRoot,
      reason: "Canonical root and existing retained mount pin match the content marker; absence can be inspected.",
      evidence: [pin, ...[root, contentRoot].map((directory) => {
        const found = stat(directory)!;
        return [found.dev, found.ino, found.mode].map(String);
      }), recoveryIdentity(stat(path.join(contentRoot, ".podible-content.json")))],
    };
  } catch {
    return { state: "unavailable", contentId: null, root, contentRoot, viewRoot, evidence: null,
      reason: "Owned mount is missing, uninitialized, replaced, or noncanonical. Missing bytes are not established; recovery is blocked.",
    };
  }
}

export function observeRecoveryFile(mount: RecoveryMount, file: { id: string; relativePath: string; sizeBytes: number }) {
  const unknown = { presence: "unknown" as const, evidence: null as ReturnType<typeof recoveryIdentity> };
  if (mount.state !== "verified") return unknown;
  try {
    if (!z.string().uuid().safeParse(file.id).success || !Number.isSafeInteger(file.sizeBytes) || file.sizeBytes < 0 ||
        !/\.(mkv|mp4|m4v|avi|mov|ts)$/.test(file.relativePath) ||
        file.relativePath !== `files/${file.id.slice(0, 2)}/${file.id}${path.extname(file.relativePath)}`) return unknown;
    const full = recoveryContained(mount.contentRoot, file.relativePath);
    if (!recoveryDirectory(path.dirname(full))) return { presence: "missing" as const, evidence: null };
    const found = stat(full);
    if (!found) return { presence: "missing" as const, evidence: null };
    if (!found.isFile() || found.isSymbolicLink() || found.size !== BigInt(file.sizeBytes)) return unknown;
    return { presence: "present" as const, evidence: recoveryIdentity(found) };
  } catch { return unknown; }
}

const manifestSchema = z.object({ contentId: z.string().uuid(), files: z.record(z.string(), z.array(z.string()).max(20)) }).strict();
export type RecoveryView = {
  state: "present" | "missing" | "conflict" | "unknown" | "not-selected";
  files: { relativePath: string; presence: "present" | "missing" | "conflict" | "unknown" }[];
  evidence: unknown;
  manifest: z.infer<typeof manifestSchema> | null;
};
export function observeRecoveryView(mount: RecoveryMount, entries: LibraryViewEntry[]): RecoveryView {
  const unknown: RecoveryView = { state: "unknown", files: [], evidence: null, manifest: null };
  if (mount.state !== "verified") return unknown;
  try {
    const exists = recoveryDirectory(mount.viewRoot);
    const raw = exists ? json(path.join(mount.viewRoot, ".podible-view.json")) : null;
    if (exists && stat(path.join(mount.viewRoot, ".podible-view-pending.json"))) return unknown;
    const manifest = raw === null ? { contentId: mount.contentId!, files: {} } : manifestSchema.parse(raw);
    if (manifest.contentId !== mount.contentId || Object.keys(manifest.files).length > RECOVERY_ROW_LIMIT) return unknown;
    if (exists && raw === null && readdirSync(mount.viewRoot).length) return unknown;
    for (const [relative, sources] of Object.entries(manifest.files)) {
      recoveryContained(mount.viewRoot, relative);
      for (const source of sources) recoveryContained(mount.contentRoot, source);
    }
    const evidence: unknown[] = [manifest];
    const files: RecoveryView["files"] = [];
    for (const entry of entries) {
      const destination = recoveryContained(mount.viewRoot, entry.relativePath);
      recoveryContained(mount.contentRoot, path.relative(mount.contentRoot, entry.contentPath));
      if (!recoveryDirectory(path.dirname(destination))) {
        files.push({ relativePath: entry.relativePath, presence: "missing" }); evidence.push(null); continue;
      }
      const found = stat(destination);
      let presence: RecoveryView["files"][number]["presence"] = "missing";
      let link: string | null = null;
      if (found?.isSymbolicLink()) {
        link = readlinkSync(destination);
        presence = path.resolve(path.dirname(destination), link) === entry.contentPath &&
          manifest.files[entry.relativePath]?.includes(path.relative(mount.contentRoot, entry.contentPath)) ? "present" : "conflict";
      } else if (found) {
        const canonical = stat(entry.contentPath);
        presence = found.isFile() && canonical?.isFile() && !canonical.isSymbolicLink() &&
          found.dev === canonical.dev && found.ino === canonical.ino ? "present" : "conflict";
      }
      files.push({ relativePath: entry.relativePath, presence });
      evidence.push([recoveryIdentity(found), link]);
    }
    return { state: !entries.length ? "not-selected" : files.some((file) => file.presence === "conflict") ? "conflict"
      : files.some((file) => file.presence === "missing") ? "missing" : "present", files, manifest, evidence };
  } catch { return unknown; }
}

/** Short command fence: metadata and at most 20 exact projected paths, not a tree/manifest scan. */
export function recoveryViewFence(mount: RecoveryMount, entries: LibraryViewEntry[]) {
  return [recoveryDirectory(mount.viewRoot), ...[".podible-view.json", ".podible-view-pending.json"].map((name) =>
    recoveryIdentity(stat(path.join(mount.viewRoot, name)))), ...entries.map((entry) => {
      const destination = recoveryContained(mount.viewRoot, entry.relativePath);
      const parent = recoveryDirectory(path.dirname(destination));
      const found = parent ? stat(destination) : null;
      return [entry.relativePath, parent, recoveryIdentity(found), found?.isSymbolicLink() ? readlinkSync(destination) : null];
    })];
}

function durableJson(file: string, value: unknown) {
  const temporary = `${file}-recovery-${randomUUID()}`;
  const fd = openSync(temporary, "wx", 0o600);
  try { writeFileSync(fd, JSON.stringify(value)); fsyncSync(fd); } finally { closeSync(fd); }
  renameSync(temporary, file);
  const directory = openSync(path.dirname(file), "r");
  try { fsyncSync(directory); } finally { closeSync(directory); }
}

/** Synchronous, additive, idempotent replay. Never unlinks/overwrites any content or view entry. */
export function repairRecoveryView(mount: RecoveryMount, entries: LibraryViewEntry[], expected: RecoveryView) {
  const current = observeRecoveryView(mount, entries);
  if (JSON.stringify(current.evidence) !== JSON.stringify(expected.evidence) || !current.manifest ||
      !["present", "missing"].includes(current.state)) throw new Error("Recovery view changed or is unsupported");
  const files = { ...current.manifest.files };
  for (const entry of entries) {
    const source = path.relative(mount.contentRoot, entry.contentPath);
    files[entry.relativePath] = [...new Set([...(files[entry.relativePath] ?? []), source])];
  }
  recoveryDirectory(mount.viewRoot, true);
  // Persist ownership before adding links. A failed receipt/write is safe to inspect and retry.
  durableJson(path.join(mount.viewRoot, ".podible-view.json"), { contentId: mount.contentId, files });
  for (const entry of entries) {
    const destination = recoveryContained(mount.viewRoot, entry.relativePath);
    recoveryDirectory(path.dirname(destination), true);
    if (stat(destination)) continue;
    symlinkSync(path.relative(path.dirname(destination), entry.contentPath), destination);
    const directory = openSync(path.dirname(destination), "r");
    try { fsyncSync(directory); } finally { closeSync(directory); }
  }
  if (observeRecoveryView(mount, entries).state !== "present") throw new Error("Recovery view could not be verified");
}
