import path from "node:path";
import { promises as fs } from "node:fs";

export type ObservedFile = {
  path: string; name: string; sizeBytes: number; mtimeMs: number;
  ctimeMs: number; device: number; inode: number;
};
export type ObservedInventory = {
  files: ObservedFile[];
  source: "download" | "library-fallback" | "unavailable";
  warnings: string[];
};
export type ObservedPathMapping = { remoteRoot: string; localRoot: string };
const within = (root: string, file: string) => {
  const relative = path.relative(root, file);
  return relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
};

/** Inventory explicit server-owned mappings only. Never follows source symlinks. */
export async function observedFiles(
  observation: { sourcePaths: string[]; libraryPath?: string | null },
  mappings: ObservedPathMapping[],
): Promise<ObservedInventory> {
  const result: ObservedInventory = { files: [], source: "unavailable", warnings: [] };
  for (const [index, remote] of observation.sourcePaths.entries()) {
    const mapping = [...mappings].sort((a, b) => b.remoteRoot.length - a.remoteRoot.length)
      .find((m) => path.isAbsolute(remote) && path.isAbsolute(m.remoteRoot) &&
        path.isAbsolute(m.localRoot) && within(path.resolve(m.remoteRoot), path.resolve(remote)));
    if (!mapping) {
      result.warnings.push("Source path has no absolute, explicit shadow mapping");
      continue;
    }
    const root = await fs.realpath(mapping.localRoot).catch(() => null);
    if (!root) { result.warnings.push("Mapped source root is unavailable"); continue; }
    const source = path.resolve(root, path.relative(mapping.remoteRoot, remote));
    const files: ObservedFile[] = [];
    let visited = 0, truncated = false;
    const walk = async (file: string, depth: number): Promise<void> => {
      if (++visited > 5000 || depth > 6 || files.length >= 50) { truncated = true; return; }
      const stat = await fs.lstat(file), real = await fs.realpath(file);
      if (stat.isSymbolicLink() || !within(root, real)) throw new Error("Unsafe observed source");
      if (stat.isDirectory()) {
        for (const name of (await fs.readdir(file)).sort()) {
          if (truncated) break;
          await walk(path.join(file, name), depth + 1);
        }
      } else if (stat.isFile() && /\.(mkv|mp4|m4v|avi|mov|ts|m2ts|mpg|mpeg)$/i.test(file)) {
        files.push({ path: real, name: path.relative(source, file) || path.basename(file),
          sizeBytes: stat.size, mtimeMs: stat.mtimeMs, ctimeMs: stat.ctimeMs, device: stat.dev, inode: stat.ino });
      }
    };
    try {
      // Reject intermediate symlinks too, including links back inside the mapped root.
      const pieces = path.relative(root, source).split(path.sep).filter(Boolean);
      let prefix = root;
      for (const piece of pieces.slice(0, -1)) {
        prefix = path.join(prefix, piece);
        if ((await fs.lstat(prefix)).isSymbolicLink()) throw new Error("Unsafe observed ancestor");
      }
      await walk(source, 0);
      if (truncated) {
        result.warnings.push("Shadow inventory exceeded safety bounds; source is unobservable");
        return result;
      }
      if (files.length) return { files,
        source: index > 0 || remote === observation.libraryPath ? "library-fallback" : "download", warnings: result.warnings };
      result.warnings.push("Mapped source contains no candidate media files");
    } catch (error) {
      result.warnings.push((error as NodeJS.ErrnoException).code === "ENOENT"
        ? "Source is missing or has moved" : "Source cannot be inspected safely");
    }
  }
  return result;
}
