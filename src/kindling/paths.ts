import { access } from "node:fs/promises";
import path from "node:path";

async function pathExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

export type ImportSourcePathResolution = {
  originalPath: string;
  resolvedPath: string;
  remapped: boolean;
  strategy: "identity" | "rtorrent_data_downloads_to_library_root" | "basename_under_library_root";
};

/**
 * Resolve an rTorrent-reported path to a host-visible path.
 *
 * Common local-dev setup: rTorrent runs in Docker and reports `/data/downloads/...`
 * while Podible runs on the host and can only see `settings.libraryRoot/...`.
 */
export async function resolveImportSourcePath(basePath: string, libraryRoot: string): Promise<ImportSourcePathResolution> {
  const originalPath = String(basePath || "").trim();
  if (!originalPath) {
    return {
      originalPath,
      resolvedPath: originalPath,
      remapped: false,
      strategy: "identity",
    };
  }

  if (await pathExists(originalPath)) {
    return {
      originalPath,
      resolvedPath: originalPath,
      remapped: false,
      strategy: "identity",
    };
  }

  const normalizedOriginal = path.posix.normalize(originalPath);
  if (normalizedOriginal === "/data/downloads" || normalizedOriginal.startsWith("/data/downloads/")) {
    const relative = normalizedOriginal === "/data/downloads" ? "" : normalizedOriginal.slice("/data/downloads/".length);
    const candidate = relative ? path.join(libraryRoot, relative) : libraryRoot;
    if (await pathExists(candidate)) {
      return {
        originalPath,
        resolvedPath: candidate,
        remapped: true,
        strategy: "rtorrent_data_downloads_to_library_root",
      };
    }
  }

  const basename = path.basename(originalPath);
  if (basename && basename !== "." && basename !== path.basename(libraryRoot)) {
    const candidate = path.join(libraryRoot, basename);
    if (await pathExists(candidate)) {
      return {
        originalPath,
        resolvedPath: candidate,
        remapped: true,
        strategy: "basename_under_library_root",
      };
    }
  }

  return {
    originalPath,
    resolvedPath: originalPath,
    remapped: false,
    strategy: "identity",
  };
}
