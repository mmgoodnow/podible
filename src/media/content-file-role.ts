import path from "node:path";

const contentExtensions = new Set([
  ".mkv", ".mp4", ".m4v", ".avi", ".mov", ".ts",
  ".m4b", ".m4a", ".mp3", ".flac", ".ogg", ".opus", ".wav", ".aac",
  ".epub", ".pdf", ".mobi", ".azw", ".azw3", ".djvu", ".cbz", ".cbr",
]);

export function torrentFileRole(filename: string): "content" | "sidecar" {
  return contentExtensions.has(path.posix.extname(filename).toLowerCase()) ? "content" : "sidecar";
}
