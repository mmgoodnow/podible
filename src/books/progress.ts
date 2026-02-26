import type { MediaStatus, ReleaseStatus } from "./status";

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  if (value <= 0) return 0;
  if (value >= 1) return 1;
  return value;
}

export function pseudoProgressForMediaStatus(status: MediaStatus): number {
  if (status === "imported") return 100;
  if (status === "downloaded") return 90;
  if (status === "downloading") return 20;
  if (status === "snatched") return 10;
  if (status === "error") return 0;
  return 0;
}

export function pseudoProgressForBook(audio: MediaStatus, ebook: MediaStatus): number {
  const audioProgress = pseudoProgressForMediaStatus(audio);
  const ebookProgress = pseudoProgressForMediaStatus(ebook);
  return Math.round((audioProgress + ebookProgress) / 2);
}

export function pseudoProgressForRelease(
  status: ReleaseStatus | null,
  downloadingFraction?: number | null
): number {
  if (status === "imported") return 100;
  if (status === "downloaded") return 90;
  if (status === "downloading") {
    const fraction = clamp01(downloadingFraction ?? 0);
    return Math.round(20 + fraction * 70);
  }
  if (status === "snatched") return 10;
  if (status === "failed") return 0;
  return 0;
}

export function computeDownloadFraction(input: {
  bytesDone: number | null;
  sizeBytes: number | null;
  leftBytes: number | null;
}): number | null {
  const bytesDone = input.bytesDone;
  const sizeBytes = input.sizeBytes;
  const leftBytes = input.leftBytes;

  if (typeof sizeBytes === "number" && Number.isFinite(sizeBytes) && sizeBytes > 0 && typeof bytesDone === "number") {
    return clamp01(bytesDone / sizeBytes);
  }
  if (typeof bytesDone === "number" && typeof leftBytes === "number") {
    const total = bytesDone + leftBytes;
    if (Number.isFinite(total) && total > 0) {
      return clamp01(bytesDone / total);
    }
  }
  return null;
}
