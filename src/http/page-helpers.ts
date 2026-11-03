export function formatMinutes(durationMs: number | null): string {
  if (!durationMs || !Number.isFinite(durationMs) || durationMs <= 0) return "Unknown";
  return `${Math.round(durationMs / 60000)} min`;
}

export function formatOverallStatus(status: string): string {
  if (status === "imported") return "Ready";
  if (status === "partial") return "Partially ready";
  if (status === "downloading") return "In progress";
  if (status === "downloaded") return "Downloaded";
  if (status === "snatched") return "Queued";
  if (status === "error") return "Needs attention";
  return "Wanted";
}

export function formatMediaStatus(label: string, status: string): string {
  if (status === "imported") return `${label} ready`;
  if (status === "downloading") return `${label} downloading`;
  if (status === "downloaded") return `${label} downloaded`;
  if (status === "snatched") return `${label} queued`;
  if (status === "error") return `${label} needs attention`;
  return `${label} wanted`;
}

export function formatBookStatusLine(book: {
  status: string;
  audioStatus: string;
  ebookStatus: string;
  fullPseudoProgress?: number;
}): string {
  const parts = [
    formatOverallStatus(book.status),
    formatMediaStatus("Audio", book.audioStatus),
    formatMediaStatus("eBook", book.ebookStatus),
  ];
  if (typeof book.fullPseudoProgress === "number" && Number.isFinite(book.fullPseudoProgress) && book.status !== "imported") {
    parts.push(`${book.fullPseudoProgress}%`);
  }
  return parts.join(" • ");
}
