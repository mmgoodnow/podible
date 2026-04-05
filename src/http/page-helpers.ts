import { escapeHtml } from "./common";

export function truncateText(value: string, max: number): string {
  if (value.length <= max) return value;
  return `${value.slice(0, Math.max(0, max - 1)).trimEnd()}…`;
}

export function formatMinutes(durationMs: number | null): string {
  if (!durationMs || !Number.isFinite(durationMs) || durationMs <= 0) return "Unknown";
  return `${Math.round(durationMs / 60000)} min`;
}

export function parseMediaSelection(value: string | null): Array<"audio" | "ebook"> {
  if (value === "audio") return ["audio"];
  if (value === "ebook") return ["ebook"];
  return ["audio", "ebook"];
}

export function describeBookState(book: {
  status: string;
  audioStatus: string;
  ebookStatus: string;
}): string {
  if (book.audioStatus === "imported" && book.ebookStatus === "imported") {
    return "Audio and eBook are both ready.";
  }
  if (book.audioStatus === "imported") {
    return "Audio is ready now.";
  }
  if (book.ebookStatus === "imported") {
    return "The eBook is ready while audio is still in progress.";
  }
  if (book.status === "error") {
    return "This book needs attention before it will be fully ready.";
  }
  if (book.status === "downloading" || book.status === "snatched") {
    return "Podible is still working on this book.";
  }
  return "This book is still being prepared.";
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

export function coverMarkup(coverUrl: string | null, title: string, large = false): string {
  const initials =
    title
      .split(/\s+/)
      .map((part) => part[0] || "")
      .join("")
      .slice(0, 2)
      .toUpperCase() || "BK";
  if (!coverUrl) {
    return `<div class="${large ? "detail-cover-fallback" : "cover-fallback"}">${escapeHtml(initials)}</div>`;
  }
  return `<img class="${large ? "detail-cover" : "cover"}" src="${escapeHtml(coverUrl)}" alt="${escapeHtml(title)} cover" />`;
}
