export type MediaType = "audio" | "ebook";

export type ReleaseStatus = "snatched" | "downloading" | "downloaded" | "imported" | "failed";

export type MediaStatus = "wanted" | "snatched" | "downloading" | "downloaded" | "imported" | "error";

export type BookStatus = MediaStatus | "partial";

export type BookStatusInput = {
  mediaType: MediaType;
  releases: ReleaseStatus[];
  hasAsset: boolean;
};

const NON_IMPORTED_PRIORITY: Record<Exclude<MediaStatus, "imported">, number> = {
  wanted: 0,
  error: 1,
  snatched: 2,
  downloaded: 3,
  downloading: 4,
};

export function deriveMediaStatus(input: BookStatusInput): MediaStatus {
  if (input.hasAsset) {
    return "imported";
  }

  const statuses = input.releases;

  if (statuses.includes("downloading")) {
    return "downloading";
  }

  if (statuses.includes("downloaded")) {
    return "downloaded";
  }

  if (statuses.includes("snatched")) {
    return "snatched";
  }

  if (statuses.length > 0 && statuses.every((status) => status === "failed")) {
    return "error";
  }

  return "wanted";
}

export function deriveBookStatus(audio: MediaStatus, ebook: MediaStatus): BookStatus {
  if (audio === "imported" && ebook === "imported") {
    return "imported";
  }

  if (audio === "imported" || ebook === "imported") {
    return "partial";
  }

  const values: Exclude<MediaStatus, "imported">[] = [audio, ebook] as Exclude<MediaStatus, "imported">[];
  return values.sort((a, b) => NON_IMPORTED_PRIORITY[b] - NON_IMPORTED_PRIORITY[a])[0] ?? "wanted";
}
