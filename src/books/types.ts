export type MediaType = "audio" | "ebook";

export type ReleaseStatus = "snatched" | "downloading" | "downloaded" | "imported" | "failed";

export type JobType =
  | "full_library_refresh"
  | "acquire"
  | "download"
  | "import"
  | "reconcile"
  | "chapter_analysis";

export type JobStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled";

export type ChapterAnalysisStatus = "pending" | "succeeded" | "failed";

export type AssetKind = "single" | "multi" | "ebook";

export type AuthMode = "apikey" | "local";

export type TorznabSource = {
  name: string;
  baseUrl: string;
  apiKey?: string;
  categories?: {
    audio?: string;
    ebook?: string;
  };
};

export type AppSettings = {
  torznab: TorznabSource[];
  rtorrent: {
    transport: "http-xmlrpc";
    url: string;
    username?: string;
    password?: string;
    downloadPath?: string;
  };
  libraryRoot: string;
  polling: {
    rtorrentMs: number;
    scanMs: number;
  };
  recovery: {
    stalledTorrentMinutes: number;
  };
  feed: {
    title: string;
    author: string;
  };
  auth: {
    mode: AuthMode;
    key: string;
  };
  agents: {
    enabled: boolean;
    provider: "openai-responses";
    model: string;
    apiKey: string;
    lowConfidenceThreshold: number;
    timeoutMs: number;
  };
  notifications: {
    pushover: {
      enabled: boolean;
      apiToken: string;
      userKey: string;
    };
  };
};

export type BookRow = {
  id: number;
  title: string;
  author: string;
  cover_path: string | null;
  duration_ms: number | null;
  added_at: string;
  updated_at: string;
  published_at: string | null;
  description: string | null;
  description_html: string | null;
  language: string | null;
  identifiers_json: string | null;
};

export type ReleaseRow = {
  id: number;
  book_id: number;
  provider: string;
  provider_guid: string | null;
  title: string;
  media_type: MediaType;
  info_hash: string;
  size_bytes: number | null;
  url: string;
  snatched_at: string;
  status: ReleaseStatus;
  error: string | null;
  updated_at: string;
};

export type AssetRow = {
  id: number;
  book_id: number;
  kind: AssetKind;
  mime: string;
  total_size: number;
  duration_ms: number | null;
  source_release_id: number | null;
  created_at: string;
  updated_at: string;
};

export type AssetFileRow = {
  id: number;
  asset_id: number;
  path: string;
  source_path: string | null;
  size: number;
  start: number;
  end: number;
  duration_ms: number;
  title: string | null;
  updated_at: string;
};

export type JobRow = {
  id: number;
  type: JobType;
  status: JobStatus;
  book_id: number | null;
  book_title?: string | null;
  release_id: number | null;
  payload_json: string | null;
  error: string | null;
  attempt_count: number;
  max_attempts: number;
  next_run_at: string | null;
  created_at: string;
  updated_at: string;
};

export type ChapterAnalysisRow = {
  asset_id: number;
  status: ChapterAnalysisStatus;
  source: string;
  algorithm_version: string;
  fingerprint: string;
  chapters_json: string | null;
  debug_json: string | null;
  resolved_boundary_count: number;
  total_boundary_count: number;
  error: string | null;
  updated_at: string;
};

export type DownloadView = {
  job_id: number;
  job_type: JobType;
  job_status: JobStatus;
  job_error: string | null;
  release_id: number | null;
  release_status: ReleaseStatus | null;
  release_error: string | null;
  media_type: MediaType | null;
  info_hash: string | null;
  book_id: number | null;
};

export type TorrentCacheRow = {
  key: string;
  provider: string | null;
  provider_guid: string | null;
  url: string;
  info_hash: string | null;
  torrent_bytes: Uint8Array;
  created_at: string;
  updated_at: string;
};

export type LibraryBook = {
  id: number;
  title: string;
  author: string;
  coverUrl: string | null;
  durationMs: number | null;
  addedAt: string;
  updatedAt: string;
  publishedAt: string | null;
  description: string | null;
  descriptionHtml: string | null;
  language: string | null;
  identifiers: Record<string, string>;
  audioStatus: "wanted" | "snatched" | "downloading" | "downloaded" | "imported" | "error";
  ebookStatus: "wanted" | "snatched" | "downloading" | "downloaded" | "imported" | "error";
  status: "wanted" | "snatched" | "downloading" | "downloaded" | "imported" | "error" | "partial";
  fullPseudoProgress: number;
};
