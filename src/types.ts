export type BookKind = "single" | "multi";

export type AudioSegment = {
  path: string;
  name: string;
  size: number;
  start: number;
  end: number;
  durationMs: number;
  title?: string;
};

export type ChapterTiming = {
  id: string;
  title: string;
  startMs: number;
  endMs: number;
};

export type Book = {
  id: string;
  title: string;
  author: string;
  kind: BookKind;
  mime: string;
  totalSize: number;
  primaryFile?: string;
  files?: AudioSegment[];
  coverPath?: string;
  epubPath?: string;
  durationSeconds?: number;
  publishedAt?: Date;
  description?: string;
  descriptionHtml?: string;
  language?: string;
  isbn?: string;
  identifiers?: Record<string, string>;
  chapters?: ChapterTiming[];
};

export type TranscodeState = "pending" | "working" | "done" | "failed";

export type TranscodeStatus = {
  source: string;
  target: string;
  mtimeMs: number;
  state: TranscodeState;
  error?: string;
  outTimeMs?: number;
  speed?: number;
  durationMs?: number;
  meta?: PendingSingleMeta;
};

export type PendingSingleMeta = {
  id: string;
  title: string;
  author: string;
  coverPath?: string;
  epubPath?: string;
  durationSeconds?: number;
  publishedAt?: Date;
  description?: string;
  descriptionHtml?: string;
  language?: string;
  isbn?: string;
  identifiers?: Record<string, string>;
  chapters?: ChapterTiming[];
};

export type TranscodeJob = {
  source: string;
  target: string;
  mtimeMs: number;
  meta: PendingSingleMeta;
};

export type BookBuildResult = {
  ready?: Book;
  pendingJob?: TranscodeJob;
  sourcePath?: string;
};

export type JobChannel<T> = {
  push: (job: T) => void;
  stream: () => AsyncGenerator<T, void, unknown>;
};

export type OpfMetadata = {
  title?: string;
  author?: string;
  description?: string;
  descriptionHtml?: string;
  language?: string;
  publishedAt?: Date;
  isbn?: string;
  identifiers: Record<string, string>;
};

export type AudioTagMetadata = {
  title?: string;
  artist?: string;
  albumArtist?: string;
  description?: string;
  descriptionHtml?: string;
  language?: string;
  date?: Date;
};

export type ProbeData = {
  duration?: number;
  tags?: Record<string, string>;
  chapters?: FfprobeChapter[];
};

export type ProbeFailure = {
  file: string;
  mtimeMs: number;
  error: string;
};

export type FfprobeChapter = {
  start_time?: string;
  end_time?: string;
  tags?: Record<string, string>;
};
