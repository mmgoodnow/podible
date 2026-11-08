export type TorrentRef = { clientId: string; nativeId: string };
export type DownloadLocation = { rootId: string; relativePath: string };
export class TorrentNotFoundError extends Error {
  constructor() { super("Managed torrent record is missing"); this.name = "TorrentNotFoundError"; }
}
export type TorrentSnapshot = {
  ref: TorrentRef;
  location: DownloadLocation;
  state: "downloading" | "seeding" | "checking" | "inactive";
  complete: boolean;
  bytesDone: number | null;
  sizeBytes: number | null;
  downloadBytesPerSecond: number | null;
  message: string | null;
  /** Tracker availability is independent of local file integrity; absent classification is not proof of safety. */
  errorKind?: "tracker" | "storage" | "unknown" | null;
  integrityError?: "piece-mismatch" | null;
  observedAt: string;
  seedOnly?: boolean;
};

export interface TorrentClient {
  readonly id: string;
  readonly capabilities: {
    pausedAdd: boolean;
    recheck: boolean;
    derivedLayouts: boolean;
    metainfoVersions: readonly number[];
    readOnlySeeding?: boolean;
    canonicalDownloads?: boolean;
    optimisticSeeding?: boolean;
  };
  ensureAdded(input: {
    metainfo: Uint8Array;
    location: DownloadLocation;
    seedOnly?: boolean;
  }): Promise<TorrentRef>;
  inspect(ref: TorrentRef): Promise<TorrentSnapshot>;
  files(ref: TorrentRef): Promise<{ index: number; path: string }[]>;
  quiesce(ref: TorrentRef): Promise<void>;
  recheck(ref: TorrentRef): Promise<void>;
  resume(ref: TorrentRef): Promise<void>;
  removeRecord(ref: TorrentRef): Promise<void>;
  /** Retire only an exact seed-only intent after confirmed daemon absence. Caller serializes registration. */
  forgetSeedOnlyIntent?(ref: TorrentRef, location: DownloadLocation): Promise<void>;
  /** Persist no-download file selection before starting an exact-match symlink layout. */
  resumeSeedOnly?(ref: TorrentRef): Promise<void>;
  /** Permanently surrender write authority after a canonical initial download completes. */
  sealDownload?(ref: TorrentRef): Promise<void>;
}
