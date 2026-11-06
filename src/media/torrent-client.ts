export type TorrentRef = { clientId: string; nativeId: string };
export type DownloadLocation = { rootId: string; relativePath: string };
export type TorrentSnapshot = {
  ref: TorrentRef;
  location: DownloadLocation;
  state: "downloading" | "seeding" | "checking" | "inactive";
  complete: boolean;
  bytesDone: number | null;
  sizeBytes: number | null;
  downloadBytesPerSecond: number | null;
  message: string | null;
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
  /** Persist no-download file selection before starting an exact-match symlink layout. */
  resumeSeedOnly?(ref: TorrentRef): Promise<void>;
  /** Permanently surrender write authority after a canonical initial download completes. */
  sealDownload?(ref: TorrentRef): Promise<void>;
}
