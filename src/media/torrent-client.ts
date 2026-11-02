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
};

export interface TorrentClient {
  readonly capabilities: {
    pausedAdd: boolean;
    recheck: boolean;
    derivedLayouts: boolean;
    metainfoVersions: readonly number[];
  };
  ensureAdded(input: {
    metainfo: Uint8Array;
    location: DownloadLocation;
  }): Promise<TorrentRef>;
  inspect(ref: TorrentRef): Promise<TorrentSnapshot>;
  files(ref: TorrentRef): Promise<{ path: string }[]>;
  quiesce(ref: TorrentRef): Promise<void>;
  recheck(ref: TorrentRef): Promise<void>;
  resume(ref: TorrentRef): Promise<void>;
  removeRecord(ref: TorrentRef): Promise<void>;
}
