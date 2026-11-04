import path from "node:path";
import { inspectMetainfo } from "../../src/media/metainfo";
import type {
  DownloadLocation,
  TorrentClient,
  TorrentRef,
  TorrentSnapshot,
} from "../../src/media/torrent-client";

/** Explicit test injection; production never chooses a client from fixture settings. */
export class MockBookTorrentClient implements TorrentClient {
  readonly id: string;
  readonly capabilities = {
    pausedAdd: true,
    recheck: true,
    derivedLayouts: false,
    metainfoVersions: [1],
  } as const;
  readonly adds: string[] = [];
  readonly resumes: string[] = [];
  readonly controls: string[] = [];
  readonly records = new Map<
    string,
    {
      location: DownloadLocation;
      files: { index: number; path: string }[];
      snapshot: TorrentSnapshot;
    }
  >();
  readonly rejectedHashes = new Set<string>();
  beforeAdd?: (hash: string) => void;
  failResumeOnce = false;

  constructor(
    readonly root: string,
    id = "transmission:fixture-books",
  ) {
    this.id = id;
  }

  async ensureAdded(input: {
    metainfo: Uint8Array;
    location: DownloadLocation;
  }) {
    const metadata = inspectMetainfo(input.metainfo);
    const ref = { clientId: this.id, nativeId: metadata.infoHash };
    this.beforeAdd?.(metadata.infoHash);
    if (this.rejectedHashes.has(metadata.infoHash))
      throw new Error("Fixture torrent engine unavailable");
    if (!this.records.has(metadata.infoHash)) {
      this.adds.push(metadata.infoHash);
      this.records.set(metadata.infoHash, {
        location: input.location,
        files: metadata.files.map((file, index) => ({
          index,
          path: path.join(this.root, input.location.relativePath, file.path),
        })),
        snapshot: {
          ref,
          location: input.location,
          state: "inactive",
          complete: false,
          bytesDone: 0,
          sizeBytes: metadata.size,
          downloadBytesPerSecond: 0,
          message: null,
          observedAt: new Date().toISOString(),
        },
      });
    }
    if (
      JSON.stringify(this.records.get(metadata.infoHash)!.location) !==
      JSON.stringify(input.location)
    )
      throw new Error("Fixture torrent location changed");
    return ref;
  }
  private record(ref: TorrentRef) {
    if (ref.clientId !== this.id) throw new Error("Foreign fixture client");
    const record = this.records.get(ref.nativeId);
    if (!record) throw new Error("Missing fixture torrent");
    return record;
  }
  async inspect(ref: TorrentRef) {
    return this.record(ref).snapshot;
  }
  async files(ref: TorrentRef) {
    return this.record(ref).files;
  }
  async resume(ref: TorrentRef) {
    if (this.failResumeOnce) {
      this.failResumeOnce = false;
      throw new Error("Lost resume acknowledgment");
    }
    const record = this.record(ref);
    this.resumes.push(ref.nativeId);
    record.snapshot.state = record.snapshot.complete
      ? "seeding"
      : "downloading";
  }
  async quiesce(ref: TorrentRef) {
    this.controls.push("quiesce");
    this.record(ref).snapshot.state = "inactive";
  }
  async recheck(ref: TorrentRef) {
    this.controls.push("recheck");
    this.record(ref).snapshot.state = "checking";
  }
  async removeRecord(_ref: TorrentRef) {
    throw new Error("Book transport must not remove torrent records");
  }
}
