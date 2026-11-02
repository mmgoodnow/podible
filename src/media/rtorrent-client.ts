import path from "node:path";
import type { AppSettings } from "../app-types";
import { normalizeInfoHash } from "../library/torrent";
import { RtorrentClient } from "../rtorrent";
import { inspectMetainfo } from "./metainfo";
import type {
  DownloadLocation,
  TorrentClient,
  TorrentRef,
  TorrentSnapshot,
} from "./torrent-client";

export class RtorrentAdapter implements TorrentClient {
  readonly capabilities = {
    pausedAdd: true,
    recheck: true,
    derivedLayouts: false,
    metainfoVersions: [1],
  } as const;
  private readonly transport: RtorrentClient;
  constructor(
    private readonly options: {
      clientId: string;
      settings: AppSettings["rtorrent"];
      roots: Record<string, string>;
      ownershipTag: string;
    },
  ) {
    if (!options.clientId.trim() || !options.ownershipTag.trim())
      throw new Error("Client identity and ownership tag are required");
    this.transport = new RtorrentClient(options.settings);
  }
  private location(location: DownloadLocation) {
    const root = Object.hasOwn(this.options.roots, location.rootId)
      ? this.options.roots[location.rootId]
      : undefined;
    if (
      !root ||
      !path.isAbsolute(root) ||
      path.isAbsolute(location.relativePath) ||
      /[\x00-\x1f\\]/.test(location.relativePath) ||
      location.relativePath
        .split("/")
        .some((part) => !part || part === "." || part === "..")
    )
      throw new Error("Invalid managed download location");
    return path.join(root, location.relativePath);
  }
  private async owned(ref: TorrentRef) {
    if (ref.clientId !== this.options.clientId)
      throw new Error("Torrent belongs to another client instance");
    const hash = normalizeInfoHash(ref.nativeId);
    if (
      (await this.transport.getOwnershipTag(hash)) !== this.options.ownershipTag
    )
      throw new Error("Refusing to control an unmanaged torrent");
    return hash;
  }
  async ensureAdded(input: {
    metainfo: Uint8Array;
    location: DownloadLocation;
  }) {
    const metainfo = inspectMetainfo(input.metainfo);
    const directory = this.location(input.location);
    const ref = {
      clientId: this.options.clientId,
      nativeId: metainfo.infoHash,
    };
    if (!(await this.transport.hasDownload(metainfo.infoHash))) {
      try {
        await this.transport.loadRawPaused(
          input.metainfo,
          directory,
          this.options.ownershipTag,
        );
      } catch (error) {
        if (!(await this.transport.hasDownload(metainfo.infoHash))) throw error;
      }
    }
    await this.owned(ref);
    const state = await this.transport.getDownloadState(ref.nativeId);
    if (
      state.directory === null ||
      path.normalize(state.directory) !== directory
    )
      throw new Error("Existing torrent uses another layout");
    return ref;
  }
  private observedLocation(directory: string | null): DownloadLocation {
    if (directory !== null) for (const [rootId, root] of Object.entries(this.options.roots)) {
      if (directory === root || directory.startsWith(`${root}${path.sep}`)) {
        const relativePath = path.relative(root, directory);
        if (!relativePath.startsWith("..")) return { rootId, relativePath };
      }
    }
    throw new Error("Torrent directory is outside managed roots");
  }
  async inspect(ref: TorrentRef): Promise<TorrentSnapshot> {
    const hash = await this.owned(ref);
    const [state, hashing] = await Promise.all([
      this.transport.getDownloadState(hash),
      this.transport.getHashingState(hash),
    ]);
    if (hashing === null) throw new Error("Torrent checking state is unknown");
    return {
      ref,
      location: this.observedLocation(state.directory),
      state:
        hashing > 0
          ? "checking"
          : !state.isActive
            ? "inactive"
            : state.complete
              ? "seeding"
              : "downloading",
      complete: state.complete,
      bytesDone: state.bytesDone,
      sizeBytes: state.sizeBytes,
      downloadBytesPerSecond: state.downRate,
      message: state.message,
      observedAt: new Date().toISOString(),
    };
  }
  async files(ref: TorrentRef) {
    const state = await this.inspect(ref);
    const directory = this.location(state.location);
    return (await this.transport.getImportSource(ref.nativeId)).selectedPaths.map(file => {
      if (!path.resolve(file).startsWith(`${directory}${path.sep}`)) throw new Error("Torrent file escaped its managed layout");
      return { path: file };
    });
  }
  async quiesce(ref: TorrentRef) {
    const hash = await this.owned(ref);
    await this.transport.control(hash, "stop");
    await this.transport.control(hash, "close");
    if ((await this.inspect(ref)).state !== "inactive")
      throw new Error("Torrent did not become quiescent");
  }
  async recheck(ref: TorrentRef) {
    await this.quiesce(ref);
    await this.transport.control(ref.nativeId, "check_hash");
  }
  async resume(ref: TorrentRef) {
    await this.owned(ref);
    if ((await this.inspect(ref)).state === "checking")
      throw new Error("Cannot resume a torrent during recheck");
    await this.transport.control(ref.nativeId, "open");
    await this.transport.control(ref.nativeId, "start");
  }
  async removeRecord(ref: TorrentRef) {
    await this.quiesce(ref);
    await this.transport.control(ref.nativeId, "erase");
  }
}
