import path from "node:path";
import { z } from "zod";
import { inspectMetainfo } from "./metainfo";
import type {
  TorrentClient,
  TorrentRef,
  DownloadLocation,
  TorrentSnapshot,
} from "./torrent-client";

export class TransmissionRpc {
  private sessionId = "";
  constructor(
    private options: {
      url: string;
      username: string;
      password: string;
      fetcher?: typeof fetch;
      timeoutMs?: number;
    },
  ) {}
  async call(
    method: string,
    args: Record<string, unknown> = {},
  ): Promise<Record<string, unknown>> {
    for (let attempt = 0; attempt < 3; attempt++) {
      let response: Response;
      try {
        response = await (this.options.fetcher ?? fetch)(this.options.url, {
          method: "POST",
          redirect: "error",
          signal: AbortSignal.timeout(this.options.timeoutMs ?? 15_000),
          headers: {
            "Content-Type": "application/json",
            "X-Transmission-Session-Id": this.sessionId,
            Authorization: `Basic ${Buffer.from(`${this.options.username}:${this.options.password}`).toString("base64")}`,
          },
          body: JSON.stringify({ method, arguments: args }),
        });
      } catch {
        throw new Error("Private torrent engine is unavailable");
      }
      if (response.status === 409) {
        const session = response.headers.get("X-Transmission-Session-Id");
        await response.body?.cancel();
        if (!session)
          throw new Error("Torrent engine omitted its session handshake");
        this.sessionId = session;
        continue;
      }
      if (!response.ok)
        throw new Error(
          `Private torrent engine returned HTTP ${response.status}`,
        );
      const result = z
        .object({
          result: z.string(),
          arguments: z.record(z.string(), z.unknown()).optional(),
        })
        .parse(await response.json());
      if (result.result !== "success")
        throw new Error(`Torrent engine rejected ${method}`);
      return result.arguments ?? {};
    }
    throw new Error("Torrent engine session handshake did not converge");
  }
}
const torrentSchema = z.object({
  hashString: z.string(),
  downloadDir: z.string(),
  labels: z.array(z.string()),
  status: z.number().int(),
  leftUntilDone: z.number().nonnegative(),
  sizeWhenDone: z.number().nonnegative(),
  rateDownload: z.number().nonnegative(),
  error: z.number(),
  errorString: z.string(),
  files: z.array(
    z.object({ name: z.string(), length: z.number().nonnegative() }),
  ),
});
export class TransmissionAdapter implements TorrentClient {
  get id() {
    return this.options.clientId;
  }
  readonly capabilities = {
    pausedAdd: true,
    recheck: true,
    // Isolated regular-file/CoW layouts, not a no-write symlink seeding mode.
    derivedLayouts: true,
    metainfoVersions: [1],
  } as const;
  constructor(
    private options: {
      clientId: string;
      roots: Record<string, string>;
      ownershipTag: string;
      rpc: TransmissionRpc;
    },
  ) {}
  private directory(location: DownloadLocation) {
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
        .some((p) => !p || p === "." || p === "..")
    )
      throw new Error("Invalid managed download location");
    return path.join(root, location.relativePath);
  }
  private location(directory: string): DownloadLocation {
    for (const [rootId, root] of Object.entries(this.options.roots))
      if (directory.startsWith(path.resolve(root) + path.sep)) {
        const location = {
          rootId,
          relativePath: path.relative(root, directory),
        };
        this.directory(location);
        return location;
      }
    throw new Error("Torrent directory is outside managed roots");
  }
  private validateRef(ref: TorrentRef) {
    if (ref.clientId !== this.id)
      throw new Error("Torrent belongs to another client instance");
    if (!/^[a-f0-9]{40}$/.test(ref.nativeId))
      throw new Error("Invalid torrent identity");
  }
  private async get(ref: TorrentRef) {
    this.validateRef(ref);
    const result = await this.options.rpc.call("torrent-get", {
      ids: [ref.nativeId],
      fields: [
        "hashString",
        "downloadDir",
        "labels",
        "status",
        "leftUntilDone",
        "sizeWhenDone",
        "rateDownload",
        "error",
        "errorString",
        "files",
      ],
    });
    const torrents = z.array(torrentSchema).parse(result.torrents);
    const row = torrents.find(
      (t) => t.hashString.toLowerCase() === ref.nativeId,
    );
    if (row) {
      if (!row.labels.includes(this.options.ownershipTag))
        throw new Error("Refusing to control an unmanaged torrent");
      this.location(row.downloadDir);
    }
    return row;
  }
  private async owned(ref: TorrentRef) {
    const row = await this.get(ref);
    if (!row) throw new Error("Managed torrent record is missing");
    return row;
  }
  async ensureAdded(input: {
    metainfo: Uint8Array;
    location: DownloadLocation;
  }): Promise<TorrentRef> {
    const metadata = inspectMetainfo(input.metainfo);
    const directory = this.directory(input.location);
    const ref = { clientId: this.id, nativeId: metadata.infoHash };
    if (!(await this.get(ref))) {
      let failed: unknown;
      try {
        await this.options.rpc.call("torrent-add", {
          metainfo: Buffer.from(input.metainfo).toString("base64"),
          "download-dir": directory,
          paused: true,
          labels: [this.options.ownershipTag],
        });
      } catch (error) {
        failed = error;
      }
      // Resolve uncertain add acknowledgments by stable infohash, never by numeric ID.
      if (!(await this.get(ref)))
        throw failed ?? new Error("Torrent addition was not observed");
    }
    const row = await this.owned(ref);
    if (path.normalize(row.downloadDir) !== directory)
      throw new Error("Existing torrent uses another layout");
    if (
      row.files.length !== metadata.files.length ||
      row.files.some(
        (file, i) =>
          file.name !== metadata.files[i]!.path ||
          file.length !== metadata.files[i]!.size,
      )
    )
      throw new Error("Torrent engine file layout differs from metainfo");
    return ref;
  }
  async inspect(ref: TorrentRef): Promise<TorrentSnapshot> {
    const row = await this.owned(ref);
    if (![0, 1, 2, 3, 4, 5, 6].includes(row.status))
      throw new Error("Unknown torrent engine state");
    return {
      ref,
      location: this.location(row.downloadDir),
      state: [1, 2].includes(row.status)
        ? "checking"
        : row.status === 0
          ? "inactive"
          : [5, 6].includes(row.status)
            ? "seeding"
            : "downloading",
      complete: row.leftUntilDone === 0,
      bytesDone: row.sizeWhenDone - row.leftUntilDone,
      sizeBytes: row.sizeWhenDone,
      downloadBytesPerSecond: row.rateDownload,
      message: row.error
        ? "Torrent engine reported a tracker or storage error"
        : null,
      observedAt: new Date().toISOString(),
    };
  }
  async files(ref: TorrentRef) {
    const row = await this.owned(ref);
    return row.files.map((file, index) => {
      if (
        path.isAbsolute(file.name) ||
        /[\x00-\x1f\\]/.test(file.name) ||
        file.name.split("/").some((p) => !p || p === "." || p === "..")
      )
        throw new Error("Torrent file escaped its managed layout");
      return { index, path: path.join(row.downloadDir, file.name) };
    });
  }
  async quiesce(ref: TorrentRef) {
    if ((await this.owned(ref)).status === 0) return;
    await this.options.rpc.call("torrent-stop", { ids: [ref.nativeId] });
    const deadline = Date.now() + 5000;
    let inactiveSince: number | null = null;
    for (;;) {
      // Transmission defers stop_soon() to its pulse. An old check can finish
      // first, exposing STOPPED before that stop cancels the next verification.
      // Only a stable inactive observation settles an issued stop request.
      if ((await this.inspect(ref)).state === "inactive") {
        inactiveSince ??= Date.now();
        if (Date.now() - inactiveSince >= 1000) return;
      } else inactiveSince = null;
      if (Date.now() > deadline)
        throw new Error("Torrent did not become quiescent");
      await Bun.sleep(50);
    }
  }
  async recheck(ref: TorrentRef) {
    await this.quiesce(ref);
    await this.options.rpc.call("torrent-verify", { ids: [ref.nativeId] });
  }
  async resume(ref: TorrentRef) {
    await this.owned(ref);
    if ((await this.inspect(ref)).state === "checking")
      throw new Error("Cannot resume a torrent during recheck");
    await this.options.rpc.call("torrent-start", { ids: [ref.nativeId] });
  }
  async removeRecord(ref: TorrentRef) {
    await this.quiesce(ref);
    await this.options.rpc.call("torrent-remove", {
      ids: [ref.nativeId],
      "delete-local-data": false,
    });
  }
}
