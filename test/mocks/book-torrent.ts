import { promises as fs } from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import bencode from "bencode";
import { inspectMetainfo } from "../../src/media/metainfo";
import type {
  DownloadLocation,
  TorrentClient,
  TorrentRef,
  TorrentSnapshot,
} from "../../src/media/torrent-client";

type FixtureOptions = {
  contents?: Record<string, Record<string, Uint8Array | string>>;
  completeAfterPolls?: number;
};
type RecordState = {
  snapshot: TorrentSnapshot;
  files: { index: number; path: string }[];
  polls: number;
};

export function bookTorrentMetainfo(
  name: string,
  contents: Uint8Array | string = "ID3-fixture",
) {
  const bytes =
    typeof contents === "string"
      ? Buffer.from(contents)
      : Buffer.from(contents);
  const pieceLength = 16_384;
  const pieces = [];
  for (let offset = 0; offset < bytes.length; offset += pieceLength)
    pieces.push(
      createHash("sha1")
        .update(bytes.subarray(offset, offset + pieceLength))
        .digest(),
    );
  return new Uint8Array(
    bencode.encode({
      info: {
        name,
        length: bytes.length,
        "piece length": pieceLength,
        pieces: Buffer.concat(pieces),
      },
    }),
  );
}

/** New book acquisitions must use the managed client seam, never an XML-RPC fallback. */
export class FakeBookTorrentClient implements TorrentClient {
  readonly id = "transmission:acceptance-books";
  readonly capabilities = {
    pausedAdd: true,
    recheck: true,
    derivedLayouts: false,
    metainfoVersions: [1],
  } as const;
  readonly adds: string[] = [];
  readonly addAttempts: string[] = [];
  readonly resumes: string[] = [];
  readonly controls: string[] = [];
  private records = new Map<string, RecordState>();
  private adding = new Map<string, Promise<TorrentRef>>();

  constructor(
    readonly libraryRoot: string,
    private options: FixtureOptions = {},
  ) {}

  async ensureAdded(input: {
    metainfo: Uint8Array;
    location: DownloadLocation;
  }): Promise<TorrentRef> {
    const metadata = inspectMetainfo(input.metainfo);
    const hash = metadata.infoHash;
    this.addAttempts.push(hash);
    if (
      input.location.rootId !== "books" ||
      input.location.relativePath !== hash
    )
      throw new Error("Unexpected book fixture download location");
    const pending = this.adding.get(hash);
    if (pending) return pending;
    const existing = this.records.get(hash);
    if (existing) return { ...existing.snapshot.ref };
    const add = (async () => {
      await fs.mkdir(this.libraryRoot, { recursive: true });
      const root = await fs.realpath(this.libraryRoot);
      const directory = path.join(root, ".podible", "downloads", hash);
      await fs.mkdir(directory, { recursive: true });
      const files = [];
      for (const [index, file] of metadata.files.entries()) {
        const filename = path.join(directory, file.path);
        await fs.mkdir(path.dirname(filename), { recursive: true });
        const defaultContents = Buffer.from("ID3-fixture");
        const contents =
          this.options.contents?.[hash]?.[file.path] ??
          (file.size === defaultContents.length
            ? defaultContents
            : Buffer.alloc(file.size, "x"));
        if (Buffer.byteLength(contents) !== file.size)
          throw new Error("Fixture payload disagrees with metainfo size");
        await fs.writeFile(filename, contents);
        files.push({ index, path: filename });
      }
      const ref = { clientId: this.id, nativeId: hash };
      this.records.set(hash, {
        snapshot: {
          ref,
          location: { ...input.location },
          state: "inactive",
          complete: false,
          bytesDone: 0,
          sizeBytes: metadata.size,
          downloadBytesPerSecond: 0,
          message: null,
          observedAt: new Date(0).toISOString(),
        },
        files,
        polls: 0,
      });
      this.adds.push(hash);
      return { ...ref };
    })();
    this.adding.set(hash, add);
    try {
      return await add;
    } finally {
      this.adding.delete(hash);
    }
  }

  private record(ref: TorrentRef) {
    if (ref.clientId !== this.id)
      throw new Error("Foreign book fixture client");
    const record = this.records.get(ref.nativeId);
    if (!record) throw new Error("Missing book fixture torrent");
    return record;
  }

  async inspect(ref: TorrentRef): Promise<TorrentSnapshot> {
    const record = this.record(ref);
    if (
      record.snapshot.state === "downloading" ||
      record.snapshot.state === "checking"
    ) {
      record.polls += 1;
      if (record.polls >= (this.options.completeAfterPolls ?? 2)) {
        record.snapshot.complete = true;
        record.snapshot.state = "seeding";
        record.snapshot.bytesDone = record.snapshot.sizeBytes;
        record.snapshot.downloadBytesPerSecond = 0;
      } else {
        record.snapshot.bytesDone = Math.floor(record.snapshot.sizeBytes! / 2);
        record.snapshot.downloadBytesPerSecond = 10;
      }
    }
    return structuredClone(record.snapshot);
  }
  async files(ref: TorrentRef) {
    return structuredClone(this.record(ref).files);
  }
  async resume(ref: TorrentRef) {
    const record = this.record(ref);
    this.resumes.push(ref.nativeId);
    record.snapshot.state = record.snapshot.complete
      ? "seeding"
      : "downloading";
  }
  async quiesce(ref: TorrentRef) {
    this.record(ref).snapshot.state = "inactive";
    this.controls.push("quiesce");
  }
  async recheck(ref: TorrentRef) {
    this.record(ref).snapshot.state = "checking";
    this.controls.push("recheck");
  }
  async removeRecord(ref: TorrentRef) {
    this.record(ref);
    throw new Error("Book acceptance must not delete torrent records");
  }
}

/** Parent-owned external state survives SIGKILL of a worker fixture. */
export function startBookTorrentFixture(client: FakeBookTorrentClient) {
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      try {
        const body = (await request.json()) as {
          method: string;
          metainfo: string;
          location: DownloadLocation;
          ref: TorrentRef;
        };
        let result: unknown;
        switch (body.method) {
          case "ensureAdded":
            result = await client.ensureAdded({
              metainfo: Buffer.from(body.metainfo, "base64"),
              location: body.location,
            });
            break;
          case "inspect":
            result = await client.inspect(body.ref);
            break;
          case "files":
            result = await client.files(body.ref);
            break;
          case "resume":
            result = await client.resume(body.ref);
            break;
          case "quiesce":
            result = await client.quiesce(body.ref);
            break;
          case "recheck":
            result = await client.recheck(body.ref);
            break;
          case "removeRecord":
            result = await client.removeRecord(body.ref);
            break;
          default:
            throw new Error("Unknown book fixture action");
        }
        return Response.json({ result: result ?? null });
      } catch (error) {
        return Response.json(
          { error: error instanceof Error ? error.message : String(error) },
          { status: 400 },
        );
      }
    },
  });
  return { url: server.url.href, stop: () => server.stop(true) };
}

export function connectBookTorrentFixture(url: string): TorrentClient {
  async function call<T>(method: string, input: object): Promise<T> {
    const response = await fetch(url, {
      method: "POST",
      body: JSON.stringify({ method, ...input }),
    });
    const body = (await response.json()) as { result: T; error?: string };
    if (!response.ok) throw new Error(body.error);
    return body.result;
  }
  return {
    id: "transmission:acceptance-books",
    capabilities: {
      pausedAdd: true,
      recheck: true,
      derivedLayouts: false,
      metainfoVersions: [1],
    },
    ensureAdded: (input) =>
      call<TorrentRef>("ensureAdded", {
        ...input,
        metainfo: Buffer.from(input.metainfo).toString("base64"),
      }),
    inspect: (ref) => call<TorrentSnapshot>("inspect", { ref }),
    files: (ref) => call<{ index: number; path: string }[]>("files", { ref }),
    resume: (ref) => call<void>("resume", { ref }),
    quiesce: (ref) => call<void>("quiesce", { ref }),
    recheck: (ref) => call<void>("recheck", { ref }),
    removeRecord: (ref) => call<void>("removeRecord", { ref }),
  };
}
