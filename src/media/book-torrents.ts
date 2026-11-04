import path from "node:path";
import { promises as fs } from "node:fs";
import type { Database } from "bun:sqlite";
import { dbSql, typedGet } from "../db/typed";
import { initializeManagedContent } from "./library-view";
import type { AppSettings } from "../app-types";
import type { BooksRepo } from "../repo";
import {
  RtorrentClient,
  type RtorrentDownloadState,
  type RtorrentImportSource,
} from "../rtorrent";
import { inspectMetainfo } from "./metainfo";
import { managedBookTransmission } from "./managed-transmission";
import { assertTorrentUseWritable, withDatabaseTorrentUse } from "./torrent-use";
import { TorrentRecordsRepo } from "./torrent-records";
import type {
  DownloadLocation,
  TorrentClient,
  TorrentRef,
  TorrentSnapshot,
} from "./torrent-client";

export type BookTorrentBinding = {
  info_hash: string;
  book_id: number;
  release_id: number | null;
  backend: "transmission";
  client_id: string;
  root_path: string;
  root_token: string;
  root_id: "books";
  relative_path: string;
  created_at: string;
};

export const BOOK_TORRENT_BINDING_SELECT = `SELECT t.native_id AS info_hash,b.book_id,b.release_id,
  t.backend,t.client_id,t.root_path,t.root_token,json_extract(t.location_json,'$.rootId') AS root_id,
  json_extract(t.location_json,'$.relativePath') AS relative_path,b.created_at
  FROM book_torrent_bindings b JOIN media_torrents t ON t.id=b.torrent_id`;

/** Durable ownership, not execution state. Tombstones survive deletion of a book/release. */
export class BookTorrentsRepo {
  constructor(
    private db: Database,
    private beforeWrite: () => void = () => {},
  ) {}

  get(infoHash: string): BookTorrentBinding | null {
    const rows = this.db.query(`${BOOK_TORRENT_BINDING_SELECT} WHERE t.native_id=? LIMIT 2`).all(infoHash.toLowerCase()) as BookTorrentBinding[];
    if (rows.length > 1) throw new Error("Book torrent hash has ambiguous client bindings");
    const row = rows[0];
    if (!row) return null;
    if (row.backend !== "transmission" || row.root_id !== "books" || !row.root_path || !row.root_token || row.relative_path !== row.info_hash)
      throw new Error("Unknown book torrent backend or root");
    return { ...row, backend: row.backend, root_id: row.root_id };
  }

  reserve(input: {
    infoHash: string;
    bookId: number;
    clientId: string;
    rootPath: string;
    rootToken: string;
  }): BookTorrentBinding {
    return this.db.transaction(() => {
    this.beforeWrite();
    const hash = input.infoHash.toLowerCase();
    assertTorrentUseWritable(this.db, { clientId: input.clientId, infoHash: hash });
    if (
      !/^[a-f0-9]{40}$/.test(hash) ||
      !Number.isSafeInteger(input.bookId) ||
      input.bookId <= 0 ||
      !input.clientId ||
      !path.isAbsolute(input.rootPath) ||
      !/^[a-f0-9-]{36}$/.test(input.rootToken)
    )
      throw new Error("Invalid book torrent intent");
    const existing = this.get(hash);
    if (existing && existing.book_id !== input.bookId)
      throw new Error(
        `Torrent intent already belongs to book ${existing.book_id}`,
      );
    if (existing && existing.client_id !== input.clientId)
      throw new Error("Book torrent belongs to another client instance");
    const record = new TorrentRecordsRepo(this.db, this.beforeWrite).reserve({ clientId: input.clientId, nativeId: hash },
      { rootId: "books", relativePath: hash }, "", { backend: "transmission", root_path: input.rootPath,
        root_token: input.rootToken, ownership_tag: "Podible Books" });
    this.db.query(`INSERT INTO book_torrent_bindings(torrent_id,book_id,created_at) VALUES(?,?,?) ON CONFLICT(torrent_id) DO NOTHING`)
      .run(record.id, input.bookId, new Date().toISOString());
    return this.get(hash)!;
    }).immediate();
  }

  attach(infoHash: string, releaseId: number) {
    return this.db.transaction(() => {
    this.beforeWrite();
    const binding = this.get(infoHash);
    if (!binding) throw new Error("Book torrent intent is missing");
    assertTorrentUseWritable(this.db, { clientId: binding.client_id, infoHash: binding.info_hash });
    const release = typedGet(
      this.db,
      dbSql
        .selectFrom("releases")
        .select(["book_id", "info_hash"])
        .where("id", "=", releaseId),
    );
    if (
      !release ||
      release.book_id !== binding.book_id ||
      release.info_hash.toLowerCase() !== binding.info_hash
    )
      throw new Error("Release does not match its book torrent intent");
    if (binding.release_id !== null && binding.release_id !== releaseId)
      throw new Error("Book torrent is already attached to another release");
    this.db.query(`UPDATE book_torrent_bindings SET release_id=? WHERE torrent_id=(SELECT id FROM media_torrents WHERE client_id=? AND native_id=?)`)
      .run(releaseId, binding.client_id, binding.info_hash);
    }).immediate();
  }

  reclaimOrphan(
    expected: BookTorrentBinding,
    bookId: number,
  ): BookTorrentBinding {
    return this.db.transaction(() => {
      this.beforeWrite();
      const binding = this.get(expected.info_hash);
      if (binding) assertTorrentUseWritable(this.db, { clientId: binding.client_id, infoHash: binding.info_hash });
      if (
        !binding ||
        binding.client_id !== expected.client_id ||
        binding.root_path !== expected.root_path ||
        binding.root_token !== expected.root_token ||
        binding.book_id !== expected.book_id ||
        binding.release_id !== null
      )
        throw new Error(
          "Orphan book torrent ownership changed during reclamation",
        );
      if (
        typedGet(
          this.db,
          dbSql
            .selectFrom("books")
            .select("id")
            .where("id", "=", binding.book_id),
        )
      )
        throw new Error("Cannot reclaim a torrent from an existing book");
      if (
        !typedGet(
          this.db,
          dbSql.selectFrom("books").select("id").where("id", "=", bookId),
        )
      )
        throw new Error("Cannot reclaim a torrent for a missing book");
      this.db.query(`UPDATE book_torrent_bindings SET book_id=? WHERE torrent_id=(SELECT id FROM media_torrents WHERE client_id=? AND native_id=?)`)
        .run(bookId, binding.client_id, binding.info_hash);
      return this.get(binding.info_hash)!;
    }).immediate();
  }

  use<T>(binding: BookTorrentBinding, action: () => Promise<T>) {
    return withDatabaseTorrentUse(this.db, this.beforeWrite, { clientId: binding.client_id, infoHash: binding.info_hash },
      action);
  }
}

export interface BookDownloadClient {
  getDownloadState(infoHash: string): Promise<RtorrentDownloadState>;
  getImportSource(infoHash: string): Promise<RtorrentImportSource>;
}

type BookRepo = Pick<BooksRepo, "findReleaseByInfoHash" | "getBookRow"> & {
  readonly bookTorrents: BookTorrentsRepo;
};
const locationFor = (binding: BookTorrentBinding): DownloadLocation => ({
  rootId: binding.root_id,
  relativePath: binding.relative_path,
});
const refFor = (binding: BookTorrentBinding): TorrentRef => ({
  clientId: binding.client_id,
  nativeId: binding.info_hash,
});
const rootInitializations = new Map<string, Promise<string>>();

function directoryFor(binding: BookTorrentBinding) {
  if (
    binding.backend !== "transmission" ||
    binding.root_id !== "books" ||
    binding.relative_path !== binding.info_hash ||
    !/^[a-f0-9]{40}$/.test(binding.info_hash) ||
    !path.isAbsolute(binding.root_path)
  )
    throw new Error("Invalid persisted book torrent location");
  return path.join(binding.root_path, binding.relative_path);
}

async function managedRoot(libraryRoot: string) {
  if (!path.isAbsolute(libraryRoot))
    throw new Error("Book library root must be absolute");
  await fs.mkdir(libraryRoot, { recursive: true });
  const canonical = await fs.realpath(libraryRoot);
  for (const directory of [
    path.join(canonical, ".podible"),
    path.join(canonical, ".podible", "downloads"),
  ]) {
    await fs.mkdir(directory, { recursive: true });
    const stat = await fs.lstat(directory);
    if (
      !stat.isDirectory() ||
      stat.isSymbolicLink() ||
      (await fs.realpath(directory)) !== directory
    )
      throw new Error("Unsafe managed book download root");
    if (stat.dev !== (await fs.stat(canonical)).dev)
      throw new Error("Book downloads must share the library filesystem");
  }
  const rootPath = path.join(canonical, ".podible", "downloads");
  let initialization = rootInitializations.get(rootPath);
  if (!initialization) {
    initialization = initializeManagedContent(rootPath);
    rootInitializations.set(rootPath, initialization);
  }
  try {
    return { rootPath, rootToken: await initialization };
  } finally {
    if (rootInitializations.get(rootPath) === initialization)
      rootInitializations.delete(rootPath);
  }
}

async function readOwnership(file: string): Promise<unknown | null> {
  let stat;
  try {
    stat = await fs.lstat(file);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
  if (!stat.isFile() || stat.isSymbolicLink())
    throw new Error("Invalid book torrent ownership marker");
  return JSON.parse(await fs.readFile(file, "utf8"));
}

async function validateDirectory(binding: BookTorrentBinding, create = false) {
  const directory = directoryFor(binding);
  const validate = async (entry: string) => {
    const stat = await fs.lstat(entry);
    if (
      !stat.isDirectory() ||
      stat.isSymbolicLink() ||
      (await fs.realpath(entry)) !== entry
    )
      throw new Error("Unsafe managed book download directory");
  };
  await validate(binding.root_path);
  const rootOwner = await readOwnership(
    path.join(binding.root_path, ".podible-content.json"),
  );
  if (
    !rootOwner ||
    typeof rootOwner !== "object" ||
    !("id" in rootOwner) ||
    rootOwner.id !== binding.root_token
  )
    throw new Error("Managed book download root identity changed");
  if (create) await fs.mkdir(directory, { recursive: true });
  await validate(directory);
  const marker = path.join(directory, ".podible-book-torrent.json");
  const expected = {
    rootToken: binding.root_token,
    clientId: binding.client_id,
    infoHash: binding.info_hash,
  };
  let owner = await readOwnership(marker);
  if (owner === null && create) {
    if ((await fs.readdir(directory)).length)
      throw new Error("Book torrent directory is nonempty and unowned");
    try {
      const file = await fs.open(marker, "wx", 0o600);
      try {
        await file.writeFile(JSON.stringify(expected));
        await file.sync();
      } finally {
        await file.close();
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    owner = await readOwnership(marker);
  }
  if (JSON.stringify(owner) !== JSON.stringify(expected))
    throw new Error("Book torrent directory ownership changed");
  return directory;
}

async function validateDownloadTree(
  directory: string,
  metadata: ReturnType<typeof inspectMetainfo>,
) {
  const files = new Set(metadata.files.map((file) => file.path));
  const directories = new Set(
    metadata.files.flatMap((file) => {
      const parts = file.path.split("/");
      return parts
        .slice(0, -1)
        .map((_, index) => parts.slice(0, index + 1).join("/"));
    }),
  );
  const walk = async (relative: string) => {
    for (const entry of await fs.readdir(path.join(directory, relative), {
      withFileTypes: true,
    })) {
      if (!relative && entry.name === ".podible-book-torrent.json") continue;
      const name = relative ? relative + "/" + entry.name : entry.name;
      const absolute = path.join(directory, name);
      if (entry.isSymbolicLink() || (await fs.realpath(absolute)) !== absolute)
        throw new Error("Unsafe managed book download file");
      if (entry.isDirectory() && directories.has(name)) await walk(name);
      else if (!entry.isFile() || !files.has(name))
        throw new Error("Unexpected file in managed book download directory");
    }
  };
  await walk("");
}

function clientFor(binding: BookTorrentBinding, override?: TorrentClient) {
  const client = override ?? managedBookTransmission(binding.root_path);
  if (client.id !== binding.client_id)
    throw new Error("Book torrent belongs to another client instance");
  return client;
}

function validateSnapshot(
  binding: BookTorrentBinding,
  snapshot: TorrentSnapshot,
) {
  if (
    snapshot.ref.clientId !== binding.client_id ||
    snapshot.ref.nativeId !== binding.info_hash ||
    snapshot.location.rootId !== binding.root_id ||
    snapshot.location.relativePath !== binding.relative_path
  )
    throw new Error("Book torrent identity or location changed");
}

export async function ensureBookTorrentAdded(
  repo: BookRepo,
  settings: AppSettings,
  bookId: number,
  metainfo: Uint8Array,
  options: {
    bookTorrentClient?: TorrentClient;
    assertActive?: () => void;
  } = {},
) {
  const metadata = inspectMetainfo(metainfo);
  if (
    metadata.files.some(
      (file) =>
        file.path.split("/")[0]?.normalize("NFC").toLowerCase() ===
        ".podible-book-torrent.json",
    )
  )
    throw new Error("Metainfo collides with the book torrent ownership marker");
  options.assertActive?.();
  let binding = repo.bookTorrents.get(metadata.infoHash);
  if (!binding) {
    const { rootPath, rootToken } = await managedRoot(settings.libraryRoot);
    const client =
      options.bookTorrentClient ?? managedBookTransmission(rootPath);
    options.assertActive?.();
    binding = repo.bookTorrents.reserve({
      infoHash: metadata.infoHash,
      bookId,
      clientId: client.id,
      rootPath,
      rootToken,
    });
  }
  const reserved = binding;
  return repo.bookTorrents.use(reserved, async () => {
  let binding = reserved;
  const reclaim = binding.book_id !== bookId;
  if (
    reclaim &&
    (binding.release_id !== null || repo.getBookRow(binding.book_id))
  )
    throw new Error(
      `Torrent intent already belongs to book ${binding.book_id}`,
    );
  const client = clientFor(binding, options.bookTorrentClient);
  const directory = await validateDirectory(binding, !reclaim);
  await validateDownloadTree(directory, metadata);
  options.assertActive?.();
  if (reclaim) {
    const snapshot = await client.inspect(refFor(binding));
    validateSnapshot(binding, snapshot);
    options.assertActive?.();
    binding = repo.bookTorrents.reclaimOrphan(binding, bookId);
  }
  const ref = await client.ensureAdded({
    metainfo,
    location: locationFor(binding),
  });
  if (ref.clientId !== binding.client_id || ref.nativeId !== binding.info_hash)
    throw new Error("Torrent engine returned another book torrent identity");
  const snapshot = await client.inspect(ref);
  validateSnapshot(binding, snapshot);
  options.assertActive?.();
  if (snapshot.state !== "checking" && snapshot.state !== "seeding")
    await client.resume(ref);
  return binding;
  });
}

/** Missing binding means a pre-Transmission release. Never infer its backend from settings. */
export function bookDownloadClient(
  repo: BookRepo,
  settings: AppSettings,
  override?: TorrentClient,
): BookDownloadClient {
  const legacy = new RtorrentClient(settings.rtorrent);
  const bindingFor = (hash: string) => {
    const binding = repo.bookTorrents.get(hash);
    if (!binding) return null;
    const release = repo.findReleaseByInfoHash(hash);
    if (
      !release ||
      release.id !== binding.release_id ||
      release.book_id !== binding.book_id
    )
      throw new Error("Book torrent release binding is missing or changed");
    return binding;
  };
  return {
    async getDownloadState(hash) {
      const binding = bindingFor(hash);
      if (!binding) return legacy.getDownloadState(hash);
      const snapshot = await clientFor(binding, override).inspect(
        refFor(binding),
      );
      validateSnapshot(binding, snapshot);
      const directory = directoryFor(binding);
      return {
        hash: binding.info_hash,
        name: null,
        complete: snapshot.complete && snapshot.state !== "checking",
        isActive: snapshot.state !== "inactive",
        basePath: directory,
        directory,
        isMultiFile: true,
        bytesDone: snapshot.bytesDone,
        sizeBytes: snapshot.sizeBytes,
        leftBytes:
          snapshot.sizeBytes === null || snapshot.bytesDone === null
            ? null
            : Math.max(0, snapshot.sizeBytes - snapshot.bytesDone),
        downRate: snapshot.downloadBytesPerSecond,
        message: snapshot.message,
      };
    },
    async getImportSource(hash) {
      const binding = bindingFor(hash);
      if (!binding) return legacy.getImportSource(hash);
      const client = clientFor(binding, override);
      const snapshot = await client.inspect(refFor(binding));
      validateSnapshot(binding, snapshot);
      if (!snapshot.complete || snapshot.state === "checking")
        throw new Error("Managed book download is not complete");
      const directory = await validateDirectory(binding);
      const files = await client.files(refFor(binding));
      if (!files.length) throw new Error("Managed book torrent has no files");
      const selectedPaths: string[] = [];
      for (const file of files) {
        const relative = path.relative(directory, file.path);
        if (
          !relative ||
          relative === ".." ||
          relative.startsWith(`..${path.sep}`) ||
          path.isAbsolute(relative) ||
          /[\x00-\x1f\\]/.test(relative) ||
          (await fs.realpath(file.path)) !== file.path ||
          !(await fs.lstat(file.path)).isFile()
        )
          throw new Error("Book torrent file escaped its managed layout");
        selectedPaths.push(file.path);
      }
      return { basePath: directory, selectedPaths };
    },
  };
}
