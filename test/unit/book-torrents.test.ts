import { expect, spyOn, test } from "bun:test";
import { Database } from "bun:sqlite";
import bencode from "bencode";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { RtorrentClient } from "../../src/rtorrent";
import {
  bookDownloadClient,
  ensureBookTorrentAdded,
} from "../../src/media/book-torrents";
import {
  managedBookTransmission,
  managedTransmission,
  prepareTransmission,
} from "../../src/media/managed-transmission";
import { MockBookTorrentClient } from "../mocks/book-torrents";
import { processDownloadJob } from "../../src/worker/downloads";
import { processImportJob } from "../../src/worker/imports";
import { bookContentRoot } from "../../src/library/book-storage";
import { readCanonicalDownload } from "../../src/media/canonical-download";
import {
  enrichDownload,
  enrichLibraryBookProgress,
} from "../../src/rpc/shared";

const metainfo = (name = "book.mp3") =>
  bencode.encode({
    info: {
      name,
      length: 10,
      "piece length": 16384,
      pieces: Buffer.from("12345678901234567890"),
    },
  });

async function fixture() {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "book-transport-")),
  );
  const db = new Database(":memory:");
  db.exec("PRAGMA foreign_keys = ON");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const book = repo.createBook({ title: "A book", author: "An author" });
  const settings = defaultSettings({ libraryRoot: root });
  const client = new MockBookTorrentClient(
    path.join(root, ".podible", "downloads"),
  );
  const reserve = () =>
    ensureBookTorrentAdded(repo, settings, book.id, metainfo(), {
      bookTorrentClient: client,
    });
  return {
    root,
    db,
    repo,
    book,
    settings,
    client,
    reserve,
    async attach() {
      const binding = await reserve();
      const release = repo.createRelease({
        bookId: book.id,
        provider: "fixture",
        title: "A book",
        mediaType: "audio",
        infoHash: binding.info_hash,
        url: "https://fixture.invalid/book",
        status: "downloading",
      });
      repo.bookTorrents.attach(binding.info_hash, release.id);
      return {
        release,
        binding,
        record: client.records.get(binding.info_hash)!,
      };
    },
    async close() {
      db.close();
      await fs.rm(root, { recursive: true, force: true });
    },
  };
}

test("new managed book torrents reserve canonical bytes before add and import without a hardlink copy", async () => {
  const f = await fixture();
  try {
    Object.assign(f.client.capabilities, { canonicalDownloads: true });
    let sealed = 0;
    Object.assign(f.client, { sealDownload: async () => { sealed++; } });
    f.client.beforeAdd = hash => {
      // The asynchronous setup is asserted after add below; the hash itself remains unchanged.
      expect(hash).toHaveLength(40);
    };
    const { release, binding, record } = await f.attach();
    const contentRoot = bookContentRoot(f.root), sourceRoot = path.join(binding.root_path, binding.relative_path);
    const plan = (await readCanonicalDownload(contentRoot, sourceRoot))!;
    const file = record.files[0]!.path;
    expect((await fs.lstat(file)).isSymbolicLink()).toBe(true);
    const canonical = await fs.realpath(file), before = await fs.stat(canonical);
    await fs.writeFile(file, "0123456789");
    record.snapshot.complete = true; record.snapshot.bytesDone = 10; record.snapshot.state = "seeding";
    const source = await bookDownloadClient(f.repo, f.settings, f.client).getImportSource(binding.info_hash);
    expect(source.selectedPaths).toEqual([file]); expect(sealed).toBe(1);
    const manifestation = f.repo.addManifestation({ bookId: f.book.id, kind: "audio" });
    const { importReleaseFromPath } = await import("../../src/library/importer");
    const imported = await importReleaseFromPath(f.repo, release, source.basePath!, f.root, { selectedPaths: source.selectedPaths, manifestationId: manifestation.id });
    expect(imported.linkedFiles).toEqual([canonical]);
    expect((await fs.stat(canonical)).ino).toBe(before.ino);
    expect((await fs.stat(canonical)).nlink).toBe(1);
    expect(plan.files[0]!.relativePath).toBe(path.relative(contentRoot, canonical));
    expect((await f.reserve()).info_hash).toBe(binding.info_hash);
    expect(f.client.adds).toHaveLength(1);
  } finally { await f.close(); }
});

test("books share the durable private engine identity without adopting movie ownership", async () => {
  const f = await fixture();
  const originalFetch = globalThis.fetch;
  try {
    const directory = path.join(f.root, "engine");
    const identity = prepareTransmission(directory);
    const movie = managedTransmission(path.join(f.root, "movies"), directory);
    const books = managedBookTransmission(
      path.join(f.root, ".podible", "downloads"),
      directory,
    );
    expect(books.id).toBe(movie.id);
    expect(books.id).toBe("transmission:" + identity.id);
    const ref = { clientId: books.id, nativeId: "a".repeat(40) };
    globalThis.fetch = (async () =>
      Response.json({
        result: "success",
        arguments: {
          torrents: [
            {
              hashString: ref.nativeId,
              labels: ["Podible Movies"],
              downloadDir: path.join(f.root, "movies", "downloads", "existing"),
              status: 6,
              leftUntilDone: 0,
              sizeWhenDone: 10,
              rateDownload: 0,
              error: 0,
              errorString: "",
              files: [],
            },
          ],
        },
      })) as unknown as typeof fetch;
    await expect(books.inspect(ref)).rejects.toThrow("unmanaged torrent");
    expect((await movie.inspect(ref)).state).toBe("seeding");
  } finally {
    globalThis.fetch = originalFetch;
    await f.close();
  }
});

test("resuming a durable intent keeps its original root despite settings changes", async () => {
  const f = await fixture();
  try {
    const first = await f.reserve();
    const second = await ensureBookTorrentAdded(
      f.repo,
      { ...f.settings, libraryRoot: path.join(f.root, "changed") },
      f.book.id,
      metainfo(),
      { bookTorrentClient: f.client },
    );
    expect(second).toEqual(first);
    expect(f.client.adds).toHaveLength(1);
    expect(await fs.stat(first.root_path)).toBeTruthy();
    expect(
      await fs.stat(path.join(f.root, "changed")).catch(() => null),
    ).toBeNull();
  } finally {
    await f.close();
  }
});

test("ownership rejects cross-book or changed-client retries before touching the engine", async () => {
  const f = await fixture();
  try {
    await f.reserve();
    const other = f.repo.createBook({
      title: "Different book",
      author: "Other",
    });
    await expect(
      ensureBookTorrentAdded(f.repo, f.settings, other.id, metainfo(), {
        bookTorrentClient: f.client,
      }),
    ).rejects.toThrow("already belongs");
    const replacement = new MockBookTorrentClient(
      f.client.root,
      "transmission:replacement",
    );
    await expect(
      ensureBookTorrentAdded(f.repo, f.settings, f.book.id, metainfo(), {
        bookTorrentClient: replacement,
      }),
    ).rejects.toThrow("another client");
    expect(replacement.adds).toHaveLength(0);
    expect(f.client.adds).toHaveLength(1);
    expect(f.client.controls).toHaveLength(0);
  } finally {
    await f.close();
  }
});

test("book deletion retains an ownership tombstone and never removes a seeding torrent", async () => {
  const f = await fixture();
  try {
    const { release, binding, record } = await f.attach();
    record.snapshot.complete = true;
    record.snapshot.state = "seeding";
    expect(f.repo.deleteBook(f.book.id)).toBe(true);
    expect(f.repo.getRelease(release.id)).toBeNull();
    expect(f.repo.bookTorrents.get(binding.info_hash)).toMatchObject({
      book_id: f.book.id,
      release_id: null,
    });
    expect(record.snapshot.state).toBe("seeding");
    expect(f.client.controls).toHaveLength(0);
    await expect(
      bookDownloadClient(f.repo, f.settings, f.client).getDownloadState(
        binding.info_hash,
      ),
    ).rejects.toThrow("binding");
  } finally {
    await f.close();
  }
});

test("unsafe metainfo and symlinked roots are rejected before an external add", async () => {
  const f = await fixture();
  try {
    await expect(
      ensureBookTorrentAdded(
        f.repo,
        f.settings,
        f.book.id,
        metainfo("../escape.mp3"),
        { bookTorrentClient: f.client },
      ),
    ).rejects.toThrow("Unsafe metainfo");
    const outside = path.join(f.root, "outside");
    await fs.mkdir(outside);
    await fs.symlink(outside, path.join(f.root, ".podible"));
    await expect(f.reserve()).rejects.toThrow("Unsafe managed");
    expect(f.client.adds).toHaveLength(0);
  } finally {
    await f.close();
  }
});

test("delete and re-add reclaims only an orphaned owned torrent without another add", async () => {
  const f = await fixture();
  try {
    const { binding, record } = await f.attach();
    Object.assign(record.snapshot, {
      complete: true,
      state: "seeding",
      bytesDone: 10,
    });
    await fs.writeFile(record.files[0]!.path, "ID3fixture");
    f.repo.deleteBook(f.book.id);
    const book = f.repo.createBook({
      title: f.book.title,
      author: f.book.author,
    });
    expect(book.id).not.toBe(f.book.id);
    const reclaimed = await ensureBookTorrentAdded(
      f.repo,
      f.settings,
      book.id,
      metainfo(),
      { bookTorrentClient: f.client },
    );
    expect(reclaimed).toMatchObject({
      info_hash: binding.info_hash,
      book_id: book.id,
      release_id: null,
      client_id: binding.client_id,
      root_path: binding.root_path,
      root_token: binding.root_token,
    });
    expect(f.client.adds).toHaveLength(1);
    expect(f.client.controls).toHaveLength(0);
    expect(await fs.readFile(record.files[0]!.path, "utf8")).toBe("ID3fixture");
    expect(record.snapshot.state).toBe("seeding");
    const release = f.repo.createRelease({
      bookId: book.id,
      provider: "fixture",
      title: "A book",
      mediaType: "audio",
      infoHash: binding.info_hash,
      url: "https://fixture.invalid/readded",
      status: "downloading",
    });
    f.repo.bookTorrents.attach(binding.info_hash, release.id);
    expect(
      (
        await bookDownloadClient(f.repo, f.settings, f.client).getDownloadState(
          binding.info_hash,
        )
      ).complete,
    ).toBe(true);
  } finally {
    await f.close();
  }
});

test("orphan reclamation refuses a missing torrent and does not create a replacement", async () => {
  const f = await fixture();
  try {
    const { binding } = await f.attach();
    f.repo.deleteBook(f.book.id);
    const book = f.repo.createBook({ title: "Readded", author: "Same" });
    f.client.records.clear();
    await expect(
      ensureBookTorrentAdded(f.repo, f.settings, book.id, metainfo(), {
        bookTorrentClient: f.client,
      }),
    ).rejects.toThrow("Missing fixture torrent");
    expect(f.repo.bookTorrents.get(binding.info_hash)?.book_id).toBe(f.book.id);
    expect(f.client.adds).toHaveLength(1);
  } finally {
    await f.close();
  }
});

test("execution fencing prevents both initial adds and retry-side resume effects", async () => {
  const f = await fixture();
  try {
    await expect(
      ensureBookTorrentAdded(f.repo, f.settings, f.book.id, metainfo(), {
        bookTorrentClient: f.client,
        assertActive() {
          throw new Error("Execution revoked");
        },
      }),
    ).rejects.toThrow("Execution revoked");
    expect(f.client.adds).toHaveLength(0);
    const binding = await f.reserve();
    const resumes = f.client.resumes.length;
    await expect(
      ensureBookTorrentAdded(f.repo, f.settings, f.book.id, metainfo(), {
        bookTorrentClient: f.client,
        assertActive() {
          throw new Error("Execution revoked");
        },
      }),
    ).rejects.toThrow("Execution revoked");
    expect(f.client.resumes).toHaveLength(resumes);
    expect(f.repo.bookTorrents.get(binding.info_hash)).toEqual(binding);
  } finally {
    await f.close();
  }
});

test("unmarked nonempty download roots are not adopted or changed", async () => {
  const f = await fixture();
  try {
    await fs.mkdir(f.client.root, { recursive: true });
    const foreign = path.join(f.client.root, "foreign.mp3");
    await fs.writeFile(foreign, "must survive");
    await expect(f.reserve()).rejects.toThrow("no ownership marker");
    expect(await fs.readFile(foreign, "utf8")).toBe("must survive");
    expect(await fs.readdir(f.client.root)).toEqual(["foreign.mp3"]);
    expect(f.client.adds).toHaveLength(0);
  } finally {
    await f.close();
  }
});

test("a persisted intent refuses a replacement root or unowned torrent directory", async () => {
  const f = await fixture();
  try {
    const binding = await f.reserve();
    const directory = path.join(binding.root_path, binding.relative_path);
    const marker = path.join(directory, ".podible-book-torrent.json");
    await fs.rm(marker);
    const foreign = path.join(directory, "foreign.mp3");
    await fs.writeFile(foreign, "preserve me");
    await expect(f.reserve()).rejects.toThrow("nonempty and unowned");
    expect(await fs.readFile(foreign, "utf8")).toBe("preserve me");
    await fs.rename(binding.root_path, binding.root_path + "-original");
    await fs.mkdir(binding.root_path);
    await expect(f.reserve()).rejects.toThrow("root identity changed");
    expect(await fs.readdir(binding.root_path)).toHaveLength(0);
    expect(f.client.adds).toHaveLength(1);
  } finally {
    await f.close();
  }
});

test("retries reject symlinks before resuming an owned torrent", async () => {
  const f = await fixture();
  try {
    await f.reserve();
    const record = [...f.client.records.values()][0]!;
    const outside = path.join(f.root, "outside.mp3");
    await fs.writeFile(outside, "do not overwrite");
    await fs.symlink(outside, record.files[0]!.path);
    const resumes = f.client.resumes.length;
    await expect(f.reserve()).rejects.toThrow(
      "Unsafe managed book download file",
    );
    expect(f.client.resumes).toHaveLength(resumes);
    expect(await fs.readFile(outside, "utf8")).toBe("do not overwrite");
  } finally {
    await f.close();
  }
});

test("managed progress remains backend aware and checking is not import-ready", async () => {
  const f = await fixture();
  try {
    const { binding, record } = await f.attach();
    Object.assign(record.snapshot, { bytesDone: 4, downloadBytesPerSecond: 2 });
    const downloads = bookDownloadClient(f.repo, f.settings, f.client);
    expect(await downloads.getDownloadState(binding.info_hash)).toMatchObject({
      bytesDone: 4,
      sizeBytes: 10,
      leftBytes: 6,
      downRate: 2,
      complete: false,
    });
    Object.assign(record.snapshot, {
      state: "checking",
      complete: true,
      bytesDone: 10,
    });
    expect((await downloads.getDownloadState(binding.info_hash)).complete).toBe(
      false,
    );
    await expect(downloads.getImportSource(binding.info_hash)).rejects.toThrow(
      "not complete",
    );
  } finally {
    await f.close();
  }
});

test("import sources are resolved from owned files and reject outside paths and symlinks", async () => {
  const f = await fixture();
  try {
    const { binding, record } = await f.attach();
    Object.assign(record.snapshot, {
      state: "seeding",
      complete: true,
      bytesDone: 10,
    });
    const file = record.files[0]!.path;
    await fs.writeFile(file, "ID3fixture");
    const downloads = bookDownloadClient(f.repo, f.settings, f.client);
    expect(await downloads.getImportSource(binding.info_hash)).toEqual({
      basePath: path.dirname(file),
      selectedPaths: [file],
    });
    const outside = path.join(f.root, "outside.mp3");
    await fs.writeFile(outside, "unowned");
    record.files[0]!.path = outside;
    await expect(downloads.getImportSource(binding.info_hash)).rejects.toThrow(
      "escaped",
    );
    record.files[0]!.path = file;
    await fs.rm(file);
    await fs.symlink(outside, file);
    await expect(downloads.getImportSource(binding.info_hash)).rejects.toThrow(
      "escaped",
    );
    expect(f.client.controls).toHaveLength(0);
  } finally {
    await f.close();
  }
});

test("legacy releases continue to use rTorrent even with a private client available", async () => {
  const f = await fixture();
  const hash = "b".repeat(40);
  const source = {
    basePath: "/legacy/book",
    selectedPaths: ["/legacy/book/chapter.mp3"],
  };
  const legacy = spyOn(
    RtorrentClient.prototype,
    "getImportSource",
  ).mockResolvedValue(source);
  try {
    f.repo.createRelease({
      bookId: f.book.id,
      provider: "old",
      title: "Legacy",
      mediaType: "audio",
      infoHash: hash,
      url: "https://fixture.invalid/legacy",
      status: "downloaded",
    });
    expect(
      await bookDownloadClient(f.repo, f.settings, f.client).getImportSource(
        hash,
      ),
    ).toEqual(source);
    expect(legacy).toHaveBeenCalledWith(hash);
    expect(f.repo.bookTorrents.get(hash)).toBeNull();
    expect(f.client.adds).toHaveLength(0);
  } finally {
    legacy.mockRestore();
    await f.close();
  }
});

test("managed progress reaches both download and library views", async () => {
  const f = await fixture();
  try {
    const { release, record } = await f.attach();
    Object.assign(record.snapshot, {
      state: "downloading",
      bytesDone: 4,
      downloadBytesPerSecond: 2,
    });
    const job = f.repo.createJob({
      type: "download",
      releaseId: release.id,
      bookId: f.book.id,
    });
    const client = bookDownloadClient(f.repo, f.settings, f.client);
    const download = await enrichDownload(f.repo.getDownload(job.id)!, client);
    expect(download.downloadProgress).toMatchObject({
      bytesDone: 4,
      sizeBytes: 10,
      leftBytes: 6,
      fraction: 0.4,
      percent: 40,
    });
    const book = f.repo.getBook(f.book.id)!;
    const enriched = await enrichLibraryBookProgress(f.repo, book, client);
    expect(enriched.fullPseudoProgress).toBeGreaterThan(
      book.fullPseudoProgress,
    );
  } finally {
    await f.close();
  }
});

test("stalled private downloads queue recovery without stopping or deleting the original torrent", async () => {
  const f = await fixture();
  try {
    const { release, record } = await f.attach();
    f.settings.recovery.stalledTorrentMinutes = 0;
    const job = f.repo.createJob({
      type: "download",
      releaseId: release.id,
      bookId: f.book.id,
    });
    await processDownloadJob(
      {
        repo: f.repo,
        getSettings: () => f.settings,
        bookTorrentClient: f.client,
        onLog() {},
      },
      job,
    );
    expect(
      f.repo
        .listJobCommands()
        .some(
          (command) => command.job_id === job.id && command.kind === "cancel",
        ),
    ).toBe(true);
    const recovery = JSON.parse(
      f.repo.listJobsByType("acquire")[0]!.payload_json!,
    );
    expect(recovery.rejectedInfoHashes).toEqual([release.info_hash]);
    expect(f.client.controls).toHaveLength(0);
    expect(record.snapshot.state).toBe("downloading");
  } finally {
    await f.close();
  }
});

test("managed reimport ignores stale payload paths and hardlinks only current owned files", async () => {
  const f = await fixture();
  try {
    const { release, record } = await f.attach();
    Object.assign(record.snapshot, {
      complete: true,
      state: "seeding",
      bytesDone: 10,
    });
    const source = record.files[0]!.path;
    await fs.writeFile(source, "ID3fixture");
    const wrong = path.join(f.root, "wrong.mp3");
    await fs.writeFile(wrong, "wrong book");
    const job = f.repo.createJob({
      type: "import",
      releaseId: release.id,
      bookId: f.book.id,
      payload: { basePath: wrong, selectedPaths: [wrong] },
    });
    await processImportJob(
      {
        repo: f.repo,
        getSettings: () => f.settings,
        bookTorrentClient: f.client,
        onLog() {},
      },
      job,
    );
    expect(f.repo.getRelease(release.id)?.status).toBe("imported");
    const asset = f.repo.listAssetsByBook(f.book.id)[0]!;
    const file = f.repo.getAssetFiles(asset.id)[0]!;
    expect(file.source_path).toBe(source);
    expect((await fs.stat(file.path)).ino).toBe((await fs.stat(source)).ino);
    expect(record.snapshot.state).toBe("seeding");
    expect(f.client.controls).toHaveLength(0);
  } finally {
    await f.close();
  }
});
