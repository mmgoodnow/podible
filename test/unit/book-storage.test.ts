import { afterEach, describe, expect, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  adoptBookContent,
  bookContentRoot,
  bookViewDirectory,
  bookViewEntries,
  prepareBookStorage,
  reconcileBookView,
  type BookStorageRepo,
} from "../../src/library/book-storage";

const temporaryRoots: string[] = [];
afterEach(async () => {
  for (const root of temporaryRoots.splice(0)) await fs.rm(root, { recursive: true, force: true });
});

async function fixture() {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "book-storage-")));
  temporaryRoots.push(root);
  const libraryRoot = path.join(root, "library");
  const source = path.join(root, "original.epub");
  await fs.mkdir(libraryRoot);
  await fs.writeFile(source, "immutable finalized book", { mode: 0o644 });
  return { root, libraryRoot, source };
}

type Book = ReturnType<BookStorageRepo["listAllBooks"]>[number];
type Asset = ReturnType<BookStorageRepo["listAssetsByBook"]>[number];
type File = ReturnType<BookStorageRepo["getAssetFiles"]>[number];

function fakeRepo(files: File[], asset: Asset = { id: 7, manifestation_id: 3 }) {
  const books: Book[] = [{ id: 1, author: "Test Author", title: "Test Book" }];
  const assets = new Map<number, Asset[]>([[1, [asset]]]);
  const assetFiles = new Map<number, File[]>([[asset.id, files]]);
  const repo: BookStorageRepo = {
    listAllBooks: () => books,
    listAssetsByBook: (id) => assets.get(id) ?? [],
    getAssetFiles: (id) => assetFiles.get(id) ?? [],
  };
  return { repo, books, assets, assetFiles };
}

async function exists(file: string) {
  return fs.lstat(file).then(() => true, (error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return false;
    throw error;
  });
}

describe("book canonical content", () => {
  test("preparation initializes only the private root, resolves aliases and is concurrent-repeat safe", async () => {
    const { root, libraryRoot } = await fixture();
    const alias = path.join(root, "library-alias");
    await fs.symlink(libraryRoot, alias);
    const prepared = await Promise.all(Array.from({ length: 12 }, (_, index) => prepareBookStorage(index % 2 ? alias : libraryRoot)));
    expect(new Set(prepared)).toEqual(new Set([path.join(libraryRoot, ".podible/content")]));
    expect(bookContentRoot(alias)).toBe(bookContentRoot(libraryRoot));
    const marker = path.join(bookContentRoot(libraryRoot), ".podible-content.json");
    const before = await fs.readFile(marker, "utf8");
    await prepareBookStorage(libraryRoot);
    expect(await fs.readFile(marker, "utf8")).toBe(before);
    expect(await fs.readdir(libraryRoot)).toEqual([".podible"]);
  });

  test("adoption hardlinks without copying, chmod, source deletion or presentation changes", async () => {
    const { libraryRoot, source } = await fixture();
    const before = await fs.stat(source);
    const canonical = await adoptBookContent(libraryRoot, source);
    const stored = await fs.stat(canonical);
    expect(canonical.startsWith(`${bookContentRoot(libraryRoot)}/`)).toBe(true);
    expect([stored.dev, stored.ino, stored.mode, stored.size, stored.mtimeMs]).toEqual([before.dev, before.ino, before.mode, before.size, before.mtimeMs]);
    expect(stored.nlink).toBe(before.nlink + 1);
    expect(await fs.readFile(source, "utf8")).toBe("immutable finalized book");
    expect(await adoptBookContent(libraryRoot, source)).toBe(canonical);
    expect(await adoptBookContent(libraryRoot, canonical)).toBe(canonical);
    expect((await fs.stat(source)).nlink).toBe(stored.nlink);
    expect(await fs.readdir(libraryRoot)).toEqual([".podible"]);
  });

  test("arbitrary stable keys are hashed and same-inode renamed sources reuse the original extension", async () => {
    const { root, libraryRoot, source } = await fixture();
    const key = "release:../outside\\name\0:edition/file:123";
    const canonical = await adoptBookContent(libraryRoot, source, key);
    expect(canonical.includes(createHash("sha256").update(`key\0${key}`).digest("hex"))).toBe(true);
    const renamed = path.join(root, "renamed.PDF");
    await fs.rename(source, renamed);
    expect(await adoptBookContent(libraryRoot, renamed, key)).toBe(canonical);
    expect(path.extname(canonical)).toBe(".epub");
    expect((await fs.stat(renamed)).ino).toBe((await fs.stat(canonical)).ino);
    await expect(adoptBookContent(libraryRoot, renamed, "")).rejects.toThrow("nonempty");
  });

  test("stable identities reject different inodes even when their bytes are identical", async () => {
    const { root, libraryRoot, source } = await fixture();
    const canonical = await adoptBookContent(libraryRoot, source, "same-key");
    const different = path.join(root, "different.epub");
    await fs.copyFile(source, different);
    await expect(adoptBookContent(libraryRoot, different, "same-key")).rejects.toThrow("different or changed");
    expect((await fs.stat(source)).ino).toBe((await fs.stat(canonical)).ino);
    expect(await fs.readFile(canonical, "utf8")).toBe("immutable finalized book");
  });

  test("retries recover both an already-linked object and a durable claim awaiting its link", async () => {
    const { libraryRoot, source } = await fixture();
    const canonical = await adoptBookContent(libraryRoot, source, "crash-retry");
    expect(await adoptBookContent(libraryRoot, source, "crash-retry")).toBe(canonical);
    await fs.unlink(canonical);
    expect(await adoptBookContent(libraryRoot, source, "crash-retry")).toBe(canonical);
    expect((await fs.stat(source)).ino).toBe((await fs.stat(canonical)).ino);
    expect((await fs.readdir(path.dirname(canonical))).sort()).toEqual([".podible-file.json", "content.epub"]);
  });

  test("concurrent adoptions of one key create one canonical hardlink", async () => {
    const { libraryRoot, source } = await fixture();
    const paths = await Promise.all(Array.from({ length: 20 }, () => adoptBookContent(libraryRoot, source, "parallel")));
    expect(new Set(paths).size).toBe(1);
    expect((await fs.stat(source)).nlink).toBe(2);
  });

  test("source symlinks, parent symlinks, directories and FIFOs fail closed", async () => {
    const { root, libraryRoot, source } = await fixture();
    const leaf = path.join(root, "alias.epub");
    await fs.symlink(source, leaf);
    await expect(adoptBookContent(libraryRoot, leaf)).rejects.toThrow("symlink");
    const parent = path.join(root, "parent-alias");
    await fs.symlink(root, parent);
    await expect(adoptBookContent(libraryRoot, path.join(parent, "original.epub"))).rejects.toThrow("symlink");
    await expect(adoptBookContent(libraryRoot, root)).rejects.toThrow("regular file");
    const fifo = path.join(root, "pipe.epub");
    const process = Bun.spawn(["mkfifo", fifo], { stdout: "pipe", stderr: "pipe" });
    expect(await process.exited).toBe(0);
    await expect(adoptBookContent(libraryRoot, fifo)).rejects.toThrow("regular file");
  });

  test("foreign nonempty content and linked private roots are never silently adopted", async () => {
    const { root, libraryRoot, source } = await fixture();
    const contentRoot = bookContentRoot(libraryRoot);
    await fs.mkdir(contentRoot, { recursive: true });
    await fs.writeFile(path.join(contentRoot, "foreign.epub"), "foreign bytes");
    await expect(prepareBookStorage(libraryRoot)).rejects.toThrow("no ownership marker");
    expect(await fs.readFile(path.join(contentRoot, "foreign.epub"), "utf8")).toBe("foreign bytes");
    await fs.rm(path.join(libraryRoot, ".podible"), { recursive: true });
    await fs.symlink(root, path.join(libraryRoot, ".podible"));
    await expect(adoptBookContent(libraryRoot, source)).rejects.toThrow("symlink");
    expect(await exists(path.join(root, "content"))).toBe(false);
  });

  test("ownership markers cannot be adopted, replaced, removed or traversed through a symlink", async () => {
    const { root, libraryRoot, source } = await fixture();
    await prepareBookStorage(libraryRoot);
    const marker = path.join(bookContentRoot(libraryRoot), ".podible-content.json");
    const original = await fs.readFile(marker, "utf8");
    await expect(adoptBookContent(libraryRoot, marker)).rejects.toThrow("ownership marker");
    const markerAlias = path.join(root, "marker.epub");
    await fs.link(marker, markerAlias);
    await expect(adoptBookContent(libraryRoot, markerAlias)).rejects.toThrow("ownership marker");
    await fs.writeFile(marker, JSON.stringify({ id: randomUUID() }));
    await expect(adoptBookContent(libraryRoot, source)).rejects.toThrow("marker changed");
    await fs.unlink(marker);
    await expect(prepareBookStorage(libraryRoot)).rejects.toThrow();
    await fs.writeFile(markerAlias, original);
    await fs.symlink(markerAlias, marker);
    await expect(prepareBookStorage(libraryRoot)).rejects.toThrow();
    expect(await fs.readFile(source, "utf8")).toBe("immutable finalized book");
  });

  test("object directory symlinks and destination collisions are not overwritten", async () => {
    const { root, libraryRoot, source } = await fixture();
    const canonical = await adoptBookContent(libraryRoot, source, "collision");
    await fs.unlink(canonical);
    await fs.writeFile(canonical, "manual bytes");
    await expect(adoptBookContent(libraryRoot, source, "collision")).rejects.toThrow("collision");
    expect(await fs.readFile(canonical, "utf8")).toBe("manual bytes");
    await fs.unlink(canonical);
    await fs.symlink(source, canonical);
    await expect(adoptBookContent(libraryRoot, source, "collision")).rejects.toThrow("collision");
    const contentRoot = bookContentRoot(libraryRoot);
    await fs.rename(path.join(contentRoot, "files"), path.join(root, "files"));
    await fs.symlink(path.join(root, "files"), path.join(contentRoot, "files"));
    await expect(adoptBookContent(libraryRoot, source, "other-key")).rejects.toThrow("symlink");
  });

  test("a modified source violates the immutable identity rather than silently reusing the claim", async () => {
    const { libraryRoot, source } = await fixture();
    await adoptBookContent(libraryRoot, source, "immutable");
    await fs.writeFile(source, "a changed finalized source");
    await expect(adoptBookContent(libraryRoot, source, "immutable")).rejects.toThrow("different or changed");
  });
});

describe("book desired view", () => {
  test("current metadata derives a pure Author/Title path with stable edition and asset suffixes", () => {
    const libraryRoot = "/nonexistent/book-storage-pure";
    const contentPath = path.join(bookContentRoot(libraryRoot), "files/canonical.epub");
    const { repo, books } = fakeRepo([{ id: 10, path: contentPath, source_path: "/downloads/release.epub" }]);
    expect(bookViewDirectory(books[0]!)).toBe("Test Author/Test Book");
    expect(bookViewEntries(repo, libraryRoot)).toEqual([{ relativePath: "Test Author/Test Book/Test Book [m3-a7].epub", contentPath }]);
    books[0]!.title = "New Title";
    books[0]!.author = "New Author";
    expect(bookViewEntries(repo, libraryRoot)[0]!.relativePath).toBe("New Author/New Title/New Title [m3-a7].epub");
    expect(bookViewEntries(repo, libraryRoot)[0]!.contentPath).toBe(contentPath);
  });

  test("multipart ordering uses byte start then file ID and original identity basename, not storage hashes", () => {
    const libraryRoot = "/nonexistent/book-storage-parts";
    const contentRoot = bookContentRoot(libraryRoot);
    const files = [
      { id: 21, start: 100, path: `${contentRoot}/files/hash-two`, identity_path: "/old/02 Original.MP3", source_path: "/downloads/wrong-name.pdf" },
      { id: 20, start: 0, path: `${contentRoot}/files/hash-one`, identity_path: null, source_path: "/downloads/01 First.mp3" },
      { id: 22, start: 100, path: `${contentRoot}/files/hash-three.mp3`, source_path: "/downloads/02 Original.mp3" },
    ];
    const { repo } = fakeRepo(files);
    expect(bookViewEntries(repo, libraryRoot).map((entry) => entry.relativePath)).toEqual([
      "Test Author/Test Book/001 - 01 First [m3-a7].mp3",
      "Test Author/Test Book/002 - 02 Original [m3-a7].mp3",
      "Test Author/Test Book/003 - 02 Original [m3-a7].mp3",
    ]);
    expect(files.map((file) => file.id)).toEqual([21, 20, 22]);
  });

  test("legacy and escaped DB paths and private records are excluded without filesystem reads", () => {
    const libraryRoot = "/nonexistent/book-storage-legacy";
    const contentRoot = bookContentRoot(libraryRoot);
    const { repo } = fakeRepo([
      { id: 1, path: `${libraryRoot}/Test Author/Test Book/legacy.epub` },
      { id: 2, path: `${contentRoot}-other/escape.epub` },
      { id: 3, path: `${contentRoot}/../escape.epub` },
      { id: 4, path: "relative.epub" },
      { id: 5, path: `${contentRoot}/.podible-content.json` },
      { id: 6, path: `${contentRoot}/files/valid.pdf` },
    ]);
    expect(bookViewEntries(repo, libraryRoot)).toEqual([{ relativePath: "Test Author/Test Book/006 - valid [m3-a7].pdf", contentPath: `${contentRoot}/files/valid.pdf` }]);
  });

  test("portable name collisions, empty names, unsafe segments and long Unicode names remain safe", () => {
    const libraryRoot = "/nonexistent/book-storage-portable";
    const contentRoot = bookContentRoot(libraryRoot);
    const { repo, books, assets, assetFiles } = fakeRepo([{ id: 1, path: `${contentRoot}/first.epub` }]);
    books.push({ id: 2, author: "TEST AUTHOR", title: "TEST BOOK" });
    assets.set(2, [{ id: 8, manifestation_id: 4 }, { id: 9, manifestation_id: null }]);
    assetFiles.set(8, [{ id: 2, path: `${contentRoot}/second.epub` }]);
    assetFiles.set(9, [{ id: 3, path: `${contentRoot}/third.epub` }]);
    const entries = bookViewEntries(repo, libraryRoot);
    expect(new Set(entries.map((entry) => entry.relativePath.normalize("NFC").toLowerCase())).size).toBe(3);
    expect(entries.some((entry) => entry.relativePath.includes("[m0-a9]"))).toBe(true);
    expect(bookViewDirectory({ id: 10, author: "..", title: "/\\:*?\"<>|\0" })).toBe("Unknown/book-10");
    expect(bookViewDirectory({ id: 10, author: ".podible", title: ".podible-view.json" })).toBe("_.podible/_.podible-view.json");
    const unicode = bookViewDirectory({ id: 10, author: "\u00e9".repeat(200), title: "\ud83d\udcd6".repeat(100) });
    for (const part of unicode.split("/")) {
      expect(Buffer.byteLength(part)).toBeLessThanOrEqual(100);
      expect(part.includes("\ufffd")).toBe(false);
    }
    expect(bookViewDirectory({ id: 1, author: "A/B", title: "A: B?" })).toBe("A B/A B");
  });
});

describe("book view reconciliation", () => {
  test("metadata renames and missing links converge without changing canonical bytes", async () => {
    const { libraryRoot, source } = await fixture();
    const canonical = await adoptBookContent(libraryRoot, source);
    const { repo, books } = fakeRepo([{ id: 1, path: canonical, source_path: source }]);
    expect(await reconcileBookView(repo, libraryRoot)).toEqual({ linked: 1, removed: 0, unchanged: 0 });
    const initial = path.join(libraryRoot, bookViewEntries(repo, libraryRoot)[0]!.relativePath);
    expect((await fs.lstat(initial)).isSymbolicLink()).toBe(true);
    expect(await fs.realpath(initial)).toBe(canonical);
    expect((await reconcileBookView(repo, libraryRoot)).unchanged).toBe(1);
    await fs.unlink(initial);
    expect((await reconcileBookView(repo, libraryRoot)).linked).toBe(1);
    books[0]!.title = "Renamed";
    expect(await reconcileBookView(repo, libraryRoot)).toEqual({ linked: 1, removed: 1, unchanged: 0 });
    expect(await exists(initial)).toBe(false);
    books.splice(0);
    expect((await reconcileBookView(repo, libraryRoot)).removed).toBe(1);
    expect(await fs.readFile(canonical, "utf8")).toBe("immutable finalized book");
    expect(await fs.readFile(source, "utf8")).toBe("immutable finalized book");
  });

  test("legacy bare presentation files remain untouched beside suffixed desired links", async () => {
    const { libraryRoot, source } = await fixture();
    const canonical = await adoptBookContent(libraryRoot, source);
    const legacy = path.join(libraryRoot, "Test Author/Test Book/Test Book.epub");
    await fs.mkdir(path.dirname(legacy), { recursive: true });
    await fs.writeFile(legacy, "manually managed book");
    const { repo } = fakeRepo([{ id: 1, path: canonical, source_path: source }, { id: 2, path: legacy }]);
    expect((await reconcileBookView(repo, libraryRoot)).linked).toBe(1);
    expect(await fs.readFile(legacy, "utf8")).toBe("manually managed book");
    expect((await fs.lstat(legacy)).isFile()).toBe(true);
  });

  test("identity paths beneath a configured root alias retire only inode-verified old regular files", async () => {
    const { root, libraryRoot } = await fixture();
    const alias = path.join(root, "configured-library");
    await fs.symlink(libraryRoot, alias);
    const oldRealPath = path.join(libraryRoot, "Old Author/Old Title/Original.epub");
    const identity = path.join(alias, "Old Author/Old Title/Original.epub");
    await fs.mkdir(path.dirname(oldRealPath), { recursive: true });
    await fs.writeFile(oldRealPath, "legacy finalized bytes");
    const canonical = await adoptBookContent(alias, oldRealPath, "file:1");
    const { repo } = fakeRepo([{ id: 1, path: canonical, identity_path: identity }]);
    await reconcileBookView(repo, alias);
    expect(await exists(oldRealPath)).toBe(false);
    expect(await fs.readFile(canonical, "utf8")).toBe("legacy finalized bytes");
    expect((await reconcileBookView(repo, alias)).unchanged).toBe(1);
    expect(repo.getAssetFiles(7)[0]!.identity_path).toBe(identity);
  });

  test("missing original paths are retry-safe and outside/private identities are not retired", async () => {
    const { libraryRoot, source } = await fixture();
    const canonical = await adoptBookContent(libraryRoot, source, "file:1");
    const missing = path.join(libraryRoot, "Old/Missing.epub");
    const privatePath = path.join(libraryRoot, ".podible/downloads/source.epub");
    await fs.mkdir(path.dirname(privatePath), { recursive: true });
    await fs.link(source, privatePath);
    const { repo, assetFiles } = fakeRepo([{ id: 1, path: canonical, identity_path: missing }]);
    await reconcileBookView(repo, libraryRoot);
    for (const identity of [source, privatePath, canonical]) {
      assetFiles.set(7, [{ id: 1, path: canonical, identity_path: identity }]);
      expect((await reconcileBookView(repo, libraryRoot)).unchanged).toBe(1);
      expect(await exists(identity)).toBe(true);
    }
  });

  test("different-inode legacy files and desired-path collisions fail closed", async () => {
    const { libraryRoot, source } = await fixture();
    const canonical = await adoptBookContent(libraryRoot, source);
    const oldPath = path.join(libraryRoot, "Old/Manual.epub");
    await fs.mkdir(path.dirname(oldPath), { recursive: true });
    await fs.copyFile(source, oldPath);
    const { repo, assetFiles } = fakeRepo([{ id: 1, path: canonical, identity_path: oldPath }]);
    await expect(reconcileBookView(repo, libraryRoot)).rejects.toThrow();
    expect((await fs.stat(oldPath)).ino).not.toBe((await fs.stat(canonical)).ino);
    expect(await fs.readFile(oldPath, "utf8")).toBe("immutable finalized book");
    assetFiles.set(7, [{ id: 1, path: canonical }]);
    const destination = path.join(libraryRoot, bookViewEntries(repo, libraryRoot)[0]!.relativePath);
    await fs.mkdir(path.dirname(destination), { recursive: true });
    await fs.writeFile(destination, "do not clobber");
    await expect(reconcileBookView(repo, libraryRoot)).rejects.toThrow("regular file");
    expect(await fs.readFile(destination, "utf8")).toBe("do not clobber");
  });

  test("concurrent reconciles are serialized by real view root and a failed run releases the queue", async () => {
    const { root, libraryRoot, source } = await fixture();
    const alias = path.join(root, "alias");
    await fs.symlink(libraryRoot, alias);
    const canonical = await adoptBookContent(libraryRoot, source);
    const { repo } = fakeRepo([{ id: 1, path: canonical }]);
    const results = await Promise.all(Array.from({ length: 12 }, (_, index) => reconcileBookView(repo, index % 2 ? alias : libraryRoot)));
    expect(results.reduce((total, result) => total + result.linked, 0)).toBe(1);
    expect(results.reduce((total, result) => total + result.unchanged, 0)).toBe(11);
    await expect(reconcileBookView(repo, libraryRoot, () => { throw new Error("cancelled"); })).rejects.toThrow("cancelled");
    expect((await reconcileBookView(repo, libraryRoot)).unchanged).toBe(1);
  });

  test("assertActive runs before link mutation, and interrupted reconciliation can resume", async () => {
    const { libraryRoot, source } = await fixture();
    const canonical = await adoptBookContent(libraryRoot, source);
    const { repo } = fakeRepo([{ id: 1, path: canonical }]);
    let checks = 0;
    await reconcileBookView(repo, libraryRoot, () => { checks++; });
    expect(checks).toBeGreaterThan(4);
    const destination = path.join(libraryRoot, bookViewEntries(repo, libraryRoot)[0]!.relativePath);
    await fs.unlink(destination);
    // Cancel as soon as a link has been applied; the generic durable pending manifest owns it.
    await expect(reconcileBookView(repo, libraryRoot, () => {
      if (Bun.file(destination).size > 0) throw new Error("interrupted");
    })).rejects.toThrow("interrupted");
    expect((await reconcileBookView(repo, libraryRoot)).unchanged).toBe(1);
    expect(await fs.realpath(destination)).toBe(canonical);
  });
});
