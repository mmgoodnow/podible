import path from "node:path";
import { promises as fs } from "node:fs";
import type { BooksRepo } from "../repo";
import { adoptBookContent, bookContentRoot, reconcileBookView } from "./book-storage";

/** Adoption is regular maintenance, not a deployment-specific filesystem migration. */
export async function reconcileBookStorage(
  repo: BooksRepo,
  libraryRoot: string,
  assertActive: () => void = () => {},
  bookId?: number,
) {
  const books = repo.listAllBooks().filter((book) => bookId === undefined || book.id === bookId);
  const hasFiles = books.some((book) => repo.listAssetsByBook(book.id).some((asset) => repo.getAssetFiles(asset.id).length));
  if (!hasFiles && !(await fs.lstat(path.join(libraryRoot, ".podible", "content", ".podible-content.json")).catch((error) => {
    if (error.code === "ENOENT") return null;
    throw error;
  }))) return { adopted: 0, failures: [], view: { linked: 0, removed: 0, unchanged: 0 } };
  const configuredRoot = path.resolve(libraryRoot);
  await fs.mkdir(libraryRoot, { recursive: true });
  libraryRoot = await fs.realpath(libraryRoot);
  const contentRoot = bookContentRoot(libraryRoot);
  const failures: Array<{ fileId: number; error: string }> = [];
  let adopted = 0;
  for (const book of books) {
    for (const asset of repo.listAssetsByBook(book.id)) {
      for (const file of repo.getAssetFiles(asset.id)) {
        if (file.path.startsWith(contentRoot + path.sep)) continue;
        try {
          assertActive();
          const relative = path.relative(configuredRoot, file.path);
          const sourcePath = relative && relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative)
            ? path.join(libraryRoot, relative) : file.path;
          const source = await fs.lstat(sourcePath);
          if (!source.isFile() || source.isSymbolicLink() || source.size !== file.size)
            throw new Error("Legacy book file is missing, changed, or not a regular file");
          const canonicalPath = await adoptBookContent(configuredRoot, sourcePath, `legacy:${file.path}`);
          const current = await fs.stat(canonicalPath);
          if (current.dev !== source.dev || current.ino !== source.ino || current.size !== source.size)
            throw new Error("Book file changed during canonical adoption");
          assertActive();
          repo.relocateAssetFile(file.id, file.path, canonicalPath);
          adopted++;
        } catch (error) {
          // Cancellation/fencing errors must not turn into successful maintenance.
          assertActive();
          failures.push({ fileId: file.id, error: (error as Error).message });
        }
      }
    }
  }
  assertActive();
  const view = await reconcileBookView(repo, configuredRoot, assertActive);
  return { adopted, failures, view };
}
