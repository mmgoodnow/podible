import { promises as fs } from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { z } from "zod";
import type { BooksRepo } from "../repo";
import { initializeManagedContent } from "../media/library-view";

function key(root: string) {
  return `movie-content-mount:${createHash("sha256").update(path.resolve(root)).digest("hex")}`;
}
export async function assertMovieStorage(repo: BooksRepo, contentRoot: string) {
  const expected = repo.getJsonState<string>(key(contentRoot));
  if (!expected)
    throw new Error("Movie content storage has not been initialized");
  const marker = path.join(contentRoot, ".podible-content.json");
  const stat = await fs.lstat(marker);
  if (
    !stat.isFile() ||
    stat.isSymbolicLink() ||
    (await fs.realpath(contentRoot)) !== path.resolve(contentRoot)
  )
    throw new Error("Movie content mount is not canonical");
  const identity = z
    .object({ id: z.string().uuid() })
    .parse(JSON.parse(await fs.readFile(marker, "utf8"))).id;
  if (identity !== expected)
    throw new Error(
      "Movie content mount identity changed; refusing filesystem writes",
    );
}
export async function prepareMovieStorage(
  repo: BooksRepo,
  contentRoot: string,
) {
  if (!repo.getJsonState<string>(key(contentRoot))) {
    const id = await initializeManagedContent(contentRoot);
    repo.setJsonState(key(contentRoot), id);
  }
  await assertMovieStorage(repo, contentRoot);
}
