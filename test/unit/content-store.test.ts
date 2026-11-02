import { expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import { randomUUID } from "node:crypto";
import { initializeManagedContent } from "../../src/media/library-view";
import { adoptContentFile } from "../../src/media/content-store";

test("canonical content adoption is idempotent, independent of seeding bytes and refuses identity collisions", async () => {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "movie-content-")),
  );
  const contentRoot = `${root}/objects`;
  const sourceRoot = `${root}/downloads`;
  await initializeManagedContent(contentRoot);
  await fs.mkdir(sourceRoot);
  const sourcePath = `${sourceRoot}/movie.mkv`;
  await fs.writeFile(sourcePath, "verified movie");
  const input = {
    contentRoot,
    sourceRoot,
    sourcePath,
    fileId: randomUUID(),
    expectedSize: 14,
  };
  try {
    const file = await adoptContentFile(input);
    expect(await adoptContentFile(input)).toEqual(file);
    const stored = path.join(contentRoot, file.relativePath);
    expect((await fs.stat(stored)).ino).not.toBe(
      (await fs.stat(sourcePath)).ino,
    );
    await fs.writeFile(sourcePath, "changed movie!");
    expect(await fs.readFile(stored, "utf8")).toBe("verified movie");
    await expect(adoptContentFile(input)).rejects.toThrow("different bytes");
    expect(await fs.readFile(stored, "utf8")).toBe("verified movie");
    const alias = `${sourceRoot}/alias.mkv`;
    await fs.symlink(sourcePath, alias);
    await expect(
      adoptContentFile({ ...input, sourcePath: alias, fileId: randomUUID() }),
    ).rejects.toThrow("escapes");
    await expect(
      adoptContentFile({ ...input, expectedSize: 1, fileId: randomUUID() }),
    ).rejects.toThrow("size differs");
    expect(
      (await fs.readdir(path.dirname(stored))).filter((name) =>
        name.startsWith(".podible-copy-"),
      ),
    ).toEqual([]);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("cancelled adoption does not publish partial bytes or mutate the source", async () => {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "movie-content-cancel-")),
  );
  try {
    await initializeManagedContent(`${root}/objects`);
    await fs.mkdir(`${root}/downloads`);
    await fs.writeFile(
      `${root}/downloads/movie.mp4`,
      Buffer.alloc(2 * 1024 ** 2, 42),
    );
    const id = randomUUID();
    let calls = 0;
    await expect(
      adoptContentFile({
        contentRoot: `${root}/objects`,
        sourceRoot: `${root}/downloads`,
        sourcePath: `${root}/downloads/movie.mp4`,
        fileId: id,
        expectedSize: 2 * 1024 ** 2,
        assertActive() {
          if (++calls >= 3) throw new Error("cancelled");
        },
      }),
    ).rejects.toThrow("cancelled");
    expect(await fs.readdir(`${root}/objects/files/${id.slice(0, 2)}`)).toEqual(
      [],
    );
    expect((await fs.stat(`${root}/downloads/movie.mp4`)).size).toBe(
      2 * 1024 ** 2,
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
