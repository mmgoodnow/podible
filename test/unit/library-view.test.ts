import { expect, test } from "bun:test";
import {
  promises as fs,
  lstatSync,
  readdirSync,
  readFileSync,
  readlinkSync,
} from "node:fs";
import path from "node:path";
import os from "node:os";
import {
  initializeManagedContent,
  reconcileLibraryView,
} from "../../src/media/library-view";

type Fixture = {
  root: string;
  contentRoot: string;
  viewRoot: string;
  file: string;
  reconcile: (relative: string) => ReturnType<typeof reconcileLibraryView>;
};

async function fixture(run: (ctx: Fixture) => Promise<void>) {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(os.tmpdir(), "podible-view-")),
  );
  const contentRoot = path.join(root, "content");
  const viewRoot = path.join(root, "view");
  await initializeManagedContent(contentRoot);
  const file = path.join(contentRoot, "source.mkv");
  await Bun.write(file, "canonical bytes");
  try {
    await run({
      root,
      contentRoot,
      viewRoot,
      file,
      reconcile: (relative) =>
        reconcileLibraryView({
          contentRoot,
          viewRoot,
          entries: [{ relativePath: relative, contentPath: file }],
        }),
    });
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
}

async function legacyFile(ctx: Fixture, relative: string) {
  const destination = path.join(ctx.viewRoot, relative);
  await fs.mkdir(path.dirname(destination), { recursive: true });
  await fs.link(ctx.file, destination);
  return { relativePath: relative, contentPath: ctx.file };
}

function snapshot(root: string) {
  return readdirSync(root, { recursive: true, encoding: "utf8" })
    .sort()
    .map((relative) => {
      const file = path.join(root, relative);
      const info = lstatSync(file);
      return [
        relative,
        info.isSymbolicLink()
          ? readlinkSync(file)
          : info.isFile()
            ? readFileSync(file, "utf8")
            : "directory",
      ];
    });
}

test("view reconciliation repairs links, converges naming changes and never deletes content", () =>
  fixture(async (ctx) => {
    expect(await ctx.reconcile("Movie (2020)/Movie.mkv")).toEqual({
      linked: 1,
      removed: 0,
      unchanged: 0,
    });
    expect((await ctx.reconcile("Movie (2020)/Movie.mkv")).unchanged).toBe(1);
    await fs.unlink(path.join(ctx.viewRoot, "Movie (2020)/Movie.mkv"));
    expect((await ctx.reconcile("Movie (2020)/Movie.mkv")).linked).toBe(1);
    expect(await ctx.reconcile("Movie (2020) {tmdb-42}/Movie.mkv")).toEqual({
      linked: 1,
      removed: 1,
      unchanged: 0,
    });
    await reconcileLibraryView({ ...ctx, entries: [] });
    expect(await Bun.file(ctx.file).text()).toBe("canonical bytes");
  }));

test("view reconciliation resumes publication after a crash between link creation and acknowledgement", () =>
  fixture(async (ctx) => {
    await expect(
      reconcileLibraryView({
        ...ctx,
        entries: [{ relativePath: "movie.mkv", contentPath: ctx.file }],
        onLinkApplied: () => {
          throw new Error("crash");
        },
      }),
    ).rejects.toThrow("crash");
    expect((await ctx.reconcile("movie.mkv")).unchanged).toBe(1);
    expect((await ctx.reconcile("renamed.mkv")).removed).toBe(1);
  }));

test("repeated interrupted layout changes retain ownership of every unfinished link", () =>
  fixture(async (ctx) => {
    for (const relativePath of ["first.mkv", "second.mkv"]) {
      await expect(
        reconcileLibraryView({
          ...ctx,
          entries: [{ relativePath, contentPath: ctx.file }],
          onLinkApplied: () => {
            throw new Error("crash");
          },
        }),
      ).rejects.toThrow("crash");
    }
    expect(await ctx.reconcile("third.mkv")).toEqual({
      linked: 1,
      removed: 2,
      unchanged: 0,
    });
  }));

test("retirement rechecks ownership after other links were applied", () =>
  fixture(async (ctx) => {
    await ctx.reconcile("old.mkv");
    await expect(
      reconcileLibraryView({
        ...ctx,
        entries: [{ relativePath: "new.mkv", contentPath: ctx.file }],
        onLinkApplied: async () => {
          await fs.unlink(path.join(ctx.viewRoot, "old.mkv"));
          await Bun.write(path.join(ctx.viewRoot, "old.mkv"), "manual file");
        },
      }),
    ).rejects.toThrow("changed during reconciliation");
    expect(await Bun.file(path.join(ctx.viewRoot, "old.mkv")).text()).toBe(
      "manual file",
    );
  }));

test("view conflicts and missing mount fail closed", () =>
  fixture(async (ctx) => {
    await fs.mkdir(ctx.viewRoot, { recursive: true });
    await Bun.write(path.join(ctx.viewRoot, "movie.mkv"), "manual file");
    await expect(ctx.reconcile("movie.mkv")).rejects.toThrow("regular file");
    await fs.unlink(path.join(ctx.viewRoot, "movie.mkv"));
    await fs.symlink(ctx.file, path.join(ctx.viewRoot, "movie.mkv"));
    await expect(ctx.reconcile("movie.mkv")).rejects.toThrow("Foreign");
    await fs.unlink(path.join(ctx.viewRoot, "movie.mkv"));
    await ctx.reconcile("movie.mkv");
    await fs.rename(ctx.contentRoot, `${ctx.contentRoot}-unmounted`);
    await expect(
      reconcileLibraryView({ ...ctx, entries: [] }),
    ).rejects.toThrow();
    expect(
      (await fs.lstat(path.join(ctx.viewRoot, "movie.mkv"))).isSymbolicLink(),
    ).toBe(true);
  }));

test("view rejects traversal, parent symlinks, source aliases and portable collisions", () =>
  fixture(async (ctx) => {
    await expect(ctx.reconcile("../escaped.mkv")).rejects.toThrow("Unsafe");
    await fs.mkdir(ctx.viewRoot, { recursive: true });
    await fs.symlink(ctx.root, path.join(ctx.viewRoot, "escape"));
    await expect(ctx.reconcile("escape/movie.mkv")).rejects.toThrow(
      "real directory",
    );
    const sourceAlias = path.join(ctx.contentRoot, "alias.mkv");
    await fs.symlink(ctx.file, sourceAlias);
    await expect(
      reconcileLibraryView({
        ...ctx,
        entries: [{ relativePath: "alias.mkv", contentPath: sourceAlias }],
      }),
    ).rejects.toThrow("canonical");
    await expect(
      reconcileLibraryView({
        ...ctx,
        entries: ["movie.mkv", "MOVIE.mkv"].map((relativePath) => ({
          relativePath,
          contentPath: ctx.file,
        })),
      }),
    ).rejects.toThrow("Duplicate");
  }));

test("view allows canonical content nested within its first reserved .podible component", () =>
  fixture(async (ctx) => {
    const contentRoot = path.join(ctx.viewRoot, ".podible/content");
    await fs.mkdir(path.dirname(contentRoot), { recursive: true });
    await fs.rename(ctx.contentRoot, contentRoot);
    const file = path.join(contentRoot, "source.mkv");
    const input = {
      contentRoot,
      viewRoot: ctx.viewRoot,
      entries: [{ relativePath: "Book/Book.epub", contentPath: file }],
    };
    expect(await reconcileLibraryView(input)).toEqual({
      linked: 1,
      removed: 0,
      unchanged: 0,
    });
    expect(await fs.realpath(path.join(ctx.viewRoot, "Book/Book.epub"))).toBe(file);
    await reconcileLibraryView({ ...input, entries: [] });
    expect(await Bun.file(file).text()).toBe("canonical bytes");
    for (const relativePath of [
      ".podible/content/source.mkv",
      "Book/.podible/source.mkv",
    ]) {
      await expect(
        reconcileLibraryView({
          ...input,
          entries: [{ relativePath, contentPath: file }],
        }),
      ).rejects.toThrow("Unsafe");
      await expect(
        reconcileLibraryView({
          ...input,
          entries: [],
          adoptRegularFiles: [{ relativePath, contentPath: file }],
        }),
      ).rejects.toThrow("Unsafe");
    }
  }));

test("view rejects all other nested, equal and enclosing content roots", () =>
  fixture(async (ctx) => {
    for (const relative of [
      "public/content",
      ".podible-other/content",
      "Book/.podible/content",
    ]) {
      const contentRoot = path.join(ctx.viewRoot, relative);
      await initializeManagedContent(contentRoot);
      await expect(
        reconcileLibraryView({ contentRoot, viewRoot: ctx.viewRoot, entries: [] }),
      ).rejects.toThrow("separate");
    }
    for (const viewRoot of [
      ctx.contentRoot,
      path.join(ctx.contentRoot, "view"),
      path.join(ctx.contentRoot, ".podible/view"),
    ]) {
      await expect(
        reconcileLibraryView({
          contentRoot: ctx.contentRoot,
          viewRoot,
          entries: [],
        }),
      ).rejects.toThrow("separate");
    }
  }));

test("nested private content still rejects a symlinked content root", () =>
  fixture(async (ctx) => {
    const contentRoot = path.join(ctx.viewRoot, ".podible/content");
    await fs.mkdir(path.dirname(contentRoot), { recursive: true });
    await fs.symlink(ctx.contentRoot, contentRoot);
    const before = snapshot(ctx.root);
    await expect(
      reconcileLibraryView({
        contentRoot,
        viewRoot: ctx.viewRoot,
        entries: [],
      }),
    ).rejects.toThrow("Content root traverses a symlink");
    expect(snapshot(ctx.root)).toEqual(before);
  }));

test("adoption replaces only matching hardlinks and retires old names after publishing new links", () =>
  fixture(async (ctx) => {
    const adoptRegularFiles = await Promise.all([
      legacyFile(ctx, "same.epub"),
      legacyFile(ctx, "Old Title/old.epub"),
    ]);
    const canonical = await fs.stat(ctx.file, { bigint: true });
    const stale = path.join(ctx.viewRoot, "Old Title/old.epub");
    expect(
      await reconcileLibraryView({
        ...ctx,
        entries: [
          { relativePath: "same.epub", contentPath: ctx.file },
          { relativePath: "New Title/new.epub", contentPath: ctx.file },
        ],
        adoptRegularFiles,
        onLinkApplied: async () => {
          expect((await fs.lstat(stale)).isFile()).toBe(true);
          const pending = await Bun.file(
            path.join(ctx.viewRoot, ".podible-view-pending.json"),
          ).json();
          expect(pending.adoptions["same.epub"]).toEqual({
            source: "source.mkv",
            dev: canonical.dev.toString(),
            ino: canonical.ino.toString(),
            size: canonical.size.toString(),
          });
          expect(pending.files["Old Title/old.epub"]).toEqual([]);
        },
      }),
    ).toEqual({ linked: 2, removed: 1, unchanged: 0 });
    expect(
      (await fs.lstat(path.join(ctx.viewRoot, "same.epub"))).isSymbolicLink(),
    ).toBe(true);
    expect(
      await fs.realpath(path.join(ctx.viewRoot, "New Title/new.epub")),
    ).toBe(ctx.file);
    expect(await Bun.file(stale).exists()).toBe(false);
    expect((await fs.stat(ctx.file, { bigint: true })).ino).toBe(canonical.ino);
    expect(await Bun.file(ctx.file).text()).toBe("canonical bytes");
    expect(
      await Bun.file(
        path.join(ctx.viewRoot, ".podible-view-pending.json"),
      ).exists(),
    ).toBe(false);
    // A completed migration does not leave regular-file overwrite permission behind.
    await fs.unlink(path.join(ctx.viewRoot, "same.epub"));
    await fs.link(ctx.file, path.join(ctx.viewRoot, "same.epub"));
    await expect(ctx.reconcile("same.epub")).rejects.toThrow("regular file");
  }));

test("adoption preflights every inode before any publication or parent creation", () =>
  fixture(async (ctx) => {
    const matching = await legacyFile(ctx, "matching.epub");
    const foreign = path.join(ctx.viewRoot, "foreign.epub");
    await fs.copyFile(ctx.file, foreign);
    expect((await fs.stat(foreign)).ino).not.toBe((await fs.stat(ctx.file)).ino);
    const before = snapshot(ctx.root);
    await expect(
      reconcileLibraryView({
        ...ctx,
        entries: [
          matching,
          { relativePath: "New Title/new.epub", contentPath: ctx.file },
        ],
        adoptRegularFiles: [
          matching,
          { relativePath: "foreign.epub", contentPath: ctx.file },
        ],
      }),
    ).rejects.toThrow("does not match canonical");
    expect(snapshot(ctx.root)).toEqual(before);
  }));

test("adoption cannot authorize a foreign symlink even when it targets canonical content", () =>
  fixture(async (ctx) => {
    await fs.mkdir(ctx.viewRoot);
    const file = path.join(ctx.viewRoot, "legacy.epub");
    await fs.symlink(ctx.file, file);
    const before = snapshot(ctx.root);
    const legacy = { relativePath: "legacy.epub", contentPath: ctx.file };
    for (const entries of [[], [legacy]]) {
      await expect(
        reconcileLibraryView({
          ...ctx,
          entries,
          adoptRegularFiles: [legacy],
        }),
      ).rejects.toThrow("Foreign");
      expect(snapshot(ctx.root)).toEqual(before);
    }
  }));

test("adoption validates canonical sources, parents, and collisions without modifying the tree", () =>
  fixture(async (ctx) => {
    const legacy = await legacyFile(ctx, "legacy.epub");
    const alias = path.join(ctx.contentRoot, "alias.mkv");
    await fs.symlink(ctx.file, alias);
    await fs.symlink(ctx.contentRoot, path.join(ctx.viewRoot, "alias"));
    const outside = path.join(ctx.root, "outside.mkv");
    await fs.link(ctx.file, outside);
    const before = snapshot(ctx.root);
    const attempts = [
      {
        adoptRegularFiles: [{ ...legacy, contentPath: alias }],
        error: "canonical",
      },
      {
        adoptRegularFiles: [{ ...legacy, contentPath: outside }],
        error: "Unsafe",
      },
      {
        adoptRegularFiles: [{ ...legacy, relativePath: "alias/source.mkv" }],
        error: "real directory",
      },
      { adoptRegularFiles: [legacy, legacy], error: "Duplicate" },
      {
        adoptRegularFiles: [legacy],
        entries: [{ ...legacy, relativePath: "LEGACY.epub" }],
        error: "Duplicate",
      },
      {
        adoptRegularFiles: [legacy],
        entries: [{ ...legacy, relativePath: "legacy.epub/child" }],
        error: "Conflicting",
      },
    ];
    for (const attempt of attempts) {
      await expect(
        reconcileLibraryView({ ...ctx, entries: [], ...attempt }),
      ).rejects.toThrow(attempt.error);
      expect(snapshot(ctx.root)).toEqual(before);
    }
  }));

test("pending adoption recovers partial swaps with or without a repeated adoption request", async () => {
  for (const repeatAdoption of [false, true]) {
    await fixture(async (ctx) => {
      const adoptRegularFiles = await Promise.all([
        legacyFile(ctx, "first.epub"),
        legacyFile(ctx, "second.epub"),
        legacyFile(ctx, "stale.epub"),
      ]);
      const entries = [
        adoptRegularFiles[0],
        adoptRegularFiles[1],
        { relativePath: "Renamed/book.epub", contentPath: ctx.file },
      ];
      await expect(
        reconcileLibraryView({
          ...ctx,
          entries,
          adoptRegularFiles,
          onLinkApplied: () => {
            throw new Error("crash");
          },
        }),
      ).rejects.toThrow("crash");
      expect(
        (await fs.lstat(path.join(ctx.viewRoot, "first.epub"))).isSymbolicLink(),
      ).toBe(true);
      expect(
        (await fs.lstat(path.join(ctx.viewRoot, "second.epub"))).isFile(),
      ).toBe(true);
      expect(
        (await fs.lstat(path.join(ctx.viewRoot, "stale.epub"))).isFile(),
      ).toBe(true);
      expect(
        await reconcileLibraryView({
          ...ctx,
          entries,
          ...(repeatAdoption ? { adoptRegularFiles } : {}),
        }),
      ).toEqual({ linked: 2, removed: 1, unchanged: 1 });
      expect(await Bun.file(ctx.file).text()).toBe("canonical bytes");
    });
  }
});

test("a missing legacy adoption path is harmless on migration retry", () =>
  fixture(async (ctx) => {
    const legacy = await legacyFile(ctx, "old.epub");
    const input = {
      ...ctx,
      entries: [{ relativePath: "new.epub", contentPath: ctx.file }],
      adoptRegularFiles: [legacy],
    };
    await reconcileLibraryView(input);
    expect(await reconcileLibraryView(input)).toEqual({
      linked: 0,
      removed: 0,
      unchanged: 1,
    });
    expect(await Bun.file(path.join(ctx.viewRoot, "old.epub")).exists()).toBe(
      false,
    );
    expect(await Bun.file(ctx.file).text()).toBe("canonical bytes");
  }));

test("adopted retirement rechecks identity after desired links were applied", () =>
  fixture(async (ctx) => {
    const legacy = await legacyFile(ctx, "old.epub");
    const old = path.join(ctx.viewRoot, legacy.relativePath);
    await expect(
      reconcileLibraryView({
        ...ctx,
        entries: [{ relativePath: "new.epub", contentPath: ctx.file }],
        adoptRegularFiles: [legacy],
        onLinkApplied: async () => {
          await fs.unlink(old);
          await fs.copyFile(ctx.file, old);
        },
      }),
    ).rejects.toThrow("changed during reconciliation");
    const before = snapshot(ctx.root);
    await expect(ctx.reconcile("new.epub")).rejects.toThrow(
      "does not match canonical",
    );
    expect(snapshot(ctx.root)).toEqual(before);
    expect(await Bun.file(old).text()).toBe("canonical bytes");
  }));

test("pending regular adoption alone never authorizes a symlink at the legacy path", () =>
  fixture(async (ctx) => {
    const legacy = await legacyFile(ctx, "old.epub");
    await expect(
      reconcileLibraryView({
        ...ctx,
        entries: [{ relativePath: "new.epub", contentPath: ctx.file }],
        adoptRegularFiles: [legacy],
        onLinkApplied: () => {
          throw new Error("crash");
        },
      }),
    ).rejects.toThrow("crash");
    const old = path.join(ctx.viewRoot, legacy.relativePath);
    await fs.unlink(old);
    await fs.symlink(ctx.file, old);
    const before = snapshot(ctx.root);
    await expect(ctx.reconcile("new.epub")).rejects.toThrow("Foreign");
    expect(snapshot(ctx.root)).toEqual(before);
  }));

test("pending adoption rejects a changed canonical file or conflicting adoption", () =>
  fixture(async (ctx) => {
    const legacy = await legacyFile(ctx, "old.epub");
    await expect(
      reconcileLibraryView({
        ...ctx,
        entries: [{ relativePath: "new.epub", contentPath: ctx.file }],
        adoptRegularFiles: [legacy],
        onLinkApplied: () => {
          throw new Error("crash");
        },
      }),
    ).rejects.toThrow("crash");
    const other = path.join(ctx.contentRoot, "other.mkv");
    await fs.copyFile(ctx.file, other);
    const before = snapshot(ctx.root);
    await expect(
      reconcileLibraryView({
        ...ctx,
        entries: [],
        adoptRegularFiles: [{ ...legacy, contentPath: other }],
      }),
    ).rejects.toThrow("Adoption identity changed");
    expect(snapshot(ctx.root)).toEqual(before);
    await fs.rename(other, ctx.file);
    const changed = snapshot(ctx.root);
    await expect(ctx.reconcile("new.epub")).rejects.toThrow(
      "canonical file identity changed",
    );
    expect(snapshot(ctx.root)).toEqual(changed);
  }));

test("execution fencing stops publication and legacy retirement after a partial swap", () =>
  fixture(async (ctx) => {
    const legacy = await legacyFile(ctx, "old.epub");
    let active = true;
    const entries = [
      { relativePath: "first.epub", contentPath: ctx.file },
      { relativePath: "second.epub", contentPath: ctx.file },
    ];
    await expect(
      reconcileLibraryView({
        ...ctx,
        entries,
        adoptRegularFiles: [legacy],
        assertActive: () => {
          if (!active) throw new Error("superseded");
        },
        onLinkApplied: () => {
          active = false;
        },
      }),
    ).rejects.toThrow("superseded");
    expect((await fs.lstat(path.join(ctx.viewRoot, "old.epub"))).isFile()).toBe(
      true,
    );
    expect(await Bun.file(path.join(ctx.viewRoot, "second.epub")).exists()).toBe(
      false,
    );
    expect(await reconcileLibraryView({ ...ctx, entries })).toEqual({
      linked: 1,
      removed: 1,
      unchanged: 1,
    });
  }));

test("execution fencing leaves no subsequent mutations at every write boundary and can retry", async () => {
  const prepare = async (ctx: Fixture) => {
    await ctx.reconcile("owned-old.epub");
    return {
      ...ctx,
      entries: [
        { relativePath: "same.epub", contentPath: ctx.file },
        { relativePath: "Nested/new.epub", contentPath: ctx.file },
      ],
      adoptRegularFiles: await Promise.all([
        legacyFile(ctx, "same.epub"),
        legacyFile(ctx, "stale.epub"),
      ]),
    };
  };
  let checks = 0;
  await fixture(async (ctx) => {
    await reconcileLibraryView({
      ...await prepare(ctx),
      assertActive: () => {
        checks++;
      },
    });
  });
  for (let fenceAt = 1; fenceAt <= checks; fenceAt++) {
    await fixture(async (ctx) => {
      const input = await prepare(ctx);
      let count = 0;
      let atFence: ReturnType<typeof snapshot> | undefined;
      await expect(
        reconcileLibraryView({
          ...input,
          assertActive: () => {
            if (++count === fenceAt) {
              atFence = snapshot(ctx.root);
              throw new Error("superseded");
            }
          },
        }),
      ).rejects.toThrow("superseded");
      expect(snapshot(ctx.root)).toEqual(atFence!);
      await reconcileLibraryView(input);
      expect(await fs.realpath(path.join(ctx.viewRoot, "Nested/new.epub"))).toBe(
        ctx.file,
      );
      expect(await Bun.file(path.join(ctx.viewRoot, "stale.epub")).exists()).toBe(
        false,
      );
      expect(await Bun.file(ctx.file).text()).toBe("canonical bytes");
    });
  }
});
