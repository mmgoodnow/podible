import { expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import {
  initializeManagedContent,
  reconcileLibraryView,
} from "../../src/media/library-view";

async function fixture(
  run: (ctx: {
    root: string;
    contentRoot: string;
    viewRoot: string;
    file: string;
    reconcile: (relative: string) => ReturnType<typeof reconcileLibraryView>;
  }) => Promise<void>,
) {
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
