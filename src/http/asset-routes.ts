import { Hono } from "hono";

import { ensureStoredTranscriptFile } from "../library/chapter-analysis";
import { buildChapters, streamAudioAsset, streamExtension } from "../library/media";
import { BooksRepo } from "../repo";
import { requireAuthenticatedRequest, type HttpEnv } from "./middleware";
import { jsonResponse, parseId } from "./route-helpers";

export function createAssetsIndexRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedRequest);
  app.get("/", (c) => {
    const bookId = parseId(c.req.query("bookId") ?? "");
    const assets = repo.listAssetsByBook(bookId).map((asset) => ({
      ...asset,
      files: repo.getAssetFiles(asset.id),
      stream_ext: streamExtension(asset),
    }));
    return c.json({ assets });
  });
  return app;
}

export function createStreamRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedRequest);
  app.get("/:idPart", async (c) => {
    const assetId = parseId((c.req.param("idPart").split(".")[0] ?? ""));
    const target = repo.getAssetWithFiles(assetId);
    if (!target) {
      return c.notFound();
    }
    const book = repo.getBookByAsset(assetId);
    return streamAudioAsset(c.req.raw, repo, target.asset, target.files, book?.cover_path);
  });
  return app;
}

export function createChaptersRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedRequest);
  app.get("/:idPart", async (c) => {
    const assetId = parseId(c.req.param("idPart").replace(/\.json$/i, ""));
    const target = repo.getAssetWithFiles(assetId);
    if (!target) {
      return c.json({ error: "not_found" }, 404);
    }
    const chapters = await buildChapters(repo, target.asset, target.files);
    if (!chapters) {
      return c.json({ error: "not_found" }, 404);
    }
    return jsonResponse(c.req.raw, chapters);
  });
  return app;
}

export function createTranscriptsRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedRequest);
  app.get("/:idPart", async (c) => {
    const assetId = parseId(c.req.param("idPart").replace(/\.json$/i, ""));
    const asset = repo.getAsset(assetId);
    if (!asset || asset.kind === "ebook") {
      return c.json({ error: "not_found" }, 404);
    }
    const transcriptPath = await ensureStoredTranscriptFile(repo, assetId);
    if (!transcriptPath) {
      return c.json({ error: "not_found" }, 404);
    }
    const file = Bun.file(transcriptPath);
    if (!(await file.exists())) {
      return c.json({ error: "not_found" }, 404);
    }
    return new Response(file, {
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store",
      },
    });
  });
  return app;
}

export function createCoverRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedRequest);
  app.get("/:idPart", async (c) => {
    const bookId = parseId(c.req.param("idPart").replace(/\.jpg$/i, ""));
    const book = repo.getBookRow(bookId);
    if (!book?.cover_path) {
      return c.notFound();
    }
    const file = Bun.file(book.cover_path);
    if (!(await file.exists())) {
      return c.notFound();
    }
    return new Response(file, {
      headers: {
        "Content-Type": book.cover_path.toLowerCase().endsWith(".png") ? "image/png" : "image/jpeg",
      },
    });
  });
  return app;
}

export function createEbookRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedRequest);
  app.get("/:assetId", async (c) => {
    const assetId = parseId(c.req.param("assetId"));
    const target = repo.getAssetWithFiles(assetId);
    if (!target || target.asset.kind !== "ebook") {
      return c.notFound();
    }
    const first = target.files[0];
    if (!first) {
      return c.notFound();
    }
    const file = Bun.file(first.path);
    if (!(await file.exists())) {
      return c.notFound();
    }
    return new Response(file, {
      headers: {
        "Content-Type": target.asset.mime,
        "Content-Disposition": `attachment; filename="${first.path.split("/").pop() ?? `book-${assetId}`}"`,
      },
    });
  });
  return app;
}
