import { Hono } from "hono";

import {
  buildManifestationChapters,
  streamAudioManifestation,
  streamExtension,
} from "../library/media";
import { BooksRepo } from "../repo";
import { requireAuthenticatedRequest, type HttpEnv } from "./middleware";
import { jsonResponse, parseId } from "./route-helpers";

function contentDispositionAttachment(filename: string): string {
  const fallback = filename
    .normalize("NFKD")
    .replace(/[^\x20-\x7E]+/g, "")
    .replace(/[\\"]/g, "_")
    .trim() || "download";
  return `attachment; filename="${fallback}"; filename*=UTF-8''${encodeURIComponent(filename)}`;
}

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
  app.get("/m/:idPart", async (c) => {
    const manifestationId = parseId(c.req.param("idPart").split(".")[0] ?? "");
    const target = repo.getManifestationWithContainers(manifestationId);
    if (!target) {
      return c.notFound();
    }
    const book = repo.getBookRow(target.manifestation.book_id);
    return streamAudioManifestation(c.req.raw, repo, target.manifestation, target.containers, book?.cover_path);
  });
  return app;
}

export function createChaptersRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  app.use("*", requireAuthenticatedRequest);
  app.get("/m/:idPart", async (c) => {
    const manifestationId = parseId(c.req.param("idPart").replace(/\.json$/i, ""));
    const target = repo.getManifestationWithContainers(manifestationId);
    if (!target) {
      return c.json({ error: "not_found" }, 404);
    }
    const chapters = await buildManifestationChapters(repo, target.manifestation, target.containers);
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
  app.get("/m/:idPart", async (c) => {
    const manifestationId = parseId(c.req.param("idPart").replace(/\.json$/i, ""));
    if (!repo.getManifestationWithContainers(manifestationId)) {
      return c.json({ error: "not_found" }, 404);
    }
    const row = repo.getManifestationTranscript(manifestationId);
    if (!row || row.status !== "succeeded" || !row.transcript_path) {
      return c.json({ error: "not_found" }, 404);
    }
    const file = Bun.file(row.transcript_path);
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
    if (!target || (target.asset.mime !== "application/epub+zip" && target.asset.mime !== "application/pdf")) {
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
        "Content-Disposition": contentDispositionAttachment(first.path.split("/").pop() ?? `book-${assetId}`),
      },
    });
  });
  return app;
}
