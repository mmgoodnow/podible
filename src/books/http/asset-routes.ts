import { Hono } from "hono";

import { loadStoredTranscriptPayload } from "../chapter-analysis";
import { buildChapters, streamAudioAsset, streamExtension } from "../media";
import { BooksRepo } from "../repo";
import { requireAuthenticatedRequest, type HttpEnv } from "./middleware";
import { json, jsonResponse, parseId } from "./route-helpers";

export function createAssetRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();

  app.use("/assets", requireAuthenticatedRequest);
  app.use("/stream/*", requireAuthenticatedRequest);
  app.use("/chapters/*", requireAuthenticatedRequest);
  app.use("/transcripts/*", requireAuthenticatedRequest);
  app.use("/covers/*", requireAuthenticatedRequest);
  app.use("/ebook/*", requireAuthenticatedRequest);

  app.get("/assets", (c) => {
    const bookId = parseId(c.req.query("bookId") ?? "");
    const assets = repo.listAssetsByBook(bookId).map((asset) => ({
      ...asset,
      files: repo.getAssetFiles(asset.id),
      stream_ext: streamExtension(asset),
    }));
    return json({ assets });
  });

  app.get("/stream/:idPart", async (c) => {
    const assetId = parseId((c.req.param("idPart").split(".")[0] ?? ""));
    const target = repo.getAssetWithFiles(assetId);
    if (!target) {
      return new Response("Not found", { status: 404 });
    }
    const book = repo.getBookByAsset(assetId);
    return streamAudioAsset(c.req.raw, repo, target.asset, target.files, book?.cover_path);
  });

  app.get("/chapters/:idPart", async (c) => {
    const assetId = parseId(c.req.param("idPart").replace(/\.json$/i, ""));
    const target = repo.getAssetWithFiles(assetId);
    if (!target) {
      return json({ error: "not_found" }, 404);
    }
    const chapters = await buildChapters(repo, target.asset, target.files);
    if (!chapters) {
      return json({ error: "not_found" }, 404);
    }
    return jsonResponse(c.req.raw, chapters);
  });

  app.get("/transcripts/:idPart", async (c) => {
    const assetId = parseId(c.req.param("idPart").replace(/\.json$/i, ""));
    const asset = repo.getAsset(assetId);
    if (!asset || asset.kind === "ebook") {
      return json({ error: "not_found" }, 404);
    }
    const transcript = await loadStoredTranscriptPayload(repo, assetId);
    if (!transcript) {
      return json({ error: "not_found" }, 404);
    }
    return jsonResponse(c.req.raw, transcript);
  });

  app.get("/covers/:idPart", async (c) => {
    const bookId = parseId(c.req.param("idPart").replace(/\.jpg$/i, ""));
    const book = repo.getBookRow(bookId);
    if (!book?.cover_path) {
      return new Response("Not found", { status: 404 });
    }
    const file = Bun.file(book.cover_path);
    if (!(await file.exists())) {
      return new Response("Not found", { status: 404 });
    }
    return new Response(file, {
      headers: {
        "Content-Type": book.cover_path.toLowerCase().endsWith(".png") ? "image/png" : "image/jpeg",
      },
    });
  });

  app.get("/ebook/:assetId", async (c) => {
    const assetId = parseId(c.req.param("assetId"));
    const target = repo.getAssetWithFiles(assetId);
    if (!target || target.asset.kind !== "ebook") {
      return new Response("Not found", { status: 404 });
    }
    const first = target.files[0];
    if (!first) {
      return new Response("Not found", { status: 404 });
    }
    const file = Bun.file(first.path);
    if (!(await file.exists())) {
      return new Response("Not found", { status: 404 });
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
